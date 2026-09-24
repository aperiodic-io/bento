package iggy

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/Jeffail/shutdown"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	iFieldAddresses      = "addresses"
	iFieldUsername       = "username"
	iFieldPassword       = "password"
	iFieldStream         = "stream"
	iFieldTopic          = "topic"
	iFieldConsumerGroup  = "consumer_group"
	iFieldExclusive      = "exclusive"
	iFieldStartFrom      = "start_from"
	iFieldBatchCount     = "batch_count"
	iFieldPollInterval   = "poll_interval"
	iFieldCommitPeriod   = "commit_period"
	iFieldCheckpointLim  = "checkpoint_limit"
	iFieldTLS            = "tls"
	iFieldTLSEnabled     = "enabled"
	iFieldTLSCAFile      = "root_cas_file"
	iFieldTLSServerName  = "server_name"
	iFieldTLSSkipVerify  = "skip_cert_verify"
	startFromFirst       = "first"
	startFromNext        = "next"
	assignmentSyncPeriod = 5 * time.Second
	requestTimeout       = 30 * time.Second
	// A connection whose requests keep failing is torn down after this many
	// consecutive failures, so the next connection dials the seeds afresh
	// (they may resolve to different nodes by then).
	maxConsecutiveFailures = 5
	maxFailureBackoff      = 10 * time.Second
	finalRequestGrace      = 5 * time.Second
)

func iggyInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Version("1.22.0").
		Categories("Services").
		Summary("Consumes messages from an Apache Iggy topic, storing consumer offsets only once messages are acknowledged downstream.").
		Description(`
Joins `+"`consumer_group`"+` (creating it when it does not exist) and consumes the partitions the server assigns to this member, or, with `+"`exclusive: true`"+`, consumes every partition as a single named consumer. Polls never auto-commit: an offset is stored for a partition only after every message of that partition up to and including it has been acknowledged by the output, so a crash, restart or failover replays the unacknowledged tail instead of dropping it (at-least-once delivery, duplicates are possible).

### Delivery and commits

Each poll of one partition becomes one message batch. Batches from the same partition may be in flight concurrently and acknowledged out of order: the input stores, every `+"`commit_period`"+`, the highest offset below which every message of the partition has been acknowledged. A rejected (nacked) batch is retried downstream until it succeeds, and no offset at or beyond it is stored meanwhile. Acknowledged offsets are also stored on shutdown, after which the input leaves the group. When a rebalance moves a partition to another member, what was acknowledged is stored as part of the handoff; messages still in flight at that point are delivered again by the new owner.

`+"`checkpoint_limit`"+` bounds the messages of one partition that may be in flight (delivered but not yet acknowledged); the partition is not polled while it is at the limit. It must exceed what the output holds before acknowledging (for example a full output batch plus the next one filling up), or consumption stalls.

A consumer group without a stored offset for a partition starts at `+"`start_from`"+`. With `+"`next`"+` the starting point is stored immediately, so a restart before the first commit does not skip what arrived in between.

### Connection handling

The client connects to the first reachable address in `+"`addresses`"+`, learns the rest of the cluster from it and follows leader changes itself. Every reconnect registers a new client identity with the server, which drops its group membership; the input then rejoins the group and keeps the delivery state of the partitions it is assigned again, so a reconnect neither replays nor skips what is already in flight. A connection whose requests keep failing is closed and dialled again from the seed addresses.

### Group membership after a failover

With Iggy 0.9.0, a group member whose session lived on the cluster's metadata leader is not removed from the group when that node dies. The client reconnects to the new leader under a new identity and rejoins, but the stale member keeps its share of the partitions (observed for as long as tested, and across the node's restart), so those partitions are no longer consumed. Nothing is lost or committed wrongly, but consumption of them stalls until the group is recreated. A deployment that runs a single instance per consumer should set `+"`exclusive: true`"+`, which does not use server-side membership.

### Metadata

This input adds the following metadata fields to each message:

`+"```text"+`
- iggy_stream
- iggy_topic
- iggy_partition_id
- iggy_offset
- iggy_timestamp (server append time, unix microseconds)
- iggy_message_id (the 128-bit id as 32 lowercase hex digits, most significant first)
- All user headers
`+"```"+`

User headers are added under their key. String and raw headers keep their bytes as they are (a binary header such as a `+"`dedup-key`"+` is byte-for-byte identical); integer headers are rendered in decimal, float headers in their shortest exact decimal form and bool headers as `+"`true`"+` or `+"`false`"+`. Keys are rendered the same way.

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).`).
		Fields(
			service.NewStringListField(iFieldAddresses).
				Description("Seed addresses (`host:port` of the TCP transport) of the Iggy nodes. They are tried in order until one accepts a connection.").
				Example([]string{"iggy-0.iggy:8090", "iggy-1.iggy:8090", "iggy-2.iggy:8090"}),
			service.NewStringField(iFieldUsername).
				Description("The user to sign in as."),
			service.NewStringField(iFieldPassword).
				Description("The password of the user.").
				Secret(),
			service.NewStringField(iFieldStream).
				Description("The stream to consume from, by name.").
				Example("raw"),
			service.NewStringField(iFieldTopic).
				Description("The topic to consume from, by name.").
				Example("trades.binance-futures"),
			service.NewStringField(iFieldConsumerGroup).
				Description("The consumer group to join, by name. It is created when it does not exist, and its stored offsets are where consumption resumes. With `exclusive` it names a single consumer instead."),
			service.NewBoolField(iFieldExclusive).
				Description("Consume every partition of the topic as the only consumer of this name, without joining a server-side consumer group. Offsets are stored for a single consumer named `consumer_group`, which is a separate namespace from the offsets of a group of the same name: switching this setting starts over from `start_from`. Run at most one instance per name. Use it for single-replica consumers, which it keeps consuming every partition across a cluster leader failover (see [group membership after a failover](#group-membership-after-a-failover)).").
				Default(false),
			service.NewStringAnnotatedEnumField(iFieldStartFrom, map[string]string{
				startFromFirst: "Start from the oldest retained message of the partition.",
				startFromNext:  "Start after the newest message of the partition at the time the input first sees it.",
			}).
				Description("Where to start a partition for which the consumer group has no stored offset. A stored offset always takes precedence.").
				Default(startFromFirst),
			service.NewIntField(iFieldBatchCount).
				Description("The maximum number of messages requested by one poll of one partition, and thus the maximum size of a message batch.").
				Default(1000),
			service.NewDurationField(iFieldPollInterval).
				Description("How long to wait before polling again when no assigned partition returned messages.").
				Default("100ms"),
			service.NewDurationField(iFieldCommitPeriod).
				Description("The period at which acknowledged offsets are stored.").
				Default("1s"),
			service.NewIntField(iFieldCheckpointLim).
				Description("The maximum number of messages of one partition that may be delivered but not yet acknowledged. Offsets are only stored once all messages before them are acknowledged, so a larger limit allows more concurrency and more duplicates after a crash.").
				Default(1024),
			service.NewObjectField(iFieldTLS,
				service.NewBoolField(iFieldTLSEnabled).
					Description("Whether to connect over TLS.").
					Default(false),
				service.NewStringField(iFieldTLSCAFile).
					Description("A PEM file of the certificate authority that signed the server certificate. The system pool is used when empty.").
					Default(""),
				service.NewStringField(iFieldTLSServerName).
					Description("The server name to verify the certificate against. Taken from the address when empty.").
					Default(""),
				service.NewBoolField(iFieldTLSSkipVerify).
					Description("Whether to skip server certificate verification.").
					Default(false),
			).
				Description("TLS for the TCP connection.").
				Advanced(),
		).
		Example("Raw archiver", "Consumes one raw topic and writes Parquet to S3; an offset is stored only after the upload containing its message succeeds.", `
input:
  iggy:
    addresses: [ iggy-0.iggy:8090, iggy-1.iggy:8090, iggy-2.iggy:8090 ]
    username: archiver
    password: ${IGGY_PASSWORD}
    stream: raw
    topic: trades.binance-futures
    consumer_group: raw-archiver-trades-binance-futures
    exclusive: true # one archiver replica per topic
    start_from: first
    batch_count: 1000
    checkpoint_limit: 400000
    commit_period: 1s

output:
  aws_s3:
    bucket: archive
    path: 'trades/binance-futures/${! timestamp_unix_nano() }-${! uuid_v4() }.parquet'
    max_in_flight: 4
    batching:
      count: 200000
      period: 30s
      processors:
        - protobuf_parquet_encode:
            message: live.producers.v1.Trade
            import_paths: [ /etc/bento/proto ]
            compression: zstd
`)
}

func init() {
	err := service.RegisterBatchInput("iggy", iggyInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			r, err := newIggyReaderFromConfig(conf, mgr)
			if err != nil {
				return nil, err
			}
			// A nacked batch is redelivered downstream until it succeeds, so
			// the ack function below only ever sees successes and the offsets
			// of a rejected batch are never stored.
			return service.AutoRetryNacksBatched(r), nil
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type readerConfig struct {
	addresses       []string
	username        string
	password        string
	stream          string
	topic           string
	consumerGroup   string
	exclusive       bool
	startFrom       string
	batchCount      uint32
	pollInterval    time.Duration
	commitPeriod    time.Duration
	checkpointLimit int64
	tls             tlsConfig
}

func readerConfigFromParsed(conf *service.ParsedConfig) (c readerConfig, err error) {
	if c.addresses, err = conf.FieldStringList(iFieldAddresses); err != nil {
		return
	}
	if len(c.addresses) == 0 {
		return c, errors.New("at least one address is required")
	}
	if c.username, err = conf.FieldString(iFieldUsername); err != nil {
		return
	}
	if c.password, err = conf.FieldString(iFieldPassword); err != nil {
		return
	}
	if c.stream, err = conf.FieldString(iFieldStream); err != nil {
		return
	}
	if c.topic, err = conf.FieldString(iFieldTopic); err != nil {
		return
	}
	if c.consumerGroup, err = conf.FieldString(iFieldConsumerGroup); err != nil {
		return
	}
	if c.consumerGroup == "" {
		return c, errors.New("consumer_group must not be empty")
	}
	if c.exclusive, err = conf.FieldBool(iFieldExclusive); err != nil {
		return
	}
	if c.startFrom, err = conf.FieldString(iFieldStartFrom); err != nil {
		return
	}
	batchCount, err := conf.FieldInt(iFieldBatchCount)
	if err != nil {
		return
	}
	if batchCount < 1 {
		return c, fmt.Errorf("batch_count must be at least 1, got %d", batchCount)
	}
	c.batchCount = uint32(batchCount)
	if c.pollInterval, err = conf.FieldDuration(iFieldPollInterval); err != nil {
		return
	}
	if c.commitPeriod, err = conf.FieldDuration(iFieldCommitPeriod); err != nil {
		return
	}
	if c.commitPeriod <= 0 {
		return c, errors.New("commit_period must be positive")
	}
	limit, err := conf.FieldInt(iFieldCheckpointLim)
	if err != nil {
		return
	}
	if limit < 1 {
		return c, fmt.Errorf("checkpoint_limit must be at least 1, got %d", limit)
	}
	c.checkpointLimit = int64(limit)
	tc := conf.Namespace(iFieldTLS)
	if c.tls.enabled, err = tc.FieldBool(iFieldTLSEnabled); err != nil {
		return
	}
	if c.tls.caFile, err = tc.FieldString(iFieldTLSCAFile); err != nil {
		return
	}
	if c.tls.serverName, err = tc.FieldString(iFieldTLSServerName); err != nil {
		return
	}
	c.tls.skipCertVerify, err = tc.FieldBool(iFieldTLSSkipVerify)
	return
}

//------------------------------------------------------------------------------

// partitionState is the delivery state of one assigned partition. It is
// guarded by iggyReader.mu and outlives connections: a partition assigned
// again after a reconnect carries on where it was.
type partitionState struct {
	id uint32
	// next is the offset the next poll starts at.
	next uint64
	// fromFirst polls with the "first" strategy until a message is seen, so a
	// partition whose head was removed by retention does not stall at 0.
	fromFirst bool
	// tracker orders the in-flight batches by their last offset and yields the
	// highest offset below which every batch is acknowledged.
	tracker *checkpoint.Uncapped[uint64]
	// acked is that offset, once there is one.
	acked    uint64
	hasAcked bool
	// committed is the offset the server holds for the group.
	committed    uint64
	hasCommitted bool
}

// commitDue reports the offset to store, if one is ahead of the stored one.
func (p *partitionState) commitDue() (uint64, bool) {
	if !p.hasAcked || (p.hasCommitted && p.acked <= p.committed) {
		return 0, false
	}
	return p.acked, true
}

type ackedBatch struct {
	batch service.MessageBatch
	ack   service.AckFunc
}

// connection is one live client with its polling and committing goroutines.
type connection struct {
	client  iggyClient
	batches chan ackedBatch
	// done is closed once the poll loop has exited and nothing more is sent.
	done chan struct{}
	stop context.CancelFunc
	// stopped is closed once both goroutines have exited.
	stopped chan struct{}
}

type iggyReader struct {
	conf readerConfig
	log  *service.Logger
	dial func(ctx context.Context) (iggyClient, error)

	streamID iggcon.Identifier
	topicID  iggcon.Identifier
	groupID  iggcon.Identifier
	consumer iggcon.Consumer

	connectMu sync.Mutex
	mu        sync.Mutex
	conn      *connection
	parts     map[uint32]*partitionState
	// inFlight counts delivered batches whose ack has not arrived yet.
	inFlight  int
	ackSignal chan struct{}
	// rejoin asks the poll loop to rejoin the group after a store was refused
	// for a lost membership.
	rejoin atomic.Bool

	shutSig *shutdown.Signaller
}

func newIggyReaderFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*iggyReader, error) {
	c, err := readerConfigFromParsed(conf)
	if err != nil {
		return nil, err
	}
	log := mgr.Logger()
	return newIggyReader(c, log, func(ctx context.Context) (iggyClient, error) {
		return dialSeeds(ctx, c.addresses, c.username, c.password, c.tls, log)
	})
}

func newIggyReader(c readerConfig, log *service.Logger, dial func(ctx context.Context) (iggyClient, error)) (*iggyReader, error) {
	r := &iggyReader{
		conf:      c,
		log:       log,
		dial:      dial,
		parts:     map[uint32]*partitionState{},
		ackSignal: make(chan struct{}, 1),
		shutSig:   shutdown.NewSignaller(),
	}
	var err error
	if r.streamID, err = iggcon.NewIdentifier(c.stream); err != nil {
		return nil, fmt.Errorf("stream: %w", err)
	}
	if r.topicID, err = iggcon.NewIdentifier(c.topic); err != nil {
		return nil, fmt.Errorf("topic: %w", err)
	}
	if r.groupID, err = iggcon.NewIdentifier(c.consumerGroup); err != nil {
		return nil, fmt.Errorf("consumer_group: %w", err)
	}
	r.consumer = iggcon.NewGroupConsumer(r.groupID)
	if c.exclusive {
		r.consumer = iggcon.NewSingleConsumer(r.groupID)
	}
	return r, nil
}

func reqCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, requestTimeout)
}

// Connect dials the cluster, makes sure the consumer group exists and joins
// it (unless exclusive), and starts polling and committing.
func (r *iggyReader) Connect(ctx context.Context) error {
	// connectMu serialises connects without holding mu, which acks take,
	// across the dial.
	r.connectMu.Lock()
	defer r.connectMu.Unlock()
	r.mu.Lock()
	connected := r.conn != nil
	r.mu.Unlock()
	if connected {
		return nil
	}
	if r.shutSig.IsSoftStopSignalled() {
		return service.ErrEndOfInput
	}

	cl, err := r.dial(ctx)
	if err != nil {
		return err
	}
	if !r.conf.exclusive {
		if err := r.ensureGroup(ctx, cl); err != nil {
			_ = cl.Close()
			return err
		}
		if err := r.join(ctx, cl); err != nil {
			_ = cl.Close()
			return err
		}
	}

	loopCtx, stop := r.shutSig.SoftStopCtx(context.Background())
	conn := &connection{
		client:  cl,
		batches: make(chan ackedBatch),
		done:    make(chan struct{}),
		stop:    stop,
		stopped: make(chan struct{}),
	}
	r.mu.Lock()
	r.conn = conn
	r.mu.Unlock()

	commitCtx, stopCommit := context.WithCancel(context.Background())
	commitDone := make(chan struct{})
	go func() {
		defer close(commitDone)
		r.commitLoop(commitCtx, cl)
	}()
	go func() {
		err := r.pollLoop(loopCtx, cl, conn.batches)
		close(conn.done)
		stopCommit()
		<-commitDone
		if err != nil && !r.shutSig.IsSoftStopSignalled() {
			r.log.Errorf("Closing the iggy connection after repeated failures, it will be re-established: %v", err)
			// Store what is acknowledged before the connection goes. What is
			// acknowledged later is stored by the next connection.
			fctx, cancel := reqCtx(context.Background())
			r.commit(fctx, cl)
			cancel()
			_ = cl.Close()
			r.mu.Lock()
			if r.conn == conn {
				r.conn = nil
			}
			r.mu.Unlock()
		}
		stop()
		close(conn.stopped)
	}()
	r.log.Infof("Consuming iggy stream %s topic %s as consumer %s (exclusive: %v)", r.conf.stream, r.conf.topic, r.conf.consumerGroup, r.conf.exclusive)
	return nil
}

func (r *iggyReader) ensureGroup(ctx context.Context, cl iggyClient) error {
	rctx, cancel := reqCtx(ctx)
	defer cancel()
	g, err := cl.GetConsumerGroup(rctx, r.streamID, r.topicID, r.groupID)
	if err == nil && g != nil {
		return nil
	}
	if err != nil && !isGroupNotFound(err) {
		return fmt.Errorf("get consumer group %s: %w", r.conf.consumerGroup, err)
	}
	if _, err := cl.CreateConsumerGroup(rctx, r.streamID, r.topicID, r.conf.consumerGroup); err != nil {
		// Another member may have created it meanwhile; joining settles it.
		r.log.Warnf("Could not create consumer group %s, joining it anyway: %v", r.conf.consumerGroup, err)
	}
	return nil
}

func (r *iggyReader) join(ctx context.Context, cl iggyClient) error {
	rctx, cancel := reqCtx(ctx)
	defer cancel()
	if err := cl.JoinConsumerGroup(rctx, r.streamID, r.topicID, r.groupID); err != nil {
		return fmt.Errorf("join consumer group %s: %w", r.conf.consumerGroup, err)
	}
	return nil
}

//------------------------------------------------------------------------------

// pollLoop polls the assigned partitions until ctx is cancelled (nil return)
// or requests keep failing (error return).
func (r *iggyReader) pollLoop(ctx context.Context, cl iggyClient, out chan<- ackedBatch) error {
	var (
		assigned  []uint32
		lastSync  time.Time
		needSync  = true
		needJoin  = false
		failures  = 0
		lastError error
		// Membership errors are expected once per reconnect or rebalance;
		// only a run of them counts as a failure.
		membershipErrs = 0
	)
	var fail func(err error) error
	membershipLost := func(err error) error {
		membershipErrs++
		if membershipErrs > 2 {
			return fail(err)
		}
		return nil
	}

	fail = func(err error) error {
		failures++
		lastError = err
		if failures >= maxConsecutiveFailures {
			return fmt.Errorf("%d consecutive failures, last: %w", failures, err)
		}
		backoff := min(time.Duration(1<<failures)*100*time.Millisecond, maxFailureBackoff)
		r.log.Warnf("Iggy request failed, retrying in %v: %v", backoff, err)
		// Any failure may have been a reconnect, which drops membership.
		needJoin, needSync = !r.conf.exclusive, true
		sleep(ctx, backoff)
		return nil
	}

	for ctx.Err() == nil {
		if r.rejoin.Swap(false) && !r.conf.exclusive {
			needJoin, needSync = true, true
		}
		if needJoin {
			if err := r.join(ctx, cl); err != nil {
				if ctx.Err() != nil {
					break
				}
				if ferr := fail(err); ferr != nil {
					return ferr
				}
				continue
			}
			needJoin = false
		}
		if needSync || time.Since(lastSync) >= assignmentSyncPeriod {
			parts, err := r.syncAssignment(ctx, cl)
			if err != nil {
				if ctx.Err() != nil {
					break
				}
				if isMembershipError(err) {
					r.log.Infof("Rejoining consumer group %s: %v", r.conf.consumerGroup, err)
					needJoin = true
					if ferr := membershipLost(err); ferr != nil {
						return ferr
					}
					continue
				}
				if ferr := fail(err); ferr != nil {
					return ferr
				}
				continue
			}
			assigned, lastSync, needSync = parts, time.Now(), false
		}

		delivered := false
		var pollErr error
		for _, p := range assigned {
			n, err := r.pollPartition(ctx, cl, p, out)
			if err != nil {
				pollErr = err
				break
			}
			if n > 0 {
				delivered = true
			}
		}
		if pollErr != nil {
			if ctx.Err() != nil {
				break
			}
			if isMembershipError(pollErr) {
				r.log.Infof("Consumer group %s assignment changed: %v", r.conf.consumerGroup, pollErr)
				needSync = true
				if ferr := membershipLost(pollErr); ferr != nil {
					return ferr
				}
				continue
			}
			if ferr := fail(pollErr); ferr != nil {
				return ferr
			}
			continue
		}
		if failures > 0 {
			r.log.Infof("Iggy requests succeeding again after %d failures (last: %v)", failures, lastError)
			failures = 0
		}
		membershipErrs = 0
		if !delivered {
			select {
			case <-ctx.Done():
			case <-time.After(r.conf.pollInterval):
			case <-r.ackSignal:
				// Capacity may have freed up for a paused partition.
			}
		}
	}
	return nil
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// assignedPartitions lists the partitions to consume: the group's assignment
// for this member, or every partition of the topic when exclusive.
func (r *iggyReader) assignedPartitions(ctx context.Context, cl iggyClient) ([]uint32, error) {
	rctx, cancel := reqCtx(ctx)
	defer cancel()
	var parts []uint32
	if r.conf.exclusive {
		td, err := cl.GetTopic(rctx, r.streamID, r.topicID)
		if err != nil {
			return nil, err
		}
		for _, p := range td.Partitions {
			parts = append(parts, p.Id)
		}
	} else {
		a, err := cl.SyncConsumerGroup(rctx, r.streamID, r.topicID, r.groupID)
		if err != nil {
			return nil, err
		}
		parts = slices.Clone(a.Partitions)
	}
	slices.Sort(parts)
	return parts, nil
}

// syncAssignment fetches the partitions to consume and reconciles the delivery
// state with them: revoked partitions store their acknowledged offset and are
// dropped, new ones are initialised from the stored offset or start_from.
func (r *iggyReader) syncAssignment(ctx context.Context, cl iggyClient) ([]uint32, error) {
	parts, err := r.assignedPartitions(ctx, cl)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	var revoked []*partitionState
	for id, st := range r.parts {
		if !slices.Contains(parts, id) {
			revoked = append(revoked, st)
		}
	}
	var added []uint32
	for _, id := range parts {
		if _, ok := r.parts[id]; !ok {
			added = append(added, id)
		}
	}
	r.mu.Unlock()

	// A revoked partition stays committable by this member until its
	// cooperative handoff completes (the commit completes it, or the server's
	// consumer_group.rebalancing_timeout), so what was acknowledged is stored
	// now. Acks that arrive after the drop are ignored: the new owner
	// redelivers those messages.
	for _, st := range revoked {
		cctx, cancel := reqCtx(ctx)
		r.commitPartition(cctx, cl, st)
		cancel()
		r.mu.Lock()
		delete(r.parts, st.id)
		r.mu.Unlock()
		r.log.Infof("Partition %d of %s/%s revoked (stored offset: %s)", st.id, r.conf.stream, r.conf.topic, committedString(st))
	}
	for _, id := range added {
		st, err := r.initPartition(ctx, cl, id)
		if err != nil {
			return nil, fmt.Errorf("partition %d: %w", id, err)
		}
		r.mu.Lock()
		r.parts[id] = st
		r.mu.Unlock()
		r.log.Infof("Partition %d of %s/%s assigned, starting at offset %d", id, r.conf.stream, r.conf.topic, st.next)
	}
	return parts, nil
}

func committedString(st *partitionState) string {
	if !st.hasCommitted {
		return "none"
	}
	return strconv.FormatUint(st.committed, 10)
}

func (r *iggyReader) initPartition(ctx context.Context, cl iggyClient, id uint32) (*partitionState, error) {
	st := &partitionState{id: id, tracker: checkpoint.NewUncapped[uint64]()}

	rctx, cancel := reqCtx(ctx)
	defer cancel()
	off, err := cl.GetConsumerOffset(rctx, r.consumer, r.streamID, r.topicID, &id)
	if err != nil {
		return nil, fmt.Errorf("get stored offset: %w", err)
	}
	if off != nil {
		// The stored offset is the last consumed message.
		st.next = off.StoredOffset + 1
		st.committed, st.hasCommitted = off.StoredOffset, true
		return st, nil
	}

	if r.conf.startFrom == startFromFirst {
		st.fromFirst = true
		return st, nil
	}

	td, err := cl.GetTopic(rctx, r.streamID, r.topicID)
	if err != nil {
		return nil, fmt.Errorf("get topic: %w", err)
	}
	for _, p := range td.Partitions {
		if p.Id != id || p.MessagesCount == 0 {
			continue
		}
		// Store the starting point, so a restart before the first commit does
		// not skip what arrived in between.
		if err := cl.StoreConsumerOffset(rctx, r.consumer, r.streamID, r.topicID, p.CurrentOffset, &id); err != nil {
			return nil, fmt.Errorf("store starting offset: %w", err)
		}
		st.next = p.CurrentOffset + 1
		st.committed, st.hasCommitted = p.CurrentOffset, true
	}
	return st, nil
}

// pollPartition polls one partition and hands the result downstream as one
// batch. It reports how many messages were delivered.
func (r *iggyReader) pollPartition(ctx context.Context, cl iggyClient, id uint32, out chan<- ackedBatch) (int, error) {
	r.mu.Lock()
	st, ok := r.parts[id]
	if !ok {
		r.mu.Unlock()
		return 0, nil
	}
	room := r.conf.checkpointLimit - st.tracker.Pending()
	next, fromFirst := st.next, st.fromFirst
	r.mu.Unlock()
	if room <= 0 {
		return 0, nil
	}
	count := uint32(min(int64(r.conf.batchCount), room))
	strategy := iggcon.OffsetPollingStrategy(next)
	if fromFirst {
		strategy = iggcon.FirstPollingStrategy()
	}

	rctx, cancel := reqCtx(ctx)
	polled, err := cl.PollMessages(rctx, r.streamID, r.topicID, r.consumer, strategy, count, false, &id)
	cancel()
	if err != nil {
		return 0, err
	}
	if polled.PartitionId == iggcon.ResyncRequiredPartition && len(polled.Messages) == 0 {
		// The server fences a poll by a non-owner with an empty batch on this
		// sentinel rather than an error: the assignment is stale, or the
		// membership was lost to a reconnect.
		return 0, ierror.ErrConsumerGroupPartitionNotOwned
	}
	msgs := polled.Messages
	if !fromFirst {
		// Never hand out anything before the cursor twice.
		for len(msgs) > 0 && msgs[0].Header.Offset < next {
			msgs = msgs[1:]
		}
	}
	if len(msgs) == 0 {
		return 0, nil
	}

	partStr := strconv.FormatUint(uint64(id), 10)
	batch := make(service.MessageBatch, len(msgs))
	for i := range msgs {
		batch[i] = toMessage(&msgs[i], r.conf.stream, r.conf.topic, partStr, r.log)
	}
	last := msgs[len(msgs)-1].Header.Offset

	r.mu.Lock()
	if r.parts[id] != st {
		// Revoked while polling: whoever owns it now delivers these.
		r.mu.Unlock()
		return 0, nil
	}
	release := st.tracker.Track(last, int64(len(msgs)))
	st.next, st.fromFirst = last+1, false
	r.inFlight++
	r.mu.Unlock()

	var ackOnce sync.Once
	ack := func(_ context.Context, res error) error {
		ackOnce.Do(func() { r.onAck(st, release, res) })
		return nil
	}

	select {
	case out <- ackedBatch{batch: batch, ack: ack}:
		return len(msgs), nil
	case <-ctx.Done():
		// Never delivered: it stays pending in the tracker, so nothing at or
		// beyond it is stored, and a restart polls it again.
		r.mu.Lock()
		r.inFlight--
		r.mu.Unlock()
		return 0, ctx.Err()
	}
}

func (r *iggyReader) onAck(st *partitionState, release func() *uint64, res error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inFlight--
	if res != nil {
		// Unreachable behind AutoRetryNacksBatched. Should it happen, the
		// batch stays unresolved so no offset at or beyond it is stored and it
		// is redelivered after a restart.
		r.log.Errorf("Iggy batch of partition %d rejected, its offsets will not be committed: %v", st.id, res)
	} else if h := release(); h != nil && r.parts[st.id] == st {
		st.acked, st.hasAcked = *h, true
	}
	select {
	case r.ackSignal <- struct{}{}:
	default:
	}
}

//------------------------------------------------------------------------------

func (r *iggyReader) commitLoop(ctx context.Context, cl iggyClient) {
	t := time.NewTicker(r.conf.commitPeriod)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			rctx, cancel := reqCtx(ctx)
			r.commit(rctx, cl)
			cancel()
		}
	}
}

// commit stores the acknowledged offset of every partition that has moved.
func (r *iggyReader) commit(ctx context.Context, cl iggyClient) {
	r.mu.Lock()
	parts := make([]*partitionState, 0, len(r.parts))
	for _, st := range r.parts {
		parts = append(parts, st)
	}
	r.mu.Unlock()
	for _, st := range parts {
		r.commitPartition(ctx, cl, st)
	}
}

func (r *iggyReader) commitPartition(ctx context.Context, cl iggyClient, st *partitionState) {
	r.mu.Lock()
	offset, due := st.commitDue()
	r.mu.Unlock()
	if !due {
		return
	}
	id := st.id
	if err := cl.StoreConsumerOffset(ctx, r.consumer, r.streamID, r.topicID, offset, &id); err != nil {
		if !r.conf.exclusive && isMembershipError(err) {
			// Every SDK reconnect, including the one after a cancelled
			// request, registers a new client identity that is not a member.
			r.rejoin.Store(true)
		}
		if ctx.Err() == nil {
			r.log.Warnf("Failed to store offset %d of partition %d, retrying next period: %v", offset, id, err)
		}
		return
	}
	r.mu.Lock()
	if !st.hasCommitted || offset > st.committed {
		st.committed, st.hasCommitted = offset, true
	}
	r.mu.Unlock()
}

//------------------------------------------------------------------------------

func (r *iggyReader) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	r.mu.Lock()
	conn := r.conn
	r.mu.Unlock()
	if conn == nil {
		return nil, nil, service.ErrNotConnected
	}
	select {
	case b := <-conn.batches:
		return b.batch, b.ack, nil
	case <-conn.done:
		if r.shutSig.IsSoftStopSignalled() {
			return nil, nil, service.ErrEndOfInput
		}
		return nil, nil, service.ErrNotConnected
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

// Close stops polling, waits (within ctx) for the delivered batches to be
// acknowledged, stores their offsets and leaves the consumer group.
func (r *iggyReader) Close(ctx context.Context) error {
	r.shutSig.TriggerSoftStop()
	r.mu.Lock()
	conn := r.conn
	r.mu.Unlock()
	if conn == nil {
		return nil
	}

	select {
	case <-conn.stopped:
	case <-ctx.Done():
	}
	r.waitForAcks(ctx)

	// Storing acknowledged offsets matters more than a prompt exit, so the
	// final requests get a short grace period if the caller's budget is spent.
	fctx, cancel := context.WithTimeout(context.Background(), finalRequestGrace)
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) > finalRequestGrace {
		cancel()
		fctx, cancel = context.WithDeadline(context.Background(), deadline)
	}
	defer cancel()
	r.commit(fctx, conn.client)
	if !r.conf.exclusive && r.rejoin.Load() {
		// The final stores were refused for a lost membership: rejoin once
		// and try again, rather than leave acknowledged offsets unstored.
		if err := r.join(fctx, conn.client); err == nil {
			r.commit(fctx, conn.client)
		}
	}
	if !r.conf.exclusive {
		if err := conn.client.LeaveConsumerGroup(fctx, r.streamID, r.topicID, r.groupID); err != nil {
			r.log.Debugf("Failed to leave consumer group %s: %v", r.conf.consumerGroup, err)
		}
	}
	err := conn.client.Close()

	r.mu.Lock()
	if r.conn == conn {
		r.conn = nil
	}
	r.mu.Unlock()
	r.shutSig.TriggerHasStopped()
	return err
}

func (r *iggyReader) waitForAcks(ctx context.Context) {
	for {
		r.mu.Lock()
		n := r.inFlight
		r.mu.Unlock()
		if n <= 0 {
			return
		}
		select {
		case <-ctx.Done():
			r.log.Warnf("Shutting down with %d iggy batches unacknowledged, they will be redelivered", n)
			return
		case <-r.ackSignal:
		case <-time.After(50 * time.Millisecond):
		}
	}
}
