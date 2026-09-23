package iggy

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

// fakeClient is an in-memory Iggy server for one topic and one consumer
// group: partitions hold messages at their offset index, the group stores
// offsets, and membership is lost the way a reconnect loses it.
type fakeClient struct {
	mu        sync.Mutex
	parts     map[uint32][]iggcon.IggyMessage
	stored    map[uint32]uint64
	stores    []storeCall
	member    bool
	hasGroup  bool
	joins     int
	leaves    int
	closed    bool
	assign    []uint32 // nil means every partition
	pollErrs  []error  // returned by the next polls, in order
	storeErrs []error  // returned by the next stores, in order
	polls     []pollCall
	// neverMember makes every sync report a lost membership, even right
	// after a successful join.
	neverMember bool
	// lenientPolls accepts group polls from non-members, as Iggy 0.9.0 does.
	lenientPolls bool
}

type storeCall struct {
	partition uint32
	offset    uint64
	kind      iggcon.ConsumerKind
}

type pollCall struct {
	partition uint32
	strategy  iggcon.PollingStrategy
	count     uint32
	auto      bool
	kind      iggcon.ConsumerKind
}

func newFakeClient(partitions int) *fakeClient {
	f := &fakeClient{parts: map[uint32][]iggcon.IggyMessage{}, stored: map[uint32]uint64{}}
	for p := range partitions {
		f.parts[uint32(p)] = nil
	}
	return f
}

func (f *fakeClient) produce(t testing.TB, partition uint32, n int) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for range n {
		off := uint64(len(f.parts[partition]))
		m, err := iggcon.NewIggyMessage([]byte(fmt.Sprintf("p%d-%d", partition, off)))
		require.NoError(t, err)
		m.Header.Offset = off
		m.Header.Timestamp = 1_700_000_000_000_000 + off
		f.parts[partition] = append(f.parts[partition], m)
	}
}

func (f *fakeClient) assignment() []uint32 {
	if f.assign != nil {
		return f.assign
	}
	var ids []uint32
	for id := range f.parts {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func (f *fakeClient) storeLog() []storeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.stores)
}

func (f *fakeClient) storedOffset(p uint32) (uint64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.stored[p]
	return o, ok
}

func (f *fakeClient) set(fn func(f *fakeClient)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

func (f *fakeClient) GetTopic(context.Context, iggcon.Identifier, iggcon.Identifier) (*iggcon.TopicDetails, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	td := &iggcon.TopicDetails{}
	for id, msgs := range f.parts {
		p := iggcon.PartitionContract{Id: id, MessagesCount: uint64(len(msgs))}
		if len(msgs) > 0 {
			p.CurrentOffset = uint64(len(msgs) - 1)
		}
		td.Partitions = append(td.Partitions, p)
	}
	return td, nil
}

func (f *fakeClient) GetConsumerGroup(context.Context, iggcon.Identifier, iggcon.Identifier, iggcon.Identifier) (*iggcon.ConsumerGroupDetails, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.hasGroup {
		return nil, ierror.ErrConsumerGroupIdNotFound
	}
	return &iggcon.ConsumerGroupDetails{}, nil
}

func (f *fakeClient) CreateConsumerGroup(context.Context, iggcon.Identifier, iggcon.Identifier, string) (*iggcon.ConsumerGroupDetails, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hasGroup = true
	return &iggcon.ConsumerGroupDetails{}, nil
}

func (f *fakeClient) JoinConsumerGroup(context.Context, iggcon.Identifier, iggcon.Identifier, iggcon.Identifier) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.hasGroup {
		return ierror.ErrConsumerGroupIdNotFound
	}
	f.member = true
	f.joins++
	return nil
}

func (f *fakeClient) LeaveConsumerGroup(context.Context, iggcon.Identifier, iggcon.Identifier, iggcon.Identifier) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.member = false
	f.leaves++
	return nil
}

func (f *fakeClient) SyncConsumerGroup(context.Context, iggcon.Identifier, iggcon.Identifier, iggcon.Identifier) (*iggcon.ConsumerGroupAssignment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.member || f.neverMember {
		return nil, ierror.ErrConsumerGroupMemberNotFound
	}
	return &iggcon.ConsumerGroupAssignment{Generation: uint64(f.joins), Partitions: slices.Clone(f.assignment())}, nil
}

func (f *fakeClient) GetConsumerOffset(_ context.Context, _ iggcon.Consumer, _, _ iggcon.Identifier, p *uint32) (*iggcon.ConsumerOffsetInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.stored[*p]
	if !ok {
		return nil, nil
	}
	return &iggcon.ConsumerOffsetInfo{PartitionId: *p, StoredOffset: o}, nil
}

func (f *fakeClient) StoreConsumerOffset(_ context.Context, c iggcon.Consumer, _, _ iggcon.Identifier, offset uint64, p *uint32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c.Kind == iggcon.ConsumerKindGroup && (!f.member || !slices.Contains(f.assignment(), *p)) {
		return ierror.ErrConsumerGroupPartitionNotOwned
	}
	if len(f.storeErrs) > 0 {
		err := f.storeErrs[0]
		f.storeErrs = f.storeErrs[1:]
		return err
	}
	f.stored[*p] = offset
	f.stores = append(f.stores, storeCall{*p, offset, c.Kind})
	return nil
}

func (f *fakeClient) PollMessages(_ context.Context, _, _ iggcon.Identifier, c iggcon.Consumer, s iggcon.PollingStrategy, count uint32, auto bool, p *uint32) (*iggcon.PolledMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if p == nil {
		return nil, errors.New("the input must always name the partition")
	}
	f.polls = append(f.polls, pollCall{*p, s, count, auto, c.Kind})
	if len(f.pollErrs) > 0 {
		err := f.pollErrs[0]
		f.pollErrs = f.pollErrs[1:]
		return nil, err
	}
	if c.Kind == iggcon.ConsumerKindGroup && !f.lenientPolls {
		if !f.member {
			return nil, ierror.ErrConsumerGroupMemberNotFound
		}
		if !slices.Contains(f.assignment(), *p) {
			return nil, ierror.ErrConsumerGroupPartitionNotOwned
		}
	}
	msgs := f.parts[*p]
	var start uint64
	switch s.Kind {
	case iggcon.POLLING_OFFSET:
		start = s.Value
	case iggcon.POLLING_FIRST:
		start = 0
	default:
		return nil, fmt.Errorf("unexpected strategy %v", s.Kind)
	}
	var out []iggcon.IggyMessage
	for off := start; off < uint64(len(msgs)) && uint32(len(out)) < count; off++ {
		out = append(out, msgs[off])
	}
	return &iggcon.PolledMessage{PartitionId: *p, Messages: out, MessageCount: uint32(len(out))}, nil
}

func (f *fakeClient) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

//------------------------------------------------------------------------------

func testConfig() readerConfig {
	return readerConfig{
		addresses:       []string{"fake:8090"},
		stream:          "raw",
		topic:           "trades.binance-futures",
		consumerGroup:   "archiver",
		startFrom:       startFromFirst,
		batchCount:      5,
		pollInterval:    5 * time.Millisecond,
		commitPeriod:    5 * time.Millisecond,
		checkpointLimit: 1000,
	}
}

func newTestReader(t testing.TB, conf readerConfig, clients ...*fakeClient) (*iggyReader, *int) {
	t.Helper()
	dials := 0
	var mu sync.Mutex
	r, err := newIggyReader(conf, service.MockResources().Logger(), func(context.Context) (iggyClient, error) {
		mu.Lock()
		defer mu.Unlock()
		c := clients[min(dials, len(clients)-1)]
		dials++
		return c, nil
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = r.Close(ctx)
	})
	return r, &dials
}

func readBatch(t testing.TB, in interface {
	ReadBatch(context.Context) (service.MessageBatch, service.AckFunc, error)
}) (service.MessageBatch, service.AckFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		b, ack, err := in.ReadBatch(ctx)
		if errors.Is(err, service.ErrNotConnected) {
			if c, ok := in.(interface{ Connect(context.Context) error }); ok {
				_ = c.Connect(ctx)
			}
			time.Sleep(5 * time.Millisecond)
			continue
		}
		require.NoError(t, err)
		return b, ack
	}
}

func assertNoBatch(t testing.TB, r *iggyReader, wait time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	b, _, err := r.ReadBatch(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded, "unexpected batch %v", offsets(t, b))
}

func offsets(t testing.TB, b service.MessageBatch) []uint64 {
	t.Helper()
	out := make([]uint64, 0, len(b))
	for _, m := range b {
		v, ok := m.MetaGet("iggy_offset")
		require.True(t, ok)
		o, err := strconv.ParseUint(v, 10, 64)
		require.NoError(t, err)
		out = append(out, o)
	}
	return out
}

func seq(from, to uint64) []uint64 {
	var out []uint64
	for i := from; i <= to; i++ {
		out = append(out, i)
	}
	return out
}

// settle gives the commit loop several periods to act on what it has.
func settle() { time.Sleep(50 * time.Millisecond) }

func ackOK(t testing.TB, ack service.AckFunc) {
	t.Helper()
	require.NoError(t, ack(context.Background(), nil))
}

//------------------------------------------------------------------------------

// The durability contract: an offset reaches the server only after the output
// has acknowledged the message, and polls never auto-commit.
func TestIggyInputCommitsOnlyAfterAck(t *testing.T) {
	fc := newFakeClient(1)
	fc.produce(t, 0, 5)
	r, _ := newTestReader(t, testConfig(), fc)
	require.NoError(t, r.Connect(context.Background()))

	b, ack := readBatch(t, r)
	assert.Equal(t, seq(0, 4), offsets(t, b))

	settle()
	assert.Empty(t, fc.storeLog(), "offset stored before the output acknowledged the batch")

	ackOK(t, ack)
	require.Eventually(t, func() bool { o, ok := fc.storedOffset(0); return ok && o == 4 }, time.Second, 5*time.Millisecond)

	fc.mu.Lock()
	defer fc.mu.Unlock()
	for _, p := range fc.polls {
		assert.False(t, p.auto, "a poll asked the server to auto-commit")
	}
}

// Batches of one partition are acknowledged out of order when several are in
// flight; storing a later batch's offset first would skip the earlier batch
// after a crash.
func TestIggyInputOutOfOrderAcksCommitContiguousOnly(t *testing.T) {
	fc := newFakeClient(1)
	fc.produce(t, 0, 15)
	r, _ := newTestReader(t, testConfig(), fc)
	require.NoError(t, r.Connect(context.Background()))

	b1, ack1 := readBatch(t, r)
	b2, ack2 := readBatch(t, r)
	b3, ack3 := readBatch(t, r)
	require.Equal(t, seq(0, 4), offsets(t, b1))
	require.Equal(t, seq(5, 9), offsets(t, b2))
	require.Equal(t, seq(10, 14), offsets(t, b3))

	ackOK(t, ack3)
	settle()
	assert.Empty(t, fc.storeLog(), "stored past unacknowledged batches")

	ackOK(t, ack1)
	require.Eventually(t, func() bool { o, ok := fc.storedOffset(0); return ok && o == 4 }, time.Second, 5*time.Millisecond)
	settle()
	o, _ := fc.storedOffset(0)
	assert.Equal(t, uint64(4), o, "stored past the unacknowledged middle batch")

	ackOK(t, ack2)
	require.Eventually(t, func() bool { o, _ := fc.storedOffset(0); return o == 14 }, time.Second, 5*time.Millisecond)
	for i, s := range fc.storeLog()[1:] {
		assert.Greater(t, s.offset, fc.storeLog()[i].offset, "stored offsets must only move forward")
	}
}

// A rejected batch comes back until it succeeds, and nothing at or beyond it
// is stored meanwhile, even when later batches succeed.
func TestIggyInputNackRedeliversAndHoldsCommit(t *testing.T) {
	fc := newFakeClient(1)
	fc.produce(t, 0, 10)
	r, _ := newTestReader(t, testConfig(), fc)
	in := service.AutoRetryNacksBatched(r)
	require.NoError(t, in.Connect(context.Background()))

	b1, ack1 := readBatch(t, in)
	b2, ack2 := readBatch(t, in)
	require.Equal(t, seq(0, 4), offsets(t, b1))
	require.Equal(t, seq(5, 9), offsets(t, b2))

	require.NoError(t, ack1(context.Background(), errors.New("s3 upload failed")))
	ackOK(t, ack2)
	settle()
	assert.Empty(t, fc.storeLog(), "stored an offset past a rejected batch")

	again, ackAgain := readBatch(t, in)
	assert.Equal(t, seq(0, 4), offsets(t, again), "the rejected batch was not redelivered")
	settle()
	assert.Empty(t, fc.storeLog())

	ackOK(t, ackAgain)
	require.Eventually(t, func() bool { o, _ := fc.storedOffset(0); return o == 9 }, time.Second, 5*time.Millisecond)
}

// Without the retry wrapper a rejection must still never become a commit: the
// partition's stored offset stays below the rejected batch for good.
func TestIggyInputRejectedAckNeverCommits(t *testing.T) {
	fc := newFakeClient(1)
	fc.produce(t, 0, 10)
	r, _ := newTestReader(t, testConfig(), fc)
	require.NoError(t, r.Connect(context.Background()))

	_, ack1 := readBatch(t, r)
	_, ack2 := readBatch(t, r)
	require.NoError(t, ack1(context.Background(), errors.New("rejected")))
	ackOK(t, ack2)
	settle()
	assert.Empty(t, fc.storeLog())
}

// A restarted consumer resumes after the stored offset, not at start_from.
func TestIggyInputResumesFromStoredOffset(t *testing.T) {
	fc := newFakeClient(1)
	fc.produce(t, 0, 10)
	fc.stored[0] = 6
	r, _ := newTestReader(t, testConfig(), fc)
	require.NoError(t, r.Connect(context.Background()))

	b, _ := readBatch(t, r)
	assert.Equal(t, seq(7, 9), offsets(t, b))
}

// start_from: next skips the backlog and pins the starting point at once, so a
// restart before the first commit neither replays the backlog nor skips what
// arrived in between.
func TestIggyInputStartFromNextStoresStartingPoint(t *testing.T) {
	fc := newFakeClient(1)
	fc.produce(t, 0, 10)
	conf := testConfig()
	conf.startFrom = startFromNext
	r, _ := newTestReader(t, conf, fc)
	require.NoError(t, r.Connect(context.Background()))

	require.Eventually(t, func() bool { o, ok := fc.storedOffset(0); return ok && o == 9 }, time.Second, 5*time.Millisecond)
	assertNoBatch(t, r, 50*time.Millisecond)

	fc.produce(t, 0, 3)
	b, _ := readBatch(t, r)
	assert.Equal(t, seq(10, 12), offsets(t, b))
}

// A reconnect inside the SDK registers a new client identity, which silently
// drops group membership. The input must rejoin and carry on exactly where it
// was: no gap, no replay of what is already in flight, and acks from before
// the reconnect still get committed.
func TestIggyInputRejoinsAfterMembershipLoss(t *testing.T) {
	for name, pollErrs := range map[string][]error{
		"membership lost silently":   nil,
		"disconnect then lost group": {ierror.ErrDisconnected},
	} {
		t.Run(name, func(t *testing.T) { testRejoin(t, pollErrs) })
	}
}

func testRejoin(t *testing.T, pollErrs []error) {
	fc := newFakeClient(1)
	fc.produce(t, 0, 5)
	r, _ := newTestReader(t, testConfig(), fc)
	require.NoError(t, r.Connect(context.Background()))

	b1, ack1 := readBatch(t, r)
	require.Equal(t, seq(0, 4), offsets(t, b1))

	fc.set(func(f *fakeClient) {
		f.member = false
		f.pollErrs = pollErrs
	})
	fc.produce(t, 0, 5)

	b2, ack2 := readBatch(t, r)
	assert.Equal(t, seq(5, 9), offsets(t, b2), "reconnect replayed or skipped messages")
	fc.mu.Lock()
	joins := fc.joins
	fc.mu.Unlock()
	assert.GreaterOrEqual(t, joins, 2, "the consumer group was not rejoined")

	ackOK(t, ack1)
	ackOK(t, ack2)
	require.Eventually(t, func() bool { o, _ := fc.storedOffset(0); return o == 9 }, time.Second, 5*time.Millisecond)
}

// When requests keep failing the connection is torn down and dialled afresh.
// Delivery state survives: the new connection continues after what was
// delivered, and commits what was acknowledged meanwhile.
func TestIggyInputReconnectsAfterRepeatedFailures(t *testing.T) {
	first := newFakeClient(1)
	first.produce(t, 0, 5)
	first.hasGroup = true
	r, dials := newTestReader(t, testConfig(), first)
	require.NoError(t, r.Connect(context.Background()))

	b1, ack1 := readBatch(t, r)
	require.Equal(t, seq(0, 4), offsets(t, b1))

	// Every request on the first connection fails from now on.
	errs := make([]error, 100)
	for i := range errs {
		errs[i] = ierror.ErrCannotEstablishConnection
	}
	first.set(func(f *fakeClient) { f.pollErrs = errs })

	require.Eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.conn == nil
	}, 10*time.Second, 10*time.Millisecond, "connection was not torn down")

	// The cluster is back: same data, the old connection's errors are gone.
	first.set(func(f *fakeClient) {
		f.pollErrs = nil
		f.member = false
	})
	first.produce(t, 0, 5)
	ackOK(t, ack1)

	b2, ack2 := readBatch(t, r)
	assert.Equal(t, seq(5, 9), offsets(t, b2))
	assert.Equal(t, 2, *dials)
	ackOK(t, ack2)
	require.Eventually(t, func() bool { o, _ := first.storedOffset(0); return o == 9 }, time.Second, 5*time.Millisecond)
}

// The server accepts polls from non-members, so a membership lost to the SDK
// moving to another node surfaces only as refused stores. Those must trigger
// a rejoin, or no offset would ever be stored again.
func TestIggyInputRejoinsWhenStoresAreRefused(t *testing.T) {
	fc := newFakeClient(1)
	fc.lenientPolls = true
	fc.produce(t, 0, 5)
	conf := testConfig()
	r, _ := newTestReader(t, conf, fc)
	require.NoError(t, r.Connect(context.Background()))

	_, ack := readBatch(t, r)
	fc.set(func(f *fakeClient) { f.member = false })
	ackOK(t, ack)
	require.Eventually(t, func() bool { o, ok := fc.storedOffset(0); return ok && o == 4 }, 2*time.Second, 5*time.Millisecond,
		"offsets were never stored after the membership was lost")
	fc.mu.Lock()
	defer fc.mu.Unlock()
	assert.GreaterOrEqual(t, fc.joins, 2)
}

// The same at shutdown: the final store rejoins once rather than dropping the
// acknowledged offsets.
func TestIggyInputCloseRejoinsForFinalCommit(t *testing.T) {
	fc := newFakeClient(1)
	fc.lenientPolls = true
	fc.produce(t, 0, 5)
	conf := testConfig()
	conf.commitPeriod = time.Hour
	r, _ := newTestReader(t, conf, fc)
	require.NoError(t, r.Connect(context.Background()))

	_, ack := readBatch(t, r)
	fc.set(func(f *fakeClient) { f.member = false })
	ackOK(t, ack)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, r.Close(ctx))
	o, ok := fc.storedOffset(0)
	assert.True(t, ok)
	assert.Equal(t, uint64(4), o)
}

// A failed store is retried on the next period rather than lost.
func TestIggyInputRetriesFailedCommit(t *testing.T) {
	fc := newFakeClient(1)
	fc.produce(t, 0, 5)
	fc.storeErrs = []error{ierror.ErrDisconnected, ierror.ErrTransientNotCommitted}
	r, _ := newTestReader(t, testConfig(), fc)
	require.NoError(t, r.Connect(context.Background()))

	_, ack := readBatch(t, r)
	ackOK(t, ack)
	require.Eventually(t, func() bool { o, ok := fc.storedOffset(0); return ok && o == 4 }, time.Second, 5*time.Millisecond)
}

// Shutdown waits for delivered batches to be acknowledged, stores their
// offsets and leaves the group.
func TestIggyInputCloseWaitsForInFlight(t *testing.T) {
	fc := newFakeClient(1)
	fc.produce(t, 0, 5)
	conf := testConfig()
	conf.commitPeriod = time.Hour // only the shutdown commit can store
	r, _ := newTestReader(t, conf, fc)
	require.NoError(t, r.Connect(context.Background()))
	_, ack := readBatch(t, r)

	closed := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		closed <- r.Close(ctx)
	}()

	select {
	case <-closed:
		t.Fatal("Close returned with a batch still in flight")
	case <-time.After(100 * time.Millisecond):
	}
	ackOK(t, ack)
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after the ack")
	}

	o, ok := fc.storedOffset(0)
	assert.True(t, ok)
	assert.Equal(t, uint64(4), o)
	fc.mu.Lock()
	defer fc.mu.Unlock()
	assert.Equal(t, 1, fc.leaves)
	assert.True(t, fc.closed)
}

// A shutdown that times out stores only what was acknowledged.
func TestIggyInputCloseTimeoutKeepsUnackedUncommitted(t *testing.T) {
	fc := newFakeClient(1)
	fc.produce(t, 0, 10)
	conf := testConfig()
	conf.commitPeriod = time.Hour
	r, _ := newTestReader(t, conf, fc)
	require.NoError(t, r.Connect(context.Background()))
	_, ack1 := readBatch(t, r)
	_, _ = readBatch(t, r)
	ackOK(t, ack1)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.NoError(t, r.Close(ctx))

	o, ok := fc.storedOffset(0)
	assert.True(t, ok)
	assert.Equal(t, uint64(4), o)
}

// checkpoint_limit bounds what one partition has in flight; the partition
// resumes as soon as acks free capacity.
func TestIggyInputCheckpointLimitBoundsInFlight(t *testing.T) {
	fc := newFakeClient(1)
	fc.produce(t, 0, 20)
	conf := testConfig()
	conf.checkpointLimit = 7
	r, _ := newTestReader(t, conf, fc)
	require.NoError(t, r.Connect(context.Background()))

	b1, ack1 := readBatch(t, r)
	b2, _ := readBatch(t, r)
	assert.Equal(t, seq(0, 4), offsets(t, b1))
	assert.Equal(t, seq(5, 6), offsets(t, b2), "second poll must only fill the remaining capacity")
	assertNoBatch(t, r, 50*time.Millisecond)

	ackOK(t, ack1)
	b3, _ := readBatch(t, r)
	assert.Equal(t, seq(7, 11), offsets(t, b3))
}

// Every assigned partition is consumed and committed independently.
func TestIggyInputMultiplePartitions(t *testing.T) {
	fc := newFakeClient(3)
	for p := range uint32(3) {
		fc.produce(t, p, 7)
	}
	r, _ := newTestReader(t, testConfig(), fc)
	require.NoError(t, r.Connect(context.Background()))

	seen := map[string][]uint64{}
	var acks []service.AckFunc
	for total := 0; total < 21; {
		b, ack := readBatch(t, r)
		p, _ := b[0].MetaGet("iggy_partition_id")
		seen[p] = append(seen[p], offsets(t, b)...)
		acks = append(acks, ack)
		total += len(b)
	}
	for p := range 3 {
		assert.Equal(t, seq(0, 6), seen[strconv.Itoa(p)], "partition %d", p)
	}
	for _, ack := range acks {
		ackOK(t, ack)
	}
	require.Eventually(t, func() bool {
		for p := range uint32(3) {
			if o, ok := fc.storedOffset(p); !ok || o != 6 {
				return false
			}
		}
		return true
	}, time.Second, 5*time.Millisecond)
}

// When a rebalance moves a partition away, the input stops polling it and
// never stores an offset for it again (the server would refuse, and a late
// store could move the new owner's position), while the partitions it keeps
// carry on.
func TestIggyInputRevokedPartitionStopsAndIsNotCommitted(t *testing.T) {
	fc := newFakeClient(2)
	fc.produce(t, 0, 10)
	fc.produce(t, 1, 5)
	r, _ := newTestReader(t, testConfig(), fc)
	require.NoError(t, r.Connect(context.Background()))

	var p0Acks []service.AckFunc
	for len(p0Acks) < 2 {
		b, ack := readBatch(t, r)
		if p, _ := b[0].MetaGet("iggy_partition_id"); p == "0" {
			p0Acks = append(p0Acks, ack)
		} else {
			ackOK(t, ack)
		}
	}
	fc.set(func(f *fakeClient) { f.assign = []uint32{1} })
	require.Eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		_, ok := r.parts[0]
		return !ok
	}, 10*time.Second, 10*time.Millisecond, "partition 0 was not dropped")

	for _, ack := range p0Acks {
		ackOK(t, ack)
	}
	fc.produce(t, 1, 5)
	b, ack := readBatch(t, r)
	p, _ := b[0].MetaGet("iggy_partition_id")
	assert.Equal(t, "1", p)
	assert.Equal(t, seq(5, 9), offsets(t, b))
	ackOK(t, ack)
	require.Eventually(t, func() bool { o, _ := fc.storedOffset(1); return o == 9 }, time.Second, 5*time.Millisecond)

	_, stored := fc.storedOffset(0)
	assert.False(t, stored, "stored an offset for a revoked partition")
	fc.mu.Lock()
	defer fc.mu.Unlock()
	for _, s := range fc.stores {
		assert.NotEqual(t, uint32(0), s.partition)
	}
}

// Exclusive mode consumes every partition as a single named consumer and
// never touches server-side group membership, so a stale member left behind
// by a leader failover cannot take partitions away from it.
func TestIggyInputExclusiveConsumesAllPartitionsWithoutGroup(t *testing.T) {
	fc := newFakeClient(2)
	fc.produce(t, 0, 3)
	fc.produce(t, 1, 3)
	fc.assign = []uint32{} // a group would assign this member nothing
	conf := testConfig()
	conf.exclusive = true
	r, _ := newTestReader(t, conf, fc)
	require.NoError(t, r.Connect(context.Background()))

	seen := map[string][]uint64{}
	for len(seen["0"])+len(seen["1"]) < 6 {
		b, ack := readBatch(t, r)
		p, _ := b[0].MetaGet("iggy_partition_id")
		seen[p] = append(seen[p], offsets(t, b)...)
		ackOK(t, ack)
	}
	assert.Equal(t, seq(0, 2), seen["0"])
	assert.Equal(t, seq(0, 2), seen["1"])
	require.Eventually(t, func() bool {
		o0, ok0 := fc.storedOffset(0)
		o1, ok1 := fc.storedOffset(1)
		return ok0 && ok1 && o0 == 2 && o1 == 2
	}, time.Second, 5*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, r.Close(ctx))
	fc.mu.Lock()
	defer fc.mu.Unlock()
	assert.Zero(t, fc.joins)
	assert.Zero(t, fc.leaves)
	assert.False(t, fc.hasGroup)
	for _, s := range fc.stores {
		assert.Equal(t, iggcon.ConsumerKindSingle, s.kind)
	}
	for _, p := range fc.polls {
		assert.Equal(t, iggcon.ConsumerKindSingle, p.kind)
	}
}

// A membership that never sticks must not turn into a tight join loop
// against the server: repeated rejoins back off and count as failures.
func TestIggyInputPersistentMembershipLossBacksOff(t *testing.T) {
	fc := newFakeClient(1)
	fc.neverMember = true
	r, _ := newTestReader(t, testConfig(), fc)
	require.NoError(t, r.Connect(context.Background()))
	time.Sleep(time.Second)
	fc.mu.Lock()
	joins := fc.joins
	fc.mu.Unlock()
	assert.Less(t, joins, 20, "rejoined %d times in a second", joins)
	assert.Greater(t, joins, 1)
}

func TestIggyInputCreatesMissingGroup(t *testing.T) {
	fc := newFakeClient(1)
	r, _ := newTestReader(t, testConfig(), fc)
	require.NoError(t, r.Connect(context.Background()))
	fc.mu.Lock()
	defer fc.mu.Unlock()
	assert.True(t, fc.hasGroup)
	assert.True(t, fc.member)
}

//------------------------------------------------------------------------------

func TestIggyMessageMetadata(t *testing.T) {
	le := func(n int, v uint64) []byte {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, v)
		return b[:n]
	}
	i128 := make([]byte, 16)
	for i := range i128 {
		i128[i] = 0xff // -1
	}
	u128 := make([]byte, 16)
	u128[8] = 1 // 2^64
	key := func(s string) iggcon.HeaderKey {
		k, err := iggcon.NewHeaderKeyString(s)
		require.NoError(t, err)
		return k
	}
	headers := []iggcon.HeaderEntry{
		{Key: key("dedup-key"), Value: iggcon.HeaderValue{Kind: iggcon.Raw, Value: []byte{0x00, 0xff, 0x10}}},
		{Key: key("s"), Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte("hello")}},
		{Key: key("b"), Value: iggcon.HeaderValue{Kind: iggcon.Bool, Value: []byte{1}}},
		{Key: key("i8"), Value: iggcon.HeaderValue{Kind: iggcon.Int8, Value: []byte{0xfe}}},
		{Key: key("i32"), Value: iggcon.HeaderValue{Kind: iggcon.Int32, Value: le(4, uint64(uint32(0xfffffff6)))}},
		{Key: key("u64"), Value: iggcon.HeaderValue{Kind: iggcon.Uint64, Value: le(8, math.MaxUint64)}},
		{Key: key("i128"), Value: iggcon.HeaderValue{Kind: iggcon.Int128, Value: i128}},
		{Key: key("u128"), Value: iggcon.HeaderValue{Kind: iggcon.Uint128, Value: u128}},
		{Key: key("f64"), Value: iggcon.HeaderValue{Kind: iggcon.Double, Value: le(8, math.Float64bits(1.5))}},
		{Key: iggcon.NewHeaderKeyInt32(7), Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte("int key")}},
	}
	var id iggcon.MessageID
	id[0], id[15] = 0x01, 0xab // little-endian u128: low byte first
	m, err := iggcon.NewIggyMessage([]byte("payload"), iggcon.WithUserHeaders(headers), iggcon.WithID(id))
	require.NoError(t, err)
	m.Header.Offset = 42
	m.Header.Timestamp = 1_790_199_160_037_847

	msg := toMessage(&m, "raw", "trades.binance-futures", "3", service.MockResources().Logger())
	b, err := msg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "payload", string(b))

	want := map[string]string{
		"iggy_stream":       "raw",
		"iggy_topic":        "trades.binance-futures",
		"iggy_partition_id": "3",
		"iggy_offset":       "42",
		"iggy_timestamp":    "1790199160037847",
		"iggy_message_id":   "ab000000000000000000000000000001",
		"dedup-key":         string([]byte{0x00, 0xff, 0x10}),
		"s":                 "hello",
		"b":                 "true",
		"i8":                "-2",
		"i32":               "-10",
		"u64":               "18446744073709551615",
		"i128":              "-1",
		"u128":              "18446744073709551616",
		"f64":               "1.5",
		"7":                 "int key",
	}
	got := map[string]string{}
	require.NoError(t, msg.MetaWalk(func(k, v string) error { got[k] = v; return nil }))
	assert.Equal(t, want, got)
}

func TestIggyMessageWithCorruptHeadersIsStillDelivered(t *testing.T) {
	m, err := iggcon.NewIggyMessage([]byte("payload"))
	require.NoError(t, err)
	m.UserHeaders = []byte{2, 0xff, 0xff, 0xff, 0xff}
	msg := toMessage(&m, "raw", "t", "0", service.MockResources().Logger())
	b, err := msg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "payload", string(b))
	_, ok := msg.MetaGet("iggy_offset")
	assert.True(t, ok)
}

//------------------------------------------------------------------------------

func TestIggyInputConfigParse(t *testing.T) {
	conf, err := iggyInputSpec().ParseYAML(`
addresses: [ a:8090, b:8090 ]
username: archiver
password: secret
stream: raw
topic: trades.binance-futures
consumer_group: raw-archiver
`, nil)
	require.NoError(t, err)
	c, err := readerConfigFromParsed(conf)
	require.NoError(t, err)
	assert.Equal(t, []string{"a:8090", "b:8090"}, c.addresses)
	assert.Equal(t, startFromFirst, c.startFrom)
	assert.Equal(t, uint32(1000), c.batchCount)
	assert.Equal(t, int64(1024), c.checkpointLimit)
	assert.Equal(t, time.Second, c.commitPeriod)
	assert.Equal(t, 100*time.Millisecond, c.pollInterval)
	assert.False(t, c.tls.enabled)
	assert.False(t, c.exclusive)

	for name, yaml := range map[string]string{
		"no group":         `{addresses: [a], username: u, password: p, stream: s, topic: t, consumer_group: ""}`,
		"no addresses":     `{addresses: [], username: u, password: p, stream: s, topic: t, consumer_group: g}`,
		"zero limit":       `{addresses: [a], username: u, password: p, stream: s, topic: t, consumer_group: g, checkpoint_limit: 0}`,
		"zero batch count": `{addresses: [a], username: u, password: p, stream: s, topic: t, consumer_group: g, batch_count: 0}`,
	} {
		conf, err := iggyInputSpec().ParseYAML(yaml, nil)
		require.NoError(t, err, name)
		_, err = readerConfigFromParsed(conf)
		assert.Error(t, err, name)
	}
}
