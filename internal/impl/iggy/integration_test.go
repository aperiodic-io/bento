package iggy

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	iggyclient "github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"

	_ "github.com/warpstreamlabs/bento/public/components/pure"
)

// The integration tests run against a real Iggy server or cluster, which they
// do not start themselves: Iggy needs io_uring (seccomp=unconfined) and a
// cluster needs fixed node addresses. testdata/cluster.sh starts a 3-node
// cluster, after which:
//
//	IGGY_INTEGRATION_ADDRESSES=172.29.0.10:8090,172.29.0.11:8090,172.29.0.12:8090 \
//	IGGY_INTEGRATION_CONTAINERS=iggy-0,iggy-1,iggy-2 \
//	go test -run '^TestIntegrationIggy' -v ./internal/impl/iggy/
//
// IGGY_INTEGRATION_CONTAINERS (docker container names, in address order) is
// only needed by the leader failover test, which kills and restarts nodes.
// A single node (IGGY_INTEGRATION_ADDRESSES=<ip>:8090) runs everything else.

type itEnv struct {
	addresses  []string
	containers []string
	username   string
	password   string
}

func integrationEnv(t *testing.T) itEnv {
	t.Helper()
	integration.CheckSkip(t)
	addrs := os.Getenv("IGGY_INTEGRATION_ADDRESSES")
	if addrs == "" {
		t.Skip("IGGY_INTEGRATION_ADDRESSES is not set")
	}
	env := itEnv{addresses: strings.Split(addrs, ","), username: "iggy", password: "iggy"}
	if c := os.Getenv("IGGY_INTEGRATION_CONTAINERS"); c != "" {
		env.containers = strings.Split(c, ",")
	}
	if u := os.Getenv("IGGY_INTEGRATION_USERNAME"); u != "" {
		env.username = u
	}
	if p := os.Getenv("IGGY_INTEGRATION_PASSWORD"); p != "" {
		env.password = p
	}
	return env
}

func (e itEnv) client(t *testing.T) iggcon.Client {
	t.Helper()
	var lastErr error
	for _, a := range e.addresses {
		c, err := iggyclient.NewIggyClient(iggyclient.WithTcp(
			tcp.WithServerAddress(a),
			tcp.WithAutoLogin(tcp.NewUsernamePasswordCredentials(e.username, e.password)),
		))
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		lastErr = c.Connect(ctx)
		cancel()
		if lastErr == nil {
			t.Cleanup(func() { _ = c.Close() })
			return c
		}
		_ = c.Close()
	}
	require.NoError(t, lastErr)
	return nil
}

func ident(t *testing.T, s string) iggcon.Identifier {
	t.Helper()
	id, err := iggcon.NewIdentifier(s)
	require.NoError(t, err)
	return id
}

// createTopic creates a uniquely named topic in stream "raw" the way the raw
// broker is provisioned (replicated durability, small segments).
func createTopic(t *testing.T, c iggcon.Client, partitions uint32) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := c.GetStream(ctx, ident(t, "raw")); err != nil {
		_, err = c.CreateStream(ctx, "raw")
		require.NoError(t, err)
	}
	name := fmt.Sprintf("it-%s.%d", strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-")), time.Now().UnixNano())
	_, err := c.CreateTopic(ctx, ident(t, "raw"), name, partitions, iggcon.CompressionAlgorithmNone,
		iggcon.IggyExpiryNeverExpire, 64<<20,
		iggcon.DurabilityOption(iggcon.DurabilityReplicated), iggcon.SegmentSizeOption(8<<20))
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = c.DeleteTopic(ctx, ident(t, "raw"), ident(t, name))
	})
	return name
}

var symbols = []string{"BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT", "BNBUSDT"}

// produce sends messages [from, from+n) partitioned by symbol, with the event
// number as payload and as an 8-byte raw dedup-key header, like the raw
// producers. It returns the event numbers whose send was confirmed; a send
// that failed may or may not have been appended.
func produce(t *testing.T, c iggcon.Client, topic string, from, n int) []int {
	t.Helper()
	var confirmed []int
	const perBatch = 100
	for b := from; b < from+n; b += perBatch {
		sym := symbols[(b/perBatch)%len(symbols)]
		var msgs []iggcon.IggyMessage
		var ids []int
		for i := b; i < min(b+perBatch, from+n); i++ {
			key, err := iggcon.NewHeaderKeyString("dedup-key")
			require.NoError(t, err)
			dk := make([]byte, 8)
			binary.BigEndian.PutUint64(dk, uint64(i))
			m, err := iggcon.NewIggyMessage([]byte("event-"+strconv.Itoa(i)), iggcon.WithUserHeaders([]iggcon.HeaderEntry{
				{Key: key, Value: iggcon.HeaderValue{Kind: iggcon.Raw, Value: dk}},
			}))
			require.NoError(t, err)
			msgs = append(msgs, m)
			ids = append(ids, i)
		}
		part, err := iggcon.EntityIdString(sym)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, err = c.SendMessages(ctx, ident(t, "raw"), ident(t, topic), part, msgs)
		cancel()
		if err != nil {
			t.Logf("send of events %d..%d failed: %v", ids[0], ids[len(ids)-1], err)
			continue
		}
		confirmed = append(confirmed, ids...)
	}
	return confirmed
}

func eventNumber(t *testing.T, m *service.Message) int {
	t.Helper()
	b, err := m.AsBytes()
	require.NoError(t, err)
	n, err := strconv.Atoi(strings.TrimPrefix(string(b), "event-"))
	require.NoError(t, err)
	dk, ok := m.MetaGet("dedup-key")
	require.True(t, ok, "dedup-key header missing")
	require.Equal(t, uint64(n), binary.BigEndian.Uint64([]byte(dk)), "dedup-key does not match its message")
	return n
}

func (e itEnv) inputConfig(t *testing.T, topic, group string, extra string) string {
	t.Helper()
	return fmt.Sprintf(`
addresses: [ %s ]
username: %s
password: %s
stream: raw
topic: %s
consumer_group: %s
checkpoint_limit: 5000
commit_period: 200ms
%s`, strings.Join(e.addresses, ", "), e.username, e.password, topic, group, extra)
}

// newInput builds the input exactly as the registered component does.
func (e itEnv) newInput(t *testing.T, topic, group, extra string) (*iggyReader, service.BatchInput) {
	t.Helper()
	conf, err := iggyInputSpec().ParseYAML(e.inputConfig(t, topic, group, extra), nil)
	require.NoError(t, err)
	r, err := newIggyReaderFromConfig(conf, service.MockResources(service.MockResourcesOptUseSlogger(
		slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))))
	require.NoError(t, err)
	return r, service.AutoRetryNacksBatched(r)
}

func readIntegration(ctx context.Context, in service.BatchInput) (service.MessageBatch, service.AckFunc, error) {
	for {
		b, ack, err := in.ReadBatch(ctx)
		if err == nil || ctx.Err() != nil {
			return b, ack, err
		}
		if err == service.ErrNotConnected {
			if cerr := in.Connect(ctx); cerr != nil {
				time.Sleep(200 * time.Millisecond)
			}
			continue
		}
		return nil, nil, err
	}
}

// storedOffsets reads the offsets stored for a consumer group, or for a
// single consumer of that name when exclusive.
func storedOffsets(t *testing.T, c iggcon.Client, topic, group string, partitions uint32, exclusive bool) map[uint32]int64 {
	t.Helper()
	consumer := iggcon.NewGroupConsumer(ident(t, group))
	if exclusive {
		consumer = iggcon.NewSingleConsumer(ident(t, group))
	}
	out := map[uint32]int64{}
	for p := range partitions {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		off, err := c.GetConsumerOffset(ctx, consumer, ident(t, "raw"), ident(t, topic), &p)
		cancel()
		require.NoError(t, err)
		out[p] = -1
		if off != nil {
			out[p] = int64(off.StoredOffset)
		}
	}
	return out
}

func partitionEnds(t *testing.T, c iggcon.Client, topic string) map[uint32]int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	td, err := c.GetTopic(ctx, ident(t, "raw"), ident(t, topic))
	require.NoError(t, err)
	out := map[uint32]int64{}
	for _, p := range td.Partitions {
		out[p.Id] = -1
		if p.MessagesCount > 0 {
			out[p.Id] = int64(p.CurrentOffset)
		}
	}
	return out
}

type partOffset struct {
	partition string
	offset    string
}

//------------------------------------------------------------------------------

// Every message of every partition is delivered exactly once to a single
// uninterrupted consumer, through a real Bento stream, and once the stream
// shuts down the group's stored offsets sit at the end of each partition.
func TestIntegrationIggyStreamConsumesEverything(t *testing.T) {
	env := integrationEnv(t)
	admin := env.client(t)
	const partitions, total = 3, 30_000
	topic := createTopic(t, admin, partitions)
	confirmed := produce(t, admin, topic, 0, total)
	require.Len(t, confirmed, total)

	sb := service.NewStreamBuilder()
	require.NoError(t, sb.AddInputYAML("iggy:\n"+indent(env.inputConfig(t, topic, "it-stream", ""))))
	var mu sync.Mutex
	perOffset := map[partOffset]int{}
	perEvent := map[int]int{}
	require.NoError(t, sb.AddBatchConsumerFunc(func(_ context.Context, b service.MessageBatch) error {
		mu.Lock()
		defer mu.Unlock()
		for _, m := range b {
			p, _ := m.MetaGet("iggy_partition_id")
			o, _ := m.MetaGet("iggy_offset")
			perOffset[partOffset{p, o}]++
			perEvent[eventNumber(t, m)]++
		}
		return nil
	}))
	require.NoError(t, sb.SetLoggerYAML("level: warn"))
	stream, err := sb.Build()
	require.NoError(t, err)

	start := time.Now()
	runErr := make(chan error, 1)
	go func() { runErr <- stream.Run(context.Background()) }()
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(perEvent) == total
	}, 2*time.Minute, 50*time.Millisecond)
	elapsed := time.Since(start)
	stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, stream.Stop(stopCtx))
	require.NoError(t, <-runErr)

	mu.Lock()
	defer mu.Unlock()
	for k, n := range perOffset {
		require.Equal(t, 1, n, "partition %s offset %s delivered %d times", k.partition, k.offset, n)
	}
	for _, i := range confirmed {
		require.Equal(t, 1, perEvent[i], "event %d", i)
	}
	assert.Equal(t, partitionEnds(t, admin, topic), storedOffsets(t, admin, topic, "it-stream", partitions, false),
		"stored offsets are not at the partition ends after a clean shutdown")
	t.Logf("consumed %d messages from %d partitions exactly once in %v (%.0f msg/s)", total, partitions, elapsed, float64(total)/elapsed.Seconds())
}

func indent(s string) string {
	return "  " + strings.ReplaceAll(strings.TrimSpace(s), "\n", "\n  ")
}

// A consumer that dies with batches delivered but not acknowledged (some
// acknowledged out of order) has stored exactly the highest contiguously
// acknowledged offset per partition; a new consumer in the group resumes right
// after it, so nothing is lost and only unacknowledged messages repeat.
func TestIntegrationIggyCrashRedeliversUnacked(t *testing.T) {
	env := integrationEnv(t)
	admin := env.client(t)
	const partitions, total = 3, 9_000
	topic := createTopic(t, admin, partitions)
	require.Len(t, produce(t, admin, topic, 0, total), total)

	r1, in1 := env.newInput(t, topic, "it-crash", "batch_count: 100")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	require.NoError(t, in1.Connect(ctx))

	type delivered struct {
		batch service.MessageBatch
		ack   service.AckFunc
	}
	var got []delivered
	for len(got) < 30 {
		b, ack, err := readIntegration(ctx, in1)
		require.NoError(t, err)
		got = append(got, delivered{b, ack})
	}
	// Per partition, acknowledge the first batches in reverse order and leave
	// a hole after them, then acknowledge one batch beyond the hole: the
	// stored offset must stop at the hole.
	byPartition := map[string][]delivered{}
	for _, d := range got {
		p, _ := d.batch[0].MetaGet("iggy_partition_id")
		byPartition[p] = append(byPartition[p], d)
	}
	wantStored := map[uint32]int64{}
	acked := map[int]bool{}
	beyondHole := map[int]bool{}
	for p, ds := range byPartition {
		pid, _ := strconv.Atoi(p)
		wantStored[uint32(pid)] = -1
		if len(ds) < 5 {
			continue
		}
		for i := 2; i >= 0; i-- {
			require.NoError(t, ds[i].ack(ctx, nil))
			for _, m := range ds[i].batch {
				acked[eventNumber(t, m)] = true
			}
		}
		require.NoError(t, ds[4].ack(ctx, nil)) // beyond the hole at ds[3]
		for _, m := range ds[4].batch {
			beyondHole[eventNumber(t, m)] = true
		}
		last, _ := ds[2].batch[len(ds[2].batch)-1].MetaGet("iggy_offset")
		lastOff, _ := strconv.ParseInt(last, 10, 64)
		wantStored[uint32(pid)] = lastOff
	}
	require.Eventually(t, func() bool {
		return assert.ObjectsAreEqual(wantStored, storedOffsets(t, admin, topic, "it-crash", partitions, false))
	}, 10*time.Second, 100*time.Millisecond)

	// Crash: the connection drops and the process never acknowledges or
	// commits anything again.
	r1.mu.Lock()
	crashed := r1.conn
	r1.mu.Unlock()
	crashed.stop()
	_ = crashed.client.Close()
	<-crashed.stopped
	require.Equal(t, wantStored, storedOffsets(t, admin, topic, "it-crash", partitions, false),
		"offsets moved after the crash")

	_, in2 := env.newInput(t, topic, "it-crash", "")
	require.NoError(t, in2.Connect(ctx))
	defer func() { _ = in2.Close(context.Background()) }()
	first := map[uint32]int64{}
	seen := map[int]bool{}
	for len(seen)+len(acked) < total || !coversAll(seen, acked, total) {
		b, ack, err := readIntegration(ctx, in2)
		require.NoError(t, err)
		for _, m := range b {
			p, _ := m.MetaGet("iggy_partition_id")
			o, _ := m.MetaGet("iggy_offset")
			pid, _ := strconv.Atoi(p)
			off, _ := strconv.ParseInt(o, 10, 64)
			if _, ok := first[uint32(pid)]; !ok {
				first[uint32(pid)] = off
			}
			seen[eventNumber(t, m)] = true
		}
		require.NoError(t, ack(ctx, nil))
	}
	for p, s := range wantStored {
		assert.Equal(t, s+1, first[p], "partition %d did not resume right after its stored offset", p)
	}
	for n := range seen {
		require.False(t, acked[n], "event %d was redelivered although its offset was committed", n)
	}
	for n := range beyondHole {
		require.True(t, seen[n], "event %d, acknowledged beyond an unacknowledged hole, was not redelivered", n)
	}
	t.Logf("after the crash: stored offsets %v, resumed at %v; %d acknowledged-and-committed messages not redelivered, %d acknowledged beyond the hole redelivered, %d consumed after the restart, none lost",
		wantStored, first, len(acked), len(beyondHole), len(seen))
}

func coversAll(seen, acked map[int]bool, total int) bool {
	for i := range total {
		if !seen[i] && !acked[i] {
			return false
		}
	}
	return true
}

// The real binary is killed with SIGKILL mid-stream while its output still
// holds unacknowledged messages. Whatever the group stored at that moment must
// already be in the output, and a restarted process must complete the set.
func TestIntegrationIggyKilledProcessLosesNothing(t *testing.T) {
	env := integrationEnv(t)
	admin := env.client(t)
	const partitions, total = 3, 60_000
	topic := createTopic(t, admin, partitions)
	require.Len(t, produce(t, admin, topic, 0, total), total)

	dir := t.TempDir()
	bin := filepath.Join(dir, "bento")
	build := exec.Command("go", "build", "-o", bin, "github.com/warpstreamlabs/bento/cmd/bento")
	build.Stderr = os.Stderr
	require.NoError(t, build.Run())

	config := func(out string) string {
		p := filepath.Join(dir, filepath.Base(out)+".yaml")
		require.NoError(t, os.WriteFile(p, []byte(fmt.Sprintf(`
http: { enabled: false }
logger: { level: warn }
input:
  iggy:
%s
pipeline:
  processors:
    - mapping: 'root = "%%s %%s %%s".format(@iggy_partition_id, @iggy_offset, content().string())'
    # Slow the pipeline so the kill lands mid-stream.
    - sleep: { duration: 2ms }
output:
  broker:
    outputs:
      - file: { path: %s, codec: lines }
    # Messages wait unacknowledged in the output until a batch is written,
    # like an archiver's S3 upload.
    batching: { count: 1000, period: 1s }
`, indentN(env.inputConfig(t, topic, "it-kill", "batch_count: 200\nexclusive: true"), 4), out)), 0o644))
		return p
	}

	run1 := filepath.Join(dir, "run1.txt")
	cmd := exec.Command(bin, "-c", config(run1))
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	require.Eventually(t, func() bool { return countLines(run1) >= total/4 }, 2*time.Minute, 20*time.Millisecond)
	require.NoError(t, cmd.Process.Signal(syscall.SIGKILL))
	_ = cmd.Wait()

	storedAtKill := storedOffsets(t, admin, topic, "it-kill", partitions, true)
	written1 := readOutput(t, run1)
	require.Less(t, len(written1), total, "the kill came too late to be mid-stream")
	for p := range uint32(partitions) {
		offs := written1[p]
		slices.Sort(offs)
		// Everything at or below the stored offset was written before the kill.
		for o := int64(0); o <= storedAtKill[p]; o++ {
			_, found := slices.BinarySearch(offs, o)
			require.True(t, found, "partition %d offset %d was committed (stored %d) but never written", p, o, storedAtKill[p])
		}
	}

	run2 := filepath.Join(dir, "run2.txt")
	cmd = exec.Command(bin, "-c", config(run2))
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	ends := partitionEnds(t, admin, topic)
	require.Eventually(t, func() bool {
		return assert.ObjectsAreEqual(ends, storedOffsets(t, admin, topic, "it-kill", partitions, true))
	}, 2*time.Minute, 200*time.Millisecond)
	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))
	require.NoError(t, cmd.Wait())

	written2 := readOutput(t, run2)
	dups, lines1, lines2 := 0, 0, 0
	for p := range uint32(partitions) {
		all := map[int64]bool{}
		for _, o := range written1[p] {
			all[o] = true
			lines1++
		}
		for _, o := range written2[p] {
			if all[o] {
				dups++
			}
			all[o] = true
			lines2++
		}
		for o := int64(0); o <= ends[p]; o++ {
			require.True(t, all[o], "partition %d offset %d lost across the kill", p, o)
		}
		require.Equal(t, storedAtKill[p]+1, slices.Min(written2[p]), "partition %d did not resume right after its stored offset", p)
	}
	t.Logf("SIGKILL after %d written lines; stored offsets at kill %v; restart wrote %d lines of which %d were duplicates of unacknowledged-at-kill messages; all %d messages present",
		lines1, storedAtKill, lines2, dups, total)
}

func indentN(s string, n int) string {
	pad := strings.Repeat(" ", n)
	return pad + strings.ReplaceAll(strings.TrimSpace(s), "\n", "\n"+pad)
}

func countLines(path string) int {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	return strings.Count(string(b), "\n")
}

func readOutput(t *testing.T, path string) map[uint32][]int64 {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	out := map[uint32][]int64{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) != 3 {
			continue // a line torn by the kill
		}
		p, err1 := strconv.Atoi(fields[0])
		o, err2 := strconv.ParseInt(fields[1], 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		out[uint32(p)] = append(out[uint32(p)], o)
	}
	require.NoError(t, sc.Err())
	return out
}

// Killing the cluster's metadata leader while messages are produced and
// consumed loses nothing that the producer had confirmed, consumption carries
// on through the surviving nodes, and offsets keep being stored.
//
// In group mode Iggy 0.9.0 keeps the member whose session lived on the killed
// leader in the group, holding its partitions; the subtest reports that
// instead of failing when it is what stalls consumption.
func TestIntegrationIggyLeaderFailover(t *testing.T) {
	env := integrationEnv(t)
	if len(env.containers) != len(env.addresses) || len(env.addresses) < 3 {
		t.Skip("needs IGGY_INTEGRATION_CONTAINERS naming a node container per address of a 3-node cluster")
	}
	t.Run("exclusive", func(t *testing.T) { testLeaderFailover(t, env, true) })
	t.Run("group", func(t *testing.T) { testLeaderFailover(t, env, false) })
}

func testLeaderFailover(t *testing.T, env itEnv, exclusive bool) {
	admin := env.client(t)
	const partitions = 3
	topic := createTopic(t, admin, partitions)
	group := "it-failover"

	_, in := env.newInput(t, topic, group, fmt.Sprintf("exclusive: %v", exclusive))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	require.NoError(t, in.Connect(ctx))

	var mu sync.Mutex
	seen := map[int]int{}
	var afterKill atomic.Int64
	var killed atomic.Bool
	consumeDone := make(chan struct{})
	consumeCtx, stopConsume := context.WithCancel(ctx)
	go func() {
		defer close(consumeDone)
		for consumeCtx.Err() == nil {
			b, ack, err := readIntegration(consumeCtx, in)
			if err != nil {
				continue
			}
			mu.Lock()
			for _, m := range b {
				seen[eventNumber(t, m)]++
			}
			mu.Unlock()
			if killed.Load() {
				afterKill.Add(int64(len(b)))
			}
			_ = ack(consumeCtx, nil)
		}
	}()
	defer func() {
		stopConsume()
		<-consumeDone
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = in.Close(closeCtx)
	}()

	// A separate producer writes before and after the kill.
	producer := env.client(t)
	confirmed := produce(t, producer, topic, 0, 5_000)

	leader := leaderIndex(t, admin, env)
	t.Logf("killing metadata leader %s (%s)", env.containers[leader], env.addresses[leader])
	require.NoError(t, exec.Command("docker", "kill", env.containers[leader]).Run())
	killedAt := time.Now()
	killed.Store(true)
	defer restartNode(t, env, leader)

	confirmed = append(confirmed, produce(t, producer, topic, 5_000, 5_000)...)
	require.Greater(t, len(confirmed), 5_000, "nothing could be produced after the failover")

	var recovered time.Duration
	allConsumed := func() bool {
		mu.Lock()
		defer mu.Unlock()
		if recovered == 0 && afterKill.Load() > 0 {
			recovered = time.Since(killedAt)
		}
		for _, i := range confirmed {
			if seen[i] == 0 {
				return false
			}
		}
		return true
	}
	wait := 2 * time.Minute
	if !exclusive {
		wait = 45 * time.Second
	}
	deadline := time.Now().Add(wait)
	for !allConsumed() && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if !allConsumed() {
		if !exclusive {
			gctx, gcancel := context.WithTimeout(context.Background(), 30*time.Second)
			d, err := admin.GetConsumerGroup(gctx, ident(t, "raw"), ident(t, topic), ident(t, group))
			gcancel()
			require.NoError(t, err)
			if d.MembersCount > 1 {
				mu.Lock()
				missing := 0
				for _, i := range confirmed {
					if seen[i] == 0 {
						missing++
					}
				}
				mu.Unlock()
				t.Skipf("known Iggy 0.9.0 behaviour: the killed leader's group member is still in the group (%d members: %+v), so %d confirmed messages on its partitions are not consumed; use exclusive: true",
					d.MembersCount, d.Members, missing)
			}
		}
		t.Fatalf("confirmed messages were not consumed within %v of the leader being killed", wait)
	}

	// Offsets keep being stored through the new leader.
	ends := partitionEnds(t, admin, topic)
	var stored map[uint32]int64
	for deadline := time.Now().Add(time.Minute); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if stored = storedOffsets(t, admin, topic, group, partitions, exclusive); assert.ObjectsAreEqual(ends, stored) {
			break
		}
	}
	require.Equal(t, ends, stored, "offsets were not stored up to the partition ends after the failover")

	// A fresh consumer of the same name resumes from what the new leader
	// holds: nothing is redelivered.
	_, again := env.newInput(t, topic, group, fmt.Sprintf("exclusive: %v", exclusive))
	require.NoError(t, again.Connect(ctx))
	actx, acancel := context.WithTimeout(ctx, 3*time.Second)
	b, _, err := readIntegration(actx, again)
	acancel()
	assert.ErrorIs(t, err, context.DeadlineExceeded, "a restarted consumer got %d messages again", len(b))
	cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
	_ = again.Close(cctx)
	ccancel()

	mu.Lock()
	defer mu.Unlock()
	dups := 0
	for _, n := range seen {
		if n > 1 {
			dups += n - 1
		}
	}
	t.Logf("leader killed; all %d confirmed messages consumed (%d delivered after the kill, the first %v after it), %d duplicates, stored offsets at the partition ends %v, a restarted consumer resumed there",
		len(confirmed), afterKill.Load(), recovered, dups, ends)
}

// restartNode starts a killed node again and waits until it accepts sign-ins.
func restartNode(t *testing.T, env itEnv, i int) {
	t.Helper()
	require.NoError(t, exec.Command("docker", "start", env.containers[i]).Run())
	require.Eventually(t, func() bool {
		c, err := iggyclient.NewIggyClient(iggyclient.WithTcp(
			tcp.WithServerAddress(env.addresses[i]),
			tcp.WithAutoLogin(tcp.NewUsernamePasswordCredentials(env.username, env.password)),
		))
		if err != nil {
			return false
		}
		defer c.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return c.Connect(ctx) == nil
	}, 2*time.Minute, time.Second, "node %s did not come back", env.containers[i])
	time.Sleep(5 * time.Second) // let it catch up with the view before the next kill
}

func leaderIndex(t *testing.T, c iggcon.Client, env itEnv) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	md, err := c.GetClusterMetadata(ctx)
	require.NoError(t, err)
	for _, n := range md.Nodes {
		if n.Role != iggcon.RoleLeader {
			continue
		}
		for i, a := range env.addresses {
			if strings.HasPrefix(a, n.IP+":") {
				return i
			}
		}
	}
	t.Fatalf("no leader in cluster metadata %+v", md.Nodes)
	return -1
}

// TLS: the input verifies the server against root_cas_file and consumes over
// the encrypted connection; without the CA it refuses to connect.
//
//	IGGY_INTEGRATION_TLS_ADDRESS=<ip>:8090 IGGY_INTEGRATION_TLS_CA_FILE=ca.pem
func TestIntegrationIggyTLS(t *testing.T) {
	integration.CheckSkip(t)
	addr, ca := os.Getenv("IGGY_INTEGRATION_TLS_ADDRESS"), os.Getenv("IGGY_INTEGRATION_TLS_CA_FILE")
	if addr == "" || ca == "" {
		t.Skip("IGGY_INTEGRATION_TLS_ADDRESS and IGGY_INTEGRATION_TLS_CA_FILE are not set")
	}
	admin, err := iggyclient.NewIggyClient(iggyclient.WithTcp(
		tcp.WithServerAddress(addr),
		tcp.WithAutoLogin(tcp.NewUsernamePasswordCredentials("iggy", "iggy")),
		tcp.WithTLS(tcp.WithTLSCAFile(ca)),
	))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	require.NoError(t, admin.Connect(ctx))
	defer admin.Close()
	topic := createTopic(t, admin, 1)
	require.Len(t, produce(t, admin, topic, 0, 1_000), 1_000)

	env := itEnv{addresses: []string{addr}, username: "iggy", password: "iggy"}
	_, in := env.newInput(t, topic, "it-tls", fmt.Sprintf("exclusive: true\ntls: { enabled: true, root_cas_file: %q }", ca))
	require.NoError(t, in.Connect(ctx))
	defer func() { _ = in.Close(context.Background()) }()
	seen := map[int]bool{}
	for len(seen) < 1_000 {
		b, ack, err := readIntegration(ctx, in)
		require.NoError(t, err)
		for _, m := range b {
			seen[eventNumber(t, m)] = true
		}
		require.NoError(t, ack(ctx, nil))
	}

	_, untrusted := env.newInput(t, topic, "it-tls", "exclusive: true\ntls: { enabled: true }")
	cctx, ccancel := context.WithTimeout(ctx, 15*time.Second)
	defer ccancel()
	err = untrusted.Connect(cctx)
	require.Error(t, err, "connected without trusting the server's CA")
	t.Logf("consumed 1000 messages over TLS; without the CA: %v", err)
}
