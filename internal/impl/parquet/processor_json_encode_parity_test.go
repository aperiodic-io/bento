package parquet_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"

	_ "github.com/warpstreamlabs/bento/internal/impl/parquet"
	_ "github.com/warpstreamlabs/bento/public/components/pure"
	"github.com/warpstreamlabs/bento/public/service"
)

// The parity harness runs the pipeline json_parquet_encode replaces and
// json_parquet_encode itself over the same batches, each as a real Bento
// stream, and requires the same Parquet files, byte for byte, under the same
// partitions with the same metadata, and the same rows dropped.
//
// The replaced pipeline is the one a Bloblang mapping per column builds: the
// mapping coerces every column and throws on a row it cannot, a catch drops the
// row, group_by_value splits the batch by the partition path the mapping put
// in metadata, and parquet_encode writes each group.

type parityColumn struct {
	name     string
	typ      string
	optional bool
	fromMeta string // the Bloblang of an interpolated column, "" to read the field
}

type paritySchema struct {
	columns   []parityColumn
	partition string // json_parquet_encode's partition path, "" for none
	// legacyPartition is the Bloblang expression the replaced mapping writes to
	// meta partition; it must mean the same as partition.
	legacyPartition string
	nanForNull      bool
}

// archiveSchema has every column type and optionality, an interpolated column
// and a time-formatted partition, as metric rows archived by exchange and day.
var archiveSchema = paritySchema{
	columns: []parityColumn{
		{name: "exchange", typ: "UTF8", fromMeta: `@kafka_topic.split(".").index(2)`},
		{name: "symbol", typ: "UTF8"},
		{name: "interval", typ: "UTF8"},
		{name: "timestamp_type", typ: "UTF8"},
		{name: "day", typ: "UTF8"},
		{name: "time", typ: "INT64"},
		{name: "count", typ: "INT32"},
		{name: "count_opt", typ: "INT32", optional: true},
		{name: "ratio", typ: "FLOAT"},
		{name: "ratio_opt", typ: "FLOAT", optional: true},
		{name: "price", typ: "DOUBLE"},
		{name: "price_opt", typ: "DOUBLE", optional: true},
	},
	partition:       "timestamp={timestamp_type}/{interval}/exchange={exchange}/{time|year=2006/month=01/day=02}",
	legacyPartition: `"timestamp=%s/%s/exchange=%s/%s".format(this.timestamp_type, this.interval, root.exchange, (this.time / 1000000).ts_format("year=2006/month=01/day=02", "UTC"))`,
	nanForNull:      true,
}

func (s paritySchema) schemaYAML() string {
	var b strings.Builder
	for _, c := range s.columns {
		fmt.Fprintf(&b, "        - { name: %s, type: %s, optional: %v }\n", c.name, c.typ, c.optional)
	}
	return b.String()
}

// legacyMapping is the Bloblang the replaced pipeline coerces rows with.
func (s paritySchema) legacyMapping() string {
	var required, checks []string
	for _, c := range s.columns {
		if c.fromMeta != "" {
			continue
		}
		if !c.optional {
			required = append(required, fmt.Sprintf("%q", c.name))
		}
		integer := c.typ == "INT32" || c.typ == "INT64"
		switch {
		case c.typ == "UTF8":
			checks = append(checks, fmt.Sprintf("root.%[1]s = this.%[1]s.not_null().string()", c.name))
		case integer && c.optional:
			checks = append(checks, fmt.Sprintf("root.%[1]s = if this.%[1]s != null { this.%[1]s.%[2]s() } else { null }", c.name, strings.ToLower(c.typ)))
		case integer:
			checks = append(checks, fmt.Sprintf("root.%[1]s = this.%[1]s.%[2]s()", c.name, strings.ToLower(c.typ)))
		case c.optional:
			checks = append(checks, fmt.Sprintf("root.%[1]s = if this.%[1]s != null { this.%[1]s.number() } else { null }", c.name))
		case s.nanForNull:
			checks = append(checks, fmt.Sprintf("root.%[1]s = this.%[1]s.or($nan).number()", c.name))
		default:
			checks = append(checks, fmt.Sprintf("root.%[1]s = this.%[1]s.not_null().number()", c.name))
		}
	}
	lines := []string{
		"root = this",
		"let missing = [" + strings.Join(required, ", ") + "].filter(c -> !this.exists(c))",
		`root = if $missing.length() > 0 { throw("missing columns: " + $missing.join(", ")) } else { this }`,
		`let nan = "NaN".number()`,
	}
	lines = append(lines, checks...)
	for _, c := range s.columns {
		if c.fromMeta != "" {
			lines = append(lines, fmt.Sprintf("root.%s = %s", c.name, c.fromMeta))
		}
	}
	if s.legacyPartition != "" {
		lines = append(lines, "meta partition = "+s.legacyPartition)
	}
	return strings.Join(lines, "\n")
}

func (s paritySchema) legacyProcessors() string {
	var b strings.Builder
	b.WriteString("pipeline:\n  processors:\n    - mapping: |\n")
	for l := range strings.SplitSeq(s.legacyMapping(), "\n") {
		b.WriteString("        " + l + "\n")
	}
	b.WriteString(`    - catch:
        - mapping: root = deleted()
`)
	if s.partition != "" {
		b.WriteString(`    - group_by_value:
        value: '${! meta("partition") }'
`)
	}
	b.WriteString("    - parquet_encode:\n        default_compression: zstd\n        schema:\n")
	b.WriteString(strings.ReplaceAll(s.schemaYAML(), "        - ", "          - "))
	return b.String()
}

func (s paritySchema) newProcessors() string {
	var b strings.Builder
	fmt.Fprintf(&b, "pipeline:\n  processors:\n    - json_parquet_encode:\n        default_compression: zstd\n        nan_for_null: %v\n        schema:\n", s.nanForNull)
	b.WriteString(strings.ReplaceAll(s.schemaYAML(), "        - ", "          - "))
	b.WriteString("        columns:\n")
	for _, c := range s.columns {
		if c.fromMeta != "" {
			fmt.Fprintf(&b, "          - { name: %s, value: '${! %s }' }\n", c.name, c.fromMeta)
		}
	}
	if s.partition != "" {
		fmt.Fprintf(&b, "        partition:\n          path: '%s'\n          time_unit: us\n", s.partition)
	}
	return b.String()
}

//------------------------------------------------------------------------------

// parityStream is a running stream fed one batch at a time; each send returns
// once the batch is acknowledged, with what reached the output.
type parityStream struct {
	send    service.MessageBatchHandlerFunc
	mu      sync.Mutex
	out     []service.MessageBatch
	stopped chan error // receives Run's error should the stream stop
}

func startParityStream(tb testing.TB, processors string) *parityStream {
	tb.Helper()
	b := service.NewStreamBuilder()
	require.NoError(tb, b.SetLoggerYAML("level: none"))
	// processors is a pipeline section: each list item is added as a processor
	body := strings.TrimPrefix(processors, "pipeline:\n  processors:\n")
	for _, item := range strings.Split("\n"+body, "\n    - ")[1:] {
		var lines []string
		for l := range strings.SplitSeq(item, "\n") {
			lines = append(lines, strings.TrimPrefix(l, "      "))
		}
		require.NoError(tb, b.AddProcessorYAML(strings.Join(lines, "\n")), item)
	}
	send, err := b.AddBatchProducerFunc()
	require.NoError(tb, err)
	ps := &parityStream{send: send}
	require.NoError(tb, b.AddBatchConsumerFunc(func(_ context.Context, batch service.MessageBatch) error {
		ps.mu.Lock()
		ps.out = append(ps.out, batch.Copy())
		ps.mu.Unlock()
		return nil
	}))
	stream, err := b.Build()
	require.NoError(tb, err, processors)
	ps.stopped = make(chan error, 1)
	go func() { ps.stopped <- stream.Run(context.Background()) }()
	return ps
}

type parityInput struct {
	topic string
	body  []byte
}

type parityFile struct {
	partition string
	data      []byte
	meta      map[string]any
}

func (ps *parityStream) run(tb testing.TB, in []parityInput) []parityFile {
	tb.Helper()
	ps.mu.Lock()
	ps.out = nil
	ps.mu.Unlock()
	batch := make(service.MessageBatch, len(in))
	for i, m := range in {
		msg := service.NewMessage(append([]byte(nil), m.body...))
		if m.topic != "" {
			msg.MetaSetMut("kafka_topic", m.topic)
		}
		msg.MetaSetMut("kafka_offset", i)
		batch[i] = msg
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sent := make(chan error, 1)
	go func() { sent <- ps.send(ctx, batch) }()
	select {
	case err := <-sent:
		require.NoError(tb, err)
	case err := <-ps.stopped:
		ps.stopped <- err
		tb.Fatalf("the stream stopped: %v", err)
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	var files []parityFile
	for _, b := range ps.out {
		for _, m := range b {
			data, err := m.AsBytes()
			require.NoError(tb, err)
			meta := map[string]any{}
			require.NoError(tb, m.MetaWalkMut(func(k string, v any) error { meta[k] = v; return nil }))
			p, _ := m.MetaGet("partition")
			files = append(files, parityFile{partition: p, data: append([]byte(nil), data...), meta: meta})
		}
	}
	return files
}

type parityPair struct{ legacy, next *parityStream }

var (
	parityPairsMu sync.Mutex
	parityPairs   = map[string]*parityPair{}
)

// pairFor starts (once per schema) the replaced pipeline and json_parquet_encode.
func pairFor(tb testing.TB, s paritySchema) *parityPair {
	tb.Helper()
	key := s.legacyProcessors() + s.newProcessors()
	parityPairsMu.Lock()
	defer parityPairsMu.Unlock()
	if p, ok := parityPairs[key]; ok {
		return p
	}
	p := &parityPair{legacy: startParityStream(tb, s.legacyProcessors()), next: startParityStream(tb, s.newProcessors())}
	parityPairs[key] = p
	return p
}

// requireParity runs a batch through both and fails on any difference.
func requireParity(tb testing.TB, s paritySchema, in []parityInput) []parityFile {
	tb.Helper()
	p := pairFor(tb, s)
	legacy := p.legacy.run(tb, in)
	next := p.next.run(tb, in)
	describe := func(fs []parityFile) string {
		var parts []string
		for _, f := range fs {
			parts = append(parts, fmt.Sprintf("%q (%d bytes, %d rows)", f.partition, len(f.data), len(parityRows(tb, f.data))))
		}
		return strings.Join(parts, ", ")
	}
	if len(legacy) != len(next) {
		tb.Fatalf("legacy wrote %d files [%s], json_parquet_encode %d [%s]\ninput: %s", len(legacy), describe(legacy), len(next), describe(next), describeInput(in))
	}
	for i := range legacy {
		l, n := legacy[i], next[i]
		if l.partition != n.partition {
			tb.Fatalf("file %d: partition %q, json_parquet_encode %q\ninput: %s", i, l.partition, n.partition, describeInput(in))
		}
		if !bytes.Equal(l.data, n.data) {
			lr, nr := parityRows(tb, l.data), parityRows(tb, n.data)
			tb.Fatalf("file %d (%q) differs:\n%s\ninput: %s", i, l.partition, diffRows(lr, nr), describeInput(in))
		}
		if fmt.Sprint(sortedMeta(l.meta)) != fmt.Sprint(sortedMeta(n.meta)) {
			tb.Fatalf("file %d (%q): metadata %v, json_parquet_encode %v", i, l.partition, sortedMeta(l.meta), sortedMeta(n.meta))
		}
	}
	return next
}

func describeInput(in []parityInput) string {
	var parts []string
	for _, m := range in {
		parts = append(parts, fmt.Sprintf("[%s] %q", m.topic, m.body))
	}
	if len(parts) > 12 {
		parts = append(parts[:12], fmt.Sprintf("... %d more", len(parts)-12))
	}
	return strings.Join(parts, "\n  ")
}

func sortedMeta(m map[string]any) []string {
	var out []string
	for k, v := range m {
		out = append(out, fmt.Sprintf("%s=%v", k, v))
	}
	sort.Strings(out)
	return out
}

// parityRows decodes a Parquet file into printable rows, float bits included.
func parityRows(tb testing.TB, data []byte) []string {
	tb.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	require.NoError(tb, err)
	var rows []string
	for _, rg := range f.RowGroups() {
		r := rg.Rows()
		buf := make([]parquet.Row, 64)
		for {
			n, err := r.ReadRows(buf)
			for _, row := range buf[:n] {
				var vals []string
				for _, v := range row {
					switch {
					case v.IsNull():
						vals = append(vals, "NULL")
					case v.Kind() == parquet.Float:
						vals = append(vals, fmt.Sprintf("f32:%v(%08x)", v.Float(), math.Float32bits(v.Float())))
					case v.Kind() == parquet.Double:
						vals = append(vals, fmt.Sprintf("f64:%v(%016x)", v.Double(), math.Float64bits(v.Double())))
					default:
						vals = append(vals, v.String())
					}
				}
				rows = append(rows, strings.Join(vals, " | "))
			}
			if err != nil {
				break
			}
		}
		_ = r.Close()
	}
	return rows
}

func diffRows(legacy, next []string) string {
	if len(legacy) != len(next) {
		return fmt.Sprintf("legacy %d rows, json_parquet_encode %d rows\nlegacy: %v\nnew:    %v", len(legacy), len(next), legacy, next)
	}
	for i := range legacy {
		if legacy[i] != next[i] {
			return fmt.Sprintf("row %d:\nlegacy: %s\nnew:    %s", i, legacy[i], next[i])
		}
	}
	return "same rows, different bytes (encoding or file metadata)"
}
