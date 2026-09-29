package parquet

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component/metrics"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/public/service"
)

func newTestJSONEncoder(tb testing.TB, conf string, opts ...service.MockResourcesOptFn) (*jsonParquetEncoder, error) {
	tb.Helper()
	parsed, err := jsonParquetEncodeSpec().ParseYAML(conf, nil)
	require.NoError(tb, err)
	return newJSONParquetEncoder(parsed, service.MockResources(opts...))
}

func TestJSONParquetEncodeRejectsWhatItCannotWrite(t *testing.T) {
	// A config json_parquet_encode would write differently from parquet_encode,
	// or not at all, must fail at start rather than on the first batch.
	for name, conf := range map[string]string{
		"no columns":              `schema: []`,
		"nested column":           `schema: [ { name: a, type: STRUCT, fields: [ { name: b, type: UTF8 } ] } ]`,
		"list column":             `schema: [ { name: a, type: LIST, fields: [ { name: element, type: UTF8 } ] } ]`,
		"unsupported type":        `schema: [ { name: a, type: BOOLEAN } ]`,
		"byte array":              `schema: [ { name: a, type: BYTE_ARRAY } ]`,
		"repeated":                `schema: [ { name: a, type: INT64, repeated: true } ]`,
		"duplicate column":        `schema: [ { name: a, type: INT64 }, { name: a, type: UTF8 } ]`,
		"unknown override":        "schema: [ { name: a, type: UTF8 } ]\ncolumns: [ { name: b, value: x } ]",
		"override not UTF8":       "schema: [ { name: a, type: INT64 } ]\ncolumns: [ { name: a, value: '1' } ]",
		"unknown placeholder":     "schema: [ { name: a, type: UTF8 } ]\npartition: { path: 'x={b}' }",
		"unclosed placeholder":    "schema: [ { name: a, type: UTF8 } ]\npartition: { path: 'x={a' }",
		"empty time layout":       "schema: [ { name: a, type: INT64 } ]\npartition: { path: 'x={a|}' }",
		"time of interpolated":    "schema: [ { name: a, type: UTF8 } ]\ncolumns: [ { name: a, value: x } ]\npartition: { path: '{a|2006}' }",
		"unknown time unit":       "schema: [ { name: a, type: INT64 } ]\npartition: { path: '{a|2006}', time_unit: days }",
		"cache of a field":        "schema: [ { name: a, type: UTF8 } ]\ncolumns: [ { name: a, value: '${! this.b }', cache_by: [ t ] } ]",
		"cache of all metadata":   "schema: [ { name: a, type: UTF8 } ]\ncolumns: [ { name: a, value: '${! @ }', cache_by: [ t ] } ]",
		"cache of meta and field": "schema: [ { name: a, type: UTF8 } ]\ncolumns: [ { name: a, value: '${! @t }-${! this.b }', cache_by: [ t ] } ]",
		"cache by too few":        "schema: [ { name: a, type: UTF8 } ]\ncolumns: [ { name: a, value: '${! @t }-${! @u }', cache_by: [ t ] } ]",
		"cache of dynamic arg":    "schema: [ { name: a, type: UTF8 } ]\ncolumns: [ { name: a, value: '${! @t.trim_prefix(@p) }', cache_by: [ t ] } ]",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := newTestJSONEncoder(t, conf)
			assert.Error(t, err)
		})
	}
}

func readRows(t *testing.T, data []byte) []parquet.Row {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	var rows []parquet.Row
	for _, rg := range f.RowGroups() {
		r := rg.Rows()
		buf := make([]parquet.Row, 16)
		for {
			n, err := r.ReadRows(buf)
			for _, row := range buf[:n] {
				rows = append(rows, row.Clone())
			}
			if err != nil {
				break
			}
		}
		require.NoError(t, r.Close())
	}
	return rows
}

func TestJSONParquetEncodeDropsInvalidRows(t *testing.T) {
	// Dropped rows are acknowledged with the batch and never archived; the rest
	// of the batch is written.
	parsed, err := jsonParquetEncodeSpec().ParseYAML(`
schema:
  - { name: id, type: INT64 }
  - { name: v, type: DOUBLE, optional: true }
`, nil)
	require.NoError(t, err)
	res := service.MockResources()
	e, err := newJSONParquetEncoder(parsed, res)
	require.NoError(t, err)

	out, err := e.ProcessBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":1,"v":null}`)),
		service.NewMessage([]byte(`{"v":2}`)),    // missing id
		service.NewMessage([]byte(`{"id":1.5}`)), // not an integer
		service.NewMessage([]byte(`not json`)),   // not JSON
		service.NewMessage([]byte(`{"id":2,"v":3}`)),
	})
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Len(t, out[0], 1, "without a partition the batch is one file")
	data, err := out[0][0].AsBytes()
	require.NoError(t, err)
	rows := readRows(t, data)
	require.Len(t, rows, 2)
	assert.Equal(t, int64(1), rows[0][0].Int64())
	assert.True(t, rows[0][1].IsNull())
	assert.Equal(t, int64(2), rows[1][0].Int64())
	assert.Equal(t, 3.0, rows[1][1].Double())
}

func TestJSONParquetEncodeAllDroppedWritesNothing(t *testing.T) {
	e, err := newTestJSONEncoder(t, `schema: [ { name: id, type: INT64 } ]`)
	require.NoError(t, err)
	out, err := e.ProcessBatch(context.Background(), service.MessageBatch{service.NewMessage([]byte(`{}`))})
	require.NoError(t, err)
	assert.Nil(t, out, "an empty Parquet file would be uploaded as an object with no rows")
	out, err = e.ProcessBatch(context.Background(), service.MessageBatch{})
	require.NoError(t, err)
	assert.Nil(t, out)
}

func TestJSONParquetEncodePartitionsKeepFirstMessageMetadata(t *testing.T) {
	e, err := newTestJSONEncoder(t, `
schema:
  - { name: ex, type: UTF8 }
  - { name: t, type: INT64 }
columns:
  - { name: ex, value: '${! @topic }' }
partition:
  path: 'ex={ex}/{t|2006-01-02}'
  time_unit: s
  metadata_key: part
`)
	require.NoError(t, err)
	msg := func(topic, body string, offset int) *service.Message {
		m := service.NewMessage([]byte(body))
		m.MetaSetMut("topic", topic)
		m.MetaSetMut("offset", offset)
		return m
	}
	out, err := e.ProcessBatch(context.Background(), service.MessageBatch{
		msg("a", `{"t":86399}`, 1), // 1970-01-01
		msg("b", `{"t":86400}`, 2), // another exchange
		msg("a", `{"t":86400}`, 3), // the next day
		msg("a", `{"t":0}`, 4),     // back to the first partition
	})
	require.NoError(t, err)
	require.Len(t, out, 1)
	var parts []string
	for _, m := range out[0] {
		p, _ := m.MetaGet("part")
		off, _ := m.MetaGetMut("offset")
		parts = append(parts, p)
		data, err := m.AsBytes()
		require.NoError(t, err)
		switch p {
		case "ex=a/1970-01-01":
			assert.Equal(t, 1, off, "a file carries its first message's metadata")
			assert.Len(t, readRows(t, data), 2)
		default:
			assert.Len(t, readRows(t, data), 1)
		}
	}
	assert.Equal(t, []string{"ex=a/1970-01-01", "ex=b/1970-01-02", "ex=a/1970-01-02"}, parts, "files in order of first appearance")
}

func TestJSONParquetEncodeOptionalStringIsNull(t *testing.T) {
	// No Bloblang the archivers generate has an optional UTF8 column; here an
	// optional column of any type reads null (or its absence) as NULL.
	e, err := newTestJSONEncoder(t, `schema: [ { name: s, type: UTF8, optional: true }, { name: n, type: FLOAT } ]`)
	require.NoError(t, err)
	out, err := e.ProcessBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(`{"s":null,"n":1}`)),
		service.NewMessage([]byte(`{"n":2}`)),
		service.NewMessage([]byte(`{"s":"x","n":null}`)), // null in a required float, nan_for_null off
	})
	require.NoError(t, err)
	data, err := out[0][0].AsBytes()
	require.NoError(t, err)
	rows := readRows(t, data)
	require.Len(t, rows, 2)
	assert.True(t, rows[0][0].IsNull())
	assert.True(t, rows[1][0].IsNull())
	assert.Equal(t, float32(2), rows[1][1].Float())
	assert.False(t, math.IsNaN(float64(rows[1][1].Float())))
}

func TestJSONParquetEncodeCachedColumn(t *testing.T) {
	// count() reads no metadata, so a cached value holding it shows how often
	// the value is evaluated: once a batch for each topic, rather than for each
	// message. Its counters live for the process, so each run takes new ones.
	values := func(cacheBy string) []string {
		e, err := newTestJSONEncoder(t, fmt.Sprintf(`
schema: [ { name: a, type: UTF8 }, { name: n, type: INT64 } ]
columns: [ { name: a, value: '${! @topic }-${! count("%v") }', cache_by: [ %v ] } ]
`, uniqueCounter(), cacheBy))
		require.NoError(t, err)
		var got []string
		for range 2 {
			var batch service.MessageBatch
			for _, topic := range []string{"x", "x", "y", "x"} {
				m := service.NewMessage([]byte(`{"n":1}`))
				m.MetaSetMut("topic", topic)
				batch = append(batch, m)
			}
			got = append(got, encodedColumn(t, e, batch)...)
		}
		return got
	}
	assert.Equal(t, []string{"x-1", "x-2", "y-3", "x-4", "x-5", "x-6", "y-7", "x-8"}, values(""))
	assert.Equal(t, []string{"x-1", "x-1", "y-2", "x-1", "x-3", "x-3", "y-4", "x-3"}, values("topic"))

	// a topic that is not a string is not cached, since it could print as a
	// string does yet read differently
	e, err := newTestJSONEncoder(t, fmt.Sprintf(`
schema: [ { name: a, type: UTF8 }, { name: n, type: INT64 } ]
columns: [ { name: a, value: '${! @topic }-${! count("%v") }', cache_by: [ topic ] } ]
`, uniqueCounter()))
	require.NoError(t, err)
	var batch service.MessageBatch
	for _, topic := range []any{"1", 1, 1} {
		m := service.NewMessage([]byte(`{"n":1}`))
		m.MetaSetMut("topic", topic)
		batch = append(batch, m)
	}
	assert.Equal(t, []string{"1-1", "1-2", "1-3"}, encodedColumn(t, e, batch))
}

func TestJSONParquetEncodeCachedByEveryField(t *testing.T) {
	// A method with a non-literal argument reports only its argument's reads,
	// so the cache is keyed by the fields listed, not by those reported: listed
	// in full, messages that differ in any of them do not share a value.
	e, err := newTestJSONEncoder(t, `
schema: [ { name: a, type: UTF8 }, { name: n, type: INT64 } ]
columns: [ { name: a, value: '${! @topic.split(@sep).index(1) }', cache_by: [ topic, sep ] } ]
`)
	require.NoError(t, err)
	var batch service.MessageBatch
	for _, meta := range [][2]string{{"a.binance", "."}, {"a.okx", "."}, {"a.okx", "."}, {"a-bybit", "-"}, {"a.bybit", "-"}} {
		m := service.NewMessage([]byte(`{"n":1}`))
		m.MetaSetMut("topic", meta[0])
		m.MetaSetMut("sep", meta[1])
		batch = append(batch, m)
	}
	assert.Equal(t, []string{"binance", "okx", "okx", "bybit"}, encodedColumn(t, e, batch), "the last topic has no '-' to split on, and is dropped")
}

var counterSeq atomic.Int64

// uniqueCounter names a Bloblang count() counter no other run has used.
func uniqueCounter() string {
	return fmt.Sprintf("jpe_test_%d", counterSeq.Add(1))
}

// encodedColumn encodes batch into one file and returns its first column.
func encodedColumn(t *testing.T, e *jsonParquetEncoder, batch service.MessageBatch) []string {
	t.Helper()
	out, err := e.ProcessBatch(context.Background(), batch)
	require.NoError(t, err)
	require.Len(t, out[0], 1)
	data, err := out[0][0].AsBytes()
	require.NoError(t, err)
	var got []string
	for _, row := range readRows(t, data) {
		got = append(got, string(row[0].ByteArray()))
	}
	return got
}

func TestJSONParquetEncodeCountsFilesAndDrops(t *testing.T) {
	local := metrics.NewLocal()
	e, err := newTestJSONEncoder(t, `
schema: [ { name: p, type: UTF8 }, { name: n, type: INT64 } ]
partition: { path: '{p}' }
`, func(m *mock.Manager) { m.M = local })
	require.NoError(t, err)
	for range 2 {
		_, err = e.ProcessBatch(context.Background(), service.MessageBatch{
			service.NewMessage([]byte(`{"p":"a","n":1}`)),
			service.NewMessage([]byte(`{"p":"b","n":2}`)),
			service.NewMessage([]byte(`{"p":"a","n":3}`)),
			service.NewMessage([]byte(`{"p":"c","n":"x"}`)), // dropped
		})
		require.NoError(t, err)
	}
	counters := local.GetCounters()
	assert.Equal(t, int64(4), counters["json_parquet_encode_files"])
	assert.Equal(t, int64(2), counters["json_parquet_encode_dropped"])
}

// BenchmarkJSONParquetEncodeRejected is a batch a producer broke: every row
// lacks a required column, each in a partition of its own.
func BenchmarkJSONParquetEncodeRejected(b *testing.B) {
	e, err := newTestJSONEncoder(b, `
schema:
  - { name: p, type: UTF8 }
  - { name: s, type: UTF8 }
  - { name: t, type: INT64 }
  - { name: x, type: DOUBLE }
partition: { path: '{p}' }
`)
	require.NoError(b, err)
	var batch service.MessageBatch
	for i := range 10000 {
		batch = append(batch, service.NewMessage(fmt.Appendf(nil, `{"p":"p%d","s":"some symbol","t":%d}`, i, i)))
	}
	b.ReportAllocs()
	for b.Loop() {
		out, err := e.ProcessBatch(context.Background(), batch)
		require.NoError(b, err)
		require.Nil(b, out)
	}
}

func TestJSONParquetEncodeColumnLookup(t *testing.T) {
	// A member name is looked up by its JSON as it is when that is how JSON
	// quotes a column's name, and unquoted otherwise: it must find what
	// unquoting every name would.
	names := []string{"a", "symbol", "q\"uote", "back\\slash", "<&>", "line\u2028sep", "del\x7f", "é😀", "tab\t"}
	e := &jsonParquetEncoder{byName: map[string]int{}, byQuoted: map[string]int{}}
	for i, n := range names {
		quoted, err := jsontext.AppendQuote(nil, n)
		require.NoError(t, err)
		e.byName[n], e.byQuoted[string(quoted)] = i, i
	}
	var literals []string
	for _, n := range append(names, "b", "symbol2", "", "\xff", "a\xff") {
		quoted, _ := jsontext.AppendQuote(nil, n)
		literals = append(literals, string(quoted), `"`+n+`"`)
		var escaped strings.Builder
		for _, c := range n {
			fmt.Fprintf(&escaped, `\u%04x`, c)
		}
		literals = append(literals, `"`+escaped.String()+`"`)
	}
	r := newJPERow(0)
	for _, lit := range literals {
		if _, err := jsontext.AppendUnquote(nil, lit); err != nil && !strings.Contains(err.Error(), "UTF-8") {
			continue // not a string JSON would read
		}
		wantI, wantOK := e.byName[string(r.unquoteString([]byte(lit)))]
		gotI, gotOK := e.column(r, []byte(lit))
		assert.Equal(t, wantOK, gotOK, "%q", lit)
		assert.Equal(t, wantI, gotI, "%q", lit)
	}
}
