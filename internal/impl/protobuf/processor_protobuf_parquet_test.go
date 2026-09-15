package protobuf

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/warpstreamlabs/bento/public/service"
)

const ppeTestProto = `syntax = "proto3";
package bento.test;

message Tick {
  int64 time_us = 1;
  int64 exchange = 2;
  string symbol = 3;
  double price = 4;
  int64 local_timestamp_us = 5;
  bool is_snapshot = 6;
  repeated double levels = 7;
  sint64 delta = 8;
  int32 venue = 9;
}
`

func ppeTestDir(t testing.TB) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "tick.proto"), []byte(ppeTestProto), 0o644))
	return dir
}

func ppeTickType(t testing.TB, dir string) protoreflect.MessageType {
	t.Helper()
	files, _, err := loadDescriptors(service.MockResources().FS(), []string{dir})
	require.NoError(t, err)
	d, err := files.FindDescriptorByName("bento.test.Tick")
	require.NoError(t, err)
	return dynamicpb.NewMessageType(d.(protoreflect.MessageDescriptor))
}

type ppeTick struct {
	timeUs, exchange, localTs, delta int64
	symbol                           string
	price                            float64
	snapshot                         bool
	levels                           []float64
	venue                            int32
}

func ppeMarshal(t testing.TB, mt protoreflect.MessageType, v ppeTick) []byte {
	t.Helper()
	m := mt.New()
	fields := m.Descriptor().Fields()
	set := func(name string, val protoreflect.Value) { m.Set(fields.ByName(protoreflect.Name(name)), val) }
	set("time_us", protoreflect.ValueOfInt64(v.timeUs))
	set("exchange", protoreflect.ValueOfInt64(v.exchange))
	set("symbol", protoreflect.ValueOfString(v.symbol))
	set("price", protoreflect.ValueOfFloat64(v.price))
	set("local_timestamp_us", protoreflect.ValueOfInt64(v.localTs))
	set("is_snapshot", protoreflect.ValueOfBool(v.snapshot))
	set("delta", protoreflect.ValueOfInt64(v.delta))
	set("venue", protoreflect.ValueOfInt32(v.venue))
	if len(v.levels) > 0 {
		l := m.Mutable(fields.ByName("levels")).List()
		for _, x := range v.levels {
			l.Append(protoreflect.ValueOfFloat64(x))
		}
	}
	b, err := proto.Marshal(m.Interface())
	require.NoError(t, err)
	return b
}

func ppeNewProc(t testing.TB, dir, extra string) *protobufParquetEncoder {
	t.Helper()
	conf, err := protobufParquetEncodeSpec().ParseYAML(fmt.Sprintf(`
message: bento.test.Tick
import_paths: [ %v ]
columns:
  - { name: exchange, constant: binance-futures }
  - { name: symbol }
  - { name: time, field: time_us }
  - { name: local_timestamp, field: local_timestamp_us }
  - { name: price }
  - { name: is_snapshot }
  - { name: levels }
  - { name: delta }
  - { name: venue }
%v`, dir, extra), nil)
	require.NoError(t, err)
	p, err := newProtobufParquetEncoder(conf, service.MockResources())
	require.NoError(t, err)
	return p
}

func ppeReadTable(t testing.TB, data []byte) arrow.Table {
	t.Helper()
	rdr, err := file.NewParquetReader(bytes.NewReader(data))
	require.NoError(t, err)
	fr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)
	tbl, err := fr.ReadTable(context.Background())
	require.NoError(t, err)
	t.Cleanup(tbl.Release)
	assert.Equal(t, 1, rdr.NumRowGroups(), "a batch must become a single row group")
	return tbl
}

func ppeCol(t testing.TB, tbl arrow.Table, name string) arrow.Array {
	t.Helper()
	idx := tbl.Schema().FieldIndices(name)
	require.Len(t, idx, 1, "column %v", name)
	require.Len(t, tbl.Column(idx[0]).Data().Chunks(), 1)
	return tbl.Column(idx[0]).Data().Chunk(0)
}

func TestProtobufParquetEncodeRoundTrip(t *testing.T) {
	dir := ppeTestDir(t)
	mt := ppeTickType(t, dir)
	p := ppeNewProc(t, dir, "")

	ticks := []ppeTick{
		{timeUs: 1_789_502_374_870_000, exchange: 11, symbol: "BTC-USDT", price: 101.5, localTs: 1_789_502_374_995_869, snapshot: true, levels: []float64{1.5, 2.5, 3.5}, delta: -42, venue: -7},
		// Zero values are not on the wire in proto3; they must still come back as zeroes, not nulls.
		{symbol: "ETH-USDT"},
		{timeUs: 3, exchange: 11, symbol: "BTC-USDT", price: -0.25, localTs: 4, levels: []float64{9}},
	}
	batch := service.MessageBatch{}
	for i, tk := range ticks {
		m := service.NewMessage(ppeMarshal(t, mt, tk))
		m.MetaSetMut("kafka_offset", fmt.Sprint(i))
		batch = append(batch, m)
	}

	out, err := p.ProcessBatch(context.Background(), batch)
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Len(t, out[0], 1)

	// The output keeps the first message's metadata, like parquet_encode.
	off, ok := out[0][0].MetaGetMut("kafka_offset")
	require.True(t, ok)
	assert.Equal(t, "0", off)

	data, err := out[0][0].AsBytes()
	require.NoError(t, err)
	tbl := ppeReadTable(t, data)
	require.EqualValues(t, len(ticks), tbl.NumRows())

	var names []string
	for _, f := range tbl.Schema().Fields() {
		names = append(names, f.Name)
	}
	assert.Equal(t, []string{"exchange", "symbol", "time", "local_timestamp", "price", "is_snapshot", "levels", "delta", "venue"}, names)

	exch := ppeCol(t, tbl, "exchange").(*array.String)
	sym := ppeCol(t, tbl, "symbol").(*array.String)
	tm := ppeCol(t, tbl, "time").(*array.Int64)
	lts := ppeCol(t, tbl, "local_timestamp").(*array.Int64)
	price := ppeCol(t, tbl, "price").(*array.Float64)
	snap := ppeCol(t, tbl, "is_snapshot").(*array.Boolean)
	levels := ppeCol(t, tbl, "levels").(*array.List)
	delta := ppeCol(t, tbl, "delta").(*array.Int64)
	venue := ppeCol(t, tbl, "venue").(*array.Int32)
	for i, tk := range ticks {
		assert.Equal(t, "binance-futures", exch.Value(i))
		assert.Equal(t, tk.symbol, sym.Value(i))
		assert.Equal(t, tk.timeUs, tm.Value(i))
		assert.Equal(t, tk.localTs, lts.Value(i))
		assert.Equal(t, tk.price, price.Value(i))
		assert.Equal(t, tk.snapshot, snap.Value(i))
		assert.Equal(t, tk.delta, delta.Value(i))
		assert.Equal(t, tk.venue, venue.Value(i))
		assert.False(t, levels.IsNull(i))
		start, end := levels.ValueOffsets(i)
		var got []float64
		for j := start; j < end; j++ {
			got = append(got, levels.ListValues().(*array.Float64).Value(int(j)))
		}
		if len(tk.levels) == 0 {
			assert.Empty(t, got)
		} else {
			assert.Equal(t, tk.levels, got)
		}
	}
}

func TestProtobufParquetEncodePartitionsByDay(t *testing.T) {
	dir := ppeTestDir(t)
	mt := ppeTickType(t, dir)
	p := ppeNewProc(t, dir, `
partition:
  field: local_timestamp_us
  unit: us
  layout: year=2006/month=01/day=02
  metadata_key: day
`)
	day1 := time.Date(2026, 9, 15, 23, 59, 59, 0, time.UTC).UnixMicro()
	day2 := time.Date(2026, 9, 16, 0, 0, 0, 0, time.UTC).UnixMicro()
	var batch service.MessageBatch
	for _, ts := range []int64{day1, day2, day1 - 1, day2 + 5, day2 + 6} {
		batch = append(batch, service.NewMessage(ppeMarshal(t, mt, ppeTick{symbol: "X", localTs: ts})))
	}
	out, err := p.ProcessBatch(context.Background(), batch)
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Len(t, out[0], 2, "one file per day")

	wantKeys := []string{"year=2026/month=09/day=15", "year=2026/month=09/day=16"}
	wantRows := []int64{2, 3}
	for i, m := range out[0] {
		key, ok := m.MetaGetMut("day")
		require.True(t, ok)
		assert.Equal(t, wantKeys[i], key)
		data, err := m.AsBytes()
		require.NoError(t, err)
		tbl := ppeReadTable(t, data)
		assert.Equal(t, wantRows[i], tbl.NumRows())
		lts := ppeCol(t, tbl, "local_timestamp").(*array.Int64)
		for j := 0; j < lts.Len(); j++ {
			assert.Equal(t, wantKeys[i], time.UnixMicro(lts.Value(j)).UTC().Format("year=2006/month=01/day=02"), "row landed in the wrong partition")
		}
	}
}

func TestProtobufParquetEncodeInvalidMessages(t *testing.T) {
	dir := ppeTestDir(t)
	mt := ppeTickType(t, dir)
	good := ppeMarshal(t, mt, ppeTick{symbol: "OK", timeUs: 1})
	truncated := good[:len(good)-1]

	p := ppeNewProc(t, dir, "")
	out, err := p.ProcessBatch(context.Background(), service.MessageBatch{
		service.NewMessage(truncated), service.NewMessage(good),
	})
	require.NoError(t, err)
	data, err := out[0][0].AsBytes()
	require.NoError(t, err)
	tbl := ppeReadTable(t, data)
	assert.EqualValues(t, 1, tbl.NumRows(), "the undecodable message is dropped, the valid one is kept")

	strict := ppeNewProc(t, dir, "skip_invalid_messages: false")
	_, err = strict.ProcessBatch(context.Background(), service.MessageBatch{service.NewMessage(truncated)})
	require.Error(t, err)

	out, err = p.ProcessBatch(context.Background(), service.MessageBatch{service.NewMessage(truncated)})
	require.NoError(t, err)
	assert.Empty(t, out, "a batch with no decodable message produces no file")
}

func TestProtobufParquetEncodeConfigErrors(t *testing.T) {
	dir := ppeTestDir(t)
	for name, conf := range map[string]string{
		"unknown field":          "columns: [ { name: nope } ]",
		"field and constant":     "columns: [ { name: s, field: symbol, constant: x } ]",
		"string partition field": "columns: [ { name: symbol } ]\npartition: { field: symbol, layout: '2006' }",
		"unknown message":        "columns: [ { name: symbol } ]",
	} {
		t.Run(name, func(t *testing.T) {
			msg := "bento.test.Tick"
			if name == "unknown message" {
				msg = "bento.test.Nope"
			}
			parsed, err := protobufParquetEncodeSpec().ParseYAML(fmt.Sprintf("message: %v\nimport_paths: [ %v ]\n%v", msg, dir, conf), nil)
			require.NoError(t, err)
			_, err = newProtobufParquetEncoder(parsed, service.MockResources())
			require.Error(t, err)
		})
	}
}

func BenchmarkProtobufParquetEncode(b *testing.B) {
	dir := ppeTestDir(b)
	mt := ppeTickType(b, dir)
	p := ppeNewProc(b, dir, "partition: { field: local_timestamp_us, layout: 'year=2006/month=01/day=02' }")
	base := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC).UnixMicro()
	const n = 50_000
	batch := make(service.MessageBatch, n)
	for i := range batch {
		batch[i] = service.NewMessage(ppeMarshal(b, mt, ppeTick{
			timeUs: base + int64(i)*1000, exchange: 11, symbol: fmt.Sprintf("SYM%d-USDT", i%400),
			price: 100 + float64(i%1000)/7, localTs: base + int64(i)*1000 + 250,
		}))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.ProcessBatch(context.Background(), batch); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(n*b.N)/b.Elapsed().Seconds(), "msgs/s")
}
