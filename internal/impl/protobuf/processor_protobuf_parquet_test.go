package protobuf

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
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

// ------------------------------------------------------------------------------
// Wire-format coverage. Every scalar kind the encoder claims to support gets its
// own decode branch and its own Arrow builder, and several of them reinterpret
// bits (zigzag, sign-extension, float bit patterns) in ways a happy-path fixture
// of int64/double/string never touches.

const ppeKindsProto = `syntax = "proto3";
package bento.test;

enum Venue {
  VENUE_UNSPECIFIED = 0;
  VENUE_SPOT = 1;
  VENUE_PERP = 2;
}

message Kinds {
  int32 f_int32 = 1;
  int64 f_int64 = 2;
  uint32 f_uint32 = 3;
  uint64 f_uint64 = 4;
  sint32 f_sint32 = 5;
  sint64 f_sint64 = 6;
  fixed32 f_fixed32 = 7;
  fixed64 f_fixed64 = 8;
  sfixed32 f_sfixed32 = 9;
  sfixed64 f_sfixed64 = 10;
  float f_float = 11;
  double f_double = 12;
  bool f_bool = 13;
  string f_string = 14;
  bytes f_bytes = 15;
  // Field 16 and up need a two-byte tag, which is its own decode path.
  Venue f_enum = 16;
  repeated int32 r_int32 = 17;
  repeated float r_float = 18;
  repeated string r_string = 19;
  map<string, int64> m_counts = 20;
}
`

func ppeKindsDir(t testing.TB) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "kinds.proto"), []byte(ppeKindsProto), 0o644))
	return dir
}

func ppeKindsType(t testing.TB, dir string) protoreflect.MessageType {
	t.Helper()
	files, _, err := loadDescriptors(service.MockResources().FS(), []string{dir})
	require.NoError(t, err)
	d, err := files.FindDescriptorByName("bento.test.Kinds")
	require.NoError(t, err)
	return dynamicpb.NewMessageType(d.(protoreflect.MessageDescriptor))
}

func ppeKindsProc(t testing.TB, dir string) *protobufParquetEncoder {
	t.Helper()
	conf, err := protobufParquetEncodeSpec().ParseYAML(fmt.Sprintf(`
message: bento.test.Kinds
import_paths: [ %v ]
columns:
  - { name: f_int32 }
  - { name: f_int64 }
  - { name: f_uint32 }
  - { name: f_uint64 }
  - { name: f_sint32 }
  - { name: f_sint64 }
  - { name: f_fixed32 }
  - { name: f_fixed64 }
  - { name: f_sfixed32 }
  - { name: f_sfixed64 }
  - { name: f_float }
  - { name: f_double }
  - { name: f_bool }
  - { name: f_string }
  - { name: f_bytes }
  - { name: f_enum }
  - { name: r_int32 }
  - { name: r_float }
`, dir), nil)
	require.NoError(t, err)
	p, err := newProtobufParquetEncoder(conf, service.MockResources())
	require.NoError(t, err)
	return p
}

// Extremes on purpose: the min/max of each width is where a missing
// sign-extension or a stray uint64->int64 conversion shows up.
func TestProtobufParquetEncodeEveryScalarKind(t *testing.T) {
	dir := ppeKindsDir(t)
	mt := ppeKindsType(t, dir)

	m := mt.New()
	fields := m.Descriptor().Fields()
	set := func(name string, v protoreflect.Value) { m.Set(fields.ByName(protoreflect.Name(name)), v) }
	set("f_int32", protoreflect.ValueOfInt32(math.MinInt32))
	set("f_int64", protoreflect.ValueOfInt64(math.MinInt64))
	set("f_uint32", protoreflect.ValueOfUint32(math.MaxUint32))
	set("f_uint64", protoreflect.ValueOfUint64(math.MaxUint64))
	set("f_sint32", protoreflect.ValueOfInt32(math.MinInt32))
	set("f_sint64", protoreflect.ValueOfInt64(math.MinInt64))
	set("f_fixed32", protoreflect.ValueOfUint32(math.MaxUint32))
	set("f_fixed64", protoreflect.ValueOfUint64(math.MaxUint64))
	set("f_sfixed32", protoreflect.ValueOfInt32(math.MinInt32))
	set("f_sfixed64", protoreflect.ValueOfInt64(math.MinInt64))
	set("f_float", protoreflect.ValueOfFloat32(-1.5))
	set("f_double", protoreflect.ValueOfFloat64(-math.MaxFloat64))
	set("f_bool", protoreflect.ValueOfBool(true))
	set("f_string", protoreflect.ValueOfString("héllo"))
	set("f_bytes", protoreflect.ValueOfBytes([]byte{0x00, 0x01, 0xff}))
	set("f_enum", protoreflect.ValueOfEnum(2))
	ints := m.Mutable(fields.ByName("r_int32")).List()
	for _, v := range []int32{1, -2, math.MaxInt32} {
		ints.Append(protoreflect.ValueOfInt32(v))
	}
	floats := m.Mutable(fields.ByName("r_float")).List()
	for _, v := range []float32{1.5, -2.5} {
		floats.Append(protoreflect.ValueOfFloat32(v))
	}
	raw, err := proto.Marshal(m.Interface())
	require.NoError(t, err)

	out, err := ppeKindsProc(t, dir).ProcessBatch(context.Background(), service.MessageBatch{service.NewMessage(raw)})
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Len(t, out[0], 1)
	data, err := out[0][0].AsBytes()
	require.NoError(t, err)
	tbl := ppeReadTable(t, data)
	require.EqualValues(t, 1, tbl.NumRows())

	assert.Equal(t, int32(math.MinInt32), ppeCol(t, tbl, "f_int32").(*array.Int32).Value(0))
	assert.Equal(t, int64(math.MinInt64), ppeCol(t, tbl, "f_int64").(*array.Int64).Value(0))
	assert.Equal(t, uint32(math.MaxUint32), ppeCol(t, tbl, "f_uint32").(*array.Uint32).Value(0))
	assert.Equal(t, uint64(math.MaxUint64), ppeCol(t, tbl, "f_uint64").(*array.Uint64).Value(0))
	assert.Equal(t, int32(math.MinInt32), ppeCol(t, tbl, "f_sint32").(*array.Int32).Value(0), "sint32 must be zigzag-decoded")
	assert.Equal(t, int64(math.MinInt64), ppeCol(t, tbl, "f_sint64").(*array.Int64).Value(0), "sint64 must be zigzag-decoded")
	assert.Equal(t, uint32(math.MaxUint32), ppeCol(t, tbl, "f_fixed32").(*array.Uint32).Value(0))
	assert.Equal(t, uint64(math.MaxUint64), ppeCol(t, tbl, "f_fixed64").(*array.Uint64).Value(0))
	assert.Equal(t, int32(math.MinInt32), ppeCol(t, tbl, "f_sfixed32").(*array.Int32).Value(0), "sfixed32 keeps its sign")
	assert.Equal(t, int64(math.MinInt64), ppeCol(t, tbl, "f_sfixed64").(*array.Int64).Value(0), "sfixed64 keeps its sign")
	assert.Equal(t, float32(-1.5), ppeCol(t, tbl, "f_float").(*array.Float32).Value(0))
	assert.Equal(t, -math.MaxFloat64, ppeCol(t, tbl, "f_double").(*array.Float64).Value(0))
	assert.True(t, ppeCol(t, tbl, "f_bool").(*array.Boolean).Value(0))
	assert.Equal(t, "héllo", ppeCol(t, tbl, "f_string").(*array.String).Value(0))
	assert.Equal(t, []byte{0x00, 0x01, 0xff}, ppeCol(t, tbl, "f_bytes").(*array.Binary).Value(0))
	assert.Equal(t, int32(2), ppeCol(t, tbl, "f_enum").(*array.Int32).Value(0), "an enum lands as its number")

	rInts := ppeCol(t, tbl, "r_int32").(*array.List)
	start, end := rInts.ValueOffsets(0)
	var gotInts []int32
	for i := start; i < end; i++ {
		gotInts = append(gotInts, rInts.ListValues().(*array.Int32).Value(int(i)))
	}
	assert.Equal(t, []int32{1, -2, math.MaxInt32}, gotInts)

	rFloats := ppeCol(t, tbl, "r_float").(*array.List)
	start, end = rFloats.ValueOffsets(0)
	var gotFloats []float32
	for i := start; i < end; i++ {
		gotFloats = append(gotFloats, rFloats.ListValues().(*array.Float32).Value(int(i)))
	}
	assert.Equal(t, []float32{1.5, -2.5}, gotFloats)
}

// proto3 packs repeated scalars, but nothing requires a producer to: proto2
// encoders, hand-rolled writers and some language runtimes emit one tag per
// element, and a decoder that only understands the packed form silently returns
// an empty list for them.
func TestProtobufParquetEncodeUnpackedRepeatedFields(t *testing.T) {
	dir := ppeKindsDir(t)

	var raw []byte
	for _, v := range []int32{7, -8, 9} {
		raw = protowire.AppendTag(raw, 17, protowire.VarintType)
		raw = protowire.AppendVarint(raw, uint64(uint32(v)))
	}
	for _, v := range []float32{0.5, -0.25} {
		raw = protowire.AppendTag(raw, 18, protowire.Fixed32Type)
		raw = protowire.AppendFixed32(raw, math.Float32bits(v))
	}

	out, err := ppeKindsProc(t, dir).ProcessBatch(context.Background(), service.MessageBatch{service.NewMessage(raw)})
	require.NoError(t, err)
	data, err := out[0][0].AsBytes()
	require.NoError(t, err)
	tbl := ppeReadTable(t, data)

	ints := ppeCol(t, tbl, "r_int32").(*array.List)
	start, end := ints.ValueOffsets(0)
	var gotInts []int32
	for i := start; i < end; i++ {
		gotInts = append(gotInts, ints.ListValues().(*array.Int32).Value(int(i)))
	}
	assert.Equal(t, []int32{7, -8, 9}, gotInts, "unpacked varint elements must all be kept")

	floats := ppeCol(t, tbl, "r_float").(*array.List)
	start, end = floats.ValueOffsets(0)
	var gotFloats []float32
	for i := start; i < end; i++ {
		gotFloats = append(gotFloats, floats.ListValues().(*array.Float32).Value(int(i)))
	}
	assert.Equal(t, []float32{0.5, -0.25}, gotFloats, "unpacked fixed32 elements must all be kept")
}

// A record written by a newer producer carries fields this config never mapped.
// Skipping them has to consume exactly the right number of bytes for each wire
// type, or every field after the unknown one decodes as garbage.
func TestProtobufParquetEncodeSkipsUnknownFields(t *testing.T) {
	dir := ppeKindsDir(t)

	var raw []byte
	raw = protowire.AppendTag(raw, 900, protowire.VarintType)
	raw = protowire.AppendVarint(raw, math.MaxUint64)
	raw = protowire.AppendTag(raw, 901, protowire.Fixed64Type)
	raw = protowire.AppendFixed64(raw, 0xdeadbeefcafef00d)
	raw = protowire.AppendTag(raw, 902, protowire.Fixed32Type)
	raw = protowire.AppendFixed32(raw, 0xfeedface)
	raw = protowire.AppendTag(raw, 903, protowire.BytesType)
	raw = protowire.AppendBytes(raw, []byte("an unmapped string"))
	// Only now the field the config actually asks for.
	raw = protowire.AppendTag(raw, 2, protowire.VarintType)
	raw = protowire.AppendVarint(raw, 4242)

	out, err := ppeKindsProc(t, dir).ProcessBatch(context.Background(), service.MessageBatch{service.NewMessage(raw)})
	require.NoError(t, err)
	data, err := out[0][0].AsBytes()
	require.NoError(t, err)
	tbl := ppeReadTable(t, data)
	require.EqualValues(t, 1, tbl.NumRows())
	assert.Equal(t, int64(4242), ppeCol(t, tbl, "f_int64").(*array.Int64).Value(0),
		"the mapped field after four unknown ones must still decode")
}

// A field repeated on the wire for a singular column is legal protobuf and means
// "last one wins" -- merge semantics, not an error.
func TestProtobufParquetEncodeLastValueWinsForSingularFields(t *testing.T) {
	dir := ppeKindsDir(t)

	var raw []byte
	for _, v := range []uint64{1, 2, 3} {
		raw = protowire.AppendTag(raw, 2, protowire.VarintType)
		raw = protowire.AppendVarint(raw, v)
	}

	out, err := ppeKindsProc(t, dir).ProcessBatch(context.Background(), service.MessageBatch{service.NewMessage(raw)})
	require.NoError(t, err)
	data, err := out[0][0].AsBytes()
	require.NoError(t, err)
	tbl := ppeReadTable(t, data)
	assert.Equal(t, int64(3), ppeCol(t, tbl, "f_int64").(*array.Int64).Value(0))
}

// The four supported units have to divide the same instant to the same day, or a
// pipeline configured in ms silently files its rows under the wrong date.
func TestProtobufParquetEncodePartitionUnits(t *testing.T) {
	dir := ppeTestDir(t)
	mt := ppeTickType(t, dir)
	instant := time.Date(2026, 9, 15, 13, 45, 30, 500_000_000, time.UTC)

	for unit, ts := range map[string]int64{
		"s":  instant.Unix(),
		"ms": instant.UnixMilli(),
		"us": instant.UnixMicro(),
		"ns": instant.UnixNano(),
	} {
		t.Run(unit, func(t *testing.T) {
			p := ppeNewProc(t, dir, fmt.Sprintf(`
partition:
  field: local_timestamp_us
  unit: %v
  layout: year=2006/month=01/day=02
`, unit))
			out, err := p.ProcessBatch(context.Background(), service.MessageBatch{
				service.NewMessage(ppeMarshal(t, mt, ppeTick{symbol: "X", localTs: ts})),
			})
			require.NoError(t, err)
			require.Len(t, out[0], 1)
			key, ok := out[0][0].MetaGetMut("partition")
			require.True(t, ok)
			assert.Equal(t, "year=2026/month=09/day=15", key)
		})
	}
}

// The codec is not observable from the decoded rows -- a file written with the
// wrong one reads back identically -- so it has to be asserted against the
// Parquet metadata, which is also what a downstream reader negotiates on.
func TestProtobufParquetEncodeCompressionOptions(t *testing.T) {
	dir := ppeTestDir(t)
	mt := ppeTickType(t, dir)

	for _, tc := range []struct {
		name  string
		extra string
		want  compress.Compression
	}{
		{name: "default is zstd", want: compress.Codecs.Zstd},
		{name: "snappy", extra: "compression: snappy", want: compress.Codecs.Snappy},
		{name: "uncompressed", extra: "compression: uncompressed", want: compress.Codecs.Uncompressed},
		{name: "gzip with a level", extra: "compression: gzip\ncompression_level: 1", want: compress.Codecs.Gzip},
		{name: "zstd without dictionary encoding", extra: "dictionary: false", want: compress.Codecs.Zstd},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := ppeNewProc(t, dir, tc.extra)
			out, err := p.ProcessBatch(context.Background(), service.MessageBatch{
				service.NewMessage(ppeMarshal(t, mt, ppeTick{symbol: "X", price: 1.25})),
			})
			require.NoError(t, err)
			data, err := out[0][0].AsBytes()
			require.NoError(t, err)

			rdr, err := file.NewParquetReader(bytes.NewReader(data))
			require.NoError(t, err)
			t.Cleanup(func() { _ = rdr.Close() })
			chunk, err := rdr.MetaData().RowGroup(0).ColumnChunk(0)
			require.NoError(t, err)
			assert.Equal(t, tc.want, chunk.Compression())

			// Whatever the codec, the rows still have to survive it.
			assert.Equal(t, "X", ppeCol(t, ppeReadTable(t, data), "symbol").(*array.String).Value(0))
		})
	}
}

// These two are rejected at construction rather than mishandled at runtime, and
// the repeated-string guard is load-bearing: a repeated string arrives as one
// length-delimited chunk per element, which the packed-scalar path would happily
// misread as a run of varints.
func TestProtobufParquetEncodeRejectsUnsupportedFieldShapes(t *testing.T) {
	dir := ppeKindsDir(t)
	for name, column := range map[string]string{
		"repeated string": "r_string",
		"map":             "m_counts",
	} {
		t.Run(name, func(t *testing.T) {
			conf, err := protobufParquetEncodeSpec().ParseYAML(fmt.Sprintf(
				"message: bento.test.Kinds\nimport_paths: [ %v ]\ncolumns: [ { name: %v } ]", dir, column), nil)
			require.NoError(t, err)
			_, err = newProtobufParquetEncoder(conf, service.MockResources())
			require.Error(t, err)
		})
	}
}
