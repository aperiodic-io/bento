package protobuf

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	ppeFieldMessage          = "message"
	ppeFieldImportPaths      = "import_paths"
	ppeFieldColumns          = "columns"
	ppeFieldColumnName       = "name"
	ppeFieldColumnField      = "field"
	ppeFieldColumnConstant   = "constant"
	ppeFieldPartition        = "partition"
	ppeFieldPartitionField   = "field"
	ppeFieldPartitionUnit    = "unit"
	ppeFieldPartitionLayout  = "layout"
	ppeFieldPartitionMetaKey = "metadata_key"
	ppeFieldCompression      = "compression"
	ppeFieldCompressionLevel = "compression_level"
	ppeFieldDictionary       = "dictionary"
	ppeFieldSkipInvalid      = "skip_invalid_messages"
)

func protobufParquetEncodeSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Parsing").
		Summary("Decodes a batch of protobuf messages straight into columnar Parquet files, without an intermediate JSON or structured representation.").
		Description(`
Each message in the batch must be a single serialized protobuf message of the configured type. The wire format is decoded directly into typed Apache Arrow column builders and written as a Parquet file with one row group, which is an order of magnitude cheaper than chaining the `+"`protobuf`"+` (to_json), `+"`mapping`"+` and `+"`parquet_encode`"+` processors.

Columns are emitted in the configured order. A column either copies a protobuf field (optionally renamed) or holds a constant string. Proto3 fields that are absent from a message take their zero value. Repeated scalar fields become Parquet LIST columns and string columns are dictionary encoded when `+"`dictionary`"+` is enabled.

When `+"`partition`"+` is set, the batch is split by formatting a timestamp field with a Go time layout, producing one Parquet file per distinct key, and the key is written to the configured metadata field of each output message. Every output message otherwise carries the metadata of the first input message of its partition.

Messages that cannot be decoded are dropped and logged when `+"`skip_invalid_messages`"+` is true, and fail the batch otherwise.`).
		Field(service.NewStringField(ppeFieldMessage).Description("The fully qualified name of the protobuf message.")).
		Field(service.NewStringListField(ppeFieldImportPaths).Description("Directories to load `.proto` files from.").Default([]any{})).
		Field(service.NewObjectListField(ppeFieldColumns,
			service.NewStringField(ppeFieldColumnName).Description("The Parquet column name."),
			service.NewStringField(ppeFieldColumnField).Description("The protobuf field copied into the column. Defaults to the column name.").Optional(),
			service.NewStringField(ppeFieldColumnConstant).Description("A constant string value for every row, instead of a protobuf field.").Optional(),
		).Description("The output columns, in order.")).
		Field(service.NewObjectField(ppeFieldPartition,
			service.NewStringField(ppeFieldPartitionField).Description("An integer protobuf field holding a Unix timestamp."),
			service.NewStringEnumField(ppeFieldPartitionUnit, "s", "ms", "us", "ns").Description("The unit of the timestamp field.").Default("us"),
			service.NewStringField(ppeFieldPartitionLayout).Description("A Go time layout, evaluated in UTC, that forms the partition key.").Example("year=2006/month=01/day=02/hour=15"),
			service.NewStringField(ppeFieldPartitionMetaKey).Description("The metadata field that receives the partition key.").Default("partition"),
		).Description("Split the batch into one Parquet file per formatted timestamp key.").Optional()).
		Field(service.NewStringEnumField(ppeFieldCompression, "zstd", "snappy", "gzip", "lz4raw", "uncompressed").Default("zstd")).
		Field(service.NewIntField(ppeFieldCompressionLevel).Description("The compression level, for codecs that support one.").Default(3).Advanced()).
		Field(service.NewBoolField(ppeFieldDictionary).Description("Dictionary encode string columns.").Default(true).Advanced()).
		Field(service.NewBoolField(ppeFieldSkipInvalid).Description("Drop (and log) messages that cannot be decoded instead of failing the batch.").Default(true)).
		Example("Archiving protobuf records to S3 as hourly Parquet",
			"Batches records at the output, splits each batch by hour of a microsecond timestamp and uploads one Parquet file per hour.",
			`
output:
  aws_s3:
    bucket: raw-archive
    path: 'quotes/${! meta("partition") }/${! uuid_v4() }.parquet'
    batching:
      count: 50000
      period: 60s
      processors:
        - protobuf_parquet_encode:
            message: live.producers.v1.QuoteRecord
            import_paths: [ /etc/bento/proto ]
            columns:
              - { name: exchange, constant: binance-futures }
              - { name: symbol }
              - { name: time }
              - { name: local_timestamp }
              - { name: bid_price }
            partition:
              field: local_timestamp
              unit: us
              layout: year=2006/month=01/day=02/hour=15
`)
}

func init() {
	err := service.RegisterBatchProcessor("protobuf_parquet_encode", protobufParquetEncodeSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return newProtobufParquetEncoder(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

// ------------------------------------------------------------------------------

// ppeColumn is one output column. slot indexes the decoded value of the row, or
// is -1 for constant columns.
type ppeColumn struct {
	name     string
	constant string
	slot     int
}

// ppeSlot is one protobuf field decoded per row.
type ppeSlot struct {
	kind     protoreflect.Kind
	repeated bool
	dataType arrow.DataType
}

type protobufParquetEncoder struct {
	log         *service.Logger
	mInvalid    *service.MetricCounter
	columns     []ppeColumn
	slots       []ppeSlot
	slotByField []int // indexed by protobuf field number, -1 when not decoded
	schema      *arrow.Schema
	props       *parquet.WriterProperties
	skipInvalid bool

	partitionSlot    int // -1 when partitioning is disabled
	partitionDivisor int64
	partitionLayout  string
	partitionMetaKey string
}

func newProtobufParquetEncoder(conf *service.ParsedConfig, mgr *service.Resources) (*protobufParquetEncoder, error) {
	msgName, err := conf.FieldString(ppeFieldMessage)
	if err != nil {
		return nil, err
	}
	importPaths, err := conf.FieldStringList(ppeFieldImportPaths)
	if err != nil {
		return nil, err
	}
	files, _, err := loadDescriptors(mgr.FS(), importPaths)
	if err != nil {
		return nil, fmt.Errorf("failed to load protobuf definitions: %w", err)
	}
	d, err := files.FindDescriptorByName(protoreflect.FullName(msgName))
	if err != nil {
		return nil, fmt.Errorf("unable to find message '%v' definition: %w", msgName, err)
	}
	md, ok := d.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("'%v' is not a message", msgName)
	}

	e := &protobufParquetEncoder{
		log:           mgr.Logger(),
		mInvalid:      mgr.Metrics().NewCounter("protobuf_parquet_encode_invalid"),
		partitionSlot: -1,
	}
	slotOfField := map[protoreflect.FieldNumber]int{}
	addSlot := func(fieldName string) (int, error) {
		fd := md.Fields().ByName(protoreflect.Name(fieldName))
		if fd == nil {
			return 0, fmt.Errorf("message '%v' has no field '%v'", msgName, fieldName)
		}
		if s, exists := slotOfField[fd.Number()]; exists {
			return s, nil
		}
		dt, err := ppeArrowType(fd)
		if err != nil {
			return 0, err
		}
		e.slots = append(e.slots, ppeSlot{kind: fd.Kind(), repeated: fd.IsList(), dataType: dt})
		slotOfField[fd.Number()] = len(e.slots) - 1
		return len(e.slots) - 1, nil
	}

	colConfs, err := conf.FieldObjectList(ppeFieldColumns)
	if err != nil {
		return nil, err
	}
	if len(colConfs) == 0 {
		return nil, errors.New("at least one column is required")
	}
	arrowFields := make([]arrow.Field, 0, len(colConfs))
	for _, cc := range colConfs {
		name, err := cc.FieldString(ppeFieldColumnName)
		if err != nil {
			return nil, err
		}
		col := ppeColumn{name: name, slot: -1}
		if cc.Contains(ppeFieldColumnConstant) {
			if cc.Contains(ppeFieldColumnField) {
				return nil, fmt.Errorf("column '%v' sets both field and constant", name)
			}
			if col.constant, err = cc.FieldString(ppeFieldColumnConstant); err != nil {
				return nil, err
			}
			arrowFields = append(arrowFields, arrow.Field{Name: name, Type: arrow.BinaryTypes.String})
		} else {
			fieldName := name
			if cc.Contains(ppeFieldColumnField) {
				if fieldName, err = cc.FieldString(ppeFieldColumnField); err != nil {
					return nil, err
				}
			}
			if col.slot, err = addSlot(fieldName); err != nil {
				return nil, fmt.Errorf("column '%v': %w", name, err)
			}
			arrowFields = append(arrowFields, arrow.Field{Name: name, Type: e.slots[col.slot].dataType})
		}
		e.columns = append(e.columns, col)
	}
	e.schema = arrow.NewSchema(arrowFields, nil)

	if conf.Contains(ppeFieldPartition) {
		pc := conf.Namespace(ppeFieldPartition)
		fieldName, err := pc.FieldString(ppeFieldPartitionField)
		if err != nil {
			return nil, err
		}
		if e.partitionSlot, err = addSlot(fieldName); err != nil {
			return nil, fmt.Errorf("partition: %w", err)
		}
		if s := e.slots[e.partitionSlot]; s.repeated || !ppeIsInteger(s.kind) {
			return nil, fmt.Errorf("partition field '%v' must be a singular integer field", fieldName)
		}
		unit, err := pc.FieldString(ppeFieldPartitionUnit)
		if err != nil {
			return nil, err
		}
		e.partitionDivisor = map[string]int64{"s": 1, "ms": 1e3, "us": 1e6, "ns": 1e9}[unit]
		if e.partitionLayout, err = pc.FieldString(ppeFieldPartitionLayout); err != nil {
			return nil, err
		}
		if e.partitionMetaKey, err = pc.FieldString(ppeFieldPartitionMetaKey); err != nil {
			return nil, err
		}
	}

	maxField := protoreflect.FieldNumber(0)
	for f := range slotOfField {
		maxField = max(maxField, f)
	}
	e.slotByField = make([]int, maxField+1)
	for i := range e.slotByField {
		e.slotByField[i] = -1
	}
	for f, s := range slotOfField {
		e.slotByField[f] = s
	}

	codecName, err := conf.FieldString(ppeFieldCompression)
	if err != nil {
		return nil, err
	}
	level, err := conf.FieldInt(ppeFieldCompressionLevel)
	if err != nil {
		return nil, err
	}
	dictionary, err := conf.FieldBool(ppeFieldDictionary)
	if err != nil {
		return nil, err
	}
	if e.skipInvalid, err = conf.FieldBool(ppeFieldSkipInvalid); err != nil {
		return nil, err
	}
	codec := map[string]compress.Compression{
		"zstd": compress.Codecs.Zstd, "snappy": compress.Codecs.Snappy, "gzip": compress.Codecs.Gzip,
		"lz4raw": compress.Codecs.Lz4Raw, "uncompressed": compress.Codecs.Uncompressed,
	}[codecName]
	propOpts := []parquet.WriterProperty{
		parquet.WithCompression(codec),
		parquet.WithDictionaryDefault(false),
		// One row group per file: a batch is already the unit of upload.
		parquet.WithMaxRowGroupLength(math.MaxInt64),
		parquet.WithStats(true),
	}
	if codecName == "zstd" || codecName == "gzip" {
		propOpts = append(propOpts, parquet.WithCompressionLevel(level))
	}
	if dictionary {
		for _, f := range arrowFields {
			if f.Type.ID() == arrow.STRING {
				propOpts = append(propOpts, parquet.WithDictionaryFor(f.Name, true))
			}
		}
	}
	e.props = parquet.NewWriterProperties(propOpts...)
	return e, nil
}

func ppeIsInteger(k protoreflect.Kind) bool {
	switch k {
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind,
		protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind, protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return true
	}
	return false
}

func ppeArrowType(fd protoreflect.FieldDescriptor) (arrow.DataType, error) {
	if fd.IsMap() {
		return nil, fmt.Errorf("map field '%v' is not supported", fd.Name())
	}
	var t arrow.DataType
	switch fd.Kind() {
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		t = arrow.PrimitiveTypes.Int64
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind, protoreflect.EnumKind:
		t = arrow.PrimitiveTypes.Int32
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		t = arrow.PrimitiveTypes.Uint64
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		t = arrow.PrimitiveTypes.Uint32
	case protoreflect.DoubleKind:
		t = arrow.PrimitiveTypes.Float64
	case protoreflect.FloatKind:
		t = arrow.PrimitiveTypes.Float32
	case protoreflect.BoolKind:
		t = arrow.FixedWidthTypes.Boolean
	case protoreflect.StringKind:
		t = arrow.BinaryTypes.String
	case protoreflect.BytesKind:
		t = arrow.BinaryTypes.Binary
	default:
		return nil, fmt.Errorf("field '%v' of kind %v is not supported", fd.Name(), fd.Kind())
	}
	if fd.IsList() {
		if t.ID() == arrow.STRING || t.ID() == arrow.BINARY {
			return nil, fmt.Errorf("repeated %v field '%v' is not supported", fd.Kind(), fd.Name())
		}
		return arrow.ListOf(t), nil
	}
	return t, nil
}

// ------------------------------------------------------------------------------

// ppeRow holds the decoded values of one message, reused across messages.
// Scalars are kept as raw wire bits and interpreted per kind when appended.
type ppeRow struct {
	bits  []uint64
	bytes [][]byte
	lists [][]uint64
}

func (r *ppeRow) reset() {
	clear(r.bits)
	clear(r.bytes)
	for i := range r.lists {
		r.lists[i] = r.lists[i][:0]
	}
}

func (e *protobufParquetEncoder) decode(b []byte, r *ppeRow) error {
	r.reset()
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
		slot := -1
		if int(num) < len(e.slotByField) {
			slot = e.slotByField[num]
		}
		if slot < 0 {
			if n = protowire.ConsumeFieldValue(num, typ, b); n < 0 {
				return protowire.ParseError(n)
			}
			b = b[n:]
			continue
		}
		s := &e.slots[slot]
		switch typ {
		case protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			b = b[n:]
			e.store(r, slot, s, v)
		case protowire.Fixed64Type:
			v, n := protowire.ConsumeFixed64(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			b = b[n:]
			e.store(r, slot, s, v)
		case protowire.Fixed32Type:
			v, n := protowire.ConsumeFixed32(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			b = b[n:]
			e.store(r, slot, s, uint64(v))
		case protowire.BytesType:
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			b = b[n:]
			if !s.repeated {
				r.bytes[slot] = v
				continue
			}
			if err := e.storePacked(r, slot, s, v); err != nil {
				return err
			}
		default:
			if n = protowire.ConsumeFieldValue(num, typ, b); n < 0 {
				return protowire.ParseError(n)
			}
			b = b[n:]
		}
	}
	return nil
}

func (e *protobufParquetEncoder) store(r *ppeRow, slot int, s *ppeSlot, v uint64) {
	if s.repeated {
		r.lists[slot] = append(r.lists[slot], v)
	} else {
		r.bits[slot] = v
	}
}

func (e *protobufParquetEncoder) storePacked(r *ppeRow, slot int, s *ppeSlot, b []byte) error {
	for len(b) > 0 {
		var v uint64
		var n int
		switch s.kind {
		case protoreflect.DoubleKind, protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind:
			v, n = protowire.ConsumeFixed64(b)
		case protoreflect.FloatKind, protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind:
			var v32 uint32
			v32, n = protowire.ConsumeFixed32(b)
			v = uint64(v32)
		default:
			v, n = protowire.ConsumeVarint(b)
		}
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
		r.lists[slot] = append(r.lists[slot], v)
	}
	return nil
}

// ------------------------------------------------------------------------------

type ppeGroup struct {
	first     *service.Message
	key       string
	rows      int
	builders  []array.Builder
	appenders []func(r *ppeRow)
}

func (e *protobufParquetEncoder) newGroup(first *service.Message, key string, mem memory.Allocator, sizeHint int) *ppeGroup {
	g := &ppeGroup{first: first, key: key}
	for _, col := range e.columns {
		if col.slot < 0 {
			bld := array.NewStringBuilder(mem)
			bld.Reserve(sizeHint)
			constant := col.constant
			g.builders = append(g.builders, bld)
			g.appenders = append(g.appenders, func(*ppeRow) { bld.Append(constant) })
			continue
		}
		bld := array.NewBuilder(mem, e.slots[col.slot].dataType)
		bld.Reserve(sizeHint)
		g.builders = append(g.builders, bld)
		g.appenders = append(g.appenders, ppeAppender(bld, col.slot, e.slots[col.slot]))
	}
	return g
}

func ppeScalar(kind protoreflect.Kind, v uint64) any {
	switch kind {
	case protoreflect.Sint64Kind:
		return protowire.DecodeZigZag(v)
	case protoreflect.Sint32Kind:
		return int32(protowire.DecodeZigZag(v & math.MaxUint32))
	}
	return nil
}

// ppeAppender binds the builder type once, so the per-row hot path does not
// type switch.
func ppeAppender(bld array.Builder, slot int, s ppeSlot) func(r *ppeRow) {
	if s.repeated {
		lb := bld.(*array.ListBuilder)
		elem := ppeAppender(lb.ValueBuilder(), 0, ppeSlot{kind: s.kind})
		scratch := &ppeRow{bits: make([]uint64, 1)}
		return func(r *ppeRow) {
			lb.Append(true)
			for _, v := range r.lists[slot] {
				scratch.bits[0] = v
				elem(scratch)
			}
		}
	}
	switch b := bld.(type) {
	case *array.Int64Builder:
		switch s.kind {
		case protoreflect.Sint64Kind:
			return func(r *ppeRow) { b.Append(protowire.DecodeZigZag(r.bits[slot])) }
		default:
			return func(r *ppeRow) { b.Append(int64(r.bits[slot])) }
		}
	case *array.Int32Builder:
		switch s.kind {
		case protoreflect.Sint32Kind:
			return func(r *ppeRow) { b.Append(ppeScalar(s.kind, r.bits[slot]).(int32)) }
		case protoreflect.Sfixed32Kind:
			return func(r *ppeRow) { b.Append(int32(uint32(r.bits[slot]))) }
		default:
			return func(r *ppeRow) { b.Append(int32(r.bits[slot])) }
		}
	case *array.Uint64Builder:
		return func(r *ppeRow) { b.Append(r.bits[slot]) }
	case *array.Uint32Builder:
		return func(r *ppeRow) { b.Append(uint32(r.bits[slot])) }
	case *array.Float64Builder:
		return func(r *ppeRow) { b.Append(math.Float64frombits(r.bits[slot])) }
	case *array.Float32Builder:
		return func(r *ppeRow) { b.Append(math.Float32frombits(uint32(r.bits[slot]))) }
	case *array.BooleanBuilder:
		return func(r *ppeRow) { b.Append(r.bits[slot] != 0) }
	case *array.StringBuilder:
		return func(r *ppeRow) { b.BinaryBuilder.Append(r.bytes[slot]) }
	case *array.BinaryBuilder:
		return func(r *ppeRow) { b.Append(r.bytes[slot]) }
	}
	panic(fmt.Sprintf("unsupported builder %T", bld))
}

func (e *protobufParquetEncoder) partitionKey(r *ppeRow, s *ppeSlot, lastSec *int64, lastKey *string) string {
	v := int64(r.bits[e.partitionSlot])
	if s.kind == protoreflect.Sint64Kind || s.kind == protoreflect.Sint32Kind {
		v = protowire.DecodeZigZag(r.bits[e.partitionSlot])
	}
	sec := v / e.partitionDivisor
	if v < 0 && v%e.partitionDivisor != 0 {
		sec--
	}
	if sec != *lastSec || *lastKey == "" {
		*lastSec = sec
		*lastKey = time.Unix(sec, 0).UTC().Format(e.partitionLayout)
	}
	return *lastKey
}

func (e *protobufParquetEncoder) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	if len(batch) == 0 {
		return nil, nil
	}
	mem := memory.NewGoAllocator()
	row := &ppeRow{bits: make([]uint64, len(e.slots)), bytes: make([][]byte, len(e.slots)), lists: make([][]uint64, len(e.slots))}

	var groups []*ppeGroup
	byKey := map[string]*ppeGroup{}
	var current *ppeGroup
	lastSec, lastKey := int64(0), ""
	invalid := 0

	for _, msg := range batch {
		raw, err := msg.AsBytes()
		if err == nil {
			err = e.decode(raw, row)
		}
		if err != nil {
			if !e.skipInvalid {
				return nil, fmt.Errorf("failed to decode protobuf message: %w", err)
			}
			invalid++
			continue
		}
		key := ""
		if e.partitionSlot >= 0 {
			key = e.partitionKey(row, &e.slots[e.partitionSlot], &lastSec, &lastKey)
		}
		if current == nil || current.key != key {
			if current = byKey[key]; current == nil {
				current = e.newGroup(msg, key, mem, len(batch))
				byKey[key] = current
				groups = append(groups, current)
			}
		}
		for _, appendCol := range current.appenders {
			appendCol(row)
		}
		current.rows++
	}
	if invalid > 0 {
		e.mInvalid.Incr(int64(invalid))
		e.log.Errorf("Dropped %d of %d messages that could not be decoded as protobuf", invalid, len(batch))
	}

	out := make(service.MessageBatch, 0, len(groups))
	for _, g := range groups {
		data, err := e.write(g)
		if err != nil {
			return nil, err
		}
		m := g.first.Copy()
		m.SetBytes(data)
		if e.partitionSlot >= 0 {
			m.MetaSetMut(e.partitionMetaKey, g.key)
		}
		out = append(out, m)
	}
	if len(out) == 0 {
		return nil, nil
	}
	return []service.MessageBatch{out}, nil
}

func (e *protobufParquetEncoder) write(g *ppeGroup) ([]byte, error) {
	cols := make([]arrow.Array, len(g.builders))
	for i, b := range g.builders {
		cols[i] = b.NewArray()
		b.Release()
	}
	rec := array.NewRecordBatch(e.schema, cols, int64(g.rows))
	defer rec.Release()
	for _, c := range cols {
		c.Release()
	}

	var buf bytes.Buffer
	w, err := pqarrow.NewFileWriter(e.schema, &buf, e.props, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}
	if err := w.Write(rec); err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("failed to write parquet row group: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("failed to finalize parquet file: %w", err)
	}
	return buf.Bytes(), nil
}

func (e *protobufParquetEncoder) Close(ctx context.Context) error {
	return nil
}
