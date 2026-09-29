package parquet

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"

	"github.com/parquet-go/parquet-go"

	"github.com/warpstreamlabs/bento/internal/value"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	jpeFieldColumns          = "columns"
	jpeFieldColumnName       = "name"
	jpeFieldColumnValue      = "value"
	jpeFieldNaNForNull       = "nan_for_null"
	jpeFieldPartition        = "partition"
	jpeFieldPartitionPath    = "path"
	jpeFieldPartitionUnit    = "time_unit"
	jpeFieldPartitionMetaKey = "metadata_key"
)

func jsonParquetEncodeSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Parsing").
		Summary("Encodes a batch of flat JSON objects straight into Parquet, row by row, without holding the batch as structured messages.").
		Description(`
Produces the same Parquet as chaining a `+"`mapping`"+` that coerces each column, a `+"`catch`"+` that drops the rows it rejects, `+"`group_by_value`"+` on a partition path and `+"`parquet_encode`"+`, at a fraction of the memory: a batch is held only as its raw bytes until it is processed, each message is then parsed once into a Parquet row, and the files are encoded one at a time.

The schema is `+"`parquet_encode`"+`'s, restricted to flat `+"`UTF8`"+`, `+"`INT32`"+`, `+"`INT64`"+`, `+"`FLOAT`"+` and `+"`DOUBLE`"+` columns, and the file is written by the same encoder. A column reads the message field of its name and coerces it as Bloblang would:

- `+"`UTF8`"+`: `+"`.not_null().string()`"+`, so a number keeps its literal text;
- `+"`INT32`"+`, `+"`INT64`"+`: `+"`.int32()`"+`, `+"`.int64()`"+` (a fraction, an exponent or an out-of-range value is rejected; a string is parsed);
- `+"`FLOAT`"+`, `+"`DOUBLE`"+`: `+"`.number()`"+`, narrowed to 32 bits for `+"`FLOAT`"+`.

A required column the message lacks rejects it. A `+"`null`"+` in an optional column is written as NULL, and in a required float column as NaN when `+"`nan_for_null`"+` is set; otherwise it rejects the message. A column may instead take an interpolated `+"`value`"+`, evaluated per message against its metadata only.

A message that is not a single JSON object, or that any column rejects, is dropped: it is logged, counted in `+"`json_parquet_encode_dropped`"+` and acknowledged with the batch.

When `+"`partition`"+` is set the batch is split into one Parquet file per distinct partition path, in order of first appearance, and the path is written to the configured metadata key. `+"`{column}`"+` in the path is replaced by the column's value as Bloblang's `+"`format`"+` would print the field, and `+"`{column|layout}`"+` by the column's number, read in `+"`time_unit`"+`, as a UTC time in the Go layout. Every output message carries the metadata of the first message of its partition.`).
		Field(parquetSchemaConfig()).
		Field(service.NewObjectListField(jpeFieldColumns,
			service.NewStringField(jpeFieldColumnName).Description("The schema column this sets."),
			service.NewInterpolatedStringField(jpeFieldColumnValue).Description("The column's value, from the message's metadata. It must not reference the message content."),
		).Description("Columns set from an interpolation instead of the message field of their name.").Default([]any{})).
		Field(service.NewBoolField(jpeFieldNaNForNull).Description("Write a `null` in a required `FLOAT` or `DOUBLE` column as NaN instead of dropping the message.").Default(false)).
		Field(service.NewObjectField(jpeFieldPartition,
			service.NewStringField(jpeFieldPartitionPath).Description("The partition path, with `{column}` and `{column|layout}` placeholders.").Example("day={time|2006-01-02}/{symbol}"),
			service.NewStringEnumField(jpeFieldPartitionUnit, "s", "ms", "us", "ns").Description("The unit of numbers formatted as a time.").Default("us"),
			service.NewStringField(jpeFieldPartitionMetaKey).Description("The metadata field that receives the partition path.").Default("partition"),
		).Description("Split the batch into one Parquet file per partition path.").Optional()).
		Field(service.NewStringEnumField("default_compression",
			"uncompressed", "snappy", "gzip", "brotli", "zstd", "lz4raw",
		).Description("The default compression type to use for fields.").Default("uncompressed")).
		Field(service.NewStringEnumField("default_encoding",
			"DELTA_LENGTH_BYTE_ARRAY", "PLAIN", "RLE_DICTIONARY",
		).Description("The default encoding type to use for fields.").Default("DELTA_LENGTH_BYTE_ARRAY").Advanced()).
		Example("Archiving JSON rows to S3 as daily Parquet",
			"Batches rows at the output and uploads one Parquet file per exchange and UTC day of a microsecond timestamp.",
			`
output:
  aws_s3:
    bucket: metrics
    path: 'ohlcv/${! meta("partition") }/${! uuid_v4() }.parquet'
    batching:
      byte_size: 10485760
      period: 30s
      processors:
        - json_parquet_encode:
            default_compression: zstd
            nan_for_null: true
            schema:
              - { name: exchange, type: UTF8 }
              - { name: symbol, type: UTF8 }
              - { name: time, type: INT64 }
              - { name: close, type: DOUBLE }
              - { name: volume, type: DOUBLE, optional: true }
            columns:
              - { name: exchange, value: '${! @kafka_topic.split(".").index(2) }' }
            partition:
              path: 'exchange={exchange}/{time|year=2006/month=01/day=02}'
              time_unit: us
`)
}

func init() {
	err := service.RegisterBatchProcessor("json_parquet_encode", jsonParquetEncodeSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return newJSONParquetEncoder(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type jpeKind int

const (
	jpeUTF8 jpeKind = iota
	jpeInt32
	jpeInt64
	jpeFloat
	jpeDouble
)

type jpeColumn struct {
	name     string
	kind     jpeKind
	optional bool
	leaf     int                         // index of the Parquet leaf column
	interp   *service.InterpolatedString // set when the column comes from metadata
}

// jpePart is one piece of a partition path: literal text, or a column formatted
// as Bloblang's format prints it (layout empty) or as a time (layout set).
type jpePart struct {
	literal string
	column  int
	layout  string
}

type jsonParquetEncoder struct {
	log      *service.Logger
	mDropped *service.MetricCounter

	schema  *parquet.Schema
	codec   parquet.WriterOption
	columns []jpeColumn
	byName  map[string]int
	nanNull bool

	partition     []jpePart
	divisor       float64
	partitionMeta string
}

func newJSONParquetEncoder(conf *service.ParsedConfig, mgr *service.Resources) (*jsonParquetEncoder, error) {
	e := &jsonParquetEncoder{
		log:      mgr.Logger(),
		mDropped: mgr.Metrics().NewCounter("json_parquet_encode_dropped"),
		byName:   map[string]int{},
	}

	fields, err := conf.FieldObjectList("schema")
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, errors.New("at least one schema column is required")
	}
	for _, f := range fields {
		name, err := f.FieldString("name")
		if err != nil {
			return nil, err
		}
		if children, err := f.FieldAnyList("fields"); err == nil && len(children) > 0 {
			return nil, fmt.Errorf("column '%v': only flat columns are supported", name)
		}
		typ, err := f.FieldString("type")
		if err != nil {
			return nil, fmt.Errorf("column '%v': %w", name, err)
		}
		kind, ok := map[string]jpeKind{"UTF8": jpeUTF8, "INT32": jpeInt32, "INT64": jpeInt64, "FLOAT": jpeFloat, "DOUBLE": jpeDouble}[typ]
		if !ok {
			return nil, fmt.Errorf("column '%v': type %v is not supported, only UTF8, INT32, INT64, FLOAT and DOUBLE", name, typ)
		}
		if repeated, _ := f.FieldBool("repeated"); repeated {
			return nil, fmt.Errorf("column '%v': repeated columns are not supported", name)
		}
		optional, err := f.FieldBool("optional")
		if err != nil {
			return nil, err
		}
		if _, exists := e.byName[name]; exists {
			return nil, fmt.Errorf("column '%v' is defined twice", name)
		}
		e.byName[name] = len(e.columns)
		e.columns = append(e.columns, jpeColumn{name: name, kind: kind, optional: optional})
	}

	overrides, err := conf.FieldObjectList(jpeFieldColumns)
	if err != nil {
		return nil, err
	}
	for _, o := range overrides {
		name, err := o.FieldString(jpeFieldColumnName)
		if err != nil {
			return nil, err
		}
		i, ok := e.byName[name]
		if !ok {
			return nil, fmt.Errorf("columns: '%v' is not a schema column", name)
		}
		if e.columns[i].kind != jpeUTF8 {
			return nil, fmt.Errorf("columns: '%v' must be a UTF8 column to take an interpolated value", name)
		}
		if e.columns[i].interp, err = o.FieldInterpolatedString(jpeFieldColumnValue); err != nil {
			return nil, err
		}
	}
	if e.nanNull, err = conf.FieldBool(jpeFieldNaNForNull); err != nil {
		return nil, err
	}

	// The schema and writer options are parquet_encode's, built by the same code,
	// so the files cannot drift from what it writes.
	encodingStr, err := conf.FieldString("default_encoding")
	if err != nil {
		return nil, err
	}
	encodingTag, ok := map[string]string{
		parquet.Plain.String(): "plain", parquet.DeltaLengthByteArray.String(): "delta", parquet.RLEDictionary.String(): "dict",
	}[encodingStr]
	if !ok {
		return nil, fmt.Errorf("default_encoding type %v not recognised", encodingStr)
	}
	schemaType, err := GenerateStructType(conf, schemaOpts{optionalsAsStructTags: true, defaultEncoding: encodingTag})
	if err != nil {
		return nil, fmt.Errorf("failed to generate struct type from parquet schema: %w", err)
	}
	e.schema = parquet.SchemaOf(reflect.New(schemaType).Interface())
	for leaf, path := range e.schema.Columns() {
		if len(path) != 1 {
			return nil, fmt.Errorf("unexpected nested column %v", path)
		}
		i, ok := e.byName[path[0]]
		if !ok {
			return nil, fmt.Errorf("schema column %v has no definition", path[0])
		}
		e.columns[i].leaf = leaf
	}

	compressStr, err := conf.FieldString("default_compression")
	if err != nil {
		return nil, err
	}
	codec, ok := map[string]parquet.WriterOption{
		"uncompressed": parquet.Compression(&parquet.Uncompressed), "snappy": parquet.Compression(&parquet.Snappy),
		"gzip": parquet.Compression(&parquet.Gzip), "brotli": parquet.Compression(&parquet.Brotli),
		"zstd": parquet.Compression(&parquet.Zstd), "lz4raw": parquet.Compression(&parquet.Lz4Raw),
	}[compressStr]
	if !ok {
		return nil, fmt.Errorf("default_compression type %v not recognised", compressStr)
	}
	e.codec = codec

	if conf.Contains(jpeFieldPartition) {
		pc := conf.Namespace(jpeFieldPartition)
		path, err := pc.FieldString(jpeFieldPartitionPath)
		if err != nil {
			return nil, err
		}
		if e.partition, err = e.parsePartitionPath(path); err != nil {
			return nil, fmt.Errorf("partition: %w", err)
		}
		unit, err := pc.FieldString(jpeFieldPartitionUnit)
		if err != nil {
			return nil, err
		}
		divisor, ok := map[string]float64{"s": 1, "ms": 1e3, "us": 1e6, "ns": 1e9}[unit]
		if !ok {
			return nil, fmt.Errorf("partition: unknown time_unit '%v'", unit)
		}
		e.divisor = divisor
		if e.partitionMeta, err = pc.FieldString(jpeFieldPartitionMetaKey); err != nil {
			return nil, err
		}
	}
	return e, nil
}

func (e *jsonParquetEncoder) parsePartitionPath(path string) ([]jpePart, error) {
	var parts []jpePart
	for len(path) > 0 {
		open := strings.IndexByte(path, '{')
		if open < 0 {
			parts = append(parts, jpePart{literal: path, column: -1})
			break
		}
		if open > 0 {
			parts = append(parts, jpePart{literal: path[:open], column: -1})
		}
		end := strings.IndexByte(path[open:], '}')
		if end < 0 {
			return nil, fmt.Errorf("unclosed '{' in %q", path)
		}
		name, layout, isTime := strings.Cut(path[open+1:open+end], "|")
		i, ok := e.byName[name]
		if !ok {
			return nil, fmt.Errorf("placeholder {%v} is not a schema column", name)
		}
		if isTime && layout == "" {
			return nil, fmt.Errorf("placeholder {%v|}: empty time layout", name)
		}
		if isTime && e.columns[i].interp != nil {
			return nil, fmt.Errorf("placeholder {%v|%v}: an interpolated column cannot be formatted as a time", name, layout)
		}
		parts = append(parts, jpePart{column: i, layout: layout})
		path = path[open+end+1:]
	}
	return parts, nil
}

//------------------------------------------------------------------------------

// jpeRow holds the fields of one message the schema reads, as the Go values
// Bento's own JSON decoding gives them (string, json.Number, bool, nil, or a
// map or slice for a nested value), so coercion runs the very functions
// Bloblang's methods do.
type jpeRow struct {
	present []bool
	values  []any
}

// jpeDecodeOptions make jsontext read a document as encoding/json does, and so
// as Bento's decoding of every message: the last of a duplicated name wins and
// invalid UTF-8 is mangled into U+FFFD.
var jpeDecodeOptions = []jsontext.Options{
	jsontext.AllowDuplicateNames(true),
	jsontext.AllowInvalidUTF8(true),
}

// parse reads one message into r. It fails where Bento's own decoding of the
// message would: anything but exactly one JSON document. A document that is not
// an object fails too, which only drops what the coercion would have dropped
// anyway, since a schema read from the message has at least one column.
func (e *jsonParquetEncoder) parse(dec *jsontext.Decoder, src *bytes.Reader, raw []byte, r *jpeRow) error {
	clear(r.present)
	clear(r.values)
	src.Reset(raw)
	dec.Reset(src, jpeDecodeOptions...)

	tok, err := dec.ReadToken()
	if err != nil {
		return fmt.Errorf("not JSON: %w", err)
	}
	if tok.Kind() != '{' {
		return fmt.Errorf("not a JSON object but %v", tok.Kind())
	}
	for dec.PeekKind() != '}' {
		name, err := dec.ReadToken()
		if err != nil {
			return fmt.Errorf("not JSON: %w", err)
		}
		i, ok := e.byName[name.String()]
		if !ok || e.columns[i].interp != nil {
			if err := dec.SkipValue(); err != nil {
				return fmt.Errorf("not JSON: %w", err)
			}
			continue
		}
		if r.values[i], err = jpeGoValue(dec); err != nil {
			return fmt.Errorf("not JSON: %w", err)
		}
		r.present[i] = true
	}
	if _, err := dec.ReadToken(); err != nil { // the closing '}'
		return fmt.Errorf("not JSON: %w", err)
	}
	if _, err := dec.ReadToken(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("message contains multiple valid documents")
		}
		return fmt.Errorf("not JSON: %w", err)
	}
	return nil
}

// jpeGoValue reads the next JSON value as Bento's decoding gives it.
func jpeGoValue(dec *jsontext.Decoder) (any, error) {
	switch dec.PeekKind() {
	case '{', '[':
		v, err := dec.ReadValue()
		if err != nil {
			return nil, err
		}
		nested := json.NewDecoder(bytes.NewReader(v))
		nested.UseNumber()
		var out any
		err = nested.Decode(&out)
		return out, err
	}
	tok, err := dec.ReadToken()
	if err != nil {
		return nil, err
	}
	switch tok.Kind() {
	case '"':
		return tok.String(), nil
	case '0':
		return json.Number(tok.String()), nil
	case 't':
		return true, nil
	case 'f':
		return false, nil
	}
	return nil, nil
}

// value coerces column i of r, as the column's Bloblang coercion would, into
// the Parquet value of its leaf.
func (e *jsonParquetEncoder) value(c *jpeColumn, present bool, v any) (parquet.Value, error) {
	def := 0
	if c.optional {
		def = 1
	}
	if !present || v == nil {
		switch {
		case c.optional:
			return parquet.NullValue().Level(0, 0, c.leaf), nil
		case !present:
			return parquet.Value{}, errors.New("missing")
		case e.nanNull && (c.kind == jpeFloat || c.kind == jpeDouble):
			v = math.NaN()
		default:
			return parquet.Value{}, errors.New("value is null")
		}
	}
	var pv parquet.Value
	switch c.kind {
	case jpeUTF8:
		pv = parquet.ByteArrayValue([]byte(jpeString(v)))
	case jpeInt32:
		i, err := value.IToInt32(v)
		if err != nil {
			return parquet.Value{}, err
		}
		pv = parquet.Int32Value(i)
	case jpeInt64:
		i, err := value.IToInt(v)
		if err != nil {
			return parquet.Value{}, err
		}
		pv = parquet.Int64Value(i)
	case jpeFloat:
		f, err := value.IToNumber(v)
		if err != nil {
			return parquet.Value{}, err
		}
		pv = parquet.FloatValue(float32(f))
	case jpeDouble:
		f, err := value.IToNumber(v)
		if err != nil {
			return parquet.Value{}, err
		}
		pv = parquet.DoubleValue(f)
	}
	return pv.Level(0, def, c.leaf), nil
}

// jpeString is Bloblang's .string(), whose objects are marshalled by gabs.
func jpeString(v any) string {
	return value.IToString(v)
}

// appendPartitionPath appends the partition path of a row to b, as the
// Bloblang "...".format(this.a, ...) and
// (this.t / divisor).ts_format(layout, "UTC") it replaces would.
func (e *jsonParquetEncoder) appendPartitionPath(b []byte, r *jpeRow, interpolated []string) ([]byte, error) {
	for _, p := range e.partition {
		switch {
		case p.column < 0:
			b = append(b, p.literal...)
		case e.columns[p.column].interp != nil:
			b = append(b, interpolated[p.column]...)
		case p.layout == "":
			b = fmt.Appendf(b, "%s", r.values[p.column])
		default:
			n, err := value.IGetNumber(r.values[p.column])
			if err != nil {
				return b, fmt.Errorf("partition: %v: %w", e.columns[p.column].name, err)
			}
			t, err := value.IGetTimestamp(n / e.divisor)
			if err != nil {
				return b, fmt.Errorf("partition: %v: %w", e.columns[p.column].name, err)
			}
			b = t.UTC().AppendFormat(b, p.layout)
		}
	}
	return b, nil
}

//------------------------------------------------------------------------------

// jpeGroup is one output file: the rows of one partition path, held until the
// batch is read, and then encoded while no other group's file is.
type jpeGroup struct {
	first *service.Message
	key   string
	rows  []parquet.Row
}

func (e *jsonParquetEncoder) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	if len(batch) == 0 {
		return nil, nil
	}
	var (
		groups       []*jpeGroup
		byKey        = map[string]*jpeGroup{}
		current      *jpeGroup
		src          bytes.Reader
		dec          = jsontext.NewDecoder(&src, jpeDecodeOptions...)
		row          = &jpeRow{present: make([]bool, len(e.columns)), values: make([]any, len(e.columns))}
		interpolated = make([]string, len(e.columns))
		key          []byte
		dropped      int
	)
	drop := func(err error) {
		dropped++
		e.log.Errorf("json_parquet_encode: dropping a message that is not a valid row: %v", err)
	}
	for _, msg := range batch {
		raw, err := msg.AsBytes()
		if err == nil && len(raw) == 0 {
			err = errors.New("empty message")
		}
		if err == nil {
			err = e.parse(dec, &src, raw, row)
		}
		if err == nil {
			err = e.interpolate(msg, interpolated)
		}
		if err != nil {
			drop(err)
			continue
		}
		values := make(parquet.Row, len(e.columns))
		for i := range e.columns {
			c := &e.columns[i]
			if c.interp != nil {
				values[c.leaf] = parquet.ByteArrayValue([]byte(interpolated[i])).Level(0, 0, c.leaf)
				continue
			}
			if values[c.leaf], err = e.value(c, row.present[i], row.values[i]); err != nil {
				err = fmt.Errorf("%v: %w", c.name, err)
				break
			}
		}
		if err != nil {
			drop(err)
			continue
		}
		if e.partition != nil {
			if key, err = e.appendPartitionPath(key[:0], row, interpolated); err != nil {
				drop(err)
				continue
			}
		}
		// The key is only copied into a string for a new partition: the
		// comparison and lookup of string(key) do not allocate.
		if current == nil || current.key != string(key) {
			if current = byKey[string(key)]; current == nil {
				current = &jpeGroup{first: msg, key: string(key)}
				byKey[current.key] = current
				groups = append(groups, current)
			}
		}
		current.rows = append(current.rows, values)
	}
	if dropped > 0 {
		e.mDropped.Incr(int64(dropped))
	}
	if len(groups) == 0 {
		return nil, nil
	}

	// Files are encoded one at a time, as parquet_encode encodes each group
	// group_by_value hands it: a writer's column buffers are the bulk of what
	// encoding holds, so a batch of many partitions must not hold one per file.
	// Each file takes a new writer, since a reset one does not write the same
	// bytes as a new one.
	out := make(service.MessageBatch, 0, len(groups))
	for _, g := range groups {
		var buf bytes.Buffer
		if err := jpeEncode(parquet.NewGenericWriter[any](&buf, e.schema, e.codec, jpeNoWriteBuffer), g.rows); err != nil {
			return nil, err
		}
		g.rows = nil
		m := g.first.Copy()
		m.SetBytes(buf.Bytes())
		if e.partition != nil {
			m.MetaSetMut(e.partitionMeta, g.key)
		}
		out = append(out, m)
	}
	return []service.MessageBatch{out}, nil
}

// interpolate resolves the interpolated columns of a message.
func (e *jsonParquetEncoder) interpolate(msg *service.Message, interpolated []string) error {
	for i, c := range e.columns {
		if c.interp == nil {
			continue
		}
		s, err := c.interp.TryString(msg)
		if err != nil {
			return fmt.Errorf("%v: %w", c.name, err)
		}
		interpolated[i] = s
	}
	return nil
}

// jpeNoWriteBuffer drops the writer's buffering of its output, which is a
// bytes.Buffer already: 32 KiB a file saved, the same bytes written.
var jpeNoWriteBuffer = parquet.WriteBufferSize(0)

// jpeEncode writes rows as one whole file.
func jpeEncode(w *parquet.GenericWriter[any], rows []parquet.Row) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("encoding panic: %v", r)
		}
	}()
	if _, err = w.WriteRows(rows); err != nil {
		return err
	}
	return w.Close()
}

func (e *jsonParquetEncoder) Close(ctx context.Context) error {
	return nil
}
