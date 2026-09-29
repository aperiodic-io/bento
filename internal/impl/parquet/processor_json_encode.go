package parquet

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"
	"unsafe"

	"github.com/parquet-go/parquet-go"

	"github.com/warpstreamlabs/bento/internal/bloblang/query"
	"github.com/warpstreamlabs/bento/internal/component/interop"
	"github.com/warpstreamlabs/bento/internal/value"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	jpeFieldColumns          = "columns"
	jpeFieldColumnName       = "name"
	jpeFieldColumnValue      = "value"
	jpeFieldColumnCacheBy    = "cache_by"
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

When `+"`partition`"+` is set the batch is split into one Parquet file per distinct partition path, in order of first appearance, and the path is written to the configured metadata key. `+"`{column}`"+` in the path is replaced by the column's value as Bloblang's `+"`format`"+` would print the field, and `+"`{column|layout}`"+` by the column's number, read in `+"`time_unit`"+`, as a UTC time in the Go layout. Every output message carries the metadata of the first message of its partition, and the files written are counted in `+"`json_parquet_encode_files`"+`.

Every file costs something of its own: a footer, pages begun for each column, and, when a text column holds a single value and it is empty, or the file is large and holds an empty string, a writer of its own. Encoding a batch as hundreds of small files takes about a third more time and half as much memory again as encoding it as a couple of dozen, and leaves many small objects to store and query. Prefer a coarse partition path, and batches closed by size rather than time where the rate allows.`).
		Field(parquetSchemaConfig()).
		Field(service.NewObjectListField(jpeFieldColumns,
			service.NewStringField(jpeFieldColumnName).Description("The schema column this sets."),
			service.NewInterpolatedStringField(jpeFieldColumnValue).Description("The column's value, from the message's metadata. It must not reference the message content."),
			service.NewStringListField(jpeFieldColumnCacheBy).Description("The metadata fields the value depends on. When set, the value is evaluated once per batch for each distinct combination of these fields' values, rather than for every message, and messages that share them share it: the value must depend on nothing else. A value found to read a message field, the whole of the metadata, or a metadata field not listed is rejected, but not everything can be found: what a function or method called with a non-literal argument reads, what a lambda reads, what a function that reads the message without naming a field reads (`content()`), and a function that differs from call to call (`now()`, `uuid_v4()`, `count()`). A message whose listed field holds anything but a string is evaluated on its own.").Example([]string{"kafka_topic"}).Default([]any{}),
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
              - { name: exchange, value: '${! @kafka_topic.split(".").index(2) }', cache_by: [ kafka_topic ] }
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
	// cacheBy, when set, are the metadata fields interp depends on: it is
	// evaluated once per batch per combination of their values
	cacheBy []string
}

// definitionLevel is the definition level of a value in the column: 1 for an
// optional one, 0 for a required one or a null.
func (c *jpeColumn) definitionLevel() int {
	if c.optional {
		return 1
	}
	return 0
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
	mFiles   *service.MetricCounter

	schema  *parquet.Schema
	codec   parquet.WriterOption
	columns []jpeColumn
	byName  map[string]int
	nanNull bool

	partition     []jpePart
	divisor       float64
	partitionMeta string

	utf8Leaves []int     // the Parquet leaves of the UTF8 columns
	writers    sync.Pool // of *parquet.GenericWriter[any], reset for each file

	// newWriters, for tests, gives every file a new writer, and reused counts
	// the files that were given a reset one
	newWriters bool
	reused     atomic.Int64
}

func newJSONParquetEncoder(conf *service.ParsedConfig, mgr *service.Resources) (*jsonParquetEncoder, error) {
	e := &jsonParquetEncoder{
		log:      mgr.Logger(),
		mDropped: mgr.Metrics().NewCounter("json_parquet_encode_dropped"),
		mFiles:   mgr.Metrics().NewCounter("json_parquet_encode_files"),
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
		if e.columns[i].cacheBy, err = o.FieldStringList(jpeFieldColumnCacheBy); err != nil {
			return nil, err
		}
		if len(e.columns[i].cacheBy) > 0 {
			raw, err := o.FieldString(jpeFieldColumnValue)
			if err != nil {
				return nil, err
			}
			if err := jpeCheckCacheBy(mgr, raw, e.columns[i].cacheBy); err != nil {
				return nil, fmt.Errorf("columns: '%v': %w", name, err)
			}
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
		if e.columns[i].kind == jpeUTF8 {
			e.utf8Leaves = append(e.utf8Leaves, leaf)
		}
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

// jpeRow holds the fields of one message the schema reads, each as the kind
// of its JSON value and its content, copied into buf: a string's decoded text,
// a number's literal, or a nested value's raw JSON. Coercion parses the content
// as the functions Bloblang's methods do would parse the Go value Bento's own
// JSON decoding gives the field (string, json.Number, bool, nil, or a map or
// slice), and hands them that value itself where it is not a string or number.
type jpeRow struct {
	present []bool
	kind    []byte // '"', '0', 't', 'f', 'n', '{' or '['
	start   []int
	end     []int
	buf     []byte
	unquote []byte
}

func newJPERow(columns int) *jpeRow {
	return &jpeRow{
		present: make([]bool, columns),
		kind:    make([]byte, columns),
		start:   make([]int, columns),
		end:     make([]int, columns),
	}
}

// content is the content of field i, valid until the next message is parsed.
func (r *jpeRow) content(i int) []byte {
	return r.buf[r.start[i]:r.end[i]]
}

// goValue is field i as Bento's JSON decoding gives it.
func (r *jpeRow) goValue(i int) (any, error) {
	c := r.content(i)
	switch r.kind[i] {
	case '"':
		return string(c), nil
	case '0':
		return json.Number(c), nil
	case 't':
		return true, nil
	case 'f':
		return false, nil
	case 'n':
		return nil, nil
	}
	nested := json.NewDecoder(bytes.NewReader(c))
	nested.UseNumber()
	var out any
	err := nested.Decode(&out)
	return out, err
}

// jpeDecodeOptions make jsontext read a document as encoding/json does, and so
// as Bento's decoding of every message: the last of a duplicated name wins and
// invalid UTF-8 is mangled into U+FFFD.
var jpeDecodeOptions = []jsontext.Options{
	jsontext.AllowDuplicateNames(true),
	jsontext.AllowInvalidUTF8(true),
}

// unquote returns the text of a JSON string as jsontext.Token.String would: the
// literal's own bytes when it has no escape and is valid UTF-8, and its decoding,
// with invalid UTF-8 mangled into U+FFFD, otherwise. The result is only valid
// until the next call.
func (r *jpeRow) unquoteString(lit []byte) []byte {
	inner := lit[1 : len(lit)-1]
	if bytes.IndexByte(inner, '\\') < 0 && utf8.Valid(inner) {
		return inner
	}
	// The error only reports invalid UTF-8, which is mangled as it should be.
	r.unquote, _ = jsontext.AppendUnquote(r.unquote[:0], lit)
	return r.unquote
}

// parse reads one message into r. It fails where Bento's own decoding of the
// message would: anything but exactly one JSON document. A document that is not
// an object fails too, which only drops what the coercion would have dropped
// anyway, since a schema read from the message has at least one column.
func (e *jsonParquetEncoder) parse(dec *jsontext.Decoder, src *bytes.Reader, raw []byte, r *jpeRow) error {
	clear(r.present)
	for i := range r.kind { // an absent field reads as null, as nil did
		r.kind[i] = 'n'
		r.start[i], r.end[i] = 0, 0
	}
	r.buf = r.buf[:0]
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
		name, err := dec.ReadValue()
		if err != nil {
			return fmt.Errorf("not JSON: %w", err)
		}
		i, ok := e.byName[string(r.unquoteString(name))]
		if !ok || e.columns[i].interp != nil {
			if err := dec.SkipValue(); err != nil {
				return fmt.Errorf("not JSON: %w", err)
			}
			continue
		}
		v, err := dec.ReadValue()
		if err != nil {
			return fmt.Errorf("not JSON: %w", err)
		}
		kind := v[0]
		switch kind {
		case '"':
			v = r.unquoteString(v)
		case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			kind = '0'
		}
		r.kind[i] = kind
		r.start[i] = len(r.buf)
		r.buf = append(r.buf, v...)
		r.end[i] = len(r.buf)
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

// jpeUnsafeString views b as a string for a call that neither keeps nor
// returns it (strconv copies the input into the errors it returns).
func jpeUnsafeString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// value coerces field i of r, as the column's Bloblang coercion would, into the
// Parquet value of the column's leaf. A string or number is parsed by the very
// strconv call the coercion would make of it (json.Number's Int64 and Float64
// parse in base 10, a string's integer in any base Go literals use); anything
// else is handed to the coercion itself.
func (e *jsonParquetEncoder) value(a *jpeArena, c *jpeColumn, r *jpeRow, i int) (parquet.Value, error) {
	def := c.definitionLevel()
	present, kind := r.present[i], r.kind[i]
	var v any // set for the coercion when the field is neither a string nor a number
	if !present || kind == 'n' {
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
	} else if kind != '"' && kind != '0' {
		var err error
		if v, err = r.goValue(i); err != nil {
			return parquet.Value{}, err
		}
	}
	content := jpeUnsafeString(r.content(i))
	var pv parquet.Value
	switch c.kind {
	case jpeUTF8:
		if v != nil {
			pv = parquet.ByteArrayValue(a.copyString(value.IToString(v)))
		} else {
			pv = parquet.ByteArrayValue(a.copy(r.content(i)))
		}
	case jpeInt32, jpeInt64:
		var n int64
		var err error
		switch {
		case v != nil:
			n, err = value.IToInt(v)
		case kind == '0':
			n, err = strconv.ParseInt(content, 10, 64)
		default:
			n, err = strconv.ParseInt(content, 0, 64)
		}
		if err != nil {
			return parquet.Value{}, err
		}
		if c.kind == jpeInt64 {
			pv = parquet.Int64Value(n)
			break
		}
		// value.IToInt32's bounds
		if n > math.MaxInt32 {
			return parquet.Value{}, errors.New("value is too large to be cast as a 32-bit signed integer")
		}
		if n < math.MinInt32 {
			return parquet.Value{}, errors.New("value is too small to be cast as a 32-bit signed integer")
		}
		pv = parquet.Int32Value(int32(n))
	case jpeFloat, jpeDouble:
		var f float64
		var err error
		if v != nil {
			f, err = value.IToNumber(v)
		} else {
			f, err = strconv.ParseFloat(content, 64)
		}
		if err != nil {
			return parquet.Value{}, err
		}
		if c.kind == jpeFloat {
			pv = parquet.FloatValue(float32(f))
		} else {
			pv = parquet.DoubleValue(f)
		}
	}
	return pv.Level(0, def, c.leaf), nil
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
		case p.layout == "" && (r.kind[p.column] == '"' || r.kind[p.column] == '0'):
			// "%s" prints a string or json.Number as its text
			b = append(b, r.content(p.column)...)
		case p.layout == "":
			v, err := r.goValue(p.column)
			if err != nil {
				return b, fmt.Errorf("partition: %v: %w", e.columns[p.column].name, err)
			}
			b = fmt.Appendf(b, "%s", v)
		default:
			var n float64
			var err error
			if r.kind[p.column] == '0' {
				// json.Number's Float64
				n, err = strconv.ParseFloat(jpeUnsafeString(r.content(p.column)), 64)
			} else {
				var v any
				if v, err = r.goValue(p.column); err == nil {
					n, err = value.IGetNumber(v)
				}
			}
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

// jpeArena holds a group's rows until they are encoded: their values, and the
// bytes of their UTF8 values, in chunks that double from one row and 64 bytes
// up to jpeArenaRows rows and jpeArenaBytes bytes, so that a partition of a
// few rows holds little more than they take, rather than an allocation for
// every row and every string. A group's chunks are dropped with it once its
// file is written, so the rows of a batch shrink as its files are encoded.
type jpeArena struct {
	values    []parquet.Value
	bytes     []byte
	nextRows  int
	nextBytes int
}

const (
	jpeArenaRows  = 256
	jpeArenaBytes = 16 << 10
)

// row returns an n-value row.
func (a *jpeArena) row(n int) parquet.Row {
	if cap(a.values)-len(a.values) < n {
		a.nextRows = min(max(2*a.nextRows, 1), jpeArenaRows)
		a.values = make([]parquet.Value, 0, a.nextRows*n)
	}
	start := len(a.values)
	a.values = a.values[:start+n]
	return a.values[start : start+n : start+n]
}

// copy returns a copy of b. A value larger than a quarter chunk is copied on
// its own, so that it does not waste the rest of one.
func (a *jpeArena) copy(b []byte) []byte {
	return jpeArenaCopy(a, b)
}

func (a *jpeArena) copyString(s string) []byte {
	return jpeArenaCopy(a, s)
}

func jpeArenaCopy[T string | []byte](a *jpeArena, b T) []byte {
	if len(b) > jpeArenaBytes/4 {
		// append, since []byte(b) of a []byte is b itself, not a copy
		return append([]byte(nil), b...)
	}
	if cap(a.bytes)-len(a.bytes) < len(b) {
		a.nextBytes = min(max(2*a.nextBytes, 64), jpeArenaBytes)
		for a.nextBytes < jpeArenaBytes && a.nextBytes < 4*len(b) {
			a.nextBytes *= 2
		}
		a.bytes = make([]byte, 0, a.nextBytes)
	}
	start := len(a.bytes)
	a.bytes = append(a.bytes, b...)
	return a.bytes[start:len(a.bytes):len(a.bytes)]
}

// jpeArenaMark is where an arena's chunks stood before a row was taken.
type jpeArenaMark struct {
	values *parquet.Value
	nv     int
	bytes  *byte
	nb     int
}

func (a *jpeArena) mark() jpeArenaMark {
	return jpeArenaMark{unsafe.SliceData(a.values), len(a.values), unsafe.SliceData(a.bytes), len(a.bytes)}
}

// release gives back what was taken since m, of the chunks still current: a
// dropped row's values and bytes.
func (a *jpeArena) release(m jpeArenaMark) {
	if unsafe.SliceData(a.values) == m.values {
		clear(a.values[m.nv:])
		a.values = a.values[:m.nv]
	}
	if unsafe.SliceData(a.bytes) == m.bytes {
		a.bytes = a.bytes[:m.nb]
	}
}

// jpeGroup is one output file: the rows of one partition path, held until the
// batch is read, and then encoded while no other group's file is.
type jpeGroup struct {
	first *service.Message
	key   string
	rows  []parquet.Row
	arena jpeArena
	// what decides whether a reset writer would write the group's file as a
	// new one would (see needsNewWriter): for each UTF8 column, by its index in
	// utf8Leaves, how many of its values are not null and how many are empty;
	// and the bytes of the group's UTF8 values
	nonNull, empty []int
	textBytes      int
}

// jpeValueBytes bounds what a value other than a string's bytes adds to a
// column buffer's size: 8 bytes of value or 4 of length, and a level.
const jpeValueBytes = 16

// needsNewWriter reports whether a reset writer could write the group's file
// otherwise than a new one. In parquet-go 0.29.0 a byte array column buffer's
// Reset keeps the scratch buffer it swaps in to write a page of one value, so
// the bounds of such a page, when its value is empty, point into memory a new
// writer does not have: a new writer leaves them out, a reset one writes them
// as empty. That is so of a column with one non-null value in the file, which
// is empty. A column that spans pages (one is cut at
// parquet.DefaultPageBufferSize bytes) can leave one value for its last page;
// the tests find no difference then, having written a byte in an earlier
// page, but a file with an empty string that could span pages takes a new
// writer too, which costs little beside a file that large.
func (e *jsonParquetEncoder) needsNewWriter(g *jpeGroup) bool {
	anyEmpty := false
	for k := range e.utf8Leaves {
		if g.empty[k] == 0 {
			continue
		}
		if g.nonNull[k] == 1 {
			return true
		}
		anyEmpty = true
	}
	return anyEmpty && len(g.rows)*jpeValueBytes+g.textBytes >= parquet.DefaultPageBufferSize
}

func (e *jsonParquetEncoder) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	if len(batch) == 0 {
		return nil, nil
	}
	var (
		groups        []*jpeGroup
		byKey         = map[string]*jpeGroup{}
		current       *jpeGroup
		src           bytes.Reader
		dec           = jsontext.NewDecoder(&src, jpeDecodeOptions...)
		row           = newJPERow(len(e.columns))
		interpolated  = make([]string, len(e.columns))
		interpolation = e.newInterpolation()
		key           []byte
		dropped       int
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
			err = e.interpolate(interpolation, msg, interpolated)
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
		// comparison and lookup of string(key) do not allocate. A new group is
		// only kept once a row of it is.
		g, isNew := current, false
		if g == nil || g.key != string(key) {
			if g = byKey[string(key)]; g == nil {
				g, isNew = &jpeGroup{first: msg, key: string(key)}, true
			}
		}
		mark := g.arena.mark()
		values := g.arena.row(len(e.columns))
		for i := range e.columns {
			c := &e.columns[i]
			if c.interp != nil {
				values[c.leaf] = parquet.ByteArrayValue(g.arena.copyString(interpolated[i])).Level(0, c.definitionLevel(), c.leaf)
				continue
			}
			if values[c.leaf], err = e.value(&g.arena, c, row, i); err != nil {
				err = fmt.Errorf("%v: %w", c.name, err)
				break
			}
		}
		if err != nil {
			g.arena.release(mark)
			drop(err)
			continue
		}
		if isNew {
			g.nonNull, g.empty = make([]int, len(e.utf8Leaves)), make([]int, len(e.utf8Leaves))
		}
		for k, leaf := range e.utf8Leaves {
			if v := values[leaf]; !v.IsNull() {
				g.nonNull[k]++
				if n := len(v.ByteArray()); n == 0 {
					g.empty[k]++
				} else {
					g.textBytes += n
				}
			}
		}
		if isNew {
			byKey[g.key] = g
			groups = append(groups, g)
		}
		current = g
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
	//
	// A writer is reset for the next file, and pooled, rather than made anew:
	// making one is a third of what a batch of many small files allocates. A
	// reset writer writes the bytes a new one would, save for a page of one
	// empty string (see needsNewWriter), so a file that could have one takes a
	// new writer.
	out := make(service.MessageBatch, 0, len(groups))
	for _, g := range groups {
		var buf bytes.Buffer
		w, _ := e.writers.Get().(*parquet.GenericWriter[any])
		if w == nil || e.newWriters || e.needsNewWriter(g) {
			w = parquet.NewGenericWriter[any](&buf, e.schema, e.codec, jpeNoWriteBuffer)
		} else {
			w.Reset(&buf)
			e.reused.Add(1)
		}
		if err := jpeEncode(w, g.rows); err != nil {
			return nil, err
		}
		w.Reset(nil) // not to keep the file written reachable from the pool
		e.writers.Put(w)
		g.rows, g.arena = nil, jpeArena{}
		m := g.first.Copy()
		m.SetBytes(buf.Bytes())
		if e.partition != nil {
			m.MetaSetMut(e.partitionMeta, g.key)
		}
		out = append(out, m)
	}
	e.mFiles.Incr(int64(len(out)))
	return []service.MessageBatch{out}, nil
}

// jpeCheckCacheBy fails if the interpolation raw is found to read anything but
// the metadata fields cacheBy: the message, a variable, the whole of the
// metadata or another field. Bloblang does not report everything a query
// reads (see the cache_by field), so this catches mistakes, not all of them.
func jpeCheckCacheBy(res *service.Resources, raw string, cacheBy []string) error {
	expr, err := interop.UnwrapManagement(res).BloblEnvironment().NewField(raw)
	if err != nil {
		return err
	}
	for _, t := range expr.QueryTargets(query.TargetsContext{}) {
		switch {
		case t.Type != query.TargetMetadata:
			return errors.New("cache_by: the value reads more than metadata")
		case len(t.Path) == 0:
			return errors.New("cache_by: the value reads the whole of the metadata")
		case !slices.Contains(cacheBy, t.Path[0]):
			return fmt.Errorf("cache_by: the value reads the metadata field %q, which is not listed", t.Path[0])
		}
	}
	return nil
}

// jpeInterpolation resolves the interpolated columns of a batch's messages,
// the cached ones once per distinct combination of the metadata they read.
type jpeInterpolation struct {
	cache []map[string]string // by column, for the cached ones
	key   []byte
}

func (e *jsonParquetEncoder) newInterpolation() *jpeInterpolation {
	in := &jpeInterpolation{cache: make([]map[string]string, len(e.columns))}
	for i, c := range e.columns {
		if len(c.cacheBy) > 0 {
			in.cache[i] = map[string]string{}
		}
	}
	return in
}

// interpolate resolves the interpolated columns of a message.
func (e *jsonParquetEncoder) interpolate(in *jpeInterpolation, msg *service.Message, interpolated []string) error {
	for i, c := range e.columns {
		if c.interp == nil {
			continue
		}
		cacheable := false
		if len(c.cacheBy) > 0 {
			in.key, cacheable = jpeCacheKey(in.key[:0], msg, c.cacheBy)
			if cacheable {
				if s, ok := in.cache[i][string(in.key)]; ok {
					interpolated[i] = s
					continue
				}
			}
		}
		s, err := c.interp.TryString(msg)
		if err != nil {
			return fmt.Errorf("%v: %w", c.name, err)
		}
		if cacheable {
			in.cache[i][string(in.key)] = s
		}
		interpolated[i] = s
	}
	return nil
}

// jpeCacheKey appends to b the values of the metadata fields keys, each marked
// absent or given with its length. It reports false, and the message is not
// cached, when a field holds anything but a string, since a value of another
// type could print the same as a string yet read differently.
func jpeCacheKey(b []byte, msg *service.Message, keys []string) ([]byte, bool) {
	for _, k := range keys {
		v, ok := msg.MetaGetMut(k)
		if !ok {
			b = append(b, 0)
			continue
		}
		s, isString := v.(string)
		if !isString {
			return b, false
		}
		b = append(b, 1)
		b = binary.AppendUvarint(b, uint64(len(s)))
		b = append(b, s...)
	}
	return b, true
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
