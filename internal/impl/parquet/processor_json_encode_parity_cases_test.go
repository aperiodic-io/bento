package parquet_test

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const parityTopic = "metric.v1.binance-futures.15s"

// parityBase is a valid row of archiveSchema, as JSON fields in order.
var parityBase = [][2]string{
	{"exchange", `11`}, // the row's own exchange id, which the topic's name overrides
	{"symbol", `"perpetual-BTC-USDT:USD"`},
	{"interval", `"15s"`},
	{"timestamp_type", `"true"`},
	{"day", `"2026-09-29"`},
	{"time", `1790677155000000`},
	{"count", `10`},
	{"count_opt", `6`},
	{"ratio", `0.16286439`},
	{"ratio_opt", `1.6666666`},
	{"price", `0.012981182352941168`},
	{"price_opt", `3.1113223111729823e-10`},
}

// rowWith renders parityBase with field set to raw JSON (missing removes it).
func rowWith(field, raw string) []byte {
	var parts []string
	for _, kv := range parityBase {
		switch {
		case kv[0] != field:
			parts = append(parts, fmt.Sprintf("%q:%s", kv[0], kv[1]))
		case raw != missing:
			parts = append(parts, fmt.Sprintf("%q:%s", kv[0], raw))
		}
	}
	return []byte("{" + strings.Join(parts, ",") + "}")
}

const missing = "<missing>"

// valueVariants are the JSON values every column is tried with: every kind,
// every numeric literal form and range edge, and the strings Bloblang parses.
var valueVariants = []string{
	missing, `null`, `true`, `false`,
	`"abc"`, `""`, `"我踏马来了"`, `"é😀"`, `"a\"b\\c\n\t\u0000"`, `"\ud800"`, "\"\xff\xfe\"", `"<&>"`,
	`0`, `-0`, `1`, `-1`, `1.0`, `-0.0`, `1.5`, `-1.5e-3`, `1e2`, `1E+2`, `0.1`, `0.30000000000000004`,
	`2147483647`, `2147483648`, `-2147483648`, `-2147483649`,
	`9223372036854775807`, `9223372036854775808`, `-9223372036854775808`, `-9223372036854775809`, `12345678901234567890`,
	`1e308`, `1.7976931348623157e308`, `1e309`, `1e400`, `-1e400`, `5e-324`, `1e-400`,
	`3.4028234663852886e38`, `3.4028236e38`, `1.401298464324817e-45`, `1e-46`,
	`1790677155000000`, `1790677155999999`, `-1`,
	`"123"`, `"-5"`, `"0x10"`, `"010"`, `"0b101"`, `"1_000"`, `" 1"`, `"1 "`, `"1.5"`, `"1e2"`, `"NaN"`, `"nan"`, `"inf"`, `"-Infinity"`, `"+1"`, `"9223372036854775808"`,
	`{}`, `{"a":1}`, `{"z":"<&>","b":1.50,"a":[true,null]}`, `[]`, `[1,"x",{"k":null}]`,
}

// rowsIn counts the rows of the files, so a case cannot pass with both sides
// writing nothing.
func rowsIn(t testing.TB, files []parityFile) int {
	n := 0
	for _, f := range files {
		n += len(parityRows(t, f.data))
	}
	return n
}

func TestJSONParquetParityEveryColumnEveryValue(t *testing.T) {
	base := requireParity(t, archiveSchema, []parityInput{{topic: parityTopic, body: rowWith("", "")}})
	require.Equal(t, 1, rowsIn(t, base), "the base row must be archived for the variants to mean anything")

	for _, kv := range parityBase {
		field := kv[0]
		kept, dropped := 0, 0
		for _, v := range valueVariants {
			t.Run(field+"="+v, func(t *testing.T) {
				if rowsIn(t, requireParity(t, archiveSchema, []parityInput{{topic: parityTopic, body: rowWith(field, v)}})) == 1 {
					kept++
				} else {
					dropped++
				}
			})
		}
		t.Logf("%-15s kept %2d of %d values, dropped %2d", field, kept, len(valueVariants), dropped)
		if field != "exchange" && field != "day" {
			require.Positive(t, dropped, "%s: no value was rejected, so the variants test nothing of its coercion", field)
		}
		require.Positive(t, kept, "%s: every value was rejected", field)
	}
}

func TestJSONParquetParityMessages(t *testing.T) {
	valid := string(rowWith("", ""))
	for name, body := range map[string]string{
		"empty":                  ``,
		"whitespace":             " \n\t ",
		"not json":               `not json`,
		"truncated":              valid[:len(valid)-1],
		"array":                  `[` + valid + `]`,
		"number":                 `5`,
		"string":                 `"x"`,
		"null":                   `null`,
		"two documents":          valid + valid,
		"two documents spaced":   valid + " " + valid,
		"trailing garbage":       valid + " x",
		"trailing comma":         strings.TrimSuffix(valid, "}") + ",}",
		"surrounding whitespace": "\n  " + valid + " \r\n",
		"bom":                    "\xef\xbb\xbf" + valid,
		"duplicate field":        strings.TrimSuffix(valid, "}") + `,"price":2.5}`,
		"duplicate null field":   strings.TrimSuffix(valid, "}") + `,"price":null}`,
		"duplicate required":     `{"symbol":null,` + valid[1:],
		"extra fields":           strings.TrimSuffix(valid, "}") + `,"extra":{"deep":[1,2,{"x":"y"}]},"more":"é"}`,
		"invalid utf8 in extra":  strings.TrimSuffix(valid, "}") + ",\"extra\":\"\xff\"}",
		"invalid utf8 in name":   strings.TrimSuffix(valid, "}") + ",\"\xff\":1}",
		"invalid json in extra":  strings.TrimSuffix(valid, "}") + `,"extra":[1,}`,
		"escaped field name":     strings.Replace(valid, `"symbol"`, `"\u0073ymbol"`, 1),
		"escaped names, values":  strings.Replace(strings.Replace(valid, `"symbol":"perpetual-BTC-USDT:USD"`, `"\u0073ymbol":"a\"b\u00e9"`, 1), `"time"`, `"t\u0069me"`, 1),
		"escaped unknown name":   strings.Replace(valid, `"symbol":`, `"s\u0079mbol2":"x","\u0073ymbol":`, 1),
		"deep nesting in extra":  strings.TrimSuffix(valid, "}") + `,"extra":` + strings.Repeat("[", 5000) + strings.Repeat("]", 5000) + `}`,
		"too deep in extra":      strings.TrimSuffix(valid, "}") + `,"extra":` + strings.Repeat("[", 20000) + strings.Repeat("]", 20000) + `}`,
		"long string":            strings.Replace(valid, `"perpetual-BTC-USDT:USD"`, `"`+strings.Repeat("x", 1<<20)+`"`, 1),
		"all fields missing":     `{}`,
	} {
		t.Run(name, func(t *testing.T) {
			requireParity(t, archiveSchema, []parityInput{{topic: parityTopic, body: []byte(body)}})
		})
	}
}

func TestJSONParquetParityLongStringsAmongOthers(t *testing.T) {
	// long strings, of every size around the arena's, in a batch of other
	// messages and partitions: a value must not share memory that a later
	// message is parsed into
	var in []parityInput
	for i, n := range []int{4095, 4096, 4097, 5000, 16 << 10, 64 << 10, 1 << 20} {
		long := fmt.Sprintf(`"%s"`, strings.Repeat(string(rune('a'+i)), n))
		in = append(in,
			parityInput{topic: parityTopic, body: rowWith("symbol", long)},
			parityInput{topic: "metric.v1.okx-perps.15s", body: rowWith("symbol", `"short"`)},
			parityInput{topic: parityTopic, body: rowWith("interval", long)},
			parityInput{topic: parityTopic, body: rowWith("count", `"x"`)}, // dropped
			parityInput{topic: "metric.v1.top-3.15s", body: rowWith("", "")},
		)
	}
	requireParity(t, archiveSchema, in)
}

func TestJSONParquetParityMetadata(t *testing.T) {
	body := rowWith("", "")
	for name, topic := range map[string]string{
		"no topic":        "",
		"short topic":     "metric.v1",
		"exactly 3 parts": "a.b.c",
		"empty segment":   "metric.v1..15s",
		"unicode":         "metric.v1.交易所.15s",
	} {
		t.Run(name, func(t *testing.T) {
			requireParity(t, archiveSchema, []parityInput{{topic: topic, body: body}})
		})
	}
}

func TestJSONParquetParityCachedColumn(t *testing.T) {
	// the interpolated column evaluated once per topic a batch, over topics
	// that differ, repeat, and are absent, within one batch and across batches
	s := archiveSchema.cached()
	body := rowWith("", "")
	var in []parityInput
	for _, topic := range []string{
		parityTopic, "", "metric.v1.okx-perps.15s", "metric.v1", parityTopic, "a.b.c",
		"metric.v1..15s", "", "metric.v1.交易所.15s", "metric.v1.okx-perps.15s", parityTopic,
	} {
		in = append(in, parityInput{topic: topic, body: body})
	}
	requireParity(t, s, in)
	r := rand.New(rand.NewPCG(5, 6))
	for range 50 {
		in := make([]parityInput, 1+r.IntN(400))
		for j := range in {
			in[j] = randomRow(r)
		}
		requireParity(t, s, in)
	}
}

func TestJSONParquetParityOptionalInterpolatedColumn(t *testing.T) {
	// an optional column set from metadata holds its value, as the mapping sets
	// it, and not NULL
	for _, s := range []paritySchema{archiveSchema, archiveSchema.cached()} {
		s.columns = slices.Clone(s.columns)
		for i := range s.columns {
			if s.columns[i].fromMeta != "" {
				s.columns[i].optional = true
			}
		}
		var in []parityInput
		for _, topic := range []string{parityTopic, "metric.v1.okx-perps.15s", "", "metric.v1..15s"} {
			in = append(in, parityInput{topic: topic, body: rowWith("", "")})
		}
		requireParity(t, s, in)
	}
}

func TestJSONParquetParityPartitions(t *testing.T) {
	// the day of a row's time, across midnight, before the epoch and far ahead,
	// and the path fields as every kind
	var in []parityInput
	for _, tm := range []string{
		`1790640000000000`, `1790639999999999`, `1790640000000001`, `1790726399999999`, `1790726400000000`,
		`0`, `-1`, `-1000000`, `-86400000001`, `253402300799999999`, `4102444800000000`,
		`1790677155000000.5`, `1.790677155e15`, `"1790677155000000"`, `1e400`, `null`,
	} {
		in = append(in, parityInput{topic: parityTopic, body: rowWith("time", tm)})
	}
	for _, v := range []string{`15`, `true`, `null`, `{"a":1}`, `[1]`, `"1m"`, `"a/b"`, `"%s"`, `""`} {
		in = append(in, parityInput{topic: parityTopic, body: rowWith("interval", v)})
		in = append(in, parityInput{topic: parityTopic, body: rowWith("timestamp_type", v)})
	}
	for _, topic := range []string{"metric.v1.okx-perps.15s", "metric.v1.top-3.15s", parityTopic} {
		in = append(in, parityInput{topic: topic, body: rowWith("", "")})
	}
	files := requireParity(t, archiveSchema, in)
	require.Greater(t, len(files), 5, "the batch must straddle many partitions to be a test of them")
	for _, m := range in {
		requireParity(t, archiveSchema, []parityInput{m})
	}
}

func TestJSONParquetParityOptionalPartitionField(t *testing.T) {
	// an optional field in the path, present, absent and null in turn, so an
	// absent one cannot be read as the previous message's
	s := archiveSchema
	s.partition = "{count_opt}/{time|2006}"
	s.legacyPartition = `"%s/%s".format(this.count_opt, (this.time / 1000000).ts_format("2006", "UTC"))`
	var in []parityInput
	for _, v := range []string{`5`, missing, `"x"`, missing, `null`, `7`, missing} {
		in = append(in, parityInput{topic: parityTopic, body: rowWith("count_opt", v)})
	}
	files := requireParity(t, s, in)
	require.Greater(t, len(files), 2)
}

// randomRow draws a row as production would send one, and now and then
// corrupts a field or the whole message.
func randomRow(r *rand.Rand) parityInput {
	topics := []string{"metric.v1.binance-futures.15s", "metric.v1.okx-perps.1m", "metric.v1.top-3.30s", "metric.v1.hyperliquid-perps.1d"}
	fields := [][2]string{
		{"exchange", fmt.Sprint(r.IntN(40))},
		{"symbol", fmt.Sprintf("%q", []string{"perpetual-BTC-USDT:USD", "perpetual-我踏马来了-USDT:USD", "perpetual-xyz:PALLADIUM-USD1", ""}[r.IntN(4)])},
		{"interval", fmt.Sprintf("%q", []string{"15s", "30s", "1m", "1d"}[r.IntN(4)])},
		{"timestamp_type", fmt.Sprintf("%q", []string{"true", "false"}[r.IntN(2)])},
		{"day", `"2026-09-29"`},
		{"time", fmt.Sprint(1790640000000000 + r.Int64N(3*86400)*1000000 - 86400000000)},
		{"count", fmt.Sprint(r.Int32() - (1 << 30))},
		{"count_opt", []string{fmt.Sprint(r.IntN(100)), "null"}[r.IntN(2)]},
		{"ratio", fmt.Sprint(r.Float32() * 100)},
		{"ratio_opt", []string{fmt.Sprint(r.NormFloat64()), "null"}[r.IntN(2)]},
		{"price", fmt.Sprint(r.ExpFloat64() * 1e4)},
		{"price_opt", []string{fmt.Sprint(-r.Float64()), "null"}[r.IntN(2)]},
	}
	if r.IntN(10) == 0 {
		f := &fields[r.IntN(len(fields))]
		f[1] = valueVariants[r.IntN(len(valueVariants))]
	}
	var parts []string
	for _, f := range fields {
		if f[1] != missing {
			parts = append(parts, fmt.Sprintf("%q:%s", f[0], f[1]))
		}
	}
	r.Shuffle(len(parts), func(i, j int) { parts[i], parts[j] = parts[j], parts[i] })
	body := "{" + strings.Join(parts, ",") + "}"
	if r.IntN(50) == 0 {
		body = body[:r.IntN(len(body))]
	}
	return parityInput{topic: topics[r.IntN(len(topics))], body: []byte(body)}
}

func TestJSONParquetParityRandomBatches(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	rows, inputs, files := 0, 0, 0
	for range 300 {
		in := make([]parityInput, 1+r.IntN(400))
		for j := range in {
			in[j] = randomRow(r)
		}
		out := requireParity(t, archiveSchema, in)
		inputs += len(in)
		files += len(out)
		rows += rowsIn(t, out)
	}
	t.Logf("%d messages: %d rows archived in %d files, %d dropped", inputs, rows, files, inputs-rows)
	require.Greater(t, rows, inputs*8/10, "most generated rows are valid")
	require.Less(t, rows, inputs, "some generated rows must be dropped")
}

func TestJSONParquetParityFullBatch(t *testing.T) {
	// a 10 MiB batch, the byte budget an archiver closes a batch at: many pages
	// per column and several row flushes per partition
	r := rand.New(rand.NewPCG(3, 4))
	var in []parityInput
	for size := 0; size < 10<<20; {
		m := randomRow(r)
		size += len(m.body)
		in = append(in, m)
	}
	files := requireParity(t, archiveSchema, in)
	t.Logf("%d messages: %d rows in %d files", len(in), rowsIn(t, files), len(files))
	require.Greater(t, rowsIn(t, files), len(in)*8/10)
}

func TestJSONParquetParityWithoutPartitionOrNaN(t *testing.T) {
	s := archiveSchema
	s.partition, s.legacyPartition, s.nanForNull = "", "", false
	r := rand.New(rand.NewPCG(5, 6))
	for range 50 {
		in := make([]parityInput, 1+r.IntN(200))
		for j := range in {
			in[j] = randomRow(r)
		}
		requireParity(t, s, in)
	}
	for _, v := range []string{`null`, missing, `1`} {
		requireParity(t, s, []parityInput{{topic: parityTopic, body: rowWith("price", v)}})
	}
}

func FuzzJSONParquetParity(f *testing.F) {
	f.Add(rowWith("", ""), parityTopic)
	// strings past the arena's, followed by other messages to parse
	for _, n := range []int{4097, 20 << 10} {
		long := rowWith("symbol", `"`+strings.Repeat("y", n)+`"`)
		f.Add(bytes.Join([][]byte{long, rowWith("", ""), rowWith("interval", `"`+strings.Repeat("z", n)+`"`)}, []byte("\n")), parityTopic)
	}
	for _, v := range valueVariants {
		f.Add(rowWith("price", v), parityTopic)
		f.Add(rowWith("time", v), "metric.v1.okx-perps.1m")
	}
	f.Fuzz(func(t *testing.T, body []byte, topic string) {
		// newline-separated messages, so the fuzzer also varies batches
		var in []parityInput
		for line := range strings.SplitSeq(string(body), "\n") {
			in = append(in, parityInput{topic: topic, body: []byte(line)})
		}
		requireParity(t, archiveSchema, in)
	})
}
