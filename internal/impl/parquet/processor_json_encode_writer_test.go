package parquet

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

// writerTestRow draws a row whose UTF8 fields are empty, null or absent now and
// then, and whose symbol is long enough now and then to fill pages.
func writerTestRow(r *rand.Rand, day int) *service.Message {
	str := func() string {
		switch r.IntN(8) {
		case 0:
			return `""`
		case 1:
			return fmt.Sprintf("%q", strings.Repeat("s", r.IntN(3000)))
		}
		return fmt.Sprintf(`"sym-%d"`, r.IntN(50))
	}
	note := []string{`null`, `""`, `"n"`, str()}[r.IntN(4)]
	body := fmt.Sprintf(`{"symbol":%s,"note":%s,"time":%d,"price":%v,"count":%s}`,
		str(), note, int64(day)*86400_000000+r.Int64N(86400_000000), r.Float64(), []string{"null", "7"}[r.IntN(2)])
	if r.IntN(6) == 0 {
		body = strings.Replace(body, `"note":`+note+`,`, "", 1)
	}
	m := service.NewMessage([]byte(body))
	m.MetaSetMut("topic", []string{"x", "y", "z"}[r.IntN(3)])
	return m
}

// writerTestBatch is a batch, and whether it is clean of empty strings, so
// that every file of it can be written by a reset writer.
type writerTestBatch struct {
	msgs  service.MessageBatch
	clean bool
}

// writerTestBatches are batches of every shape a writer can be reset between:
// one-row files, files of a few rows, and files whose text columns span pages,
// ending in an empty string and clean of them.
func writerTestBatches(r *rand.Rand) []writerTestBatch {
	var batches []writerTestBatch
	for range 30 {
		var b service.MessageBatch
		for range 1 + r.IntN(60) {
			b = append(b, writerTestRow(r, r.IntN(40)))
		}
		batches = append(batches, writerTestBatch{msgs: b})
	}
	// Symbols of 256 bytes fill a page (256 KiB) within the 16th chunk of 64
	// rows a writer takes at a time, so the page is cut after 1024 rows: a
	// batch of 1025 ending in an empty symbol leaves it alone in the last page.
	long := fmt.Sprintf("%q", strings.Repeat("l", 256))
	for _, clean := range []bool{false, true} {
		for n := 1020; n <= 1030; n++ {
			var b service.MessageBatch
			for i := range n {
				m := writerTestRow(r, 0)
				body, _ := m.AsBytes()
				body = withTestField(body, "symbol", long)
				if !clean && i == n-1 {
					body = withTestField(body, "symbol", `""`)
				}
				if clean {
					body = bytes.ReplaceAll(body, []byte(`:""`), []byte(`:"n"`))
				}
				m.SetBytes(body)
				b = append(b, m)
			}
			batches = append(batches, writerTestBatch{msgs: b, clean: clean})
		}
	}
	return batches
}

// withTestField sets a field of a writerTestRow body to raw.
func withTestField(body []byte, field, raw string) []byte {
	i := bytes.Index(body, []byte(`"`+field+`":`))
	if i < 0 {
		panic("no field " + field)
	}
	start := i + len(field) + 3
	var end int
	if body[start] == '"' {
		end = start + 1 + bytes.IndexByte(body[start+1:], '"') + 1
	} else {
		end = start + bytes.IndexAny(body[start:], ",}")
	}
	return append(append(append([]byte{}, body[:start]...), raw...), body[end:]...)
}

func TestJSONParquetEncodeResetWriterWritesAsNew(t *testing.T) {
	// A writer reset for the next file, and for the next batch, must write the
	// bytes a new one would: with every encoding and codec, for one-row files,
	// few-row files and files spanning pages, with empty, null and absent
	// strings anywhere in them.
	for _, encoding := range []string{"DELTA_LENGTH_BYTE_ARRAY", "PLAIN", "RLE_DICTIONARY"} {
		for _, codec := range []string{"uncompressed", "snappy", "gzip", "brotli", "zstd", "lz4raw"} {
			for _, partition := range []string{"", "partition: { path: '{exchange}/{time|2006-01-02}' }"} {
				t.Run(fmt.Sprintf("%v/%v/partitioned=%v", encoding, codec, partition != ""), func(t *testing.T) {
					conf := fmt.Sprintf(`
default_encoding: %v
default_compression: %v
schema:
  - { name: exchange, type: UTF8 }
  - { name: symbol, type: UTF8 }
  - { name: note, type: UTF8, optional: true }
  - { name: time, type: INT64 }
  - { name: price, type: DOUBLE }
  - { name: count, type: INT32, optional: true }
columns:
  - { name: exchange, value: '${! @topic }', cache_by: [ topic ] }
%v
`, encoding, codec, partition)
					// No GC, which would empty the pool between batches, and one
					// P, whose private slot a writer is put in: every file but
					// the first can be given a reset writer.
					defer debug.SetGCPercent(debug.SetGCPercent(-1))
					defer runtime.GC()
					defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
					reused, err := newTestJSONEncoder(t, conf)
					require.NoError(t, err)
					fresh, err := newTestJSONEncoder(t, conf)
					require.NoError(t, err)
					fresh.newWriters = true

					files, clean := 0, 0
					for bi, batch := range writerTestBatches(rand.New(rand.NewPCG(11, 12))) {
						before := reused.reused.Load()
						got, err := reused.ProcessBatch(context.Background(), batch.msgs)
						require.NoError(t, err)
						want, err := fresh.ProcessBatch(context.Background(), batch.msgs)
						require.NoError(t, err)
						require.Len(t, got[0], len(want[0]))
						for fi := range want[0] {
							wb, _ := want[0][fi].AsBytes()
							gb, _ := got[0][fi].AsBytes()
							if !bytes.Equal(wb, gb) {
								t.Fatalf("batch %d file %d (%d rows): a reset writer wrote other bytes than a new one", bi, fi, len(readRows(t, wb)))
							}
						}
						files += len(want[0])
						if batch.clean {
							clean += len(want[0])
							require.Equal(t, int64(len(want[0])), reused.reused.Load()-before, "batch %d: with no GC and no empty string, every file is written by a reset writer", bi)
						}
					}
					require.NotZero(t, clean)
					t.Logf("%d of %d files written by a reset writer, %d of them clean", reused.reused.Load(), files, clean)
				})
			}
		}
	}
}
