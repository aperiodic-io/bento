package parquet_test

import (
	"bytes"
	"math/rand/v2"
	"runtime"
	"runtime/metrics"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/bloblang"
	"github.com/warpstreamlabs/bento/public/service"
)

// benchBatch is a batch of valid rows the size an archiver closes a batch at.
func benchBatch(tb testing.TB) []parityInput {
	tb.Helper()
	r := rand.New(rand.NewPCG(7, 8))
	var in []parityInput
	for size := 0; size < 10<<20; {
		m := randomRow(r)
		if bytes.Contains(m.body, []byte(`"`+missing)) {
			continue
		}
		size += len(m.body)
		in = append(in, m)
	}
	return in
}

func heapBytes() uint64 {
	s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(s)
	return s[0].Value.Uint64()
}

func liveHeap() uint64 {
	runtime.GC()
	runtime.GC()
	return heapBytes()
}

// BenchmarkJSONParquetHeldBatch measures what holding a batch costs until it
// closes: the replaced pipeline holds each row as the structured value its
// mapping made, json_parquet_encode the row's bytes.
func BenchmarkJSONParquetHeldBatch(b *testing.B) {
	in := benchBatch(b)
	raw := 0
	for _, m := range in {
		raw += len(m.body)
	}
	exe, err := bloblang.Parse(archiveSchema.legacyMapping())
	require.NoError(b, err)

	b.Run("legacy", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			before := liveHeap()
			held := make(service.MessageBatch, 0, len(in))
			for _, m := range in {
				msg := service.NewMessage(bytes.Clone(m.body))
				msg.MetaSetMut("kafka_topic", m.topic)
				out, err := msg.BloblangQuery(exe)
				if err == nil && out != nil {
					held = append(held, out)
				}
			}
			after := liveHeap()
			b.ReportMetric(float64(after-before)/float64(raw), "held-bytes/JSON-byte")
			runtime.KeepAlive(held)
		}
	})
	b.Run("json_parquet_encode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			before := liveHeap()
			held := make(service.MessageBatch, 0, len(in))
			for _, m := range in {
				msg := service.NewMessage(bytes.Clone(m.body))
				msg.MetaSetMut("kafka_topic", m.topic)
				held = append(held, msg)
			}
			after := liveHeap()
			b.ReportMetric(float64(after-before)/float64(raw), "held-bytes/JSON-byte")
			runtime.KeepAlive(held)
		}
	})
}

// BenchmarkJSONParquetEncode runs a full batch through each pipeline, and
// reports the heap's peak above what it held before the batch.
func BenchmarkJSONParquetEncode(b *testing.B) {
	in := benchBatch(b)
	raw := 0
	for _, m := range in {
		raw += len(m.body)
	}
	p := pairFor(b, archiveSchema)
	for name, s := range map[string]*parityStream{"legacy": p.legacy, "json_parquet_encode": p.next} {
		b.Run(name, func(b *testing.B) {
			b.SetBytes(int64(raw))
			b.ReportAllocs()
			var peakSum float64
			for i := 0; i < b.N; i++ {
				before := liveHeap()
				var peak atomic.Uint64
				done := make(chan struct{})
				go func() {
					t := time.NewTicker(time.Millisecond)
					defer t.Stop()
					for {
						select {
						case <-done:
							return
						case <-t.C:
							if h := heapBytes(); h > peak.Load() {
								peak.Store(h)
							}
						}
					}
				}()
				files := s.run(b, in)
				close(done)
				require.NotEmpty(b, files)
				peakSum += float64(peak.Load()-min(before, peak.Load())) / (1 << 20)
			}
			b.ReportMetric(peakSum/float64(b.N), "peak-heap-MiB")
		})
	}
}
