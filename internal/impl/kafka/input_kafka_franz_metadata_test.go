package kafka

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestFranzRecordToMessageMetadata(t *testing.T) {
	newRecord := func() *kgo.Record {
		return &kgo.Record{
			Key: []byte("k"), Value: []byte("payload"), Topic: "raw.quotes", Partition: 1, Offset: 42,
			Timestamp: time.Unix(1_789_502_374, 0), Headers: []kgo.RecordHeader{{Key: "redpanda-dedup-key", Value: []byte("abc")}},
		}
	}

	withMeta := (&franzKafkaReader{addMetadata: true}).recordToMessage(newRecord())
	b, err := withMeta.msg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "payload", string(b))
	for key, want := range map[string]any{"kafka_key": "k", "kafka_topic": "raw.quotes", "kafka_partition": 1, "kafka_offset": 42, "redpanda-dedup-key": "abc"} {
		got, ok := withMeta.msg.MetaGetMut(key)
		require.True(t, ok, key)
		assert.Equal(t, want, got, key)
	}

	// Opting out must keep the payload and the record (needed for offset
	// checkpointing) while adding no metadata at all.
	bare := (&franzKafkaReader{addMetadata: false}).recordToMessage(newRecord())
	b, err = bare.msg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "payload", string(b))
	assert.Equal(t, int64(42), bare.r.Offset)
	assert.Equal(t, int32(1), bare.r.Partition)
	count := 0
	require.NoError(t, bare.msg.MetaWalkMut(func(string, any) error { count++; return nil }))
	assert.Zero(t, count)
}
