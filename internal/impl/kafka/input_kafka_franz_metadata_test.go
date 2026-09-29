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

func TestFranzRecordToMessageCopyRecordValues(t *testing.T) {
	// franz-go hands out records whose Key, Value and header values are slices of
	// the fetch response they arrived in, which holds every record fetched from
	// that broker in the same request. A message held in a long batch (and the
	// record kept for checkpointing) would keep that whole response alive: with
	// copy_record_values neither may reference it.
	for _, addMetadata := range []bool{true, false} {
		fetched := []byte("payload|k|abc")
		record := &kgo.Record{
			Value: fetched[0:7], Key: fetched[8:9], Topic: "metric.v1.x.15s", Partition: 1, Offset: 42,
			Headers: []kgo.RecordHeader{{Key: "h", Value: fetched[10:13]}},
		}
		m := (&franzKafkaReader{addMetadata: addMetadata, copyValues: true}).recordToMessage(record)
		for i := range fetched {
			fetched[i] = 'X' // the fetch buffer is reused or freed once nothing points at it
		}
		b, err := m.msg.AsBytes()
		require.NoError(t, err)
		assert.Equal(t, "payload", string(b), "addMetadata=%v: the message must own its bytes", addMetadata)
		assert.Nil(t, m.r.Key, "addMetadata=%v", addMetadata)
		assert.Nil(t, m.r.Value, "addMetadata=%v", addMetadata)
		assert.Nil(t, m.r.Headers, "addMetadata=%v: header values alias the fetch buffer", addMetadata)
		assert.Equal(t, int64(42), m.r.Offset, "addMetadata=%v: checkpointing needs the offset", addMetadata)
		if addMetadata {
			got, _ := m.msg.MetaGetMut("h")
			assert.Equal(t, "abc", got)
			got, _ = m.msg.MetaGetMut("kafka_key")
			assert.Equal(t, "k", got)
		}
	}

	// The default is unchanged: the message shares the record's bytes.
	fetched := []byte("payload")
	m := (&franzKafkaReader{addMetadata: true}).recordToMessage(&kgo.Record{Value: fetched})
	fetched[0] = 'X'
	b, err := m.msg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "Xayload", string(b))
}
