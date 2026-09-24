package iggy

import (
	"encoding/binary"
	"encoding/hex"
	"math"
	"math/big"
	"slices"
	"strconv"

	iggcon "github.com/apache/iggy/foreign/go/contracts"

	"github.com/warpstreamlabs/bento/public/service"
)

// toMessage converts one polled Iggy message into a Bento message. The payload
// is not copied: the SDK allocates a fresh reply buffer per poll, and the
// messages of one poll travel downstream as one batch.
func toMessage(m *iggcon.IggyMessage, stream, topic, partition string, log *service.Logger) *service.Message {
	msg := service.NewMessage(m.Payload)
	msg.MetaSetMut("iggy_stream", stream)
	msg.MetaSetMut("iggy_topic", topic)
	msg.MetaSetMut("iggy_partition_id", partition)
	msg.MetaSetMut("iggy_offset", strconv.FormatUint(m.Header.Offset, 10))
	msg.MetaSetMut("iggy_timestamp", strconv.FormatUint(m.Header.Timestamp, 10))
	msg.MetaSetMut("iggy_message_id", messageIDString(m.Header.Id))

	if len(m.UserHeaders) == 0 {
		return msg
	}
	headers, err := iggcon.DeserializeHeaders(m.UserHeaders)
	if err != nil {
		// The payload is still intact; losing the headers of one message is
		// better than refusing to deliver it.
		log.Errorf("Failed to decode the user headers of iggy message at offset %d: %v", m.Header.Offset, err)
		return msg
	}
	for _, h := range headers {
		msg.MetaSetMut(headerString(h.Key.Kind, h.Key.Value), headerString(h.Value.Kind, h.Value.Value))
	}
	return msg
}

// messageIDString renders the 128-bit message id as the unsigned integer the
// server holds, in 32 lowercase hex digits (most significant first). The wire
// carries it little-endian, so a UUID id assigned by the Rust SDK reads back
// as that UUID without dashes.
func messageIDString(id iggcon.MessageID) string {
	b := id
	slices.Reverse(b[:])
	return hex.EncodeToString(b[:])
}

// headerString renders a user header key or value as a metadata string.
// Raw and string kinds are the bytes as they are, which keeps a binary
// dedup-key byte-for-byte identical. Numeric kinds (little-endian on the wire)
// render as decimal, floats in the shortest round-tripping form, and bools as
// true/false. An unknown kind, or a numeric kind of the wrong width, falls
// back to the raw bytes.
func headerString(kind iggcon.HeaderKind, v []byte) string {
	if w := kind.ExpectedSize(); w != -1 && len(v) != w {
		return string(v)
	}
	switch kind {
	case iggcon.Bool:
		return strconv.FormatBool(v[0] != 0)
	case iggcon.Int8:
		return strconv.FormatInt(int64(int8(v[0])), 10)
	case iggcon.Int16:
		return strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(v))), 10)
	case iggcon.Int32:
		return strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(v))), 10)
	case iggcon.Int64:
		return strconv.FormatInt(int64(binary.LittleEndian.Uint64(v)), 10)
	case iggcon.Uint8:
		return strconv.FormatUint(uint64(v[0]), 10)
	case iggcon.Uint16:
		return strconv.FormatUint(uint64(binary.LittleEndian.Uint16(v)), 10)
	case iggcon.Uint32:
		return strconv.FormatUint(uint64(binary.LittleEndian.Uint32(v)), 10)
	case iggcon.Uint64:
		return strconv.FormatUint(binary.LittleEndian.Uint64(v), 10)
	case iggcon.Int128, iggcon.Uint128:
		be := slices.Clone(v)
		slices.Reverse(be)
		n := new(big.Int).SetBytes(be)
		if kind == iggcon.Int128 && be[0]&0x80 != 0 {
			n.Sub(n, new(big.Int).Lsh(big.NewInt(1), 128))
		}
		return n.String()
	case iggcon.Float:
		return strconv.FormatFloat(float64(math.Float32frombits(binary.LittleEndian.Uint32(v))), 'g', -1, 32)
	case iggcon.Double:
		return strconv.FormatFloat(math.Float64frombits(binary.LittleEndian.Uint64(v)), 'g', -1, 64)
	default:
		return string(v)
	}
}
