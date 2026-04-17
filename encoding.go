package embeddbcore

import (
	"encoding/binary"
	"errors"
	"math"
)

const (
	RecordVersion   byte = 0x01
	RecordStartMark byte = 0x02
	RecordEndMark   byte = 0x03

	FlagsActive         byte = 0x01
	FlagsHasPrevVersion byte = 0x02

	RecordHeaderSize = 23
	RecordFooterSize = 6

	KeyTypeUint   byte = 0x01
	KeyTypeInt    byte = 0x02
	KeyTypeString byte = 0x03
	KeyTypeFloat  byte = 0x04
	KeyTypeBool   byte = 0x05
	KeyTypeTime   byte = 0x06
	KeyTypeBytes  byte = 0x07
)

var (
	ErrInvalidVarint    = errors.New("invalid varint encoding")
	ErrInvalidUvarint   = errors.New("invalid uvarint encoding")
	ErrTruncatedField   = errors.New("truncated field data")
	ErrInvalidFieldType = errors.New("invalid field type")
)

func EncodeUvarint(buffer []byte, value uint64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], value)
	return append(buffer, tmp[:n]...)
}

func EncodeVarint(buffer []byte, value int64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutVarint(tmp[:], value)
	return append(buffer, tmp[:n]...)
}

func DecodeUvarint(data []byte) (uint64, []byte, error) {
	val, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, data, ErrInvalidUvarint
	}
	return val, data[n:], nil
}

func DecodeVarint(data []byte) (int64, []byte, error) {
	val, n := binary.Varint(data)
	if n <= 0 {
		return 0, data, ErrInvalidVarint
	}
	return val, data[n:], nil
}

func EncodeTLVField(buffer []byte, name string, value []byte) []byte {
	buffer = EncodeUvarint(buffer, uint64(len(name)))
	buffer = append(buffer, name...)
	buffer = EncodeUvarint(buffer, uint64(len(value)))
	buffer = append(buffer, value...)
	return buffer
}

func DecodeTLVField(data []byte) (name string, value []byte, remaining []byte, err error) {
	nameLen, data, err := DecodeUvarint(data)
	if err != nil {
		return "", nil, data, err
	}
	if uint64(len(data)) < nameLen {
		return "", nil, data, ErrTruncatedField
	}
	name = string(data[:nameLen])
	data = data[nameLen:]

	valLen, data, err := DecodeUvarint(data)
	if err != nil {
		return "", nil, data, err
	}
	if uint64(len(data)) < valLen {
		return "", nil, data, ErrTruncatedField
	}
	value = make([]byte, valLen)
	copy(value, data[:valLen])
	data = data[valLen:]

	return name, value, data, nil
}

func EncodeBool(buffer []byte, value bool) []byte {
	if value {
		return append(buffer, 1)
	}
	return append(buffer, 0)
}

func DecodeBool(data []byte) (bool, []byte, error) {
	if len(data) < 1 {
		return false, data, ErrTruncatedField
	}
	return data[0] != 0, data[1:], nil
}

func EncodeString(buffer []byte, value string) []byte {
	buffer = EncodeUvarint(buffer, uint64(len(value)))
	return append(buffer, value...)
}

func DecodeString(data []byte) (string, []byte, error) {
	length, n := binary.Uvarint(data)
	if n <= 0 {
		return "", data, ErrInvalidUvarint
	}
	data = data[n:]
	if uint64(len(data)) < length {
		return "", data, ErrTruncatedField
	}
	val := string(data[:length])
	return val, data[length:], nil
}

func EncodeBytes(buffer []byte, value []byte) []byte {
	buffer = EncodeUvarint(buffer, uint64(len(value)))
	return append(buffer, value...)
}

func DecodeBytes(data []byte) ([]byte, []byte, error) {
	length, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, data, ErrInvalidUvarint
	}
	data = data[n:]
	if uint64(len(data)) < length {
		return nil, data, ErrTruncatedField
	}
	val := make([]byte, length)
	copy(val, data[:length])
	return val, data[length:], nil
}

func EncodeUint64(buffer []byte, value uint64) []byte {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], value)
	return append(buffer, tmp[:]...)
}

func DecodeUint64(data []byte) (uint64, []byte, error) {
	if len(data) < 8 {
		return 0, data, ErrTruncatedField
	}
	return binary.BigEndian.Uint64(data[:8]), data[8:], nil
}

func EncodeInt64(buffer []byte, value int64) []byte {
	encoded := uint64(value) ^ (1 << 63)
	return EncodeUint64(buffer, encoded)
}

func DecodeInt64(data []byte) (int64, []byte, error) {
	encoded, remaining, err := DecodeUint64(data)
	if err != nil {
		return 0, data, err
	}
	return int64(encoded ^ (1 << 63)), remaining, nil
}

func EncodeFloat64(buffer []byte, value float64) []byte {
	bits := math.Float64bits(value)
	if bits&(1<<63) != 0 {
		bits = ^bits
	} else {
		bits |= (1 << 63)
	}
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], bits)
	return append(buffer, tmp[:]...)
}

func DecodeFloat64(data []byte) (float64, []byte, error) {
	if len(data) < 8 {
		return 0, data, ErrTruncatedField
	}
	bits := binary.BigEndian.Uint64(data[:8])
	if bits&(1<<63) != 0 {
		bits = bits & ^(uint64(1) << 63)
	} else {
		bits = ^bits
	}
	return math.Float64frombits(bits), data[8:], nil
}

func EncodeTime(buffer []byte, unixNano int64) []byte {
	return EncodeInt64(buffer, unixNano)
}

func DecodeTime(data []byte) (int64, []byte, error) {
	return DecodeInt64(data)
}

func EncodeIndexKeyUint(value uint64) []byte {
	buf := make([]byte, 9)
	buf[0] = KeyTypeUint
	binary.BigEndian.PutUint64(buf[1:], value)
	return buf[:9]
}

func EncodeIndexKeyInt(value int64) []byte {
	buf := make([]byte, 9)
	buf[0] = KeyTypeInt
	binary.BigEndian.PutUint64(buf[1:], uint64(value)^(1<<63))
	return buf[:9]
}

func EncodeIndexKeyString(value string) []byte {
	buf := make([]byte, 1, 1+binary.MaxVarintLen64+len(value))
	buf[0] = KeyTypeString
	buf = EncodeUvarint(buf, uint64(len(value)))
	buf = append(buf, value...)
	return buf
}

func EncodeIndexKeyFloat(value float64) []byte {
	buf := make([]byte, 9)
	buf[0] = KeyTypeFloat
	bits := math.Float64bits(value)
	if bits&(1<<63) != 0 {
		bits = ^bits
	} else {
		bits |= (1 << 63)
	}
	binary.BigEndian.PutUint64(buf[1:], bits)
	return buf[:9]
}

func EncodeIndexKeyBool(value bool) []byte {
	buf := make([]byte, 2)
	buf[0] = KeyTypeBool
	if value {
		buf[1] = 1
	}
	return buf
}

func EncodeIndexKeyTime(unixNano int64) []byte {
	buf := make([]byte, 9)
	buf[0] = KeyTypeTime
	binary.BigEndian.PutUint64(buf[1:], uint64(unixNano)^(1<<63))
	return buf[:9]
}

func DecodeIndexKeyType(data []byte) (byte, error) {
	if len(data) < 1 {
		return 0, ErrTruncatedField
	}
	return data[0], nil
}
