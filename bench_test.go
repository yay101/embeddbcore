package embeddbcore

import (
	"fmt"
	"os"
	"runtime/pprof"
	"testing"
	"time"
)

type BenchPrimitive struct {
	ID      uint64  `db:"id"`
	Name    string  `db:"unique"`
	Age     int     `db:"index"`
	Score   float64 `db:"index"`
	Active  bool
	Balance int64
	Rating  uint8
	BigNum  uint32
	Small   int16
}

type BenchNested struct {
	ID   uint64 `db:"primary"`
	Info BenchInfo
	Meta BenchMeta `db:"-"`
}

type BenchInfo struct {
	Title string `db:"index"`
	Count int32  `db:"index"`
}

type BenchMeta struct {
	Version uint64
}

type BenchWithSlices struct {
	ID      uint64    `db:"id"`
	Tags    []string  `db:"index"`
	Counts  []int     `db:"index"`
	Weights []float64 `db:"index"`
	Flags   []bool
	Data    []byte
}

type BenchWithTime struct {
	ID        uint64    `db:"id"`
	CreatedAt time.Time `db:"index"`
	UpdatedAt time.Time
}

type BenchFull struct {
	ID         uint64              `db:"id"`
	Name       string              `db:"unique"`
	Age        int                 `db:"index"`
	Score      float64             `db:"index"`
	Active     bool                `db:"index"`
	CreatedAt  time.Time           `db:"index"`
	Tags       []string            `db:"index"`
	Counts     []int               `db:"index"`
	Weights    []float64           `db:"index"`
	RawData    []byte              `db:"encrypt"`
	Properties map[string]string   `db:"index"`
	Children   []BenchChildStruct  `db:"index"`
	Embedded   BenchEmbedded
}

type BenchChildStruct struct {
	Key   string
	Value float64
}

type BenchEmbedded struct {
	Flag bool
	Note string
}

func makePrimitive() BenchPrimitive {
	return BenchPrimitive{
		ID:      12345,
		Name:    "test_record",
		Age:     30,
		Score:   98.6,
		Active:  true,
		Balance: -500,
		Rating:  5,
		BigNum:  99999,
		Small:   -100,
	}
}

func makeFull() BenchFull {
	return BenchFull{
		ID:        42,
		Name:      "benchmark_full_record",
		Age:       25,
		Score:     99.99,
		Active:    true,
		CreatedAt: time.Now(),
		Tags:      []string{"alpha", "beta", "gamma", "delta"},
		Counts:    []int{1, 2, 3, 4, 5},
		Weights:   []float64{1.1, 2.2, 3.3},
		RawData:   []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		Properties: map[string]string{
			"key1": "val1",
			"key2": "val2",
			"key3": "val3",
		},
		Children: []BenchChildStruct{
			{Key: "a", Value: 1.0},
			{Key: "b", Value: 2.0},
			{Key: "c", Value: 3.0},
		},
		Embedded: BenchEmbedded{Flag: true, Note: "hello"},
	}
}

func BenchmarkComputeStructLayout_Primitive(b *testing.B) {
	p := makePrimitive()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ComputeStructLayout(&p)
	}
}

func BenchmarkComputeStructLayout_Nested(b *testing.B) {
	n := BenchNested{ID: 1, Info: BenchInfo{Title: "test", Count: 100}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ComputeStructLayout(&n)
	}
}

func BenchmarkComputeStructLayout_Slices(b *testing.B) {
	s := BenchWithSlices{ID: 1, Tags: []string{"a"}, Counts: []int{1}, Weights: []float64{1.0}, Flags: []bool{true}, Data: []byte{1}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ComputeStructLayout(&s)
	}
}

func BenchmarkComputeStructLayout_Full(b *testing.B) {
	f := makeFull()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ComputeStructLayout(&f)
	}
}

func BenchmarkEncodeUvarint(b *testing.B) {
	buf := make([]byte, 0, 128)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = EncodeUvarint(buf[:0], 123456789)
	}
	_ = buf
}

func BenchmarkDecodeUvarint(b *testing.B) {
	buf := EncodeUvarint(nil, 123456789)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeUvarint(buf)
	}
}

func BenchmarkEncodeVarint(b *testing.B) {
	buf := make([]byte, 0, 128)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = EncodeVarint(buf[:0], -123456789)
	}
	_ = buf
}

func BenchmarkDecodeVarint(b *testing.B) {
	buf := EncodeVarint(nil, -123456789)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeVarint(buf)
	}
}

func BenchmarkEncodeString(b *testing.B) {
	buf := make([]byte, 0, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = EncodeString(buf[:0], "hello world benchmark test string")
	}
	_ = buf
}

func BenchmarkDecodeString(b *testing.B) {
	buf := EncodeString(nil, "hello world benchmark test string")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeString(buf)
	}
}

func BenchmarkEncodeBool(b *testing.B) {
	buf := make([]byte, 0, 16)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = EncodeBool(buf[:0], i%2 == 0)
	}
	_ = buf
}

func BenchmarkDecodeBool(b *testing.B) {
	buf := EncodeBool(nil, true)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeBool(buf)
	}
}

func BenchmarkEncodeFloat64(b *testing.B) {
	buf := make([]byte, 0, 16)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = EncodeFloat64(buf[:0], 1.23456789)
	}
	_ = buf
}

func BenchmarkDecodeFloat64(b *testing.B) {
	buf := EncodeFloat64(nil, 1.23456789)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeFloat64(buf)
	}
}

func BenchmarkEncodeInt64(b *testing.B) {
	buf := make([]byte, 0, 16)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = EncodeInt64(buf[:0], -987654321)
	}
	_ = buf
}

func BenchmarkDecodeInt64(b *testing.B) {
	buf := EncodeInt64(nil, -987654321)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeInt64(buf)
	}
}

func BenchmarkEncodeTime(b *testing.B) {
	buf := make([]byte, 0, 16)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = EncodeTime(buf[:0], 1711234567890123456)
	}
	_ = buf
}

func BenchmarkEncodeTLVField(b *testing.B) {
	buf := make([]byte, 0, 512)
	name := "field_name"
	value := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = EncodeTLVField(buf[:0], name, value)
	}
	_ = buf
}

func BenchmarkDecodeTLVField(b *testing.B) {
	buf := EncodeTLVField(nil, "field_name", []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, _ = DecodeTLVField(buf)
	}
}

func BenchmarkEncodeBytes(b *testing.B) {
	buf := make([]byte, 0, 512)
	val := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = EncodeBytes(buf[:0], val)
	}
	_ = buf
}

func BenchmarkDecodeBytes(b *testing.B) {
	buf := EncodeBytes(nil, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeBytes(buf)
	}
}

func BenchmarkEncodeSlice_String(b *testing.B) {
	buf := make([]byte, 0, 512)
	val := []string{"one", "two", "three", "four", "five"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = EncodeSlice(buf[:0], val)
	}
	_ = buf
}

func BenchmarkDecodeSlice_String(b *testing.B) {
	buf := EncodeSlice(nil, []string{"one", "two", "three", "four", "five"})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeSlice(buf)
	}
}

func BenchmarkEncodeIntSlice(b *testing.B) {
	buf := make([]byte, 0, 512)
	val := []int{1, 2, 3, 4, 5, 6, 7, 8}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = EncodeIntSlice(buf[:0], val)
	}
	_ = buf
}

func BenchmarkDecodeIntSlice(b *testing.B) {
	buf := EncodeIntSlice(nil, []int{1, 2, 3, 4, 5, 6, 7, 8})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeIntSlice(buf)
	}
}

func BenchmarkEncodeFloat64Slice(b *testing.B) {
	buf := make([]byte, 0, 512)
	val := []float64{1.1, 2.2, 3.3, 4.4, 5.5}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = EncodeFloat64Slice(buf[:0], val)
	}
	_ = buf
}

func BenchmarkDecodeFloat64Slice(b *testing.B) {
	buf := EncodeFloat64Slice(nil, []float64{1.1, 2.2, 3.3, 4.4, 5.5})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeFloat64Slice(buf)
	}
}

func BenchmarkEncodeBoolSlice(b *testing.B) {
	buf := make([]byte, 0, 128)
	val := []bool{true, false, true, true, false, true, false, true}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = EncodeBoolSlice(buf[:0], val)
	}
	_ = buf
}

func BenchmarkEncodeIndexKeyUint(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeIndexKeyUint(123456789)
	}
}

func BenchmarkEncodeIndexKeyInt(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeIndexKeyInt(-123456789)
	}
}

func BenchmarkEncodeIndexKeyString(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeIndexKeyString("test_key_value")
	}
}

func BenchmarkEncodeIndexKeyFloat(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeIndexKeyFloat(1.23456789)
	}
}

func BenchmarkEncodeIndexKeyBool(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeIndexKeyBool(true)
	}
}

func BenchmarkEncodeIndexKeyTime(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeIndexKeyTime(1711234567890123456)
	}
}

func BenchmarkGetFieldValue_Int(b *testing.B) {
	p := makePrimitive()
	layout, _ := ComputeStructLayout(&p)
	var offset FieldOffset
	for _, f := range layout.Fields {
		if f.Name == "Age" {
			offset = f
			break
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = GetFieldValue(&p, offset)
	}
}

func BenchmarkGetFieldValue_String(b *testing.B) {
	p := makePrimitive()
	layout, _ := ComputeStructLayout(&p)
	var offset FieldOffset
	for _, f := range layout.Fields {
		if f.Name == "Name" {
			offset = f
			break
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = GetFieldValue(&p, offset)
	}
}

func BenchmarkGetFieldValue_Float64(b *testing.B) {
	p := makePrimitive()
	layout, _ := ComputeStructLayout(&p)
	var offset FieldOffset
	for _, f := range layout.Fields {
		if f.Name == "Score" {
			offset = f
			break
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = GetFieldValue(&p, offset)
	}
}

func BenchmarkGetFieldValue_Bool(b *testing.B) {
	p := makePrimitive()
	layout, _ := ComputeStructLayout(&p)
	var offset FieldOffset
	for _, f := range layout.Fields {
		if f.Name == "Active" {
			offset = f
			break
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = GetFieldValue(&p, offset)
	}
}

func BenchmarkGetFieldValue_Uint8(b *testing.B) {
	p := makePrimitive()
	layout, _ := ComputeStructLayout(&p)
	var offset FieldOffset
	for _, f := range layout.Fields {
		if f.Name == "Rating" {
			offset = f
			break
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = GetFieldValue(&p, offset)
	}
}

func BenchmarkGetFieldValue_Int16(b *testing.B) {
	p := makePrimitive()
	layout, _ := ComputeStructLayout(&p)
	var offset FieldOffset
	for _, f := range layout.Fields {
		if f.Name == "Small" {
			offset = f
			break
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = GetFieldValue(&p, offset)
	}
}

func BenchmarkGetFieldValue_Uint32(b *testing.B) {
	p := makePrimitive()
	layout, _ := ComputeStructLayout(&p)
	var offset FieldOffset
	for _, f := range layout.Fields {
		if f.Name == "BigNum" {
			offset = f
			break
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = GetFieldValue(&p, offset)
	}
}

func BenchmarkGetFieldValue_StringSlice(b *testing.B) {
	f := makeFull()
	layout, _ := ComputeStructLayout(&f)
	var offset FieldOffset
	for _, o := range layout.Fields {
		if o.Name == "Tags" {
			offset = o
			break
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = GetFieldValue(&f, offset)
	}
}

func BenchmarkGetFieldValue_Bytes(b *testing.B) {
	f := makeFull()
	layout, _ := ComputeStructLayout(&f)
	var offset FieldOffset
	for _, o := range layout.Fields {
		if o.Name == "RawData" {
			offset = o
			break
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = GetFieldValue(&f, offset)
	}
}

func BenchmarkSetFieldValue_Int(b *testing.B) {
	tmp := makePrimitive()
	layout, _ := ComputeStructLayout(&tmp)
	var offset FieldOffset
	for _, f := range layout.Fields {
		if f.Name == "Age" {
			offset = f
			break
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := makePrimitive()
		_ = SetFieldValue(&p, offset, int64(42))
	}
}

func BenchmarkSetFieldValue_String(b *testing.B) {
	tmp := makePrimitive()
	layout, _ := ComputeStructLayout(&tmp)
	var offset FieldOffset
	for _, f := range layout.Fields {
		if f.Name == "Name" {
			offset = f
			break
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := makePrimitive()
		_ = SetFieldValue(&p, offset, "updated")
	}
}

func BenchmarkSetFieldValue_Bool(b *testing.B) {
	tmp := makePrimitive()
	layout, _ := ComputeStructLayout(&tmp)
	var offset FieldOffset
	for _, f := range layout.Fields {
		if f.Name == "Active" {
			offset = f
			break
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := makePrimitive()
		_ = SetFieldValue(&p, offset, false)
	}
}

func BenchmarkSetFieldValue_Float64(b *testing.B) {
	tmp := makePrimitive()
	layout, _ := ComputeStructLayout(&tmp)
	var offset FieldOffset
	for _, f := range layout.Fields {
		if f.Name == "Score" {
			offset = f
			break
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := makePrimitive()
		_ = SetFieldValue(&p, offset, float64(55.5))
	}
}

func BenchmarkSetFieldValue_Time(b *testing.B) {
	bt := BenchWithTime{ID: 1}
	layout, _ := ComputeStructLayout(&bt)
	var offset FieldOffset
	for _, f := range layout.Fields {
		if f.Name == "CreatedAt" {
			offset = f
			break
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := BenchWithTime{ID: 1}
		_ = SetFieldValue(&t, offset, time.Now())
	}
}

func BenchmarkComputeSchemaHash(b *testing.B) {
	tmp := makeFull()
	layout, _ := ComputeStructLayout(&tmp)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ComputeSchemaHash(layout.Fields)
	}
}

func BenchmarkGetFieldAsString_Int(b *testing.B) {
	p := makePrimitive()
	layout, _ := ComputeStructLayout(&p)
	var offset FieldOffset
	for _, f := range layout.Fields {
		if f.Name == "Age" {
			offset = f
			break
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = GetFieldAsString(&p, offset)
	}
}

func BenchmarkGetFieldAsString_String(b *testing.B) {
	p := makePrimitive()
	layout, _ := ComputeStructLayout(&p)
	var offset FieldOffset
	for _, f := range layout.Fields {
		if f.Name == "Name" {
			offset = f
			break
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = GetFieldAsString(&p, offset)
	}
}

func BenchmarkGetFieldAsString_Bool(b *testing.B) {
	p := makePrimitive()
	layout, _ := ComputeStructLayout(&p)
	var offset FieldOffset
	for _, f := range layout.Fields {
		if f.Name == "Active" {
			offset = f
			break
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = GetFieldAsString(&p, offset)
	}
}

func BenchmarkGetMapField(b *testing.B) {
	f := makeFull()
	layout, _ := ComputeStructLayout(&f)
	var offset FieldOffset
	for _, o := range layout.Fields {
		if o.Name == "Properties" {
			offset = o
			break
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = GetMapField(&f, offset)
	}
}

func BenchmarkSetFieldValue_Bytes(b *testing.B) {
	tmp := makeFull()
	layout, _ := ComputeStructLayout(&tmp)
	var offset FieldOffset
	for _, o := range layout.Fields {
		if o.Name == "RawData" {
			offset = o
			break
		}
	}
	data := []byte{0x10, 0x20, 0x30, 0x40}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f2 := makeFull()
		_ = SetFieldValue(&f2, offset, data)
	}
}

func BenchmarkSetFieldValue_StringSlice(b *testing.B) {
	f := makeFull()
	layout, _ := ComputeStructLayout(&f)
	var offset FieldOffset
	for _, o := range layout.Fields {
		if o.Name == "Tags" {
			offset = o
			break
		}
	}
	tags := []string{"x", "y", "z"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f2 := makeFull()
		_ = SetFieldValue(&f2, offset, tags)
	}
}

func BenchmarkGetSliceOfStructs(b *testing.B) {
	f := makeFull()
	layout, _ := ComputeStructLayout(&f)
	var offset FieldOffset
	for _, o := range layout.Fields {
		if o.Name == "Children" {
			offset = o
			break
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = GetSliceOfStructs(&f, offset)
	}
}

func TestCPUProfile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping CPU profile in short mode")
	}

	f, err := os.Create("cpu.prof")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if err := pprof.StartCPUProfile(f); err != nil {
		t.Fatal(err)
	}
	defer pprof.StopCPUProfile()

	var total uint64
	for i := 0; i < 100000; i++ {
		p := makePrimitive()
		layout, err := ComputeStructLayout(&p)
		if err != nil {
			t.Fatal(err)
		}
		total += uint64(layout.SchemaVersion)

		for _, offset := range layout.Fields {
			switch offset.Type {
			case 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 24:
				val, ferr := GetFieldValue(&p, offset)
				if ferr == nil {
					_ = SetFieldValue(&p, offset, val)
				}
			case 17:
				// skip struct
			case 23:
				// skip slice
			}
		}

		buf := make([]byte, 0, 512)
		buf = EncodeUvarint(buf, uint64(p.Age))
		buf = EncodeString(buf, p.Name)
		buf = EncodeFloat64(buf, p.Score)
		buf = EncodeBool(buf, p.Active)
		buf = EncodeInt64(buf, p.Balance)
		_, _, _ = DecodeUvarint(buf)
	}
	_ = total
	fmt.Println("CPU profile written to cpu.prof")
}
