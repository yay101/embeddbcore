package embeddbcore

import (
	"reflect"
	"testing"
)

func TestComputeStructLayout_Primitive(t *testing.T) {
	p := makePrimitive()
	layout, err := ComputeStructLayout(&p)
	if err != nil {
		t.Fatal(err)
	}

	if layout.PrimaryKey != "ID" {
		t.Errorf("expected primary key 'ID', got '%s'", layout.PrimaryKey)
	}
	if layout.PKType != reflect.Uint64 {
		t.Errorf("expected PKType Uint64, got %v", layout.PKType)
	}

	names := map[string]bool{}
	for _, f := range layout.Fields {
		names[f.Name] = true
	}
	for _, name := range []string{"ID", "Name", "Age", "Score", "Active", "Balance", "Rating", "BigNum", "Small"} {
		if !names[name] {
			t.Errorf("expected field %s not found", name)
		}
	}
}

func TestComputeStructLayout_Nested(t *testing.T) {
	n := BenchNested{ID: 1, Info: BenchInfo{Title: "test", Count: 100}}
	layout, err := ComputeStructLayout(&n)
	if err != nil {
		t.Fatal(err)
	}

	foundTitle := false
	foundCount := false
	foundMeta := false
	for _, f := range layout.Fields {
		if f.Name == "Info.Title" {
			foundTitle = true
		}
		if f.Name == "Info.Count" {
			foundCount = true
		}
		if f.Name == "Meta" {
			foundMeta = true
			if f.Index || f.Unique || f.Primary {
				t.Error("Meta should have no flags (tagged '-')")
			}
		}
	}
	if !foundTitle {
		t.Error("expected Info.Title field not found")
	}
	if !foundCount {
		t.Error("expected Info.Count field not found")
	}
	if foundMeta {
		t.Error("Meta should not be in Fields (tagged '-')")
	}
}

func TestComputeStructLayout_Tags(t *testing.T) {
	p := makePrimitive()
	layout, _ := ComputeStructLayout(&p)

	for _, f := range layout.Fields {
		switch f.Name {
		case "ID":
			if !f.Primary {
				t.Error("ID should be primary")
			}
		case "Name":
			if !f.Unique {
				t.Error("Name should be unique")
			}
		case "Age":
			if !f.Index {
				t.Error("Age should be index")
			}
		case "Score":
			if !f.Index {
				t.Error("Score should be index")
			}
		}
	}
}

func TestComputeStructLayout_SliceStruct(t *testing.T) {
	f := makeFull()
	layout, _ := ComputeStructLayout(&f)

	foundChildren := false
	foundProperties := false
	foundRawData := false
	for _, f := range layout.Fields {
		if f.Name == "Children" {
			foundChildren = true
			if !f.IsSlice {
				t.Error("Children should be slice")
			}
			if f.SliceElem == nil || f.SliceElem.Kind() != reflect.Struct {
				t.Error("Children slice elem should be struct")
			}
		}
		if f.Name == "Properties" {
			foundProperties = true
			if !f.IsMap {
				t.Error("Properties should be map")
			}
		}
		if f.Name == "RawData" {
			foundRawData = true
			if !f.IsBytes {
				t.Error("RawData should be bytes")
			}
			if !f.Encrypted {
				t.Error("RawData should be encrypted")
			}
		}
	}
	if !foundChildren {
		t.Error("expected Children field")
	}
	if !foundProperties {
		t.Error("expected Properties field")
	}
	if !foundRawData {
		t.Error("expected RawData field")
	}
}

func TestGetFieldValue_Numeric(t *testing.T) {
	p := makePrimitive()
	layout, _ := ComputeStructLayout(&p)

	for _, f := range layout.Fields {
		val, err := GetFieldValue(&p, f)
		if err != nil {
			t.Errorf("GetFieldValue(%s) error: %v", f.Name, err)
			continue
		}
		switch f.Name {
		case "ID":
			if v, ok := val.(uint64); !ok || v != 12345 {
				t.Errorf("ID: expected 12345, got %v", val)
			}
		case "Name":
			if v, ok := val.(string); !ok || v != "test_record" {
				t.Errorf("Name: expected 'test_record', got %v", val)
			}
		case "Age":
			if v, ok := val.(int); !ok || v != 30 {
				t.Errorf("Age: expected 30, got %v", val)
			}
		case "Score":
			if v, ok := val.(float64); !ok || v != 98.6 {
				t.Errorf("Score: expected 98.6, got %v", val)
			}
		case "Active":
			if v, ok := val.(bool); !ok || v != true {
				t.Errorf("Active: expected true, got %v", val)
			}
		}
	}
}

func TestSetFieldValue_AndGetFieldValue(t *testing.T) {
	p := makePrimitive()
	layout, _ := ComputeStructLayout(&p)

	for _, f := range layout.Fields {
		if f.Name == "Age" {
			err := SetFieldValue(&p, f, int64(99))
			if err != nil {
				t.Fatal(err)
			}
			break
		}
	}

	for _, f := range layout.Fields {
		if f.Name == "Age" {
			val, err := GetFieldValue(&p, f)
			if err != nil {
				t.Fatal(err)
			}
			v, ok := val.(int)
			if !ok || v != 99 {
				t.Errorf("expected Age=99, got %v", val)
			}
			break
		}
	}
}

func TestSetFieldValue_String(t *testing.T) {
	p := makePrimitive()
	layout, _ := ComputeStructLayout(&p)

	for _, f := range layout.Fields {
		if f.Name == "Name" {
			err := SetFieldValue(&p, f, "updated_name")
			if err != nil {
				t.Fatal(err)
			}
			break
		}
	}

	for _, f := range layout.Fields {
		if f.Name == "Name" {
			val, err := GetFieldValue(&p, f)
			if err != nil {
				t.Fatal(err)
			}
			v, ok := val.(string)
			if !ok || v != "updated_name" {
				t.Errorf("expected Name='updated_name', got %v", val)
			}
			break
		}
	}
}

func TestGetFieldAsString(t *testing.T) {
	p := makePrimitive()
	layout, _ := ComputeStructLayout(&p)

	for _, f := range layout.Fields {
		s := GetFieldAsString(&p, f)
		switch f.Name {
		case "Age":
			if s != "30" {
				t.Errorf("Age as string: expected '30', got '%s'", s)
			}
		case "Name":
			if s != "test_record" {
				t.Errorf("Name as string: expected 'test_record', got '%s'", s)
			}
		case "Active":
			if s != "true" {
				t.Errorf("Active as string: expected 'true', got '%s'", s)
			}
		}
	}
}

func TestGetMapField(t *testing.T) {
	f := makeFull()
	layout, _ := ComputeStructLayout(&f)

	for _, o := range layout.Fields {
		if o.Name == "Properties" {
			m, err := GetMapField(&f, o)
			if err != nil {
				t.Fatal(err)
			}
			if len(m) != 3 {
				t.Errorf("expected 3 map entries, got %d", len(m))
			}
			if m["key1"] != "val1" {
				t.Errorf("expected key1=val1, got %v", m["key1"])
			}
			break
		}
	}
}

func TestGetBytesField(t *testing.T) {
	f := makeFull()
	layout, _ := ComputeStructLayout(&f)

	for _, o := range layout.Fields {
		if o.Name == "RawData" && o.IsBytes {
			b, err := GetBytesField(&f, o)
			if err != nil {
				t.Fatal(err)
			}
			expected := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
			if len(b) != len(expected) {
				t.Errorf("expected %d bytes, got %d", len(expected), len(b))
				break
			}
			for i, v := range expected {
				if b[i] != v {
					t.Errorf("byte at %d: expected %x, got %x", i, v, b[i])
				}
			}
			break
		}
	}
}

func TestEncodeDecode_Roundtrip(t *testing.T) {
	buf := make([]byte, 0, 256)

	buf = EncodeUvarint(buf[:0], 12345)
	v, _, err := DecodeUvarint(buf)
	if err != nil || v != 12345 {
		t.Errorf("uvarint: expected 12345, got %d, err=%v", v, err)
	}

	buf = EncodeVarint(buf[:0], -98765)
	vi, _, err := DecodeVarint(buf)
	if err != nil || vi != -98765 {
		t.Errorf("varint: expected -98765, got %d, err=%v", vi, err)
	}

	buf = EncodeString(buf[:0], "hello world")
	s, _, err := DecodeString(buf)
	if err != nil || s != "hello world" {
		t.Errorf("string: expected 'hello world', got '%s', err=%v", s, err)
	}

	buf = EncodeBool(buf[:0], true)
	b, _, err := DecodeBool(buf)
	if err != nil || b != true {
		t.Errorf("bool: expected true, got %v, err=%v", b, err)
	}

	buf = EncodeFloat64(buf[:0], 3.14159)
	f, _, err := DecodeFloat64(buf)
	if err != nil || f != 3.14159 {
		t.Errorf("float64: expected 3.14159, got %f, err=%v", f, err)
	}

	buf = EncodeInt64(buf[:0], -42)
	ii, _, err := DecodeInt64(buf)
	if err != nil || ii != -42 {
		t.Errorf("int64: expected -42, got %d, err=%v", ii, err)
	}
}

func TestEncodeDecode_Slices(t *testing.T) {
	buf := make([]byte, 0, 512)

	buf = EncodeSlice(buf[:0], []string{"a", "b", "c"})
	ss, _, err := DecodeSlice(buf)
	if err != nil || len(ss) != 3 || ss[0] != "a" || ss[1] != "b" || ss[2] != "c" {
		t.Errorf("string slice: got %v, err=%v", ss, err)
	}

	buf = EncodeIntSlice(buf[:0], []int{10, 20, 30})
	is, _, err := DecodeIntSlice(buf)
	if err != nil || len(is) != 3 || is[0] != 10 || is[1] != 20 || is[2] != 30 {
		t.Errorf("int slice: got %v, err=%v", is, err)
	}
}

func TestIndexKey_Roundtrip(t *testing.T) {
	k := EncodeIndexKeyUint(42)
	typ, err := DecodeIndexKeyType(k)
	if err != nil || typ != KeyTypeUint {
		t.Errorf("uint key type: expected 0x01, got 0x%02x", typ)
	}

	k = EncodeIndexKeyInt(-100)
	typ, _ = DecodeIndexKeyType(k)
	if typ != KeyTypeInt {
		t.Errorf("int key type: expected 0x02, got 0x%02x", typ)
	}

	k = EncodeIndexKeyString("hello")
	typ, _ = DecodeIndexKeyType(k)
	if typ != KeyTypeString {
		t.Errorf("string key type: expected 0x03, got 0x%02x", typ)
	}

	k = EncodeIndexKeyFloat(1.5)
	typ, _ = DecodeIndexKeyType(k)
	if typ != KeyTypeFloat {
		t.Errorf("float key type: expected 0x04, got 0x%02x", typ)
	}

	k = EncodeIndexKeyBool(true)
	typ, _ = DecodeIndexKeyType(k)
	if typ != KeyTypeBool {
		t.Errorf("bool key type: expected 0x05, got 0x%02x", typ)
	}

	k = EncodeIndexKeyTime(1234567890)
	typ, _ = DecodeIndexKeyType(k)
	if typ != KeyTypeTime {
		t.Errorf("time key type: expected 0x06, got 0x%02x", typ)
	}
}

func TestTLVField_Roundtrip(t *testing.T) {
	buf := EncodeTLVField(nil, "test_field", []byte{0xAA, 0xBB, 0xCC})
	name, value, _, err := DecodeTLVField(buf)
	if err != nil {
		t.Fatal(err)
	}
	if name != "test_field" {
		t.Errorf("TLV name: expected 'test_field', got '%s'", name)
	}
	if len(value) != 3 || value[0] != 0xAA || value[1] != 0xBB || value[2] != 0xCC {
		t.Errorf("TLV value: got %v", value)
	}
}

func TestEncryptedIndexExclusion(t *testing.T) {
	p := makePrimitive()
	p.Balance = 100
	p.Age = 25
	layout, _ := ComputeStructLayout(&p)

	for _, f := range layout.Fields {
		if f.Encrypted && f.Index {
			t.Errorf("field %s should not be both encrypted and indexed", f.Name)
		}
	}
}

func TestBytesFieldRoundtrip(t *testing.T) {
	data := []byte{0x10, 0x20, 0x30, 0x40, 0x50}
	f := BenchWithSlices{ID: 1, Data: data}
	layout, _ := ComputeStructLayout(&f)

	for _, o := range layout.Fields {
		if o.Name == "Data" {
			b, err := GetBytesField(&f, o)
			if err != nil {
				t.Fatal(err)
			}
			if len(b) != len(data) {
				t.Errorf("expected %d bytes, got %d", len(data), len(b))
			}

			err = SetBytesField(&f, o, []byte{0xFF, 0xFE})
			if err != nil {
				t.Fatal(err)
			}
			b2, _ := GetBytesField(&f, o)
			if len(b2) != 2 || b2[0] != 0xFF || b2[1] != 0xFE {
				t.Errorf("after set: expected [0xFF 0xFE], got %v", b2)
			}
			break
		}
	}
}

func TestSetFieldValue_Slice(t *testing.T) {
	f := makeFull()
	layout, _ := ComputeStructLayout(&f)

	for _, o := range layout.Fields {
		if o.Name == "Tags" {
			newTags := []string{"new_a", "new_b"}
			err := SetFieldValue(&f, o, newTags)
			if err != nil {
				t.Fatal(err)
			}
			val, _ := GetFieldValue(&f, o)
			tags, ok := val.([]string)
			if !ok || len(tags) != 2 || tags[0] != "new_a" {
				t.Errorf("Tags after set: expected [new_a new_b], got %v", tags)
			}
			break
		}
	}
}

func TestNestedFieldValue(t *testing.T) {
	n := BenchNested{ID: 42, Info: BenchInfo{Title: "nested_test", Count: 99}}
	layout, _ := ComputeStructLayout(&n)

	for _, f := range layout.Fields {
		if f.Name == "Info.Title" {
			val, err := GetFieldValue(&n, f)
			if err != nil {
				t.Fatal(err)
			}
			v, ok := val.(string)
			if !ok || v != "nested_test" {
				t.Errorf("expected 'nested_test', got %v", val)
			}
		}
		if f.Name == "Info.Count" {
			val, err := GetFieldValue(&n, f)
			if err != nil {
				t.Fatal(err)
			}
			v, ok := val.(int32)
			if !ok || v != 99 {
				t.Errorf("expected 99, got %v", val)
			}
		}
	}
}

func TestSchemaVersion_Stability(t *testing.T) {
	p1 := makePrimitive()
	p2 := makePrimitive()
	layout1, _ := ComputeStructLayout(&p1)
	layout2, _ := ComputeStructLayout(&p2)
	if layout1.SchemaVersion != layout2.SchemaVersion {
		t.Errorf("schema version changed between identical structs: %d vs %d", layout1.SchemaVersion, layout2.SchemaVersion)
	}
}

func TestGetBytesField_LargeSlice(t *testing.T) {
	large := make([]byte, 10000)
	for i := range large {
		large[i] = byte(i % 256)
	}
	f := BenchWithSlices{ID: 1, Data: large}
	layout, _ := ComputeStructLayout(&f)

	for _, o := range layout.Fields {
		if o.Name == "Data" {
			b, err := GetBytesField(&f, o)
			if err != nil {
				t.Fatal(err)
			}
			if len(b) != 10000 {
				t.Errorf("expected 10000 bytes, got %d", len(b))
			}
			break
		}
	}
}
