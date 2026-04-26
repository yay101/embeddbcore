package embeddbcore

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

type FieldOffset struct {
	Name       string
	Offset     uintptr
	Type       reflect.Kind
	Size       uintptr
	Primary    bool
	Unique     bool
	Index      bool
	Encrypted  bool
	IsStruct   bool
	StructType reflect.Type
	Parent     []string
	IsTime     bool
	IsSlice    bool
	IsBytes    bool
	SliceElem  reflect.Type
}

type StructLayout struct {
	Fields        []FieldOffset
	Size          uintptr
	SchemaVersion uint32
	PrimaryKey    string
	PKType        reflect.Kind
}

func ComputeStructLayout(data interface{}) (*StructLayout, error) {
	t := reflect.TypeOf(data)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct type, got %v", t.Kind())
	}

	layout := &StructLayout{
		PKType: 0,
	}

	computeFieldOffsets(t, 0, []string{}, layout)

	hash := ComputeSchemaHash(layout.Fields)
	layout.SchemaVersion = hash

	layout.Size = t.Size()

	return layout, nil
}

func ComputeSchemaHash(fields []FieldOffset) uint32 {
	var h uint32 = 0x811c9dc5
	for _, f := range fields {
		if f.IsStruct && !f.IsTime {
			continue
		}
		for _, c := range f.Name {
			h ^= uint32(c)
			h *= 0x01000193
		}
		h ^= 0x2F
		h *= 0x01000193
		h ^= uint32(f.Type)
		h *= 0x01000193
		if f.Encrypted {
			h ^= 0x45
			h *= 0x01000193
		}
	}
	return h
}

func computeFieldOffsets(t reflect.Type, baseOffset uintptr, parentPath []string, layout *StructLayout) {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		if !field.IsExported() {
			continue
		}

		fieldPath := append([]string{}, parentPath...)

		isAnonymous := field.Anonymous

		if !isAnonymous {
			fieldPath = append(fieldPath, field.Name)
		}

		dbTag := field.Tag.Get("db")

		if dbTag == "-" {
			continue
		}

		isPrimary := strings.Contains(dbTag, "id") || strings.Contains(dbTag, "primary")
		if isPrimary && layout.PrimaryKey == "" {
			layout.PrimaryKey = strings.Join(fieldPath, ".")
			layout.PKType = field.Type.Kind()
		}

		isUnique := strings.Contains(dbTag, "unique")
		isIndex := strings.Contains(dbTag, "index")
		isEncrypted := strings.Contains(dbTag, "encrypt")

		if isEncrypted && isIndex {
			// Skip fields that request both encryption and indexing.
			// Encryption makes deterministic ordering impossible.
			continue
		}

		isTimeField := field.Type.PkgPath() == "time" && field.Type.Name() == "Time"

		isSliceField := field.Type.Kind() == reflect.Slice
		var sliceElem reflect.Type
		isBytesField := false
		if isSliceField {
			sliceElem = field.Type.Elem()
			if sliceElem.Kind() == reflect.Uint8 {
				isBytesField = true
			}
		}

		absoluteOffset := baseOffset + field.Offset
		fieldOffset := FieldOffset{
			Name:      strings.Join(fieldPath, "."),
			Offset:    absoluteOffset,
			Type:      field.Type.Kind(),
			Size:      field.Type.Size(),
			Primary:   isPrimary,
			Unique:    isUnique,
			Index:     isIndex,
			Encrypted: isEncrypted,
			Parent:    parentPath,
			IsTime:    isTimeField,
			IsSlice:   isSliceField,
			IsBytes:   isBytesField,
			SliceElem: sliceElem,
		}

		if field.Type.Kind() == reflect.Struct {
			fieldOffset.IsStruct = true
			fieldOffset.StructType = field.Type

			layout.Fields = append(layout.Fields, fieldOffset)

			anonymous := field.Anonymous
			var recursivePath []string
			if anonymous {
				recursivePath = parentPath
			} else {
				recursivePath = fieldPath
			}
			computeFieldOffsets(field.Type, absoluteOffset, recursivePath, layout)
		} else {
			layout.Fields = append(layout.Fields, fieldOffset)
		}
	}
}

func GetFieldValue(data interface{}, offset FieldOffset) (interface{}, error) {
	switch offset.Type {
	case reflect.Int:
		return GetIntField(data, offset), nil
	case reflect.Int8:
		return GetInt8Field(data, offset), nil
	case reflect.Int16:
		return GetInt16Field(data, offset), nil
	case reflect.Int32:
		return GetInt32Field(data, offset), nil
	case reflect.Int64:
		return GetInt64Field(data, offset), nil
	case reflect.Uint:
		return GetUintField(data, offset), nil
	case reflect.Uint8:
		return GetUint8Field(data, offset), nil
	case reflect.Uint16:
		return GetUint16Field(data, offset), nil
	case reflect.Uint32:
		return GetUint32Field(data, offset), nil
	case reflect.Uint64:
		return GetUint64Field(data, offset), nil
	case reflect.Float32:
		return GetFloat32Field(data, offset), nil
	case reflect.Float64:
		return GetFloat64Field(data, offset), nil
	case reflect.Bool:
		return GetBoolField(data, offset), nil
	case reflect.String:
		return GetStringField(data, offset), nil
	case reflect.Slice:
		if offset.IsBytes {
			return GetBytesField(data, offset)
		}
		if offset.SliceElem != nil {
			switch offset.SliceElem.Kind() {
			case reflect.String:
				return GetStringSlice(data, offset), nil
			case reflect.Int:
				return GetIntSlice(data, offset), nil
			case reflect.Int8:
				return GetInt8Slice(data, offset), nil
			case reflect.Int16:
				return GetInt16Slice(data, offset), nil
			case reflect.Int32:
				return GetInt32Slice(data, offset), nil
			case reflect.Int64:
				return GetInt64Slice(data, offset), nil
			case reflect.Uint:
				return GetUintSlice(data, offset), nil
			case reflect.Uint16:
				return GetUint16Slice(data, offset), nil
			case reflect.Uint32:
				return GetUint32Slice(data, offset), nil
			case reflect.Uint64:
				return GetUint64Slice(data, offset), nil
			case reflect.Float32:
				return GetFloat32Slice(data, offset), nil
			case reflect.Float64:
				return GetFloat64Slice(data, offset), nil
			case reflect.Bool:
				return GetBoolSlice(data, offset), nil
			case reflect.Struct:
				return GetSliceOfStructs(data, offset), nil
			}
		}
		return nil, fmt.Errorf("unsupported slice field type: %v", offset.Type)
	case reflect.Struct:
		if offset.IsTime {
			return GetTimeField(data, offset), nil
		}
		return nil, fmt.Errorf("unsupported struct field type: %v", offset.Type)
	default:
		return nil, fmt.Errorf("unsupported field type: %v", offset.Type)
	}
}

func GetIntField(data interface{}, offset FieldOffset) int {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	return *(*int)(unsafe.Add(ptr, offset.Offset))
}

func GetInt8Field(data interface{}, offset FieldOffset) int8 {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	return *(*int8)(unsafe.Add(ptr, offset.Offset))
}

func GetInt16Field(data interface{}, offset FieldOffset) int16 {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	return *(*int16)(unsafe.Add(ptr, offset.Offset))
}

func GetInt32Field(data interface{}, offset FieldOffset) int32 {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	return *(*int32)(unsafe.Add(ptr, offset.Offset))
}

func GetInt64Field(data interface{}, offset FieldOffset) int64 {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	return *(*int64)(unsafe.Add(ptr, offset.Offset))
}

func GetUintField(data interface{}, offset FieldOffset) uint {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	return *(*uint)(unsafe.Add(ptr, offset.Offset))
}

func GetUint8Field(data interface{}, offset FieldOffset) uint8 {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	return *(*uint8)(unsafe.Add(ptr, offset.Offset))
}

func GetUint16Field(data interface{}, offset FieldOffset) uint16 {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	return *(*uint16)(unsafe.Add(ptr, offset.Offset))
}

func GetUint32Field(data interface{}, offset FieldOffset) uint32 {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	return *(*uint32)(unsafe.Add(ptr, offset.Offset))
}

func GetUint64Field(data interface{}, offset FieldOffset) uint64 {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	return *(*uint64)(unsafe.Add(ptr, offset.Offset))
}

func GetFloat32Field(data interface{}, offset FieldOffset) float32 {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	return *(*float32)(unsafe.Add(ptr, offset.Offset))
}

func GetFloat64Field(data interface{}, offset FieldOffset) float64 {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	return *(*float64)(unsafe.Add(ptr, offset.Offset))
}

func GetBoolField(data interface{}, offset FieldOffset) bool {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	return *(*bool)(unsafe.Add(ptr, offset.Offset))
}

func GetStringField(data interface{}, offset FieldOffset) string {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	return *(*string)(unsafe.Add(ptr, offset.Offset))
}

func GetTimeField(data interface{}, offset FieldOffset) time.Time {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	return *(*time.Time)(unsafe.Add(ptr, offset.Offset))
}

func GetStringSlice(data interface{}, offset FieldOffset) []string {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	return *(*[]string)(unsafe.Add(ptr, offset.Offset))
}

func GetIntSlice(data interface{}, offset FieldOffset) []int {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	return *(*[]int)(unsafe.Add(ptr, offset.Offset))
}

func GetInt8Slice(data interface{}, offset FieldOffset) []int8 {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	return *(*[]int8)(unsafe.Add(ptr, offset.Offset))
}

func GetInt16Slice(data interface{}, offset FieldOffset) []int16 {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	return *(*[]int16)(unsafe.Add(ptr, offset.Offset))
}

func GetInt32Slice(data interface{}, offset FieldOffset) []int32 {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	return *(*[]int32)(unsafe.Add(ptr, offset.Offset))
}

func GetInt64Slice(data interface{}, offset FieldOffset) []int64 {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	return *(*[]int64)(unsafe.Add(ptr, offset.Offset))
}

func GetUintSlice(data interface{}, offset FieldOffset) []uint {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	return *(*[]uint)(unsafe.Add(ptr, offset.Offset))
}

func GetUint16Slice(data interface{}, offset FieldOffset) []uint16 {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	return *(*[]uint16)(unsafe.Add(ptr, offset.Offset))
}

func GetUint32Slice(data interface{}, offset FieldOffset) []uint32 {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	return *(*[]uint32)(unsafe.Add(ptr, offset.Offset))
}

func GetUint64Slice(data interface{}, offset FieldOffset) []uint64 {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	return *(*[]uint64)(unsafe.Add(ptr, offset.Offset))
}

func GetFloat32Slice(data interface{}, offset FieldOffset) []float32 {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	return *(*[]float32)(unsafe.Add(ptr, offset.Offset))
}

func GetFloat64Slice(data interface{}, offset FieldOffset) []float64 {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	return *(*[]float64)(unsafe.Add(ptr, offset.Offset))
}

func GetBoolSlice(data interface{}, offset FieldOffset) []bool {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	return *(*[]bool)(unsafe.Add(ptr, offset.Offset))
}

func GetSliceOfStructs(data interface{}, offset FieldOffset) interface{} {
	rootVal := reflect.ValueOf(data).Elem()
	val := rootVal.FieldByName(offset.Name)
	if len(offset.Parent) > 0 {
		val = rootVal
		for _, part := range offset.Parent {
			val = val.FieldByName(part)
			if !val.IsValid() {
				return nil
			}
		}
		fieldParts := strings.Split(offset.Name, ".")
		lastPart := fieldParts[len(fieldParts)-1]
		val = val.FieldByName(lastPart)
	}
	if !val.IsValid() || val.IsNil() {
		return nil
	}
	return val.Interface()
}

func GetBytesField(data interface{}, offset FieldOffset) ([]byte, error) {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	fieldPtr := unsafe.Add(ptr, offset.Offset)

	sliceHeader := (*reflect.SliceHeader)(fieldPtr)
	if sliceHeader.Len == 0 {
		return nil, nil
	}

	bytesPtr := unsafe.Pointer(sliceHeader.Data)
	bytes := make([]byte, sliceHeader.Len)
	copy(bytes, (*[8192]byte)(bytesPtr)[:sliceHeader.Len])
	return bytes, nil
}

func SetBytesField(data interface{}, offset FieldOffset, value []byte) error {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	fieldPtr := unsafe.Add(ptr, offset.Offset)

	sliceHeader := (*reflect.SliceHeader)(fieldPtr)
	if len(value) == 0 {
		sliceHeader.Data = 0
		sliceHeader.Len = 0
		sliceHeader.Cap = 0
		return nil
	}

	bytesCopy := make([]byte, len(value))
	copy(bytesCopy, value)

	sliceHeader.Data = uintptr(unsafe.Pointer(&bytesCopy[0]))
	sliceHeader.Len = len(value)
	sliceHeader.Cap = len(value)
	return nil
}

func GetFieldAsString(data interface{}, offset FieldOffset) string {
	switch offset.Type {
	case reflect.Int:
		return strconv.FormatInt(int64(GetIntField(data, offset)), 10)
	case reflect.Int8:
		return strconv.FormatInt(int64(GetInt8Field(data, offset)), 10)
	case reflect.Int16:
		return strconv.FormatInt(int64(GetInt16Field(data, offset)), 10)
	case reflect.Int32:
		return strconv.FormatInt(int64(GetInt32Field(data, offset)), 10)
	case reflect.Int64:
		return strconv.FormatInt(GetInt64Field(data, offset), 10)
	case reflect.Uint:
		return strconv.FormatUint(uint64(GetUintField(data, offset)), 10)
	case reflect.Uint8:
		return strconv.FormatUint(uint64(GetUint8Field(data, offset)), 10)
	case reflect.Uint16:
		return strconv.FormatUint(uint64(GetUint16Field(data, offset)), 10)
	case reflect.Uint32:
		return strconv.FormatUint(uint64(GetUint32Field(data, offset)), 10)
	case reflect.Uint64:
		return strconv.FormatUint(GetUint64Field(data, offset), 10)
	case reflect.Float32:
		return strconv.FormatFloat(float64(GetFloat32Field(data, offset)), 'f', -1, 32)
	case reflect.Float64:
		return strconv.FormatFloat(GetFloat64Field(data, offset), 'f', -1, 64)
	case reflect.Bool:
		return strconv.FormatBool(GetBoolField(data, offset))
	case reflect.String:
		return GetStringField(data, offset)
	case reflect.Struct:
		if offset.IsTime {
			return strconv.FormatInt(GetTimeField(data, offset).UnixNano(), 10)
		}
		return ""
	case reflect.Slice:
		return ""
	default:
		return ""
	}
}

func SetFieldValue(data interface{}, offset FieldOffset, value interface{}) error {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	fieldPtr := unsafe.Add(ptr, offset.Offset)

	switch offset.Type {
	case reflect.Int:
		switch v := value.(type) {
		case int64:
			*(*int)(fieldPtr) = int(v)
		case int:
			*(*int)(fieldPtr) = v
		default:
			return fmt.Errorf("cannot convert %T to int", value)
		}
	case reflect.Int8:
		switch v := value.(type) {
		case int64:
			*(*int8)(fieldPtr) = int8(v)
		case int8:
			*(*int8)(fieldPtr) = v
		default:
			return fmt.Errorf("cannot convert %T to int8", value)
		}
	case reflect.Int16:
		switch v := value.(type) {
		case int64:
			*(*int16)(fieldPtr) = int16(v)
		case int16:
			*(*int16)(fieldPtr) = v
		default:
			return fmt.Errorf("cannot convert %T to int16", value)
		}
	case reflect.Int32:
		switch v := value.(type) {
		case int64:
			*(*int32)(fieldPtr) = int32(v)
		case int32:
			*(*int32)(fieldPtr) = v
		default:
			return fmt.Errorf("cannot convert %T to int32", value)
		}
	case reflect.Int64:
		switch v := value.(type) {
		case int64:
			*(*int64)(fieldPtr) = v
		default:
			return fmt.Errorf("cannot convert %T to int64", value)
		}
	case reflect.Uint:
		switch v := value.(type) {
		case uint64:
			*(*uint)(fieldPtr) = uint(v)
		case uint:
			*(*uint)(fieldPtr) = v
		default:
			return fmt.Errorf("cannot convert %T to uint", value)
		}
	case reflect.Uint8:
		switch v := value.(type) {
		case uint64:
			*(*uint8)(fieldPtr) = uint8(v)
		case uint8:
			*(*uint8)(fieldPtr) = v
		default:
			return fmt.Errorf("cannot convert %T to uint8", value)
		}
	case reflect.Uint16:
		switch v := value.(type) {
		case uint64:
			*(*uint16)(fieldPtr) = uint16(v)
		case uint16:
			*(*uint16)(fieldPtr) = v
		default:
			return fmt.Errorf("cannot convert %T to uint16", value)
		}
	case reflect.Uint32:
		switch v := value.(type) {
		case uint64:
			*(*uint32)(fieldPtr) = uint32(v)
		case uint32:
			*(*uint32)(fieldPtr) = v
		default:
			return fmt.Errorf("cannot convert %T to uint32", value)
		}
	case reflect.Uint64:
		switch v := value.(type) {
		case uint64:
			*(*uint64)(fieldPtr) = v
		default:
			return fmt.Errorf("cannot convert %T to uint64", value)
		}
	case reflect.Float32:
		switch v := value.(type) {
		case float64:
			*(*float32)(fieldPtr) = float32(v)
		case float32:
			*(*float32)(fieldPtr) = v
		default:
			return fmt.Errorf("cannot convert %T to float32", value)
		}
	case reflect.Float64:
		switch v := value.(type) {
		case float64:
			*(*float64)(fieldPtr) = v
		default:
			return fmt.Errorf("cannot convert %T to float64", value)
		}
	case reflect.Bool:
		switch v := value.(type) {
		case bool:
			*(*bool)(fieldPtr) = v
		default:
			return fmt.Errorf("cannot convert %T to bool", value)
		}
	case reflect.String:
		switch v := value.(type) {
		case string:
			*(*string)(fieldPtr) = v
		default:
			return fmt.Errorf("cannot convert %T to string", value)
		}
	case reflect.Struct:
		if offset.IsTime {
			switch v := value.(type) {
			case time.Time:
				*(*time.Time)(fieldPtr) = v
			default:
				return fmt.Errorf("cannot convert %T to time.Time", value)
			}
		} else {
			return fmt.Errorf("unsupported struct field type: %v", offset.Type)
		}
	case reflect.Slice:
		if offset.IsBytes {
			bytesVal, ok := value.([]byte)
			if !ok {
				return fmt.Errorf("cannot convert %T to []byte", value)
			}
			sliceHeader := (*reflect.SliceHeader)(fieldPtr)
			if len(bytesVal) == 0 {
				sliceHeader.Data = 0
				sliceHeader.Len = 0
				sliceHeader.Cap = 0
				break
			}
			bytesCopy := make([]byte, len(bytesVal))
			copy(bytesCopy, bytesVal)
			sliceHeader.Data = uintptr(unsafe.Pointer(&bytesCopy[0]))
			sliceHeader.Len = len(bytesVal)
			sliceHeader.Cap = len(bytesVal)
			break
		}
		if offset.SliceElem.Kind() == reflect.Struct {
			sliceVal := reflect.ValueOf(value)
			if !sliceVal.IsValid() || sliceVal.Kind() != reflect.Slice {
				return fmt.Errorf("cannot convert %T to slice of structs", value)
			}
			sliceHeader := (*reflect.SliceHeader)(fieldPtr)
			elemType := offset.SliceElem
			sliceLen := sliceVal.Len()
			sliceHeader.Len = sliceLen
			sliceHeader.Cap = sliceLen
			if sliceLen == 0 {
				break
			}
			sliceSize := elemType.Size()
			data := make([]byte, sliceLen*int(sliceSize))
			for i := 0; i < sliceLen; i++ {
				elemPtr := unsafe.Pointer(uintptr(unsafe.Pointer(&data[0])) + uintptr(i)*uintptr(sliceSize))
				elemVal := sliceVal.Index(i)
				if elemVal.Kind() == reflect.Ptr {
					elemVal = elemVal.Elem()
				}
				if elemVal.IsValid() && elemVal.Kind() == reflect.Struct {
					srcPtr := unsafe.Pointer(elemVal.UnsafeAddr())
					srcData := unsafe.Slice((*byte)(srcPtr), elemType.Size())
					copy(unsafe.Slice((*byte)(elemPtr), elemType.Size()), srcData)
				}
			}
			sliceHeader.Data = uintptr(unsafe.Pointer(&data[0]))
			break
		}
		if strSlice, ok := value.([]string); ok && offset.SliceElem.Kind() == reflect.String {
			sliceHeader := (*reflect.SliceHeader)(fieldPtr)
			sliceHeader.Len = len(strSlice)
			sliceHeader.Cap = len(strSlice)
			if len(strSlice) == 0 {
				break
			}
			data := make([]string, len(strSlice))
			copy(data, strSlice)
			sliceHeader.Data = uintptr(unsafe.Pointer(&data[0]))
			break
		}
		if intSlice, ok := value.([]int); ok && offset.SliceElem.Kind() == reflect.Int {
			sliceHeader := (*reflect.SliceHeader)(fieldPtr)
			sliceHeader.Len = len(intSlice)
			sliceHeader.Cap = len(intSlice)
			if len(intSlice) == 0 {
				break
			}
			data := make([]int, len(intSlice))
			copy(data, intSlice)
			sliceHeader.Data = uintptr(unsafe.Pointer(&data[0]))
			break
		}
		if int8Slice, ok := value.([]int8); ok && offset.SliceElem.Kind() == reflect.Int8 {
			sliceHeader := (*reflect.SliceHeader)(fieldPtr)
			sliceHeader.Len = len(int8Slice)
			sliceHeader.Cap = len(int8Slice)
			if len(int8Slice) == 0 {
				break
			}
			data := make([]int8, len(int8Slice))
			copy(data, int8Slice)
			sliceHeader.Data = uintptr(unsafe.Pointer(&data[0]))
			break
		}
		if int16Slice, ok := value.([]int16); ok && offset.SliceElem.Kind() == reflect.Int16 {
			sliceHeader := (*reflect.SliceHeader)(fieldPtr)
			sliceHeader.Len = len(int16Slice)
			sliceHeader.Cap = len(int16Slice)
			if len(int16Slice) == 0 {
				break
			}
			data := make([]int16, len(int16Slice))
			copy(data, int16Slice)
			sliceHeader.Data = uintptr(unsafe.Pointer(&data[0]))
			break
		}
		if int32Slice, ok := value.([]int32); ok && offset.SliceElem.Kind() == reflect.Int32 {
			sliceHeader := (*reflect.SliceHeader)(fieldPtr)
			sliceHeader.Len = len(int32Slice)
			sliceHeader.Cap = len(int32Slice)
			if len(int32Slice) == 0 {
				break
			}
			data := make([]int32, len(int32Slice))
			copy(data, int32Slice)
			sliceHeader.Data = uintptr(unsafe.Pointer(&data[0]))
			break
		}
		if int64Slice, ok := value.([]int64); ok && offset.SliceElem.Kind() == reflect.Int64 {
			sliceHeader := (*reflect.SliceHeader)(fieldPtr)
			sliceHeader.Len = len(int64Slice)
			sliceHeader.Cap = len(int64Slice)
			if len(int64Slice) == 0 {
				break
			}
			data := make([]int64, len(int64Slice))
			copy(data, int64Slice)
			sliceHeader.Data = uintptr(unsafe.Pointer(&data[0]))
			break
		}
		if uintSlice, ok := value.([]uint); ok && offset.SliceElem.Kind() == reflect.Uint {
			sliceHeader := (*reflect.SliceHeader)(fieldPtr)
			sliceHeader.Len = len(uintSlice)
			sliceHeader.Cap = len(uintSlice)
			if len(uintSlice) == 0 {
				break
			}
			data := make([]uint, len(uintSlice))
			copy(data, uintSlice)
			sliceHeader.Data = uintptr(unsafe.Pointer(&data[0]))
			break
		}
		if uint16Slice, ok := value.([]uint16); ok && offset.SliceElem.Kind() == reflect.Uint16 {
			sliceHeader := (*reflect.SliceHeader)(fieldPtr)
			sliceHeader.Len = len(uint16Slice)
			sliceHeader.Cap = len(uint16Slice)
			if len(uint16Slice) == 0 {
				break
			}
			data := make([]uint16, len(uint16Slice))
			copy(data, uint16Slice)
			sliceHeader.Data = uintptr(unsafe.Pointer(&data[0]))
			break
		}
		if uint32Slice, ok := value.([]uint32); ok && offset.SliceElem.Kind() == reflect.Uint32 {
			sliceHeader := (*reflect.SliceHeader)(fieldPtr)
			sliceHeader.Len = len(uint32Slice)
			sliceHeader.Cap = len(uint32Slice)
			if len(uint32Slice) == 0 {
				break
			}
			data := make([]uint32, len(uint32Slice))
			copy(data, uint32Slice)
			sliceHeader.Data = uintptr(unsafe.Pointer(&data[0]))
			break
		}
		if uint64Slice, ok := value.([]uint64); ok && offset.SliceElem.Kind() == reflect.Uint64 {
			sliceHeader := (*reflect.SliceHeader)(fieldPtr)
			sliceHeader.Len = len(uint64Slice)
			sliceHeader.Cap = len(uint64Slice)
			if len(uint64Slice) == 0 {
				break
			}
			data := make([]uint64, len(uint64Slice))
			copy(data, uint64Slice)
			sliceHeader.Data = uintptr(unsafe.Pointer(&data[0]))
			break
		}
		if float32Slice, ok := value.([]float32); ok && offset.SliceElem.Kind() == reflect.Float32 {
			sliceHeader := (*reflect.SliceHeader)(fieldPtr)
			sliceHeader.Len = len(float32Slice)
			sliceHeader.Cap = len(float32Slice)
			if len(float32Slice) == 0 {
				break
			}
			data := make([]float32, len(float32Slice))
			copy(data, float32Slice)
			sliceHeader.Data = uintptr(unsafe.Pointer(&data[0]))
			break
		}
		if float64Slice, ok := value.([]float64); ok && offset.SliceElem.Kind() == reflect.Float64 {
			sliceHeader := (*reflect.SliceHeader)(fieldPtr)
			sliceHeader.Len = len(float64Slice)
			sliceHeader.Cap = len(float64Slice)
			if len(float64Slice) == 0 {
				break
			}
			data := make([]float64, len(float64Slice))
			copy(data, float64Slice)
			sliceHeader.Data = uintptr(unsafe.Pointer(&data[0]))
			break
		}
		if boolSlice, ok := value.([]bool); ok && offset.SliceElem.Kind() == reflect.Bool {
			sliceHeader := (*reflect.SliceHeader)(fieldPtr)
			sliceHeader.Len = len(boolSlice)
			sliceHeader.Cap = len(boolSlice)
			if len(boolSlice) == 0 {
				break
			}
			data := make([]bool, len(boolSlice))
			copy(data, boolSlice)
			sliceHeader.Data = uintptr(unsafe.Pointer(&data[0]))
			break
		}
		sliceVal, ok := value.([]interface{})
		if !ok {
			return fmt.Errorf("cannot convert %T to slice", value)
		}
		if len(sliceVal) == 0 {
			break
		}
		sliceHeader := (*reflect.SliceHeader)(fieldPtr)
		elemType := offset.SliceElem
		sliceSize := elemType.Size()
		data := make([]byte, len(sliceVal)*int(sliceSize))
		for i, elem := range sliceVal {
			elemPtr := unsafe.Pointer(uintptr(unsafe.Pointer(&data[0])) + uintptr(i)*uintptr(sliceSize))
			switch elemType.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				var intVal int64
				switch v := elem.(type) {
				case int64:
					intVal = v
				case int:
					intVal = int64(v)
				case int32:
					intVal = int64(v)
				case int16:
					intVal = int64(v)
				case int8:
					intVal = int64(v)
				}
				switch elemType.Kind() {
				case reflect.Int:
					*(*int)(elemPtr) = int(intVal)
				case reflect.Int8:
					*(*int8)(elemPtr) = int8(intVal)
				case reflect.Int16:
					*(*int16)(elemPtr) = int16(intVal)
				case reflect.Int32:
					*(*int32)(elemPtr) = int32(intVal)
				case reflect.Int64:
					*(*int64)(elemPtr) = intVal
				}
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				var uintVal uint64
				switch v := elem.(type) {
				case uint64:
					uintVal = v
				case uint:
					uintVal = uint64(v)
				case uint32:
					uintVal = uint64(v)
				case uint16:
					uintVal = uint64(v)
				case uint8:
					uintVal = uint64(v)
				}
				switch elemType.Kind() {
				case reflect.Uint:
					*(*uint)(elemPtr) = uint(uintVal)
				case reflect.Uint8:
					*(*uint8)(elemPtr) = uint8(uintVal)
				case reflect.Uint16:
					*(*uint16)(elemPtr) = uint16(uintVal)
				case reflect.Uint32:
					*(*uint32)(elemPtr) = uint32(uintVal)
				case reflect.Uint64:
					*(*uint64)(elemPtr) = uintVal
				}
			case reflect.Float32, reflect.Float64:
				var floatVal float64
				switch v := elem.(type) {
				case float64:
					floatVal = v
				case float32:
					floatVal = float64(v)
				}
				if elemType.Kind() == reflect.Float32 {
					*(*float32)(elemPtr) = float32(floatVal)
				} else {
					*(*float64)(elemPtr) = floatVal
				}
			case reflect.String:
				strVal, _ := elem.(string)
				strHdr := (*reflect.StringHeader)(elemPtr)
				if len(strVal) > 0 {
					strData := make([]byte, len(strVal))
					copy(strData, strVal)
					strHdr.Data = uintptr(unsafe.Pointer(&strData[0]))
					strHdr.Len = len(strVal)
				}
			case reflect.Bool:
				boolVal, _ := elem.(bool)
				*(*bool)(elemPtr) = boolVal
			}
		}
		sliceHeader.Data = uintptr(unsafe.Pointer(&data[0]))
		sliceHeader.Len = len(sliceVal)
		sliceHeader.Cap = len(sliceVal)
	default:
		return fmt.Errorf("unsupported field type: %v", offset.Type)
	}

	return nil
}
