package embeddbcore

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

// FieldOffset stores the offset and type information for a field in a struct
type FieldOffset struct {
	Name       string
	Offset     uintptr // Absolute offset from the start of the root struct
	Type       reflect.Kind
	Size       uintptr
	Primary    bool
	Unique     bool
	Key        byte
	IsStruct   bool
	StructType reflect.Type
	Parent     []string     // For nested structs
	IsTime     bool         // True if the field is time.Time
	IsSlice    bool         // True if the field is a slice
	IsBytes    bool         // True if the field is []byte
	SliceElem  reflect.Type // Element type of the slice
}

// StructLayout contains the mapping of field byte keys to their offsets
type StructLayout struct {
	FieldOffsets map[byte]FieldOffset
	Size         uintptr
	Hash         string       // Hash of the struct layout to detect changes
	PrimaryKey   byte         // Byte key of the primary key field (255 = none)
	PKType       reflect.Kind // Type of the PK field (0 = no PK)
}

// ComputeStructLayout analyzes the provided struct and returns a StructLayout
// containing offsets for all fields, used for direct memory access
//
// Deprecated: internal use only. This function will be made private in a future release.
func ComputeStructLayout(data interface{}) (*StructLayout, error) {
	t := reflect.TypeOf(data)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct type, got %v", t.Kind())
	}

	layout := &StructLayout{
		FieldOffsets: make(map[byte]FieldOffset),
		PrimaryKey:   255, // Use 255 as sentinel value meaning "no primary key"
		PKType:       0,   // 0 means no primary key (reflect.Kind(0) is invalid)
	}

	byteKey := byte(0)
	computeFieldOffsets(t, 0, &byteKey, []string{}, layout)

	// Generate a simple hash of the struct layout for checking compatibility
	var hashBuilder strings.Builder
	for i := byte(0); i < byteKey; i++ {
		if field, exists := layout.FieldOffsets[i]; exists {
			hashBuilder.WriteString(fmt.Sprintf("%s:%d:%v,", field.Name, field.Offset, field.Type))
		}
	}
	layout.Hash = hashBuilder.String()
	layout.Size = t.Size()

	return layout, nil
}

// computeFieldOffsets recursively computes the offset of each field in the struct
// baseOffset is the accumulated offset from the root struct to the current struct
func computeFieldOffsets(t reflect.Type, baseOffset uintptr, byteKey *byte, parentPath []string, layout *StructLayout) {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		// Create the complete field path for nested structs
		fieldPath := append([]string{}, parentPath...)

		// Check if this is an anonymous/embedded struct field
		isAnonymous := field.Anonymous

		// For anonymous embedded structs, don't add the field name to the path
		// This allows promoted fields to be named correctly (e.g., "City" instead of "EmbeddedAddress.City")
		if !isAnonymous {
			fieldPath = append(fieldPath, field.Name)
		}

		// Check for db tag
		dbTag := field.Tag.Get("db")

		// Skip fields explicitly marked with "-"
		if dbTag == "-" {
			continue
		}

		// Check for primary key tag (handles comma-separated values like "id,primary")
		isPrimary := strings.Contains(dbTag, "id") || strings.Contains(dbTag, "primary")
		if isPrimary {
			layout.PrimaryKey = *byteKey
			layout.PKType = field.Type.Kind()
		}

		// Check for unique tag (handles comma-separated values like "unique,index")
		isUnique := strings.Contains(dbTag, "unique")

		// Check if this is a time.Time field
		isTimeField := field.Type.PkgPath() == "time" && field.Type.Name() == "Time"

		// Check if this is a slice field
		isSliceField := field.Type.Kind() == reflect.Slice
		var sliceElem reflect.Type
		isBytesField := false
		if isSliceField {
			sliceElem = field.Type.Elem()
			// Check if it's []byte (uint8 slice)
			if sliceElem.Kind() == reflect.Uint8 {
				isBytesField = true
			}
		}

		// Create field offset info
		// Calculate absolute offset from root struct by adding base offset
		absoluteOffset := baseOffset + field.Offset
		fieldOffset := FieldOffset{
			Name:      strings.Join(fieldPath, "."),
			Offset:    absoluteOffset,
			Type:      field.Type.Kind(),
			Size:      field.Type.Size(),
			Primary:   isPrimary,
			Unique:    isUnique,
			Key:       *byteKey,
			Parent:    parentPath,
			IsTime:    isTimeField,
			IsSlice:   isSliceField,
			IsBytes:   isBytesField,
			SliceElem: sliceElem,
		}

		// Handle nested structs
		if field.Type.Kind() == reflect.Struct {
			fieldOffset.IsStruct = true
			fieldOffset.StructType = field.Type

			// Store the struct field itself
			layout.FieldOffsets[*byteKey] = fieldOffset
			*byteKey++

			// Recursively process the nested struct fields
			// For anonymous embedded structs, pass the parent's path to promote fields
			// For named nested structs, pass fieldPath to prefix the nested field names
			anonymous := field.Anonymous
			var recursivePath []string
			if anonymous {
				recursivePath = parentPath
			} else {
				recursivePath = fieldPath
			}
			computeFieldOffsets(field.Type, absoluteOffset, byteKey, recursivePath, layout)
		} else {
			// Store regular field
			layout.FieldOffsets[*byteKey] = fieldOffset
			*byteKey++
		}
	}
}

// GetFieldValue uses the field offset to directly read a field's value from the struct
// This avoids reflection during database operations
//
// Deprecated: internal use only. This function will be made private in a future release.
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
	case reflect.Struct:
		if offset.IsTime {
			return GetTimeField(data, offset), nil
		}
		return nil, fmt.Errorf("unsupported struct field type: %v", offset.Type)
	default:
		return nil, fmt.Errorf("unsupported field type: %v", offset.Type)
	}
}

// Type-specific getters that avoid interface{} allocation
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

// GetBytesField reads a []byte field directly from struct memory
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

// SetBytesField sets a []byte field directly in struct memory
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

// GetFieldAsString returns a string representation of a field value without interface{} allocation
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
			return strconv.FormatInt(GetTimeField(data, offset).Unix(), 10)
		}
		return ""
	default:
		return ""
	}
}

// SetFieldValue uses the field offset to directly set a field's value in the struct
// This avoids reflection during database operations
// Handles type conversions from decoded values (e.g., int64 -> int, uint64 -> uint32)
//
// Deprecated: internal use only. This function will be made private in a future release.
func SetFieldValue(data interface{}, offset FieldOffset, value interface{}) error {
	ptr := unsafe.Pointer(reflect.ValueOf(data).Pointer())
	fieldPtr := unsafe.Add(ptr, offset.Offset)

	switch offset.Type {
	case reflect.Int:
		// Handle conversion from int64 (varint decode result)
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
		// Handle conversion from uint64 (uvarint decode result)
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
		// Check if this is a time.Time field
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
		// Handle []byte specially
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
		// Handle []struct{} slices using reflection
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
