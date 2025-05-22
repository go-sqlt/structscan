// Package structscan provides fast, reusable mapping from SQL query results to Go structs.

// Example usage:
/*

package main

import (
	"database/sql"
	"fmt"
	"math/big"
	"net/url"
	"time"

	"github.com/go-sqlt/structscan"
	_ "modernc.org/sqlite"
)

type Data struct {
	Int    int
	String string
	Bool   bool
	Time   *time.Time
	Big    big.Int
	URL    *url.URL
	JSON   map[string]string
	Slice  []string
}

var (
	schema = structscan.New[Data]()
	mapper = structscan.Map(
		schema["Int"].MustIntEnum(
			structscan.Enum{String: "one", Int: 1},
			structscan.Enum{String: "two", Int: 2},
			structscan.Enum{String: "three", Int: 3},
			structscan.Enum{String: "hundred", Int: 100},
		),
		schema["String"].MustStringEnum(
			structscan.Enum{String: "one", Int: 1},
			structscan.Enum{String: "two", Int: 2},
			structscan.Enum{String: "three", Int: 3},
			structscan.Enum{String: "hundred", Int: 100},
		),
		schema["Bool"].MustBool(),
		schema["Time"].MustParseTime(time.DateOnly).Default("2001-02-03"),
		schema["Big"].MustUnmarshalText(),
		schema["URL"].MustUnmarshalBinary(),
		schema["JSON"].UnmarshalJSON().Default([]byte(`{"hello":"world"}`)),
		schema["Slice"].MustSplit(","),
	)
)

func main() {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		panic(err)
	}

	rows, err := db.Query(`
		SELECT
			'one'
			, 2
			, true
			, NULL
			, '300'
			, 'https://example.com/path?query=yes'
			, NULL
			, 'hello,world'
	`)
	if err != nil {
		panic(err)
	}

	data, err := mapper.One(rows)
	if err != nil {
		panic(err)
	}

	fmt.Println(data)
	// {1 two true 2001-02-03 00:00:00 +0000 UTC {false [300]} https://example.com/path?query=yes map[hello:world] [hello world]}
}


*/
package structscan

import (
	"database/sql"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

// Scanner provides a value for sql.Rows.Scan and a function
// that maps the scanned result into a value of type T.
// Both the destination and the mapping function can be reused.
type Scanner[T any] interface {
	Scan() (any, func(*T) error)
}

// Map returns a Mapper that combines positional scanners for populating a struct of type T.
func Map[T any](scanners ...Scanner[T]) Mapper[T] {
	if len(scanners) == 0 {
		var src T

		return Mapper[T]{
			Dest: []any{&src},
			Set: func(t *T) error {
				*t = src

				return nil
			},
		}
	}

	var (
		values  = make([]any, len(scanners))
		setters = make([]func(*T) error, len(scanners))
	)

	for i, s := range scanners {
		values[i], setters[i] = s.Scan()
	}

	return Mapper[T]{
		Dest: values,
		Set: func(t *T) error {
			for _, s := range setters {
				if s != nil {
					if err := s(t); err != nil {
						return err
					}
				}
			}

			return nil
		},
	}
}

// Mapper holds scan destinations and a setter to populate values of type T.
// It is reusable across multiple sql.Rows or sql.Row instances.
type Mapper[T any] struct {
	Dest []any
	Set  func(*T) error
}

// All scans all rows into a slice of T using the configured Mapper.
func (m Mapper[T]) All(rows *sql.Rows) ([]T, error) {
	var all []T

	for rows.Next() {
		if err := rows.Scan(m.Dest...); err != nil {
			return nil, combineErrs(err, rows.Close())
		}

		var t T

		if err := m.Set(&t); err != nil {
			return nil, combineErrs(err, rows.Close())
		}

		all = append(all, t)
	}

	return all, combineErrs(rows.Err(), rows.Close())
}

// ErrTooManyRows is returned by One if more than one row exists in the result set.
var ErrTooManyRows = errors.New("too many rows")

// One maps the first row of sql.Rows into a value of type T.
// Returns ErrTooManyRows if more than one row exists in the result set.
func (m Mapper[T]) One(rows *sql.Rows) (T, error) {
	var one T

	if !rows.Next() {
		return one, combineErrs(sql.ErrNoRows, rows.Close())
	}

	if err := rows.Scan(m.Dest...); err != nil {
		return one, combineErrs(err, rows.Close())
	}

	if err := m.Set(&one); err != nil {
		return one, combineErrs(err, rows.Close())
	}

	if rows.Next() {
		return one, combineErrs(ErrTooManyRows, rows.Close())
	}

	return one, combineErrs(rows.Err(), rows.Close())
}

// Row maps a sql.Row into a value of type T.
func (m Mapper[T]) Row(row *sql.Row) (T, error) {
	var first T

	if err := row.Scan(m.Dest...); err != nil {
		return first, err
	}

	if err := m.Set(&first); err != nil {
		return first, err
	}

	return first, nil
}

// New returns a Struct schema that maps all accessible (exported) fields of T,
// including nested ones, using dot notation (e.g., "Nested.Field").
func New[T any]() Struct[T] {
	s := Struct[T]{}

	s.fillSchema(nil, "", reflect.TypeFor[T]())

	return s
}

// Struct is a schema mapping from field paths to Field definitions for struct type T.
// Nested fields are addressed using dot notation (e.g., "Parent.Child").
type Struct[T any] map[string]Field[T]

// fillSchema recursively registers all exported fields of a struct, including pointers and nested structs.
func (s Struct[T]) fillSchema(indices []int, path string, t reflect.Type) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()

		indices = append(indices, -1)
	}

	s[path] = Field[T]{
		dstType:  t,
		indices:  indices,
		nullable: false,
	}

	if t.Kind() != reflect.Struct {
		return
	}

	for i := range t.NumField() {
		sf := t.Field(i)

		if !sf.IsExported() {
			continue
		}

		name := sf.Name

		if path != "" {
			name = path + "." + sf.Name
		}

		s.fillSchema(append(indices, sf.Index[0]), name, sf.Type)
	}
}

// access dereferences pointer chains and accesses the target field on a struct T using its field indices.
func access[T any](t *T, indices []int) reflect.Value {
	dst := reflect.ValueOf(t).Elem()

	for _, idx := range indices {
		if idx < 0 {
			if dst.IsNil() {
				dst.Set(reflect.New(dst.Type().Elem()))
			}

			dst = dst.Elem()

			continue
		}

		dst = dst.Field(idx)
	}

	return dst
}

// Field is a Scanner that uses the underlying field's type to scan values into T.
type Field[T any] struct {
	dstType  reflect.Type
	nullable bool
	indices  []int
}

// Scan returns a destination and a function to assign the scanned value to a struct field.
func (f Field[T]) Scan() (any, func(*T) error) {
	if f.nullable {
		src := reflect.New(reflect.PointerTo(f.dstType))

		return src.Interface(), func(t *T) error {
			elem := src.Elem()

			if !elem.IsValid() || elem.IsNil() {
				return nil
			}

			access(t, f.indices).Set(elem.Elem())

			return nil
		}
	}

	src := reflect.New(f.dstType)

	return src.Interface(), func(t *T) error {
		access(t, f.indices).Set(src.Elem())

		return nil
	}
}

// Nullable marks the field as nullable, allowing it to accept NULL values.
func (f Field[T]) Nullable() Field[T] {
	f.nullable = true

	return f
}

// MustString is like String but panics if an error occurs.
func (f Field[T]) MustString() ValueField[string, T] {
	return must(f.String())
}

// String scans into a string value and sets its value into the fields destination.
func (f Field[T]) String() (ValueField[string, T], error) {
	if f.dstType.Kind() != reflect.String {
		return ValueField[string, T]{}, fmt.Errorf("string: invalid type: %s", f.dstType)
	}

	return ValueField[string, T]{
		nullable:     f.nullable,
		indices:      f.indices,
		defaultValue: nil,
		set: func(dst reflect.Value, src string) error {
			dst.SetString(src)

			return nil
		},
	}, nil
}

// MustInt is like Int but panics if an error occurs.
func (f Field[T]) MustInt() ValueField[int64, T] {
	return must(f.Int())
}

// Int scans into an int64 and sets the value into the field's destination.
func (f Field[T]) Int() (ValueField[int64, T], error) {
	switch f.dstType.Kind() {
	default:
		return ValueField[int64, T]{}, fmt.Errorf("int: invalid type: %s", f.dstType)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return ValueField[int64, T]{
			nullable:     f.nullable,
			indices:      f.indices, // MustString is like String but panics if an error occurs.
			defaultValue: nil,
			set: func(dst reflect.Value, src int64) error {
				dst.SetInt(src)

				return nil
			},
		}, nil
	}
}

// MustFloat is like Float but panics if an error occurs.
func (f Field[T]) MustFloat() ValueField[float64, T] {
	return must(f.Float())
}

// Float scans into a float value and sets its value into the fields destination.
func (f Field[T]) Float() (ValueField[float64, T], error) {
	switch f.dstType.Kind() {
	default:
		return ValueField[float64, T]{}, fmt.Errorf("float: invalid type: %s", f.dstType)
	case reflect.Float32, reflect.Float64:
		return ValueField[float64, T]{
			nullable:     f.nullable,
			indices:      f.indices,
			defaultValue: nil,
			set: func(dst reflect.Value, src float64) error {
				dst.SetFloat(src)

				return nil
			},
		}, nil
	}
}

// MustBool is like Bool but panics if an error occurs.
func (f Field[T]) MustBool() ValueField[bool, T] {
	return must(f.Bool())
}

// Bool scans into a bool value and sets its value into the fields destination.
func (f Field[T]) Bool() (ValueField[bool, T], error) {
	switch f.dstType.Kind() {
	default:
		return ValueField[bool, T]{}, fmt.Errorf("bool: invalid type: %s", f.dstType)
	case reflect.Bool:
		return ValueField[bool, T]{
			nullable:     f.nullable,
			indices:      f.indices,
			defaultValue: nil,
			set: func(dst reflect.Value, src bool) error {
				dst.SetBool(src)

				return nil
			},
		}, nil
	}
}

// MustBytes is like Bytes but panics if an error occurs.
func (f Field[T]) MustBytes() ValueField[[]byte, T] {
	return must(f.Bytes())
}

// Bytes scans into a []byte value and sets its value into the fields destination.
func (f Field[T]) Bytes() (ValueField[[]byte, T], error) {
	if f.dstType == byteSliceType {
		return ValueField[[]byte, T]{
			nullable:     f.nullable,
			indices:      f.indices,
			defaultValue: nil,
			set: func(dst reflect.Value, src []byte) error {
				dst.SetBytes(src)

				return nil
			},
		}, nil
	}

	if byteSliceType.ConvertibleTo(f.dstType) {
		return ValueField[[]byte, T]{
			nullable:     f.nullable,
			indices:      f.indices,
			defaultValue: nil,
			set: func(dst reflect.Value, src []byte) error {
				dst.Set(reflect.ValueOf(src).Convert(f.dstType))

				return nil
			},
		}, nil
	}

	return ValueField[[]byte, T]{}, fmt.Errorf("bytes: invalid type: %s", f.dstType)
}

// MustTime is like Time but panics if an error occurs.
func (f Field[T]) MustTime() ValueField[time.Time, T] {
	return must(f.Time())
}

// Time scans into a time.Time value and sets its value into the fields destination.
func (f Field[T]) Time() (ValueField[time.Time, T], error) {
	if f.dstType == timeType {
		return ValueField[time.Time, T]{
			nullable:     f.nullable,
			indices:      f.indices,
			defaultValue: nil,
			set: func(dst reflect.Value, src time.Time) error {
				//nolint:gosec
				*(*time.Time)(unsafe.Pointer(dst.UnsafeAddr())) = src

				return nil
			},
		}, nil
	}

	if timeType.ConvertibleTo(f.dstType) {
		return ValueField[time.Time, T]{
			nullable:     f.nullable,
			indices:      f.indices,
			defaultValue: nil,
			set: func(dst reflect.Value, src time.Time) error {
				dst.Set(reflect.ValueOf(src).Convert(f.dstType))

				return nil
			},
		}, nil
	}

	return ValueField[time.Time, T]{}, fmt.Errorf("time: invalid type: %s", f.dstType)
}

// MustSplit is like Split but panics if an error occurs.
func (f Field[T]) MustSplit(sep string) ValueField[string, T] {
	return must(f.Split(sep))
}

// Split parses a delimited string and assigns the parts to a slice or array field.
// Supports slices and fixed-length arrays of strings.
//
//nolint:cyclop,funlen
func (f Field[T]) Split(sep string) (ValueField[string, T], error) {
	if f.dstType == stringSliceType {
		return ValueField[string, T]{
			nullable:     f.nullable,
			indices:      f.indices,
			defaultValue: nil,
			set: func(dst reflect.Value, src string) error {
				if src == "" {
					return nil
				}

				dst.Set(reflect.ValueOf(strings.Split(src, sep)))

				return nil
			},
		}, nil
	}

	switch f.dstType.Kind() {
	case reflect.Slice:
		levels, elem := indirections(f.dstType.Elem())

		if elem.Kind() == reflect.String {
			return ValueField[string, T]{
				nullable:     f.nullable,
				indices:      f.indices,
				defaultValue: nil,
				set: func(dst reflect.Value, src string) error {
					if src == "" {
						return nil
					}

					parts := strings.Split(src, sep)

					dst.Set(reflect.MakeSlice(f.dstType, len(parts), len(parts)))

					for i, p := range parts {
						deref(dst.Index(i), levels).SetString(p)
					}

					return nil
				},
			}, nil
		}
	case reflect.Array:
		levels, elem := indirections(f.dstType.Elem())

		if elem.Kind() == reflect.String {
			return ValueField[string, T]{
				nullable:     f.nullable,
				indices:      f.indices,
				defaultValue: nil,
				set: func(dst reflect.Value, src string) error {
					if src == "" {
						return nil
					}

					parts := strings.Split(src, sep)

					if len(parts) > f.dstType.Len() {
						return fmt.Errorf("split: too many elements for type %s: %v", f.dstType, len(parts))
					}

					for i, p := range parts {
						deref(dst.Index(i), levels).SetString(p)
					}

					return nil
				},
			}, nil
		}
	}

	return ValueField[string, T]{}, fmt.Errorf("split: invalid type: %s", f.dstType)
}

// MustParseInt is like ParseInt but panics if an error occurs.
func (f Field[T]) MustParseInt(base int, bitSize int) ValueField[string, T] {
	return must(f.ParseInt(base, bitSize))
}

// ParseInt scans into a string value, parses its value and sets the result into the fields destination.
func (f Field[T]) ParseInt(base int, bitSize int) (ValueField[string, T], error) {
	switch f.dstType.Kind() {
	default:
		return ValueField[string, T]{}, fmt.Errorf("parse int: invalid type: %s", f.dstType)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return ValueField[string, T]{
			nullable:     f.nullable,
			indices:      f.indices,
			defaultValue: nil,
			set: func(dst reflect.Value, src string) error {
				val, err := strconv.ParseInt(src, base, bitSize)
				if err != nil {
					return err
				}

				dst.SetInt(val)

				return nil
			},
		}, nil
	}
}

// MustParseUint is like ParseUint but panics if an error occurs.
func (f Field[T]) MustParseUint(base int, bitSize int) ValueField[string, T] {
	return must(f.ParseUint(base, bitSize))
}

// ParseUint scans into a string value, parses its value and sets the result into the fields destination.
func (f Field[T]) ParseUint(base int, bitSize int) (ValueField[string, T], error) {
	switch f.dstType.Kind() {
	default:
		return ValueField[string, T]{}, fmt.Errorf("parse uint: invalid type: %s", f.dstType)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return ValueField[string, T]{
			nullable:     f.nullable,
			indices:      f.indices,
			defaultValue: nil,
			set: func(dst reflect.Value, src string) error {
				val, err := strconv.ParseUint(src, base, bitSize)
				if err != nil {
					return err
				}

				dst.SetUint(val)

				return nil
			},
		}, nil
	}
}

// MustParseFloat is like ParseFloat but panics if an error occurs.
func (f Field[T]) MustParseFloat(bitSize int) ValueField[string, T] {
	return must(f.ParseFloat(bitSize))
}

// ParseFloat scans into a string value, parses its value and sets the result into the fields destination.
func (f Field[T]) ParseFloat(bitSize int) (ValueField[string, T], error) {
	switch f.dstType.Kind() {
	default:
		return ValueField[string, T]{}, fmt.Errorf("parse float: invalid type: %s", f.dstType)
	case reflect.Float32, reflect.Float64:
		return ValueField[string, T]{
			nullable:     f.nullable,
			indices:      f.indices,
			defaultValue: nil,
			set: func(dst reflect.Value, src string) error {
				val, err := strconv.ParseFloat(src, bitSize)
				if err != nil {
					return err
				}

				dst.SetFloat(val)

				return nil
			},
		}, nil
	}
}

// MustParseComplex is like ParseComplex but panics if an error occurs.
func (f Field[T]) MustParseComplex(bitSize int) ValueField[string, T] {
	return must(f.ParseComplex(bitSize))
}

// ParseComplex scans into a string value, parses its value and sets the result into the fields destination.
func (f Field[T]) ParseComplex(bitSize int) (ValueField[string, T], error) {
	switch f.dstType.Kind() {
	default:
		return ValueField[string, T]{}, fmt.Errorf("parse complex: invalid type: %s", f.dstType)
	case reflect.Complex128, reflect.Complex64:
		return ValueField[string, T]{
			nullable:     f.nullable,
			indices:      f.indices,
			defaultValue: nil,
			set: func(dst reflect.Value, src string) error {
				val, err := strconv.ParseComplex(src, bitSize)
				if err != nil {
					return err
				}

				dst.SetComplex(val)

				return nil
			},
		}, nil
	}
}

// MustParseBool is like ParseBool but panics if an error occurs.
func (f Field[T]) MustParseBool() ValueField[string, T] {
	return must(f.ParseBool())
}

// ParseBool scans into a string value, parses its value and sets the result into the fields destination.
func (f Field[T]) ParseBool() (ValueField[string, T], error) {
	switch f.dstType.Kind() {
	default:
		return ValueField[string, T]{}, fmt.Errorf("parse bool: invalid type: %s", f.dstType)
	case reflect.Bool:
		return ValueField[string, T]{
			nullable:     f.nullable,
			indices:      f.indices,
			defaultValue: nil,
			set: func(dst reflect.Value, src string) error {
				val, err := strconv.ParseBool(src)
				if err != nil {
					return err
				}

				dst.SetBool(val)

				return nil
			},
		}, nil
	}
}

// MustParseTime is like ParseTime but panics if an error occurs.
func (f Field[T]) MustParseTime(layout string) ValueField[string, T] {
	return must(f.ParseTime(layout))
}

// ParseTime scans into a string value, parses its value and sets the result into the fields destination.
func (f Field[T]) ParseTime(layout string) (ValueField[string, T], error) {
	if f.dstType == timeType {
		return ValueField[string, T]{
			nullable:     f.nullable,
			indices:      f.indices,
			defaultValue: nil,
			set: func(dst reflect.Value, src string) error {
				val, err := time.Parse(layout, src)
				if err != nil {
					return err
				}

				dst.Set(reflect.ValueOf(val))

				return nil
			},
		}, nil
	}

	if timeType.ConvertibleTo(f.dstType) {
		return ValueField[string, T]{
			nullable:     f.nullable,
			indices:      f.indices,
			defaultValue: nil,
			set: func(dst reflect.Value, src string) error {
				val, err := time.Parse(layout, src)
				if err != nil {
					return err
				}

				dst.Set(reflect.ValueOf(val).Convert(f.dstType))

				return nil
			},
		}, nil
	}

	return ValueField[string, T]{}, fmt.Errorf("parse time: invalid type: %s", f.dstType)
}

// MustParseTimeInLocation is like ParseTimeInLocation but panics if an error occurs.
func (f Field[T]) MustParseTimeInLocation(layout string, loc *time.Location) ValueField[string, T] {
	return must(f.ParseTimeInLocation(layout, loc))
}

// ParseTimeInLocation scans into a string value, parses its value and sets the result into the fields destination.
func (f Field[T]) ParseTimeInLocation(layout string, loc *time.Location) (ValueField[string, T], error) {
	if f.dstType == timeType {
		return ValueField[string, T]{
			nullable:     f.nullable,
			indices:      f.indices,
			defaultValue: nil,
			set: func(dst reflect.Value, src string) error {
				val, err := time.ParseInLocation(layout, src, loc)
				if err != nil {
					return err
				}

				dst.Set(reflect.ValueOf(val))

				return nil
			},
		}, nil
	}

	if timeType.ConvertibleTo(f.dstType) {
		return ValueField[string, T]{
			nullable:     f.nullable,
			indices:      f.indices,
			defaultValue: nil,
			set: func(dst reflect.Value, src string) error {
				val, err := time.ParseInLocation(layout, src, loc)
				if err != nil {
					return err
				}

				dst.Set(reflect.ValueOf(val).Convert(f.dstType))

				return nil
			},
		}, nil
	}

	return ValueField[string, T]{}, fmt.Errorf("parse time in location: invalid type: %s", f.dstType)
}

// UnmarshalJSON scans into a []byte value, unmarshals its value into the fields destination.
//
//nolint:govet
func (f Field[T]) UnmarshalJSON() ValueField[[]byte, T] {
	return ValueField[[]byte, T]{
		nullable:     f.nullable,
		indices:      f.indices,
		defaultValue: nil,
		set: func(dst reflect.Value, src []byte) error {
			return json.Unmarshal(src, dst.Addr().Interface())
		},
	}
}

// MustUnmarshalText is like UnmarshalText but panics if an error occurs.
func (f Field[T]) MustUnmarshalText() ValueField[[]byte, T] {
	return must(f.UnmarshalText())
}

// UnmarshalText scans into a []byte value, unmarshals its value into the fields destination.
func (f Field[T]) UnmarshalText() (ValueField[[]byte, T], error) {
	if !reflect.PointerTo(f.dstType).Implements(textUnmarshalerType) {
		return ValueField[[]byte, T]{}, fmt.Errorf("unmarshal text: invalid type: %s", f.dstType)
	}

	return ValueField[[]byte, T]{
		nullable:     f.nullable,
		indices:      f.indices,
		defaultValue: nil,
		set: func(dst reflect.Value, src []byte) error {
			//nolint:forcetypeassert
			return dst.Addr().Interface().(encoding.TextUnmarshaler).UnmarshalText(src)
		},
	}, nil
}

// MustUnmarshalBinary is like UnmarshalBinary but panics if an error occurs.
func (f Field[T]) MustUnmarshalBinary() ValueField[[]byte, T] {
	return must(f.UnmarshalBinary())
}

// UnmarshalBinary scans into a []byte value, unmarshals its value into the fields destination.
func (f Field[T]) UnmarshalBinary() (ValueField[[]byte, T], error) {
	if !reflect.PointerTo(f.dstType).Implements(binaryUnmarshalerType) {
		return ValueField[[]byte, T]{}, fmt.Errorf("unmarshal binary: invalid type: %s", f.dstType)
	}

	return ValueField[[]byte, T]{
		nullable:     f.nullable,
		indices:      f.indices,
		defaultValue: nil,
		set: func(dst reflect.Value, src []byte) error {
			//nolint:forcetypeassert
			return dst.Addr().Interface().(encoding.BinaryUnmarshaler).UnmarshalBinary(src)
		},
	}, nil
}

// Enum defines a string ↔ int mapping used by IntEnum and StringEnum transformations.
type Enum struct {
	String string
	Int    int64
}

// MustIntEnum is like IntEnum but panics if an error occurs.
func (f Field[T]) MustIntEnum(enums ...Enum) ValueField[string, T] {
	return must(f.IntEnum(enums...))
}

// IntEnum scans into a string value and sets the corresponding int value to the fields destination.
func (f Field[T]) IntEnum(enums ...Enum) (ValueField[string, T], error) {
	switch f.dstType.Kind() {
	default:
		return ValueField[string, T]{}, fmt.Errorf("int enum: invalid type: %s", f.dstType)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
	}

	mapping := map[string]int64{}

	for _, p := range enums {
		mapping[p.String] = p.Int
	}

	return ValueField[string, T]{
		nullable:     f.nullable,
		indices:      f.indices,
		defaultValue: nil,
		set: func(dst reflect.Value, src string) error {
			val, ok := mapping[src]
			if !ok {
				return fmt.Errorf("int enum: invalid value: %v", src)
			}

			dst.SetInt(val)

			return nil
		},
	}, nil
}

// MustStringEnum is like StringEnum but panics if an error occurs.
func (f Field[T]) MustStringEnum(enums ...Enum) ValueField[int64, T] {
	return must(f.StringEnum(enums...))
}

// StringEnum scans into a int value and sets the corresponding string value to the fields destination.
func (f Field[T]) StringEnum(enums ...Enum) (ValueField[int64, T], error) {
	if f.dstType.Kind() != reflect.String {
		return ValueField[int64, T]{}, fmt.Errorf("string enum: invalid type: %s", f.dstType)
	}

	mapping := map[int64]string{}

	for _, p := range enums {
		mapping[p.Int] = p.String
	}

	return ValueField[int64, T]{
		nullable:     f.nullable,
		indices:      f.indices,
		defaultValue: nil,
		set: func(dst reflect.Value, src int64) error {
			val, ok := mapping[src]
			if !ok {
				return fmt.Errorf("string enum: invalid value: %v", src)
			}

			dst.SetString(val)

			return nil
		},
	}, nil
}

// ValueField implements the Scanner interface.
type ValueField[S, T any] struct {
	nullable     bool
	indices      []int
	defaultValue *S
	set          func(dst reflect.Value, src S) error
}

// Scan returns a destination and a function to assign the scanned value to a struct field.
func (f ValueField[S, T]) Scan() (any, func(*T) error) {
	if f.nullable {
		var src sql.Null[S]

		return &src, func(t *T) error {
			if !src.Valid {
				if f.defaultValue != nil {
					return f.set(access(t, f.indices), *f.defaultValue)
				}

				return nil
			}

			return f.set(access(t, f.indices), src.V)
		}
	}

	var src S

	return &src, func(t *T) error {
		return f.set(access(t, f.indices), src)
	}
}

// Nullable marks the field as nullable, allowing it to accept NULL values.
func (f ValueField[S, T]) Nullable() ValueField[S, T] {
	f.nullable = true

	return f
}

// Default sets a fallback value that is used if the scanned value is NULL.
func (f ValueField[S, T]) Default(value S) ValueField[S, T] {
	f.defaultValue = &value
	f.nullable = true

	return f
}

func combineErrs(err1, err2 error) error {
	if err1 == nil {
		return err2
	}

	if err2 == nil {
		return err1
	}

	return errors.Join(err1, err2)
}

func must[T any](t T, err error) T {
	if err != nil {
		panic(err)
	}

	return t
}

//nolint:gochecknoglobals
var (
	byteSliceType         = reflect.TypeFor[[]byte]()
	timeType              = reflect.TypeFor[time.Time]()
	textUnmarshalerType   = reflect.TypeFor[encoding.TextUnmarshaler]()
	binaryUnmarshalerType = reflect.TypeFor[encoding.BinaryUnmarshaler]()
	stringSliceType       = reflect.TypeFor[[]string]()
)

func indirections(t reflect.Type) (int, reflect.Type) {
	var levels int

	for t.Kind() == reflect.Pointer {
		t = t.Elem()
		levels++
	}

	return levels, t
}

func deref(v reflect.Value, levels int) reflect.Value {
	for range levels {
		v.Set(reflect.New(v.Type().Elem()))

		v = v.Elem()
	}

	return v
}
