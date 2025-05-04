// Package structscan provides a flexible and composable mechanism for scanning SQL query results
// into Go structs using reflection-based schema definitions and type-safe field bindings.
//
// It is designed to work with the standard `database/sql` package and supports optional and required fields,
// automatic decoding of strings, numbers, booleans, JSON blobs, URLs, and custom types via interfaces such as
// encoding.TextUnmarshaler and encoding.BinaryUnmarshaler.
//
// Example:
//
// package main
//
// import (
//
//	"database/sql"
//	"fmt"
//	"math/big"
//	"net/url"
//	"time"
//
//	"github.com/go-sqlt/structscan"
//	_ "modernc.org/sqlite"
//
// )
//
//	type Data struct {
//		Int      int64
//		String   *string
//		Bool     bool
//		Time     time.Time
//		Big      *big.Int
//		URL      *url.URL
//		IntSlice []int32
//		JSON     map[string]any
//	}
//
// // Schema holds a reflection-based description of the Data type.
// // This provides addressable access to fields by name, for mapping values.
// var Schema = structscan.Describe[Data]()
//
//	func main() {
//		db, err := sql.Open("sqlite", ":memory:")
//		if err != nil {
//			panic(err)
//		}
//
//		rows, err := db.Query(`
//			SELECT
//				100                                    -- Int (int64)
//				, '200'                                -- String (*string)
//				, true                                 -- Bool (bool)
//				, '2025-05-01'                         -- Time (parsed from string)
//				, '300'                                -- Big (decoded from text)
//				, 'https://example.com/path?query=yes' -- URL (decoded from binary)
//				, '400,500,600'                        -- IntSlice (comma-separated ints)
//				, '{"hello":"world"}'                  -- JSON (parsed into a map)
//		`)
//		if err != nil {
//			panic(err)
//		}
//
//		// Use structscan to scan the row into a slice of Data structs.
//		// Each field maps to a column, with optional decoding/parsing behavior.
//		data, err := structscan.All(rows,
//			// Scans an int64 value into the Int field.
//			Schema["Int"],
//
//			// Fails if the value is NULL.
//			Schema["String"].Required(),
//
//			// Scans a boolean directly.
//			Schema["Bool"].Bool(),
//
//			// Parses a date string (in 'YYYY-MM-DD' format) into a time.Time.
//			Schema["Time"].String(structscan.ParseTime(time.DateOnly, time.UTC)),
//
//			// Scans raw bytes and decodes them using encoding.TextUnmarshaler.
//			// In this case, it populates a *big.Int from string like "300".
//			Schema["Big"].Bytes(structscan.UnmarshalText()),
//
//			// Decodes binary input into a *url.URL using encoding.BinaryUnmarshaler.
//			Schema["URL"].Bytes(structscan.UnmarshalBinary()),
//
//			// Splits a comma-separated string and parses each part into an int32 slice.
//			Schema["IntSlice"].String(structscan.Split(",", structscan.ParseInt(10, 32))),
//
//			// Scans bytes and parses them as JSON into a map[string]any.
//			Schema["JSON"].Bytes(structscan.UnmarshalJSON()),
//		)
//		if err != nil {
//			panic(err)
//		}
//
//		fmt.Println(data)
//		// [{100 0x1400012c240 true 2025-05-01 00:00:00 +0000 UTC 300 https://example.com/path?query=yes [400 500 600] map[hello:world]}]
//	}
package structscan

import (
	"database/sql"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Scanner defines an interface for scanning values from SQL rows into a struct of type T.
// It returns a destination value for sql.Rows.Scan, a setter to populate the struct, and any error encountered.
type Scanner[T any] interface {
	Scan() (any, func(*T) error, error)
}

// ScanFunc is a functional implementation of Scanner.
// Useful for wrapping inline scan logic without defining a full struct.
type ScanFunc[T any] func() (any, func(*T) error, error)

// Scan implements the Scanner interface.
func (s ScanFunc[T]) Scan() (any, func(*T) error, error) {
	return s()
}

// All reads all rows from the given *sql.Rows and populates a slice of T.
// Each row is scanned using the provided Scanner implementations.
func All[T any](rows *sql.Rows, scanners ...Scanner[T]) ([]T, error) {
	dest, set, err := destSet(scanners...)
	if err != nil {
		return nil, twoErr(err, rows.Close())
	}

	var all []T

	for rows.Next() {
		if err = rows.Scan(dest...); err != nil {
			return nil, twoErr(err, rows.Close())
		}

		var t T

		if err = set(&t); err != nil {
			return nil, twoErr(err, rows.Close())
		}

		all = append(all, t)
	}

	return all, twoErr(rows.Err(), rows.Close())
}

var ErrTooManyRows = errors.New("too many rows")

// One scans a single row from *sql.Rows into a value of type T.
// Returns an error if there are no rows or more than one.
func One[T any](rows *sql.Rows, scanners ...Scanner[T]) (T, error) {
	var one T

	dest, set, err := destSet(scanners...)
	if err != nil {
		return one, twoErr(err, rows.Close())
	}

	if !rows.Next() {
		return one, twoErr(sql.ErrNoRows, rows.Close())
	}

	if err = rows.Scan(dest...); err != nil {
		return one, twoErr(err, rows.Close())
	}

	if err = set(&one); err != nil {
		return one, twoErr(err, rows.Close())
	}

	if rows.Next() {
		return one, twoErr(ErrTooManyRows, rows.Close())
	}

	return one, twoErr(rows.Err(), rows.Close())
}

// First scans the result of *sql.Row into a value of type T.
// This is useful for single-row queries like `QueryRowContext`.
// Returns an error if scanning or assignment fails.
func First[T any](row *sql.Row, scanners ...Scanner[T]) (T, error) {
	var first T

	dest, set, err := destSet(scanners...)
	if err != nil {
		return first, err
	}

	if err = row.Scan(dest...); err != nil {
		return first, err
	}

	if err = set(&first); err != nil {
		return first, err
	}

	return first, nil
}

func destSet[T any](scanners ...Scanner[T]) ([]any, func(*T) error, error) {
	if len(scanners) == 0 {
		scanners = []Scanner[T]{
			ScanFunc[T](func() (any, func(*T) error, error) {
				var src *T

				return &src, func(t *T) error {
					if src == nil {
						return nil
					}

					*t = *src

					return nil
				}, nil
			}),
		}
	}

	var (
		values  = make([]any, len(scanners))
		setters = make([]func(*T) error, len(scanners))
		err     error
	)

	for i, s := range scanners {
		values[i], setters[i], err = s.Scan()
		if err != nil {
			return nil, nil, err
		}
	}

	return values, func(t *T) error {
		for _, s := range setters {
			if s != nil {
				if err := s(t); err != nil {
					return err
				}
			}
		}

		return nil
	}, nil
}

// Describe analyzes the structure of type T using reflection and returns a Schema,
// which maps field paths to Field objects representing metadata and accessors for each field.
func Describe[T any]() Schema[T] {
	s := Schema[T]{}

	fillSchema(s, nil, "", reflect.TypeFor[T]())

	return s
}

func fillSchema[T any](s Schema[T], indices []int, path string, t reflect.Type) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()

		indices = append(indices, -1)

		continue
	}

	s[path] = &Field[T]{
		typ:      t,
		indices:  indices,
		required: !slices.Contains(indices, -1),
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

		fillSchema(s, append(indices, sf.Index[0]), name, sf.Type)
	}
}

// Schema maps field names (including nested paths like "User.Email")
// to *Field definitions describing how to access and bind those fields.
type Schema[T any] map[string]*Field[T]

// Field is a helper function for usage in sqlt templates.
func (s Schema[T]) Field(path string) *Field[T] {
	f, ok := s[path]
	if !ok {
		return nil
	}

	return f
}

// Field contains metadata about a struct field, including its type, index path in the struct,
// and whether it is required (i.e., cannot be nil). Used for scanning and binding.
type Field[T any] struct {
	typ      reflect.Type
	indices  []int
	required bool
}

// Optional marks the field as optional (nullable).
// This affects how nils are treated during scanning.
// If a SQL value is NULL, the field will remain zero-valued.
func (f *Field[T]) Optional() *Field[T] {
	return &Field[T]{
		typ:      f.typ,
		indices:  f.indices,
		required: false,
	}
}

// Required marks the field as required (non-nullable).
// If a SQL value is NULL, the field will be zeroed and scanning will continue,
// but you may later enforce presence through validation.
func (f *Field[T]) Required() *Field[T] {
	return &Field[T]{
		typ:      f.typ,
		indices:  f.indices,
		required: true,
	}
}

// Value returns the reflect.Value of the field within the given struct pointer t.
// It walks through nested fields and pointer layers using the field's index path.
func (f *Field[T]) Value(t *T) reflect.Value {
	dst := reflect.ValueOf(t).Elem()

	for _, idx := range f.indices {
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

// Scan returns a value to be scanned into and a function to apply that value to the field.
// The behavior differs based on whether the field is marked required.
func (f *Field[T]) Scan() (any, func(*T) error, error) {
	if f.required {
		src := reflect.New(f.typ).Elem()

		return src.Addr().Interface(), func(t *T) error {
			f.Value(t).Set(src)

			return nil
		}, nil
	}

	ptr := reflect.New(reflect.PointerTo(f.typ))

	return ptr.Interface(), func(t *T) error {
		if ptr.IsNil() {
			return nil
		}

		elem := ptr.Elem()
		if elem.IsNil() {
			return nil
		}

		f.Value(t).Set(elem.Elem())

		return nil
	}, nil
}

// String binds a field expecting a string value, optionally using decoders to convert or validate input.
func (f *Field[T]) String(decoders ...Decoder[string]) Scanner[T] {
	return bind(f, decoders...)
}

var (
	rawMessageType = reflect.TypeFor[json.RawMessage]()
	byteSliceType  = reflect.TypeFor[[]byte]()
)

// UnmarshalJSON returns a Decoder that unmarshals JSON-encoded []byte into a compatible Go type.
// Supports json.RawMessage, []byte, or types implementing json.Unmarshaler.
func UnmarshalJSON() Decoder[[]byte] {
	return func(dstType reflect.Type) (Assign[[]byte], error) {
		switch dstType {
		case rawMessageType, byteSliceType:
			return func(dst reflect.Value, src []byte) error {
				if !json.Valid(src) {
					return fmt.Errorf("unmarshal json: invalid json encoding: %s", string(src))
				}

				dst.SetBytes(src)

				return nil
			}, nil
		}

		return func(dst reflect.Value, src []byte) error {
			return json.Unmarshal(src, dst.Addr().Interface())
		}, nil
	}
}

var textUnmarshalerType = reflect.TypeFor[encoding.TextUnmarshaler]()

// UnmarshalText returns a Decoder that uses encoding.TextUnmarshaler to decode byte slices.
// Useful for types like *big.Int or *time.Duration that support text-based decoding.
func UnmarshalText() Decoder[[]byte] {
	return func(dstType reflect.Type) (Assign[[]byte], error) {
		if reflect.PointerTo(dstType).Implements(textUnmarshalerType) {
			return func(dst reflect.Value, src []byte) error {
				i, ok := dst.Addr().Interface().(encoding.TextUnmarshaler)
				if !ok {
					return fmt.Errorf("unmarshal text: invalid type: %s", dst.Type())
				}

				return i.UnmarshalText(src)
			}, nil
		}

		return nil, fmt.Errorf("unmarshal text: invalid type %s", dstType)
	}
}

var binaryUnmarshalerType = reflect.TypeFor[encoding.BinaryUnmarshaler]()

// UnmarshalBinary returns a Decoder that uses encoding.BinaryUnmarshaler to decode byte slices.
func UnmarshalBinary() Decoder[[]byte] {
	return func(dstType reflect.Type) (Assign[[]byte], error) {
		if reflect.PointerTo(dstType).Implements(binaryUnmarshalerType) {
			return func(dst reflect.Value, src []byte) error {
				i, ok := dst.Addr().Interface().(encoding.BinaryUnmarshaler)
				if !ok {
					return fmt.Errorf("unmarshal binary: invalid type: %s", dst.Type())
				}

				return i.UnmarshalBinary(src)
			}, nil
		}

		return nil, fmt.Errorf("unmarshal binary: invalid type %s", dstType)
	}
}

// Time binds a field expecting a time.Time value.
func (f *Field[T]) Time(decoders ...Decoder[time.Time]) Scanner[T] {
	return bind(f, decoders...)
}

// Bytes binds a field expecting a []byte value.
func (f *Field[T]) Bytes(decoders ...Decoder[[]byte]) Scanner[T] {
	return bind(f, decoders...)
}

// Int binds a field expecting an int64 value.
func (f *Field[T]) Int(decoders ...Decoder[int64]) Scanner[T] {
	return bind(f, decoders...)
}

// Uint binds a field expecting a uint64 value.
func (f *Field[T]) Uint(decoders ...Decoder[uint64]) Scanner[T] {
	return bind(f, decoders...)
}

// Float binds a field expecting a float64 value.
func (f *Field[T]) Float(decoders ...Decoder[float64]) Scanner[T] {
	return bind(f, decoders...)
}

// Bool binds a field expecting a boolean value.
func (f *Field[T]) Bool(decoders ...Decoder[bool]) Scanner[T] {
	return bind(f, decoders...)
}

var timeType = reflect.TypeFor[time.Time]()

// ParseTime returns a Decoder that parses time strings using the specified layout and location.
// Useful for scanning string-based date/time formats into time.Time fields.
func ParseTime(layout string, loc *time.Location) Decoder[string] {
	return func(dstType reflect.Type) (Assign[string], error) {
		if dstType == timeType {
			return func(dst reflect.Value, src string) error {
				v, err := time.ParseInLocation(layout, src, loc)
				if err != nil {
					return err
				}

				dst.Set(reflect.ValueOf(v))

				return nil
			}, nil
		}

		if timeType.ConvertibleTo(dstType) {
			return func(dst reflect.Value, src string) error {
				v, err := time.ParseInLocation(layout, src, loc)
				if err != nil {
					return err
				}

				dst.Set(reflect.ValueOf(v).Convert(dstType))

				return nil
			}, nil
		}

		return nil, fmt.Errorf("parse time: invalid type %s", dstType)
	}
}

// ParseInt returns a Decoder that parses strings into int64 values.
// The base (e.g. 10) and bit size (e.g. 64) determine valid input formats.
func ParseInt(base int, bitSize int) Decoder[string] {
	return func(dstType reflect.Type) (Assign[string], error) {
		switch dstType.Kind() {
		default:
			return nil, fmt.Errorf("parse int: invalid type %s", dstType)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return func(dst reflect.Value, src string) error {
				v, err := strconv.ParseInt(src, base, bitSize)
				if err != nil {
					return err
				}

				dst.SetInt(v)

				return nil
			}, nil
		}
	}
}

// ParseUint returns a Decoder that parses strings into unsigned integers.
func ParseUint(base int, bitSize int) Decoder[string] {
	return func(dstType reflect.Type) (Assign[string], error) {
		switch dstType.Kind() {
		default:
			return nil, fmt.Errorf("parse uint: invalid type %s", dstType)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return func(dst reflect.Value, src string) error {
				v, err := strconv.ParseUint(src, base, bitSize)
				if err != nil {
					return err
				}

				dst.SetUint(v)

				return nil
			}, nil
		}
	}
}

// ParseFloat returns a Decoder that parses strings into float32 or float64.
func ParseFloat(bitSize int) Decoder[string] {
	return func(dstType reflect.Type) (Assign[string], error) {
		switch dstType.Kind() {
		default:
			return nil, fmt.Errorf("parse float: invalid type %s", dstType)
		case reflect.Float32, reflect.Float64:
			return func(dst reflect.Value, src string) error {
				v, err := strconv.ParseFloat(src, bitSize)
				if err != nil {
					return err
				}

				dst.SetFloat(v)

				return nil
			}, nil
		}
	}
}

// ParseBool returns a Decoder that parses strings into boolean values (true/false).
func ParseBool() Decoder[string] {
	return func(dstType reflect.Type) (Assign[string], error) {
		switch dstType.Kind() {
		default:
			return nil, fmt.Errorf("parse bool: invalid type %s", dstType)
		case reflect.Bool:
			return func(dst reflect.Value, src string) error {
				v, err := strconv.ParseBool(src)
				if err != nil {
					return err
				}

				dst.SetBool(v)

				return nil
			}, nil
		}
	}
}

// ParseComplex returns a Decoder that parses complex numbers from string input.
// Supports complex64 and complex128 depending on bitSize.
func ParseComplex(bitSize int) Decoder[string] {
	return func(dstType reflect.Type) (Assign[string], error) {
		switch dstType.Kind() {
		default:
			return nil, fmt.Errorf("parse complex: invalid type %s", dstType)
		case reflect.Complex64, reflect.Complex128:
			return func(dst reflect.Value, src string) error {
				v, err := strconv.ParseComplex(src, bitSize)
				if err != nil {
					return err
				}

				dst.SetComplex(v)

				return nil
			}, nil
		}
	}
}

// Decoder defines a function that takes a destination type and returns
// an Assign function to convert and assign a decoded value.
type Decoder[V any] func(dstType reflect.Type) (Assign[V], error)

func noAssign[V any](dstType reflect.Type) (Assign[V], error) {
	v := reflect.TypeFor[V]()

	switch v.Kind() {
	case reflect.String:
		if dstType.Kind() == reflect.String {
			return func(dst reflect.Value, src V) error {
				dst.SetString(any(src).(string))

				return nil
			}, nil
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch dstType.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return func(dst reflect.Value, src V) error {
				dst.SetInt(any(src).(int64))

				return nil
			}, nil
		}
	case reflect.Bool:
		if dstType.Kind() == reflect.Bool {
			return func(dst reflect.Value, src V) error {
				dst.SetBool(any(src).(bool))

				return nil
			}, nil
		}
	case reflect.Float32, reflect.Float64:
		switch dstType.Kind() {
		case reflect.Float32, reflect.Float64:
			return func(dst reflect.Value, src V) error {
				dst.SetFloat(any(src).(float64))

				return nil
			}, nil
		}
	}

	if dstType == v {
		return func(dst reflect.Value, src V) error {
			dst.SetBytes(any(src).([]byte))

			return nil
		}, nil
	}

	if v.ConvertibleTo(dstType) {
		return func(dst reflect.Value, src V) error {
			dst.Set(reflect.ValueOf(src).Convert(dstType))

			return nil
		}, nil
	}

	return nil, fmt.Errorf("cannot convert %s to %s", v, dstType)
}

// Assign defines a function that sets a decoded value of type V into a reflect.Value.
// Used to abstract away reflection logic in field assignment.
type Assign[V any] func(dst reflect.Value, src V) error

func bind[V, T any](field *Field[T], decoders ...Decoder[V]) Scanner[T] {
	if len(decoders) == 0 {
		assign, err := noAssign[V](field.typ)
		if err != nil {
			return errorScanner[T]{
				err: err,
			}
		}

		return assignScanner[V, T]{
			field:  field,
			assign: assign,
		}
	}

	assign, err := decoders[0](field.typ)
	if err != nil {
		return errorScanner[T]{
			err: err,
		}
	}

	return assignScanner[V, T]{
		field:  field,
		assign: assign,
	}
}

type errorScanner[T any] struct {
	err error
}

func (s errorScanner[T]) Scan() (any, func(t *T) error, error) {
	return nil, nil, s.err
}

type assignScanner[V, T any] struct {
	field  *Field[T]
	assign Assign[V]
}

func (s assignScanner[V, T]) Scan() (any, func(t *T) error, error) {
	if s.field.required {
		var src V

		return &src, func(t *T) error {
			return s.assign(s.field.Value(t), src)
		}, nil
	}

	var src *V

	return &src, func(t *T) error {
		if src == nil {
			return nil
		}

		return s.assign(s.field.Value(t), *src)
	}, nil
}

var stringSliceType = reflect.TypeFor[[]string]()

// Split returns a Decoder that splits a string by the given separator and applies another decoder
// to each split value. Useful for scanning into slices of strings, ints, etc.
func Split(sep string, decoders ...Decoder[string]) Decoder[string] {
	var decoder Decoder[string]

	if len(decoders) > 0 {
		decoder = decoders[0]
	} else {
		decoder = func(dstType reflect.Type) (Assign[string], error) {
			switch dstType.Kind() {
			default:
				return nil, fmt.Errorf("string: invalid type %s", dstType)
			case reflect.String:
				return func(dst reflect.Value, src string) error {
					dst.SetString(src)

					return nil
				}, nil
			}
		}
	}

	return func(dstType reflect.Type) (Assign[string], error) {
		if dstType == stringSliceType {
			return func(dst reflect.Value, src string) error {
				if src == "" {
					return nil
				}

				dst.Set(reflect.ValueOf(strings.Split(src, sep)))

				return nil
			}, nil
		}

		if dstType.Kind() != reflect.Slice {
			return nil, fmt.Errorf("split: invalid type %s", dstType)
		}

		var indirections int

		elem := dstType.Elem()
		for elem.Kind() == reflect.Pointer {
			elem = elem.Elem()

			indirections++

			continue
		}

		apply, err := decoder(elem)
		if err != nil {
			return nil, err
		}

		return func(dst reflect.Value, src string) error {
			split := strings.Split(src, sep)

			dst.Set(reflect.MakeSlice(dstType, len(split), len(split)))

			for i, v := range split {
				index := dst.Index(i)

				for range indirections {
					if index.IsNil() {
						index.Set(reflect.New(index.Type().Elem()))
					}

					index = index.Elem()
				}

				if err = apply(index, v); err != nil {
					return err
				}
			}

			return nil
		}, nil
	}
}

func twoErr(err1, err2 error) error {
	if err1 == nil {
		return err2
	}

	if err2 == nil {
		return err1
	}

	return errors.Join(err1, err2)
}
