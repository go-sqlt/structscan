// Package structscan is a lightweight Go library that maps SQL query results to Go structs.
//
/*

package main

import (
	"database/sql"
	"fmt"

	"github.com/go-sqlt/structscan"
	_ "modernc.org/sqlite"
)

type Data struct {
	ID   int64
	Bool bool
}

func main() {
	scan, err := structscan.New[Data](
		structscan.String().Enum(
			structscan.Enum{String: "1", Int: 1},
			structscan.Enum{String: "2", Int: 2}).To("ID"),
		structscan.String().TrimSpace().ParseBool().To("Bool"),
	)
	if err != nil {
		panic(err)
	}

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		panic(err)
	}

	rows, err := db.Query("SELECT '2', '   true   '")
	if err != nil {
		panic(err)
	}

	defer rows.Close()

	data, err := scan.One(rows)
	if err != nil {
		panic(err)
	}

	fmt.Println(data) // {2 true}
}

*/
package structscan

import (
	"database/sql"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

func New[T any](scanners ...Scanner) (*Schema[T], error) {
	schema := &Schema[T]{
		pool: &sync.Pool{
			New: func() any {
				runner, err := NewRunner[T](scanners...)
				if err != nil {
					return err
				}

				return runner
			},
		},
	}

	runner, err := schema.GetRunner()
	if err != nil {
		return nil, err
	}

	schema.PutRunner(runner)

	return schema, nil
}

type Schema[T any] struct {
	pool *sync.Pool
}

func (s *Schema[T]) GetRunner() (*Runner[T], error) {
	switch r := s.pool.Get().(type) {
	case *Runner[T]:
		return r, nil
	case error:
		return nil, r
	}

	return nil, fmt.Errorf("invalid runner")
}

func (s *Schema[T]) PutRunner(r *Runner[T]) {
	s.pool.Put(r)
}

func (s *Schema[T]) All(rows Rows) ([]T, error) {
	runner, err := s.GetRunner()
	if err != nil {
		return nil, err
	}

	result, err := runner.All(rows)

	s.PutRunner(runner)

	return result, err
}

func (s *Schema[T]) One(rows Rows) (T, error) {
	runner, err := s.GetRunner()
	if err != nil {
		return *new(T), err
	}

	result, err := runner.One(rows)

	s.PutRunner(runner)

	return result, err
}

func (s *Schema[T]) First(rows Rows) (T, error) {
	runner, err := s.GetRunner()
	if err != nil {
		return *new(T), err
	}

	result, err := runner.First(rows)

	s.PutRunner(runner)

	return result, err
}

func NewRunner[T any](scanners ...Scanner) (*Runner[T], error) {
	if len(scanners) == 0 {
		var typ = derefType(reflect.TypeFor[T]())
		var val = reflect.New(typ)

		return &Runner[T]{
			Src: []any{val.Interface()},
			Set: []func(dst reflect.Value) error{
				func(dst reflect.Value) error {
					dst.Set(val.Elem())

					return nil
				},
			},
		}, nil
	}

	var (
		typ = derefType(reflect.TypeFor[T]())
		src = make([]any, len(scanners))
		set = make([]func(dst reflect.Value) error, len(scanners))
		err error
	)

	for i, s := range scanners {
		src[i], set[i], err = s.Scan(typ)
		if err != nil {
			return nil, err
		}
	}

	return &Runner[T]{
		Src: src,
		Set: set,
	}, nil
}

type Runner[T any] struct {
	Src []any
	Set []func(dst reflect.Value) error
}

func (r *Runner[T]) All(rows Rows) ([]T, error) {
	var result []T

	for rows.Next() {
		if err := rows.Scan(r.Src...); err != nil {
			return nil, err
		}

		var (
			t   T
			dst = deref(reflect.ValueOf(&t))
		)

		for i, set := range r.Set {
			if set != nil {
				if err := set(dst); err != nil {
					return nil, fmt.Errorf("scanner at position %d: %w", i, err)
				}
			}
		}

		result = append(result, t)
	}

	return result, rows.Err()
}

var ErrTooManyRows = errors.New("too many rows")

func (r *Runner[T]) One(rows Rows) (T, error) {
	var (
		t   T
		dst = deref(reflect.ValueOf(&t))
	)

	if !rows.Next() {
		return t, sql.ErrNoRows
	}

	if err := rows.Scan(r.Src...); err != nil {
		return t, err
	}

	for _, set := range r.Set {
		if set != nil {
			if err := set(dst); err != nil {
				return t, err
			}
		}
	}

	if rows.Next() {
		return t, ErrTooManyRows
	}

	return t, rows.Err()
}

func (r *Runner[T]) First(rows Rows) (T, error) {
	var (
		t   T
		dst = deref(reflect.ValueOf(&t))
	)

	if !rows.Next() {
		return t, sql.ErrNoRows
	}

	if err := rows.Scan(r.Src...); err != nil {
		return t, err
	}

	for _, set := range r.Set {
		if set != nil {
			if err := set(dst); err != nil {
				return t, err
			}
		}
	}

	return t, rows.Err()
}

type Scanner interface {
	Scan(typ reflect.Type) (any, func(dst reflect.Value) error, error)
}

func Scan() DefaultScanner {
	return DefaultScanner{}
}

type DefaultScanner struct {
	nullable bool
}

func Nullable() DefaultScanner {
	return DefaultScanner{}.Nullable()
}

func (s DefaultScanner) Nullable() DefaultScanner {
	s.nullable = true

	return s
}

func String() StringScanner[string] {
	return DefaultScanner{}.String()
}

func (s DefaultScanner) String() StringScanner[string] {
	return StringScanner[string]{
		nullable: s.nullable,
		convert:  func(src string) (string, error) { return src, nil },
	}
}

func Int() IntScanner[int64] {
	return DefaultScanner{}.Int()
}

func (s DefaultScanner) Int() IntScanner[int64] {
	return IntScanner[int64]{
		nullable: s.nullable,
		convert:  func(src int64) (int64, error) { return src, nil },
	}
}

func Uint() UintScanner[uint64] {
	return DefaultScanner{}.Uint()
}

func (s DefaultScanner) Uint() UintScanner[uint64] {
	return UintScanner[uint64]{
		nullable: s.nullable,
		convert:  func(src uint64) (uint64, error) { return src, nil },
	}
}

func Float() FloatScanner[float64] {
	return DefaultScanner{}.Float()
}

func (s DefaultScanner) Float() FloatScanner[float64] {
	return FloatScanner[float64]{
		nullable: s.nullable,
		convert:  func(src float64) (float64, error) { return src, nil },
	}
}

func Bool() BoolScanner[bool] {
	return DefaultScanner{}.Bool()
}

func (s DefaultScanner) Bool() BoolScanner[bool] {
	return BoolScanner[bool]{
		nullable: s.nullable,
		convert:  func(src bool) (bool, error) { return src, nil },
	}
}

func Time() TimeScanner[time.Time] {
	return DefaultScanner{}.Time()
}

func (s DefaultScanner) Time() TimeScanner[time.Time] {
	return TimeScanner[time.Time]{
		nullable: s.nullable,
		convert:  func(src time.Time) (time.Time, error) { return src, nil },
	}
}

func Bytes() BytesScanner[[]byte] {
	return DefaultScanner{}.Bytes()
}

func (s DefaultScanner) Bytes() BytesScanner[[]byte] {
	return BytesScanner[[]byte]{
		nullable: s.nullable,
		convert:  func(src []byte) ([]byte, error) { return src, nil },
	}
}

func StringSlice() StringSliceScanner[[]string] {
	return DefaultScanner{}.StringSlice()
}

func (s DefaultScanner) StringSlice() StringSliceScanner[[]string] {
	return StringSliceScanner[[]string]{
		nullable: s.nullable,
		convert:  func(src []string) ([]string, error) { return src, nil },
	}
}

func IntSlice() IntSliceScanner[[]int64] {
	return DefaultScanner{}.IntSlice()
}

func (s DefaultScanner) IntSlice() IntSliceScanner[[]int64] {
	return IntSliceScanner[[]int64]{
		nullable: s.nullable,
		convert:  func(src []int64) ([]int64, error) { return src, nil },
	}
}

func JSON() JSONScanner[sql.RawBytes] {
	return DefaultScanner{}.JSON()
}

func (s DefaultScanner) JSON() JSONScanner[sql.RawBytes] {
	return JSONScanner[sql.RawBytes]{
		nullable: s.nullable,
		convert:  func(src sql.RawBytes) (sql.RawBytes, error) { return src, nil },
	}
}

func Text() TextScanner[sql.RawBytes] {
	return DefaultScanner{}.Text()
}

func (s DefaultScanner) Text() TextScanner[sql.RawBytes] {
	return TextScanner[sql.RawBytes]{
		nullable: s.nullable,
		convert:  func(src sql.RawBytes) (sql.RawBytes, error) { return src, nil },
	}
}

func Binary() BinaryScanner[sql.RawBytes] {
	return DefaultScanner{}.Binary()
}

func (s DefaultScanner) Binary() BinaryScanner[sql.RawBytes] {
	return BinaryScanner[sql.RawBytes]{
		nullable: s.nullable,
		convert:  func(src sql.RawBytes) (sql.RawBytes, error) { return src, nil },
	}
}

func To(path string) Scanner {
	return DefaultScanner{}.To(path)
}

func (s DefaultScanner) To(path string) Scanner {
	return ScanFunc(func(typ reflect.Type) (any, func(dst reflect.Value) error, error) {
		indices, dstType, err := accessor(typ, path)
		if err != nil {
			return nil, nil, err
		}

		if s.nullable {
			src := reflect.New(reflect.PointerTo(dstType))

			return src.Interface(), func(dst reflect.Value) error {
				elem := src.Elem()

				if elem.IsNil() {
					return nil
				}

				access(dst, indices).Set(elem.Elem())

				return nil
			}, nil
		}

		src := reflect.New(dstType)

		return src.Interface(), func(dst reflect.Value) error {
			access(dst, indices).Set(src.Elem())

			return nil
		}, nil
	})
}

func (s DefaultScanner) Scan(typ reflect.Type) (any, func(dst reflect.Value) error, error) {
	return s.To("").Scan(typ)
}

type StringScanner[S any] struct {
	nullable bool
	convert  func(src S) (string, error)
}

func (s StringScanner[S]) ParseInt(base int, bitSize int) IntScanner[S] {
	return IntScanner[S]{
		nullable: s.nullable,
		convert: func(src S) (int64, error) {
			val, err := s.convert(src)
			if err != nil {
				return 0, err
			}

			return strconv.ParseInt(val, base, bitSize)
		},
	}
}

func (s StringScanner[S]) ParseUint(base int, bitSize int) UintScanner[S] {
	return UintScanner[S]{
		nullable: s.nullable,
		convert: func(src S) (uint64, error) {
			val, err := s.convert(src)
			if err != nil {
				return 0, err
			}

			return strconv.ParseUint(val, base, bitSize)
		},
	}
}

func (s StringScanner[S]) ParseFloat(bitSize int) FloatScanner[S] {
	return FloatScanner[S]{
		nullable: s.nullable,
		convert: func(src S) (float64, error) {
			val, err := s.convert(src)
			if err != nil {
				return 0, err
			}

			return strconv.ParseFloat(val, bitSize)
		},
	}
}

func (s StringScanner[S]) ParseBool() BoolScanner[S] {
	return BoolScanner[S]{
		nullable: s.nullable,
		convert: func(src S) (bool, error) {
			val, err := s.convert(src)
			if err != nil {
				return false, err
			}

			return strconv.ParseBool(val)
		},
	}
}

func (s StringScanner[S]) ParseTime(layout string) TimeScanner[S] {
	return TimeScanner[S]{
		nullable: s.nullable,
		convert: func(src S) (time.Time, error) {
			val, err := s.convert(src)
			if err != nil {
				return time.Time{}, err
			}

			return time.Parse(layout, val)
		},
	}
}

func (s StringScanner[S]) ParseTimeInLocation(layout string, loc *time.Location) TimeScanner[S] {
	return TimeScanner[S]{
		nullable: s.nullable,
		convert: func(src S) (time.Time, error) {
			val, err := s.convert(src)
			if err != nil {
				return time.Time{}, err
			}

			return time.ParseInLocation(layout, val, loc)
		},
	}
}

func (s StringScanner[S]) Trim(cutset string) StringScanner[S] {
	return StringScanner[S]{
		nullable: s.nullable,
		convert: func(src S) (string, error) {
			val, err := s.convert(src)
			if err != nil {
				return "", err
			}

			return strings.Trim(val, cutset), nil
		},
	}
}

func (s StringScanner[S]) TrimSpace() StringScanner[S] {
	return StringScanner[S]{
		nullable: s.nullable,
		convert: func(src S) (string, error) {
			val, err := s.convert(src)
			if err != nil {
				return "", err
			}

			return strings.TrimSpace(val), nil
		},
	}
}

func (s StringScanner[S]) TrimPrefix(prefix string) StringScanner[S] {
	return StringScanner[S]{
		nullable: s.nullable,
		convert: func(src S) (string, error) {
			val, err := s.convert(src)
			if err != nil {
				return "", err
			}

			return strings.TrimPrefix(val, prefix), nil
		},
	}
}

func (s StringScanner[S]) TrimSuffix(suffix string) StringScanner[S] {
	return StringScanner[S]{
		nullable: s.nullable,
		convert: func(src S) (string, error) {
			val, err := s.convert(src)
			if err != nil {
				return "", err
			}

			return strings.TrimSuffix(val, suffix), nil
		},
	}
}

type Enum struct {
	String string
	Int    int64
}

func (s StringScanner[S]) Enum(enums ...Enum) IntScanner[S] {
	return IntScanner[S]{
		nullable: s.nullable,
		convert: func(src S) (int64, error) {
			conv, err := s.convert(src)
			if err != nil {
				return 0, err
			}

			for _, each := range enums {
				if each.String == conv {
					return each.Int, nil
				}
			}

			return 0, fmt.Errorf("value %s is not one of enums: %v", conv, enums)
		},
	}
}

func (s StringScanner[S]) Split(sep string) StringSliceScanner[S] {
	return StringSliceScanner[S]{
		nullable: s.nullable,
		convert: func(src S) ([]string, error) {
			val, err := s.convert(src)
			if err != nil {
				return nil, err
			}

			if val == "" {
				return []string{}, nil
			}

			return strings.Split(val, sep), nil
		},
	}
}

func (s StringScanner[S]) To(path string) Scanner {
	return indirectScanFunc(s.nullable, s.setter, s.convert, path)
}

func (s StringScanner[S]) Scan(typ reflect.Type) (any, func(dst reflect.Value) error, error) {
	return s.To("").Scan(typ)
}

var stringType = reflect.TypeFor[string]()

func (s StringScanner[S]) setter(dstType reflect.Type) (func(dst reflect.Value, conv string) error, error) {
	if dstType == stringType {
		return func(dst reflect.Value, conv string) error {
			*dst.Addr().Interface().(*string) = conv

			return nil
		}, nil
	}

	if dstType.Kind() == reflect.String {
		return func(dst reflect.Value, conv string) error {
			dst.SetString(conv)

			return nil
		}, nil
	}

	return nil, fmt.Errorf("%s is not assignable to string value", dstType)
}

type IntScanner[S any] struct {
	nullable bool
	convert  func(src S) (int64, error)
}

func (s IntScanner[S]) Format(base int) StringScanner[S] {
	return StringScanner[S]{
		convert: func(src S) (string, error) {
			val, err := s.convert(src)
			if err != nil {
				return "", err
			}

			return strconv.FormatInt(val, base), nil
		},
	}
}

func (s IntScanner[S]) Enum(enums ...Enum) StringScanner[S] {
	return StringScanner[S]{
		nullable: s.nullable,
		convert: func(src S) (string, error) {
			conv, err := s.convert(src)
			if err != nil {
				return "", err
			}

			for _, each := range enums {
				if each.Int == conv {
					return each.String, nil
				}
			}

			return "", fmt.Errorf("value %d is not one of enums: %v", conv, enums)
		},
	}
}

func (s IntScanner[S]) To(path string) Scanner {
	return indirectScanFunc(s.nullable, s.setter, s.convert, path)
}

func (s IntScanner[S]) Scan(typ reflect.Type) (any, func(dst reflect.Value) error, error) {
	return s.To("").Scan(typ)
}

var intType = reflect.TypeFor[int64]()

func (s IntScanner[S]) setter(dstType reflect.Type) (func(dst reflect.Value, conv int64) error, error) {
	if dstType == intType {
		return func(dst reflect.Value, conv int64) error {
			*dst.Addr().Interface().(*int64) = conv

			return nil
		}, nil
	}

	switch dstType.Kind() {
	case reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8, reflect.Int:
		return func(dst reflect.Value, conv int64) error {
			if dst.OverflowInt(conv) {
				return fmt.Errorf("overflow of int64 value %d to %s", conv, dstType)
			}

			dst.SetInt(conv)

			return nil
		}, nil
	case reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8, reflect.Uint:
		return func(dst reflect.Value, conv int64) error {
			if conv < 0 {
				return fmt.Errorf("lossy conversion of int64 value %d to %s", conv, dstType)
			}

			v := uint64(conv)

			if dst.OverflowUint(v) {
				return fmt.Errorf("overflow of int64 value %d to %s", conv, dstType)
			}

			dst.SetUint(v)

			return nil
		}, nil
	case reflect.Float64, reflect.Float32:
		return func(dst reflect.Value, conv int64) error {
			v := float64(conv)

			if dst.OverflowFloat(v) {
				return fmt.Errorf("overflow of int64 value %d to %s", conv, dstType)
			}

			dst.SetFloat(v)

			return nil
		}, nil
	}

	return nil, fmt.Errorf("%s is not assignable to int64 value", dstType)
}

type UintScanner[S any] struct {
	nullable bool
	convert  func(src S) (uint64, error)
}

func (s UintScanner[S]) Format(base int) StringScanner[S] {
	return StringScanner[S]{
		convert: func(src S) (string, error) {
			val, err := s.convert(src)
			if err != nil {
				return "", err
			}

			return strconv.FormatUint(val, base), nil
		},
	}
}

func (s UintScanner[S]) To(path string) Scanner {
	return indirectScanFunc(s.nullable, s.setter, s.convert, path)
}

func (s UintScanner[S]) Scan(typ reflect.Type) (any, func(dst reflect.Value) error, error) {
	return s.To("").Scan(typ)
}

var uint64Type = reflect.TypeFor[uint64]()

func (s UintScanner[S]) setter(dstType reflect.Type) (func(dst reflect.Value, conv uint64) error, error) {
	if dstType == uint64Type {
		return func(dst reflect.Value, conv uint64) error {
			*dst.Addr().Interface().(*uint64) = conv

			return nil
		}, nil
	}

	switch dstType.Kind() {
	case reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8, reflect.Uint:
		return func(dst reflect.Value, conv uint64) error {
			if dst.OverflowUint(conv) {
				return fmt.Errorf("overflow of uint64 value %d to %s", conv, dstType)
			}

			dst.SetUint(conv)

			return nil
		}, nil
	case reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8, reflect.Int:
		return func(dst reflect.Value, conv uint64) error {
			if conv > math.MaxInt64 {
				return fmt.Errorf("lossy conversion of uint64 value %d to %s", conv, dstType)
			}

			v := int64(conv)

			if dst.OverflowInt(v) {
				return fmt.Errorf("overflow of uint64 value %d to %s", conv, dstType)
			}

			dst.SetInt(v)

			return nil
		}, nil
	case reflect.Float64, reflect.Float32:
		return func(dst reflect.Value, conv uint64) error {
			v := float64(conv)

			if dst.OverflowFloat(v) {
				return fmt.Errorf("overflow of uint64 value %d to %s", conv, dstType)
			}

			dst.SetFloat(v)

			return nil
		}, nil
	}

	return nil, fmt.Errorf("%s is not assignable to uint64 value", dstType)
}

type FloatScanner[S any] struct {
	nullable bool
	convert  func(src S) (float64, error)
}

func (s FloatScanner[S]) Format(fmt byte, prec int, bitSize int) StringScanner[S] {
	return StringScanner[S]{
		convert: func(src S) (string, error) {
			val, err := s.convert(src)
			if err != nil {
				return "", err
			}

			return strconv.FormatFloat(val, fmt, prec, bitSize), nil
		},
	}
}

func (s FloatScanner[S]) To(path string) Scanner {
	return indirectScanFunc(s.nullable, s.setter, s.convert, path)
}

func (s FloatScanner[S]) Scan(typ reflect.Type) (any, func(dst reflect.Value) error, error) {
	return s.To("").Scan(typ)
}

var float64Type = reflect.TypeFor[float64]()

func (s FloatScanner[S]) setter(dstType reflect.Type) (func(dst reflect.Value, conv float64) error, error) {
	if dstType == float64Type {
		return func(dst reflect.Value, conv float64) error {
			*dst.Addr().Interface().(*float64) = conv

			return nil
		}, nil
	}

	switch dstType.Kind() {
	case reflect.Float64, reflect.Float32:
		return func(dst reflect.Value, conv float64) error {
			dst.SetFloat(conv)

			return nil
		}, nil
	}

	return nil, fmt.Errorf("%s is not assignable to float64 value", dstType)
}

type BoolScanner[S any] struct {
	nullable bool
	convert  func(src S) (bool, error)
}

func (s BoolScanner[S]) Format() StringScanner[S] {
	return StringScanner[S]{
		convert: func(src S) (string, error) {
			val, err := s.convert(src)
			if err != nil {
				return "", err
			}

			return strconv.FormatBool(val), nil
		},
	}
}

func (s BoolScanner[S]) To(path string) Scanner {
	return indirectScanFunc(s.nullable, s.setter, s.convert, path)
}

func (s BoolScanner[S]) Scan(typ reflect.Type) (any, func(dst reflect.Value) error, error) {
	return s.To("").Scan(typ)
}

var boolType = reflect.TypeFor[bool]()

func (s BoolScanner[S]) setter(dstType reflect.Type) (func(dst reflect.Value, conv bool) error, error) {
	if dstType == boolType {
		return func(dst reflect.Value, conv bool) error {
			*dst.Addr().Interface().(*bool) = conv

			return nil
		}, nil
	}

	if dstType.Kind() == reflect.Bool {
		return func(dst reflect.Value, conv bool) error {
			dst.SetBool(conv)

			return nil
		}, nil
	}

	return nil, fmt.Errorf("%s is not assignable to bool value", dstType)
}

type TimeScanner[S any] struct {
	nullable bool
	convert  func(src S) (time.Time, error)
}

func (s TimeScanner[S]) Format(layout string) StringScanner[S] {
	return StringScanner[S]{
		convert: func(src S) (string, error) {
			val, err := s.convert(src)
			if err != nil {
				return "", err
			}

			return val.Format(layout), nil
		},
	}
}

func (s TimeScanner[S]) To(path string) Scanner {
	return indirectScanFunc(s.nullable, s.setter, s.convert, path)
}

func (s TimeScanner[S]) Scan(typ reflect.Type) (any, func(dst reflect.Value) error, error) {
	return s.To("").Scan(typ)
}

var timeType = reflect.TypeFor[time.Time]()

func (s TimeScanner[S]) setter(dstType reflect.Type) (func(dst reflect.Value, conv time.Time) error, error) {
	if dstType == timeType {
		return func(dst reflect.Value, conv time.Time) error {
			*dst.Addr().Interface().(*time.Time) = conv

			return nil
		}, nil
	}

	if timeType.ConvertibleTo(dstType) {
		return func(dst reflect.Value, conv time.Time) error {
			dst.Set(reflect.ValueOf(conv).Convert(dstType))

			return nil
		}, nil
	}

	return nil, fmt.Errorf("%s is not assignable to time.Time value", dstType)
}

type BytesScanner[S any] struct {
	nullable bool
	convert  func(src S) ([]byte, error)
}

func (s BytesScanner[S]) To(path string) Scanner {
	return indirectScanFunc(s.nullable, s.setter, s.convert, path)
}

func (s BytesScanner[S]) Scan(typ reflect.Type) (any, func(dst reflect.Value) error, error) {
	return s.To("").Scan(typ)
}

var bytesType = reflect.TypeFor[[]byte]()

func (s BytesScanner[S]) setter(dstType reflect.Type) (func(dst reflect.Value, conv []byte) error, error) {
	if dstType == bytesType {
		return func(dst reflect.Value, conv []byte) error {
			*dst.Addr().Interface().(*[]byte) = conv

			return nil
		}, nil
	}

	if bytesType.ConvertibleTo(dstType) {
		return func(dst reflect.Value, conv []byte) error {
			dst.Set(reflect.ValueOf(conv).Convert(dstType))

			return nil
		}, nil
	}

	return nil, fmt.Errorf("%s is not assignable to []byte value", dstType)
}

type StringSliceScanner[S any] struct {
	nullable bool
	convert  func(src S) ([]string, error)
}

func (s StringSliceScanner[S]) Asc() StringSliceScanner[S] {
	return StringSliceScanner[S]{
		nullable: s.nullable,
		convert: func(src S) ([]string, error) {
			val, err := s.convert(src)
			if err != nil {
				return nil, err
			}

			slices.Sort(val)

			return val, nil
		},
	}
}

func (s StringSliceScanner[S]) Desc() StringSliceScanner[S] {
	return StringSliceScanner[S]{
		nullable: s.nullable,
		convert: func(src S) ([]string, error) {
			val, err := s.convert(src)
			if err != nil {
				return nil, err
			}

			slices.Sort(val)
			slices.Reverse(val)

			return val, nil
		},
	}
}

func (s StringSliceScanner[S]) ParseInt(base int, bitSize int) IntSliceScanner[S] {
	return IntSliceScanner[S]{
		nullable: s.nullable,
		convert: func(src S) ([]int64, error) {
			val, err := s.convert(src)
			if err != nil {
				return nil, err
			}

			conv := make([]int64, len(val))

			for i, v := range val {
				c, err := strconv.ParseInt(v, base, bitSize)
				if err != nil {
					return nil, err
				}

				conv[i] = c
			}

			return conv, nil
		},
	}
}

func (s StringSliceScanner[S]) To(path string) Scanner {
	return indirectScanFunc(s.nullable, s.setter, s.convert, path)
}

func (s StringSliceScanner[S]) Scan(typ reflect.Type) (any, func(dst reflect.Value) error, error) {
	return s.To("").Scan(typ)
}

var stringSliceType = reflect.TypeFor[[]string]()

func (s StringSliceScanner[S]) setter(dstType reflect.Type) (func(dst reflect.Value, conv []string) error, error) {
	if dstType == stringSliceType {
		return func(dst reflect.Value, conv []string) error {
			*dst.Addr().Interface().(*[]string) = conv

			return nil
		}, nil
	}

	if stringSliceType.ConvertibleTo(dstType) {
		return func(dst reflect.Value, conv []string) error {
			dst.Set(reflect.ValueOf(conv).Convert(dstType))

			return nil
		}, nil
	}

	if dstType.Kind() == reflect.Slice && derefType(dstType.Elem()).Kind() == reflect.String {
		return func(dst reflect.Value, conv []string) error {
			dst.Set(reflect.MakeSlice(dstType, len(conv), len(conv)))

			for i, v := range conv {
				deref(dst.Index(i)).SetString(v)
			}

			return nil
		}, nil
	}

	return nil, fmt.Errorf("%s is not assignable to []string value", dstType)
}

type IntSliceScanner[S any] struct {
	nullable bool
	convert  func(src S) ([]int64, error)
}

func (s IntSliceScanner[S]) Asc() IntSliceScanner[S] {
	return IntSliceScanner[S]{
		nullable: s.nullable,
		convert: func(src S) ([]int64, error) {
			val, err := s.convert(src)
			if err != nil {
				return nil, err
			}

			slices.Sort(val)

			return val, nil
		},
	}
}

func (s IntSliceScanner[S]) Desc() IntSliceScanner[S] {
	return IntSliceScanner[S]{
		nullable: s.nullable,
		convert: func(src S) ([]int64, error) {
			val, err := s.convert(src)
			if err != nil {
				return nil, err
			}

			slices.Sort(val)
			slices.Reverse(val)

			return val, nil
		},
	}
}

func (s IntSliceScanner[S]) Format(base int) StringSliceScanner[S] {
	return StringSliceScanner[S]{
		nullable: s.nullable,
		convert: func(src S) ([]string, error) {
			val, err := s.convert(src)
			if err != nil {
				return nil, err
			}

			conv := make([]string, len(val))

			for i, v := range val {
				conv[i] = strconv.FormatInt(v, base)
			}

			return conv, nil
		},
	}
}

func (s IntSliceScanner[S]) To(path string) Scanner {
	return indirectScanFunc(s.nullable, s.setter, s.convert, path)
}

func (s IntSliceScanner[S]) Scan(typ reflect.Type) (any, func(dst reflect.Value) error, error) {
	return s.To("").Scan(typ)
}

var int64SliceType = reflect.TypeFor[[]int64]()

func (s IntSliceScanner[S]) setter(dstType reflect.Type) (func(dst reflect.Value, conv []int64) error, error) {
	if dstType == int64SliceType {
		return func(dst reflect.Value, conv []int64) error {
			*dst.Addr().Interface().(*[]int64) = conv

			return nil
		}, nil
	}

	if int64SliceType.ConvertibleTo(dstType) {
		return func(dst reflect.Value, conv []int64) error {
			dst.Set(reflect.ValueOf(conv).Convert(dstType))

			return nil
		}, nil
	}

	if dstType.Kind() == reflect.Slice {
		switch derefType(dstType.Elem()).Kind() {
		case reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8, reflect.Int:
			return func(dst reflect.Value, conv []int64) error {
				dst.Set(reflect.MakeSlice(dstType, len(conv), len(conv)))

				for i, v := range conv {
					deref(dst.Index(i)).SetInt(v)
				}

				return nil
			}, nil
		}
	}

	return nil, fmt.Errorf("%s is not assignable to []int64 value", dstType)
}

type JSONScanner[S any] struct {
	nullable bool
	convert  func(src S) (sql.RawBytes, error)
}

func (s JSONScanner[S]) To(path string) Scanner {
	return indirectScanFunc(s.nullable, s.setter, s.convert, path)
}

func (s JSONScanner[S]) Scan(typ reflect.Type) (any, func(dst reflect.Value) error, error) {
	return s.To("").Scan(typ)
}

func (s JSONScanner[S]) setter(_ reflect.Type) (func(dst reflect.Value, conv sql.RawBytes) error, error) {
	return func(dst reflect.Value, conv sql.RawBytes) error {
		return json.Unmarshal(conv, dst.Addr().Interface())
	}, nil
}

type TextScanner[S any] struct {
	nullable bool
	convert  func(src S) (sql.RawBytes, error)
}

func (s TextScanner[S]) To(path string) Scanner {
	return indirectScanFunc(s.nullable, s.setter, s.convert, path)
}

func (s TextScanner[S]) Scan(typ reflect.Type) (any, func(dst reflect.Value) error, error) {
	return s.To("").Scan(typ)
}

var textUnmarshalerType = reflect.TypeFor[encoding.TextUnmarshaler]()

func (s TextScanner[S]) setter(dstType reflect.Type) (func(dst reflect.Value, conv sql.RawBytes) error, error) {
	if reflect.PointerTo(dstType).Implements(textUnmarshalerType) {
		return func(dst reflect.Value, conv sql.RawBytes) error {
			return dst.Addr().Interface().(encoding.TextUnmarshaler).UnmarshalText(conv)
		}, nil
	}

	return nil, fmt.Errorf("%s doesn't implement encoding.TextUnmarshaler", dstType)
}

type BinaryScanner[S any] struct {
	nullable bool
	convert  func(src S) (sql.RawBytes, error)
}

func (s BinaryScanner[S]) To(path string) Scanner {
	return indirectScanFunc(s.nullable, s.setter, s.convert, path)
}

func (s BinaryScanner[S]) Scan(typ reflect.Type) (any, func(dst reflect.Value) error, error) {
	return s.To("").Scan(typ)
}

var binaryUnmarshalerType = reflect.TypeFor[encoding.BinaryUnmarshaler]()

func (s BinaryScanner[S]) setter(dstType reflect.Type) (func(dst reflect.Value, conv sql.RawBytes) error, error) {
	if reflect.PointerTo(dstType).Implements(binaryUnmarshalerType) {
		return func(dst reflect.Value, conv sql.RawBytes) error {
			return dst.Addr().Interface().(encoding.BinaryUnmarshaler).UnmarshalBinary(conv)
		}, nil
	}

	return nil, fmt.Errorf("%s doesn't implement encoding.BinaryUnmarshaler", dstType)
}

type ScanFunc func(typ reflect.Type) (any, func(dst reflect.Value) error, error)

func (sf ScanFunc) Scan(typ reflect.Type) (any, func(dst reflect.Value) error, error) {
	return sf(typ)
}

func indirectScanFunc[S, C any](
	nullable bool,
	setter func(dstType reflect.Type) (func(dst reflect.Value, conv C) error, error),
	convert func(src S) (C, error),
	path string,
) ScanFunc {
	return func(typ reflect.Type) (any, func(dst reflect.Value) error, error) {
		indices, dstType, err := accessor(typ, path)
		if err != nil {
			return nil, nil, err
		}

		set, err := setter(dstType)
		if err != nil {
			if path != "" {
				return nil, nil, fmt.Errorf("path %s: %w", path, err)
			}

			return nil, nil, err
		}

		if nullable {
			var src sql.Null[S]

			return &src, func(dst reflect.Value) error {
				if !src.Valid {
					return nil
				}

				conv, err := convert(src.V)
				if err != nil {
					return err
				}

				return set(access(dst, indices), conv)
			}, nil
		}

		var src S

		return &src, func(dst reflect.Value) error {
			conv, err := convert(src)
			if err != nil {
				return err
			}

			return set(access(dst, indices), conv)
		}, nil
	}
}

func accessor(typ reflect.Type, path string) ([]int, reflect.Type, error) {
	if path == "" {
		return nil, derefType(typ), nil
	}

	var indices []int

	for p := range strings.SplitSeq(path, ".") {
		sf, ok := derefType(typ).FieldByName(p)
		if !ok {
			return nil, nil, fmt.Errorf("path %s: not found", path)
		}

		if !sf.IsExported() {
			return nil, nil, fmt.Errorf("path %s: not exported", path)
		}

		typ = sf.Type

		indices = append(indices, sf.Index...)
	}

	return indices, derefType(typ), nil
}

func derefType(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	return t
}

func deref(dst reflect.Value) reflect.Value {
	for dst.Kind() == reflect.Pointer {
		if dst.IsNil() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}

		dst = dst.Elem()
	}

	return dst
}

func access(dst reflect.Value, indices []int) reflect.Value {
	for _, idx := range indices {
		dst = deref(dst).Field(idx)
	}

	return deref(dst)
}
