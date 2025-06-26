// Package structscan is a lightweight Go library that maps SQL query results to Go structs.
//
// Usage:
/*

package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/go-sqlt/structscan"
	_ "modernc.org/sqlite"
)

type Data struct {
	Int  uint64
	Bool bool
}

func main() {
	dest := structscan.NewSchema[Data]()

	mapper, err := structscan.NewMapper(
		dest.Scan().String().Int(10, 64).MustTo("Int"),
		dest.Scan().MustTo("Bool"),
	)
	if err != nil {
		panic(err)
	}

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		panic(err)
	}

	data, err := mapper.QueryOne(context.Background(), db, "SELECT '2', true")
	if err != nil {
		panic(err)
	}

	fmt.Println(data) // {2 true}
}

*/
package structscan

import (
	"context"
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
	"unsafe"
)

type DB interface {
	QueryContext(ctx context.Context, sql string, args ...any) (*sql.Rows, error)
}

func NewMapper[T any](scanners ...Scanner[T]) (*Mapper[T], error) {
	return &Mapper[T]{
		pool: &sync.Pool{
			New: func() any {
				return newRunner(scanners...)
			},
		},
	}, nil
}

type Mapper[T any] struct {
	pool *sync.Pool
}

func (m *Mapper[T]) QueryAll(ctx context.Context, db DB, sql string, args ...any) ([]T, error) {
	rows, err := db.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	return m.All(rows)
}

func (m *Mapper[T]) QueryOne(ctx context.Context, db DB, sql string, args ...any) (T, error) {
	rows, err := db.QueryContext(ctx, sql, args...)
	if err != nil {
		return *new(T), err
	}

	return m.One(rows)
}

func (m *Mapper[T]) QueryFirst(ctx context.Context, db DB, sql string, args ...any) (T, error) {
	rows, err := db.QueryContext(ctx, sql, args...)
	if err != nil {
		return *new(T), err
	}

	return m.First(rows)
}

func (m *Mapper[T]) All(rows *sql.Rows) ([]T, error) {
	//nolint:forcetypeassert
	runner := m.pool.Get().(*runner[T])

	result, err := runner.all(rows)

	m.pool.Put(runner)

	return result, err
}

func (m *Mapper[T]) One(rows *sql.Rows) (T, error) {
	//nolint:forcetypeassert
	runner := m.pool.Get().(*runner[T])

	result, err := runner.one(rows)

	m.pool.Put(runner)

	return result, err
}

func (m *Mapper[T]) First(rows *sql.Rows) (T, error) {
	//nolint:forcetypeassert
	runner := m.pool.Get().(*runner[T])

	result, err := runner.first(rows)

	m.pool.Put(runner)

	return result, err
}

func newRunner[T any](scanners ...Scanner[T]) *runner[T] {
	if len(scanners) == 0 {
		return &runner[T]{
			src: []any{},
			set: nil,
		}
	}

	var (
		src = make([]any, len(scanners))
		set = make([]func(t *T) error, len(scanners))
	)

	for i, s := range scanners {
		src[i], set[i] = s.Scan()
	}

	return &runner[T]{
		src: src,
		set: set,
	}
}

type runner[T any] struct {
	src []any
	set []func(t *T) error
}

func (r *runner[T]) init() {
	if len(r.src) == 0 {
		var d T

		r.src = append(r.src, &d)
		r.set = append(r.set, func(t *T) error {
			*t = d

			return nil
		})
	}
}

func (r *runner[T]) all(rows *sql.Rows) ([]T, error) {
	r.init()

	var result []T

	for rows.Next() {
		if err := rows.Scan(r.src...); err != nil {
			return nil, errors.Join(err, rows.Close())
		}

		var t T

		for i, set := range r.set {
			if set != nil {
				if err := set(&t); err != nil {
					return nil, errors.Join(fmt.Errorf("scanner at position %d: %w", i, err), rows.Close())
				}
			}
		}

		result = append(result, t)
	}

	return result, errors.Join(rows.Err(), rows.Close())
}

var ErrTooManyRows = errors.New("too many rows")

func (r *runner[T]) one(rows *sql.Rows) (T, error) {
	r.init()

	var t T

	if !rows.Next() {
		return t, errors.Join(sql.ErrNoRows, rows.Close())
	}

	if err := rows.Scan(r.src...); err != nil {
		return t, errors.Join(err, rows.Close())
	}

	for _, set := range r.set {
		if set != nil {
			if err := set(&t); err != nil {
				return t, errors.Join(err, rows.Close())
			}
		}
	}

	if rows.Next() {
		return t, errors.Join(ErrTooManyRows, rows.Close())
	}

	return t, errors.Join(rows.Err(), rows.Close())
}

func (r *runner[T]) first(rows *sql.Rows) (T, error) {
	r.init()

	var t T

	if !rows.Next() {
		return t, errors.Join(sql.ErrNoRows, rows.Close())
	}

	if err := rows.Scan(r.src...); err != nil {
		return t, errors.Join(err, rows.Close())
	}

	for _, set := range r.set {
		if set != nil {
			if err := set(&t); err != nil {
				return t, errors.Join(err, rows.Close())
			}
		}
	}

	return t, errors.Join(rows.Err(), rows.Close())
}

type Scanner[T any] interface {
	Scan() (any, func(t *T) error)
}

type Field[T any] struct {
	Type      reflect.Type
	DerefType reflect.Type
	DerefKind reflect.Kind
	Indices   []int
	Offset    uintptr
}

func (f Field[T]) AccessDeref(t *T) reflect.Value {
	return derefDst(f.Access(t))
}

func (f Field[T]) Access(t *T) reflect.Value {
	if f.Offset > 0 {
		return reflect.NewAt(f.Type, f.Pointer(t)).Elem()
	}

	dst := reflect.ValueOf(t).Elem()

	for _, idx := range f.Indices {
		dst = derefDst(dst).Field(idx)
	}

	return dst
}

func (f Field[T]) Pointer(t *T) unsafe.Pointer {
	if f.Offset > 0 {
		//nolint:gosec
		return unsafe.Pointer(uintptr(unsafe.Pointer(t)) + f.Offset)
	}

	return f.Access(t).Addr().UnsafePointer()
}

func NewSchema[T any]() *Schema[T] {
	t := reflect.TypeFor[T]()

	return &Schema[T]{
		typ:       t,
		derefType: derefType(t),
		store:     &sync.Map{},
	}
}

type Schema[T any] struct {
	typ       reflect.Type
	derefType reflect.Type
	store     *sync.Map
}

func (s *Schema[T]) makeField(path string) (Field[T], error) {
	if path == "" || path == "." {
		return Field[T]{
			Indices:   nil,
			Offset:    0,
			Type:      s.typ,
			DerefType: s.derefType,
			DerefKind: s.derefType.Kind(),
		}, nil
	}

	var (
		t       = s.typ
		indices []int
		pointer bool
		offset  uintptr
		depth   int
	)

	for p := range strings.SplitSeq(path, ".") {
		//nolint:mnd
		if depth > 10 {
			return Field[T]{}, fmt.Errorf("field %s: depth limit exceeded", path)
		}

		if t.Kind() == reflect.Pointer {
			pointer = true
			t = derefType(t)
		}

		sf, ok := t.FieldByName(p)
		if !ok {
			return Field[T]{}, fmt.Errorf("field %s: not found", path)
		}

		if !sf.IsExported() {
			return Field[T]{}, fmt.Errorf("field %s: not exported", path)
		}

		t = sf.Type

		indices = append(indices, sf.Index...)
		offset += sf.Offset

		depth++
	}

	if pointer {
		offset = 0
	}

	d := derefType(t)

	return Field[T]{
		Indices:   indices,
		Offset:    offset,
		Type:      t,
		DerefType: d,
		DerefKind: d.Kind(),
	}, nil
}

func (s *Schema[T]) Field(path string) (Field[T], error) {
	field, ok := s.store.Load(path)
	if ok {
		//nolint:forcetypeassert
		return field.(Field[T]), nil
	}

	f, err := s.makeField(path)
	if err != nil {
		return Field[T]{}, err
	}

	s.store.Store(path, f)

	return f, nil
}

func (s *Schema[T]) Scan() SchemaScanner[T] {
	return SchemaScanner[T]{
		schema:   s,
		nullable: false,
	}
}

type SchemaScanner[T any] struct {
	schema   *Schema[T]
	nullable bool
}

func (s SchemaScanner[T]) Nullable() SchemaScanner[T] {
	s.nullable = true

	return s
}

func (s SchemaScanner[T]) MustTo(path string) Scanner[T] {
	return must(s.To(path))
}

//nolint:gochecknoglobals
var scannerType = reflect.TypeFor[sql.Scanner]()

func (s SchemaScanner[T]) To(path string) (Scanner[T], error) {
	f, err := s.schema.Field(path)
	if err != nil {
		return nil, err
	}

	if reflect.PointerTo(f.DerefType).Implements(scannerType) {
		return valueFieldScanner[any, T]{
			field:    f,
			nullable: s.nullable,
			setter: func(t *T, src any) error {
				//nolint:forcetypeassert
				return f.AccessDeref(t).Addr().Interface().(sql.Scanner).Scan(src)
			},
		}, nil
	}

	return fieldScanner[T]{
		field:    f,
		nullable: s.nullable,
	}, nil
}

func (s SchemaScanner[T]) Scan() (any, func(t *T) error) {
	return s.MustTo(".").Scan()
}

func (s SchemaScanner[T]) String() StringScanner[string, T] {
	return StringScanner[string, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert:  justString,
	}
}

func (s SchemaScanner[T]) Int() IntScanner[int64, T] {
	return IntScanner[int64, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert:  justInt,
	}
}

func (s SchemaScanner[T]) Uint() UintScanner[uint64, T] {
	return UintScanner[uint64, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert:  justUint,
	}
}

func (s SchemaScanner[T]) Float() FloatScanner[float64, T] {
	return FloatScanner[float64, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert:  justFloat,
	}
}

func (s SchemaScanner[T]) Bool() BoolScanner[bool, T] {
	return BoolScanner[bool, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert:  justBool,
	}
}

func (s SchemaScanner[T]) Time() TimeScanner[time.Time, T] {
	return TimeScanner[time.Time, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert:  justTime,
	}
}

func (s SchemaScanner[T]) Duration() DurationScanner[time.Duration, T] {
	return DurationScanner[time.Duration, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert:  justDuration,
	}
}

func (s SchemaScanner[T]) Bytes() BytesScanner[sql.RawBytes, T] {
	return BytesScanner[sql.RawBytes, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert:  justBytes,
	}
}

func (s SchemaScanner[T]) JSON() JSONScanner[sql.RawBytes, T] {
	return JSONScanner[sql.RawBytes, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert:  justBytes,
	}
}

func (s SchemaScanner[T]) Text() TextScanner[sql.RawBytes, T] {
	return TextScanner[sql.RawBytes, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert:  justBytes,
	}
}

func (s SchemaScanner[T]) Binary() BinaryScanner[sql.RawBytes, T] {
	return BinaryScanner[sql.RawBytes, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert:  justBytes,
	}
}

func (s SchemaScanner[T]) Assign(init func() Assigner) AssignScanner[T] {
	return AssignScanner[T]{
		schema:   s.schema,
		nullable: s.nullable,
		init:     init,
	}
}

type fieldScanner[T any] struct {
	field    Field[T]
	nullable bool
}

func (s fieldScanner[T]) Scan() (any, func(t *T) error) {
	if s.nullable {
		var src = reflect.New(reflect.PointerTo(s.field.Type))

		return src.Interface(), func(t *T) error {
			dst := s.field.Access(t)

			elem := src.Elem()
			if elem.IsNil() {
				dst.SetZero()

				return nil
			}

			dst.Set(elem.Elem())

			return nil
		}
	}

	var src = reflect.New(s.field.Type)

	return src.Interface(), func(t *T) error {
		s.field.Access(t).Set(src.Elem())

		return nil
	}
}

type StringScanner[S, T any] struct {
	schema   *Schema[T]
	nullable bool
	convert  Convert[S, string]
}

func (s StringScanner[S, T]) Scan() (any, func(*T) error) {
	return s.MustTo("").Scan()
}

func (s StringScanner[S, T]) MustTo(path string) Scanner[T] {
	return must(s.To(path))
}

//nolint:gochecknoglobals
var (
	stringType        = reflect.TypeFor[string]()
	stringPointerType = reflect.TypeFor[*string]()
)

//nolint:dupl
func (s StringScanner[S, T]) To(path string) (Scanner[T], error) {
	f, err := s.schema.Field(path)
	if err != nil {
		return nil, err
	}

	set, err := makeSetter(f,
		stringType,
		stringPointerType,
		[]reflect.Kind{reflect.String},
		func(dst reflect.Value, src string) error {
			dst.SetString(src)

			return nil
		})
	if err != nil {
		return nil, err
	}

	if reflect.ValueOf(s.convert).Pointer() == justStringPointer {
		return valueFieldScanner[string, T]{
			nullable: s.nullable,
			field:    f,
			setter: func(t *T, src string) error {
				return set(t, src)
			},
		}, nil
	}

	return valueFieldScanner[S, T]{
		nullable: s.nullable,
		field:    f,
		setter: func(t *T, src S) error {
			val, err := s.convert(src)
			if err != nil {
				return err
			}

			return set(t, val)
		},
	}, nil
}

func (s StringScanner[S, T]) Convert(conv Convert[string, string]) StringScanner[S, T] {
	return StringScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (string, error) {
			val, err := s.convert(src)
			if err != nil {
				return "", err
			}

			return conv(val)
		},
	}
}

func (s StringScanner[S, T]) Trim(cutset string) StringScanner[S, T] {
	return s.Convert(func(src string) (string, error) {
		return strings.Trim(src, cutset), nil
	})
}

func (s StringScanner[S, T]) TrimSpace() StringScanner[S, T] {
	return s.Convert(func(src string) (string, error) {
		return strings.TrimSpace(src), nil
	})
}

func (s StringScanner[S, T]) Replace(find, replace string, n int) StringScanner[S, T] {
	return s.Convert(func(src string) (string, error) {
		return strings.Replace(src, find, replace, n), nil
	})
}

func (s StringScanner[S, T]) ReplaceAll(find, replace string) StringScanner[S, T] {
	return s.Convert(func(src string) (string, error) {
		return strings.ReplaceAll(src, find, replace), nil
	})
}

func (s StringScanner[S, T]) ConvertInt(conv Convert[string, int64]) IntScanner[S, T] {
	return IntScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (int64, error) {
			val, err := s.convert(src)
			if err != nil {
				return 0, err
			}

			return conv(val)
		},
	}
}

func (s StringScanner[S, T]) Int(base int, bitSize int) IntScanner[S, T] {
	return s.ConvertInt(func(src string) (int64, error) {
		return strconv.ParseInt(src, base, bitSize)
	})
}

func (s StringScanner[S, T]) ConvertUint(conv Convert[string, uint64]) UintScanner[S, T] {
	return UintScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (uint64, error) {
			val, err := s.convert(src)
			if err != nil {
				return 0, err
			}

			return conv(val)
		},
	}
}

func (s StringScanner[S, T]) Uint(base int, bitSize int) UintScanner[S, T] {
	return s.ConvertUint(func(src string) (uint64, error) {
		return strconv.ParseUint(src, base, bitSize)
	})
}

func (s StringScanner[S, T]) ConvertFloat(conv Convert[string, float64]) FloatScanner[S, T] {
	return FloatScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (float64, error) {
			val, err := s.convert(src)
			if err != nil {
				return 0, err
			}

			return conv(val)
		},
	}
}

func (s StringScanner[S, T]) Float(bitSize int) FloatScanner[S, T] {
	return s.ConvertFloat(func(src string) (float64, error) {
		return strconv.ParseFloat(src, bitSize)
	})
}

func (s StringScanner[S, T]) ConvertComplex(conv Convert[string, complex128]) ComplexScanner[S, T] {
	return ComplexScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (complex128, error) {
			val, err := s.convert(src)
			if err != nil {
				return 0, err
			}

			return conv(val)
		},
	}
}

func (s StringScanner[S, T]) Complex(bitSize int) ComplexScanner[S, T] {
	return s.ConvertComplex(func(src string) (complex128, error) {
		return strconv.ParseComplex(src, bitSize)
	})
}

func (s StringScanner[S, T]) ConvertBool(conv Convert[string, bool]) BoolScanner[S, T] {
	return BoolScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (bool, error) {
			val, err := s.convert(src)
			if err != nil {
				return false, err
			}

			return conv(val)
		},
	}
}

func (s StringScanner[S, T]) Bool() BoolScanner[S, T] {
	return s.ConvertBool(strconv.ParseBool)
}

func (s StringScanner[S, T]) ConvertTime(conv Convert[string, time.Time]) TimeScanner[S, T] {
	return TimeScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (time.Time, error) {
			val, err := s.convert(src)
			if err != nil {
				return time.Time{}, err
			}

			return conv(val)
		},
	}
}

func (s StringScanner[S, T]) Time(layout string) TimeScanner[S, T] {
	return s.ConvertTime(func(src string) (time.Time, error) {
		return time.Parse(layout, src)
	})
}

func (s StringScanner[S, T]) TimeInLocation(layout string, loc *time.Location) TimeScanner[S, T] {
	return s.ConvertTime(func(src string) (time.Time, error) {
		return time.ParseInLocation(layout, src, loc)
	})
}

func (s StringScanner[S, T]) ConvertDuration(conv Convert[string, time.Duration]) DurationScanner[S, T] {
	return DurationScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (time.Duration, error) {
			val, err := s.convert(src)
			if err != nil {
				return 0, err
			}

			return conv(val)
		},
	}
}

func (s StringScanner[S, T]) Duration() DurationScanner[S, T] {
	return s.ConvertDuration(time.ParseDuration)
}

type Enum struct {
	String string
	Int    int64
}

func (s StringScanner[S, T]) Enum(enums ...Enum) IntScanner[S, T] {
	return s.ConvertInt(func(src string) (int64, error) {
		for _, each := range enums {
			if each.String == src {
				return each.Int, nil
			}
		}

		return 0, fmt.Errorf("value %s is not one of enums: %v", src, enums)
	})
}

func (s StringScanner[S, T]) ConvertStringSlice(conv Convert[string, []string]) StringSliceScanner[S, T] {
	return StringSliceScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) ([]string, error) {
			val, err := s.convert(src)
			if err != nil {
				return nil, err
			}

			return conv(val)
		},
	}
}

func (s StringScanner[S, T]) Split(sep string) StringSliceScanner[S, T] {
	return s.ConvertStringSlice(func(src string) ([]string, error) {
		if src == "" {
			return nil, nil
		}

		return strings.Split(src, sep), nil
	})
}

type IntScanner[S, T any] struct {
	schema   *Schema[T]
	nullable bool
	convert  func(src S) (int64, error)
}

func (s IntScanner[S, T]) Scan() (any, func(*T) error) {
	return s.MustTo("").Scan()
}

func (s IntScanner[S, T]) MustTo(path string) Scanner[T] {
	return must(s.To(path))
}

//nolint:gochecknoglobals
var (
	int64Type        = reflect.TypeFor[int64]()
	int64PointerType = reflect.TypeFor[*int64]()
)

func (s IntScanner[S, T]) To(path string) (Scanner[T], error) {
	f, err := s.schema.Field(path)
	if err != nil {
		return nil, err
	}

	set, err := makeSetter(f,
		int64Type,
		int64PointerType,
		[]reflect.Kind{
			reflect.Int64,
			reflect.Int32,
			reflect.Int16,
			reflect.Int8,
			reflect.Int,
		},
		func(dst reflect.Value, src int64) error {
			if dst.OverflowInt(src) {
				return fmt.Errorf("lossy conversion of int64 value %d to %s", src, dst.Type())
			}

			dst.SetInt(src)

			return nil
		})
	if err != nil {
		return nil, err
	}

	if reflect.ValueOf(s.convert).Pointer() == justIntPointer {
		return valueFieldScanner[int64, T]{
			nullable: s.nullable,
			field:    f,
			setter: func(t *T, src int64) error {
				return set(t, src)
			},
		}, nil
	}

	return valueFieldScanner[S, T]{
		nullable: s.nullable,
		field:    f,
		setter: func(t *T, src S) error {
			val, err := s.convert(src)
			if err != nil {
				return err
			}

			return set(t, val)
		},
	}, nil
}

func (s IntScanner[S, T]) Convert(conv Convert[int64, int64]) IntScanner[S, T] {
	return IntScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (int64, error) {
			val, err := s.convert(src)
			if err != nil {
				return 0, err
			}

			return conv(val)
		},
	}
}

func (s IntScanner[S, T]) Multiply(factor int64) IntScanner[S, T] {
	return s.Convert(func(src int64) (int64, error) {
		return src * factor, nil
	})
}

func (s IntScanner[S, T]) Add(summand int64) IntScanner[S, T] {
	return s.Convert(func(src int64) (int64, error) {
		return src + summand, nil
	})
}

func (s IntScanner[S, T]) Subtract(subtrahend int64) IntScanner[S, T] {
	return s.Convert(func(src int64) (int64, error) {
		return src - subtrahend, nil
	})
}

func (s IntScanner[S, T]) ConvertString(conv Convert[int64, string]) StringScanner[S, T] {
	return StringScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (string, error) {
			val, err := s.convert(src)
			if err != nil {
				return "", err
			}

			return conv(val)
		},
	}
}

func (s IntScanner[S, T]) Format(base int) StringScanner[S, T] {
	return s.ConvertString(func(src int64) (string, error) {
		return strconv.FormatInt(src, base), nil
	})
}

func (s IntScanner[S, T]) Enum(enums ...Enum) StringScanner[S, T] {
	return s.ConvertString(func(src int64) (string, error) {
		for _, each := range enums {
			if each.Int == src {
				return each.String, nil
			}
		}

		return "", fmt.Errorf("value %d is not one of enums: %v", src, enums)
	})
}

func (s IntScanner[S, T]) ConvertUint(conv Convert[int64, uint64]) UintScanner[S, T] {
	return UintScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (uint64, error) {
			val, err := s.convert(src)
			if err != nil {
				return 0, err
			}

			return conv(val)
		},
	}
}

func (s IntScanner[S, T]) Uint() UintScanner[S, T] {
	return s.ConvertUint(func(src int64) (uint64, error) {
		if src < 0 {
			return 0, fmt.Errorf("lossy conversion of int64 value %d to uint64", src)
		}

		return uint64(src), nil
	})
}

func (s IntScanner[S, T]) ConvertFloat(conv Convert[int64, float64]) FloatScanner[S, T] {
	return FloatScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (float64, error) {
			val, err := s.convert(src)
			if err != nil {
				return 0, err
			}

			return conv(val)
		},
	}
}

func (s IntScanner[S, T]) Float() FloatScanner[S, T] {
	return s.ConvertFloat(func(src int64) (float64, error) {
		return float64(src), nil
	})
}

func (s IntScanner[S, T]) Divide(divisor float64) FloatScanner[S, T] {
	return s.ConvertFloat(func(src int64) (float64, error) {
		return float64(src) / divisor, nil
	})
}

func (s IntScanner[S, T]) ConvertTime(conv Convert[int64, time.Time]) TimeScanner[S, T] {
	return TimeScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (time.Time, error) {
			val, err := s.convert(src)
			if err != nil {
				return time.Time{}, err
			}

			return conv(val)
		},
	}
}

func (s IntScanner[S, T]) Unix() TimeScanner[S, T] {
	return s.ConvertTime(func(src int64) (time.Time, error) {
		return time.Unix(src, 0), nil
	})
}

func (s IntScanner[S, T]) UnixMilli() TimeScanner[S, T] {
	return s.ConvertTime(func(src int64) (time.Time, error) {
		return time.UnixMilli(src), nil
	})
}

func (s IntScanner[S, T]) UnixMicro() TimeScanner[S, T] {
	return s.ConvertTime(func(src int64) (time.Time, error) {
		return time.UnixMicro(src), nil
	})
}

func (s IntScanner[S, T]) UnixNano() TimeScanner[S, T] {
	return s.ConvertTime(func(src int64) (time.Time, error) {
		return time.Unix(0, src), nil
	})
}

type UintScanner[S, T any] struct {
	schema   *Schema[T]
	nullable bool
	convert  func(src S) (uint64, error)
}

func (s UintScanner[S, T]) Scan() (any, func(*T) error) {
	return s.MustTo("").Scan()
}

func (s UintScanner[S, T]) MustTo(path string) Scanner[T] {
	return must(s.To(path))
}

//nolint:gochecknoglobals
var (
	uint64Type        = reflect.TypeFor[uint64]()
	uint64PointerType = reflect.TypeFor[*uint64]()
)

func (s UintScanner[S, T]) To(path string) (Scanner[T], error) {
	f, err := s.schema.Field(path)
	if err != nil {
		return nil, err
	}

	set, err := makeSetter(f,
		uint64Type,
		uint64PointerType,
		[]reflect.Kind{
			reflect.Uint64,
			reflect.Uint32,
			reflect.Uint16,
			reflect.Uint8,
			reflect.Uint,
		},
		func(dst reflect.Value, src uint64) error {
			if dst.OverflowUint(src) {
				return fmt.Errorf("lossy conversion of uint64 value %d to %s", src, dst.Type())
			}

			dst.SetUint(src)

			return nil
		})
	if err != nil {
		return nil, fmt.Errorf("field %s: %w", path, err)
	}

	if reflect.ValueOf(s.convert).Pointer() == justUintPointer {
		return valueFieldScanner[uint64, T]{
			nullable: s.nullable,
			field:    f,
			setter: func(t *T, src uint64) error {
				return set(t, src)
			},
		}, nil
	}

	return valueFieldScanner[S, T]{
		nullable: s.nullable,
		field:    f,
		setter: func(t *T, src S) error {
			val, err := s.convert(src)
			if err != nil {
				return err
			}

			return set(t, val)
		},
	}, nil
}

func (s UintScanner[S, T]) ConvertUint(conv Convert[uint64, uint64]) UintScanner[S, T] {
	return UintScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (uint64, error) {
			val, err := s.convert(src)
			if err != nil {
				return 0, err
			}

			return conv(val)
		},
	}
}

func (s UintScanner[S, T]) ConvertString(conv Convert[uint64, string]) StringScanner[S, T] {
	return StringScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (string, error) {
			val, err := s.convert(src)
			if err != nil {
				return "", err
			}

			return conv(val)
		},
	}
}

func (s UintScanner[S, T]) Format(base int) StringScanner[S, T] {
	return s.ConvertString(func(src uint64) (string, error) {
		return strconv.FormatUint(src, base), nil
	})
}

func (s UintScanner[S, T]) ConvertInt(conv Convert[uint64, int64]) IntScanner[S, T] {
	return IntScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (int64, error) {
			val, err := s.convert(src)
			if err != nil {
				return 0, err
			}

			return conv(val)
		},
	}
}

func (s UintScanner[S, T]) Int() IntScanner[S, T] {
	return s.ConvertInt(func(src uint64) (int64, error) {
		if src > uint64(math.MaxInt64) {
			return 0, fmt.Errorf("lossy conversion of uint64 value %d to int64", src)
		}

		return int64(src), nil
	})
}

func (s UintScanner[S, T]) ConvertFloat(conv Convert[uint64, float64]) FloatScanner[S, T] {
	return FloatScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (float64, error) {
			val, err := s.convert(src)
			if err != nil {
				return 0, err
			}

			return conv(val)
		},
	}
}

func (s UintScanner[S, T]) Float() FloatScanner[S, T] {
	return s.ConvertFloat(func(src uint64) (float64, error) {
		return float64(src), nil
	})
}

type FloatScanner[S, T any] struct {
	schema   *Schema[T]
	nullable bool
	convert  func(src S) (float64, error)
}

func (s FloatScanner[S, T]) Scan() (any, func(*T) error) {
	return s.MustTo("").Scan()
}

func (s FloatScanner[S, T]) MustTo(path string) Scanner[T] {
	return must(s.To(path))
}

//nolint:gochecknoglobals
var (
	float64Type        = reflect.TypeFor[float64]()
	float64PointerType = reflect.TypeFor[*float64]()
)

func (s FloatScanner[S, T]) To(path string) (Scanner[T], error) {
	f, err := s.schema.Field(path)
	if err != nil {
		return nil, err
	}

	set, err := makeSetter(f,
		float64Type,
		float64PointerType,
		[]reflect.Kind{
			reflect.Float64,
			reflect.Float32,
		},
		func(dst reflect.Value, src float64) error {
			if dst.OverflowFloat(src) {
				return fmt.Errorf("lossy conversion of float64 value %f to %s", src, dst.Type())
			}

			dst.SetFloat(src)

			return nil
		})
	if err != nil {
		return nil, err
	}

	if reflect.ValueOf(s.convert).Pointer() == justFloatPointer {
		return valueFieldScanner[float64, T]{
			nullable: s.nullable,
			field:    f,
			setter: func(t *T, src float64) error {
				return set(t, src)
			},
		}, nil
	}

	return valueFieldScanner[S, T]{
		nullable: s.nullable,
		field:    f,
		setter: func(t *T, src S) error {
			val, err := s.convert(src)
			if err != nil {
				return err
			}

			return set(t, val)
		},
	}, nil
}

func (s FloatScanner[S, T]) ConvertFloat(conv Convert[float64, float64]) FloatScanner[S, T] {
	return FloatScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (float64, error) {
			val, err := s.convert(src)
			if err != nil {
				return 0, err
			}

			return conv(val)
		},
	}
}

func (s FloatScanner[S, T]) ConvertString(conv Convert[float64, string]) StringScanner[S, T] {
	return StringScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (string, error) {
			val, err := s.convert(src)
			if err != nil {
				return "", err
			}

			return conv(val)
		},
	}
}

func (s FloatScanner[S, T]) Format(fmt byte, prec int, bitSize int) StringScanner[S, T] {
	return s.ConvertString(func(src float64) (string, error) {
		return strconv.FormatFloat(src, fmt, prec, bitSize), nil
	})
}

func (s FloatScanner[S, T]) ConvertInt(conv Convert[float64, int64]) IntScanner[S, T] {
	return IntScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (int64, error) {
			val, err := s.convert(src)
			if err != nil {
				return 0, err
			}

			return conv(val)
		},
	}
}

func (s FloatScanner[S, T]) Int() IntScanner[S, T] {
	maximum := float64(math.MaxInt64)
	minimum := float64(math.MinInt64)

	return s.ConvertInt(func(src float64) (int64, error) {
		if src < minimum || src > maximum || math.Trunc(src) != src {
			return 0, fmt.Errorf("lossy conversion of float64 value %f to int64", src)
		}

		return int64(src), nil
	})
}

func (s FloatScanner[S, T]) ConvertUint(conv Convert[float64, uint64]) UintScanner[S, T] {
	return UintScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (uint64, error) {
			val, err := s.convert(src)
			if err != nil {
				return 0, err
			}

			return conv(val)
		},
	}
}

func (s FloatScanner[S, T]) Uint() UintScanner[S, T] {
	maximum := float64(math.MaxInt64)

	return s.ConvertUint(func(src float64) (uint64, error) {
		if src < 0 || src > maximum || math.Trunc(src) != src {
			return 0, fmt.Errorf("lossy conversion of float64 value %f to uint64", src)
		}

		return uint64(src), nil
	})
}

func (s FloatScanner[S, T]) Round() FloatScanner[S, T] {
	return s.ConvertFloat(func(src float64) (float64, error) {
		return math.Round(src), nil
	})
}

func (s FloatScanner[S, T]) Multiply(factor float64) FloatScanner[S, T] {
	return s.ConvertFloat(func(src float64) (float64, error) {
		return src * factor, nil
	})
}

func (s FloatScanner[S, T]) Divide(divisor float64) FloatScanner[S, T] {
	return s.ConvertFloat(func(src float64) (float64, error) {
		return src / divisor, nil
	})
}

func (s FloatScanner[S, T]) Add(summand float64) FloatScanner[S, T] {
	return s.ConvertFloat(func(src float64) (float64, error) {
		return src + summand, nil
	})
}

func (s FloatScanner[S, T]) Subtract(subtrahend float64) FloatScanner[S, T] {
	return s.ConvertFloat(func(src float64) (float64, error) {
		return src - subtrahend, nil
	})
}

type ComplexScanner[S, T any] struct {
	schema   *Schema[T]
	nullable bool
	convert  func(src S) (complex128, error)
}

func (s ComplexScanner[S, T]) Scan() (any, func(*T) error) {
	return s.MustTo("").Scan()
}

func (s ComplexScanner[S, T]) MustTo(path string) Scanner[T] {
	return must(s.To(path))
}

//nolint:gochecknoglobals
var (
	complex128Type        = reflect.TypeFor[complex128]()
	complex128PointerType = reflect.TypeFor[*complex128]()
)

func (s ComplexScanner[S, T]) To(path string) (Scanner[T], error) {
	f, err := s.schema.Field(path)
	if err != nil {
		return nil, err
	}

	set, err := makeSetter(f,
		complex128Type,
		complex128PointerType,
		[]reflect.Kind{
			reflect.Complex128,
			reflect.Complex64,
		},
		func(dst reflect.Value, src complex128) error {
			if dst.OverflowComplex(src) {
				return fmt.Errorf("lossy conversion of complex128 value %f to %s", src, dst.Type())
			}

			dst.SetComplex(src)

			return nil
		})
	if err != nil {
		return nil, err
	}

	return valueFieldScanner[S, T]{
		nullable: s.nullable,
		field:    f,
		setter: func(t *T, src S) error {
			val, err := s.convert(src)
			if err != nil {
				return err
			}

			return set(t, val)
		},
	}, nil
}

type BoolScanner[S, T any] struct {
	schema   *Schema[T]
	nullable bool
	convert  func(src S) (bool, error)
}

func (s BoolScanner[S, T]) Scan() (any, func(*T) error) {
	return s.MustTo("").Scan()
}

func (s BoolScanner[S, T]) MustTo(path string) Scanner[T] {
	return must(s.To(path))
}

//nolint:gochecknoglobals
var (
	boolType        = reflect.TypeFor[bool]()
	boolPointerType = reflect.TypeFor[*bool]()
)

//nolint:dupl
func (s BoolScanner[S, T]) To(path string) (Scanner[T], error) {
	f, err := s.schema.Field(path)
	if err != nil {
		return nil, err
	}

	set, err := makeSetter(f,
		boolType,
		boolPointerType,
		[]reflect.Kind{
			reflect.Bool,
		},
		func(dst reflect.Value, src bool) error {
			dst.SetBool(src)

			return nil
		})
	if err != nil {
		return nil, err
	}

	if reflect.ValueOf(s.convert).Pointer() == justBoolPointer {
		return valueFieldScanner[bool, T]{
			nullable: s.nullable,
			field:    f,
			setter: func(t *T, src bool) error {
				return set(t, src)
			},
		}, nil
	}

	return valueFieldScanner[S, T]{
		nullable: s.nullable,
		field:    f,
		setter: func(t *T, src S) error {
			val, err := s.convert(src)
			if err != nil {
				return err
			}

			return set(t, val)
		},
	}, nil
}

func (s BoolScanner[S, T]) ConvertString(conv func(src bool) (string, error)) StringScanner[S, T] {
	return StringScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (string, error) {
			val, err := s.convert(src)
			if err != nil {
				return "", err
			}

			return conv(val)
		},
	}
}

func (s BoolScanner[S, T]) Format() StringScanner[S, T] {
	return s.ConvertString(func(src bool) (string, error) {
		return strconv.FormatBool(src), nil
	})
}

func (s BoolScanner[S, T]) ConvertInt(conv func(src bool) (int64, error)) IntScanner[S, T] {
	return IntScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (int64, error) {
			val, err := s.convert(src)
			if err != nil {
				return 0, err
			}

			return conv(val)
		},
	}
}

func (s BoolScanner[S, T]) Int() IntScanner[S, T] {
	return s.ConvertInt(func(src bool) (int64, error) {
		if src {
			return 1, nil
		}

		return 0, nil
	})
}

func (s BoolScanner[S, T]) ConvertUint(conv func(src bool) (uint64, error)) UintScanner[S, T] {
	return UintScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (uint64, error) {
			val, err := s.convert(src)
			if err != nil {
				return 0, err
			}

			return conv(val)
		},
	}
}

func (s BoolScanner[S, T]) Uint() UintScanner[S, T] {
	return s.ConvertUint(func(src bool) (uint64, error) {
		if src {
			return 1, nil
		}

		return 0, nil
	})
}

func (s BoolScanner[S, T]) ConvertFloat(conv func(src bool) (float64, error)) FloatScanner[S, T] {
	return FloatScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (float64, error) {
			val, err := s.convert(src)
			if err != nil {
				return 0, err
			}

			return conv(val)
		},
	}
}

func (s BoolScanner[S, T]) Float() FloatScanner[S, T] {
	return s.ConvertFloat(func(src bool) (float64, error) {
		if src {
			return 1, nil
		}

		return 0, nil
	})
}

type TimeScanner[S, T any] struct {
	schema   *Schema[T]
	nullable bool
	convert  func(src S) (time.Time, error)
}

func (s TimeScanner[S, T]) Scan() (any, func(*T) error) {
	return s.MustTo("").Scan()
}

func (s TimeScanner[S, T]) MustTo(path string) Scanner[T] {
	return must(s.To(path))
}

//nolint:gochecknoglobals
var (
	timeType        = reflect.TypeFor[time.Time]()
	timePointerType = reflect.TypeFor[*time.Time]()
)

//nolint:dupl
func (s TimeScanner[S, T]) To(path string) (Scanner[T], error) {
	f, err := s.schema.Field(path)
	if err != nil {
		return nil, err
	}

	set, err := makeSetter[time.Time](f, timeType, timePointerType, nil, nil)
	if err != nil {
		return nil, err
	}

	if reflect.ValueOf(s.convert).Pointer() == justTimePointer {
		return valueFieldScanner[time.Time, T]{
			nullable: s.nullable,
			field:    f,
			setter: func(t *T, src time.Time) error {
				return set(t, src)
			},
		}, nil
	}

	return valueFieldScanner[S, T]{
		nullable: s.nullable,
		field:    f,
		setter: func(t *T, src S) error {
			val, err := s.convert(src)
			if err != nil {
				return err
			}

			return set(t, val)
		},
	}, nil
}

func (s TimeScanner[S, T]) ConvertString(conv func(src time.Time) (string, error)) StringScanner[S, T] {
	return StringScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (string, error) {
			val, err := s.convert(src)
			if err != nil {
				return "", err
			}

			return conv(val)
		},
	}
}

func (s TimeScanner[S, T]) Format(layout string) StringScanner[S, T] {
	return s.ConvertString(func(src time.Time) (string, error) {
		return src.Format(layout), nil
	})
}

func (s TimeScanner[S, T]) String() StringScanner[S, T] {
	return s.ConvertString(func(src time.Time) (string, error) {
		return src.String(), nil
	})
}

type DurationScanner[S, T any] struct {
	schema   *Schema[T]
	nullable bool
	convert  func(src S) (time.Duration, error)
}

func (s DurationScanner[S, T]) Scan() (any, func(*T) error) {
	return s.MustTo("").Scan()
}

func (s DurationScanner[S, T]) MustTo(path string) Scanner[T] {
	return must(s.To(path))
}

//nolint:gochecknoglobals
var (
	durationType        = reflect.TypeFor[time.Duration]()
	durationPointerType = reflect.TypeFor[*time.Duration]()
)

//nolint:dupl
func (s DurationScanner[S, T]) To(path string) (Scanner[T], error) {
	f, err := s.schema.Field(path)
	if err != nil {
		return nil, err
	}

	set, err := makeSetter[time.Duration](f, durationType, durationPointerType, nil, nil)
	if err != nil {
		return nil, err
	}

	if reflect.ValueOf(s.convert).Pointer() == justDurationPointer {
		return valueFieldScanner[time.Duration, T]{
			nullable: s.nullable,
			field:    f,
			setter: func(t *T, src time.Duration) error {
				return set(t, src)
			},
		}, nil
	}

	return valueFieldScanner[S, T]{
		nullable: s.nullable,
		field:    f,
		setter: func(t *T, src S) error {
			val, err := s.convert(src)
			if err != nil {
				return err
			}

			return set(t, val)
		},
	}, nil
}

func (s DurationScanner[S, T]) ConvertDuration(conv func(time.Duration) (time.Duration, error)) DurationScanner[S, T] {
	return DurationScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (time.Duration, error) {
			val, err := s.convert(src)
			if err != nil {
				return 0, err
			}

			return conv(val)
		},
	}
}

func (s DurationScanner[S, T]) Round(m time.Duration) DurationScanner[S, T] {
	return s.ConvertDuration(func(d time.Duration) (time.Duration, error) {
		return d.Round(m), nil
	})
}

func (s DurationScanner[S, T]) Truncate(m time.Duration) DurationScanner[S, T] {
	return s.ConvertDuration(func(d time.Duration) (time.Duration, error) {
		return d.Truncate(m), nil
	})
}

func (s DurationScanner[S, T]) ConvertString(conv func(time.Duration) (string, error)) StringScanner[S, T] {
	return StringScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (string, error) {
			val, err := s.convert(src)
			if err != nil {
				return "", err
			}

			return conv(val)
		},
	}
}

func (s DurationScanner[S, T]) String() StringScanner[S, T] {
	return s.ConvertString(func(d time.Duration) (string, error) {
		return d.String(), nil
	})
}

func (s DurationScanner[S, T]) ConvertInt(conv func(time.Duration) (int64, error)) IntScanner[S, T] {
	return IntScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (int64, error) {
			val, err := s.convert(src)
			if err != nil {
				return 0, err
			}

			return conv(val)
		},
	}
}

func (s DurationScanner[S, T]) Milliseconds() IntScanner[S, T] {
	return s.ConvertInt(func(d time.Duration) (int64, error) {
		return d.Milliseconds(), nil
	})
}

func (s DurationScanner[S, T]) Microseconds() IntScanner[S, T] {
	return s.ConvertInt(func(d time.Duration) (int64, error) {
		return d.Microseconds(), nil
	})
}

func (s DurationScanner[S, T]) Nanoseconds() IntScanner[S, T] {
	return s.ConvertInt(func(d time.Duration) (int64, error) {
		return d.Nanoseconds(), nil
	})
}

func (s DurationScanner[S, T]) ConvertFloat(conv func(time.Duration) (float64, error)) FloatScanner[S, T] {
	return FloatScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) (float64, error) {
			val, err := s.convert(src)
			if err != nil {
				return 0, err
			}

			return conv(val)
		},
	}
}

func (s DurationScanner[S, T]) Hours() FloatScanner[S, T] {
	return s.ConvertFloat(func(d time.Duration) (float64, error) {
		return d.Hours(), nil
	})
}

func (s DurationScanner[S, T]) Minutes() FloatScanner[S, T] {
	return s.ConvertFloat(func(d time.Duration) (float64, error) {
		return d.Minutes(), nil
	})
}

func (s DurationScanner[S, T]) Seconds() FloatScanner[S, T] {
	return s.ConvertFloat(func(d time.Duration) (float64, error) {
		return d.Seconds(), nil
	})
}

type BytesScanner[S, T any] struct {
	schema   *Schema[T]
	nullable bool
	convert  func(src S) (sql.RawBytes, error)
}

func (s BytesScanner[S, T]) Scan() (any, func(*T) error) {
	return s.MustTo("").Scan()
}

func (s BytesScanner[S, T]) MustTo(path string) Scanner[T] {
	return must(s.To(path))
}

//nolint:gochecknoglobals
var bytesType = reflect.TypeFor[[]byte]()

func (s BytesScanner[S, T]) To(path string) (Scanner[T], error) {
	f, err := s.schema.Field(path)
	if err != nil {
		return nil, err
	}

	var set func(t *T, src sql.RawBytes) error

	switch {
	case f.Type == bytesType:
		set = func(t *T, src sql.RawBytes) error {
			*(*[]byte)(f.Pointer(t)) = slices.Clone(src)

			return nil
		}
	case f.DerefType == bytesType:
		set = func(t *T, src sql.RawBytes) error {
			f.AccessDeref(t).Set(reflect.ValueOf(slices.Clone(src)))

			return nil
		}
	case bytesType.ConvertibleTo(f.DerefType):
		set = func(t *T, src sql.RawBytes) error {
			f.AccessDeref(t).Set(reflect.ValueOf(slices.Clone(src)).Convert(f.DerefType))

			return nil
		}
	default:
		return nil, fmt.Errorf("field %s: not assignable to []byte: %s", path, f.DerefType)
	}

	if reflect.ValueOf(s.convert).Pointer() == justBytesPointer {
		return valueFieldScanner[sql.RawBytes, T]{
			nullable: s.nullable,
			field:    f,
			setter: func(t *T, src sql.RawBytes) error {
				return set(t, src)
			},
		}, nil
	}

	return valueFieldScanner[S, T]{
		nullable: s.nullable,
		field:    f,
		setter: func(t *T, src S) error {
			val, err := s.convert(src)
			if err != nil {
				return err
			}

			return set(t, val)
		},
	}, nil
}

func (s BytesScanner[S, T]) JSON() JSONScanner[S, T] {
	return JSONScanner[S, T](s)
}

func (s BytesScanner[S, T]) Text() TextScanner[S, T] {
	return TextScanner[S, T](s)
}

func (s BytesScanner[S, T]) Binary() BinaryScanner[S, T] {
	return BinaryScanner[S, T](s)
}

type JSONScanner[S, T any] struct {
	schema   *Schema[T]
	nullable bool
	convert  func(src S) (sql.RawBytes, error)
}

func (s JSONScanner[S, T]) Scan() (any, func(*T) error) {
	return s.MustTo("").Scan()
}

func (s JSONScanner[S, T]) MustTo(path string) Scanner[T] {
	return must(s.To(path))
}

func (s JSONScanner[S, T]) To(path string) (Scanner[T], error) {
	f, err := s.schema.Field(path)
	if err != nil {
		return nil, err
	}

	if reflect.ValueOf(s.convert).Pointer() == justBytesPointer {
		return valueFieldScanner[sql.RawBytes, T]{
			nullable: s.nullable,
			field:    f,
			setter: func(t *T, src sql.RawBytes) error {
				return json.Unmarshal(src, f.AccessDeref(t).Addr().Interface())
			},
		}, nil
	}

	return valueFieldScanner[S, T]{
		nullable: s.nullable,
		field:    f,
		setter: func(t *T, src S) error {
			val, err := s.convert(src)
			if err != nil {
				return err
			}

			return json.Unmarshal(val, f.AccessDeref(t).Addr().Interface())
		},
	}, nil
}

type TextScanner[S, T any] struct {
	schema   *Schema[T]
	nullable bool
	convert  func(src S) (sql.RawBytes, error)
}

func (s TextScanner[S, T]) Scan() (any, func(*T) error) {
	return s.MustTo("").Scan()
}

func (s TextScanner[S, T]) MustTo(path string) Scanner[T] {
	return must(s.To(path))
}

//nolint:gochecknoglobals
var unmarshalTextType = reflect.TypeFor[encoding.TextUnmarshaler]()

//nolint:dupl
func (s TextScanner[S, T]) To(path string) (Scanner[T], error) {
	f, err := s.schema.Field(path)
	if err != nil {
		return nil, err
	}

	if !reflect.PointerTo(f.DerefType).Implements(unmarshalTextType) {
		return nil, fmt.Errorf("field %s: encoding.TextUnmarshaler not implemented: %s", path, f.DerefType)
	}

	if reflect.ValueOf(s.convert).Pointer() == justBytesPointer {
		return valueFieldScanner[sql.RawBytes, T]{
			nullable: s.nullable,
			field:    f,
			setter: func(t *T, src sql.RawBytes) error {
				//nolint:forcetypeassert
				return f.AccessDeref(t).Addr().Interface().(encoding.TextUnmarshaler).UnmarshalText(src)
			},
		}, nil
	}

	return valueFieldScanner[S, T]{
		nullable: s.nullable,
		field:    f,
		setter: func(t *T, src S) error {
			val, err := s.convert(src)
			if err != nil {
				return err
			}

			//nolint:forcetypeassert
			return f.AccessDeref(t).Addr().Interface().(encoding.TextUnmarshaler).UnmarshalText(val)
		},
	}, nil
}

type BinaryScanner[S, T any] struct {
	schema   *Schema[T]
	nullable bool
	convert  func(src S) (sql.RawBytes, error)
}

func (s BinaryScanner[S, T]) Scan() (any, func(*T) error) {
	return s.MustTo("").Scan()
}

func (s BinaryScanner[S, T]) MustTo(path string) Scanner[T] {
	return must(s.To(path))
}

//nolint:gochecknoglobals
var unmarshalBinaryType = reflect.TypeFor[encoding.BinaryUnmarshaler]()

//nolint:dupl
func (s BinaryScanner[S, T]) To(path string) (Scanner[T], error) {
	f, err := s.schema.Field(path)
	if err != nil {
		return nil, err
	}

	if !reflect.PointerTo(f.DerefType).Implements(unmarshalBinaryType) {
		return nil, fmt.Errorf("field %s: encoding.BinaryUnmarshaler not implemented: %s", path, f.DerefType)
	}

	if reflect.ValueOf(s.convert).Pointer() == justBytesPointer {
		return valueFieldScanner[sql.RawBytes, T]{
			nullable: s.nullable,
			field:    f,
			setter: func(t *T, src sql.RawBytes) error {
				//nolint:forcetypeassert
				return f.AccessDeref(t).Addr().Interface().(encoding.BinaryUnmarshaler).UnmarshalBinary(src)
			},
		}, nil
	}

	return valueFieldScanner[S, T]{
		nullable: s.nullable,
		field:    f,
		setter: func(t *T, src S) error {
			val, err := s.convert(src)
			if err != nil {
				return err
			}

			//nolint:forcetypeassert
			return f.AccessDeref(t).Addr().Interface().(encoding.BinaryUnmarshaler).UnmarshalBinary(val)
		},
	}, nil
}

type StringSliceScanner[S, T any] struct {
	schema   *Schema[T]
	nullable bool
	convert  func(src S) ([]string, error)
}

func (s StringSliceScanner[S, T]) Scan() (any, func(*T) error) {
	return s.MustTo("").Scan()
}

func (s StringSliceScanner[S, T]) MustTo(path string) Scanner[T] {
	return must(s.To(path))
}

func (s StringSliceScanner[S, T]) Convert(conv Convert[[]string, []string]) StringSliceScanner[S, T] {
	return StringSliceScanner[S, T]{
		schema:   s.schema,
		nullable: s.nullable,
		convert: func(src S) ([]string, error) {
			res, err := s.convert(src)
			if err != nil {
				return nil, err
			}

			return conv(res)
		},
	}
}

func (s StringSliceScanner[S, T]) Asc() StringSliceScanner[S, T] {
	return s.Convert(func(src []string) ([]string, error) {
		slices.Sort(src)

		return src, nil
	})
}

func (s StringSliceScanner[S, T]) Desc() StringSliceScanner[S, T] {
	return s.Convert(func(src []string) ([]string, error) {
		slices.Sort(src)
		slices.Reverse(src)

		return src, nil
	})
}

//nolint:gochecknoglobals
var stringSliceType = reflect.TypeFor[[]string]()

//nolint:funlen,cyclop
func (s StringSliceScanner[S, T]) To(path string) (Scanner[T], error) {
	f, err := s.schema.Field(path)
	if err != nil {
		return nil, err
	}

	var set func(t *T, src []string) error

	switch {
	case f.Type == stringSliceType:
		set = func(t *T, src []string) error {
			*(*[]string)(f.Pointer(t)) = src

			return nil
		}
	case f.DerefType == stringSliceType:
		set = func(t *T, src []string) error {
			f.AccessDeref(t).Set(reflect.ValueOf(src))

			return nil
		}
	case f.DerefKind == reflect.Array && derefType(f.DerefType.Elem()).Kind() == reflect.String:
		set = func(t *T, src []string) error {
			if len(src) > f.DerefType.Len() {
				return fmt.Errorf("field %s: too many elements for %s: %d", path, f.DerefType, len(src))
			}

			dst := f.AccessDeref(t)

			for i, p := range src {
				derefDst(dst.Index(i)).SetString(p)
			}

			return nil
		}
	case f.DerefKind == reflect.Slice && derefType(f.DerefType.Elem()).Kind() == reflect.String:
		set = func(t *T, src []string) error {
			dst := f.AccessDeref(t)

			dst.Set(reflect.MakeSlice(f.DerefType, len(src), len(src)))

			for i, p := range src {
				derefDst(dst.Index(i)).SetString(p)
			}

			return nil
		}
	default:
		return nil, fmt.Errorf("field %s: not assignable to []string: %s", path, f.DerefType)
	}

	return valueFieldScanner[S, T]{
		nullable: s.nullable,
		field:    f,
		setter: func(t *T, src S) error {
			val, err := s.convert(src)
			if err != nil {
				return err
			}

			return set(t, val)
		},
	}, nil
}

type Assigner interface {
	Scan(src any) error
	AssignTo(dst any) error
}

type AssignScanner[T any] struct {
	schema   *Schema[T]
	nullable bool
	init     func() Assigner
}

func (s AssignScanner[T]) To(path string) (Scanner[T], error) {
	f, err := s.schema.Field(path)
	if err != nil {
		return nil, err
	}

	return valueFieldScanner[any, T]{
		nullable: false,
		field:    f,
		setter: func(t *T, src any) error {
			if !s.nullable && src == nil {
				return fmt.Errorf("field %s: is not nullable", path)
			}

			a := s.init()

			if err := a.Scan(src); err != nil {
				return fmt.Errorf("field %s: %w", path, err)
			}

			return a.AssignTo(f.AccessDeref(t).Addr())
		},
	}, nil
}

type valueFieldScanner[V, T any] struct {
	setter   func(t *T, src V) error
	field    Field[T]
	nullable bool
}

func (s valueFieldScanner[V, T]) Scan() (any, func(t *T) error) {
	if s.nullable {
		var src sql.Null[V]

		return &src, func(t *T) error {
			if !src.Valid {
				if s.field.Type.Kind() == reflect.Pointer {
					s.field.Access(t).SetZero()
				}

				return nil
			}

			return s.setter(t, src.V)
		}
	}

	var src V

	return &src, func(t *T) error {
		return s.setter(t, src)
	}
}

type Convert[S, V any] func(src S) (V, error)

func just[V any]() Convert[V, V] {
	return func(src V) (V, error) {
		return src, nil
	}
}

//nolint:gochecknoglobals
var (
	justString   = just[string]()
	justInt      = just[int64]()
	justUint     = just[uint64]()
	justFloat    = just[float64]()
	justBool     = just[bool]()
	justTime     = just[time.Time]()
	justDuration = just[time.Duration]()
	justBytes    = just[sql.RawBytes]()

	justStringPointer   = reflect.ValueOf(justString).Pointer()
	justIntPointer      = reflect.ValueOf(justInt).Pointer()
	justUintPointer     = reflect.ValueOf(justUint).Pointer()
	justFloatPointer    = reflect.ValueOf(justFloat).Pointer()
	justBoolPointer     = reflect.ValueOf(justBool).Pointer()
	justTimePointer     = reflect.ValueOf(justTime).Pointer()
	justDurationPointer = reflect.ValueOf(justDuration).Pointer()
	justBytesPointer    = reflect.ValueOf(justBytes).Pointer()
)

type Setter[V, T any] func(t *T, src V) error

func makeSetter[V, T any](
	field Field[T],
	typ reflect.Type,
	pointerType reflect.Type,
	kinds []reflect.Kind,
	set func(dst reflect.Value, src V) error) (Setter[V, T], error) {
	switch {
	case field.Type == typ:
		return func(t *T, src V) error {
			*(*V)(field.Pointer(t)) = src

			return nil
		}, nil
	case field.Type == pointerType:
		return func(t *T, src V) error {
			*(**V)(field.Pointer(t)) = &src

			return nil
		}, nil
	case field.DerefType == typ:
		return func(t *T, src V) error {
			field.AccessDeref(t).Set(reflect.ValueOf(src))

			return nil
		}, nil
	case slices.Contains(kinds, field.DerefKind) && set != nil:
		return func(t *T, src V) error {
			return set(field.AccessDeref(t), src)
		}, nil
	case typ.ConvertibleTo(field.DerefType):
		return func(t *T, src V) error {
			field.AccessDeref(t).Set(reflect.ValueOf(src).Convert(field.DerefType))

			return nil
		}, nil
	default:
		return nil, fmt.Errorf("not assignable to %s: %s", typ, field.DerefType)
	}
}

func derefType(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	return t
}

func derefDst(dst reflect.Value) reflect.Value {
	for dst.Kind() == reflect.Pointer {
		if dst.IsNil() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}

		dst = dst.Elem()
	}

	return dst
}

func must[T any](t T, err error) T {
	if err != nil {
		panic(err)
	}

	return t
}
