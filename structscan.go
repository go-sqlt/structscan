package structscan

import (
	"database/sql"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"
)

type Scanner[T any] func() (any, func(t *T) error)

func All[T any](rows *sql.Rows, scanners ...Scanner[T]) ([]T, error) {
	dest, set := destSet(scanners...)

	var (
		all []T
		err error
	)

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

func One[T any](rows *sql.Rows, scanners ...Scanner[T]) (T, error) {
	dest, set := destSet(scanners...)

	var (
		one T
		err error
	)

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

func First[T any](row *sql.Row, scanners ...Scanner[T]) (T, error) {
	dest, set := destSet(scanners...)

	var (
		first T
		err   error
	)

	if err = row.Scan(dest...); err != nil {
		return first, err
	}

	if err = set(&first); err != nil {
		return first, err
	}

	return first, nil
}

func destSet[T any](scanners ...Scanner[T]) ([]any, func(*T) error) {
	if len(scanners) == 0 {
		scanners = []Scanner[T]{
			func() (any, func(t *T) error) {
				var value T

				return &value, func(t *T) error {
					*t = value

					return nil
				}
			},
		}
	}

	var (
		values  = make([]any, len(scanners))
		setters = make([]func(*T) error, len(scanners))
	)

	for i, d := range scanners {
		values[i], setters[i] = d()
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
	}
}

func New[T any]() *Struct[T] {
	return &Struct[T]{
		typ:      reflect.TypeFor[T](),
		fieldMap: &sync.Map{},
	}
}

type Struct[T any] struct {
	typ      reflect.Type
	fieldMap *sync.Map
}

func (s *Struct[T]) Field(path string) (Field[T], error) {
	if loaded, ok := s.fieldMap.Load(path); ok {
		return loaded.(Field[T]), nil
	}

	t := s.typ
	indices := []int{}

	for t.Kind() == reflect.Pointer {
		t = t.Elem()

		indices = append(indices, -1)

		continue
	}

	if path != "" {
		for part := range strings.SplitSeq(path, ".") {
			switch t.Kind() {
			default:
				return Field[T]{}, fmt.Errorf("invalid field access on type %s", t.Name())
			case reflect.Struct:
				sf, found := t.FieldByName(part)
				if !found {
					return Field[T]{}, fmt.Errorf("field %s not found in struct %s", path, t.Name())
				}

				if !sf.IsExported() {
					return Field[T]{}, fmt.Errorf("field %s in struct %s is not exported", path, t.Name())
				}

				indices = append(indices, sf.Index[0])
				t = sf.Type
			}

			for t.Kind() == reflect.Pointer {
				t = t.Elem()

				indices = append(indices, -1)

				continue
			}
		}
	}

	l := len(indices)

	field := Field[T]{
		typ:         t,
		pointerType: reflect.PointerTo(t),
		pointer:     l > 0 && indices[l-1] == -1,
		indices:     indices,
	}

	s.fieldMap.Store(path, field)

	return field, nil
}

func (s *Struct[T]) Scan(path string) (Scanner[T], error) {
	f, err := s.Field(path)
	if err != nil {
		return nil, err
	}

	return f.Scan()
}

func (s *Struct[T]) ScanString(path string) (Scanner[T], error) {
	f, err := s.Field(path)
	if err != nil {
		return nil, err
	}

	return f.ScanString()
}

func (s *Struct[T]) ScanInt(path string) (Scanner[T], error) {
	f, err := s.Field(path)
	if err != nil {
		return nil, err
	}

	return f.ScanInt()
}

func (s *Struct[T]) ScanUint(path string) (Scanner[T], error) {
	f, err := s.Field(path)
	if err != nil {
		return nil, err
	}

	return f.ScanUint()
}

func (s *Struct[T]) ScanFloat(path string) (Scanner[T], error) {
	f, err := s.Field(path)
	if err != nil {
		return nil, err
	}

	return f.ScanFloat()
}

func (s *Struct[T]) ScanBool(path string) (Scanner[T], error) {
	f, err := s.Field(path)
	if err != nil {
		return nil, err
	}

	return f.ScanBool()
}

func (s *Struct[T]) ScanBytes(path string) (Scanner[T], error) {
	f, err := s.Field(path)
	if err != nil {
		return nil, err
	}

	return f.ScanBytes()
}

func (s *Struct[T]) ScanTime(path string) (Scanner[T], error) {
	f, err := s.Field(path)
	if err != nil {
		return nil, err
	}

	return f.ScanTime()
}

func (s *Struct[T]) ScanJSON(path string) (Scanner[T], error) {
	f, err := s.Field(path)
	if err != nil {
		return nil, err
	}

	return f.ScanJSON()
}

func (s *Struct[T]) ScanBinary(path string) (Scanner[T], error) {
	f, err := s.Field(path)
	if err != nil {
		return nil, err
	}

	return f.ScanBinary()
}

func (s *Struct[T]) ScanText(path string) (Scanner[T], error) {
	f, err := s.Field(path)
	if err != nil {
		return nil, err
	}

	return f.ScanText()
}

func (s *Struct[T]) ScanStringTime(path string, layout string, location string) (Scanner[T], error) {
	f, err := s.Field(path)
	if err != nil {
		return nil, err
	}

	return f.ScanStringTime(layout, location)
}

func (s *Struct[T]) ScanStringSlice(path string, sep string) (Scanner[T], error) {
	f, err := s.Field(path)
	if err != nil {
		return nil, err
	}

	return f.ScanStringSlice(sep)
}

type Field[T any] struct {
	indices     []int
	typ         reflect.Type
	pointerType reflect.Type
	pointer     bool
}

func (f Field[T]) Access(t *T) reflect.Value {
	v := reflect.ValueOf(t).Elem()

	for _, idx := range f.indices {
		if idx < 0 {
			if v.IsNil() {
				v.Set(reflect.New(v.Type().Elem()))
			}

			v = v.Elem()

			continue
		}

		v = v.Field(idx)
	}

	return v
}

var scannerType = reflect.TypeFor[sql.Scanner]()

func (f Field[T]) Scan() (Scanner[T], error) {
	if !f.pointerType.Implements(scannerType) {
		return nil, fmt.Errorf("invalid field type %s for scanner", f.typ)
	}

	return scanAny(f, func(dest reflect.Value, value any) error {
		return dest.Addr().Interface().(sql.Scanner).Scan(value)
	}), nil
}

func (f Field[T]) ScanString() (Scanner[T], error) {
	if f.typ.Kind() != reflect.String {
		return nil, fmt.Errorf("invalid field type %s for string scanner", f.typ)
	}

	return scanAny(f, func(dest reflect.Value, value string) error {
		dest.SetString(value)

		return nil
	}), nil
}

func (f Field[T]) ScanInt() (Scanner[T], error) {
	switch f.typ.Kind() {
	default:
		return nil, fmt.Errorf("invalid field type %s for int scanner", f.typ)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return scanAny(f, func(dest reflect.Value, value int64) error {
			dest.SetInt(value)

			return nil
		}), nil
	}
}

func (f Field[T]) ScanUint() (Scanner[T], error) {
	switch f.typ.Kind() {
	default:
		return nil, fmt.Errorf("invalid field type %s for uint scanner", f.typ)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return scanAny(f, func(dest reflect.Value, value uint64) error {
			dest.SetUint(value)

			return nil
		}), nil
	}
}

func (f Field[T]) ScanFloat() (Scanner[T], error) {
	switch f.typ.Kind() {
	default:
		return nil, fmt.Errorf("invalid field type %s for float scanner", f.typ)
	case reflect.Float64, reflect.Float32:
		return scanAny(f, func(dest reflect.Value, value float64) error {
			dest.SetFloat(value)

			return nil
		}), nil
	}
}

func (f Field[T]) ScanBool() (Scanner[T], error) {
	if f.typ.Kind() != reflect.Bool {
		return nil, fmt.Errorf("invalid field type %s for bool scanner", f.typ)
	}

	return scanAny(f, func(dest reflect.Value, value bool) error {
		dest.SetBool(value)

		return nil
	}), nil
}

var byteSliceType = reflect.TypeFor[[]byte]()

func (f Field[T]) ScanBytes() (Scanner[T], error) {
	if f.typ != byteSliceType {
		return nil, fmt.Errorf("invalid field type %s for bytes scanner", f.typ)
	}

	return scanAny(f, func(dest reflect.Value, value []byte) error {
		dest.SetBytes(value)

		return nil
	}), nil
}

var timeType = reflect.TypeFor[time.Time]()

func (f Field[T]) ScanTime() (Scanner[T], error) {
	if f.typ != timeType {
		return nil, fmt.Errorf("invalid field type %s for time scanner", f.typ)
	}

	return scanAny(f, func(dest reflect.Value, value time.Time) error {
		dest.Set(reflect.ValueOf(value))

		return nil
	}), nil
}

func (f Field[T]) ScanJSON() (Scanner[T], error) {
	return scanAny(f, func(dest reflect.Value, value []byte) error {
		return json.Unmarshal(value, dest.Addr().Interface())
	}), nil
}

var binaryMarshalerType = reflect.TypeFor[encoding.BinaryUnmarshaler]()

func (f Field[T]) ScanBinary() (Scanner[T], error) {
	if !f.pointerType.Implements(binaryMarshalerType) {
		return nil, fmt.Errorf("invalid field type %s for binary scanner", f.typ)
	}

	return scanAny(f, func(dest reflect.Value, value []byte) error {
		return dest.Addr().Interface().(encoding.BinaryUnmarshaler).UnmarshalBinary(value)
	}), nil
}

var textUnarshalerType = reflect.TypeFor[encoding.TextUnmarshaler]()

func (f Field[T]) ScanText() (Scanner[T], error) {
	if !f.pointerType.Implements(textUnarshalerType) {
		return nil, fmt.Errorf("invalid field type %s for text scanner", f.typ)
	}

	return scanAny(f, func(dest reflect.Value, value []byte) error {
		return dest.Addr().Interface().(encoding.TextUnmarshaler).UnmarshalText(value)
	}), nil
}

func (f Field[T]) ScanStringSlice(sep string) (Scanner[T], error) {
	if f.typ.Kind() != reflect.Slice || f.typ.Elem().Kind() != reflect.String {
		return nil, fmt.Errorf("invalid field type %s for string slice scanner", f.typ)
	}

	var indirections int

	elem := f.typ.Elem()
	for elem.Kind() == reflect.Pointer {
		elem = elem.Elem()

		indirections++

		continue
	}

	return scanAny(f, func(dest reflect.Value, value string) error {
		split := slices.DeleteFunc(strings.Split(value, sep), func(d string) bool {
			return d == ""
		})

		dest.Set(reflect.MakeSlice(f.typ, len(split), len(split)))

		for i, v := range split {
			index := dest.Index(i)

			for range indirections {
				if index.IsNil() {
					index.Set(reflect.New(index.Type().Elem()))
				}

				index = index.Elem()
			}

			index.SetString(v)
		}

		return nil
	}), nil
}

var layoutMap = map[string]string{
	"DateTime":    time.DateTime,
	"DateOnly":    time.DateOnly,
	"TimeOnly":    time.TimeOnly,
	"RFC3339":     time.RFC3339,
	"RFC3339Nano": time.RFC3339Nano,
	"Layout":      time.Layout,
	"ANSIC":       time.ANSIC,
	"UnixDate":    time.UnixDate,
	"RubyDate":    time.RubyDate,
	"RFC822":      time.RFC822,
	"RFC822Z":     time.RFC822Z,
	"RFC850":      time.RFC850,
	"RFC1123":     time.RFC1123,
	"RFC1123Z":    time.RFC1123Z,
	"Kitchen":     time.Kitchen,
	"Stamp":       time.Stamp,
	"StampMilli":  time.StampMilli,
	"StampMicro":  time.StampMicro,
	"StampNano":   time.StampNano,
}

func (f Field[T]) ScanStringTime(layout string, location string) (Scanner[T], error) {
	if f.typ != timeType {
		return nil, fmt.Errorf("invalid field type %s for string time scanner", f.typ)
	}

	loc, err := time.LoadLocation(location)
	if err != nil {
		return nil, err
	}

	if l, ok := layoutMap[layout]; ok {
		layout = l
	}

	if layout == "" {
		return nil, fmt.Errorf("invalid layout %s for string time scanner", layout)
	}

	return scanAny(f, func(dest reflect.Value, value string) error {
		t, err := time.ParseInLocation(layout, value, loc)
		if err != nil {
			return err
		}

		dest.Set(reflect.ValueOf(t))

		return nil
	}), nil
}

func scanAny[V, T any](field Field[T], set func(dest reflect.Value, value V) error) Scanner[T] {
	if field.pointer {
		return func() (any, func(t *T) error) {
			var dest *V

			return &dest, func(t *T) error {
				if dest == nil {
					return nil
				}

				return set(field.Access(t), *dest)
			}
		}
	}

	return func() (any, func(t *T) error) {
		var dest V

		return &dest, func(t *T) error {
			return set(field.Access(t), dest)
		}
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
