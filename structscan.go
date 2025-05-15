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

type Scanner[T any] interface {
	Scan() (any, func(*T) error)
}

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

type Mapper[T any] struct {
	Dest []any
	Set  func(*T) error
}

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

var ErrTooManyRows = errors.New("too many rows")

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

type Schema[T any] map[string]Field[T]

func (s Schema[T]) MustField(path string) Field[T] {
	f, err := s.Field(path)
	if err != nil {
		panic(err)
	}

	return f
}

func (s Schema[T]) Field(path string) (Field[T], error) {
	f, ok := s[path]
	if !ok {
		return Field[T]{}, fmt.Errorf("field not found: %s", path)
	}

	return f, nil
}

func (s Schema[T]) Scan() (any, func(*T) error) {
	field, err := s.Field("")
	if err != nil {
		var ignore any

		return []any{ignore}, func(t *T) error {
			return err
		}
	}

	return field.Scan()
}

func Describe[T any]() Schema[T] {
	s := Schema[T]{}

	fillSchema(s, nil, "", reflect.TypeFor[T]())

	return s
}

func fillSchema[T any](s Schema[T], indices []int, path string, t reflect.Type) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()

		indices = append(indices, -1)
	}

	s[path] = Field[T]{
		typ:     t,
		indices: indices,
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

type Field[T any] struct {
	typ     reflect.Type
	indices []int
}

func (f Field[T]) access(t *T) reflect.Value {
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

func (f Field[T]) MustConvert(converter Converter) Scanner[T] {
	s, err := f.Convert(converter)
	if err != nil {
		panic(err)
	}

	return s
}

func (f Field[T]) Convert(converter Converter) (Scanner[T], error) {
	srcType, convert, err := converter(f.typ)
	if err != nil {
		return nil, err
	}

	return convertedField[T]{
		field:   f,
		srcType: srcType,
		convert: convert,
	}, nil
}

func (f Field[T]) Scan() (any, func(*T) error) {
	src := reflect.New(f.typ)

	return src.Interface(), func(t *T) error {
		if !src.IsValid() {
			return nil
		}

		f.access(t).Set(src.Elem())

		return nil
	}
}

type convertedField[T any] struct {
	field   Field[T]
	srcType reflect.Type
	convert Convert
}

func (c convertedField[T]) Scan() (any, func(*T) error) {
	src := reflect.New(c.srcType)

	return src.Interface(), func(t *T) error {
		dst, err := c.convert(src.Elem())
		if err != nil {
			return err
		}

		if !dst.IsValid() {
			return nil
		}

		c.field.access(t).Set(dst)

		return nil
	}
}

var (
	stringType    = reflect.TypeFor[string]()
	byteSliceType = reflect.TypeFor[[]byte]()
	invalid       reflect.Value
)

type Converter func(dstType reflect.Type) (reflect.Type, Convert, error)

type Convert func(src reflect.Value) (reflect.Value, error)

func Nullable() Converter {
	return func(dstType reflect.Type) (reflect.Type, Convert, error) {
		return reflect.PointerTo(dstType), func(src reflect.Value) (reflect.Value, error) {
			if !src.IsValid() || src.IsNil() {
				return invalid, nil
			}

			return src.Elem(), nil
		}, nil
	}
}

func Default(value any) Converter {
	var (
		r   = reflect.ValueOf(value)
		typ = r.Type()
	)

	return func(dstType reflect.Type) (reflect.Type, Convert, error) {
		conv, err := autoConverter(dstType, typ)
		if err != nil {
			return nil, nil, fmt.Errorf("default converter: %w", err)
		}

		return reflect.PointerTo(typ), func(src reflect.Value) (reflect.Value, error) {
			if !src.IsValid() || src.IsNil() {
				return conv(r)
			}

			return conv(src.Elem())
		}, nil
	}
}

func UnmarshalJSON() Converter {
	return func(dstType reflect.Type) (reflect.Type, Convert, error) {
		return byteSliceType, func(src reflect.Value) (reflect.Value, error) {
			dst := reflect.New(dstType)

			err := json.Unmarshal(src.Bytes(), dst.Interface())
			if err != nil {
				return invalid, fmt.Errorf("unmarshal json converter: %w", err)
			}

			return dst.Elem(), err
		}, nil
	}
}

var textUnmarshalerType = reflect.TypeFor[encoding.TextUnmarshaler]()

func UnmarshalText() Converter {
	return convertInterface("unmarshal text", textUnmarshalerType, func(iface any, src []byte) error {
		return iface.(encoding.TextUnmarshaler).UnmarshalText(src)
	})
}

var binaryUnmarshalerType = reflect.TypeFor[encoding.BinaryUnmarshaler]()

func UnmarshalBinary() Converter {
	return convertInterface("unmarshal binary", binaryUnmarshalerType, func(iface any, src []byte) error {
		return iface.(encoding.BinaryUnmarshaler).UnmarshalBinary(src)
	})
}

func convertInterface(name string, ifaceType reflect.Type, fn func(any, []byte) error) Converter {
	return func(dstType reflect.Type) (reflect.Type, Convert, error) {
		deref := dstType.Kind() != reflect.Pointer

		if !dstType.Implements(ifaceType) {
			dstType = reflect.PointerTo(dstType)

			if !dstType.Implements(ifaceType) {
				return nil, nil, fmt.Errorf("%s converter: %s does not implement %s", name, dstType, ifaceType)
			}
		}

		return byteSliceType, func(src reflect.Value) (reflect.Value, error) {
			ptr := reflect.New(dstType.Elem())
			if err := fn(ptr.Interface(), src.Bytes()); err != nil {
				return invalid, fmt.Errorf("%s converter: %w", name, err)
			}

			if deref {
				return ptr.Elem(), nil
			}

			return ptr, nil
		}, nil
	}
}

func ParseTime(layout string) Converter {
	return StringFunc("parse time", func(src string) (time.Time, error) {
		return time.Parse(layout, src)
	})
}

func ParseTimeInLocation(layout string, location *time.Location) Converter {
	return StringFunc("parse time in location", func(src string) (time.Time, error) {
		return time.ParseInLocation(layout, src, location)
	})
}

func Atoi() Converter {
	return StringFunc("atoi", strconv.Atoi)
}

func ParseInt(base int, bitSize int) Converter {
	return StringFunc("parse int", func(src string) (int64, error) {
		return strconv.ParseInt(src, base, bitSize)
	})
}

func ParseUint(base int, bitSize int) Converter {
	return StringFunc("parse uint", func(src string) (uint64, error) {
		return strconv.ParseUint(src, base, bitSize)
	})
}

func ParseFloat(bitSize int) Converter {
	return StringFunc("parse float", func(src string) (float64, error) {
		return strconv.ParseFloat(src, bitSize)
	})
}

func ParseBool() Converter {
	return StringFunc("parse bool", strconv.ParseBool)
}

func ParseComplex(bitSize int) Converter {
	return StringFunc("parse complex", func(src string) (complex128, error) {
		return strconv.ParseComplex(src, bitSize)
	})
}

func StringFunc[D any](name string, fn func(src string) (D, error)) Converter {
	typ := reflect.TypeFor[D]()

	return func(dstType reflect.Type) (reflect.Type, Convert, error) {
		conv, err := autoConverter(dstType, typ)
		if err != nil {
			return nil, nil, fmt.Errorf("%s converter: %w", name, err)
		}

		return stringType, func(src reflect.Value) (reflect.Value, error) {
			v, err := fn(src.String())
			if err != nil {
				return invalid, fmt.Errorf("%s converter: %w", name, err)
			}

			return conv(reflect.ValueOf(v))
		}, nil
	}
}

func Trim(cutset string) Converter {
	return StringFunc("trim", func(str string) (string, error) {
		return strings.Trim(str, cutset), nil
	})
}

func TrimPrefix(prefix string) Converter {
	return StringFunc("trimprefix", func(str string) (string, error) {
		return strings.TrimPrefix(str, prefix), nil
	})
}

func TrimSuffix(suffix string) Converter {
	return StringFunc("trimsuffix", func(str string) (string, error) {
		return strings.TrimSuffix(str, suffix), nil
	})
}

func Contains(substr string) Converter {
	return StringFunc("contains", func(str string) (bool, error) {
		return strings.Contains(str, substr), nil
	})
}

func ContainsAny(chars string) Converter {
	return StringFunc("containsany", func(str string) (bool, error) {
		return strings.ContainsAny(str, chars), nil
	})
}

func HasPrefix(prefix string) Converter {
	return StringFunc("hasprefix", func(str string) (bool, error) {
		return strings.HasPrefix(str, prefix), nil
	})
}

func HasSuffix(suffix string) Converter {
	return StringFunc("hassuffix", func(str string) (bool, error) {
		return strings.HasSuffix(str, suffix), nil
	})
}

func EqualFold(substr string) Converter {
	return StringFunc("equalfold", func(str string) (bool, error) {
		return strings.EqualFold(str, substr), nil
	})
}

func Index(substr string) Converter {
	return StringFunc("index", func(str string) (int, error) {
		return strings.Index(str, substr), nil
	})
}

func Count(substr string) Converter {
	return StringFunc("count", func(str string) (int, error) {
		return strings.Count(str, substr), nil
	})
}

func ToLower() Converter {
	return StringFunc("tolower", func(str string) (string, error) {
		return strings.ToLower(str), nil
	})
}

func ToUpper() Converter {
	return StringFunc("toupper", func(str string) (string, error) {
		return strings.ToUpper(str), nil
	})
}

func Chain(converters ...Converter) Converter {
	switch len(converters) {
	case 0:
		return func(dstType reflect.Type) (reflect.Type, Convert, error) {
			return dstType, func(src reflect.Value) (reflect.Value, error) {
				return src, nil
			}, nil
		}
	case 1:
		return func(dstType reflect.Type) (reflect.Type, Convert, error) {
			return converters[0](dstType)
		}
	}

	return func(dstType reflect.Type) (reflect.Type, Convert, error) {
		var (
			convert = make([]Convert, len(converters))
			sType   = dstType
			err     error
		)

		for i, d := range slices.Backward(converters) {
			sType, convert[i], err = d(sType)
			if err != nil {
				return nil, nil, fmt.Errorf("chain converter: %w", err)
			}
		}

		return sType, func(src reflect.Value) (reflect.Value, error) {
			var err error

			for _, a := range convert {
				if !src.IsValid() {
					return invalid, nil
				}

				src, err = a(src)
				if err != nil {
					return invalid, fmt.Errorf("chain converter: %w", err)
				}
			}

			return src, nil
		}, nil
	}
}

func MustOneOf(values ...any) Converter {
	d, err := OneOf(values...)
	if err != nil {
		panic(err)
	}

	return d
}

func OneOf(values ...any) (Converter, error) {
	var valueType reflect.Type

	for i, v := range values {
		r := reflect.ValueOf(v)

		if i == 0 {
			valueType = r.Type()
		} else if r.Type() != valueType {
			return nil, fmt.Errorf("oneof converter: invalid value: %s", v)
		}
	}

	return func(dstType reflect.Type) (reflect.Type, Convert, error) {
		conv, err := autoConverter(dstType, valueType)
		if err != nil {
			return nil, nil, fmt.Errorf("oneof converter: %w", err)
		}

		return valueType, func(src reflect.Value) (reflect.Value, error) {
			if !slices.ContainsFunc(values, func(each any) bool { return reflect.DeepEqual(each, src.Interface()) }) {
				return invalid, fmt.Errorf("oneof converter: invalid value: %v", src)
			}

			return conv(src)
		}, nil
	}, nil
}

func MustEnum(pairs ...any) Converter {
	d, err := Enum(pairs...)
	if err != nil {
		panic(err)
	}

	return d
}

func Enum(pairs ...any) (Converter, error) {
	if len(pairs)%2 != 0 {
		return nil, errors.New("enum converter: invalid number of arguments, must be even")
	}

	var (
		keyType, valueType reflect.Type
		mapping            = map[any]reflect.Value{}
	)

	for pair := range slices.Chunk(pairs, 2) {
		k, v := reflect.ValueOf(pair[0]), reflect.ValueOf(pair[1])

		if !k.IsValid() || !v.IsValid() {
			return nil, fmt.Errorf("enum converter: invalid pair: %v", pair)
		}

		if keyType == nil {
			keyType = k.Type()

			if !keyType.Comparable() {
				return nil, fmt.Errorf("enum converter: key type is not comparable: %s", keyType)
			}

			valueType = v.Type()
		} else if k.Type() != keyType || v.Type() != valueType {
			return nil, fmt.Errorf("enum converter: invalid pair: %v", pair)
		}

		mapping[pair[0]] = v
	}

	return func(dstType reflect.Type) (reflect.Type, Convert, error) {
		conv, err := autoConverter(dstType, valueType)
		if err != nil {
			return nil, nil, fmt.Errorf("enum converter: %w", err)
		}

		return keyType, func(src reflect.Value) (reflect.Value, error) {
			value, ok := mapping[src.Interface()]
			if !ok {
				return invalid, fmt.Errorf("enum converter: invalid enum: %s", src)
			}

			return conv(value)
		}, nil
	}, nil
}

func Cut(sep string, converters ...Converter) Converter {
	converter := Chain(converters...)

	return func(dstType reflect.Type) (reflect.Type, Convert, error) {
		srcType, convert, err := converter(dstType.Elem())
		if err != nil {
			return nil, nil, fmt.Errorf("cut converter: %w", err)
		}

		assign, err := assigner(srcType, stringType)
		if err != nil {
			return nil, nil, fmt.Errorf("cut converter: %w", err)
		}

		switch dstType.Kind() {
		case reflect.Slice:
			return stringType, func(src reflect.Value) (reflect.Value, error) {
				str := src.String()
				if str == "" {
					return invalid, nil
				}

				var (
					before, after, _ = strings.Cut(str, sep)
					dst              = reflect.MakeSlice(dstType, 2, 2)
					val              reflect.Value
					err              error
				)

				val, err = convert(reflect.ValueOf(before))
				if err != nil {
					return invalid, fmt.Errorf("cut converter: %w", err)
				}

				if err = assign(dst.Index(0), val); err != nil {
					return invalid, err
				}

				val, err = convert(reflect.ValueOf(after))
				if err != nil {
					return invalid, fmt.Errorf("cut converter: %w", err)
				}

				if err = assign(dst.Index(1), val); err != nil {
					return invalid, err
				}

				return dst, nil
			}, nil
		case reflect.Array:
			if dstType.Len() < 2 {
				return nil, nil, fmt.Errorf("cut converter: invalid type: %s", dstType)
			}

			return stringType, func(src reflect.Value) (reflect.Value, error) {
				str := src.String()
				if str == "" {
					return invalid, nil
				}

				var (
					before, after, _ = strings.Cut(str, sep)
					dst              = reflect.New(dstType).Elem()
					val              reflect.Value
					err              error
				)

				val, err = convert(reflect.ValueOf(before))
				if err != nil {
					return invalid, fmt.Errorf("cut converter: %w", err)
				}

				if err = assign(dst.Index(0), val); err != nil {
					return invalid, err
				}

				val, err = convert(reflect.ValueOf(after))
				if err != nil {
					return invalid, fmt.Errorf("cut converter: %w", err)
				}

				if err = assign(dst.Index(1), val); err != nil {
					return invalid, err
				}

				return dst, nil
			}, nil
		default:
			return nil, nil, fmt.Errorf("split converter: invalid dst type: %s", dstType)
		}
	}
}

func Split(sep string, converters ...Converter) Converter {
	converter := Chain(converters...)

	return func(dstType reflect.Type) (reflect.Type, Convert, error) {
		srcType, convert, err := converter(dstType.Elem())
		if err != nil {
			return nil, nil, fmt.Errorf("split converter: %w", err)
		}

		assign, err := assigner(srcType, stringType)
		if err != nil {
			return nil, nil, fmt.Errorf("split converter: %w", err)
		}

		switch dstType.Kind() {
		case reflect.Slice:
			return stringType, func(src reflect.Value) (reflect.Value, error) {
				str := src.String()
				if str == "" {
					return invalid, nil
				}

				var (
					split = strings.Split(str, sep)
					dst   = reflect.MakeSlice(dstType, len(split), len(split))
					val   reflect.Value
					err   error
				)

				for i, each := range split {
					val, err = convert(reflect.ValueOf(each))
					if err != nil {
						return invalid, fmt.Errorf("split converter: %w", err)
					}

					if err = assign(dst.Index(i), val); err != nil {
						return invalid, fmt.Errorf("split converter: %w", err)
					}
				}

				return dst, nil
			}, nil
		case reflect.Array:
			return stringType, func(src reflect.Value) (reflect.Value, error) {
				str := src.String()
				if str == "" {
					return invalid, nil
				}

				var (
					split = strings.Split(str, sep)
					dst   = reflect.New(dstType).Elem()
					val   reflect.Value
					err   error
				)

				if len(split) > dstType.Len() {
					return invalid, fmt.Errorf("split converter: too many values for %s", dstType)
				}

				for i, each := range split {
					val, err = convert(reflect.ValueOf(each))
					if err != nil {
						return invalid, fmt.Errorf("split converter: %w", err)
					}

					if err = assign(dst.Index(i), val); err != nil {
						return invalid, fmt.Errorf("split converter: %w", err)
					}
				}

				return dst, nil
			}, nil
		default:
			return nil, nil, fmt.Errorf("split converter: invalid dst type: %s", dstType)
		}
	}
}

func assigner(dstType, srcType reflect.Type) (func(dst, src reflect.Value) error, error) {
	conv, err := autoConverter(dstType, srcType)
	if err != nil {
		return nil, err
	}

	return func(dst, src reflect.Value) error {
		v, err := conv(src)
		if err != nil {
			return err
		}

		if !v.IsValid() {
			return nil
		}

		dst.Set(v)

		return nil
	}, nil
}

func derefType(t reflect.Type) (reflect.Type, int) {
	var levels int

	for t.Kind() == reflect.Pointer {
		t = t.Elem()
		levels++
	}

	return t, levels
}

func derefValue(v reflect.Value, levels int) reflect.Value {
	for range levels {
		if !v.IsValid() || v.Kind() != reflect.Pointer {
			return invalid
		}

		v = v.Elem()
	}

	return v
}

func refValue(v reflect.Value, levels int) reflect.Value {
	for range levels {
		if !v.IsValid() {
			return invalid
		}

		ptr := reflect.New(v.Type())
		ptr.Elem().Set(v)
		v = ptr
	}

	return v
}

func autoConverter(dstType, srcType reflect.Type) (Convert, error) {
	if dstType.Kind() == reflect.Pointer {
		elem, levels := derefType(dstType)

		conv, err := autoConverter(elem, srcType)
		if err != nil {
			return nil, err
		}

		return func(src reflect.Value) (reflect.Value, error) {
			var err error

			src, err = conv(src)
			if err != nil {
				return invalid, err
			}

			return refValue(src, levels), nil
		}, nil
	}

	if srcType.Kind() == reflect.Pointer {
		srcType, levels := derefType(srcType)

		convert, err := autoConverter(dstType, srcType)
		if err != nil {
			return nil, err
		}

		return func(src reflect.Value) (reflect.Value, error) {
			src = derefValue(src, levels)
			if !src.IsValid() {
				return invalid, nil
			}

			return convert(src)
		}, nil
	}

	if srcType.AssignableTo(dstType) {
		return func(f reflect.Value) (reflect.Value, error) {
			return f, nil
		}, nil
	}

	if srcType.ConvertibleTo(dstType) {
		return func(f reflect.Value) (reflect.Value, error) {
			return f.Convert(dstType), nil
		}, nil
	}

	return nil, fmt.Errorf("value of type %s cannot be converted to type %s", srcType, dstType)
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
