package structscan_test

import (
	"database/sql"
	"encoding/json"
	"math/big"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/go-sqlt/structscan"
	_ "modernc.org/sqlite"
)

type MyString string

type MyInt64 int64

type MyFloat64 float64

type MyBool bool

type Data struct {
	Time                 time.Time
	Nested               *Data
	NullStringPointer    *sql.Null[string]
	Int32Pointer         *int32
	StringPointerPointer **string
	StringPointer        *string
	AnyMap               map[string]any
	BigIntPointer        *big.Int
	URLPointer           *url.URL
	TimePointer          *time.Time
	URL                  url.URL
	Array                [2]string
	String               string
	MyString             MyString
	BigInt               big.Int
	NullString           sql.Null[string]
	Strings              []string
	RawJSON              json.RawMessage
	StringPointers       []*string
	Bytes                []byte
	Complex64            complex64
	Float64              float64
	Uint64               uint64
	MyInt64              MyInt64
	Int16                int16
	Bool                 bool
	Duration             time.Duration
}

func TestOne(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}

	dest := structscan.NewSchema[Data]()

	type Case struct {
		Scanners []structscan.Scanner[Data]
		SQL      string
		Expect   Data
	}

	cases := []Case{
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().MustInto("String"),
			},
			SQL:    "SELECT 'hello'",
			Expect: Data{String: "hello"},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().String().MustInto("MyString"),
			},
			SQL:    "SELECT 'hello'",
			Expect: Data{MyString: "hello"},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().MustInto("NullString"),
			},
			SQL:    "SELECT 'hello'",
			Expect: Data{NullString: sql.Null[string]{Valid: true, V: "hello"}},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().MustInto("NullStringPointer"),
			},
			SQL:    "SELECT 'hello'",
			Expect: Data{NullStringPointer: &sql.Null[string]{Valid: true, V: "hello"}},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Nullable(),
			},
			SQL:    "SELECT NULL",
			Expect: Data{},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().String().MustInto("String"),
			},
			SQL:    "SELECT 'hello'",
			Expect: Data{String: "hello"},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Int().MustInto("Int16"),
			},
			SQL:    "SELECT 100",
			Expect: Data{Int16: 100},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Int().MustInto("Float64"),
			},
			SQL:    "SELECT 100",
			Expect: Data{Float64: 100},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Int().Add(50).Subtract(100).Multiply(2).MustInto("Int16"),
			},
			SQL:    "SELECT 100",
			Expect: Data{Int16: 100},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Int().MustInto("Int32Pointer"),
			},
			SQL:    "SELECT 100",
			Expect: Data{Int32Pointer: ptr[int32](100)},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Int().Divide(2).MustInto("Float64"),
			},
			SQL:    "SELECT 100",
			Expect: Data{Float64: 50},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Int().Format(10).MustInto("MyString"),
			},
			SQL:    "SELECT 100",
			Expect: Data{MyString: "100"},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Int().Uint().MustInto("Uint64"),
			},
			SQL:    "SELECT 100",
			Expect: Data{Uint64: 100},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Uint().MustInto("Uint64"),
			},
			SQL:    "SELECT 100",
			Expect: Data{Uint64: 100},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Int().MustInto("MyInt64"),
			},
			SQL:    "SELECT 100",
			Expect: Data{MyInt64: 100},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Int().Uint().MustInto("Uint64"),
			},
			SQL:    "SELECT 100",
			Expect: Data{Uint64: 100},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Float().MustInto("Nested.Float64"),
			},
			SQL:    "SELECT 1.23",
			Expect: Data{Nested: &Data{Float64: 1.23}},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Float().MustInto("Nested.Float64"),
			},
			SQL:    "SELECT 1.23",
			Expect: Data{Nested: &Data{Float64: 1.23}},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Float().Uint().MustInto("Uint64"),
			},
			SQL:    "SELECT 1",
			Expect: Data{Uint64: 1},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Bool().MustInto("Nested.Bool"),
			},
			SQL:    "SELECT 'true'",
			Expect: Data{Nested: &Data{Bool: true}},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Bytes().MustInto("Bytes"),
			},
			SQL:    "SELECT 'hello'",
			Expect: Data{Bytes: []byte("hello")},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Nullable().String().MustInto("String"),
			},
			SQL:    "SELECT 'hello'",
			Expect: Data{String: "hello"},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Nullable().String().MustInto("String"),
			},
			SQL:    "SELECT NULL",
			Expect: Data{String: ""},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().String().Split(",").MustInto("Strings"),
			},
			SQL:    "SELECT 'hello,world'",
			Expect: Data{Strings: []string{"hello", "world"}},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().String().Split(",").MustInto("Array"),
			},
			SQL:    "SELECT 'hello,world'",
			Expect: Data{Array: [2]string{"hello", "world"}},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().String().Split(",").MustInto("Nested.StringPointers"),
			},
			SQL:    "SELECT 'hello,world'",
			Expect: Data{Nested: &Data{StringPointers: []*string{ptr("hello"), ptr("world")}}},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().String().Int(10, 64).MustInto("Int16"),
			},
			SQL:    "SELECT '100'",
			Expect: Data{Int16: 100},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().String().Uint(10, 64).MustInto("Uint64"),
			},
			SQL:    "SELECT '100'",
			Expect: Data{Uint64: 100},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().String().Uint(10, 64).Int().MustInto("Int16"),
			},
			SQL:    "SELECT '100'",
			Expect: Data{Int16: 100},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().String().Float(64).MustInto("Float64"),
			},
			SQL:    "SELECT '100'",
			Expect: Data{Float64: 100},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().String().MustInto("Nested.String"),
			},
			SQL:    "SELECT '100'",
			Expect: Data{Nested: &Data{String: "100"}},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().String().Float(64).MustInto("Float64"),
			},
			SQL:    "SELECT '1.23'",
			Expect: Data{Float64: 1.23},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().String().Complex(64).MustInto("Complex64"),
			},
			SQL:    "SELECT '2+3i'",
			Expect: Data{Complex64: complex64(2 + 3i)},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().String().Time(time.DateOnly).MustInto("Time"),
			},
			SQL:    "SELECT '2200-01-07'",
			Expect: Data{Time: must(time.Parse(time.DateOnly, "2200-01-07"))},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().String().TimeInLocation(time.DateOnly, time.UTC).MustInto("Time"),
			},
			SQL:    "SELECT '2200-01-07'",
			Expect: Data{Time: must(time.ParseInLocation(time.DateOnly, "2200-01-07", time.UTC))},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().String().TimeInLocation(time.DateOnly, time.UTC).MustInto("TimePointer"),
			},
			SQL:    "SELECT '2200-01-07'",
			Expect: Data{TimePointer: ptr(must(time.ParseInLocation(time.DateOnly, "2200-01-07", time.UTC)))},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().String().TrimSpace().Replace("Euro", "$", 1).Trim("$").Float(64).Add(2).Round().Int().MustInto("Int16"),
			},
			SQL:    "SELECT '   1.23Euro'",
			Expect: Data{Int16: 3},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Float().Format('f', -1, 64).ReplaceAll(".", "").Int(10, 64).MustInto("Int16"),
			},
			SQL:    "SELECT 1.23",
			Expect: Data{Int16: 123},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Duration().MustInto("Duration"),
			},
			SQL:    "SELECT 10",
			Expect: Data{Duration: 10},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().String().Duration().MustInto("Duration"),
			},
			SQL:    "SELECT '10m'",
			Expect: Data{Duration: time.Minute * 10},
		},
	}

	for _, c := range cases {
		t.Run(c.SQL, func(t *testing.T) {
			t.Parallel()

			mapper, err := structscan.NewMapper(c.Scanners...)
			if err != nil {
				t.Fatal(c.SQL, err)
			}

			result, err := mapper.QueryOne(t.Context(), db, c.SQL)
			if err != nil {
				t.Fatal(c.SQL, err)
			}

			if !reflect.DeepEqual(c.Expect, result) {
				t.Fatalf("not equal: \n expected: %v \n   result: %v", c.Expect, result)
			}
		})
	}
}

func TestFirst(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}

	dest := structscan.NewSchema[Data]()

	type Case struct {
		Scanners []structscan.Scanner[Data]
		SQL      string
		Expect   Data
	}

	cases := []Case{
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().String().Enum(
					structscan.Enum{String: "one", Int: 1},
					structscan.Enum{String: "two", Int: 2},
					structscan.Enum{String: "three", Int: 3},
				).Float().MustInto("Float64"),
			},
			SQL:    "SELECT 'two'",
			Expect: Data{Float64: 2},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Int().Enum(
					structscan.Enum{String: "one", Int: 1},
					structscan.Enum{String: "two", Int: 2},
					structscan.Enum{String: "three", Int: 3},
				).MustInto("String"),
			},
			SQL:    "SELECT 2",
			Expect: Data{String: "two"},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Bytes().Binary().MustInto("URL"),
			},
			SQL:    "SELECT 'https://example.com/path?query=true'",
			Expect: Data{URL: *must(url.Parse("https://example.com/path?query=true"))},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Bytes().Binary().MustInto("URLPointer"),
			},
			SQL:    "SELECT 'https://example.com/path?query=true'",
			Expect: Data{URLPointer: must(url.Parse("https://example.com/path?query=true"))},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Binary().MustInto("URLPointer"),
			},
			SQL:    "SELECT 'https://example.com/path?query=true'",
			Expect: Data{URLPointer: must(url.Parse("https://example.com/path?query=true"))},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Bytes().Text().MustInto("BigInt"),
			},
			SQL:    "SELECT '100'",
			Expect: Data{BigInt: *big.NewInt(100)},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Bytes().Text().MustInto("BigIntPointer"),
			},
			SQL:    "SELECT '100'",
			Expect: Data{BigIntPointer: big.NewInt(100)},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Bytes().MustInto("RawJSON"),
			},
			SQL:    `SELECT '{"hello":"world"}'`,
			Expect: Data{RawJSON: json.RawMessage(`{"hello":"world"}`)},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Bytes().JSON().MustInto("RawJSON"),
			},
			SQL:    `SELECT '{"hello":"earth"}'`,
			Expect: Data{RawJSON: json.RawMessage(`{"hello":"earth"}`)},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().JSON().MustInto("RawJSON"),
			},
			SQL:    `SELECT '{"hello":"earth"}'`,
			Expect: Data{RawJSON: json.RawMessage(`{"hello":"earth"}`)},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Bytes().JSON().MustInto("AnyMap"),
			},
			SQL:    `SELECT '{"hello":"moon"}'`,
			Expect: Data{AnyMap: map[string]any{"hello": "moon"}},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().String().MustInto("String"),
			},
			SQL:    `SELECT '2300-01-07T10:30:00+00:00'`,
			Expect: Data{String: "2300-01-07T10:30:00+00:00"},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().String().Bool().MustInto("Bool"),
			},
			SQL:    `SELECT 'f'`,
			Expect: Data{Bool: false},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().String().MustInto("StringPointer"),
			},
			SQL:    `SELECT 'hello'`,
			Expect: Data{StringPointer: ptr("hello")},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Nullable().String().MustInto("String"),
			},
			SQL:    `SELECT 'hello'`,
			Expect: Data{String: "hello"},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().MustInto("StringPointer"),
			},
			SQL:    `SELECT 'hello'`,
			Expect: Data{StringPointer: ptr("hello")},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Nullable().MustInto("String"),
			},
			SQL:    `SELECT 'hello'`,
			Expect: Data{String: "hello"},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Nullable().MustInto("String"),
				dest.Scan().Nullable().MustInto("Int16"),
			},
			SQL:    `SELECT 'hello', 100`,
			Expect: Data{String: "hello", Int16: 100},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Nullable().MustInto("String"),
			},
			SQL:    "SELECT NULL",
			Expect: Data{},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Nullable().MustInto("StringPointer"),
			},
			SQL:    "SELECT NULL",
			Expect: Data{},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Nullable().MustInto("StringPointerPointer"),
			},
			SQL:    "SELECT NULL",
			Expect: Data{},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Nullable().MustInto("String"),
			},
			SQL:    "SELECT 'nullable'",
			Expect: Data{String: "nullable"},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Nullable().MustInto("StringPointer"),
			},
			SQL: "SELECT 'nullable'",
			Expect: Data{
				StringPointer: ptr("nullable"),
			},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().Nullable().MustInto("StringPointerPointer"),
			},
			SQL:    "SELECT 'nullable'",
			Expect: Data{StringPointerPointer: ptr(ptr("nullable"))},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().MustInto("String"),
			},
			SQL:    "SELECT 'nullable'",
			Expect: Data{String: "nullable"},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().MustInto("StringPointer"),
			},
			SQL: "SELECT 'nullable'",
			Expect: Data{
				StringPointer: ptr("nullable"),
			},
		},
		{
			Scanners: []structscan.Scanner[Data]{
				dest.Scan().MustInto("StringPointerPointer"),
			},
			SQL:    "SELECT 'nullable'",
			Expect: Data{StringPointerPointer: ptr(ptr("nullable"))},
		},
	}

	for _, c := range cases {
		t.Run(c.SQL, func(t *testing.T) {
			t.Parallel()

			mapper, err := structscan.NewMapper(c.Scanners...)
			if err != nil {
				t.Fatal(c.SQL, err)
			}

			result, err := mapper.QueryFirst(t.Context(), db, c.SQL)
			if err != nil {
				t.Fatal(c.SQL, err)
			}

			if !reflect.DeepEqual(c.Expect, result) {
				t.Fatalf("not equal: \n expected: %v \n   result: %v", c.Expect, result)
			}
		})
	}
}

func TestAll(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}

	dest := structscan.NewSchema[*Data]()

	type Case struct {
		Scanners []structscan.Scanner[*Data]
		SQL      string
		Expect   []*Data
	}

	cases := []Case{
		{
			Scanners: []structscan.Scanner[*Data]{
				dest.Scan().MustInto("String"),
				dest.Scan().MustInto("Int16"),
			},
			SQL: `SELECT * FROM (VALUES ('one', 1), ('two', 2));`,
			Expect: []*Data{
				{String: "one", Int16: 1},
				{String: "two", Int16: 2},
			},
		},
		{
			Scanners: []structscan.Scanner[*Data]{
				dest.Scan().String().Bool().MustInto("Bool"),
			},
			SQL: `SELECT * FROM (VALUES ('true'), ('false'));`,
			Expect: []*Data{
				{Bool: true},
				{Bool: false},
			},
		},
		{
			Scanners: []structscan.Scanner[*Data]{
				dest.Scan().MustInto("Float64"),
			},
			SQL: `SELECT * FROM (VALUES (3.14), (2.71));`,
			Expect: []*Data{
				{Float64: 3.14},
				{Float64: 2.71},
			},
		},
		{
			Scanners: []structscan.Scanner[*Data]{
				dest.Scan().String().Split(",").MustInto("Strings"),
			},
			SQL: `SELECT * FROM (VALUES ('foo,bar'), ('baz,qux'));`,
			Expect: []*Data{
				{Strings: []string{"foo", "bar"}},
				{Strings: []string{"baz", "qux"}},
			},
		},
		{
			Scanners: []structscan.Scanner[*Data]{
				dest.Scan().String().Float(64).MustInto("Float64"),
			},
			SQL: `SELECT * FROM (VALUES ('1.1'), ('2.2'));`,
			Expect: []*Data{
				{Float64: 1.1},
				{Float64: 2.2},
			},
		},
		{
			Scanners: []structscan.Scanner[*Data]{
				dest.Scan().String().Int(10, 64).MustInto("Int16"),
			},
			SQL: `SELECT * FROM (VALUES ('10'), ('20'));`,
			Expect: []*Data{
				{Int16: 10},
				{Int16: 20},
			},
		},
		{
			Scanners: []structscan.Scanner[*Data]{
				dest.Scan().String().Bool().MustInto("Bool"),
			},
			SQL: `SELECT * FROM (VALUES ('true'), ('false'));`,
			Expect: []*Data{
				{Bool: true},
				{Bool: false},
			},
		},
		{
			Scanners: []structscan.Scanner[*Data]{
				dest.Scan().String().Uint(10, 64).MustInto("Uint64"),
			},
			SQL: `SELECT * FROM (VALUES ('100'), ('200'));`,
			Expect: []*Data{
				{Uint64: 100},
				{Uint64: 200},
			},
		},
		{
			Scanners: []structscan.Scanner[*Data]{
				dest.Scan().String().MustInto("StringPointer"),
			},
			SQL: `SELECT * FROM (VALUES ('hi'), ('bye'));`,
			Expect: []*Data{
				{StringPointer: ptr("hi")},
				{StringPointer: ptr("bye")},
			},
		},
		{
			Scanners: []structscan.Scanner[*Data]{
				dest.Scan().Nullable().MustInto("String"),
			},
			SQL: `SELECT * FROM (VALUES ('a'), (NULL));`,
			Expect: []*Data{
				{String: "a"},
				{String: ""},
			},
		},
		{
			Scanners: []structscan.Scanner[*Data]{
				dest.Scan().String().Complex(128).MustInto("Complex64"),
			},
			SQL: `SELECT * FROM (VALUES ('1+2i'), ('3+4i'));`,
			Expect: []*Data{
				{Complex64: complex(1, 2)},
				{Complex64: complex(3, 4)},
			},
		},
		{
			Scanners: []structscan.Scanner[*Data]{
				dest.Scan().String().Time(time.DateOnly).MustInto("Time"),
			},
			SQL: `SELECT * FROM (VALUES ('2020-01-01'), ('2030-01-01'));`,
			Expect: []*Data{
				{Time: must(time.Parse(time.DateOnly, "2020-01-01"))},
				{Time: must(time.Parse(time.DateOnly, "2030-01-01"))},
			},
		},
		{
			Scanners: []structscan.Scanner[*Data]{
				dest.Scan().Bytes().Text().MustInto("BigInt"),
			},
			SQL: `SELECT * FROM (VALUES ('123456789012345'), ('987654321098765'));`,
			Expect: []*Data{
				{BigInt: *big.NewInt(123456789012345)},
				{BigInt: *big.NewInt(987654321098765)},
			},
		},
		{
			Scanners: []structscan.Scanner[*Data]{
				dest.Scan().Text().MustInto("BigInt"),
			},
			SQL: `SELECT * FROM (VALUES ('123456789012345'), ('987654321098765'));`,
			Expect: []*Data{
				{BigInt: *big.NewInt(123456789012345)},
				{BigInt: *big.NewInt(987654321098765)},
			},
		},
		{
			Scanners: []structscan.Scanner[*Data]{
				dest.Scan().Nullable().MustInto("StringPointer"),
			},
			SQL: `SELECT * FROM (VALUES ('a'), (NULL));`,
			Expect: []*Data{
				{StringPointer: ptr("a")},
				{},
			},
		},
		{
			Scanners: []structscan.Scanner[*Data]{
				dest.Scan().String().Split(",").MustInto("Array"),
			},
			SQL: `SELECT * FROM (VALUES ('a,b'), ('c,d'));`,
			Expect: []*Data{
				{Array: [2]string{"a", "b"}},
				{Array: [2]string{"c", "d"}},
			},
		},
		{
			Scanners: []structscan.Scanner[*Data]{
				dest.Scan().Bytes().MustInto("Bytes"),
			},
			SQL: `SELECT * FROM (VALUES ('abc'), ('def'));`,
			Expect: []*Data{
				{Bytes: []byte("abc")},
				{Bytes: []byte("def")},
			},
		},
		{
			Scanners: []structscan.Scanner[*Data]{
				dest.Scan().Bytes().JSON().MustInto("AnyMap"),
			},
			SQL: `SELECT * FROM (VALUES ('{"a":1}'), ('{"b":2}'));`,
			Expect: []*Data{
				{AnyMap: map[string]any{"a": float64(1)}},
				{AnyMap: map[string]any{"b": float64(2)}},
			},
		},
		{
			Scanners: []structscan.Scanner[*Data]{
				dest.Scan().MustInto("Nested.String"),
			},
			SQL: `SELECT * FROM (VALUES ('nested1'), ('nested2'));`,
			Expect: []*Data{
				{Nested: &Data{String: "nested1"}},
				{Nested: &Data{String: "nested2"}},
			},
		},
		{
			Scanners: []structscan.Scanner[*Data]{
				dest.Scan().MustInto("Nested.Int16"),
			},
			SQL: `SELECT * FROM (VALUES (100), (200));`,
			Expect: []*Data{
				{Nested: &Data{Int16: 100}},
				{Nested: &Data{Int16: 200}},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.SQL, func(t *testing.T) {
			t.Parallel()

			mapper, err := structscan.NewMapper(c.Scanners...)
			if err != nil {
				t.Fatal(c.SQL, err)
			}

			results, err := mapper.QueryAll(t.Context(), db, c.SQL)
			if err != nil {
				t.Fatal(c.SQL, err)
			}

			if !reflect.DeepEqual(c.Expect, results) {
				t.Fatalf("not equal: \n expected: [%v %v] \n   result: [%v %v]",
					*c.Expect[0], *c.Expect[1], *results[0], *results[1],
				)
			}
		})
	}
}

func ptr[T any](t T) *T {
	return &t
}

func must[T any](t T, err error) T {
	if err != nil {
		panic(err)
	}

	return t
}
