package structscan_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/go-sqlt/structscan"
	_ "modernc.org/sqlite"
)

func ptr[T any](v T) *T {
	return &v
}

// func bigFromString(s string) *big.Int {
// 	i := new(big.Int)
// 	i.SetString(s, 10)
// 	return i
// }

// func mustURL(raw string) *url.URL {
// 	u, err := url.Parse(raw)
// 	if err != nil {
// 		panic(err)
// 	}
// 	return u
// }

// func mustParse(date string) time.Time {
// 	t, err := time.Parse(time.DateOnly, date)
// 	if err != nil {
// 		panic(err)
// 	}
// 	return t
// }

func TestString(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	type T struct {
		Direct            string
		Nullable          *string
		NullableNull      string
		Value             *string
		ValueNullable     string
		ValueNullableNull *string
	}

	schema := structscan.New[T]()

	mapper := structscan.Map(
		schema["Direct"],
		schema["Nullable"].Nullable(),
		schema["NullableNull"].Nullable(),
		schema["Value"].MustString(),
		schema["ValueNullable"].MustString().Nullable(),
		schema["ValueNullableNull"].MustString().Nullable(),
	)

	expect := T{
		Direct:            "hello",
		Nullable:          ptr("hello"),
		NullableNull:      "",
		Value:             ptr("hello"),
		ValueNullable:     "hello",
		ValueNullableNull: nil,
	}

	rows, err := db.Query("SELECT 'hello', 'hello', NULL, 'hello', 'hello', NULL")
	if err != nil {
		t.Fatal(err)
	}

	result, err := mapper.One(rows)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(result, expect) {
		t.Fatalf("test error: \nresult: %v \nexpect: %v", result, expect)
	}
}

func TestInt(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	type T struct {
		Direct            int64
		Nullable          *int32
		NullableNull      int16
		Value             *int8
		ValueNullable     int
		ValueNullableNull *int64
	}

	schema := structscan.New[T]()

	mapper := structscan.Map(
		schema["Direct"],
		schema["Nullable"].Nullable(),
		schema["NullableNull"].Nullable(),
		schema["Value"].MustInt(),
		schema["ValueNullable"].MustInt().Nullable(),
		schema["ValueNullableNull"].MustInt().Nullable(),
	)

	expect := T{
		Direct:            1,
		Nullable:          ptr[int32](2),
		NullableNull:      0,
		Value:             ptr[int8](3),
		ValueNullable:     4,
		ValueNullableNull: nil,
	}

	rows, err := db.Query("SELECT 1, 2, NULL, 3, 4, NULL")
	if err != nil {
		t.Fatal(err)
	}

	result, err := mapper.One(rows)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(result, expect) {
		t.Fatalf("test error: \nresult: %v \nexpect: %v", result, expect)
	}
}

func TestFloat(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	type T struct {
		Direct            float64
		Nullable          *float64
		NullableNull      float32
		Value             *float32
		ValueNullable     float64
		ValueNullableNull *float64
	}

	schema := structscan.New[T]()

	mapper := structscan.Map(
		schema["Direct"],
		schema["Nullable"].Nullable(),
		schema["NullableNull"].Nullable(),
		schema["Value"].MustFloat(),
		schema["ValueNullable"].MustFloat().Nullable(),
		schema["ValueNullableNull"].MustFloat().Nullable(),
	)

	expect := T{
		Direct:            1.23,
		Nullable:          ptr(2.34),
		NullableNull:      0,
		Value:             ptr[float32](3.45),
		ValueNullable:     4.56,
		ValueNullableNull: nil,
	}

	row := db.QueryRow("SELECT 1.23, 2.34, NULL, 3.45, 4.56, NULL")

	result, err := mapper.Row(row)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(result, expect) {
		t.Fatalf("test error: \nresult: %v \nexpect: %v", result, expect)
	}
}

func TestBool(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	type T struct {
		Direct            bool
		Nullable          *bool
		NullableNull      bool
		Value             *bool
		ValueNullable     bool
		ValueNullableNull *bool
	}

	schema := structscan.New[T]()

	mapper := structscan.Map(
		schema["Direct"],
		schema["Nullable"].Nullable(),
		schema["NullableNull"].Nullable(),
		schema["Value"].MustBool(),
		schema["ValueNullable"].MustBool().Nullable(),
		schema["ValueNullableNull"].MustBool().Nullable(),
	)

	expect := T{
		Direct:            true,
		Nullable:          ptr(false),
		NullableNull:      false,
		Value:             ptr(false),
		ValueNullable:     true,
		ValueNullableNull: nil,
	}

	row := db.QueryRow("SELECT true, false, NULL, false, true, NULL")

	result, err := mapper.Row(row)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(result, expect) {
		t.Fatalf("test error: \nresult: %v \nexpect: %v", result, expect)
	}
}

func TestTime(t *testing.T) {
	type MyTime time.Time

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	type T struct {
		Direct            time.Time
		Nullable          *time.Time
		NullableNull      MyTime
		Value             *MyTime
		ValueNullable     time.Time
		ValueNullableNull *time.Time
	}

	schema := structscan.New[T]()

	mapper := structscan.Map(
		schema["Direct"],
		schema["Nullable"].Nullable(),
		schema["NullableNull"].Nullable(),
		schema["Value"].MustTime(),
		schema["ValueNullable"].MustTime().Nullable(),
		schema["ValueNullableNull"].MustTime().Nullable(),
	)

	time.Local = time.UTC
	now := time.Now().UTC()
	fmt.Println(now)

	expect := T{
		Direct:            now,
		Nullable:          ptr(now),
		NullableNull:      MyTime{},
		Value:             ptr(MyTime(now)),
		ValueNullable:     now,
		ValueNullableNull: nil,
	}

	_, err = db.Exec("CREATE TABLE my_time ( value DATE )")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("INSERT INTO my_time (value) VALUES (?)", now)
	if err != nil {
		t.Fatal(err)
	}

	row := db.QueryRow("SELECT value, value, NULL, value, value, NULL FROM my_time")

	result, err := mapper.Row(row)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(result, expect) {
		t.Fatalf("test error: \nresult: %v \nexpect: %v", result, expect)
	}
}

func TestBytes(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	type T struct {
		Direct            []byte
		Nullable          *[]byte
		NullableNull      json.RawMessage
		Value             *json.RawMessage
		ValueNullable     []byte
		ValueNullableNull *[]byte
	}

	schema := structscan.New[T]()

	mapper := structscan.Map(
		schema["Direct"],
		schema["Nullable"].Nullable(),
		schema["NullableNull"].Nullable(),
		schema["Value"].MustBytes(),
		schema["ValueNullable"].MustBytes().Nullable(),
		schema["ValueNullableNull"].MustBytes().Nullable(),
	)

	expect := T{
		Direct:            []byte(`"hello"`),
		Nullable:          ptr([]byte(`"hello"`)),
		NullableNull:      nil,
		Value:             ptr(json.RawMessage(`"hello"`)),
		ValueNullable:     []byte(`"hello"`),
		ValueNullableNull: nil,
	}

	rows, err := db.Query(`SELECT '"hello"', '"hello"', NULL, '"hello"', '"hello"', NULL`)

	result, err := mapper.All(rows)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(result[0], expect) {
		t.Fatalf("test error: \nresult: %v \nexpect: %v", result, expect)
	}
}

func TestSplit(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	type T struct {
		Direct            []string
		Nullable          *[]string
		NullableNull      [2]string
		Value             *[2]*string
		ValueNullable     []*string
		ValueNullableNull *[]string
	}

	schema := structscan.New[T]()

	mapper := structscan.Map(
		schema["Direct"].MustSplit(","),
		schema["Nullable"].Nullable().MustSplit(","),
		schema["NullableNull"].MustSplit(",").Nullable(),
		schema["Value"].MustSplit(","),
		schema["ValueNullable"].Nullable().MustSplit(","),
		schema["ValueNullableNull"].MustSplit(",").Nullable(),
	)

	expect := T{
		Direct:            []string{"hello", "world"},
		Nullable:          ptr([]string{"hello", "world"}),
		NullableNull:      [2]string{},
		Value:             ptr([2]*string{ptr("hello"), ptr("world")}),
		ValueNullable:     []*string{ptr("hello"), ptr("world")},
		ValueNullableNull: nil,
	}

	rows, err := db.Query(`SELECT 'hello,world', 'hello,world', NULL, 'hello,world', 'hello,world', NULL`)

	result, err := mapper.All(rows)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(result[0], expect) {
		t.Fatalf("test error: \nresult: %v \nexpect: %v", result, expect)
	}
}

func TestParseInt(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	type T struct {
		Direct            int64
		Nullable          *int32
		NullableNull      int16
		Value             *int8
		ValueNullable     int
		ValueNullableNull *int64
	}

	schema := structscan.New[T]()

	mapper := structscan.Map(
		schema["Direct"].MustParseInt(10, 64),
		schema["Nullable"].Nullable().MustParseInt(10, 32),
		schema["NullableNull"].MustParseInt(10, 16).Nullable(),
		schema["Value"].MustParseInt(10, 8),
		schema["ValueNullable"].MustParseInt(10, 64).Nullable(),
		schema["ValueNullableNull"].Nullable().MustParseInt(10, 64),
	)

	expect := T{
		Direct:            1,
		Nullable:          ptr[int32](2),
		NullableNull:      0,
		Value:             ptr[int8](3),
		ValueNullable:     4,
		ValueNullableNull: nil,
	}

	rows, err := db.Query("SELECT '1', '2', NULL, '3', '4', NULL")
	if err != nil {
		t.Fatal(err)
	}

	result, err := mapper.One(rows)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(result, expect) {
		t.Fatalf("test error: \nresult: %v \nexpect: %v", result, expect)
	}
}

func TestParseUint(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	type T struct {
		Direct            uint64
		Nullable          *uint32
		NullableNull      uint16
		Value             *uint8
		ValueNullable     uint
		ValueNullableNull *uint64
	}

	schema := structscan.New[T]()

	mapper := structscan.Map(
		schema["Direct"].MustParseUint(10, 64),
		schema["Nullable"].Nullable().MustParseUint(10, 32),
		schema["NullableNull"].MustParseUint(10, 16).Nullable(),
		schema["Value"].MustParseUint(10, 8),
		schema["ValueNullable"].MustParseUint(10, 64).Nullable(),
		schema["ValueNullableNull"].Nullable().MustParseUint(10, 64),
	)

	expect := T{
		Direct:            1,
		Nullable:          ptr[uint32](2),
		NullableNull:      0,
		Value:             ptr[uint8](3),
		ValueNullable:     4,
		ValueNullableNull: nil,
	}

	rows, err := db.Query("SELECT '1', '2', NULL, '3', '4', NULL")
	if err != nil {
		t.Fatal(err)
	}

	result, err := mapper.One(rows)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(result, expect) {
		t.Fatalf("test error: \nresult: %v \nexpect: %v", result, expect)
	}
}

func TestParseFloat(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	type T struct {
		Direct            float64
		Nullable          *float64
		NullableNull      float32
		Value             *float32
		ValueNullable     float64
		ValueNullableNull *float64
	}

	schema := structscan.New[T]()

	mapper := structscan.Map(
		schema["Direct"].MustParseFloat(64),
		schema["Nullable"].Nullable().MustParseFloat(64),
		schema["NullableNull"].Nullable().MustParseFloat(64),
		schema["Value"].MustParseFloat(64),
		schema["ValueNullable"].MustParseFloat(64).Nullable(),
		schema["ValueNullableNull"].MustParseFloat(64).Nullable(),
	)

	expect := T{
		Direct:            1.23,
		Nullable:          ptr(2.34),
		NullableNull:      0,
		Value:             ptr[float32](3.45),
		ValueNullable:     4.56,
		ValueNullableNull: nil,
	}

	row := db.QueryRow("SELECT '1.23', '2.34', NULL, '3.45', '4.56', NULL")

	result, err := mapper.Row(row)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(result, expect) {
		t.Fatalf("test error: \nresult: %v \nexpect: %v", result, expect)
	}
}

func TestParseComplex(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	type T struct {
		Direct            complex128
		Nullable          *complex128
		NullableNull      complex64
		Value             *complex64
		ValueNullable     complex128
		ValueNullableNull *complex128
	}

	schema := structscan.New[T]()

	mapper := structscan.Map(
		schema["Direct"].MustParseComplex(128),
		schema["Nullable"].Nullable().MustParseComplex(128),
		schema["NullableNull"].Nullable().MustParseComplex(64),
		schema["Value"].MustParseComplex(64),
		schema["ValueNullable"].MustParseComplex(128).Nullable(),
		schema["ValueNullableNull"].MustParseComplex(128).Nullable(),
	)

	expect := T{
		Direct:            complex(10, 3),
		Nullable:          ptr(complex(10, 3)),
		NullableNull:      complex(0, 0),
		Value:             ptr[complex64](complex(10, 3)),
		ValueNullable:     complex(10, 3),
		ValueNullableNull: nil,
	}

	row := db.QueryRow("SELECT '10+3i', '10+3i', NULL, '10+3i', '10+3i', NULL")

	result, err := mapper.Row(row)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(result, expect) {
		t.Fatalf("test error: \nresult: %v \nexpect: %v", result, expect)
	}
}

func TestParseBool(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	type T struct {
		Direct            bool
		Nullable          *bool
		NullableNull      bool
		Value             *bool
		ValueNullable     bool
		ValueNullableNull *bool
	}

	schema := structscan.New[T]()

	mapper := structscan.Map(
		schema["Direct"].MustParseBool(),
		schema["Nullable"].Nullable().MustParseBool(),
		schema["NullableNull"].Nullable().MustParseBool(),
		schema["Value"].MustParseBool(),
		schema["ValueNullable"].MustParseBool().Nullable(),
		schema["ValueNullableNull"].MustParseBool().Nullable(),
	)

	expect := T{
		Direct:            true,
		Nullable:          ptr(false),
		NullableNull:      false,
		Value:             ptr(false),
		ValueNullable:     true,
		ValueNullableNull: nil,
	}

	row := db.QueryRow("SELECT 'true', '0', NULL, 'f', 't', NULL")

	result, err := mapper.Row(row)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(result, expect) {
		t.Fatalf("test error: \nresult: %v \nexpect: %v", result, expect)
	}
}

func TestParseTime(t *testing.T) {
	type MyTime time.Time

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	type T struct {
		Direct            time.Time
		Nullable          *time.Time
		NullableNull      MyTime
		Value             *MyTime
		ValueNullable     time.Time
		ValueNullableNull *time.Time
	}

	schema := structscan.New[T]()

	mapper := structscan.Map(
		schema["Direct"].MustParseTime(time.DateOnly),
		schema["Nullable"].Nullable().MustParseTime(time.DateOnly),
		schema["NullableNull"].Nullable().MustParseTime(time.DateOnly),
		schema["Value"].MustParseTime(time.DateOnly),
		schema["ValueNullable"].MustParseTime(time.DateOnly).Nullable(),
		schema["ValueNullableNull"].MustParseTime(time.DateOnly).Nullable(),
	)

	date, err := time.Parse(time.DateOnly, "2020-12-31")
	if err != nil {
		t.Fatal(err)
	}

	expect := T{
		Direct:            date,
		Nullable:          ptr(date),
		NullableNull:      MyTime{},
		Value:             ptr(MyTime(date)),
		ValueNullable:     date,
		ValueNullableNull: nil,
	}

	row := db.QueryRow("SELECT '2020-12-31', '2020-12-31', NULL, '2020-12-31', '2020-12-31', NULL")

	result, err := mapper.Row(row)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(result, expect) {
		t.Fatalf("test error: \nresult: %v \nexpect: %v", result, expect)
	}
}

func TestParseTimeInLocation(t *testing.T) {
	type MyTime time.Time

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	type T struct {
		Direct            time.Time
		Nullable          *time.Time
		NullableNull      MyTime
		Value             *MyTime
		ValueNullable     time.Time
		ValueNullableNull *time.Time
	}

	schema := structscan.New[T]()

	mapper := structscan.Map(
		schema["Direct"].MustParseTimeInLocation(time.DateOnly, time.UTC),
		schema["Nullable"].Nullable().MustParseTimeInLocation(time.DateOnly, time.UTC),
		schema["NullableNull"].Nullable().MustParseTimeInLocation(time.DateOnly, time.UTC),
		schema["Value"].MustParseTimeInLocation(time.DateOnly, time.UTC),
		schema["ValueNullable"].MustParseTimeInLocation(time.DateOnly, time.UTC).Nullable(),
		schema["ValueNullableNull"].MustParseTimeInLocation(time.DateOnly, time.UTC).Nullable(),
	)

	date, err := time.ParseInLocation(time.DateOnly, "2020-12-31", time.UTC)
	if err != nil {
		t.Fatal(err)
	}

	expect := T{
		Direct:            date,
		Nullable:          ptr(date),
		NullableNull:      MyTime{},
		Value:             ptr(MyTime(date)),
		ValueNullable:     date,
		ValueNullableNull: nil,
	}

	row := db.QueryRow("SELECT '2020-12-31', '2020-12-31', NULL, '2020-12-31', '2020-12-31', NULL")

	result, err := mapper.Row(row)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(result, expect) {
		t.Fatalf("test error: \nresult: %v \nexpect: %v", result, expect)
	}
}

func TestStringEnum(t *testing.T) {
	type Status string

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	type T struct {
		Direct            Status
		Nullable          *Status
		NullableNull      Status
		Value             *Status
		ValueNullable     Status
		ValueNullableNull *Status
	}

	schema := structscan.New[T]()

	mapper := structscan.Map(
		schema["Direct"].MustStringEnum(
			structscan.Enum{String: "Inactive", Int: 0},
			structscan.Enum{String: "Active", Int: 1},
		),
		schema["Nullable"].Nullable().MustStringEnum(
			structscan.Enum{String: "Inactive", Int: 0},
			structscan.Enum{String: "Active", Int: 1},
		),
		schema["NullableNull"].Nullable().MustStringEnum(
			structscan.Enum{String: "Inactive", Int: 0},
			structscan.Enum{String: "Active", Int: 1},
		),
		schema["Value"].MustStringEnum(
			structscan.Enum{String: "Inactive", Int: 0},
			structscan.Enum{String: "Active", Int: 1},
		),
		schema["ValueNullable"].MustStringEnum(
			structscan.Enum{String: "Inactive", Int: 0},
			structscan.Enum{String: "Active", Int: 1},
		).Nullable(),
		schema["ValueNullableNull"].MustStringEnum(
			structscan.Enum{String: "Inactive", Int: 0},
			structscan.Enum{String: "Active", Int: 1},
		).Nullable(),
	)

	expect := T{
		Direct:            "Active",
		Nullable:          ptr[Status]("Inactive"),
		NullableNull:      Status(""),
		Value:             ptr(Status("Active")),
		ValueNullable:     "Inactive",
		ValueNullableNull: nil,
	}

	row := db.QueryRow("SELECT 1, 0, NULL, 1, 0, NULL")

	result, err := mapper.Row(row)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(result, expect) {
		t.Fatalf("test error: \nresult: %v \nexpect: %v", result, expect)
	}
}

func TestIntEnum(t *testing.T) {
	type Status int

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	type T struct {
		Direct            Status
		Nullable          *Status
		NullableNull      Status
		Value             *Status
		ValueNullable     Status
		ValueNullableNull *Status
	}

	schema := structscan.New[T]()

	mapper := structscan.Map(
		schema["Direct"].MustIntEnum(
			structscan.Enum{String: "Inactive", Int: 0},
			structscan.Enum{String: "Active", Int: 1},
		),
		schema["Nullable"].Nullable().MustIntEnum(
			structscan.Enum{String: "Inactive", Int: 0},
			structscan.Enum{String: "Active", Int: 1},
		),
		schema["NullableNull"].Nullable().MustIntEnum(
			structscan.Enum{String: "Inactive", Int: 0},
			structscan.Enum{String: "Active", Int: 1},
		),
		schema["Value"].MustIntEnum(
			structscan.Enum{String: "Inactive", Int: 0},
			structscan.Enum{String: "Active", Int: 1},
		),
		schema["ValueNullable"].MustIntEnum(
			structscan.Enum{String: "Inactive", Int: 0},
			structscan.Enum{String: "Active", Int: 1},
		).Nullable(),
		schema["ValueNullableNull"].MustIntEnum(
			structscan.Enum{String: "Inactive", Int: 0},
			structscan.Enum{String: "Active", Int: 1},
		).Nullable(),
	)

	expect := T{
		Direct:            1,
		Nullable:          ptr[Status](0),
		NullableNull:      Status(0),
		Value:             ptr(Status(1)),
		ValueNullable:     0,
		ValueNullableNull: nil,
	}

	row := db.QueryRow("SELECT 'Active', 'Inactive', NULL, 'Active', 'Inactive', NULL")

	result, err := mapper.Row(row)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(result, expect) {
		t.Fatalf("test error: \nresult: %v \nexpect: %v", result, expect)
	}
}

func TestUnmarshalJSON(t *testing.T) {
	type T map[string]any

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	schema := structscan.New[T]()

	mapper := structscan.Map(
		schema[""].UnmarshalJSON(),
	)

	expect := T{
		"hello": "world",
	}

	row := db.QueryRow(`SELECT '{"hello": "world"}'`)

	result, err := mapper.Row(row)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(result, expect) {
		t.Fatalf("test error: \nresult: %v \nexpect: %v", result, expect)
	}
}

func TestUnmarshalBinary(t *testing.T) {
	type T struct {
		URL *url.URL
	}

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	schema := structscan.New[T]()

	mapper := structscan.Map(
		schema["URL"].MustUnmarshalBinary(),
	)

	u, err := url.Parse("https://localhost:1234/path?query=true")
	if err != nil {
		t.Fatal(err)
	}

	expect := T{
		URL: u,
	}

	rows, err := db.Query(`SELECT 'https://localhost:1234/path?query=true'`)
	if err != nil {
		t.Fatal(err)
	}

	result, err := mapper.One(rows)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(result, expect) {
		t.Fatalf("test error: \nresult: %v \nexpect: %v", result, expect)
	}
}

func TestUnmarshalText(t *testing.T) {
	type T struct {
		Big *big.Int
	}

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	schema := structscan.New[T]()

	mapper := structscan.Map(
		schema["Big"].MustUnmarshalText(),
	)

	expect := T{
		Big: big.NewInt(10),
	}

	rows, err := db.Query(`SELECT '10'`)
	if err != nil {
		t.Fatal(err)
	}

	result, err := mapper.One(rows)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(result, expect) {
		t.Fatalf("test error: \nresult: %v \nexpect: %v", result, expect)
	}
}
