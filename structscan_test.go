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

func ptr[T any](v T) *T {
	return &v
}

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
		Default           string
	}

	schema := structscan.New[T]()

	mapper := structscan.Map(
		schema.MustField("Direct"),
		schema.MustNullable("Nullable"),
		schema.MustNullable("NullableNull"),
		schema.MustString("Value"),
		schema.MustString("ValueNullable").Nullable(),
		schema.MustString("ValueNullableNull").Nullable(),
		schema.MustDefaultString("Default", "default"),
	)

	expect := T{
		Direct:            "hello",
		Nullable:          ptr("hello"),
		NullableNull:      "",
		Value:             ptr("hello"),
		ValueNullable:     "hello",
		ValueNullableNull: nil,
		Default:           "default",
	}

	rows, err := db.Query("SELECT 'hello', 'hello', NULL, 'hello', 'hello', NULL, NULL")
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
		Default           int64
	}

	schema := structscan.New[T]()

	mapper := structscan.Map(
		schema.MustField("Direct"),
		schema.MustNullable("Nullable"),
		schema.MustNullable("NullableNull"),
		schema.MustInt("Value"),
		schema.MustInt("ValueNullable").Nullable(),
		schema.MustInt("ValueNullableNull").Nullable(),
		schema.MustDefaultInt("Default", 10),
	)

	expect := T{
		Direct:            1,
		Nullable:          ptr[int32](2),
		NullableNull:      0,
		Value:             ptr[int8](3),
		ValueNullable:     4,
		ValueNullableNull: nil,
		Default:           10,
	}

	rows, err := db.Query("SELECT 1, 2, NULL, 3, 4, NULL, NULL")
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

func TestUint(t *testing.T) {
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
		Default           uint64
	}

	schema := structscan.New[T]()

	mapper := structscan.Map(
		schema.MustField("Direct"),
		schema.MustNullable("Nullable"),
		schema.MustNullable("NullableNull"),
		schema.MustUint("Value"),
		schema.MustUint("ValueNullable").Nullable(),
		schema.MustUint("ValueNullableNull").Nullable(),
		schema.MustDefaultUint("Default", 10),
	)

	expect := T{
		Direct:            uint64(1),
		Nullable:          ptr[uint32](2),
		NullableNull:      0,
		Value:             ptr[uint8](3),
		ValueNullable:     uint(4),
		ValueNullableNull: nil,
		Default:           10,
	}

	rows, err := db.Query("SELECT 1, 2, NULL, 3, 4, NULL, NULL")
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
		Default           float64
	}

	schema := structscan.New[T]()

	mapper := structscan.Map(
		schema.MustField("Direct"),
		schema.MustNullable("Nullable"),
		schema.MustNullable("NullableNull"),
		schema.MustFloat("Value"),
		schema.MustFloat("ValueNullable").Nullable(),
		schema.MustFloat("ValueNullableNull").Nullable(),
		schema.MustDefaultFloat("Default", 1.23),
	)

	expect := T{
		Direct:            1.23,
		Nullable:          ptr(2.34),
		NullableNull:      0,
		Value:             ptr[float32](3.45),
		ValueNullable:     4.56,
		ValueNullableNull: nil,
		Default:           1.23,
	}

	row := db.QueryRow("SELECT 1.23, 2.34, NULL, 3.45, 4.56, NULL, NULL")

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
		Default           bool
	}

	schema := structscan.New[T]()

	mapper := structscan.Map(
		schema.MustField("Direct"),
		schema.MustNullable("Nullable"),
		schema.MustNullable("NullableNull"),
		schema.MustBool("Value"),
		schema.MustBool("ValueNullable").Nullable(),
		schema.MustBool("ValueNullableNull").Nullable(),
		schema.MustDefaultBool("Default", true),
	)

	expect := T{
		Direct:            true,
		Nullable:          ptr(false),
		NullableNull:      false,
		Value:             ptr(false),
		ValueNullable:     true,
		ValueNullableNull: nil,
		Default:           true,
	}

	row := db.QueryRow("SELECT true, false, NULL, false, true, NULL, NULL")

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
		Default           time.Time
	}

	schema := structscan.New[T]()

	time.Local = time.UTC
	now := time.Now().UTC()

	mapper := structscan.Map(
		schema.MustField("Direct"),
		schema.MustNullable("Nullable"),
		schema.MustNullable("NullableNull"),
		schema.MustTime("Value"),
		schema.MustTime("ValueNullable").Nullable(),
		schema.MustTime("ValueNullableNull").Nullable(),
		schema.MustDefaultTime("Default", now),
	)

	expect := T{
		Direct:            now,
		Nullable:          ptr(now),
		NullableNull:      MyTime{},
		Value:             ptr(MyTime(now)),
		ValueNullable:     now,
		ValueNullableNull: nil,
		Default:           now,
	}

	_, err = db.Exec("CREATE TABLE my_time ( value DATE )")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("INSERT INTO my_time (value) VALUES (?)", now)
	if err != nil {
		t.Fatal(err)
	}

	row := db.QueryRow("SELECT value, value, NULL, value, value, NULL, NULL FROM my_time")

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
		schema.MustField("Direct"),
		schema.MustNullable("Nullable"),
		schema.MustNullable("NullableNull"),
		schema.MustBytes("Value"),
		schema.MustBytes("ValueNullable").Nullable(),
		schema.MustBytes("ValueNullableNull").Nullable(),
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
		schema.MustSplit("Direct", ","),
		schema.MustSplit("Nullable", ",").Nullable(),
		schema.MustSplit("NullableNull", ",").Nullable(),
		schema.MustSplit("Value", ","),
		schema.MustSplit("ValueNullable", ",").Nullable(),
		schema.MustSplit("ValueNullableNull", ",").Nullable(),
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
		schema.MustParseInt("Direct", 10, 64),
		schema.MustParseInt("Nullable", 10, 32).Nullable(),
		schema.MustParseInt("NullableNull", 10, 16).Nullable(),
		schema.MustParseInt("Value", 10, 8),
		schema.MustParseInt("ValueNullable", 10, 64).Nullable(),
		schema.MustParseInt("ValueNullableNull", 10, 64).Nullable(),
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
		schema.MustParseUint("Direct", 10, 64),
		schema.MustParseUint("Nullable", 10, 32).Nullable(),
		schema.MustParseUint("NullableNull", 10, 16).Nullable(),
		schema.MustParseUint("Value", 10, 8),
		schema.MustParseUint("ValueNullable", 10, 64).Nullable(),
		schema.MustParseUint("ValueNullableNull", 10, 64).Nullable(),
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
		schema.MustParseFloat("Direct", 64),
		schema.MustParseFloat("Nullable", 64).Nullable(),
		schema.MustParseFloat("NullableNull", 64).Nullable(),
		schema.MustParseFloat("Value", 64),
		schema.MustParseFloat("ValueNullable", 64).Nullable(),
		schema.MustParseFloat("ValueNullableNull", 64).Nullable(),
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
		schema.MustParseComplex("Direct", 128),
		schema.MustParseComplex("Nullable", 128).Nullable(),
		schema.MustParseComplex("NullableNull", 64).Nullable(),
		schema.MustParseComplex("Value", 64),
		schema.MustParseComplex("ValueNullable", 128).Nullable(),
		schema.MustParseComplex("ValueNullableNull", 128).Nullable(),
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
		schema.MustParseBool("Direct"),
		schema.MustParseBool("Nullable").Nullable(),
		schema.MustParseBool("NullableNull").Nullable(),
		schema.MustParseBool("Value"),
		schema.MustParseBool("ValueNullable").Nullable(),
		schema.MustParseBool("ValueNullableNull").Nullable(),
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
		schema.MustParseTime("Direct", time.DateOnly),
		schema.MustParseTime("Nullable", time.DateOnly).Nullable(),
		schema.MustParseTime("NullableNull", time.DateOnly).Nullable(),
		schema.MustParseTime("Value", time.DateOnly),
		schema.MustParseTime("ValueNullable", time.DateOnly).Nullable(),
		schema.MustParseTime("ValueNullableNull", time.DateOnly).Nullable(),
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
		schema.MustParseTimeInLocation("Direct", time.DateOnly, time.UTC),
		schema.MustParseTimeInLocation("Nullable", time.DateOnly, time.UTC).Nullable(),
		schema.MustParseTimeInLocation("NullableNull", time.DateOnly, time.UTC).Nullable(),
		schema.MustParseTimeInLocation("Value", time.DateOnly, time.UTC),
		schema.MustParseTimeInLocation("ValueNullable", time.DateOnly, time.UTC).Nullable(),
		schema.MustParseTimeInLocation("ValueNullableNull", time.DateOnly, time.UTC).Nullable(),
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
		schema.MustStringEnum("Direct",
			structscan.Enum{String: "Inactive", Int: 0},
			structscan.Enum{String: "Active", Int: 1},
		),
		schema.MustStringEnum("Nullable",
			structscan.Enum{String: "Inactive", Int: 0},
			structscan.Enum{String: "Active", Int: 1},
		).Nullable(),
		schema.MustStringEnum("NullableNull",
			structscan.Enum{String: "Inactive", Int: 0},
			structscan.Enum{String: "Active", Int: 1},
		).Nullable(),
		schema.MustStringEnum("Value",
			structscan.Enum{String: "Inactive", Int: 0},
			structscan.Enum{String: "Active", Int: 1},
		),
		schema.MustStringEnum("ValueNullable",
			structscan.Enum{String: "Inactive", Int: 0},
			structscan.Enum{String: "Active", Int: 1},
		).Nullable(),
		schema.MustStringEnum("ValueNullableNull",
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
		schema.MustIntEnum("Direct",
			structscan.Enum{String: "Inactive", Int: 0},
			structscan.Enum{String: "Active", Int: 1},
		),
		schema.MustIntEnum("Nullable",
			structscan.Enum{String: "Inactive", Int: 0},
			structscan.Enum{String: "Active", Int: 1},
		).Nullable(),
		schema.MustIntEnum("NullableNull",
			structscan.Enum{String: "Inactive", Int: 0},
			structscan.Enum{String: "Active", Int: 1},
		).Nullable(),
		schema.MustIntEnum("Value",
			structscan.Enum{String: "Inactive", Int: 0},
			structscan.Enum{String: "Active", Int: 1},
		),
		schema.MustIntEnum("ValueNullable",
			structscan.Enum{String: "Inactive", Int: 0},
			structscan.Enum{String: "Active", Int: 1},
		).Nullable(),
		schema.MustIntEnum("ValueNullableNull",
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
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	schema := structscan.New[map[string]any]()

	mapper := structscan.Map(
		schema.MustUnmarshalJSON(""),
	)

	expect := map[string]any{
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
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	schema := structscan.New[*url.URL]()

	mapper := structscan.Map(
		schema.MustUnmarshalBinary(""),
	)

	expect, err := url.Parse("https://localhost:1234/path?query=true")
	if err != nil {
		t.Fatal(err)
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
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	schema := structscan.New[*big.Int]()

	mapper := structscan.Map(
		schema.MustUnmarshalText(""),
	)

	expect := big.NewInt(10)

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

func TestSingleInt(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	schema := structscan.New[int64]()

	mapper := structscan.Map(
		schema.MustNullable(""),
	)

	expect := int64(10)

	rows, err := db.Query(`SELECT 10`)
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

func TestSingleString(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mapper := structscan.Map(
		structscan.New[string](),
	)

	expect := "10"

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

func TestSingleFloat(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mapper := structscan.Map[float64]()

	expect := 1.23

	rows, err := db.Query(`SELECT 1.23`)
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

func TestSingleFloatDefault(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mapper := structscan.Map(
		structscan.New[float64]().MustDefaultFloat("", 1.23),
	)

	expect := 1.23

	rows, err := db.Query(`SELECT NULL`)
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
