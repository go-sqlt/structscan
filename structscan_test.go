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

type MyTime time.Time

type MyString string

type Sample struct {
	Int     int64
	String  *string
	Bool    bool
	Time    time.Time
	MyTime  MyTime
	Big     *big.Int
	URL     *url.URL
	Slice   []*string
	JSON    map[string]any
	RawJSON json.RawMessage
}

var (
	schema       = structscan.Describe[Sample]()
	sampleMapper = structscan.Map(
		structscan.MustScan(schema.MustField("Int"), structscan.Default(100)),
		structscan.MustScan(schema["String"], structscan.Nullable()),
		structscan.MustScan(schema["Bool"], structscan.ParseBool()),
		structscan.MustScan(schema["Time"], structscan.ParseTime(time.DateOnly)),
		structscan.MustScan(schema["MyTime"], structscan.ParseTimeInLocation(time.RFC3339Nano, time.UTC)),
		structscan.MustScan(schema["Big"], structscan.UnmarshalText()),
		structscan.MustScan(schema["URL"], structscan.UnmarshalBinary()),
		structscan.MustScan(schema["Slice"], structscan.Split(",")),
		structscan.MustScan(schema["JSON"], structscan.UnmarshalJSON()),
		structscan.MustScan(schema["RawJSON"], structscan.UnmarshalJSON()),
	)
)

func TestSample(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE sample (
			int_val INTEGER,
			str_val TEXT,
			bool_val BOOLEAN,
			time_val TEXT,
			my_time_val DATE,
			big_val TEXT,
			url_val TEXT,
			ints_val TEXT,
			json_val TEXT,
			raw_json_val BLOB
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(`
		INSERT INTO sample VALUES
		(1, 'test', 't', '2025-05-01', '2025-05-01', '12345678901234567890', 'https://example.com/test?q=1', '10,20,30', '{"a": 1}', '{"a": 1}')
	`)
	if err != nil {
		t.Fatal(err)
	}

	expected := Sample{
		Int:    1,
		String: ptr("test"),
		Bool:   true,
		Time:   mustParse("2025-05-01"),
		MyTime: MyTime(mustParse("2025-05-01")),
		Big:    bigFromString("12345678901234567890"),
		URL:    mustURL("https://example.com/test?q=1"),
		Slice: []*string{
			ptr("10"), ptr("20"), ptr("30"),
		},
		JSON:    map[string]any{"a": float64(1)},
		RawJSON: []byte(`{"a":1}`),
	}

	tests := []struct {
		name   string
		scanFn func(*sql.DB) (Sample, error)
	}{
		{
			name: "Row",
			scanFn: func(db *sql.DB) (Sample, error) {
				row := db.QueryRow(`SELECT * FROM sample`)
				return sampleMapper.Row(row)
			},
		},
		{
			name: "One",
			scanFn: func(db *sql.DB) (Sample, error) {
				rows, err := db.Query(`SELECT * FROM sample`)
				if err != nil {
					return Sample{}, err
				}

				return sampleMapper.One(rows)
			},
		},
		{
			name: "All[0]",
			scanFn: func(db *sql.DB) (Sample, error) {
				rows, err := db.Query(`SELECT * FROM sample`)
				if err != nil {
					return Sample{}, err
				}

				all, err := sampleMapper.All(rows)
				if err != nil {
					return Sample{}, err
				}

				if len(all) != 1 {
					return Sample{}, fmt.Errorf("expected 1 row, got %d", len(all))
				}

				return all[0], nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.scanFn(db)
			if err != nil {
				t.Fatalf("scanFn failed: %v", err)
			}

			if !reflect.DeepEqual(got.Int, expected.Int) ||
				*got.String != *expected.String ||
				got.Bool != expected.Bool ||
				!got.Time.Equal(expected.Time) ||
				got.Big.String() != expected.Big.String() ||
				got.URL.String() != expected.URL.String() ||
				!reflect.DeepEqual(got.Slice, expected.Slice) ||
				!reflect.DeepEqual(got.JSON, expected.JSON) {
				t.Errorf("got %+v; want %+v", got, expected)
			}
		})
	}
}

func TestIntColumn(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		scanFn   func(db *sql.DB) *sql.Row
		expected int64
	}{
		{
			name: "int",
			scanFn: func(db *sql.DB) *sql.Row {
				return db.QueryRow("SELECT 100")
			},
			expected: 100,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mapper := structscan.Map[int64]()

			row := tt.scanFn(db)
			if err != nil {
				t.Fatalf("scanFn failed: %v", err)
			}

			result, err := mapper.Row(row)
			if err != nil {
				t.Fatalf("scanFn failed: %v", err)
			}

			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("got %+v; want %+v", result, tt.expected)
			}
		})
	}
}

func ptr[T any](v T) *T {
	return &v
}

func bigFromString(s string) *big.Int {
	i := new(big.Int)
	i.SetString(s, 10)
	return i
}

func mustURL(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		panic(err)
	}
	return u
}

func mustParse(date string) time.Time {
	t, err := time.Parse(time.DateOnly, date)
	if err != nil {
		panic(err)
	}
	return t
}

func TestDecoders(t *testing.T) {
	tests := []struct {
		converter structscan.Converter
		src       any
		result    any
	}{
		{
			converter: structscan.Atoi(),
			src:       "100",
			result:    int64(100),
		},
		{
			converter: structscan.ParseInt(10, 64),
			src:       "100",
			result:    int32(100),
		},
		{
			converter: structscan.ParseInt(10, 64),
			src:       "100",
			result:    int64(100),
		},
		{
			converter: structscan.ParseInt(10, 64),
			src:       "100",
			result:    int32(100),
		},
		{
			converter: structscan.ParseBool(),
			src:       "t",
			result:    true,
		},
		{
			converter: structscan.ParseUint(10, 64),
			src:       "10",
			result:    uint16(10),
		},
		{
			converter: structscan.ParseFloat(64),
			src:       "10.123",
			result:    float32(10.123),
		},
		{
			converter: structscan.ParseComplex(128),
			src:       "10",
			result:    complex128(10),
		},
		{
			converter: structscan.ParseTime("2006"),
			src:       "2022",
			result: func() time.Time {
				v, _ := time.Parse("2006", "2022")

				return v
			}(),
		},
		{
			converter: structscan.ParseTimeInLocation("2006", time.UTC),
			src:       "2022",
			result: func() MyTime {
				v, _ := time.Parse("2006", "2022")

				return MyTime(v)
			}(),
		},
		{
			converter: structscan.ParseTimeInLocation("2006", time.UTC),
			src:       "2022",
			result: func() time.Time {
				v, _ := time.Parse("2006", "2022")

				return v
			}(),
		},
		{
			converter: structscan.ParseTimeInLocation("2006", time.UTC),
			src:       "2022",
			result: func() MyTime {
				v, _ := time.Parse("2006", "2022")

				return MyTime(v)
			}(),
		},
		{
			converter: structscan.Split("-"),
			src:       "100-200-300",
			result:    []string{"100", "200", "300"},
		},
		{
			converter: structscan.Split("-"),
			src:       "100-200-300",
			result:    [3]string{"100", "200", "300"},
		},
		{
			converter: structscan.Split("-"),
			src:       "100-200-300",
			result:    []MyString{"100", "200", "300"},
		},
		{
			converter: structscan.Cut("-"),
			src:       "100-200",
			result:    [2]MyString{"100", "200"},
		},
		{
			converter: structscan.Cut("-"),
			src:       "100-200",
			result:    []MyString{"100", "200"},
		},
		{
			converter: structscan.Default(url.URL{Path: "path", Host: "host"}),
			src:       nil,
			result:    url.URL{Path: "path", Host: "host"},
		},
		{
			converter: structscan.Trim("()"),
			src:       "(hello)",
			result:    "hello",
		},
		{
			converter: structscan.TrimPrefix("prefix-"),
			src:       "prefix-(hello)",
			result:    "(hello)",
		},
		{
			converter: structscan.Chain(structscan.TrimSuffix("-suffix"), structscan.Trim("()")),
			src:       "(hello)-suffix",
			result:    "hello",
		},
		{
			converter: structscan.EqualFold("hello"),
			src:       "HELLO",
			result:    true,
		},
		{
			converter: structscan.Contains("hello"),
			src:       "hello world",
			result:    true,
		},
		{
			converter: structscan.Index("world"),
			src:       "hello world",
			result:    6,
		},
		{
			converter: structscan.ContainsAny("abc"),
			src:       "hello world",
			result:    false,
		},
		{
			converter: structscan.HasPrefix("hello"),
			src:       "hello world",
			result:    true,
		},
		{
			converter: structscan.HasSuffix("world"),
			src:       "hello world",
			result:    true,
		},
		{
			converter: structscan.Count("l"),
			src:       "hello world",
			result:    3,
		},
		{
			converter: structscan.MustEnum("hello", 1, "world", 2),
			src:       "hello",
			result:    1,
		},
		{
			converter: structscan.MustOneOf("hello", "world"),
			src:       "world",
			result:    "world",
		},
		{
			converter: structscan.Nullable(),
			src:       ptr("hello"),
			result:    "hello",
		},
		{
			converter: structscan.ToLower(),
			src:       "HELLO",
			result:    "hello",
		},
		{
			converter: structscan.ToUpper(),
			src:       "hello",
			result:    "HELLO",
		},
		{
			converter: structscan.UnmarshalBinary(),
			src:       []byte(`https://localhost`),
			result: url.URL{
				Scheme: "https",
				Host:   "localhost",
			},
		},
		{
			converter: structscan.UnmarshalBinary(),
			src:       []byte(`https://localhost`),
			result: &url.URL{
				Scheme: "https",
				Host:   "localhost",
			},
		},
		{
			converter: structscan.UnmarshalText(),
			src:       []byte(`10000`),
			result:    big.NewInt(10_000),
		},
		{
			converter: structscan.UnmarshalText(),
			src:       []byte(`10000`),
			result:    *big.NewInt(10_000),
		},
		{
			converter: structscan.Chain(),
			src:       10000,
			result:    10000,
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%v", tt.src), func(t *testing.T) {
			dstType := reflect.TypeOf(tt.result)

			_, convert, err := tt.converter(dstType)
			if err != nil {
				t.Fatal(err)
			}

			val, err := convert(reflect.ValueOf(tt.src))
			if err != nil {
				panic(err)
			}

			if !reflect.DeepEqual(val.Interface(), tt.result) {
				t.Fatal("value is not equal", val.Interface(), tt.result)
			}
		})
	}
}
