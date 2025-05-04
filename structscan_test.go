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

type Sample struct {
	Int      int64
	String   *string
	Bool     bool
	Time     time.Time
	MyTime   MyTime
	Big      *big.Int
	URL      *url.URL
	IntSlice []int32
	JSON     map[string]any
	RawJSON  json.RawMessage
}

var schema = structscan.Describe[Sample]()

func TestStructScan_All_One_First(t *testing.T) {
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
		(1, 'test', 1, '2025-05-01', '2025-05-01', '12345678901234567890', 'https://example.com/test?q=1', '10,20,30', '{"a": 1}', '{"a": 1}')
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
		IntSlice: []int32{
			10, 20, 30,
		},
		JSON:    map[string]any{"a": float64(1)},
		RawJSON: []byte(`{"a":1}`),
	}

	tests := []struct {
		name   string
		scanFn func(*sql.DB) (Sample, error)
	}{
		{
			name: "First",
			scanFn: func(db *sql.DB) (Sample, error) {
				row := db.QueryRow(`SELECT * FROM sample`)
				return structscan.First(row, scanSchema()...)
			},
		},
		{
			name: "One",
			scanFn: func(db *sql.DB) (Sample, error) {
				rows, err := db.Query(`SELECT * FROM sample`)
				if err != nil {
					return Sample{}, err
				}
				return structscan.One(rows, scanSchema()...)
			},
		},
		{
			name: "All[0]",
			scanFn: func(db *sql.DB) (Sample, error) {
				rows, err := db.Query(`SELECT * FROM sample`)
				if err != nil {
					return Sample{}, err
				}
				all, err := structscan.All(rows, scanSchema()...)
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
				!reflect.DeepEqual(got.IntSlice, expected.IntSlice) ||
				!reflect.DeepEqual(got.JSON, expected.JSON) {
				t.Errorf("got %+v; want %+v", got, expected)
			}
		})
	}
}

func scanSchema() []structscan.Scanner[Sample] {
	return []structscan.Scanner[Sample]{
		schema["Int"].Optional(),

		schema.Field("String").Required(),

		schema["Bool"].Optional().Bool(),

		schema["Time"].String(structscan.ParseTime(time.DateOnly, time.UTC)),

		schema["MyTime"].String(structscan.ParseTime(time.RFC3339, time.UTC)),

		schema["Big"].Bytes(structscan.UnmarshalText()),

		schema["URL"].Bytes(structscan.UnmarshalBinary()),

		schema["IntSlice"].String(structscan.Split(",", structscan.ParseInt(10, 32))),

		schema["JSON"].Bytes(structscan.UnmarshalJSON()),

		schema["RawJSON"].Bytes(structscan.UnmarshalJSON()),
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
