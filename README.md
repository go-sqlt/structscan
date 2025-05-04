# structscan

[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white)](https://pkg.go.dev/github.com/go-sqlt/structscan)
[![GitHub tag (latest SemVer)](https://img.shields.io/github/tag/go-sqlt/structscan.svg?style=social)](https://github.com/go-sqlt/structscan/tags)
[![Coverage](https://img.shields.io/badge/Coverage-51.7%25-yellow)](https://github.com/go-sqlt/structscan/actions)

**structscan** is a lightweight Go library that maps SQL query results directly into Go structs using reflection and configurable decoders. It provides composable, zero-magic scanning utilities built around `database/sql`, with support for nested fields, decoding, validation, and optional/required semantics.

```sh
go get -u github.com/go-sqlt/structscan
```

## Example

```go
package main

import (
	"database/sql"
	"fmt"
	"math/big"
	"net/url"
	"time"

	"github.com/go-sqlt/structscan"
	_ "modernc.org/sqlite"
)

type Data struct {
	Int      int64
	String   *string
	Bool     bool
	Time     time.Time
	Big      *big.Int
	URL      *url.URL
	IntSlice []int32
	JSON     map[string]any
}

// Schema holds a reflection-based description of the Data type.
// This provides addressable access to fields by name, for mapping values.
var Schema = structscan.Describe[Data]()

func main() {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		panic(err)
	}

	rows, err := db.Query(`
		SELECT
			100                                    -- Int (int64)
			, '200'                                -- String (*string)
			, true                                 -- Bool (bool)
			, '2025-05-01'                         -- Time (parsed from string)
			, '300'                                -- Big (decoded from text)
			, 'https://example.com/path?query=yes' -- URL (decoded from binary)
			, '400,500,600'                        -- IntSlice (comma-separated ints)
			, '{"hello":"world"}'                  -- JSON (parsed into a map)
	`)
	if err != nil {
		panic(err)
	}

	// Use structscan to scan the row into a slice of Data structs.
	// Each field maps to a column, with optional decoding/parsing behavior.
	data, err := structscan.All(rows,
		// Scans an int64 value into the Int field.
		Schema["Int"],

		// Fails if the value is NULL.
		Schema["String"].Required(),

		// Scans a boolean directly.
		Schema["Bool"].Bool(),

		// Parses a date string (in 'YYYY-MM-DD' format) into a time.Time.
		Schema["Time"].String(structscan.ParseTime(time.DateOnly, time.UTC)),

		// Scans raw bytes and decodes them using encoding.TextUnmarshaler.
		// In this case, it populates a *big.Int from string like "300".
		Schema["Big"].Bytes(structscan.UnmarshalText()),

		// Decodes binary input into a *url.URL using encoding.BinaryUnmarshaler.
		Schema["URL"].Bytes(structscan.UnmarshalBinary()),

		// Splits a comma-separated string and parses each part into an int32 slice.
		Schema["IntSlice"].String(structscan.Split(",", structscan.ParseInt(10, 32))),

		// Scans bytes and parses them as JSON into a map[string]any.
		Schema["JSON"].Bytes(structscan.UnmarshalJSON()),
	)
	if err != nil {
		panic(err)
	}

	fmt.Println(data)
	// [{100 0x1400012c240 true 2025-05-01 00:00:00 +0000 UTC 300 https://example.com/path?query=yes [400 500 600] map[hello:world]}]
}
```