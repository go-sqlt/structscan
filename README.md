# structscan

[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white)](https://pkg.go.dev/github.com/go-sqlt/structscan)
[![GitHub tag (latest SemVer)](https://img.shields.io/github/tag/go-sqlt/structscan.svg?style=social)](https://github.com/go-sqlt/structscan/tags)
[![Coverage](https://img.shields.io/badge/Coverage-76.5%25-brightgreen)](https://github.com/go-sqlt/structscan/actions)

**structscan** is a lightweight Go library that maps SQL query results into Go structs.

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

type Enum int

const (
	Invalid Enum = iota
	Active
	Inactive
)

type EnumString string

const (
	InvalidString  EnumString = "invalid"
	ActiveString   EnumString = "active"
	InactiveString EnumString = "inactive"
)

type Data struct {
	Int           int64
	String        string
	Bool          bool
	Time          time.Time
	Big           big.Int
	URL           *url.URL
	SliceSliceInt [2][3]int
	JSON          map[string]string
	Enum          Enum
	EnumSlice     []EnumString
}

var (
	schema = structscan.Describe[Data]()
	mapper = structscan.Map(
		schema["Int"].MustConvert(structscan.MustOneOf(100, 200, 300)),
		schema["String"].MustConvert(structscan.Default("default")),
		schema["Bool"],
		schema["Time"].MustConvert(structscan.ParseTime(time.DateOnly)),
		schema["Big"].MustConvert(structscan.UnmarshalText()),
		schema["URL"].MustConvert(structscan.UnmarshalBinary()),
		schema["SliceSliceInt"].MustConvert(
			structscan.Cut(",",
				structscan.Split("-",
					structscan.ParseInt(10, 64),
				),
			),
		),
		schema["JSON"].MustConvert(structscan.UnmarshalJSON()),
		schema["Enum"].MustConvert(structscan.MustEnum(InvalidString, Invalid, ActiveString, Active, InactiveString, Inactive)),
		schema["EnumSlice"].MustConvert(
			structscan.Split(",",
				structscan.Atoi(),
				structscan.MustEnum(Invalid, InvalidString, Active, ActiveString, Inactive, InactiveString),
			),
		),
	)
)

func main() {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		panic(err)
	}

	rows, err := db.Query(`
		SELECT
			100
			, NULL
			, true
			, '2025-05-01'
			, '300' as big
			, 'https://example.com/path?query=yes'
			, '100-200-300,400-500-600'
			, '{"hello":"world"}'
			, 'inactive'
			, '1,2,0'
	`)
	if err != nil {
		panic(err)
	}

	data, err := mapper.One(rows)
	if err != nil {
		panic(err)
	}

	fmt.Println(data)
	// {100 default true 2025-05-01 00:00:00 +0000 UTC {false [300]} https://example.com/path?query=yes [[100 200 300] [400 500 600]] map[hello:world] 2 [active inactive invalid]}
}
```