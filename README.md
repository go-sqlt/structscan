# structscan

[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white)](https://pkg.go.dev/github.com/go-sqlt/structscan)
[![GitHub tag (latest SemVer)](https://img.shields.io/github/tag/go-sqlt/structscan.svg?style=social)](https://github.com/go-sqlt/structscan/tags)
[![Coverage](https://img.shields.io/badge/Coverage-81.9%25-brightgreen)](https://github.com/go-sqlt/structscan/actions)

**structscan** is a lightweight Go library that maps SQL query results to Go structs.

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
	Int    int
	String string
	Bool   bool
	Time   *time.Time
	Big    big.Int
	URL    *url.URL
	JSON   map[string]string
	Slice  []string
}

var (
	schema = structscan.New[Data]()
	mapper = structscan.Map(
		schema["Int"].MustIntEnum(
			structscan.Enum{String: "one", Int: 1},
			structscan.Enum{String: "two", Int: 2},
			structscan.Enum{String: "three", Int: 3},
			structscan.Enum{String: "hundred", Int: 100},
		),
		schema["String"].MustStringEnum(
			structscan.Enum{String: "one", Int: 1},
			structscan.Enum{String: "two", Int: 2},
			structscan.Enum{String: "three", Int: 3},
			structscan.Enum{String: "hundred", Int: 100},
		),
		schema["Bool"].MustBool(),
		schema["Time"].MustParseTime(time.DateOnly).Default("2001-02-03"),
		schema["Big"].MustUnmarshalText(),
		schema["URL"].MustUnmarshalBinary(),
		schema["JSON"].UnmarshalJSON().Default([]byte(`{"hello":"world"}`)),
		schema["Slice"].MustSplit(","),
	)
)

func main() {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		panic(err)
	}

	rows, err := db.Query(`
		SELECT
			'one'
			, 2
			, true
			, NULL
			, '300'
			, 'https://example.com/path?query=yes'
			, NULL
			, 'hello,world'
	`)
	if err != nil {
		panic(err)
	}

	data, err := mapper.One(rows)
	if err != nil {
		panic(err)
	}

	fmt.Println(data)
	// {1 two true 2001-02-03 00:00:00 +0000 UTC {false [300]} https://example.com/path?query=yes map[hello:world] [hello world]}
}
```