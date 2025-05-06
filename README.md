# structscan

[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white)](https://pkg.go.dev/github.com/go-sqlt/structscan)
[![GitHub tag (latest SemVer)](https://img.shields.io/github/tag/go-sqlt/structscan.svg?style=social)](https://github.com/go-sqlt/structscan/tags)
[![Coverage](https://img.shields.io/badge/Coverage-56.5%25-yellow)](https://github.com/go-sqlt/structscan/actions)

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
	Bool     *bool
	Time     time.Time
	Big      *big.Int
	URL      *url.URL
	IntSlice []int32
	JSON     map[string]any
}

var Schema = structscan.Describe[Data]()

func main() {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		panic(err)
	}

	rows, err := db.Query(`
		SELECT
			100 as int
			, NULL as string
			, true as bool
			, '2025-05-01' as time
			, '300' as big
			, 'https://example.com/path?query=yes' as url
			, '400,500,600' as int_slice
			, '{"hello":"world"}' as json
	`)
	if err != nil {
		panic(err)
	}

	data, err := structscan.All(rows,
		Schema["Int"],             // int64
		Schema["String"],          // *string
		Schema["Bool"].Required(), // bool
		Schema["Time"].String(structscan.ParseTime(time.DateOnly, time.UTC)),          // string + time.ParseInLocation
		Schema["Big"].Bytes(structscan.UnmarshalText()),                               // []byte + encoding.UnmarshalText
		Schema["URL"].Bytes(structscan.UnmarshalBinary()),                             // []byte + encoding.UnmarshalBinary
		Schema["IntSlice"].String(structscan.Split(",", structscan.ParseInt(10, 32))), // string + strings.Split + strconv.ParseInt
		Schema["JSON"].Bytes(structscan.UnmarshalJSON()),                              // []byte + json.Unmarshal
	)
	if err != nil {
		panic(err)
	}

	fmt.Println(data[0])
	// {100 <nil> 0x14000098368 2025-05-01 00:00:00 +0000 UTC 300 https://example.com/path?query=yes [400 500 600] map[hello:world]}
}
```