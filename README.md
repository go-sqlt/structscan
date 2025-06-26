# structscan

[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white)](https://pkg.go.dev/github.com/go-sqlt/structscan)
[![GitHub tag (latest SemVer)](https://img.shields.io/github/tag/go-sqlt/structscan.svg?style=social)](https://github.com/go-sqlt/structscan/tags)
[![Coverage](https://img.shields.io/badge/Coverage-61.8%25-yellow)](https://github.com/go-sqlt/structscan/actions)

**structscan** is a lightweight Go library that maps SQL query results to Go structs.

```sh
go get -u github.com/go-sqlt/structscan
```

## Example

```go
package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/go-sqlt/structscan"
	_ "modernc.org/sqlite"
)

type Data struct {
	Int  uint64
	Bool bool
}

func main() {
	dest := structscan.NewSchema[Data]()

	mapper, err := structscan.NewMapper(
		dest.Scan().String().Int(10, 64).MustTo("Int"),
		dest.Scan().MustTo("Bool"),
	)
	if err != nil {
		panic(err)
	}

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		panic(err)
	}

	data, err := mapper.QueryOne(context.Background(), db, "SELECT '2', true")
	if err != nil {
		panic(err)
	}

	fmt.Println(data) // {2 true}
}
```
