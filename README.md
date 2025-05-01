# structscan

[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white)](https://pkg.go.dev/github.com/go-sqlt/structscan)
[![GitHub tag (latest SemVer)](https://img.shields.io/github/tag/go-sqlt/structscan.svg?style=social)](https://github.com/go-sqlt/structscan/tags)
[![Coverage](https://img.shields.io/badge/Coverage-51.7%25-yellow)](https://github.com/go-sqlt/structscan/actions)

```go
package main

import (
	"database/sql"
	"fmt"

	"github.com/go-sqlt/structscan"
	_ "modernc.org/sqlite"
)

type Book struct {
	ID    int64
	Title string
}

var (
	bookstruct = structscan.New[Book]()
	id, _      = bookstruct.Int("ID")
	title, _   = bookstruct.String("Title")
)

func main() {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(`
		CREATE TABLE books (
			id INTEGER PRIMARY KEY,
			title TEXT NOT NULL
		);
	`)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(
		`INSERT INTO books (title) VALUES (?),(?);`,
		"The Hobbit or There and Back Again",
		"The Lord of the Rings",
	)
	if err != nil {
		panic(err)
	}

	rows, err := db.Query("SELECT id, title FROM books")
	if err != nil {
		panic(err)
	}

	books, err := structscan.All(rows, id, title)
	if err != nil {
		panic(err)
	}

	fmt.Println(books) // [{1 The Hobbit or There and Back Again} {2 The Lord of the Rings}]
}

```