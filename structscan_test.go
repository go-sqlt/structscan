package structscan_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/go-sqlt/structscan"
	_ "modernc.org/sqlite"
)

type Book struct {
	ID          int64
	Title       string
	Author      string
	PublishedAt time.Time
}

func TestBook(t *testing.T) {
	db := must(sql.Open("sqlite", ":memory:")).test(t)

	_ = must(db.Exec(`
		CREATE TABLE books (
			id INTEGER PRIMARY KEY,
			title TEXT NOT NULL,
			author TEXT NOT NULL,
			published_at DATE NOT NULL
		);
	`)).test(t)

	_ = must(db.Exec(`
		INSERT INTO books (title, author, published_at) VALUES
		(?, ?, ?), (?, ?, ?);
	`,
		"The Hobbit or There and Back Again", "J. R. R. Tolkien", "1937-09-21",
		"The Fellowship of the Ring", "J. R. R. Tolkien", "1954-07-29",
	))

	rows, err := db.Query(`
		SELECT id, title, author, published_at FROM books;
	`)
	if err != nil {
		t.Fatal(err)
	}

	bookstruct := structscan.New[Book]()

	id := must(bookstruct.Int("ID")).test(t)
	title := must(bookstruct.String("Title")).test(t)
	author := must(bookstruct.String("Author")).test(t)
	publishedAt := must(bookstruct.Time("PublishedAt")).test(t)

	books := must(structscan.All(rows, id, title, author, publishedAt)).test(t)

	if len(books) != 2 {
		t.Fatal("invalid number of books", len(books))
	}

}

type muster[T any] struct {
	t   T
	err error
}

func (m muster[T]) test(t *testing.T) T {
	if m.err != nil {
		t.Fatal(m.err)
	}

	return m.t
}

func must[T any](t T, err error) muster[T] {
	return muster[T]{
		t:   t,
		err: err,
	}
}
