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
	Genres      []string
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

		CREATE TABLE genres (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL
		);

		CREATE TABLE book_genres (
			book_id INTEGER REFERENCES books (id),
			genre_id INTEGER REFERENCES genres (id),
			PRIMARY KEY (book_id, genre_id)
		);
	`)).test(t)

	bookIDs := must(structscan.All[int64](must(db.Query(`
		INSERT INTO books (title, author, published_at) VALUES
		(?, ?, ?), (?, ?, ?) RETURNING id;
	`,
		"The Hobbit or There and Back Again", "J. R. R. Tolkien", "1937-09-21",
		"The Fellowship of the Ring", "J. R. R. Tolkien", "1954-07-29",
	)).test(t))).test(t)

	genreIDs := must(structscan.All[int64](must(db.Query(`INSERT INTO genres (name) VALUES (?), (?), (?) RETURNING id;`,
		"High fantasy", "Children's fantasy", "Adventure",
	)).test(t))).test(t)

	_ = must(db.Exec(`
		INSERT INTO book_genres (book_id, genre_id) VALUES
			(?, ?), (?, ?), (?, ?), (?, ?);
	`,
		bookIDs[0], genreIDs[0],
		bookIDs[0], genreIDs[1],
		bookIDs[1], genreIDs[0],
		bookIDs[1], genreIDs[2],
	))

	rows := must(db.Query(`
		SELECT 
			books.id
			, title
			, author 
			, published_at 
			, GROUP_CONCAT(genres.name)
		FROM books
		LEFT JOIN book_genres ON book_genres.book_id = books.id
		LEFT JOIN genres ON genres.id = book_genres.genre_id
		GROUP BY books.id, title, author, published_at;
	`)).test(t)

	bookstruct := structscan.New[Book]()

	id := must(bookstruct.Int("ID")).test(t)
	title := must(bookstruct.String("Title")).test(t)
	author := must(bookstruct.String("Author")).test(t)
	publishedAt := must(bookstruct.Time("PublishedAt")).test(t)
	genres := must(bookstruct.StringSlice("Genres", ",")).test(t)

	books := must(structscan.All(rows, id, title, author, publishedAt, genres)).test(t)

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
