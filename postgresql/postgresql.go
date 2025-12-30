package postgresql

import (
	"sync"

	"github.com/jmoiron/sqlx"
)

type PostgresBackend struct {
	sync.Mutex
	*sqlx.DB
	DatabaseURL       string
	QueryLimit        int
	QueryIDsLimit     int
	QueryAuthorsLimit int
	QueryKindsLimit   int
	QueryTagsLimit    int
	KeepRecentEvents  bool

	// PostgreSQL Full-Text Search settings
	// FullTextSearchUseTrgm enables the pg_trgm extension for better search performance.
	// if FullTextSearchUseTrgm is false, tsvector and tsquery are used without pg_trgm.
	// If tsvector and tsquery are used, searches may be slower for large datasets.
	// You can use FullTextSearchConfig for different text search configurations (e.g., "english", "simple").
	FullTextSearchUseTrgm   bool   // it need to create the pg_trgm extension in the database. and create indexes for the table.
	FullTextSearchMaxLength int    // maximum content length for full-text search, 0 means no limit
	FullTextSearchConfig    string // text search configuration for to_tsvector/to_tsquery, defaults to "simple"
	FullTextSearchColumn    string // column to search in, defaults to "content"
}

func (b *PostgresBackend) Close() {
	b.DB.Close()
}
