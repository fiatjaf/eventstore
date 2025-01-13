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
}

func (b *PostgresBackend) Close() {
	b.DB.Close()
}
