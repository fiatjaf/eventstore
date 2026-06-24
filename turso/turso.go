package turso

import (
	"sync"

	"github.com/jmoiron/sqlx"
)

type TursoBackend struct {
	sync.Mutex
	*sqlx.DB
	DatabaseURL       string
	QueryLimit        int
	QueryIDsLimit     int
	QueryAuthorsLimit int
	QueryKindsLimit   int
	QueryTagsLimit    int
}

func (b *TursoBackend) Close() {
	b.DB.Close()
}
