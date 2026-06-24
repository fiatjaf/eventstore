package test

import (
	"context"
	"os"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/fiatjaf/eventstore"
	"github.com/fiatjaf/eventstore/badger"
	"github.com/fiatjaf/eventstore/lmdb"
	"github.com/fiatjaf/eventstore/postgresql"
	"github.com/fiatjaf/eventstore/slicestore"
	"github.com/fiatjaf/eventstore/sqlite3"
	"github.com/fiatjaf/eventstore/turso"
)

const (
	dbpath = "/tmp/eventstore-test"
	sk3    = "0000000000000000000000000000000000000000000000000000000000000003"
	sk4    = "0000000000000000000000000000000000000000000000000000000000000004"
)

var ctx = context.Background()

var tests = []struct {
	name string
	run  func(*testing.T, eventstore.Store)
}{
	{"first", runFirstTestOn},
	{"second", runSecondTestOn},
	{"manyauthors", manyAuthorsTest},
	{"unbalanced", unbalancedTest},
}

func TestSliceStore(t *testing.T) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) { test.run(t, &slicestore.SliceStore{}) })
	}
}

func TestLMDB(t *testing.T) {
	for _, test := range tests {
		os.RemoveAll(dbpath + "lmdb")
		t.Run(test.name, func(t *testing.T) { test.run(t, &lmdb.LMDBBackend{Path: dbpath + "lmdb"}) })
	}
}

func TestBadger(t *testing.T) {
	for _, test := range tests {
		os.RemoveAll(dbpath + "badger")
		t.Run(test.name, func(t *testing.T) { test.run(t, &badger.BadgerBackend{Path: dbpath + "badger"}) })
	}
}

func TestSQLite(t *testing.T) {
	for _, test := range tests {
		os.RemoveAll(dbpath + "sqlite")
		t.Run(test.name, func(t *testing.T) {
			test.run(t, &sqlite3.SQLite3Backend{DatabaseURL: dbpath + "sqlite", QueryLimit: 1000, QueryTagsLimit: 50, QueryAuthorsLimit: 2000})
		})
	}
}

func TestTurso(t *testing.T) {
	// turso is a remote-only backend; it requires a real libsql:// (or https://) URL,
	// so the test only runs when TURSO_DATABASE_URL is set.
	url := os.Getenv("TURSO_DATABASE_URL")
	if url == "" {
		t.Skip("set TURSO_DATABASE_URL to run the turso test")
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.run(t, &turso.TursoBackend{DatabaseURL: url, QueryLimit: 1000, QueryTagsLimit: 50, QueryAuthorsLimit: 2000})
		})
	}
}

func TestPostgres(t *testing.T) {
	for _, test := range tests {
		postgres := embeddedpostgres.NewDatabase()
		err := postgres.Start()
		if err != nil {
			t.Fatalf("failed to start embedded postgres: %s", err)
			return
		}
		t.Run(test.name, func(t *testing.T) {
			test.run(t, &postgresql.PostgresBackend{DatabaseURL: "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable", QueryLimit: 1000, QueryTagsLimit: 50, QueryAuthorsLimit: 2000})
		})
		postgres.Stop()
	}
}
