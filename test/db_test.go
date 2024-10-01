package test

import (
	"context"
	"os"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/fiatjaf/eventstore/badger"
	"github.com/fiatjaf/eventstore/lmdb"
	"github.com/fiatjaf/eventstore/postgresql"
	"github.com/fiatjaf/eventstore/slicestore"
	"github.com/fiatjaf/eventstore/sqlite3"
)

const (
	dbpath = "/tmp/eventstore-test"
	sk3    = "0000000000000000000000000000000000000000000000000000000000000003"
	sk4    = "0000000000000000000000000000000000000000000000000000000000000004"
)

var ctx = context.Background()

func TestSliceStore(t *testing.T) {
	t.Run("first", func(t *testing.T) {
		runFirstTestOn(t, &slicestore.SliceStore{})
	})
	t.Run("second", func(t *testing.T) {
		runSecondTestOn(t, &slicestore.SliceStore{})
	})
}

func TestLMDB(t *testing.T) {
	os.RemoveAll(dbpath + "lmdb")
	t.Run("first", func(t *testing.T) {
		runFirstTestOn(t, &lmdb.LMDBBackend{Path: dbpath + "lmdb"})
	})
	os.RemoveAll(dbpath + "lmdb")
	t.Run("second", func(t *testing.T) {
		runSecondTestOn(t, &lmdb.LMDBBackend{Path: dbpath + "lmdb"})
	})
}

func TestBadger(t *testing.T) {
	os.RemoveAll(dbpath + "badger")
	t.Run("first", func(t *testing.T) {
		runFirstTestOn(t, &badger.BadgerBackend{Path: dbpath + "badger"})
	})
	os.RemoveAll(dbpath + "badger")
	t.Run("second", func(t *testing.T) {
		runSecondTestOn(t, &badger.BadgerBackend{Path: dbpath + "badger"})
	})
}

func TestSQLite(t *testing.T) {
	os.RemoveAll(dbpath + "sqlite")
	t.Run("first", func(t *testing.T) {
		runFirstTestOn(t, &sqlite3.SQLite3Backend{DatabaseURL: dbpath + "sqlite", QueryTagsLimit: 50})
	})
	os.RemoveAll(dbpath + "sqlite")
	t.Run("second", func(t *testing.T) {
		runSecondTestOn(t, &sqlite3.SQLite3Backend{DatabaseURL: dbpath + "sqlite", QueryTagsLimit: 50})
	})
}

func TestPostgres(t *testing.T) {
	postgres := embeddedpostgres.NewDatabase()
	err := postgres.Start()
	if err != nil {
		t.Fatalf("failed to start embedded postgres: %s", err)
		return
	}
	defer postgres.Stop()
	runFirstTestOn(t, &postgresql.PostgresBackend{DatabaseURL: "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable", QueryTagsLimit: 50})
	runSecondTestOn(t, &postgresql.PostgresBackend{DatabaseURL: "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable", QueryTagsLimit: 50})
}
