package lmdb

import (
	"encoding/binary"
	"os"
	"sync/atomic"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/fiatjaf/eventstore"
)

const (
	maxuint16 = 65535
	maxuint32 = 4294967295
)

var _ eventstore.Store = (*LMDBBackend)(nil)

type LMDBBackend struct {
	Path     string
	MaxLimit int
	MapSize int64

	lmdbEnv *lmdb.Env

	settingsStore   lmdb.DBI
	rawEventStore   lmdb.DBI
	indexCreatedAt  lmdb.DBI
	indexId         lmdb.DBI
	indexKind       lmdb.DBI
	indexPubkey     lmdb.DBI
	indexPubkeyKind lmdb.DBI
	indexTag        lmdb.DBI
	indexTag32      lmdb.DBI
	indexTagAddr    lmdb.DBI

	lastId atomic.Uint32
}

func (b *LMDBBackend) Init() error {
	if b.MaxLimit == 0 {
		b.MaxLimit = 500
	}

	// open lmdb
	env, err := lmdb.NewEnv()
	if err != nil {
		return err
	}

	env.SetMaxDBs(10)
	env.SetMaxReaders(1000)
	if b.MapSize == 0 {
		env.SetMapSize(1 << 38) // ~273GB
	} else {
		env.SetMapSize(b.MapSize)
		fmt.Printf("MapSize: %d\n", b.MapSize)
	}

	// create directory if it doesn't exist and open it
	if err := os.MkdirAll(b.Path, 0755); err != nil {
		return err
	}

	err = env.Open(b.Path, lmdb.NoTLS, 0644)
	if err != nil {
		return err
	}
	b.lmdbEnv = env

	var multiIndexCreationFlags uint = lmdb.Create | lmdb.DupSort | lmdb.DupFixed

	// open each db
	if err := b.lmdbEnv.Update(func(txn *lmdb.Txn) error {
		if dbi, err := txn.OpenDBI("settings", lmdb.Create); err != nil {
			return err
		} else {
			b.settingsStore = dbi
		}
		if dbi, err := txn.OpenDBI("raw", lmdb.Create); err != nil {
			return err
		} else {
			b.rawEventStore = dbi
		}
		if dbi, err := txn.OpenDBI("created_at", multiIndexCreationFlags); err != nil {
			return err
		} else {
			b.indexCreatedAt = dbi
		}
		if dbi, err := txn.OpenDBI("id", lmdb.Create); err != nil {
			return err
		} else {
			b.indexId = dbi
		}
		if dbi, err := txn.OpenDBI("kind", multiIndexCreationFlags); err != nil {
			return err
		} else {
			b.indexKind = dbi
		}
		if dbi, err := txn.OpenDBI("pubkey", multiIndexCreationFlags); err != nil {
			return err
		} else {
			b.indexPubkey = dbi
		}
		if dbi, err := txn.OpenDBI("pubkeyKind", multiIndexCreationFlags); err != nil {
			return err
		} else {
			b.indexPubkeyKind = dbi
		}
		if dbi, err := txn.OpenDBI("tag", multiIndexCreationFlags); err != nil {
			return err
		} else {
			b.indexTag = dbi
		}
		if dbi, err := txn.OpenDBI("tag32", multiIndexCreationFlags); err != nil {
			return err
		} else {
			b.indexTag32 = dbi
		}
		if dbi, err := txn.OpenDBI("tagaddr", multiIndexCreationFlags); err != nil {
			return err
		} else {
			b.indexTagAddr = dbi
		}
		return nil
	}); err != nil {
		return err
	}

	// get lastId
	if err := b.lmdbEnv.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true
		cursor, err := txn.OpenCursor(b.rawEventStore)
		if err != nil {
			return err
		}
		defer cursor.Close()
		k, _, err := cursor.Get(nil, nil, lmdb.Last)
		if operr, ok := err.(*lmdb.OpError); ok && operr.Errno == lmdb.NotFound {
			// nothing found, so we're at zero
			return nil
		}
		if err != nil {
		}
		b.lastId.Store(binary.BigEndian.Uint32(k))

		return nil
	}); err != nil {
		return err
	}

	return b.runMigrations()
}

func (b *LMDBBackend) Close() {
	b.lmdbEnv.Close()
}

func (b *LMDBBackend) Serial() []byte {
	v := b.lastId.Add(1)
	vb := make([]byte, 4)
	binary.BigEndian.PutUint32(vb[:], uint32(v))
	return vb
}

type key struct {
	dbi lmdb.DBI
	key []byte
}
