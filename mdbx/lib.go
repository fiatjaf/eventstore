package mdbx

import (
	"encoding/binary"
	"os"
	"sync/atomic"

	"github.com/erigontech/mdbx-go/mdbx"
	"github.com/fiatjaf/eventstore"
)

var _ eventstore.Store = (*MDBXBackend)(nil)

type MDBXBackend struct {
	Path               string
	MaxLimit           int
	MaxLimitNegentropy int
	MapSize            int64

	mdbxEnv    *mdbx.Env
	extraFlags uint // (for debugging and testing)

	settingsStore   mdbx.DBI
	rawEventStore   mdbx.DBI
	indexCreatedAt  mdbx.DBI
	indexId         mdbx.DBI
	indexKind       mdbx.DBI
	indexPubkey     mdbx.DBI
	indexPubkeyKind mdbx.DBI
	indexTag        mdbx.DBI
	indexTag32      mdbx.DBI
	indexTagAddr    mdbx.DBI
	indexPTagKind   mdbx.DBI

	hllCache          mdbx.DBI
	EnableHLLCacheFor func(kind int) (useCache bool, skipSavingActualEvent bool)

	lastId atomic.Uint32
}

func (b *MDBXBackend) Init() error {
	if b.MaxLimit != 0 {
		b.MaxLimitNegentropy = b.MaxLimit
	} else {
		b.MaxLimit = 500
		if b.MaxLimitNegentropy == 0 {
			b.MaxLimitNegentropy = 16777216
		}
	}

	// create directory if it doesn't exist and open it
	if err := os.MkdirAll(b.Path, 0755); err != nil {
		return err
	}

	return b.initialize()
}

func (b *MDBXBackend) Close() {
	b.mdbxEnv.Close()
}

func (b *MDBXBackend) Serial() []byte {
	v := b.lastId.Add(1)
	vb := make([]byte, 4)
	binary.BigEndian.PutUint32(vb[:], uint32(v))
	return vb
}

func (b *MDBXBackend) initialize() error {
	env, err := mdbx.NewEnv()
	if err != nil {
		return err
	}

	b.mdbxEnv = env
	if err := b.mdbxEnv.SetOption(mdbx.OptMaxDB, 12); err != nil {
		return err
	}
	if err := b.mdbxEnv.SetOption(mdbx.OptMaxReaders, 1000); err != nil {
		return err
	}

	if err := env.Open(b.Path, mdbx.NoTLS|mdbx.WriteMap|b.extraFlags, 0644); err != nil {
		return err
	}

	var multiIndexCreationFlags uint = mdbx.Create | mdbx.DupSort | mdbx.DupFixed

	// open each db
	if err := b.mdbxEnv.Update(func(txn *mdbx.Txn) error {
		if dbi, err := txn.OpenDBISimple("settings", mdbx.Create); err != nil {
			return err
		} else {
			b.settingsStore = dbi
		}
		if dbi, err := txn.OpenDBISimple("raw", mdbx.Create); err != nil {
			return err
		} else {
			b.rawEventStore = dbi
		}
		if dbi, err := txn.OpenDBISimple("created_at", multiIndexCreationFlags); err != nil {
			return err
		} else {
			b.indexCreatedAt = dbi
		}
		if dbi, err := txn.OpenDBISimple("id", mdbx.Create); err != nil {
			return err
		} else {
			b.indexId = dbi
		}
		if dbi, err := txn.OpenDBISimple("kind", multiIndexCreationFlags); err != nil {
			return err
		} else {
			b.indexKind = dbi
		}
		if dbi, err := txn.OpenDBISimple("pubkey", multiIndexCreationFlags); err != nil {
			return err
		} else {
			b.indexPubkey = dbi
		}
		if dbi, err := txn.OpenDBISimple("pubkeyKind", multiIndexCreationFlags); err != nil {
			return err
		} else {
			b.indexPubkeyKind = dbi
		}
		if dbi, err := txn.OpenDBISimple("tag", multiIndexCreationFlags); err != nil {
			return err
		} else {
			b.indexTag = dbi
		}
		if dbi, err := txn.OpenDBISimple("tag32", multiIndexCreationFlags); err != nil {
			return err
		} else {
			b.indexTag32 = dbi
		}
		if dbi, err := txn.OpenDBISimple("tagaddr", multiIndexCreationFlags); err != nil {
			return err
		} else {
			b.indexTagAddr = dbi
		}
		if dbi, err := txn.OpenDBISimple("ptagKind", multiIndexCreationFlags); err != nil {
			return err
		} else {
			b.indexPTagKind = dbi
		}
		if dbi, err := txn.OpenDBISimple("hllCache", mdbx.Create); err != nil {
			return err
		} else {
			b.hllCache = dbi
		}
		return nil
	}); err != nil {
		return err
	}

	// get lastId
	if err := b.mdbxEnv.View(func(txn *mdbx.Txn) error {
		cursor, err := txn.OpenCursor(b.rawEventStore)
		if err != nil {
			return err
		}
		defer cursor.Close()
		k, _, err := cursor.Get(nil, nil, mdbx.Last)
		if operr, ok := err.(*mdbx.OpError); ok && operr.Errno == mdbx.NotFound {
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
