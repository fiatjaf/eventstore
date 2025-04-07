package mmm

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/fiatjaf/eventstore"
	"github.com/nbd-wtf/go-nostr"
)

var _ eventstore.Store = (*IndexingLayer)(nil)

type IndexingLayer struct {
	isInitialized bool
	name          string

	ShouldIndex func(context.Context, *nostr.Event) bool
	MaxLimit    int

	mmmm *MultiMmapManager

	// this is stored in the knownLayers db as a value, and used to keep track of which layer owns each event
	id uint16

	lmdbEnv *lmdb.Env

	indexCreatedAt  lmdb.DBI
	indexKind       lmdb.DBI
	indexPubkey     lmdb.DBI
	indexPubkeyKind lmdb.DBI
	indexTag        lmdb.DBI
	indexTag32      lmdb.DBI
	indexTagAddr    lmdb.DBI
	indexPTagKind   lmdb.DBI
}

type IndexingLayers []*IndexingLayer

func (ils IndexingLayers) ByID(ilid uint16) *IndexingLayer {
	for _, il := range ils {
		if il.id == ilid {
			return il
		}
	}
	return nil
}

const multiIndexCreationFlags uint = lmdb.Create | lmdb.DupSort

func (il *IndexingLayer) Init() error {
	if il.isInitialized {
		return nil
	}
	il.isInitialized = true

	path := filepath.Join(il.mmmm.Dir, il.name)

	if il.MaxLimit == 0 {
		il.MaxLimit = 500
	}

	// open lmdb
	env, err := lmdb.NewEnv()
	if err != nil {
		return err
	}

	env.SetMaxDBs(8)
	env.SetMaxReaders(1000)
	env.SetMapSize(1 << 38) // ~273GB

	// create directory if it doesn't exist and open it
	if err := os.MkdirAll(path, 0755); err != nil {
		return err
	}

	err = env.Open(path, lmdb.NoTLS, 0644)
	if err != nil {
		return err
	}
	il.lmdbEnv = env

	// open each db
	if err := il.lmdbEnv.Update(func(txn *lmdb.Txn) error {
		if dbi, err := txn.OpenDBI("created_at", multiIndexCreationFlags); err != nil {
			return err
		} else {
			il.indexCreatedAt = dbi
		}
		if dbi, err := txn.OpenDBI("kind", multiIndexCreationFlags); err != nil {
			return err
		} else {
			il.indexKind = dbi
		}
		if dbi, err := txn.OpenDBI("pubkey", multiIndexCreationFlags); err != nil {
			return err
		} else {
			il.indexPubkey = dbi
		}
		if dbi, err := txn.OpenDBI("pubkeyKind", multiIndexCreationFlags); err != nil {
			return err
		} else {
			il.indexPubkeyKind = dbi
		}
		if dbi, err := txn.OpenDBI("tag", multiIndexCreationFlags); err != nil {
			return err
		} else {
			il.indexTag = dbi
		}
		if dbi, err := txn.OpenDBI("tag32", multiIndexCreationFlags); err != nil {
			return err
		} else {
			il.indexTag32 = dbi
		}
		if dbi, err := txn.OpenDBI("tagaddr", multiIndexCreationFlags); err != nil {
			return err
		} else {
			il.indexTagAddr = dbi
		}
		if dbi, err := txn.OpenDBI("ptagKind", multiIndexCreationFlags); err != nil {
			return err
		} else {
			il.indexPTagKind = dbi
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (il *IndexingLayer) Name() string { return il.name }

func (il *IndexingLayer) runThroughEvents(txn *lmdb.Txn) error {
	ctx := context.Background()
	b := il.mmmm

	// run through all events we have and see if this new index wants them
	cursor, err := txn.OpenCursor(b.indexId)
	if err != nil {
		return fmt.Errorf("when opening cursor on %v: %w", b.indexId, err)
	}
	defer cursor.Close()

	for {
		idPrefix8, val, err := cursor.Get(nil, nil, lmdb.Next)
		if lmdb.IsNotFound(err) {
			break
		}
		if err != nil {
			return fmt.Errorf("when moving the cursor: %w", err)
		}

		update := false

		posb := val[0:12]
		pos := positionFromBytes(posb)
		evt := &nostr.Event{}
		if err := b.loadEvent(pos, evt); err != nil {
			return fmt.Errorf("when loading event from mmap: %w", err)
		}

		if il.ShouldIndex != nil && il.ShouldIndex(ctx, evt) {
			// add the current reference
			val = binary.BigEndian.AppendUint16(val, il.id)

			// if we were already updating to remove the reference
			// now that we've added the reference back we don't really have to update
			update = !update

			// actually index
			if err := il.lmdbEnv.Update(func(iltxn *lmdb.Txn) error {
				for k := range il.getIndexKeysForEvent(evt) {
					if err := iltxn.Put(k.dbi, k.key, posb, 0); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				return fmt.Errorf("failed to index: %w", err)
			}
		}

		if update {
			if err := txn.Put(b.indexId, idPrefix8, val, 0); err != nil {
				return fmt.Errorf("failed to put updated index+refs: %w", err)
			}
		}
	}
	return nil
}

func (il *IndexingLayer) Close() {
	il.lmdbEnv.Close()
}
