package lmdb

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"

	"github.com/PowerDNS/lmdb-go/lmdb"
	bin "github.com/fiatjaf/eventstore/internal/binary"
	"github.com/nbd-wtf/go-nostr"
)

const (
	DB_VERSION byte = 'v'
)

func (b *LMDBBackend) runMigrations() error {
	return b.lmdbEnv.Update(func(txn *lmdb.Txn) error {
		var version uint16
		v, err := txn.Get(b.settingsStore, []byte{DB_VERSION})
		if err != nil {
			if lmdbErr, ok := err.(*lmdb.OpError); ok && lmdbErr.Errno == lmdb.NotFound {
				version = 0
			} else if v == nil {
				return fmt.Errorf("failed to read database version: %w", err)
			}
		} else {
			version = binary.BigEndian.Uint16(v)
		}

		// the 4 first migrations go to trash because on version 3 we need to export and import all the data anyway
		if version == 0 {
			// if there is any data in the relay we will just set the version to the max without saying anything
			cursor, err := txn.OpenCursor(b.indexId)
			if err != nil {
				return fmt.Errorf("failed to open cursor in migration: %w", err)
			}
			defer cursor.Close()

			hasAnyEntries := false
			_, _, err = cursor.Get(nil, nil, lmdb.First)
			for err == nil {
				hasAnyEntries = true
				break
			}

			if !hasAnyEntries {
				b.setVersion(txn, 5)
				return nil
			}
		}

		// do the migrations in increasing steps (there is no rollback)
		//

		// this is when we added the ptag-kind-createdat index
		if version < 5 {
			log.Println("[lmdb] migration 5: reindex events with \"p\" tags for the ptagKind index")

			cursor, err := txn.OpenCursor(b.indexTag32)
			if err != nil {
				return fmt.Errorf("failed to open cursor in migration 5: %w", err)
			}
			defer cursor.Close()

			key, idx, err := cursor.Get(nil, nil, lmdb.First)
			for err == nil {
				if val, err := txn.Get(b.rawEventStore, idx); err != nil {
					return fmt.Errorf("error getting binary event for %x on migration 5: %w", idx, err)
				} else {
					evt := &nostr.Event{}
					if err := bin.Unmarshal(val, evt); err != nil {
						return fmt.Errorf("error decoding event %x on migration 5: %w", idx, err)
					}

					tagFirstChars := hex.EncodeToString(key[0:8])
					// we do this to prevent indexing other tags as "p" and also to not index the same event twice
					// this ensure we only do one ptagKind for each tag32 entry (if the tag32 happens to be a "p")
					if evt.Tags.GetFirst([]string{"p", tagFirstChars}) != nil {
						log.Printf("[lmdb] applying to key %x", key)
						newkey := make([]byte, 8+2+4)
						copy(newkey, key[0:8])
						binary.BigEndian.PutUint16(newkey[8:8+2], uint16(evt.Kind))
						binary.BigEndian.PutUint32(newkey[8+2:8+2+4], uint32(evt.CreatedAt))
						if err := txn.Put(b.indexPTagKind, newkey, idx, 0); err != nil {
							return fmt.Errorf("error saving tag on migration 5: %w", err)
						}
					}
				}

				// next
				key, idx, err = cursor.Get(nil, nil, lmdb.Next)
			}
			if lmdbErr, ok := err.(*lmdb.OpError); ok && lmdbErr.Errno != lmdb.NotFound {
				// exited the loop with an error different from NOTFOUND
				return err
			}

			// bump version
			if err := b.setVersion(txn, 5); err != nil {
				return err
			}
		}

		return nil
	})
}

func (b *LMDBBackend) setVersion(txn *lmdb.Txn, version uint16) error {
	buf, err := txn.PutReserve(b.settingsStore, []byte{DB_VERSION}, 4, 0)
	binary.BigEndian.PutUint16(buf, version)
	return err
}
