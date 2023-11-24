package lmdb

import (
	"encoding/binary"
	"fmt"
	"log"

	"github.com/bmatsuo/lmdb-go/lmdb"
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

		// do the migrations in increasing steps (there is no rollback)
		//

		if version < 1 {
			log.Println("migration 1: move all keys from indexTag to indexTag32 if they are 32-bytes")
			cursor, err := txn.OpenCursor(b.indexTag)
			if err != nil {
				return fmt.Errorf("failed to open cursor in migration 1: %w", err)
			}
			defer cursor.Close()

			key, val, err := cursor.Get(nil, nil, lmdb.First)
			for err == nil {
				if len(key)-4 /* uint32 created_at */ == 32 {
					log.Printf("moving key %x->%x", key, val)
					txn.Put(b.indexTag32, key, val, 0)
					txn.Del(b.indexTag, key, val)
				}

				// next
				key, val, err = cursor.Get(nil, nil, lmdb.Next)
			}
			if lmdbErr, ok := err.(*lmdb.OpError); ok && lmdbErr.Errno != lmdb.NotFound {
				// exited the loop with an error different from NOTFOUND
				return err
			}

			// bump version
			if err := b.bumpVersion(txn, 1); err != nil {
				return err
			}
		}

		if version < 2 {
			// ...
		}

		return nil
	})
}

func (b *LMDBBackend) bumpVersion(txn *lmdb.Txn, version uint16) error {
	buf, err := txn.PutReserve(b.settingsStore, []byte{DB_VERSION}, 4, 0)
	binary.BigEndian.PutUint16(buf, version)
	return err
}
