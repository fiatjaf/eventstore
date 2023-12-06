package lmdb

import (
	"encoding/binary"
	"fmt"
	"log"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/fiatjaf/eventstore"
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
					if err := txn.Put(b.indexTag32, key, val, 0); err != nil {
						return err
					}
					if err := txn.Del(b.indexTag, key, val); err != nil {
						return err
					}
				}

				// next -- will end on err
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
			log.Println("migration 2: use just 8 bytes for pubkeys and ids instead of 32 bytes")
			// rewrite all keys from indexTag32, indexId, indexPubkey and indexPubkeyKind
			for _, dbi := range []lmdb.DBI{b.indexTag32, b.indexId, b.indexPubkey, b.indexPubkeyKind} {
				cursor, err := txn.OpenCursor(dbi)
				if err != nil {
					return fmt.Errorf("failed to open cursor in migration 2: %w", err)
				}
				defer cursor.Close()
				key, val, err := cursor.Get(nil, nil, lmdb.First)
				for err == nil {
					if err := txn.Del(dbi, key, val); err != nil {
						return err
					}
					oldkey := fmt.Sprintf("%x", key)

					// these keys are always 32 bytes of an id or pubkey, then something afterwards, doesn't matter
					// so we just keep 8 bytes and overwrite the rest
					if len(key) > 32 {
						copy(key[8:], key[32:])
						key = key[0 : len(key)-24]
						if err := txn.Put(dbi, key, val, 0); err != nil {
							return err
						}
						log.Printf("moved key %s:%x to %x:%x", oldkey, val, key, val)
					}

					// next -- will end on err
					key, val, err = cursor.Get(nil, nil, lmdb.Next)
				}
				if lmdbErr, ok := err.(*lmdb.OpError); ok && lmdbErr.Errno != lmdb.NotFound {
					// exited the loop with an error different from NOTFOUND
					return err
				}
			}

			// bump version
			if err := b.bumpVersion(txn, 2); err != nil {
				return err
			}
		}

		if version < 3 {
			log.Println("migration 3: move all keys from indexTag to indexTagAddr if they are like 'a' tags")
			cursor, err := txn.OpenCursor(b.indexTag)
			if err != nil {
				return fmt.Errorf("failed to open cursor in migration 2: %w", err)
			}
			defer cursor.Close()

			key, val, err := cursor.Get(nil, nil, lmdb.First)
			for err == nil {
				if kind, pkb, d := eventstore.GetAddrTagElements(string(key[1 : len(key)-4])); len(pkb) == 32 {
					// it's an 'a' tag or alike
					if err := txn.Del(b.indexTag, key, val); err != nil {
						return err
					}

					k := make([]byte, 2+8+len(d)+4)
					binary.BigEndian.PutUint16(k[1:], kind)
					copy(k[2:], pkb[0:8]) // use only the first 8 bytes of the public key in the index
					copy(k[2+8:], d)
					copy(k[2+8+len(d):], key[len(key)-4:])
					if err := txn.Put(b.indexTagAddr, k, val, 0); err != nil {
						return err
					}
					log.Printf("moved key %x:%x to %x:%x", key, val, k, val)
				}

				// next -- will end on err
				key, val, err = cursor.Get(nil, nil, lmdb.Next)
			}
			if lmdbErr, ok := err.(*lmdb.OpError); ok && lmdbErr.Errno != lmdb.NotFound {
				// exited the loop with an error different from NOTFOUND
				return err
			}

			// bump version
			if err := b.bumpVersion(txn, 3); err != nil {
				return err
			}
		}

		if version < 4 {
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
