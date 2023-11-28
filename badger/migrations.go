package badger

import (
	"encoding/binary"
	"log"

	"github.com/dgraph-io/badger/v4"
)

func (b *BadgerBackend) runMigrations() error {
	return b.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte{dbVersionKey})
		if err != nil {
			return err
		}
		item.Value(func(val []byte) error {
			var version uint16

			// do the migrations in increasing steps (there is no rollback)
			//

			if version < 1 {
				log.Println("migration 1: move all keys from indexTag to indexTag32 if they are 32-bytes")
				prefix := []byte{indexTagPrefix}
				it := txn.NewIterator(badger.IteratorOptions{
					PrefetchValues: true,
					PrefetchSize:   100,
					Prefix:         prefix,
				})
				defer it.Close()

				for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
					item := it.Item()
					key := item.Key()

					if len(key) == 1+32+2+4 {
						// it's 32 bytes
						log.Printf("moving key %x", key)
						if err := txn.Delete(key); err != nil {
							return err
						}
						key[0] = indexTag32Prefix
						txn.Set(key, nil)
					}
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

		return nil
	})
}

func (b *BadgerBackend) bumpVersion(txn *badger.Txn, version uint16) error {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, version)
	return txn.Set([]byte{dbVersionKey}, buf)
}
