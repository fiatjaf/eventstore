package badger

import (
	"encoding/binary"
	"log"

	"github.com/dgraph-io/badger/v4"
)

func (b *BadgerBackend) runMigrations() error {
	return b.Update(func(txn *badger.Txn) error {
		var version uint16

		item, err := txn.Get([]byte{dbVersionKey})
		if err == badger.ErrKeyNotFound {
			version = 0
		} else if err != nil {
			return err
		} else {
			item.Value(func(val []byte) error {
				version = binary.BigEndian.Uint16(val)
				return nil
			})
		}

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

				if len(key) == 1+32+4+4 {
					// it's 32 bytes
					log.Printf("moving key %x", key)
					if err := txn.Delete(key); err != nil {
						return err
					}
					key[0] = indexTag32Prefix
					if err := txn.Set(key, nil); err != nil {
						return err
					}
				}
			}

			// bump version
			version = 1
			if err := b.bumpVersion(txn, 1); err != nil {
				return err
			}
		}

		if version < 2 {
			log.Println("migration 2: move all keys from indexTag to indexTagAddr if they are like 'a' tags")
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

				if kind, pkb, d := getAddrTagElements(string(key[1 : len(key)-4-4])); len(pkb) == 32 {
					// it's an 'a' tag or alike
					if err := txn.Delete(key); err != nil {
						return err
					}
					k := make([]byte, 1+2+32+len(d)+4+4)
					k[0] = indexTagAddrPrefix
					binary.BigEndian.PutUint16(k[1:], kind)
					copy(k[1+2:], pkb)
					copy(k[1+2+32:], d)
					copy(k[1+2+32+len(d):], key[len(key)-4-4:])
					if err := txn.Set(k, nil); err != nil {
						return err
					}
					log.Printf("moved key %x to %x", key, k)
				}
			}

			// bump version
			version = 2
			if err := b.bumpVersion(txn, 2); err != nil {
				return err
			}
		}

		if version < 3 {
			// ...
		}

		return nil
	})
}

func (b *BadgerBackend) bumpVersion(txn *badger.Txn, version uint16) error {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, version)
	return txn.Set([]byte{dbVersionKey}, buf)
}
