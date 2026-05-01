package badger

import (
	"encoding/binary"
	"fmt"
	"log"

	"github.com/dgraph-io/badger/v4"
	bin "github.com/fiatjaf/eventstore/internal/binary"
	"github.com/nbd-wtf/go-nostr"
)

func (b *BadgerBackend) runMigrations() error {
	var version uint16
	if err := b.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte{dbVersionKey})
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			version = binary.BigEndian.Uint16(val)
			return nil
		})
	}); err != nil {
		return err
	}

	// do the migrations in increasing steps (there is no rollback)
	//

	// the 5 first migrations go to trash because on version 5 we need to export and import all the data anyway
	if version < 5 {
		log.Println("[badger] migration 5: delete all indexes and recreate them")

		// delete all index entries (in a WriteBatch so we don't blow past the txn size limit)
		prefixes := []byte{
			indexIdPrefix,
			indexCreatedAtPrefix,
			indexKindPrefix,
			indexPubkeyPrefix,
			indexPubkeyKindPrefix,
			indexTagPrefix,
			indexTag32Prefix,
			indexTagAddrPrefix,
		}

		wb := b.NewWriteBatch()
		for _, prefix := range prefixes {
			err := b.View(func(txn *badger.Txn) error {
				it := txn.NewIterator(badger.IteratorOptions{
					PrefetchValues: false,
					Prefix:         []byte{prefix},
				})
				defer it.Close()

				for it.Seek([]byte{prefix}); it.ValidForPrefix([]byte{prefix}); it.Next() {
					key := it.Item().KeyCopy(nil)
					if err := wb.Delete(key); err != nil {
						return fmt.Errorf("failed to delete index key %x: %w", key, err)
					}
				}
				return nil
			})
			if err != nil {
				wb.Cancel()
				return err
			}
		}
		if err := wb.Flush(); err != nil {
			return fmt.Errorf("failed to flush index deletions on migration 5: %w", err)
		}

		// recreate indexes (also in a WriteBatch)
		wb = b.NewWriteBatch()
		err := b.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.IteratorOptions{
				PrefetchValues: true,
				Prefix:         []byte{rawEventStorePrefix},
			})
			defer it.Close()

			for it.Seek([]byte{rawEventStorePrefix}); it.ValidForPrefix([]byte{rawEventStorePrefix}); it.Next() {
				item := it.Item()
				idx := item.KeyCopy(nil)

				err := item.Value(func(val []byte) error {
					evt := &nostr.Event{}
					if err := bin.Unmarshal(val, evt); err != nil {
						return fmt.Errorf("error decoding event %x on migration 5: %w", idx, err)
					}

					for key := range b.getIndexKeysForEvent(evt, idx[1:]) {
						if err := wb.Set(key, nil); err != nil {
							return fmt.Errorf("failed to save index for event %s on migration 5: %w", evt.ID, err)
						}
					}

					return nil
				})
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			wb.Cancel()
			return err
		}
		if err := wb.Flush(); err != nil {
			return fmt.Errorf("failed to flush index creation on migration 5: %w", err)
		}

		// bump version
		if err := b.Update(func(txn *badger.Txn) error {
			return b.bumpVersion(txn, 5)
		}); err != nil {
			return err
		}
	}

	return nil
}

func (b *BadgerBackend) bumpVersion(txn *badger.Txn, version uint16) error {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, version)
	return txn.Set([]byte{dbVersionKey}, buf)
}
