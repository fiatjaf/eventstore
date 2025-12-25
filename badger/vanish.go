package badger

import (
	"context"
	"encoding/binary"
	"encoding/hex"

	"github.com/dgraph-io/badger/v4"
	bin "github.com/fiatjaf/eventstore/internal/binary"
	"github.com/nbd-wtf/go-nostr"
)

func (b *BadgerBackend) VanishPubkey(ctx context.Context, pubkey string, until int64) error {
	return b.Update(func(txn *badger.Txn) error {
		pubkeyPrefix8, _ := hex.DecodeString(pubkey[0 : 8*2])
		prefix := make([]byte, 1+8)
		prefix[0] = indexPubkeyPrefix
		copy(prefix[1:], pubkeyPrefix8)

		opts := badger.IteratorOptions{
			PrefetchValues: false,
			Prefix:         prefix,
		}
		it := txn.NewIterator(opts)
		defer it.Close()

		toDelete := make([][]byte, 0, 1000)
		
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			
			// Key format: [prefix(1)][pubkey_prefix8(8)][created_at(4)][idx(4)]
			if len(key) < 1+8+4+4 {
				continue
			}
			
			// Extract created_at timestamp (stored as uint32)
			createdAt := int64(binary.BigEndian.Uint32(key[1+8 : 1+8+4]))
			
			if createdAt <= until {
				// Extract idx
				idx := make([]byte, 1+4)
				idx[0] = rawEventStorePrefix
				copy(idx[1:], key[1+8+4:])
				
				// Get the event to find all its index keys
				eventItem, err := txn.Get(idx)
				if err != nil {
					continue
				}
				
				var evt nostr.Event
				err = eventItem.Value(func(val []byte) error {
					return bin.Unmarshal(val, &evt)
				})
				if err != nil {
					continue
				}
				
				// Verify this is the correct pubkey (prefix match might have false positives)
				if evt.PubKey != pubkey {
					continue
				}
				
				// Skip kind 62 events (vanish requests should be kept for bookkeeping)
				if evt.Kind == 62 {
					continue
				}
				
				// Collect all index keys for this event
				for k := range b.getIndexKeysForEvent(&evt, idx[1:]) {
					toDelete = append(toDelete, k)
				}
				
				// Add raw event key
				toDelete = append(toDelete, idx)
			}
		}
		
		// Delete all collected keys
		for _, key := range toDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		
		return nil
	})
}
