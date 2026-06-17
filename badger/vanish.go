package badger

import (
	"context"
	"encoding/binary"
	"encoding/hex"

	"github.com/dgraph-io/badger/v4"
	bin "github.com/fiatjaf/eventstore/internal/binary"
	"github.com/nbd-wtf/go-nostr"
)

// kindRequestToVanish is the NIP-62 "request to vanish" event kind. These
// events are kept for bookkeeping instead of being deleted.
const kindRequestToVanish = 62

func (b *BadgerBackend) VanishPubkey(ctx context.Context, pubkey string, until int64) error {
	pubkeyPrefix8, err := hex.DecodeString(pubkey[0 : 8*2])
	if err != nil {
		return err
	}
	prefix := make([]byte, 1+8)
	prefix[0] = indexPubkeyPrefix
	copy(prefix[1:], pubkeyPrefix8)

	// iterate over a consistent read snapshot and stream the deletes through a
	// WriteBatch so we don't blow past the per-transaction size limit on pubkeys
	// with a lot of events
	wb := b.NewWriteBatch()
	err = b.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
			Prefix:         prefix,
		})
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			key := it.Item().Key()

			// Key format: [prefix(1)][pubkey_prefix8(8)][created_at(4)][idx(4)]
			if len(key) < 1+8+4+4 {
				continue
			}

			// Extract created_at timestamp (stored as uint32)
			createdAt := int64(binary.BigEndian.Uint32(key[1+8 : 1+8+4]))
			if createdAt > until {
				continue
			}

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
			if err := eventItem.Value(func(val []byte) error {
				return bin.Unmarshal(val, &evt)
			}); err != nil {
				continue
			}

			// the prefix match might have false positives, so verify the pubkey
			if evt.PubKey != pubkey {
				continue
			}

			// vanish requests themselves are kept for bookkeeping
			if evt.Kind == kindRequestToVanish {
				continue
			}

			// delete all index keys for this event plus the raw event
			for k := range b.getIndexKeysForEvent(&evt, idx[1:]) {
				if err := wb.Delete(k); err != nil {
					return err
				}
			}
			if err := wb.Delete(idx); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		wb.Cancel()
		return err
	}

	return wb.Flush()
}
