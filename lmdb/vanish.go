package lmdb

import (
	"context"
	"encoding/binary"
	"encoding/hex"

	"github.com/PowerDNS/lmdb-go/lmdb"
	bin "github.com/fiatjaf/eventstore/internal/binary"
	"github.com/nbd-wtf/go-nostr"
)

// kindRequestToVanish is the NIP-62 "request to vanish" event kind. These
// events are kept for bookkeeping instead of being deleted.
const kindRequestToVanish = 62

func (b *LMDBBackend) VanishPubkey(ctx context.Context, pubkey string, until int64) error {
	return b.lmdbEnv.Update(func(txn *lmdb.Txn) error {
		prefix := make([]byte, 8)
		if _, err := hex.Decode(prefix[0:8], []byte(pubkey[0:8*2])); err != nil {
			return err
		}

		cursor, err := txn.OpenCursor(b.indexPubkey)
		if err != nil {
			return err
		}

		// collect the matching events first: deleting from b.indexPubkey -- the
		// dbi we are iterating over -- mid-cursor would corrupt the iteration and
		// leave entries behind
		var toDelete []*nostr.Event
		key, idx, err := cursor.Get(prefix, nil, lmdb.SetRange)
		for err == nil {
			select {
			case <-ctx.Done():
				cursor.Close()
				return ctx.Err()
			default:
			}

			// stop once we leave this pubkey's range (first 8 bytes)
			if len(key) < 8 || string(key[:8]) != string(prefix) {
				break
			}

			// Key format: [pubkey_prefix8(8)][created_at(4)]
			if len(key) >= 8+4 {
				createdAt := int64(binary.BigEndian.Uint32(key[8 : 8+4]))
				if createdAt <= until {
					if eventBytes, gerr := txn.Get(b.rawEventStore, idx); gerr == nil {
						evt := &nostr.Event{}
						if uerr := bin.Unmarshal(eventBytes, evt); uerr == nil {
							// the prefix match might have false positives, and vanish
							// requests themselves are kept for bookkeeping
							if evt.PubKey == pubkey && evt.Kind != kindRequestToVanish {
								toDelete = append(toDelete, evt)
							}
						}
					}
				}
			}

			key, idx, err = cursor.Get(nil, nil, lmdb.Next)
		}
		cursor.Close()

		if err != nil && !lmdb.IsNotFound(err) {
			return err
		}

		for _, evt := range toDelete {
			if err := b.delete(txn, evt); err != nil {
				return err
			}
		}

		return nil
	})
}
