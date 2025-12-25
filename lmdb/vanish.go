package lmdb

import (
	"context"
	"encoding/binary"
	"encoding/hex"

	"github.com/PowerDNS/lmdb-go/lmdb"
	bin "github.com/fiatjaf/eventstore/internal/binary"
	"github.com/nbd-wtf/go-nostr"
)

func (b *LMDBBackend) VanishPubkey(ctx context.Context, pubkey string, until int64) error {
	return b.lmdbEnv.Update(func(txn *lmdb.Txn) error {
		prefix := make([]byte, 8)
		hex.Decode(prefix[0:8], []byte(pubkey[0:8*2]))
		
		cursor, err := txn.OpenCursor(b.indexPubkey)
		if err != nil {
			return err
		}
		defer cursor.Close()
		
		// Seek to the first key with this pubkey
		key, idx, err := cursor.Get(prefix, nil, lmdb.SetRange)
		for err == nil {
			// Check if we're still in the pubkey range (first 8 bytes)
			if len(key) < 8 || string(key[:8]) != string(prefix) {
				break
			}
			
			// Key format: [pubkey_prefix8(8)][created_at(4)]
			if len(key) >= 8+4 {
				createdAt := int64(binary.BigEndian.Uint32(key[8 : 8+4]))
				
				if createdAt <= until {
					// Get the event
					eventBytes, err := txn.Get(b.rawEventStore, idx)
					if err == nil {
						var evt nostr.Event
						if err := bin.Unmarshal(eventBytes, &evt); err == nil {
							// Verify this is the correct pubkey (prefix match might have false positives)
							if evt.PubKey != pubkey {
								key, idx, err = cursor.Get(nil, nil, lmdb.Next)
								continue
							}
							
							// Delete index entries
							for k := range b.getIndexKeysForEvent(&evt) {
								if err := txn.Del(k.dbi, k.key, idx); err != nil {
									return err
								}
							}
							
							// Delete raw event
							if err := txn.Del(b.rawEventStore, idx, nil); err != nil {
								return err
							}
						}
					}
				}
			}
			
			key, idx, err = cursor.Get(nil, nil, lmdb.Next)
		}
		
		if err != nil && !lmdb.IsNotFound(err) {
			return err
		}
		
		return nil
	})
}
