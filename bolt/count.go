package bolt

import (
	"bytes"
	"context"
	"encoding/binary"
	"log"

	"github.com/boltdb/bolt"
	"github.com/nbd-wtf/go-nostr"
	nostr_binary "github.com/nbd-wtf/go-nostr/binary"
)

func (b *BoltBackend) CountEvents(ctx context.Context, filter nostr.Filter) (int64, error) {
	var count int64 = 0

	queries, extraFilter, since, err := prepareQueries(filter)
	if err != nil {
		return 0, err
	}

	err = b.db.View(func(txn *bolt.Tx) error {
		// actually iterate
		for _, q := range queries {
			bucket := txn.Bucket(q.bucket)
			raw := txn.Bucket(bucketRaw)

			c := bucket.Cursor()
			for k, v := c.Seek(q.startingPoint); k != nil && bytes.HasPrefix(k, q.prefix); k, v = c.Prev() {
				// "id" indexes don't contain a timestamp
				if !q.skipTimestamp {
					createdAt := binary.BigEndian.Uint32(k[len(k)-4:])
					if createdAt < since {
						break
					}
				}

				// fetch actual event
				val := raw.Get(v)
				evt := &nostr.Event{}
				if err := nostr_binary.Unmarshal(val, evt); err != nil {
					log.Printf("bolt: value read error (id %x): %s\n", val[0:32], err)
					break
				}

				// check if this matches the other filters that were not part of the index before yielding
				if extraFilter == nil || extraFilter.Matches(evt) {
					count++
				}
			}
		}

		return nil
	})

	return count, err
}
