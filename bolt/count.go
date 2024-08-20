package bolt

import (
	"bytes"
	"context"
	"encoding/binary"
	"log"

	"github.com/nbd-wtf/go-nostr"
	nostr_binary "github.com/nbd-wtf/go-nostr/binary"
	bolt "go.etcd.io/bbolt"
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

			key, _ := c.Seek(q.startingPoint)
			if key == nil {
				key, _ = c.Last()
			} else {
				key, _ = c.Prev()
			}

			for ; key != nil && bytes.HasPrefix(key, q.prefix); key, _ = c.Prev() {
				idxOffset := len(key) - 4 // this is where the idx actually starts

				// "id" indexes don't contain a timestamp
				if !q.skipTimestamp {
					createdAt := binary.BigEndian.Uint32(key[idxOffset-4 : idxOffset])
					if createdAt < since {
						break
					}
				}

				if extraFilter == nil {
					count++
				} else {
					// fetch actual event
					val := raw.Get(key[len(key)-4:])
					evt := &nostr.Event{}
					if err := nostr_binary.Unmarshal(val, evt); err != nil {
						log.Printf("bolt: value read error (id %x): %s\n", val[0:32], err)
						break
					}

					// check if this matches the other filters that were not part of the index before yielding
					if extraFilter.Matches(evt) {
						count++
					}
				}
			}
		}

		return nil
	})

	return count, err
}
