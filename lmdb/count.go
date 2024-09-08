package lmdb

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/PowerDNS/lmdb-go/lmdb"
	bin "github.com/fiatjaf/eventstore/internal/binary"
	"github.com/nbd-wtf/go-nostr"
)

func (b *LMDBBackend) CountEvents(ctx context.Context, filter nostr.Filter) (int64, error) {
	var count int64 = 0

	queries, extraFilter, since, err := b.prepareQueries(filter)
	if err != nil {
		return 0, err
	}

	err = b.lmdbEnv.View(func(txn *lmdb.Txn) error {
		// actually iterate
		for _, q := range queries {
			cursor, err := txn.OpenCursor(q.dbi)
			if err != nil {
				continue
			}

			var k []byte
			var idx []byte
			var iterr error

			if _, _, errsr := cursor.Get(q.startingPoint, nil, lmdb.SetRange); errsr != nil {
				if operr, ok := errsr.(*lmdb.OpError); !ok || operr.Errno != lmdb.NotFound {
					// in this case it's really an error
					panic(operr)
				} else {
					// we're at the end and we just want notes before this,
					// so we just need to set the cursor the last key, this is not a real error
					k, idx, iterr = cursor.Get(nil, nil, lmdb.Last)
				}
			} else {
				// move one back as the first step
				k, idx, iterr = cursor.Get(nil, nil, lmdb.Prev)
			}

			for {
				// we already have a k and a v and an err from the cursor setup, so check and use these
				if iterr != nil ||
					len(k) != q.prefixSize+q.timestampSize ||
					!bytes.Equal(k[:q.prefixSize], q.prefix) {
					// either iteration has errored or we reached the end of this prefix
					break // stop this cursor and move to the next one
				}

				// "id" indexes don't contain a timestamp
				if q.timestampSize == 4 {
					createdAt := binary.BigEndian.Uint32(k[len(k)-4:])
					if createdAt < since {
						break
					}
				}

				if extraFilter == nil {
					count++
				} else {
					// fetch actual event
					val, err := txn.Get(b.rawEventStore, idx)
					if err != nil {
						panic(err)
					}

					evt := &nostr.Event{}
					if err := bin.Unmarshal(val, evt); err != nil {
						return err
					}

					// check if this matches the other filters that were not part of the index
					if extraFilter.Matches(evt) {
						count++
					}

					return nil
				}

				// move one back (we'll look into k and v and err in the next iteration)
				k, idx, iterr = cursor.Get(nil, nil, lmdb.Prev)
			}
		}

		return nil
	})

	return count, err
}
