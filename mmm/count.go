package mmm

import (
	"bytes"
	"context"
	"encoding/binary"
	"slices"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/fiatjaf/eventstore/mmm/betterbinary"
	"github.com/nbd-wtf/go-nostr"
)

func (il *IndexingLayer) CountEvents(ctx context.Context, filter nostr.Filter) (int64, error) {
	var count int64 = 0

	queries, extraAuthors, extraKinds, extraTagKey, extraTagValues, since, err := il.prepareQueries(filter)
	if err != nil {
		return 0, err
	}

	err = il.lmdbEnv.View(func(txn *lmdb.Txn) error {
		// actually iterate
		for _, q := range queries {
			cursor, err := txn.OpenCursor(q.dbi)
			if err != nil {
				continue
			}

			it := &iterator{cursor: cursor}
			it.seek(q.startingPoint)

			for {
				// we already have a k and a v and an err from the cursor setup, so check and use these
				if it.err != nil ||
					len(it.key) != q.keySize ||
					!bytes.HasPrefix(it.key, q.prefix) {
					// either iteration has errored or we reached the end of this prefix
					break // stop this cursor and move to the next one
				}

				// "id" indexes don't contain a timestamp
				if q.timestampSize == 4 {
					createdAt := binary.BigEndian.Uint32(it.key[len(it.key)-4:])
					if createdAt < since {
						break
					}
				}

				if extraAuthors == nil && extraKinds == nil && extraTagValues == nil {
					count++
				} else {
					// fetch actual event
					pos := positionFromBytes(it.posb)
					bin := il.mmmm.mmapf[pos.start : pos.start+uint64(pos.size)]

					// check it against pubkeys without decoding the entire thing
					if extraAuthors != nil && !slices.Contains(extraAuthors, [32]byte(bin[39:71])) {
						it.next()
						continue
					}

					// check it against kinds without decoding the entire thing
					if extraKinds != nil && !slices.Contains(extraKinds, [2]byte(bin[1:3])) {
						it.next()
						continue
					}

					// decode the entire thing (TODO: do a conditional decode while also checking the extra tag)
					event := &nostr.Event{}
					if err := betterbinary.Unmarshal(bin, event); err != nil {
						it.next()
						continue
					}

					// if there is still a tag to be checked, do it now
					if !event.Tags.ContainsAny(extraTagKey, extraTagValues) {
						it.next()
						continue
					}

					count++
				}
			}
		}

		return nil
	})

	return count, err
}
