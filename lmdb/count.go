package lmdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"

	"github.com/PowerDNS/lmdb-go/lmdb"
	bin "github.com/fiatjaf/eventstore/internal/binary"
	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip45"
	"github.com/nbd-wtf/go-nostr/nip45/hyperloglog"
	"golang.org/x/exp/slices"
)

func (b *LMDBBackend) CountEvents(ctx context.Context, filter nostr.Filter) (int64, error) {
	var count int64 = 0

	queries, extraAuthors, extraKinds, extraTagKey, extraTagValues, since, err := b.prepareQueries(filter)
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
					len(k) != q.keySize ||
					!bytes.HasPrefix(k, q.prefix) {
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

				if extraAuthors == nil && extraKinds == nil && extraTagValues == nil {
					count++
				} else {
					// fetch actual event
					val, err := txn.Get(b.rawEventStore, idx)
					if err != nil {
						panic(err)
					}

					// check it against pubkeys without decoding the entire thing
					if !slices.Contains(extraAuthors, [32]byte(val[32:64])) {
						goto loopend
					}

					// check it against kinds without decoding the entire thing
					if !slices.Contains(extraKinds, [2]byte(val[132:134])) {
						goto loopend
					}

					evt := &nostr.Event{}
					if err := bin.Unmarshal(val, evt); err != nil {
						goto loopend
					}

					// if there is still a tag to be checked, do it now
					if !evt.Tags.ContainsAny(extraTagKey, extraTagValues) {
						goto loopend
					}

					count++
				}

				// move one back (we'll look into k and v and err in the next iteration)
			loopend:
				k, idx, iterr = cursor.Get(nil, nil, lmdb.Prev)
			}
		}

		return nil
	})

	return count, err
}

// CountEventsHLL is like CountEvents, but it will build a hyperloglog value while iterating through results, following NIP-45
func (b *LMDBBackend) CountEventsHLL(ctx context.Context, filter nostr.Filter, offset int) (int64, *hyperloglog.HyperLogLog, error) {
	if useCache, _ := b.EnableHLLCacheFor(filter.Kinds[0]); useCache {
		return b.countEventsHLLCached(filter)
	}

	var count int64 = 0

	// this is different than CountEvents because some of these extra checks are not applicable in HLL-valid filters
	queries, _, extraKinds, _, _, since, err := b.prepareQueries(filter)
	if err != nil {
		return 0, nil, err
	}

	hll := hyperloglog.New(offset)

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
					len(k) != q.keySize ||
					!bytes.HasPrefix(k, q.prefix) {
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

				// fetch actual event (we need it regardless because we need the pubkey for the hll)
				val, err := txn.Get(b.rawEventStore, idx)
				if err != nil {
					panic(err)
				}

				if extraKinds == nil {
					// nothing extra to check
					count++
					hll.AddBytes(val[32:64])
				} else {
					// check it against kinds without decoding the entire thing
					if !slices.Contains(extraKinds, [2]byte(val[132:134])) {
						goto loopend
					}

					evt := &nostr.Event{}
					if err := bin.Unmarshal(val, evt); err != nil {
						goto loopend
					}

					count++
					hll.Add(evt.PubKey)
				}

				// move one back (we'll look into k and v and err in the next iteration)
			loopend:
				k, idx, iterr = cursor.Get(nil, nil, lmdb.Prev)
			}
		}

		return nil
	})

	return count, hll, err
}

// countEventsHLLCached will just return a cached value from disk (and presumably we don't even have the events required to compute this anymore).
func (b *LMDBBackend) countEventsHLLCached(filter nostr.Filter) (int64, *hyperloglog.HyperLogLog, error) {
	cacheKey := make([]byte, 2+8)
	binary.BigEndian.PutUint16(cacheKey[0:2], uint16(filter.Kinds[0]))
	switch filter.Kinds[0] {
	case 3:
		hex.Decode(cacheKey[2:2+8], []byte(filter.Tags["p"][0][0:8*2]))
	case 7:
		hex.Decode(cacheKey[2:2+8], []byte(filter.Tags["e"][0][0:8*2]))
	}

	var count int64
	var hll *hyperloglog.HyperLogLog

	err := b.lmdbEnv.View(func(txn *lmdb.Txn) error {
		val, err := txn.Get(b.hllCache, cacheKey)
		if err != nil {
			if lmdb.IsNotFound(err) {
				return nil
			}
			return err
		}
		hll = hyperloglog.NewWithRegisters(val, 0) // offset doesn't matter here
		count = int64(hll.Count())
		return nil
	})

	return count, hll, err
}

func (b *LMDBBackend) updateHyperLogLogCachedValues(txn *lmdb.Txn, evt *nostr.Event) error {
	cacheKey := make([]byte, 2+8)
	binary.BigEndian.PutUint16(cacheKey[0:2], uint16(evt.Kind))

	for ref, offset := range nip45.HyperLogLogEventPubkeyOffsetsAndReferencesForEvent(evt) {
		// setup cache key (reusing buffer)
		hex.Decode(cacheKey[2:2+8], []byte(ref[0:8*2]))

		// fetch hll value from cache db
		hll := hyperloglog.New(offset)
		val, err := txn.Get(b.hllCache, cacheKey)
		if err == nil {
			hll.SetRegisters(val)
		} else if !lmdb.IsNotFound(err) {
			return err
		}

		// add this event
		hll.Add(evt.PubKey)

		// save values back again
		if err := txn.Put(b.hllCache, cacheKey, hll.GetRegisters(), 0); err != nil {
			return err
		}
	}

	return nil
}
