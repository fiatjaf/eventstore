package lmdb

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"

	"github.com/PowerDNS/lmdb-go/lmdb"
	bin "github.com/fiatjaf/eventstore/internal/binary"
	"github.com/nbd-wtf/go-nostr"
)

type query struct {
	i             int
	dbi           lmdb.DBI
	prefix        []byte
	results       chan *nostr.Event
	prefixSize    int
	timestampSize int
	startingPoint []byte
}

type queryEvent struct {
	*nostr.Event
	query int
}

func (b *LMDBBackend) QueryEvents(ctx context.Context, filter nostr.Filter) (chan *nostr.Event, error) {
	ch := make(chan *nostr.Event)

	queries, extraFilter, since, err := b.prepareQueries(filter)
	if err != nil {
		return nil, err
	}

	// max number of events we'll return
	limit := b.MaxLimit / 4
	if filter.Limit > 0 && filter.Limit < b.MaxLimit {
		limit = filter.Limit
	}

	if filter.Search != "" {
		close(ch)
		return ch, nil
	}

	go func() {
		defer close(ch)

		for _, q := range queries {
			q := q

			pulled := 0 // this will be hard-capped at the global limit of the query

			go b.lmdbEnv.View(func(txn *lmdb.Txn) error {
				txn.RawRead = true
				defer close(q.results)

				cursor, err := txn.OpenCursor(q.dbi)
				if err != nil {
					return err
				}
				defer cursor.Close()

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
						return nil
					}

					// "id" indexes don't contain a timestamp
					if q.timestampSize == 4 {
						createdAt := binary.BigEndian.Uint32(k[len(k)-4:])
						if createdAt < since {
							return nil
						}
					}

					// fetch actual event
					val, err := txn.Get(b.rawEventStore, idx)
					if err != nil {
						log.Printf(
							"lmdb: failed to get %x based on prefix %x, index key %x from raw event store: %s\n",
							idx, q.prefix, k, err)
						return fmt.Errorf("error: %w", err)
					}

					evt := &nostr.Event{}
					if err := bin.Unmarshal(val, evt); err != nil {
						log.Printf("lmdb: value read error (id %x) on query prefix %x sp %x dbi %d: %s\n", val[0:32], q.prefix, q.startingPoint, q.dbi, err)
						return fmt.Errorf("error: %w", err)
					}

					// check if this matches the other filters that were not part of the index before yielding
					if extraFilter == nil || extraFilter.Matches(evt) {
						select {
						case q.results <- evt:
							pulled++
							if pulled >= limit {
								return nil
							}
						case <-ctx.Done():
							return nil
						}
					}

					// move one back (we'll look into k and v and err in the next iteration)
					k, idx, iterr = cursor.Get(nil, nil, lmdb.Prev)
				}
			})
		}
		if err != nil {
			log.Printf("lmdb: error on cursor iteration: %v\n", err)
		}

		// receive results and ensure we only return the most recent ones always
		emittedEvents := 0

		// first pass
		emitQueue := make(priorityQueue, 0, len(queries))
		for _, q := range queries {
			evt, ok := <-q.results
			if ok {
				emitQueue = append(emitQueue, &queryEvent{Event: evt, query: q.i})
			}
		}

		// queue may be empty here if we have literally nothing
		if len(emitQueue) == 0 {
			return
		}

		heap.Init(&emitQueue)

		// iterate until we've emitted all events required
		for {
			// emit latest event in queue
			latest := emitQueue[0]
			select {
			case ch <- latest.Event:
			case <-ctx.Done():
				return
			}

			// stop when reaching limit
			emittedEvents++
			if emittedEvents >= limit {
				break
			}

			// fetch a new one from query results and replace the previous one with it
			if evt, ok := <-queries[latest.query].results; ok {
				emitQueue[0].Event = evt
				heap.Fix(&emitQueue, 0)
			} else {
				// if this query has no more events we just remove this and proceed normally
				heap.Remove(&emitQueue, 0)

				// check if the list is empty and end
				if len(emitQueue) == 0 {
					break
				}
			}
		}
	}()

	return ch, nil
}

type priorityQueue []*queryEvent

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].CreatedAt > pq[j].CreatedAt
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *priorityQueue) Push(x any) {
	item := x.(*queryEvent)
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*pq = old[0 : n-1]
	return item
}

func (b *LMDBBackend) prepareQueries(filter nostr.Filter) (
	queries []query,
	extraFilter *nostr.Filter,
	since uint32,
	err error,
) {
	if len(filter.IDs) > 0 {
		queries = make([]query, len(filter.IDs))
		for i, idHex := range filter.IDs {
			if len(idHex) != 64 {
				return nil, nil, 0, fmt.Errorf("invalid id '%s'", idHex)
			}
			prefix, _ := hex.DecodeString(idHex[0 : 8*2])
			queries[i] = query{i: i, dbi: b.indexId, prefix: prefix, prefixSize: 8, timestampSize: 0}
		}
	} else if len(filter.Authors) > 0 {
		if len(filter.Kinds) == 0 {
			queries = make([]query, len(filter.Authors))
			for i, pubkeyHex := range filter.Authors {
				if len(pubkeyHex) != 64 {
					return nil, nil, 0, fmt.Errorf("invalid pubkey '%s'", pubkeyHex)
				}
				prefix, _ := hex.DecodeString(pubkeyHex[0 : 8*2])
				queries[i] = query{i: i, dbi: b.indexPubkey, prefix: prefix, prefixSize: 8, timestampSize: 4}
			}
		} else {
			queries = make([]query, len(filter.Authors)*len(filter.Kinds))
			i := 0
			for _, pubkeyHex := range filter.Authors {
				for _, kind := range filter.Kinds {
					if len(pubkeyHex) != 64 {
						return nil, nil, 0, fmt.Errorf("invalid pubkey '%s'", pubkeyHex)
					}
					pubkey, _ := hex.DecodeString(pubkeyHex[0 : 8*2])
					prefix := binary.BigEndian.AppendUint16(pubkey, uint16(kind))
					queries[i] = query{i: i, dbi: b.indexPubkeyKind, prefix: prefix, prefixSize: 10, timestampSize: 4}
					i++
				}
			}
		}
		extraFilter = &nostr.Filter{Tags: filter.Tags}
	} else if len(filter.Tags) > 0 {
		// determine the size of the queries array by inspecting all tags sizes
		size := 0
		for _, values := range filter.Tags {
			size += len(values)
		}
		if size == 0 {
			return nil, nil, 0, fmt.Errorf("empty tag filters")
		}

		queries = make([]query, size)

		extraFilter = &nostr.Filter{Kinds: filter.Kinds}
		i := 0
		for _, values := range filter.Tags {
			for _, value := range values {
				// get key prefix (with full length) and offset where to write the created_at
				dbi, k, offset := b.getTagIndexPrefix(value)
				// remove the last parts part to get just the prefix we want here
				prefix := k[0:offset]
				queries[i] = query{i: i, dbi: dbi, prefix: prefix, prefixSize: len(prefix), timestampSize: 4}
				i++
			}
		}
	} else if len(filter.Kinds) > 0 {
		queries = make([]query, len(filter.Kinds))
		for i, kind := range filter.Kinds {
			prefix := make([]byte, 2)
			binary.BigEndian.PutUint16(prefix[:], uint16(kind))
			queries[i] = query{i: i, dbi: b.indexKind, prefix: prefix, prefixSize: 2, timestampSize: 4}
		}
	} else {
		queries = make([]query, 1)
		prefix := make([]byte, 0)
		queries[0] = query{i: 0, dbi: b.indexCreatedAt, prefix: prefix, prefixSize: 0, timestampSize: 4}
		extraFilter = nil
	}

	var until uint32 = 4294967295
	if filter.Until != nil {
		if fu := uint32(*filter.Until); fu < until {
			until = fu + 1
		}
	}
	for i, q := range queries {
		queries[i].startingPoint = binary.BigEndian.AppendUint32(q.prefix, uint32(until))
		queries[i].results = make(chan *nostr.Event, 12)
	}

	// this is where we'll end the iteration
	if filter.Since != nil {
		if fs := uint32(*filter.Since); fs > since {
			since = fs
		}
	}

	return queries, extraFilter, since, nil
}
