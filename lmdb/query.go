package lmdb

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"slices"

	"github.com/PowerDNS/lmdb-go/lmdb"
	bin "github.com/fiatjaf/eventstore/internal/binary"
	"github.com/nbd-wtf/go-nostr"
)

type queryEvent struct {
	*nostr.Event
	query int
}

func (b *LMDBBackend) QueryEvents(ctx context.Context, filter nostr.Filter) (chan *nostr.Event, error) {
	ch := make(chan *nostr.Event)

	queries, extraAuthors, extraKinds, extraTagKey, extraTagValues, since, err := b.prepareQueries(filter)
	if err != nil {
		return nil, err
	}

	// max number of events we'll return
	limit := b.MaxLimit / 4
	if filter.Limit > 0 && filter.Limit <= b.MaxLimit {
		limit = filter.Limit
	}

	if filter.Search != "" {
		close(ch)
		return ch, nil
	}

	go func() {
		ctx, cancel := context.WithCancel(ctx)
		defer func() {
			cancel()
			close(ch)
			for _, q := range queries {
				q.free()
			}
		}()

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
						len(k) != q.keySize ||
						!bytes.HasPrefix(k, q.prefix) {
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
					var evt *nostr.Event

					// check it against pubkeys without decoding the entire thing
					if extraAuthors != nil && !slices.Contains(extraAuthors, [32]byte(val[32:64])) {
						goto loopend
					}

					// check it against kinds without decoding the entire thing
					if extraKinds != nil && !slices.Contains(extraKinds, [2]byte(val[132:134])) {
						goto loopend
					}

					// decode the entire thing
					evt = &nostr.Event{}
					if err := bin.Unmarshal(val, evt); err != nil {
						log.Printf("lmdb: value read error (id %x) on query prefix %x sp %x dbi %d: %s\n", val[0:32], q.prefix, q.startingPoint, q.dbi, err)
						return fmt.Errorf("error: %w", err)
					}

					// if there is still a tag to be checked, do it now
					if extraTagValues != nil && !evt.Tags.ContainsAny(extraTagKey, extraTagValues) {
						goto loopend
					}

					select {
					case q.results <- evt:
						pulled++
						if pulled >= limit {
							return nil
						}
					case <-ctx.Done():
						return nil
					}

				loopend:
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
			select {
			case evt, ok := <-q.results:
				if ok {
					emitQueue = append(emitQueue, &queryEvent{Event: evt, query: q.i})
				}
			case <-ctx.Done():
				return
			}
		}

		// queue may be empty here if we have literally nothing
		if len(emitQueue) == 0 {
			return
		}

		heap.Init(&emitQueue)

		// iterate until we've emitted all events required
		var lastEmitted string
		for {
			// emit latest event in queue
			latest := emitQueue[0]
			if lastEmitted != "" && latest.ID == lastEmitted {
				goto skip
			}
			select {
			case ch <- latest.Event:
				lastEmitted = latest.ID
			case <-ctx.Done():
				return
			}

			// stop when reaching limit
			emittedEvents++
			if emittedEvents >= limit {
				break
			}

		skip:
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
