package badger

import (
	"container/heap"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"log"

	"github.com/dgraph-io/badger/v4"
	bin "github.com/fiatjaf/eventstore/internal/binary"
	"github.com/nbd-wtf/go-nostr"
	"golang.org/x/exp/slices"
)

type query struct {
	i             int
	prefix        []byte
	startingPoint []byte
	results       chan *nostr.Event
	skipTimestamp bool
}

type queryEvent struct {
	*nostr.Event
	query int
}

var exit = errors.New("exit")

func (b BadgerBackend) QueryEvents(ctx context.Context, filter nostr.Filter) (chan *nostr.Event, error) {
	ch := make(chan *nostr.Event)

	if filter.Search != "" {
		close(ch)
		return ch, nil
	}

	queries, extraFilter, since, err := prepareQueries(filter)
	if err != nil {
		return nil, err
	}

	// max number of events we'll return
	limit := b.MaxLimit / 4
	if filter.Limit > 0 && filter.Limit < b.MaxLimit {
		limit = filter.Limit
	}

	go func() {
		ctx, cancel := context.WithCancel(ctx)

		defer cancel()
		defer close(ch)

		// actually iterate
		for _, q := range queries {
			q := q

			pulled := 0 // this query will be hardcapped at this global limit

			go b.View(func(txn *badger.Txn) error {
				// iterate only through keys and in reverse order
				opts := badger.IteratorOptions{
					Reverse: true,
				}

				it := txn.NewIterator(opts)
				defer it.Close()
				defer close(q.results)

				for it.Seek(q.startingPoint); it.ValidForPrefix(q.prefix); it.Next() {
					item := it.Item()
					key := item.Key()

					idxOffset := len(key) - 4 // this is where the idx actually starts

					// "id" indexes don't contain a timestamp
					if !q.skipTimestamp {
						createdAt := binary.BigEndian.Uint32(key[idxOffset-4 : idxOffset])
						if createdAt < since {
							break
						}
					}

					idx := make([]byte, 5)
					idx[0] = rawEventStorePrefix
					copy(idx[1:], key[idxOffset:])

					// fetch actual event
					item, err := txn.Get(idx)
					if err != nil {
						if err == badger.ErrDiscardedTxn {
							return err
						}
						log.Printf("badger: failed to get %x based on prefix %x, index key %x from raw event store: %s\n",
							idx, q.prefix, key, err)
						return err
					}

					if err := item.Value(func(val []byte) error {
						// check it against pubkeys without decoding the entire thing
						if extraFilter != nil && extraFilter.Authors != nil &&
							!slices.Contains(extraFilter.Authors, hex.EncodeToString(val[32:64])) {
							return nil
						}

						// check it against kinds without decoding the entire thing
						if extraFilter != nil && extraFilter.Kinds != nil &&
							!slices.Contains(extraFilter.Kinds, int(binary.BigEndian.Uint16(val[132:134]))) {
							return nil
						}

						evt := &nostr.Event{}
						if err := bin.Unmarshal(val, evt); err != nil {
							log.Printf("badger: value read error (id %x): %s\n", val[0:32], err)
							return err
						}

						// check if this matches the other filters that were not part of the index
						if extraFilter == nil || extraFilter.Matches(evt) {
							select {
							case q.results <- evt:
								pulled++
								if pulled > limit {
									return exit
								}
							case <-ctx.Done():
								return exit
							}
						}

						return nil
					}); err == exit {
						return nil
					} else if err != nil {
						return err
					}
				}

				return nil
			})
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
			if emittedEvents == limit {
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
