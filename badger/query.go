package badger

import (
	"container/heap"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/nbd-wtf/go-nostr"
	nostr_binary "github.com/nbd-wtf/go-nostr/binary"
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

func (b BadgerBackend) QueryEvents(ctx context.Context, filter nostr.Filter) (chan *nostr.Event, error) {
	ch := make(chan *nostr.Event)

	queries, extraFilter, since, err := prepareQueries(filter)
	if err != nil {
		return nil, err
	}

	go func() {
		err := b.View(func(txn *badger.Txn) error {
			// iterate only through keys and in reverse order
			opts := badger.IteratorOptions{
				Reverse: true,
			}

			// the transaction inside a badger view must not be used concurrently
			var txMx sync.Mutex
			// actually iterate
			iteratorClosers := make([]func(), len(queries))
			for i, q := range queries {
				go func(i int, q query) {
					// lock transaction before making new iterator
					txMx.Lock()
					defer txMx.Unlock()
					it := txn.NewIterator(opts)
					iteratorClosers[i] = it.Close
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
								return
							}
							log.Printf("badger: failed to get %x based on prefix %x, index key %x from raw event store: %s\n",
								idx, q.prefix, key, err)
							return
						}
						item.Value(func(val []byte) error {
							evt := &nostr.Event{}
							if err := nostr_binary.Unmarshal(val, evt); err != nil {
								log.Printf("badger: value read error (id %x): %s\n", val[0:32], err)
								return err
							}

							// check if this matches the other filters that were not part of the index
							if extraFilter == nil || extraFilter.Matches(evt) {
								q.results <- evt
							}

							return nil
						})
					}
				}(i, q)
			}

			// max number of events we'll return
			limit := b.MaxLimit
			if filter.Limit > 0 && filter.Limit < limit {
				limit = filter.Limit
			}

			// receive results and ensure we only return the most recent ones always
			emittedEvents := 0

			// first pass
			emitQueue := make(priorityQueue, 0, len(queries)+limit)
			for _, q := range queries {
				evt, ok := <-q.results
				if ok {
					emitQueue = append(emitQueue, &queryEvent{Event: evt, query: q.i})
				}
			}

			// now it's a good time to schedule this
			defer func() {
				close(ch)
				// lock transaction before closing iterators
				txMx.Lock()
				defer txMx.Unlock()
				for _, itclose := range iteratorClosers {
					itclose()
				}
				// this is the correct time to close the results channels
				for _, q := range queries {
					close(q.results)
				}
			}()

			// queue may be empty here if we have literally nothing
			if len(emitQueue) == 0 {
				return nil
			}

			heap.Init(&emitQueue)

			// iterate until we've emitted all events required
			for {
				// emit latest event in queue
				latest := emitQueue[0]
				ch <- latest.Event

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

			return nil
		})
		if err != nil {
			log.Printf("badger: query txn error: %s\n", err)
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

func prepareQueries(filter nostr.Filter) (
	queries []query,
	extraFilter *nostr.Filter,
	since uint32,
	err error,
) {
	var index byte

	if len(filter.IDs) > 0 {
		index = indexIdPrefix
		queries = make([]query, len(filter.IDs))
		for i, idHex := range filter.IDs {
			prefix := make([]byte, 1+8)
			prefix[0] = index
			if len(idHex) != 64 {
				return nil, nil, 0, fmt.Errorf("invalid id '%s'", idHex)
			}
			idPrefix8, _ := hex.DecodeString(idHex[0 : 8*2])
			copy(prefix[1:], idPrefix8)
			queries[i] = query{i: i, prefix: prefix, skipTimestamp: true}
		}
	} else if len(filter.Authors) > 0 {
		if len(filter.Kinds) == 0 {
			index = indexPubkeyPrefix
			queries = make([]query, len(filter.Authors))
			for i, pubkeyHex := range filter.Authors {
				if len(pubkeyHex) != 64 {
					return nil, nil, 0, fmt.Errorf("invalid pubkey '%s'", pubkeyHex)
				}
				pubkeyPrefix8, _ := hex.DecodeString(pubkeyHex[0 : 8*2])
				prefix := make([]byte, 1+8)
				prefix[0] = index
				copy(prefix[1:], pubkeyPrefix8)
				queries[i] = query{i: i, prefix: prefix}
			}
		} else {
			index = indexPubkeyKindPrefix
			queries = make([]query, len(filter.Authors)*len(filter.Kinds))
			i := 0
			for _, pubkeyHex := range filter.Authors {
				for _, kind := range filter.Kinds {
					if len(pubkeyHex) != 64 {
						return nil, nil, 0, fmt.Errorf("invalid pubkey '%s'", pubkeyHex)
					}
					pubkeyPrefix8, _ := hex.DecodeString(pubkeyHex[0 : 8*2])
					prefix := make([]byte, 1+8+2)
					prefix[0] = index
					copy(prefix[1:], pubkeyPrefix8)
					binary.BigEndian.PutUint16(prefix[1+8:], uint16(kind))
					queries[i] = query{i: i, prefix: prefix}
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
				// get key prefix (with full length) and offset where to write the last parts
				k, offset := getTagIndexPrefix(value)
				// remove the last parts part to get just the prefix we want here
				prefix := k[0:offset]

				queries[i] = query{i: i, prefix: prefix}
				i++
			}
		}
	} else if len(filter.Kinds) > 0 {
		index = indexKindPrefix
		queries = make([]query, len(filter.Kinds))
		for i, kind := range filter.Kinds {
			prefix := make([]byte, 1+2)
			prefix[0] = index
			binary.BigEndian.PutUint16(prefix[1:], uint16(kind))
			queries[i] = query{i: i, prefix: prefix}
		}
	} else {
		index = indexCreatedAtPrefix
		queries = make([]query, 1)
		prefix := make([]byte, 1)
		prefix[0] = index
		queries[0] = query{i: 0, prefix: prefix}
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
