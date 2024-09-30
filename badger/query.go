package badger

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log"

	"github.com/dgraph-io/badger/v4"
	bin "github.com/fiatjaf/eventstore/internal/binary"
	"github.com/nbd-wtf/go-nostr"
	"golang.org/x/exp/slices"
)

type queryEvent struct {
	*nostr.Event
	query int
}

var (
	exit        = errors.New("exit")
	BatchFilled = errors.New("batch-filled")
)

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

	// fmt.Println("limit", limit)

	go b.View(func(txn *badger.Txn) error {
		defer close(ch)

		iterators := make([]*badger.Iterator, len(queries))
		exhausted := make([]bool, len(queries)) // indicates that a query won't be used anymore
		results := make([][]*nostr.Event, len(queries))
		secondPhaseParticipants := make([]int, 0, len(queries)+1)

		batchSizePerQuery := batchSizePerNumberOfQueries(limit, len(queries))
		totalPulled := 0

		// these are kept updated so we never pull from the iterator that is at further distance
		// (i.e. the one that has the oldest event among all)
		// we will continue to pull from it as soon as some other iterator takes the position
		var furtherEvent *nostr.Event = nil
		furtherIter := -1

		secondPhase := false                 // after we have gathered enough events we will change the way we iterate
		remainingUnexhausted := len(queries) // when all queries are exhausted we can finally end this thing

		var partialResults []*nostr.Event

		for q := range queries {
			iterators[q] = txn.NewIterator(badger.IteratorOptions{
				Reverse:        true,
				PrefetchValues: false, // we don't even have values, only keys
				Prefix:         queries[q].prefix,
			})
			defer iterators[q].Close()

			results[q] = make([]*nostr.Event, 0, batchSizePerQuery)
		}

		// we will reuse this throughout the iteration
		valIdx := make([]byte, 5)

		// fmt.Println("filter", filter, "queries", len(queries))

		for c := 0; ; c++ {
			// fmt.Println("  iteration", c)
			if c > 10 {
				panic("")
			}

			// we will go through all the iterators in batches until we have pulled all the required results
			for q, query := range queries {
				// fmt.Println("    query", q)

				if exhausted[q] {
					// fmt.Println("      exhausted")
					continue
				}
				if furtherIter == q && remainingUnexhausted > 1 {
					// fmt.Println("      skipping for now")
					continue
				}

				it := iterators[q]
				pulled := 0

				for it.Seek(query.startingPoint); ; it.Next() {
					if !it.Valid() {
						exhausted[q] = true
						remainingUnexhausted--
						// fmt.Println("      reached end")
						if q == furtherIter {
							furtherEvent = nil
							furtherIter = -1
						}
						break
					}

					item := it.Item()
					key := item.Key()

					idxOffset := len(key) - 4 // this is where the idx actually starts

					// "id" indexes don't contain a timestamp
					if !query.skipTimestamp {
						createdAt := binary.BigEndian.Uint32(key[idxOffset-4 : idxOffset])
						if createdAt < since {
							exhausted[q] = true
							remainingUnexhausted--
							// fmt.Println("      (")
							if q == furtherIter {
								furtherEvent = nil
								furtherIter = -1
							}
							break
						}
					}

					valIdx[0] = rawEventStorePrefix
					copy(valIdx[1:], key[idxOffset:])

					// fetch actual event
					item, err := txn.Get(valIdx)
					if err != nil {
						if err == badger.ErrDiscardedTxn {
							return err
						}
						log.Printf("badger: failed to get %x based on prefix %x, index key %x from raw event store: %s\n",
							valIdx, query.prefix, key, err)
						return err
					}

					if err := item.Value(func(val []byte) error {
						// fmt.Println("      event", hex.EncodeToString(val[0:4]), "kind", binary.BigEndian.Uint16(val[132:134]), "ts", nostr.Timestamp(binary.BigEndian.Uint32(val[128:132])))

						// check it against pubkeys without decoding the entire thing
						if extraFilter != nil && extraFilter.Authors != nil &&
							!slices.Contains(extraFilter.Authors, hex.EncodeToString(val[32:64])) {
							// fmt.Println("        skipped (authors)")
							return nil
						}

						// check it against kinds without decoding the entire thing
						if extraFilter != nil && extraFilter.Kinds != nil &&
							!slices.Contains(extraFilter.Kinds, int(binary.BigEndian.Uint16(val[132:134]))) {
							// fmt.Println("        skipped (kinds)")
							return nil
						}

						evt := &nostr.Event{}
						if err := bin.Unmarshal(val, evt); err != nil {
							log.Printf("badger: value read error (id %x): %s\n", val[0:32], err)
							return err
						}

						// check if this matches the other filters that were not part of the index
						if extraFilter != nil && !filterMatchesTags(extraFilter, evt) {
							// fmt.Println("        skipped (filter)", extraFilter, evt)
							return nil
						}

						// this event is good to be used
						//
						//
						if secondPhase {
							// do the process described below at HIWAWVRTP.
							// if we've reached here this means we've already passed the `since` check.
							// now we have to eliminate the event currently at the `since` threshold.
							nextThreshold := partialResults[len(partialResults)-2].CreatedAt
							if furtherEvent == nil {
								// when we don't have the further event set, we will keep the results
								//   and not change the cutting point -- it's bad, but hopefully not that bad.
							} else if nextThreshold > furtherEvent.CreatedAt {
								// one of the events we have stored is the actual next threshold
								partialResults[len(partialResults)-1] = furtherEvent
								since = uint32(evt.CreatedAt)
								// we now remove it from the results
								results[furtherIter] = results[furtherIter][0 : len(results[furtherIter])-1]
								// and we set furtherIter to -1 to indicate that from now on we won't rely on
								//   the furtherEvent anymore.
								furtherIter = -1
								furtherEvent = nil
								// anything we got that would be above this won't trigger an update to
								//   the furtherEvent anyway, because it will be discarded as being after the limit.
							} else if nextThreshold < evt.CreatedAt {
								// eliminate one, update since
								partialResults = partialResults[0 : len(partialResults)-1]
								since = uint32(nextThreshold)
								// add us to the results to be merged later
								results[q] = append(results[q], evt)
								// update the further event
								if furtherEvent == nil || evt.CreatedAt < furtherEvent.CreatedAt {
									furtherEvent = evt
									furtherIter = q
								}
							} else {
								// oops, we're the next `since` threshold
								partialResults[len(partialResults)-1] = evt
								since = uint32(evt.CreatedAt)
								// do not add us to the results to be merged later
								//   as we're already inhabiting the partialResults
							}
						} else {
							results[q] = append(results[q], evt)
							totalPulled++

							// update the further event
							if furtherEvent == nil || evt.CreatedAt < furtherEvent.CreatedAt {
								furtherEvent = evt
								furtherIter = q
							}
						}

						pulled++
						if pulled > batchSizePerQuery {
							return BatchFilled
						}

						return nil
					}); err == BatchFilled {
						// fmt.Println("      #")
						break
					} else if err != nil {
						return fmt.Errorf("iteration error: %w", err)
					}
				}
			}

			// we will do this check if we don't accumulated the requested number of events yet
			// fmt.Println("further", furtherEvent, "from iter", furtherIter)
			if secondPhase {
				// what am I supposed to do here?
			} else if totalPulled >= limit {
				// fmt.Println("have enough!")

				// once we reached the limit we will stop fetching from the iterator that has gone further
				// we just mark it as exhausted so we will never try it again
				exhausted[furtherIter] = true
				remainingUnexhausted--

				// we will exclude this oldest number as it is not relevant anymore
				// (we now want to keep track only of the oldest among the remaining iterators)
				furtherEvent = nil
				furtherIter = -1

				// from now on we won't run this block anymore
				secondPhase = true

				// HOW IT WORKS AFTER WE'VE REACHED THIS POINT (HIWAWVRTP)
				// now we can combine the results we have and check what is our current oldest event.
				// we also discard anything that is after the current cutting point (`limit`).
				// so if we have [1,2,3], [10, 15, 20] and [7, 21, 49] but we only want 6 total
				//   we can just keep [1,2,3,7,10,15] and discard [20, 21, 49],
				//   and also adjust our `since` parameter to `15`, discarding anything we get after it
				//   and immediately declaring that iterator exhausted.
				// also every time we get result that is more recent than this updated `since` we can
				//   keep it but also discard the previous since, moving the needle one back -- for example,
				//   if we get an `8` we can keep it and move the `since` parameter to `10`, discarding `15`
				//   in the process.
				partialResults = mergeSortMultiple(results, limit)
				oldestElligible := partialResults[limit-1].CreatedAt
				since = uint32(oldestElligible)
				// fmt.Println("new since", since)

				for q := range queries {
					if exhausted[q] {
						continue
					}

					// we also automatically exhaust any of the iterators that have already passed the
					// cutting point (`since`)
					if results[q][len(results[q])-1].CreatedAt < oldestElligible {
						exhausted[q] = true
						remainingUnexhausted--
						continue
					}

					// for all the remaining iterators,
					// since we have merged all the events in this `partialResults` slice, we can empty the
					//   current `results` slices and reuse them.
					results[q] = results[q][:0]

					secondPhaseParticipants = append(secondPhaseParticipants, q)
				}
			}

			// fmt.Println("remaining", remainingUnexhausted)
			if remainingUnexhausted == 0 {
				break
			}
		}

		// fmt.Println("results", len(results))
		// for q, res := range results {
		// 	fmt.Println(" ", q, len(res))
		// }
		// fmt.Println("exhausted", exhausted)
		// fmt.Println("secondPhase", secondPhase, "partial", len(partialResults))

		var combinedResults []*nostr.Event

		if secondPhase {
			// reuse the results slice just because we are very smart
			// build it the second batch of results that will be merged
			secondBatch := results[0:len(secondPhaseParticipants)]
			for s, q := range secondPhaseParticipants {
				secondBatch[s] = results[q] // if s == q fine, otherwise it can only be s < q, which is also fine
			}
			secondBatch = append(secondBatch, partialResults)
			combinedResults = mergeSortMultiple(secondBatch, limit)
		} else {
			combinedResults = mergeSortMultiple(results, limit)
		}

		for _, evt := range combinedResults {
			ch <- evt
		}

		return nil
	})

	return ch, nil
}
