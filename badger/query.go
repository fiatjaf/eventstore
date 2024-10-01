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

type iterEvent struct {
	*nostr.Event
	q int
}

var BatchFilled = errors.New("batch-filled")

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
		results := make([][]iterEvent, len(queries))
		pulledPerQuery := make([]int, len(queries))

		batchSizePerQuery := batchSizePerNumberOfQueries(limit, len(queries))
		totalPulled := 0

		// these are kept updated so we never pull from the iterator that is at further distance
		// (i.e. the one that has the oldest event among all)
		// we will continue to pull from it as soon as some other iterator takes the position
		oldest := iterEvent{q: -1}

		secondPhase := false // after we have gathered enough events we will change the way we iterate
		secondPhaseParticipants := make([]int, 0, len(queries)+1)

		// while merging results in the second phase we will alternate between these two lists
		//   to avoid having to create new lists all the time
		var secondPhaseResultsA []iterEvent
		var secondPhaseResultsB []iterEvent
		var secondPhaseResultsToggle bool // this is just a dummy thing we use to keep track of the alternating

		remainingUnexhausted := len(queries) // when all queries are exhausted we can finally end this thing

		exhaust := func(q int) {
			// fmt.Println("~ exhausting", q, "~")
			exhausted[q] = true
			remainingUnexhausted--
			if q == oldest.q {
				oldest = iterEvent{q: -1}
			}
			if secondPhase {
				secondPhaseParticipants = swapDelete(secondPhaseParticipants,
					slices.Index(secondPhaseParticipants, q))
			}
		}

		var firstPhaseResults []iterEvent

		for q := range queries {
			iterators[q] = txn.NewIterator(badger.IteratorOptions{
				Reverse:        true,
				PrefetchValues: false, // we don't even have values, only keys
				Prefix:         queries[q].prefix,
			})
			defer iterators[q].Close()

			results[q] = make([]iterEvent, 0, batchSizePerQuery)
		}

		// we will reuse this throughout the iteration
		valIdx := make([]byte, 5)

		// fmt.Println("filter", filter, "queries", len(queries))

		for c := 0; ; c++ {
			// fmt.Println("  iteration", c)
			// if c > 10 {
			// 	panic("")
			// }

			// we will go through all the iterators in batches until we have pulled all the required results
			for q, query := range queries {
				// fmt.Println("    query", q)

				if exhausted[q] {
					// fmt.Println("      exhausted")
					continue
				}
				if oldest.q == q && remainingUnexhausted > 1 {
					// fmt.Println("      skipping for now")
					continue
				}

				it := iterators[q]
				pulledThisIteration := 0

				for it.Seek(query.startingPoint); ; it.Next() {
					if !it.Valid() {
						// fmt.Println("      reached end")
						exhaust(q)
						break
					}

					item := it.Item()
					key := item.Key()

					idxOffset := len(key) - 4 // this is where the idx actually starts

					// "id" indexes don't contain a timestamp
					if !query.skipTimestamp {
						createdAt := binary.BigEndian.Uint32(key[idxOffset-4 : idxOffset])
						if createdAt < since {
							// fmt.Println("      (")
							exhaust(q)
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

						event := &nostr.Event{}
						if err := bin.Unmarshal(val, event); err != nil {
							log.Printf("badger: value read error (id %x): %s\n", val[0:32], err)
							return err
						}

						// check if this matches the other filters that were not part of the index
						if extraFilter != nil && !filterMatchesTags(extraFilter, event) {
							// fmt.Println("        skipped (filter)", extraFilter, event)
							return nil
						}

						// this event is good to be used
						evt := iterEvent{Event: event, q: q}
						//
						//
						if secondPhase {
							// do the process described below at HIWAWVRTP.
							// if we've reached here this means we've already passed the `since` check.
							// now we have to eliminate the event currently at the `since` threshold.
							nextThreshold := firstPhaseResults[len(firstPhaseResults)-2]
							if oldest.Event == nil {
								// BRANCH WHEN WE DON'T HAVE THE OLDEST EVENT (BWWDHTOE)
								// when we don't have the oldest set, we will keep the results
								//   and not change the cutting point -- it's bad, but hopefully not that bad.
							} else if nextThreshold.CreatedAt > oldest.CreatedAt {
								// one of the events we have stored is the actual next threshold
								firstPhaseResults[len(firstPhaseResults)-1] = oldest
								since = uint32(evt.CreatedAt)
								// we now remove it from the results
								results[oldest.q] = results[oldest.q][0 : len(results[oldest.q])-1]
								// and we null the oldest Event as we can't rely on it anymore
								//   (we'll fall under BWWDHTOE above) until we have a new oldest set.
								oldest = iterEvent{q: -1}
								// anything we got that would be above this won't trigger an update to
								//   the oldest anyway, because it will be discarded as being after the limit.
							} else if nextThreshold.CreatedAt < evt.CreatedAt {
								// eliminate one, update since
								firstPhaseResults = firstPhaseResults[0 : len(firstPhaseResults)-1]
								since = uint32(nextThreshold.CreatedAt)
								// add us to the results to be merged later
								results[q] = append(results[q], evt)
								// update the oldest event
								if oldest.Event == nil || evt.CreatedAt < oldest.CreatedAt {
									oldest = evt
								}
							} else {
								// oops, _we_ are the next `since` threshold
								firstPhaseResults[len(firstPhaseResults)-1] = evt
								since = uint32(evt.CreatedAt)
								// do not add us to the results to be merged later
								//   as we're already inhabiting the firstPhaseResults slice
							}
						} else {
							results[q] = append(results[q], evt)
							totalPulled++

							// update the oldest event
							if oldest.Event == nil || evt.CreatedAt < oldest.CreatedAt {
								oldest = evt
							}
						}

						pulledPerQuery[q]++
						pulledThisIteration++
						if pulledThisIteration > batchSizePerQuery {
							return BatchFilled
						}
						if pulledPerQuery[q] >= limit {
							exhaust(q)
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
			// fmt.Println("oldest", oldest.Event, "from iter", oldest.q)
			if secondPhase {
				// when we are in the second phase we will aggressively aggregate results on every iteration
				//
				// reuse the results slice just because we are very smart
				// -- use it to build the second batch of results that will be merged
				secondBatch := results[0 : len(secondPhaseParticipants)+1]
				for s, q := range secondPhaseParticipants {
					if len(results[q]) > 0 {
						secondBatch[s] = results[q] // if s == q fine,
						//                             otherwise it can only be s < q, which is also fine
					}
				}

				// every time we get here we will alternate between these A and B lists
				//   combining everything we have into a new partial results list.
				// after we've done that we can again set the oldest.
				if secondPhaseResultsToggle {
					secondBatch = append(secondBatch, secondPhaseResultsB)
					secondPhaseResultsA = secondPhaseResultsA[0:limit]
					secondPhaseResultsA = mergeSortMultiple(secondBatch, limit, secondPhaseResultsA)
					oldest = secondPhaseResultsA[len(secondPhaseResultsA)-1]
				} else {
					secondBatch = append(secondBatch, secondPhaseResultsA)
					secondPhaseResultsB = secondPhaseResultsB[0:limit]
					secondPhaseResultsB = mergeSortMultiple(secondBatch, limit, secondPhaseResultsB)
					oldest = secondPhaseResultsB[len(secondPhaseResultsB)-1]
				}
				secondPhaseResultsToggle = !secondPhaseResultsToggle

				// reset the `results` list so we can keep using it
				results = results[:len(queries)]
				for _, q := range secondPhaseParticipants {
					results[q] = results[q][:0]
				}
			} else if totalPulled >= limit {
				// fmt.Println("have enough!")

				// we will exclude this oldest number as it is not relevant anymore
				// (we now want to keep track only of the oldest among the remaining iterators)
				oldest = iterEvent{q: -1}

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
				firstPhaseResults = mergeSortMultiple(results, limit, make([]iterEvent, limit))
				oldestElligible := firstPhaseResults[limit-1].CreatedAt
				since = uint32(oldestElligible)
				// fmt.Println("new since", since)

				for q := range queries {
					if exhausted[q] {
						continue
					}

					// we also automatically exhaust any of the iterators that have already passed the
					// cutting point (`since`)
					if results[q][len(results[q])-1].CreatedAt < oldestElligible {
						exhaust(q)
						continue
					}

					// for all the remaining iterators,
					// since we have merged all the events in this `firstPhaseResults` slice, we can empty the
					//   current `results` slices and reuse them.
					results[q] = results[q][:0]

					// build this index of indexes with everybody who remains
					secondPhaseParticipants = append(secondPhaseParticipants, q)
				}

				// we create these two lists and alternate between them so we don't have to create a
				//   a new one every time
				secondPhaseResultsA = make([]iterEvent, limit)
				secondPhaseResultsB = make([]iterEvent, limit)

				// from now on we won't run this block anymore
				secondPhase = true
			}

			// fmt.Println("remaining", remainingUnexhausted)
			if remainingUnexhausted == 0 {
				break
			}
		}

		// fmt.Println("results", len(results))
		// for q, res := range results {
		// fmt.Println(" ", q, len(res))
		// }
		// fmt.Println("exhausted", exhausted)
		// fmt.Println("secondPhase", secondPhase, "partial", len(firstPhaseResults))

		var combinedResults []iterEvent

		if secondPhase {
			// fmt.Println("ending second phase")
			// when we reach this point either secondPhaseResultsA or secondPhaseResultsB will be full of stuff,
			//   the other will be empty
			secondPhaseResults := secondPhaseResultsA
			combinedResults = secondPhaseResultsB[0:limit] // reuse this
			if len(secondPhaseResultsA) == 0 {
				secondPhaseResults = secondPhaseResultsB
				combinedResults = secondPhaseResultsA[0:limit] // or this
			}

			all := [][]iterEvent{firstPhaseResults, secondPhaseResults}
			combinedResults = mergeSortMultiple(all, limit, combinedResults)
		} else {
			combinedResults = make([]iterEvent, limit)
			combinedResults = mergeSortMultiple(results, limit, combinedResults)
		}

		for _, evt := range combinedResults {
			ch <- evt.Event
		}

		return nil
	})

	return ch, nil
}
