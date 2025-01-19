package mmm

import (
	"context"
	"fmt"
	"math"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/fiatjaf/eventstore/internal"
	"github.com/nbd-wtf/go-nostr"
)

func (il *IndexingLayer) ReplaceEvent(ctx context.Context, evt *nostr.Event) error {
	// sanity checking
	if evt.CreatedAt > math.MaxUint32 || evt.Kind > math.MaxUint16 {
		return fmt.Errorf("event with values out of expected boundaries")
	}

	filter := nostr.Filter{Limit: 1, Kinds: []int{evt.Kind}, Authors: []string{evt.PubKey}}
	if nostr.IsAddressableKind(evt.Kind) {
		// when addressable, add the "d" tag to the filter
		filter.Tags = nostr.TagMap{"d": []string{evt.Tags.GetD()}}
	}

	return il.mmmm.lmdbEnv.Update(func(mmmtxn *lmdb.Txn) error {
		mmmtxn.RawRead = true

		return il.lmdbEnv.Update(func(iltxn *lmdb.Txn) error {
			// now we fetch the past events, whatever they are, delete them and then save the new
			prevResults, err := il.query(iltxn, filter, 10) // in theory limit could be just 1 and this should work
			if err != nil {
				return fmt.Errorf("failed to query past events with %s: %w", filter, err)
			}

			shouldStore := true
			for _, previous := range prevResults {
				if internal.IsOlder(previous.Event, evt) {
					if err := il.delete(mmmtxn, iltxn, previous.Event); err != nil {
						return fmt.Errorf("failed to delete event %s for replacing: %w", previous.Event.ID, err)
					}
				} else {
					// there is a newer event already stored, so we won't store this
					shouldStore = false
				}
			}
			if shouldStore {
				_, err := il.mmmm.storeOn(mmmtxn, []*IndexingLayer{il}, []*lmdb.Txn{iltxn}, evt)
				return err
			}

			return nil
		})
	})
}
