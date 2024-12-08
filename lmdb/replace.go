package lmdb

import (
	"context"
	"fmt"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/fiatjaf/eventstore/internal"
	bin "github.com/fiatjaf/eventstore/internal/binary"
	"github.com/nbd-wtf/go-nostr"
)

func (b *LMDBBackend) ReplaceEvent(ctx context.Context, evt *nostr.Event) error {
	// sanity checking
	if evt.CreatedAt > maxuint32 || evt.Kind > maxuint16 {
		return fmt.Errorf("event with values out of expected boundaries")
	}

	return b.lmdbEnv.Update(func(txn *lmdb.Txn) error {
		filter := nostr.Filter{Limit: 1, Kinds: []int{evt.Kind}, Authors: []string{evt.PubKey}}
		if nostr.IsAddressableKind(evt.Kind) {
			// when addressable, add the "d" tag to the filter
			filter.Tags = nostr.TagMap{"d": []string{evt.Tags.GetD()}}
		}

		// now we fetch the past events, whatever they are, delete them and then save the new
		results, err := b.query(txn, filter, 10) // in theory limit could be just 1 and this should work
		if err != nil {
			return fmt.Errorf("failed to query past events with %s: %w", filter, err)
		}

		shouldStore := true
		for _, previous := range results {
			if internal.IsOlder(previous.Event, evt) {
				if err := b.delete(txn, previous.Event); err != nil {
					return fmt.Errorf("failed to delete event %s for replacing: %w", previous.Event.ID, err)
				}
			} else {
				// there is a newer event already stored, so we won't store this
				shouldStore = false
			}
		}
		if shouldStore {
			// encode to binary form so we'll save it
			bin, err := bin.Marshal(evt)
			if err != nil {
				return err
			}

			idx := b.Serial()
			// raw event store
			if err := txn.Put(b.rawEventStore, idx, bin, 0); err != nil {
				return err
			}

			// put indexes
			for k := range b.getIndexKeysForEvent(evt) {
				err := txn.Put(k.dbi, k.key, idx, 0)
				if err != nil {
					return err
				}
			}
		}

		return nil
	})
}
