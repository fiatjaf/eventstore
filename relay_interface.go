package eventstore

import (
	"context"
	"fmt"

	"github.com/fiatjaf/eventstore/internal"
	"github.com/nbd-wtf/go-nostr"
)

type RelayWrapper struct {
	Store
}

var _ nostr.RelayStore = (*RelayWrapper)(nil)

func (w RelayWrapper) Publish(ctx context.Context, evt nostr.Event) error {
	if nostr.IsEphemeralKind(evt.Kind) {
		// do not store ephemeral events
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if nostr.IsRegularKind(evt.Kind) {
		// regular events are just saved directly
		if err := w.SaveEvent(ctx, &evt); err != nil && err != ErrDupEvent {
			return fmt.Errorf("failed to save: %w", err)
		}
		return nil
	}

	// from now on we know they are replaceable or addressable
	if replacer, ok := w.Store.(Replacer); ok {
		// use the replacer interface to potentially reduce queries and race conditions
		replacer.Replace(ctx, &evt)
	} else {
		// otherwise do it the manual way
		filter := nostr.Filter{Limit: 1, Kinds: []int{evt.Kind}, Authors: []string{evt.PubKey}}
		if nostr.IsAddressableKind(evt.Kind) {
			// when addressable, add the "d" tag to the filter
			filter.Tags = nostr.TagMap{"d": []string{evt.Tags.GetD()}}
		}

		// now we fetch the past events, whatever they are, delete them and then save the new
		ch, err := w.Store.QueryEvents(ctx, filter)
		if err != nil {
			return fmt.Errorf("failed to query before replacing: %w", err)
		}

		shouldStore := true
		for previous := range ch {
			if internal.IsOlder(previous, &evt) {
				if err := w.Store.DeleteEvent(ctx, previous); err != nil {
					return fmt.Errorf("failed to delete event for replacing: %w", err)
				}
			} else {
				// there is a newer event already stored, so we won't store this
				shouldStore = false
			}
		}
		if shouldStore {
			if err := w.SaveEvent(ctx, &evt); err != nil && err != ErrDupEvent {
				return fmt.Errorf("failed to save: %w", err)
			}
		}
	}

	return nil
}

func (w RelayWrapper) QuerySync(ctx context.Context, filter nostr.Filter) ([]*nostr.Event, error) {
	ch, err := w.Store.QueryEvents(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to query: %w", err)
	}

	n := filter.Limit
	if n == 0 {
		n = 500
	}

	results := make([]*nostr.Event, 0, n)
	for evt := range ch {
		results = append(results, evt)
	}

	return results, nil
}
