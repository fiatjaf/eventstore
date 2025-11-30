package eventstore

import (
	"context"
	"fmt"

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

	// Handle NIP-62 Request to Vanish (Kind 62)
	if evt.Kind == 62 {
		if vanisher, ok := w.Store.(VanishPubkey); ok {
			for _, tag := range evt.Tags {
				if len(tag) >= 2 && tag[0] == "relay" {
					relayURL := tag[1]
					if relayURL == "ALL_RELAYS" {
						if err := w.SaveEvent(ctx, &evt); err != nil && err != ErrDupEvent {
							return fmt.Errorf("failed to save vanish request: %w", err)
						}
						if err := vanisher.VanishPubkey(ctx, evt.PubKey, evt.CreatedAt.Time().Unix()); err != nil {
							return fmt.Errorf("failed to vanish pubkey: %w", err)
						}
						return nil
					}
					if err := w.SaveEvent(ctx, &evt); err != nil && err != ErrDupEvent {
						return fmt.Errorf("failed to save vanish request: %w", err)
					}
					if err := vanisher.VanishPubkey(ctx, evt.PubKey, evt.CreatedAt.Time().Unix()); err != nil {
						return fmt.Errorf("failed to vanish pubkey: %w", err)
					}
					return nil
				}
			}
		}
		if err := w.SaveEvent(ctx, &evt); err != nil && err != ErrDupEvent {
			return fmt.Errorf("failed to save: %w", err)
		}
		return nil
	}

	if nostr.IsRegularKind(evt.Kind) {
		// regular events are just saved directly
		if err := w.SaveEvent(ctx, &evt); err != nil && err != ErrDupEvent {
			return fmt.Errorf("failed to save: %w", err)
		}
		return nil
	}

	// others are replaced
	w.Store.ReplaceEvent(ctx, &evt)

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
