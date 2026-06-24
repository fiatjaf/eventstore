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
	if evt.Kind == KindRequestToVanish {
		// vanishing is irreversible and deletes events for evt.PubKey, so the
		// request must be authentic: only the pubkey that signed it can vanish
		// its own events. Verify the signature before deleting anything in case
		// the caller didn't already.
		if ok, err := evt.CheckSignature(); err != nil {
			return fmt.Errorf("failed to check vanish request signature: %w", err)
		} else if !ok {
			return fmt.Errorf("vanish request has an invalid signature")
		}

		// a vanish request must name at least one relay (a specific URL or the
		// "ALL_RELAYS" sentinel) for us to act on it
		hasRelayTag := false
		for _, tag := range evt.Tags {
			if len(tag) >= 2 && tag[0] == "relay" {
				hasRelayTag = true
				break
			}
		}

		// keep the request itself for bookkeeping
		if err := w.SaveEvent(ctx, &evt); err != nil && err != ErrDupEvent {
			return fmt.Errorf("failed to save vanish request: %w", err)
		}

		if vanisher, ok := w.Store.(VanishPubkey); ok && hasRelayTag {
			if err := vanisher.VanishPubkey(ctx, evt.PubKey, evt.CreatedAt.Time().Unix()); err != nil {
				return fmt.Errorf("failed to vanish pubkey: %w", err)
			}
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
	if err := w.Store.ReplaceEvent(ctx, &evt); err != nil {
		return fmt.Errorf("failed to replace: %w", err)
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
