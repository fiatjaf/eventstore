package slicestore

import (
	"context"

	"github.com/nbd-wtf/go-nostr"
)

// kindRequestToVanish is the NIP-62 "request to vanish" event kind. These
// events are kept for bookkeeping instead of being deleted.
const kindRequestToVanish = 62

func (b *SliceStore) VanishPubkey(ctx context.Context, pubkey string, until int64) error {
	b.Lock()
	defer b.Unlock()

	newInternal := make([]*nostr.Event, 0, len(b.internal))
	for _, evt := range b.internal {
		// drop this pubkey's events up to `until`, but keep vanish requests
		if evt.PubKey == pubkey && evt.CreatedAt.Time().Unix() <= until && evt.Kind != kindRequestToVanish {
			continue
		}
		newInternal = append(newInternal, evt)
	}

	b.internal = newInternal
	return nil
}
