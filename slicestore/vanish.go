package slicestore

import (
	"context"

	"github.com/nbd-wtf/go-nostr"
)

func (b *SliceStore) VanishPubkey(ctx context.Context, pubkey string, until int64) error {
	b.Lock()
	defer b.Unlock()
	
	newInternal := make([]*nostr.Event, 0, len(b.internal))
	for _, evt := range b.internal {
		// Skip events that match pubkey, time, and are NOT kind 62
		if evt.PubKey == pubkey && evt.CreatedAt.Time().Unix() <= until && evt.Kind != 62 {
			continue
		}
		newInternal = append(newInternal, evt)
	}
	
	b.internal = newInternal
	return nil
}
