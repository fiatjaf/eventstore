package bleve

import (
	"context"

	"github.com/nbd-wtf/go-nostr"
)

func (b *BleveBackend) DeleteEvent(ctx context.Context, evt *nostr.Event) error {
	return b.index.Delete(evt.ID)
}
