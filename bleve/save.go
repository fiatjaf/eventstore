package bleve

import (
	"context"
	"fmt"
	"strconv"

	"github.com/nbd-wtf/go-nostr"
)

func (b *BleveBackend) SaveEvent(ctx context.Context, evt *nostr.Event) error {
	ev := &IndexedEvent{
		PubKey:    evt.PubKey[56:],
		CreatedAt: float64(evt.CreatedAt),
		Kind:      strconv.Itoa(evt.Kind),
		Content:   evt.Content,
	}
	if err := b.index.Index(evt.ID, ev); err != nil {
		return fmt.Errorf("failed to write '%s' document: %w", evt.ID, err)
	}
	return nil
}
