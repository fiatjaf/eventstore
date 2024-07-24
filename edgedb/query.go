package edgedb

import (
	"context"

	"github.com/nbd-wtf/go-nostr"
)

// TODO
func (b *EdgeDBBackend) QueryEvents(ctx context.Context, filter nostr.Filter) (chan *nostr.Event, error) {
	ch := make(chan *nostr.Event)
	return ch, nil
}
