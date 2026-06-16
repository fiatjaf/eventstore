package disablesearch

import (
	"context"

	"github.com/fiatjaf/eventstore"
	"github.com/nbd-wtf/go-nostr"
)

type Wrapper struct {
	eventstore.Store
}

var _ eventstore.Store = (*Wrapper)(nil)

func (w Wrapper) QueryEvents(ctx context.Context, filter nostr.Filter) (chan *nostr.Event, error) {
	if filter.Search != "" {
		ch := make(chan *nostr.Event)
		close(ch)
		return ch, nil
	}
	return w.Store.QueryEvents(ctx, filter)
}
