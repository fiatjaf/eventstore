package edgedb

import (
	"context"

	"github.com/nbd-wtf/go-nostr"
)

// DeleteEvent implements the method of the eventstore.Store interface
func (b *EdgeDBBackend) DeleteEvent(ctx context.Context, event *nostr.Event) error {
	query := "DELETE Event FILTER .eventId = <str>$eventId"
	args := map[string]interface{}{
		"eventId": event.ID,
	}
	e := NostrEventToEdgeDBEvent(event)
	return b.Client.QuerySingle(ctx, query, &e, args)
}