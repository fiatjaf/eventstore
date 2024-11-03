package edgedb

import (
	"context"

	"github.com/nbd-wtf/go-nostr"
)

// DeleteEvent implements the method of the eventstore.Store interface
func (b *EdgeDBBackend) DeleteEvent(ctx context.Context, event *nostr.Event) error {
	query := "DELETE events::Event FILTER .eventId = <str>$eventId"
	args := map[string]interface{}{
		"eventId": event.ID,
	}
	e, err := NostrEventToEdgeDBEvent(event)
	if err != nil {
		return err
	}
	return b.Client.QuerySingle(ctx, query, &e, args)
}