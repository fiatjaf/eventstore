package edgedb

import (
	"context"

	"github.com/nbd-wtf/go-nostr"
)

func (b *EdgeDBBackend) SaveEvent(ctx context.Context, event *nostr.Event) error {
	e := NostrEventToEdgeDBEvent(event)
	query := "INSERT Event { eventId := <str>$eventId, pubkey := <str>$pubkey, createdAt := <datetime>$createdAt, kind := <int64>$kind, tags := <json>$tags, content := <str>$content, sig := <str>$sig }"
	args := map[string]interface{}{
		"eventId":   event.ID,
		"pubkey":    event.PubKey,
		"createdAt": event.CreatedAt,
		"kind":      event.Kind,
		"tags":      event.Tags, // may need to JSON stringify
		"content":   event.Content,
		"sig":       event.Sig,
	}
	return b.Client.QuerySingle(ctx, query, &e, args)
}
