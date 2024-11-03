package edgedb

import (
	"context"

	"github.com/nbd-wtf/go-nostr"
)

func (b *EdgeDBBackend) SaveEvent(ctx context.Context, event *nostr.Event) error {
	e, err := NostrEventToEdgeDBEvent(event)
	if err != nil {
		return err
	}
	query := "INSERT events::Event { eventId := <str>$eventId, pubkey := <str>$pubkey, createdAt := <datetime>$createdAt, kind := <int64>$kind, tags := <array<json>>$tags, content := <str>$content, sig := <str>$sig }"
	args := map[string]interface{}{
		"eventId":   event.ID,
		"pubkey":    event.PubKey,
		"createdAt": e.CreatedAt,
		"kind":      e.Kind,
		"tags":      e.Tags,
		"content":   event.Content,
		"sig":       event.Sig,
	}
	return b.Client.QuerySingle(ctx, query, &e, args)
}
