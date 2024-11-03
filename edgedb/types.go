package edgedb

import (
	"github.com/edgedb/edgedb-go"
	"github.com/nbd-wtf/go-nostr"
)

type Event struct {
	ID        edgedb.UUID             `edgedb:"id"`
	EventID   string                  `edgedb:"eventId"`
	Pubkey    string                  `edgedb:"pubkey"`
	CreatedAt edgedb.OptionalDateTime `edgedb:"createdAt"`
	Kind      int64                   `edgedb:"kind"`
	Tags      [][]string              `edgedb:"tags"`
	Content   string                  `edgedb:"content"`
	Sig       string                  `edgedb:"sig"`
}

// NostrEventToEdgeDBEvent converts the event from the nostr.Event datatype to edgedb.Event
func NostrEventToEdgeDBEvent(event *nostr.Event) Event {
	tags := [][]string{}
	for _, tag := range event.Tags {
		tags = append(tags, tag)
	}
	return Event{
		EventID:   event.ID,
		Pubkey:    event.PubKey,
		CreatedAt: edgedb.NewOptionalDateTime(event.CreatedAt.Time()),
		Kind:      int64(event.Kind),
		Tags:      tags,
		Content:   event.Content,
		Sig:       event.Sig,
	}
}

// EdgeDBEventToNostrEvent converts the event from the edgedb.Event datatype to nostr.Event
func EdgeDBEventToNostrEvent(event Event) *nostr.Event {
	var tags nostr.Tags
	for _, tag := range event.Tags {
		tags = append(tags, nostr.Tag(tag))
	}
	createdAt, _ := event.CreatedAt.Get()
	return &nostr.Event{
		ID:        event.EventID,
		PubKey:    event.Pubkey,
		CreatedAt: nostr.Timestamp(createdAt.Unix()),
		Kind:      int(event.Kind),
		Tags:      tags,
		Content:   event.Content,
		Sig:       event.Sig,
	}
}
