package edgedb

import (
	"encoding/json"

	"github.com/edgedb/edgedb-go"
	"github.com/nbd-wtf/go-nostr"
)

type Event struct {
	ID        edgedb.UUID             `edgedb:"id"`
	EventID   string                  `edgedb:"eventId"`
	Pubkey    string                  `edgedb:"pubkey"`
	CreatedAt edgedb.OptionalDateTime `edgedb:"createdAt"`
	Kind      int64                   `edgedb:"kind"`
	Tags      []byte                  `edgedb:"tags"`
	Content   string                  `edgedb:"content"`
	Sig       string                  `edgedb:"sig"`
}

// NostrEventToEdgeDBEvent converts the event from the nostr.Event datatype to edgedb.Event
func NostrEventToEdgeDBEvent(event *nostr.Event) (Event, error) {
	tagsBytes, err := json.Marshal(event.Tags)
	if err != nil {
		return Event{}, err
	}
	return Event{
		EventID:   event.ID,
		Pubkey:    event.PubKey,
		CreatedAt: edgedb.NewOptionalDateTime(event.CreatedAt.Time()),
		Kind:      int64(event.Kind),
		Tags:      tagsBytes,
		Content:   event.Content,
		Sig:       event.Sig,
	}, nil
}

// EdgeDBEventToNostrEvent converts the event from the edgedb.Event datatype to nostr.Event
func EdgeDBEventToNostrEvent(event Event) (*nostr.Event, error) {
	var tags nostr.Tags
	if err := json.Unmarshal(event.Tags, &tags); err != nil {
		return nil, err
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
	}, nil
}
