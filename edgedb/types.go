package edgedb

import (
	"bytes"
	"encoding/json"

	"github.com/edgedb/edgedb-go"
	"github.com/nbd-wtf/go-nostr"
)

// NewOptionalTags is a convenience function for creating an OptionalTags with its value set to tags
func NewOptionalTags(tags [][]byte) OptionalTags {
	o := OptionalTags{}
	o.Set(tags)
	return o
}

type OptionalTags struct {
	edgedb.Optional
	val [][]byte
}

func (o OptionalTags) Get() ([][]byte, bool) {
	return o.val, !o.Missing()
}

// Set sets the vaue
func (o *OptionalTags) Set(val [][]byte) {
	o.val = val
	o.SetMissing(false)
}

// MarshalJSON returns o marshaled as json.
func (o OptionalTags) MarshalJSON() ([]byte, error) {
	if !o.Missing() {
		return json.Marshal(o.val)
	}
	return []byte("[]"), nil
}

// UnmarshalJSON unmarshals bytes into *o.
func (o *OptionalTags) UnmarshalJSON(b []byte) error {
	if b[0] == 0x6e || (len(b) == 2 && bytes.Equal(b, []byte("[]"))) { //null
		o.Unset()
		return nil
	}

	if err := json.Unmarshal(b, &o.val); err != nil {
		return err
	}
	o.SetMissing(false)
	return nil
}

type Event struct {
	ID        edgedb.UUID             `edgedb:"id"`
	EventID   string                  `edgedb:"eventId"`
	Pubkey    string                  `edgedb:"pubkey"`
	CreatedAt edgedb.OptionalDateTime `edgedb:"createdAt"`
	Kind      int64                   `edgedb:"kind"`
	Tags      OptionalTags            `edgedb:"tags"`
	Content   edgedb.OptionalStr      `edgedb:"content"`
	Sig       string                  `edgedb:"sig"`
}

// NostrEventToEdgeDBEvent converts the event from the nostr.Event datatype to edgedb.Event
func NostrEventToEdgeDBEvent(event *nostr.Event) (Event, error) {
	var tagsBytes [][]byte
	for _, t := range event.Tags {
		tagBytes, err := json.Marshal(t)
		if err != nil {
			return Event{}, err
		}
		tagsBytes = append(tagsBytes, tagBytes)
	}
	return Event{
		EventID:   event.ID,
		Pubkey:    event.PubKey,
		CreatedAt: edgedb.NewOptionalDateTime(event.CreatedAt.Time()),
		Kind:      int64(event.Kind),
		Tags:      NewOptionalTags(tagsBytes),
		Content:   edgedb.NewOptionalStr(event.Content),
		Sig:       event.Sig,
	}, nil
}

// EdgeDBEventToNostrEvent converts the event from the edgedb.Event datatype to nostr.Event
func EdgeDBEventToNostrEvent(event Event) (*nostr.Event, error) {
	var tags nostr.Tags
	tagsBytes, ok := event.Tags.Get()
	if ok {
		for _, tagBytes := range tagsBytes {
			var tag nostr.Tag
			if err := json.Unmarshal(tagBytes, &tag); err != nil {
				return nil, err
			}
			tags = append(tags, tag)
		}
	}
	createdAt, _ := event.CreatedAt.Get()
	content, _ := event.Content.Get()
	return &nostr.Event{
		ID:        event.EventID,
		PubKey:    event.Pubkey,
		CreatedAt: nostr.Timestamp(createdAt.Unix()),
		Kind:      int(event.Kind),
		Tags:      tags,
		Content:   content,
		Sig:       event.Sig,
	}, nil
}
