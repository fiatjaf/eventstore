package firestore

import (
	"context"
	"encoding/json"

	"github.com/nbd-wtf/go-nostr"
)

// tagValues flattens indexable tags (single-letter names, per NIP-01) into the
// "<letter>:<value>" form stored in the tagvalues array and matched by
// array-contains-any tag queries.
func tagValues(tags nostr.Tags) []string {
	values := make([]string, 0, len(tags))
	for _, tag := range tags {
		if len(tag) < 2 || len(tag[0]) != 1 {
			continue
		}
		values = append(values, tag[0]+":"+tag[1])
	}
	return values
}

func (b *FirestoreBackend) SaveEvent(ctx context.Context, event *nostr.Event) error {
	tags, err := json.Marshal(event.Tags)
	if err != nil {
		return err
	}
	_, err = b.Client.Collection(b.Collection).Doc(event.ID).Set(ctx, firestoreEvent{
		ID:        event.ID,
		PubKey:    event.PubKey,
		CreatedAt: int64(event.CreatedAt),
		Kind:      event.Kind,
		Tags:      string(tags),
		TagValues: tagValues(event.Tags),
		Content:   event.Content,
		Sig:       event.Sig,
	})
	return err
}
