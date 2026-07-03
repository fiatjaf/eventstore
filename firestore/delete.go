package firestore

import (
	"context"

	"github.com/nbd-wtf/go-nostr"
)

func (b *FirestoreBackend) DeleteEvent(ctx context.Context, event *nostr.Event) error {
	_, err := b.Client.Collection(b.Collection).Doc(event.ID).Delete(ctx)
	return err
}
