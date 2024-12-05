package mongo

import (
	"context"
	"github.com/nbd-wtf/go-nostr"
)

func (m *MongoDBBackend) DeleteEvent(context.Context, *nostr.Event) error {
	return nil
}
