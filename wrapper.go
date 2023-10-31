package eventstore

import (
	"context"

	"github.com/nbd-wtf/go-nostr"
)

type Wrapper struct {
	Store
}

func (w Wrapper) InjectEvent(ctx context.Context, evt *nostr.Event) error {
	w.SaveEvent(ctx, evt)

	return nil
}
