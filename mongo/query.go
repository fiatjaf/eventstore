package mongo

import (
	"context"
	"errors"
	"github.com/nbd-wtf/go-nostr"
)

var (
	ErrTooManyIDs       = errors.New("too many ids")
	ErrTooManyAuthors   = errors.New("too many authors")
	ErrTooManyKinds     = errors.New("too many kinds")
	ErrEmptyTagSet      = errors.New("empty tag set")
	ErrTooManyTagValues = errors.New("too many tag values")
)

func (b *MongoDBBackend) QueryEvents(ctx context.Context, filter nostr.Filter) (chan *nostr.Event, error) {
	ch := make(chan *nostr.Event)

	go func() {
		var evt nostr.Event
		for {
			select {
			case ch <- &evt:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}
