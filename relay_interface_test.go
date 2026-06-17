package eventstore

import (
	"context"
	"errors"
	"testing"

	"github.com/nbd-wtf/go-nostr"
	"github.com/stretchr/testify/require"
)

type failingReplaceStore struct{}

func (failingReplaceStore) Init() error { return nil }

func (failingReplaceStore) Close() {}

func (failingReplaceStore) QueryEvents(context.Context, nostr.Filter) (chan *nostr.Event, error) {
	ch := make(chan *nostr.Event)
	close(ch)
	return ch, nil
}

func (failingReplaceStore) DeleteEvent(context.Context, *nostr.Event) error { return nil }

func (failingReplaceStore) SaveEvent(context.Context, *nostr.Event) error { return nil }

func (failingReplaceStore) ReplaceEvent(context.Context, *nostr.Event) error {
	return errors.New("replace failed")
}

func TestPublishReturnsReplaceEventErrors(t *testing.T) {
	w := RelayWrapper{Store: failingReplaceStore{}}

	err := w.Publish(context.Background(), nostr.Event{Kind: 3})

	require.ErrorContains(t, err, "failed to replace")
	require.ErrorContains(t, err, "replace failed")
}
