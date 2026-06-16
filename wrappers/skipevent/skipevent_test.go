package skipevent

import (
	"context"
	"testing"

	"github.com/fiatjaf/eventstore"
	"github.com/fiatjaf/eventstore/nullstore"
	"github.com/nbd-wtf/go-nostr"
	"github.com/stretchr/testify/require"
)

type replaceCountingStore struct {
	nullstore.NullStore
	replaces int
}

func (s *replaceCountingStore) ReplaceEvent(context.Context, *nostr.Event) error {
	s.replaces++
	return nil
}

func TestReplaceEventCanBeSkipped(t *testing.T) {
	store := &replaceCountingStore{}
	w := eventstore.RelayWrapper{Store: Wrapper{
		Store: store,
		Skip:  func(context.Context, *nostr.Event) bool { return true },
	}}

	err := w.Publish(context.Background(), nostr.Event{Kind: 3})

	require.NoError(t, err)
	require.Zero(t, store.replaces)
}
