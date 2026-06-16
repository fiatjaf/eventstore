package disablesearch

import (
	"context"
	"testing"

	"github.com/fiatjaf/eventstore"
	"github.com/fiatjaf/eventstore/nullstore"
	"github.com/nbd-wtf/go-nostr"
	"github.com/stretchr/testify/require"
)

func TestQueryEventsWithSearchReturnsClosedChannel(t *testing.T) {
	w := eventstore.RelayWrapper{Store: Wrapper{Store: nullstore.NullStore{}}}

	events, err := w.QuerySync(context.Background(), nostr.Filter{Search: "nostr"})

	require.NoError(t, err)
	require.Empty(t, events)
}
