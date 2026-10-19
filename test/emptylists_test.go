package test

import (
	"testing"

	"github.com/fiatjaf/eventstore"
	"github.com/nbd-wtf/go-nostr"
	"github.com/stretchr/testify/require"
)

// NIP-01: an empty filter list matches nothing and must not be treated as if
// the attribute were absent, while a nil tag value list is no constraint at all
// (matching nostr.Filter.Matches).
func emptyListsTest(t *testing.T, db eventstore.Store) {
	db.Init()

	pk3, _ := nostr.GetPublicKey(sk3)
	stored := make([]*nostr.Event, 0, 3)
	for i := 0; i < 3; i++ {
		evt := &nostr.Event{
			CreatedAt: nostr.Timestamp(1000 + i),
			Content:   "empty lists",
			Tags:      nostr.Tags{{"e", "0000000000000000000000000000000000000000000000000000000000000001"}, {"t", "x"}},
			Kind:      1,
		}
		require.NoError(t, evt.Sign(sk3))
		require.NoError(t, db.SaveEvent(ctx, evt))
		stored = append(stored, evt)
	}

	for _, tt := range []struct {
		name   string
		filter nostr.Filter
		want   int
	}{
		{"no lists", nostr.Filter{Kinds: []int{1}}, 3},
		{"empty ids", nostr.Filter{IDs: []string{}}, 0},
		{"empty authors", nostr.Filter{Authors: []string{}}, 0},
		{"empty kinds", nostr.Filter{Kinds: []int{}}, 0},
		{"empty tag", nostr.Filter{Tags: nostr.TagMap{"e": {}}}, 0},
		{"empty tag next to a matching one", nostr.Filter{Kinds: []int{1}, Tags: nostr.TagMap{"t": {"x"}, "e": {}}}, 0},
		{"empty kinds next to matching authors", nostr.Filter{Authors: []string{pk3}, Kinds: []int{}}, 0},
		{"nil tag values", nostr.Filter{Kinds: []int{1}, Tags: nostr.TagMap{"e": nil}}, 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ch, err := db.QueryEvents(ctx, tt.filter)
			require.NoError(t, err)
			got := 0
			for evt := range ch {
				require.True(t, tt.filter.Matches(evt), "returned an event the filter does not match")
				got++
			}
			require.Equal(t, tt.want, got)

			if counter, ok := db.(eventstore.Counter); ok {
				count, err := counter.CountEvents(ctx, tt.filter)
				require.NoError(t, err)
				require.Equal(t, int64(tt.want), count)
			}
		})
	}
}
