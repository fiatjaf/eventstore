package sqlite3

import (
	"context"
	"testing"

	"github.com/nbd-wtf/go-nostr"
	"github.com/stretchr/testify/assert"
)

// NIP-45 OR's the filters together, so an event matching more than one of
// them is counted once. Summing a per-filter count would report it twice.
func TestCountEventsFiltersCountsTheUnion(t *testing.T) {
	b := &SQLite3Backend{DatabaseURL: t.TempDir() + "/union.sqlite",
		QueryLimit: 1000, QueryIDsLimit: 500, QueryAuthorsLimit: 500,
		QueryKindsLimit: 100, QueryTagsLimit: 100}
	assert.NoError(t, b.Init())
	defer b.Close()

	ctx := context.Background()
	sk := nostr.GeneratePrivateKey()
	mk := func(kind int, content string) *nostr.Event {
		ev := &nostr.Event{Kind: kind, Content: content, CreatedAt: nostr.Now(), Tags: nostr.Tags{}}
		assert.NoError(t, ev.Sign(sk))
		return ev
	}
	for i := 0; i < 3; i++ {
		assert.NoError(t, b.SaveEvent(ctx, mk(1, string(rune('a'+i)))))
	}
	for i := 0; i < 2; i++ {
		assert.NoError(t, b.SaveEvent(ctx, mk(7, string(rune('x'+i)))))
	}

	one := nostr.Filter{Kinds: []int{1}}
	oneAndSeven := nostr.Filter{Kinds: []int{1, 7}}
	seven := nostr.Filter{Kinds: []int{7}}

	n, err := b.CountEvents(ctx, one)
	assert.NoError(t, err)
	assert.EqualValues(t, 3, n, "single filter")

	// overlapping filters: the union is 5, summing would give 3+5=8
	n, err = b.CountEventsFilters(ctx, nostr.Filters{one, oneAndSeven})
	assert.NoError(t, err)
	assert.EqualValues(t, 5, n, "overlapping filters count each event once")

	// the same filter twice must not double
	n, err = b.CountEventsFilters(ctx, nostr.Filters{one, one})
	assert.NoError(t, err)
	assert.EqualValues(t, 3, n, "duplicate filters count each event once")

	// disjoint filters: the union equals the sum
	n, err = b.CountEventsFilters(ctx, nostr.Filters{one, seven})
	assert.NoError(t, err)
	assert.EqualValues(t, 5, n, "disjoint filters add up")

	n, err = b.CountEventsFilters(ctx, nostr.Filters{one})
	assert.NoError(t, err)
	assert.EqualValues(t, 3, n, "one filter")
}
