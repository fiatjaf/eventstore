//go:build integration

package firestore

import (
	"context"
	"os"
	"testing"

	"github.com/nbd-wtf/go-nostr"
	"github.com/stretchr/testify/require"
)

// TestIntegration exercises the backend against a real Firestore database.
//
// It is gated behind the "integration" build tag and the FIRESTORE_TEST_PROJECT
// environment variable, and requires Application Default Credentials plus the
// composite indexes from firestore.indexes.json deployed for the collection.
//
//	FIRESTORE_TEST_PROJECT=my-firebase-d0b6e \
//	FIRESTORE_TEST_COLLECTION=nostr-relay \
//	go test -tags integration -run TestIntegration -v ./firestore/
func TestIntegration(t *testing.T) {
	project := os.Getenv("FIRESTORE_TEST_PROJECT")
	if project == "" {
		t.Skip("set FIRESTORE_TEST_PROJECT to run the Firestore integration test")
	}
	collection := os.Getenv("FIRESTORE_TEST_COLLECTION")
	if collection == "" {
		collection = "nostr-events"
	}

	b := &FirestoreBackend{ProjectID: project, Collection: collection}
	require.NoError(t, b.Init())
	defer b.Close()

	ctx := context.Background()

	// two authors with distinct, deterministic keys
	skAlice := nostr.GeneratePrivateKey()
	pkAlice, _ := nostr.GetPublicKey(skAlice)
	skBob := nostr.GeneratePrivateKey()
	pkBob, _ := nostr.GetPublicKey(skBob)

	mkNote := func(sk string, createdAt nostr.Timestamp, content string, tags nostr.Tags) *nostr.Event {
		evt := &nostr.Event{
			CreatedAt: createdAt,
			Kind:      1,
			Tags:      tags,
			Content:   content,
		}
		require.NoError(t, evt.Sign(sk))
		return evt
	}

	// a small, well-known corpus
	events := []*nostr.Event{
		mkNote(skAlice, 1000, "alice-1", nostr.Tags{{"t", "nostr"}}),
		mkNote(skAlice, 2000, "alice-2", nostr.Tags{{"e", "abc"}}),
		mkNote(skBob, 1500, "bob-1", nostr.Tags{{"t", "nostr"}, {"e", "abc"}}),
		mkNote(skBob, 3000, "bob-2", nostr.Tags{{"p", pkAlice}}),
	}

	// save
	for _, evt := range events {
		require.NoError(t, b.SaveEvent(ctx, evt))
	}
	// ensure a clean slate afterwards regardless of assertions
	defer func() {
		for _, evt := range events {
			_ = b.DeleteEvent(ctx, evt)
		}
	}()

	collect := func(filter nostr.Filter) []*nostr.Event {
		ch, err := b.QueryEvents(ctx, filter)
		require.NoError(t, err)
		var got []*nostr.Event
		for evt := range ch {
			got = append(got, evt)
		}
		return got
	}
	contents := func(evts []*nostr.Event) []string {
		out := make([]string, len(evts))
		for i, e := range evts {
			out[i] = e.Content
		}
		return out
	}

	t.Run("by author", func(t *testing.T) {
		got := collect(nostr.Filter{Authors: []string{pkAlice}})
		require.ElementsMatch(t, []string{"alice-1", "alice-2"}, contents(got))
	})

	t.Run("ordered desc by created_at", func(t *testing.T) {
		got := collect(nostr.Filter{Authors: []string{pkAlice}})
		require.Equal(t, []string{"alice-2", "alice-1"}, contents(got)) // 2000 before 1000
	})

	t.Run("by tag", func(t *testing.T) {
		got := collect(nostr.Filter{Tags: nostr.TagMap{"t": {"nostr"}}})
		require.ElementsMatch(t, []string{"alice-1", "bob-1"}, contents(got))
	})

	t.Run("multiple tags are AND (client-side residual)", func(t *testing.T) {
		got := collect(nostr.Filter{Tags: nostr.TagMap{"t": {"nostr"}, "e": {"abc"}}})
		require.ElementsMatch(t, []string{"bob-1"}, contents(got))
	})

	t.Run("since/until range", func(t *testing.T) {
		since := nostr.Timestamp(1400)
		until := nostr.Timestamp(2500)
		got := collect(nostr.Filter{Since: &since, Until: &until})
		require.ElementsMatch(t, []string{"alice-2", "bob-1"}, contents(got))
	})

	t.Run("limit", func(t *testing.T) {
		got := collect(nostr.Filter{Limit: 1, Authors: []string{pkAlice}})
		require.Len(t, got, 1)
		require.Equal(t, "alice-2", got[0].Content) // newest first
	})

	t.Run("count exact (single dimension, uses aggregation)", func(t *testing.T) {
		n, err := b.CountEvents(ctx, nostr.Filter{Authors: []string{pkBob}})
		require.NoError(t, err)
		require.Equal(t, int64(2), n)
	})

	t.Run("count with residual (multi-tag)", func(t *testing.T) {
		n, err := b.CountEvents(ctx, nostr.Filter{Tags: nostr.TagMap{"t": {"nostr"}, "e": {"abc"}}})
		require.NoError(t, err)
		require.Equal(t, int64(1), n)
	})

	t.Run("replace addressable/replaceable", func(t *testing.T) {
		// kind 0 is replaceable; newer should win
		older := &nostr.Event{CreatedAt: 5000, Kind: 0, Tags: nostr.Tags{}, Content: "profile-v1"}
		require.NoError(t, older.Sign(skAlice))
		require.NoError(t, b.ReplaceEvent(ctx, older))
		defer b.DeleteEvent(ctx, older)

		newer := &nostr.Event{CreatedAt: 6000, Kind: 0, Tags: nostr.Tags{}, Content: "profile-v2"}
		require.NoError(t, newer.Sign(skAlice))
		require.NoError(t, b.ReplaceEvent(ctx, newer))
		defer b.DeleteEvent(ctx, newer)

		got := collect(nostr.Filter{Kinds: []int{0}, Authors: []string{pkAlice}})
		require.Equal(t, []string{"profile-v2"}, contents(got))
	})

	t.Run("delete", func(t *testing.T) {
		require.NoError(t, b.DeleteEvent(ctx, events[0]))
		got := collect(nostr.Filter{Authors: []string{pkAlice}})
		require.ElementsMatch(t, []string{"alice-2"}, contents(got))
		// re-save so the outer cleanup stays consistent
		require.NoError(t, b.SaveEvent(ctx, events[0]))
	})
}
