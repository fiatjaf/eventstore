package test

import (
	"testing"

	"github.com/fiatjaf/eventstore"
	"github.com/nbd-wtf/go-nostr"
	"github.com/stretchr/testify/require"
)

// a tag value must match exactly, not as a prefix of a longer value.
// this is the bug behind https://github.com/fiatjaf/khatru/issues/52: with "a-b" stored first and newer,
// publishing an addressable event with d="a" was silently dropped because the replace query for d="a"
// also matched "a-b" through the undelimited index key.
func tagPrefixTest(t *testing.T, db eventstore.Store) {
	err := db.Init()
	require.NoError(t, err)

	w := eventstore.RelayWrapper{Store: db}
	pk, err := nostr.GetPublicKey(sk3)
	require.NoError(t, err)

	mk := func(kind int, ts int, tags nostr.Tags) *nostr.Event {
		evt := &nostr.Event{CreatedAt: nostr.Timestamp(ts), Kind: kind, Content: "x", Tags: tags}
		evt.Sign(sk3)
		return evt
	}

	// an addressable event whose d tag is a prefix of another one's, published after it and older than it
	ab := mk(30023, 200, nostr.Tags{{"d", "a-b"}})
	a := mk(30023, 100, nostr.Tags{{"d", "a"}})
	require.NoError(t, w.Publish(ctx, *ab))
	require.NoError(t, w.Publish(ctx, *a))

	for _, f := range []nostr.Filter{
		{Kinds: []int{30023}, Authors: []string{pk}, Tags: nostr.TagMap{"d": {"a"}}},
		{Tags: nostr.TagMap{"d": {"a"}}},
	} {
		res, err := w.QuerySync(ctx, f)
		require.NoError(t, err)
		require.Len(t, res, 1, "filter %v", f)
		require.Equal(t, a.ID, res[0].ID, "filter %v", f)
	}
	res, err := w.QuerySync(ctx, nostr.Filter{Tags: nostr.TagMap{"d": {"a-b"}}})
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Equal(t, ab.ID, res[0].ID)

	// the same for "a" tags pointing at those addresses and for plain string tags
	refA := mk(1, 300, nostr.Tags{{"a", "30023:" + pk + ":a"}})
	refAB := mk(1, 301, nostr.Tags{{"a", "30023:" + pk + ":a-b"}})
	tagA := mk(1, 302, nostr.Tags{{"t", "nostr"}})
	tagAB := mk(1, 303, nostr.Tags{{"t", "nostrdev"}})
	for _, evt := range []*nostr.Event{refA, refAB, tagA, tagAB} {
		require.NoError(t, db.SaveEvent(ctx, evt))
	}
	res, err = w.QuerySync(ctx, nostr.Filter{Tags: nostr.TagMap{"a": {"30023:" + pk + ":a"}}})
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Equal(t, refA.ID, res[0].ID)
	res, err = w.QuerySync(ctx, nostr.Filter{Kinds: []int{1}, Tags: nostr.TagMap{"t": {"nostr"}}})
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Equal(t, tagA.ID, res[0].ID)

	// when a filter has two tag conditions both must hold
	res, err = w.QuerySync(ctx, nostr.Filter{Authors: []string{pk}, Tags: nostr.TagMap{"t": {"nostr"}, "a": {"30023:" + pk + ":a"}}})
	require.NoError(t, err)
	require.Len(t, res, 0)

	// also when both tags are ones the planner has no index preference for, so the author index is used
	xy := mk(1, 304, nostr.Tags{{"x", "1"}, {"y", "1"}})
	xOnly := mk(1, 305, nostr.Tags{{"x", "1"}})
	yOnly := mk(1, 306, nostr.Tags{{"y", "1"}})
	for _, evt := range []*nostr.Event{xy, xOnly, yOnly} {
		require.NoError(t, db.SaveEvent(ctx, evt))
	}
	res, err = w.QuerySync(ctx, nostr.Filter{Authors: []string{pk}, Tags: nostr.TagMap{"x": {"1"}, "y": {"1"}}})
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Equal(t, xy.ID, res[0].ID)
}
