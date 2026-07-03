package firestore

import (
	"testing"

	"github.com/nbd-wtf/go-nostr"
	"github.com/stretchr/testify/require"
)

func ts(v nostr.Timestamp) *nostr.Timestamp { return &v }

func TestFilterIsExact(t *testing.T) {
	until := ts(42)

	// at most one disjunctive dimension, within limits => exact
	require.True(t, filterIsExact(nostr.Filter{Until: until}))
	require.True(t, filterIsExact(nostr.Filter{Authors: []string{"a", "b"}}))
	require.True(t, filterIsExact(nostr.Filter{Kinds: []int{1}, Since: ts(10)}))
	require.True(t, filterIsExact(nostr.Filter{Tags: nostr.TagMap{"e": {"x"}}}))

	// two disjunctive dimensions => only one gets pushed down, so not exact
	require.False(t, filterIsExact(nostr.Filter{Authors: []string{"a"}, Kinds: []int{1}}))
	require.False(t, filterIsExact(nostr.Filter{IDs: []string{"a"}, Tags: nostr.TagMap{"e": {"x"}}}))

	// beyond Firestore's disjunction limit => not exact
	big := make([]string, inLimit+1)
	require.False(t, filterIsExact(nostr.Filter{Authors: big}))

	// non-indexable tag name or full-text search => not exact
	require.False(t, filterIsExact(nostr.Filter{Tags: nostr.TagMap{"foo": {"x"}}}))
	require.False(t, filterIsExact(nostr.Filter{Search: "hello"}))
}

func TestFirstSmallTag(t *testing.T) {
	key, values := firstSmallTag(nostr.Filter{Tags: nostr.TagMap{"e": {"x", "y"}}})
	require.Equal(t, "e", key)
	require.Equal(t, []string{"x", "y"}, values)

	// multi-letter tag names are not indexable single-letter tags
	key, _ = firstSmallTag(nostr.Filter{Tags: nostr.TagMap{"foo": {"x"}}})
	require.Equal(t, "", key)

	// oversized value set can't be a single array-contains-any clause
	big := make([]string, inLimit+1)
	key, _ = firstSmallTag(nostr.Filter{Tags: nostr.TagMap{"e": big}})
	require.Equal(t, "", key)
}

func TestTagValues(t *testing.T) {
	values := tagValues(nostr.Tags{
		{"e", "eventid", "relay"},
		{"p", "pubkey"},
		{"d", "identifier"},
		{"imeta", "url ..."},      // multi-letter name is not indexed
		{"e"},                     // no value, skipped
	})
	require.Equal(t, []string{"e:eventid", "p:pubkey", "d:identifier"}, values)
}

func TestTagValueInterfaces(t *testing.T) {
	require.Equal(t,
		[]interface{}{"e:x", "e:y"},
		tagValueInterfaces("e", []string{"x", "y"}),
	)
}
