package mysql

import (
	"strings"
	"testing"

	"github.com/nbd-wtf/go-nostr"
	"github.com/stretchr/testify/require"
)

func tsPtr(v int64) *nostr.Timestamp {
	ts := nostr.Timestamp(v)
	return &ts
}

func TestQueryEventsSqlBuildsTagValueAlternatives(t *testing.T) {
	backend := MySQLBackend{
		QueryLimit:        queryLimit,
		QueryIDsLimit:     queryIDsLimit,
		QueryAuthorsLimit: queryAuthorsLimit,
		QueryKindsLimit:   queryKindsLimit,
		QueryTagsLimit:    queryTagsLimit,
	}

	query, params, err := backend.queryEventsSql(nostr.Filter{
		Tags: nostr.TagMap{"p": []string{"pubkey1", "pubkey2"}},
	}, false)

	require.NoError(t, err)
	require.Contains(t, query, `(tags LIKE ? OR tags LIKE ?)`)
	require.Contains(t, params, `%["p","pubkey1"%`)
	require.Contains(t, params, `%["p","pubkey2"%`)
}

// created_at and kind are 32-bit integer columns; a since/until/kind they cannot
// hold used to fail the whole query with "out of range" instead of matching
// nothing. An unreachable bound must yield a valid no-row query (WHERE false),
// an always-true bound must be dropped, and an out-of-range kind must not reach
// the backend as a bound value.
func TestQueryEventsSqlOutOfRangeFilterValues(t *testing.T) {
	backend := MySQLBackend{
		QueryLimit:        queryLimit,
		QueryIDsLimit:     queryIDsLimit,
		QueryAuthorsLimit: queryAuthorsLimit,
		QueryKindsLimit:   queryKindsLimit,
		QueryTagsLimit:    queryTagsLimit,
	}

	tests := []struct {
		name          string
		filter        nostr.Filter
		unsatisfiable bool // expect WHERE false
		wantParams    []any
	}{
		{
			name:          "since above column max matches nothing",
			filter:        nostr.Filter{Since: tsPtr(9_999_999_999)},
			unsatisfiable: true,
		},
		{
			name:          "until below column min matches nothing",
			filter:        nostr.Filter{Until: tsPtr(-9_999_999_999)},
			unsatisfiable: true,
		},
		{
			name:          "all kinds out of range matches nothing",
			filter:        nostr.Filter{Kinds: []int{9_999_999_999}},
			unsatisfiable: true,
		},
		{
			name:       "since below column min is dropped",
			filter:     nostr.Filter{Since: tsPtr(-9_999_999_999)},
			wantParams: []any{queryLimit},
		},
		{
			name:       "until above column max is dropped",
			filter:     nostr.Filter{Until: tsPtr(9_999_999_999)},
			wantParams: []any{queryLimit},
		},
		{
			name:       "out-of-range kind dropped from a mixed list",
			filter:     nostr.Filter{Kinds: []int{1, 9_999_999_999}},
			wantParams: []any{1, queryLimit},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, params, err := backend.queryEventsSql(tt.filter, false)
			require.NoError(t, err)
			if tt.unsatisfiable {
				require.Contains(t, strings.ReplaceAll(query, " ", ""), "WHEREfalse")
				require.Equal(t, []any{queryLimit}, params)
				return
			}
			require.NotContains(t, query, "false")
			require.Equal(t, tt.wantParams, params)
		})
	}
}
