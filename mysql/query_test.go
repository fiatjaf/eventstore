package mysql

import (
	"testing"

	"github.com/nbd-wtf/go-nostr"
	"github.com/stretchr/testify/require"
)

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
