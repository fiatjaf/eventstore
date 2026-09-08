package postgresql

import (
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/nbd-wtf/go-nostr"
	"github.com/stretchr/testify/assert"
)

func tsPtr(v int64) *nostr.Timestamp {
	ts := nostr.Timestamp(v)
	return &ts
}

var defaultBackend = &PostgresBackend{
	QueryLimit:        queryLimit,
	QueryIDsLimit:     queryIDsLimit,
	QueryAuthorsLimit: queryAuthorsLimit,
	QueryKindsLimit:   queryKindsLimit,
	QueryTagsLimit:    queryTagsLimit,
}

var substringSearchBackend = &PostgresBackend{
	QueryLimit:        queryLimit,
	QueryIDsLimit:     queryIDsLimit,
	QueryAuthorsLimit: queryAuthorsLimit,
	QueryKindsLimit:   queryKindsLimit,
	QueryTagsLimit:    queryTagsLimit,
	SubstringSearch:   true,
}

func TestQueryEventsSql(t *testing.T) {
	tests := []struct {
		name    string
		backend *PostgresBackend
		filter  nostr.Filter
		query   string
		params  []any
		err     error
	}{
		{
			name:    "empty filter",
			backend: defaultBackend,
			filter:  nostr.Filter{},
			query:   "SELECT id, pubkey, created_at, kind, tags, content, sig FROM event WHERE true ORDER BY created_at DESC, id LIMIT $1",
			params:  []any{100},
			err:     nil,
		},
		{
			name:    "valid filter limit",
			backend: defaultBackend,
			filter: nostr.Filter{
				Limit: 50,
			},
			query:  "SELECT id, pubkey, created_at, kind, tags, content, sig FROM event WHERE true ORDER BY created_at DESC, id LIMIT $1",
			params: []any{50},
			err:    nil,
		},
		{
			name:    "too large filter limit",
			backend: defaultBackend,
			filter: nostr.Filter{
				Limit: 2000,
			},
			query:  "SELECT id, pubkey, created_at, kind, tags, content, sig FROM event WHERE true ORDER BY created_at DESC, id LIMIT $1",
			params: []any{100},
			err:    nil,
		},
		{
			name:    "ids filter",
			backend: defaultBackend,
			filter: nostr.Filter{
				IDs: []string{"083ec57f36a7b39ab98a57bedab4f85355b2ee89e4b205bed58d7c3ef9edd294"},
			},
			query: `SELECT id, pubkey, created_at, kind, tags, content, sig
			FROM event
			WHERE id IN ($1)
			ORDER BY created_at DESC, id LIMIT $2`,
			params: []any{"083ec57f36a7b39ab98a57bedab4f85355b2ee89e4b205bed58d7c3ef9edd294", 100},
			err:    nil,
		},
		{
			name:    "kind filter",
			backend: defaultBackend,
			filter: nostr.Filter{
				Kinds: []int{1, 2, 3},
			},
			query: `SELECT id, pubkey, created_at, kind, tags, content, sig
			FROM event
			WHERE kind IN($1,$2,$3)
			ORDER BY created_at DESC, id LIMIT $4`,
			params: []any{1, 2, 3, 100},
			err:    nil,
		},
		{
			name:    "authors filter",
			backend: defaultBackend,
			filter: nostr.Filter{
				Authors: []string{"7bdef7bdebb8721f77927d0e77c66059360fa62371fdf15f3add93923a613229"},
			},
			query: `SELECT id, pubkey, created_at, kind, tags, content, sig
			FROM event
			WHERE pubkey IN ($1)
			ORDER BY created_at DESC, id LIMIT $2`,
			params: []any{"7bdef7bdebb8721f77927d0e77c66059360fa62371fdf15f3add93923a613229", 100},
			err:    nil,
		},
		// errors
		{
			name:    "too many ids",
			backend: defaultBackend,
			filter: nostr.Filter{
				IDs: strSlice(501),
			},
			query:  "",
			params: nil,
			err:    TooManyIDs,
		},
		{
			name:    "too many authors",
			backend: defaultBackend,
			filter: nostr.Filter{
				Authors: strSlice(501),
			},
			query:  "",
			params: nil,
			err:    TooManyAuthors,
		},
		{
			name:    "too many kinds",
			backend: defaultBackend,
			filter: nostr.Filter{
				Kinds: intSlice(11),
			},
			query:  "",
			params: nil,
			err:    TooManyKinds,
		},
		{
			name:    "tags of empty array",
			backend: defaultBackend,
			filter: nostr.Filter{
				Tags: nostr.TagMap{
					"#e": []string{},
				},
			},
			query:  "",
			params: nil,
			err:    EmptyTagSet,
		},
		{
			name:    "tag filter with key prefix",
			backend: defaultBackend,
			filter: nostr.Filter{
				Tags: nostr.TagMap{
					"#e": {"abc123"},
				},
			},
			query: `SELECT id, pubkey, created_at, kind, tags, content, sig
			FROM event
			WHERE tagvalues && ARRAY[$1]
			ORDER BY created_at DESC, id LIMIT $2`,
			params: []any{"e:abc123", 100},
			err:    nil,
		},
		{
			name:    "too many tag values",
			backend: defaultBackend,
			filter: nostr.Filter{
				Tags: nostr.TagMap{
					"#e": strSlice(101),
				},
			},
			query:  "",
			params: nil,
			err:    TooManyTagValues,
		},
		// out-of-range created_at / kind: the sql columns are 32-bit integer, so
		// a value they cannot hold used to fail the whole query with "out of
		// range for type integer" (22003) instead of matching nothing.
		{
			name:    "since above column max matches nothing",
			backend: defaultBackend,
			filter:  nostr.Filter{Since: tsPtr(9_999_999_999)},
			query: `SELECT id, pubkey, created_at, kind, tags, content, sig
			FROM event
			WHERE false
			ORDER BY created_at DESC, id LIMIT $1`,
			params: []any{100},
			err:    nil,
		},
		{
			name:    "until below column min matches nothing",
			backend: defaultBackend,
			filter:  nostr.Filter{Until: tsPtr(-9_999_999_999)},
			query: `SELECT id, pubkey, created_at, kind, tags, content, sig
			FROM event
			WHERE false
			ORDER BY created_at DESC, id LIMIT $1`,
			params: []any{100},
			err:    nil,
		},
		{
			name:    "since below column min is dropped",
			backend: defaultBackend,
			filter:  nostr.Filter{Since: tsPtr(-9_999_999_999)},
			query: `SELECT id, pubkey, created_at, kind, tags, content, sig
			FROM event
			WHERE true
			ORDER BY created_at DESC, id LIMIT $1`,
			params: []any{100},
			err:    nil,
		},
		{
			name:    "until above column max is dropped",
			backend: defaultBackend,
			filter:  nostr.Filter{Until: tsPtr(9_999_999_999)},
			query: `SELECT id, pubkey, created_at, kind, tags, content, sig
			FROM event
			WHERE true
			ORDER BY created_at DESC, id LIMIT $1`,
			params: []any{100},
			err:    nil,
		},
		{
			name:    "out-of-range kind dropped from a mixed list",
			backend: defaultBackend,
			filter:  nostr.Filter{Kinds: []int{1, 9_999_999_999}},
			query: `SELECT id, pubkey, created_at, kind, tags, content, sig
			FROM event
			WHERE kind IN ($1)
			ORDER BY created_at DESC, id LIMIT $2`,
			params: []any{1, 100},
			err:    nil,
		},
		{
			name:    "all kinds out of range matches nothing",
			backend: defaultBackend,
			filter:  nostr.Filter{Kinds: []int{9_999_999_999}},
			query: `SELECT id, pubkey, created_at, kind, tags, content, sig
			FROM event
			WHERE false
			ORDER BY created_at DESC, id LIMIT $1`,
			params: []any{100},
			err:    nil,
		},
		{
			name:    "column boundary values are in range",
			backend: defaultBackend,
			filter:  nostr.Filter{Since: tsPtr(math.MinInt32), Until: tsPtr(math.MaxInt32)},
			query: `SELECT id, pubkey, created_at, kind, tags, content, sig
			FROM event
			WHERE created_at >= $1 AND created_at <= $2
			ORDER BY created_at DESC, id LIMIT $3`,
			params: []any{tsPtr(math.MinInt32), tsPtr(math.MaxInt32), 100},
			err:    nil,
		},
		// NIP-50 search: tsvector full-text by default, ILIKE substring when opted in.
		{
			name:    "search uses tsvector by default",
			backend: defaultBackend,
			filter:  nostr.Filter{Search: "hello"},
			query: `SELECT id, pubkey, created_at, kind, tags, content, sig
			FROM event
			WHERE to_tsvector($1, content) @@ plainto_tsquery($2, $3)
			ORDER BY created_at DESC, id LIMIT $4`,
			params: []any{"simple", "simple", "hello", 100},
			err:    nil,
		},
		{
			name:    "search uses ILIKE substring when SubstringSearch is set",
			backend: substringSearchBackend,
			filter:  nostr.Filter{Search: "東京"},
			query: `SELECT id, pubkey, created_at, kind, tags, content, sig
			FROM event
			WHERE content ILIKE $1 ESCAPE '\'
			ORDER BY created_at DESC, id LIMIT $2`,
			params: []any{"%東京%", 100},
			err:    nil,
		},
		{
			name:    "substring search escapes like wildcards",
			backend: substringSearchBackend,
			filter:  nostr.Filter{Search: `50%_x`},
			query: `SELECT id, pubkey, created_at, kind, tags, content, sig
			FROM event
			WHERE content ILIKE $1 ESCAPE '\'
			ORDER BY created_at DESC, id LIMIT $2`,
			params: []any{`%50\%\_x%`, 100},
			err:    nil,
		},
		{
			name:    "substring search ANDs each whitespace-separated term",
			backend: substringSearchBackend,
			filter:  nostr.Filter{Search: "東京 京都"},
			query: `SELECT id, pubkey, created_at, kind, tags, content, sig
			FROM event
			WHERE content ILIKE $1 ESCAPE '\' AND content ILIKE $2 ESCAPE '\'
			ORDER BY created_at DESC, id LIMIT $3`,
			params: []any{"%東京%", "%京都%", 100},
			err:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, params, err := tt.backend.queryEventsSql(tt.filter, false)
			assert.Equal(t, tt.err, err)
			if err != nil {
				return
			}

			assert.Equal(t, clean(tt.query), clean(query))
			assert.Equal(t, tt.params, params)
		})
	}
}

func clean(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(s, "\t", ""), "\n", ""), " ", "")
}

func intSlice(n int) []int {
	slice := make([]int, 0, n)
	for i := 0; i < n; i++ {
		slice = append(slice, i)
	}
	return slice
}

func strSlice(n int) []string {
	slice := make([]string, 0, n)
	for i := 0; i < n; i++ {
		slice = append(slice, strconv.Itoa(i))
	}
	return slice
}

func TestCountEventsFiltersBuildsAUnion(t *testing.T) {
	// NIP-45 OR's the filters together, so the ids are UNION'd -- not UNION
	// ALL'd -- to drop the events that match more than one of them.
	conditions1, params1, err := defaultBackend.filterConditions(nostr.Filter{Kinds: []int{1}})
	assert.NoError(t, err)
	conditions2, params2, err := defaultBackend.filterConditions(nostr.Filter{Kinds: []int{1, 7}})
	assert.NoError(t, err)

	assert.NotContains(t, strings.Join(conditions1, " "), "LIMIT",
		"conditions carry no LIMIT of their own")
	assert.Equal(t, []any{1}, params1)
	assert.Equal(t, []any{1, 7}, params2)

	// the placeholders have to be numbered across every filter, not restarted
	// for each one, so the whole statement is rebound at once
	query := "SELECT COUNT(*) FROM (" +
		"SELECT id FROM event WHERE " + strings.Join(conditions1, " AND ") + " UNION " +
		"SELECT id FROM event WHERE " + strings.Join(conditions2, " AND ") + ") AS matched"
	assert.Equal(t, 3, strings.Count(query, "?"))
}

func TestFilterConditionsOmitsLimit(t *testing.T) {
	// queryEventsSql appends the LIMIT parameter itself; filterConditions must
	// not, or a UNION built from it would bind the wrong values.
	conditions, params, err := defaultBackend.filterConditions(nostr.Filter{Kinds: []int{1}, Limit: 5})
	assert.NoError(t, err)
	assert.Equal(t, []any{1}, params)
	assert.NotContains(t, strings.Join(conditions, " "), "LIMIT")

	_, sqlParams, err := defaultBackend.queryEventsSql(nostr.Filter{Kinds: []int{1}, Limit: 5}, true)
	assert.NoError(t, err)
	assert.Equal(t, []any{1, 5}, sqlParams)
}
