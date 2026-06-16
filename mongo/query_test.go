package mongo

import (
	"testing"

	"github.com/nbd-wtf/go-nostr"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestQueryEventsUsesCreatedAtForTimeFilters(t *testing.T) {
	backend := &MongoDBBackend{
		QueryLimit:        queryLimit,
		QueryIDsLimit:     queryIDsLimit,
		QueryAuthorsLimit: queryAuthorsLimit,
		QueryKindsLimit:   queryKindsLimit,
		QueryTagsLimit:    queryTagsLimit,
	}
	since := nostr.Timestamp(10)
	until := nostr.Timestamp(20)

	conditions, _, err := backend.queryEvents(nostr.Filter{
		Since: &since,
		Until: &until,
	}, false)

	require.NoError(t, err)
	require.Contains(t, conditions, bson.E{
		Key:   "createdat",
		Value: bson.M{"$gte": since},
	})
	require.Contains(t, conditions, bson.E{
		Key:   "createdat",
		Value: bson.M{"$lte": until},
	})
	require.NotContains(t, conditions, bson.E{
		Key:   "since",
		Value: bson.M{"$gte": &since},
	})
	require.NotContains(t, conditions, bson.E{
		Key:   "until",
		Value: bson.M{"$lte": &until},
	})
}

func TestQueryEventsUsesTagsFieldForTagFilters(t *testing.T) {
	backend := &MongoDBBackend{
		QueryLimit:        queryLimit,
		QueryIDsLimit:     queryIDsLimit,
		QueryAuthorsLimit: queryAuthorsLimit,
		QueryKindsLimit:   queryKindsLimit,
		QueryTagsLimit:    queryTagsLimit,
	}

	conditions, _, err := backend.queryEvents(nostr.Filter{
		Tags: nostr.TagMap{"p": []string{"pubkey1", "pubkey2"}},
	}, false)

	require.NoError(t, err)
	require.Contains(t, conditions, bson.E{
		Key: "tags",
		Value: bson.M{"$elemMatch": bson.M{
			"0": "p",
			"1": bson.M{"$in": bson.A{"pubkey1", "pubkey2"}},
		}},
	})
	require.NotContains(t, conditions, bson.E{
		Key:   "colors",
		Value: bson.M{"$in": bson.A{"pubkey1", "pubkey2"}},
	})
}
