package test

import (
	"context"
	"fmt"
	"os"
	"slices"
	"testing"

	"github.com/fiatjaf/eventstore"
	"github.com/fiatjaf/eventstore/badger"
	"github.com/fiatjaf/eventstore/bolt"
	"github.com/fiatjaf/eventstore/lmdb"
	"github.com/fiatjaf/eventstore/slicestore"
	"github.com/nbd-wtf/go-nostr"
	"github.com/stretchr/testify/require"
)

const (
	dbpath = "/tmp/eventstore-test"
	sk     = "0000000000000000000000000000000000000000000000000000000000000003"
)

func TestSliceStore(t *testing.T) {
	runTestOn(t, &slicestore.SliceStore{})
}

func TestLMDB(t *testing.T) {
	os.RemoveAll(dbpath + "lmdb")
	runTestOn(t, &lmdb.LMDBBackend{Path: dbpath + "lmdb"})
}

func TestBadger(t *testing.T) {
	os.RemoveAll(dbpath + "badger")
	runTestOn(t, &badger.BadgerBackend{Path: dbpath + "badger"})
}

func TestBolt(t *testing.T) {
	os.RemoveAll(dbpath + "bolt")
	runTestOn(t, &bolt.BoltBackend{Path: dbpath + "bolt"})
}

func runTestOn(t *testing.T, db eventstore.Store) {
	err := db.Init()
	require.NoError(t, err)

	ctx := context.Background()

	allEvents := make([]*nostr.Event, 0, 10)

	// insert
	for i := 0; i < 10; i++ {
		evt := &nostr.Event{
			CreatedAt: nostr.Timestamp(i*10 + 2),
			Content:   fmt.Sprintf("hello %d", i),
			Tags:      nostr.Tags{{"n", fmt.Sprintf("%d", i)}},
			Kind:      1,
		}
		evt.Sign(sk)
		allEvents = append(allEvents, evt)
		err = db.SaveEvent(ctx, evt)
		require.NoError(t, err)
	}

	// query
	w := eventstore.RelayWrapper{Store: db}
	{
		results, err := w.QuerySync(ctx, nostr.Filter{})
		require.NoError(t, err)
		require.ElementsMatch(t,
			allEvents,
			results,
			"open-ended query results error")
	}

	{
		results, err := w.QuerySync(ctx, nostr.Filter{Tags: nostr.TagMap{"n": []string{"2", "4", "6"}}})
		require.NoError(t, err)
		require.ElementsMatch(t,
			[]*nostr.Event{allEvents[2], allEvents[4], allEvents[6]},
			results,
			"tag query results error")
	}

	{
		for i := 0; i < 10; i++ {
			since := nostr.Timestamp(i*10 + 1)
			results, err := w.QuerySync(ctx, nostr.Filter{Since: &since})
			require.NoError(t, err)
			require.ElementsMatch(t,
				allEvents[i:],
				results,
				"since query results error %d", i)
		}
	}

	{
		results, err := w.QuerySync(ctx, nostr.Filter{IDs: []string{allEvents[7].ID, allEvents[9].ID}})
		require.NoError(t, err)
		require.ElementsMatch(t,
			[]*nostr.Event{allEvents[7], allEvents[9]},
			results,
			"id query error")
	}

	// delete
	require.NoError(t, db.DeleteEvent(ctx, allEvents[4]), "delete 1 error")
	require.NoError(t, db.DeleteEvent(ctx, allEvents[5]), "delete 2 error")

	// query again
	{
		results, err := w.QuerySync(ctx, nostr.Filter{})
		require.NoError(t, err)
		require.ElementsMatch(t,
			slices.Concat(allEvents[0:4], allEvents[6:]),
			results,
			"second open-ended query results error")
	}

	{
		results, err := w.QuerySync(ctx, nostr.Filter{Tags: nostr.TagMap{"n": []string{"2", "6"}}})
		require.NoError(t, err)
		require.ElementsMatch(t,
			[]*nostr.Event{allEvents[2], allEvents[6]},
			results,
			"second tag query results error")
	}

	{
		for i := 0; i < 4; i++ {
			until := nostr.Timestamp(i*10 + 1)
			results, err := w.QuerySync(ctx, nostr.Filter{Until: &until})
			require.NoError(t, err)

			require.ElementsMatch(t,
				allEvents[:i],
				results,
				"until query results error %d", i)
		}
	}
}
