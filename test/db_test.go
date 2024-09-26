package test

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/fiatjaf/eventstore"
	"github.com/fiatjaf/eventstore/badger"
	"github.com/fiatjaf/eventstore/lmdb"
	"github.com/fiatjaf/eventstore/postgresql"
	"github.com/fiatjaf/eventstore/slicestore"
	"github.com/fiatjaf/eventstore/sqlite3"
	"github.com/nbd-wtf/go-nostr"
	"github.com/stretchr/testify/require"
)

const (
	dbpath = "/tmp/eventstore-test"
	sk3    = "0000000000000000000000000000000000000000000000000000000000000003"
	sk4    = "0000000000000000000000000000000000000000000000000000000000000004"
)

func TestSliceStore(t *testing.T) {
	t.Run("first", func(t *testing.T) {
		runTestOn(t, &slicestore.SliceStore{})
	})
	t.Run("second", func(t *testing.T) {
		runSecondTestOn(t, &slicestore.SliceStore{})
	})
}

func TestLMDB(t *testing.T) {
	os.RemoveAll(dbpath + "lmdb")
	t.Run("first", func(t *testing.T) {
		runTestOn(t, &lmdb.LMDBBackend{Path: dbpath + "lmdb"})
	})
	os.RemoveAll(dbpath + "lmdb")
	t.Run("second", func(t *testing.T) {
		runSecondTestOn(t, &lmdb.LMDBBackend{Path: dbpath + "lmdb"})
	})
}

func TestBadger(t *testing.T) {
	os.RemoveAll(dbpath + "badger")
	t.Run("first", func(t *testing.T) {
		runTestOn(t, &badger.BadgerBackend{Path: dbpath + "badger"})
	})
	os.RemoveAll(dbpath + "badger")
	t.Run("second", func(t *testing.T) {
		runSecondTestOn(t, &badger.BadgerBackend{Path: dbpath + "badger"})
	})
}

func TestSQLite(t *testing.T) {
	os.RemoveAll(dbpath + "sqlite")
	t.Run("first", func(t *testing.T) {
		runTestOn(t, &sqlite3.SQLite3Backend{DatabaseURL: dbpath + "sqlite", QueryTagsLimit: 50})
	})
	os.RemoveAll(dbpath + "sqlite")
	t.Run("second", func(t *testing.T) {
		runSecondTestOn(t, &sqlite3.SQLite3Backend{DatabaseURL: dbpath + "sqlite", QueryTagsLimit: 50})
	})
}

func TestPostgres(t *testing.T) {
	postgres := embeddedpostgres.NewDatabase()
	err := postgres.Start()
	if err != nil {
		t.Fatalf("failed to start embedded postgres: %s", err)
		return
	}
	defer postgres.Stop()
	runTestOn(t, &postgresql.PostgresBackend{DatabaseURL: "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable", QueryTagsLimit: 50})
	runSecondTestOn(t, &postgresql.PostgresBackend{DatabaseURL: "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable", QueryTagsLimit: 50})
}

var ctx = context.Background()

func runTestOn(t *testing.T, db eventstore.Store) {
	err := db.Init()
	require.NoError(t, err)

	allEvents := make([]*nostr.Event, 0, 10)

	// insert
	for i := 0; i < 10; i++ {
		evt := &nostr.Event{
			CreatedAt: nostr.Timestamp(i*10 + 2),
			Content:   fmt.Sprintf("hello %d", i),
			Tags: nostr.Tags{
				{"t", fmt.Sprintf("%d", i)},
				{"e", "0" + strconv.Itoa(i) + strings.Repeat("0", 62)},
			},
			Kind: 1,
		}
		sk := sk3
		if i%3 == 0 {
			sk = sk4
		}
		if i%2 == 0 {
			evt.Kind = 9
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

	{
		results, err := w.QuerySync(ctx, nostr.Filter{Kinds: []int{1}})
		require.NoError(t, err)
		require.ElementsMatch(t,
			[]*nostr.Event{allEvents[1], allEvents[3], allEvents[5], allEvents[7], allEvents[9]},
			results,
			"kind query error")
	}

	{
		results, err := w.QuerySync(ctx, nostr.Filter{Kinds: []int{9}})
		require.NoError(t, err)
		require.ElementsMatch(t,
			[]*nostr.Event{allEvents[0], allEvents[2], allEvents[4], allEvents[6], allEvents[8]},
			results,
			"second kind query error")
	}

	{
		pk4, _ := nostr.GetPublicKey(sk4)
		results, err := w.QuerySync(ctx, nostr.Filter{Authors: []string{pk4}})
		require.NoError(t, err)
		require.ElementsMatch(t,
			[]*nostr.Event{allEvents[0], allEvents[3], allEvents[6], allEvents[9]},
			results,
			"pubkey query error")
	}

	{
		pk3, _ := nostr.GetPublicKey(sk3)
		results, err := w.QuerySync(ctx, nostr.Filter{Kinds: []int{9}, Authors: []string{pk3}})
		require.NoError(t, err)
		require.ElementsMatch(t,
			[]*nostr.Event{allEvents[2], allEvents[4], allEvents[8]},
			results,
			"pubkey kind query error")
	}

	{
		pk3, _ := nostr.GetPublicKey(sk3)
		pk4, _ := nostr.GetPublicKey(sk4)
		results, err := w.QuerySync(ctx, nostr.Filter{Kinds: []int{9, 5, 7}, Authors: []string{pk3, pk4, pk4[1:] + "a"}})
		require.NoError(t, err)
		require.ElementsMatch(t,
			[]*nostr.Event{allEvents[0], allEvents[2], allEvents[4], allEvents[6], allEvents[8]},
			results,
			"2 pubkeys and kind query error")
	}

	{
		results, err := w.QuerySync(ctx, nostr.Filter{Tags: nostr.TagMap{"t": []string{"2", "4", "6"}}})
		require.NoError(t, err)
		require.ElementsMatch(t,
			[]*nostr.Event{allEvents[2], allEvents[4], allEvents[6]},
			results,
			"tag query error")
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
			"second open-ended query error")
	}

	{
		results, err := w.QuerySync(ctx, nostr.Filter{Tags: nostr.TagMap{"t": []string{"2", "6"}}})
		require.NoError(t, err)
		require.ElementsMatch(t,
			[]*nostr.Event{allEvents[2], allEvents[6]},
			results,
			"second tag query error")
	}

	{
		results, err := w.QuerySync(ctx, nostr.Filter{Tags: nostr.TagMap{"e": []string{allEvents[3].Tags[1][1]}}})
		require.NoError(t, err)
		require.ElementsMatch(t,
			[]*nostr.Event{allEvents[3]},
			results,
			"'e' tag query error")
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

	// test p-tag querying
	{
		p := "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
		p2 := "2eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

		newEvents := []*nostr.Event{
			{Tags: nostr.Tags{nostr.Tag{"p", p}}, Kind: 1984, CreatedAt: nostr.Timestamp(100), Content: "first"},
			{Tags: nostr.Tags{nostr.Tag{"p", p}, nostr.Tag{"t", "x"}}, Kind: 1984, CreatedAt: nostr.Timestamp(101), Content: "middle"},
			{Tags: nostr.Tags{nostr.Tag{"p", p}}, Kind: 1984, CreatedAt: nostr.Timestamp(102), Content: "last"},
			{Tags: nostr.Tags{nostr.Tag{"p", p}}, Kind: 1111, CreatedAt: nostr.Timestamp(101), Content: "bulufas"},
			{Tags: nostr.Tags{nostr.Tag{"p", p}}, Kind: 1111, CreatedAt: nostr.Timestamp(102), Content: "safulub"},
			{Tags: nostr.Tags{nostr.Tag{"p", p}}, Kind: 1, CreatedAt: nostr.Timestamp(103), Content: "bololo"},
			{Tags: nostr.Tags{nostr.Tag{"p", p2}}, Kind: 1, CreatedAt: nostr.Timestamp(104), Content: "wololo"},
			{Tags: nostr.Tags{nostr.Tag{"p", p}, nostr.Tag{"p", p2}}, Kind: 1, CreatedAt: nostr.Timestamp(104), Content: "trololo"},
		}

		sk := nostr.GeneratePrivateKey()
		for _, newEvent := range newEvents {
			newEvent.Sign(sk)
			require.NoError(t, db.SaveEvent(ctx, newEvent))
		}

		{
			results, err := w.QuerySync(ctx, nostr.Filter{
				Tags:  nostr.TagMap{"p": []string{p}},
				Kinds: []int{1984},
				Limit: 2,
			})
			require.NoError(t, err)
			require.ElementsMatch(t,
				[]*nostr.Event{newEvents[2], newEvents[1]},
				results,
				"'p' tag 1 query error")
		}

		{
			results, err := w.QuerySync(ctx, nostr.Filter{
				Tags:  nostr.TagMap{"p": []string{p}, "t": []string{"x"}},
				Limit: 4,
			})
			require.NoError(t, err)
			require.ElementsMatch(t,
				// the results won't be in canonical time order because this query is too awful, needs a kind
				[]*nostr.Event{newEvents[1]},
				results,
				"'p' tag 2 query error")
		}

		{
			results, err := w.QuerySync(ctx, nostr.Filter{
				Tags:  nostr.TagMap{"p": []string{p, p2}},
				Kinds: []int{1},
				Limit: 4,
			})
			require.NoError(t, err)
			require.ElementsMatch(t,
				// the results won't be in canonical time order because this query is too awful, needs a kind
				[]*nostr.Event{newEvents[5], newEvents[6], newEvents[7]},
				results,
				"'p' tag 3 query error")
		}
	}
}

func runSecondTestOn(t *testing.T, db eventstore.Store) {
	db.Init()

	for i := 0; i < 10000; i++ {
		eTag := make([]byte, 32)
		binary.BigEndian.PutUint16(eTag, uint16(i))

		ref, _ := nostr.GetPublicKey(sk3)
		if i%3 == 0 {
			ref, _ = nostr.GetPublicKey(sk4)
		}

		evt := &nostr.Event{
			CreatedAt: nostr.Timestamp(i*10 + 2),
			Content:   fmt.Sprintf("hello %d", i),
			Tags: nostr.Tags{
				{"t", fmt.Sprintf("t%d", i)},
				{"e", hex.EncodeToString(eTag)},
				{"p", ref},
			},
			Kind: i % 10,
		}
		sk := sk3
		if i%3 == 0 {
			sk = sk4
		}
		evt.Sign(sk)
		err := db.SaveEvent(ctx, evt)
		require.NoError(t, err)
	}

	w := eventstore.RelayWrapper{Store: db}
	pk3, _ := nostr.GetPublicKey(sk3)
	pk4, _ := nostr.GetPublicKey(sk4)
	eTags := make([]string, 20)
	for i := 0; i < 20; i++ {
		eTag := make([]byte, 32)
		binary.BigEndian.PutUint16(eTag, uint16(i))
		eTags[i] = hex.EncodeToString(eTag)
	}

	filters := make([]nostr.Filter, 0, 10)
	filters = append(filters, nostr.Filter{Kinds: []int{1, 4, 8, 16}})
	filters = append(filters, nostr.Filter{Authors: []string{pk3, nostr.GeneratePrivateKey()}})
	filters = append(filters, nostr.Filter{Authors: []string{pk3, nostr.GeneratePrivateKey()}, Kinds: []int{3, 4}})
	filters = append(filters, nostr.Filter{})
	filters = append(filters, nostr.Filter{Limit: 20})
	filters = append(filters, nostr.Filter{Kinds: []int{8, 9}, Tags: nostr.TagMap{"p": []string{pk3}}})
	filters = append(filters, nostr.Filter{Kinds: []int{8, 9}, Tags: nostr.TagMap{"p": []string{pk3, pk4}}})
	filters = append(filters, nostr.Filter{Kinds: []int{8, 9}, Tags: nostr.TagMap{"p": []string{pk3, pk4}}})
	filters = append(filters, nostr.Filter{Kinds: []int{9}, Tags: nostr.TagMap{"e": eTags}})
	filters = append(filters, nostr.Filter{Kinds: []int{5}, Tags: nostr.TagMap{"e": eTags, "t": []string{"t5"}}})
	filters = append(filters, nostr.Filter{Tags: nostr.TagMap{"e": eTags}})
	filters = append(filters, nostr.Filter{Tags: nostr.TagMap{"e": eTags}, Limit: 50})

	t.Run("filter", func(t *testing.T) {
		for q, filter := range filters {
			q := q
			filter := filter
			label := fmt.Sprintf("filter %d: %s", q, filter)

			t.Run(fmt.Sprintf("q-%d", q), func(t *testing.T) {
				results, err := w.QuerySync(ctx, filter)
				require.NoError(t, err, filter)
				require.NotEmpty(t, results, label)
			})
		}
	})
}
