package test

import (
	"context"
	"os"
	"testing"

	"fiatjaf.com/lib/combinations"
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

func FuzzSliceStore(f *testing.F) {
	runFuzzOn(f, &slicestore.SliceStore{})
}

func FuzzLMDB(f *testing.F) {
	os.RemoveAll(dbpath + "lmdb")
	runFuzzOn(f, &lmdb.LMDBBackend{Path: dbpath + "lmdb"})
}

func FuzzBadger(f *testing.F) {
	os.RemoveAll(dbpath + "badger")
	runFuzzOn(f, &badger.BadgerBackend{Path: dbpath + "badger"})
}

func FuzzSQLite(f *testing.F) {
	os.RemoveAll(dbpath + "sqlite")
	runFuzzOn(f, &sqlite3.SQLite3Backend{DatabaseURL: dbpath + "sqlite"})
}

func FuzzPostgres(f *testing.F) {
	postgres := embeddedpostgres.NewDatabase()
	err := postgres.Start()
	if err != nil {
		f.Fatalf("failed to start embedded postgres: %s", err)
		return
	}
	defer postgres.Stop()
	runFuzzOn(f, &postgresql.PostgresBackend{DatabaseURL: "postgres://postgres:postgres@:5432/postgres?sslmode=disable"})
}

func runFuzzOn(f *testing.F, db eventstore.Store) {
	tagNames := []string{"q", "e", "t", "whatever", "something"}
	keys := []string{sk3, sk4}

	f.Add(
		[]uint8{1},
		[]uint8{2, 4},
		[]uint8{1, 9}, "qwe12983ybasdkb13b",
	)
	f.Fuzz(func(t *testing.T,
		keyIdxs []uint8,
		kinds []uint8,
		tagNameIdxs []uint8, tagValuesSrc string,
	) {
		if len(keyIdxs) > 20 || len(kinds) > 20 || len(tagNameIdxs) > 20 || len(tagValuesSrc) > 200 {
			return
		}

		err := db.Init()
		require.NoError(t, err)

		ctx := context.Background()

		tagValues := make([]string, 1, len(tagValuesSrc)/3+1)
		for i := 0; i < len(tagValues); i++ {
			end := i*3 + 3
			if len(tagValuesSrc) < end {
				end = len(tagValuesSrc)
			}
			tagValues[i] = tagValuesSrc[i*3 : end]
		}

		// make filters (all possible combinations of filters)
		var filters nostr.Filters
		{
			elements := make([]nostr.Filter, 0, 12)
			for _, kind := range kinds {
				elements = append(elements, nostr.Filter{Kinds: []int{int(kind)}})
			}
			for _, keyIdx := range keyIdxs {
				pubkey, _ := nostr.GetPublicKey(keys[int(keyIdx)%len(keys)])
				elements = append(elements, nostr.Filter{Authors: []string{pubkey}})
			}
			for _, tagNameIdx := range tagNameIdxs {
				for _, tagValue := range tagValues {
					elements = append(elements, nostr.Filter{
						Tags: nostr.TagMap{tagNames[int(tagNameIdx)%len(tagNames)]: []string{tagValue}},
					})
				}
			}
			combs := make([][]nostr.Filter, 0, 200)
			for r := 1; r <= len(elements); r++ {
				combs = append(combs, combinations.Combinations(elements, r)...)
			}
			filters = make([]nostr.Filter, len(combs))
			for f, comb := range combs {
				// combine multiple filters into one
				filter := comb[0]
				for _, add := range filters[1:] {
					filter.Authors = append(filter.Authors, add.Authors...)
					filter.Kinds = append(filter.Kinds, add.Kinds...)
					if filter.Tags == nil {
						filter.Tags = make(nostr.TagMap)
					}
					for k, v := range add.Tags {
						filter.Tags[k] = append(filter.Tags[k], v...)
					}
				}
				filters[f] = filter
			}
		}

		// make events (all possible combinations of events)
		events := make([]nostr.Event, 0, len(keyIdxs)*len(kinds)*len(tagNameIdxs))
		{
			// all possible combinations of tags
			tagPossibilities := make([]nostr.Tag, 0, 12)
			for _, tagNameIdx := range tagNameIdxs {
				for _, tagValue := range tagValues {
					tagPossibilities = append(tagPossibilities,
						nostr.Tag{tagNames[int(tagNameIdx)%len(tagNames)], tagValue})
				}
			}
			tagCombinations := make([][]nostr.Tag, 0, 24)
			for r := 1; r <= len(tagPossibilities); r++ {
				tagCombinations = append(tagCombinations, combinations.Combinations(tagPossibilities, r)...)
			}

			emptyEvent := nostr.Event{}
			for _, kind := range kinds {
				eventWithKind := emptyEvent
				eventWithKind.Kind = int(kind)

				for _, keyIdx := range keyIdxs {
					eventWithPubKey := eventWithKind
					eventWithPubKey.PubKey, _ = nostr.GetPublicKey(keys[int(keyIdx)%len(keys)])

					for _, tags := range tagCombinations {
						eventWithTags := eventWithPubKey
						eventWithTags.Tags = tags

						events = append(events, eventWithTags)
					}
				}
			}
		}

		// insert
		for _, evt := range events {
			err = db.SaveEvent(ctx, &evt)
			require.NoError(t, err)
		}

		// query
		w := eventstore.RelayWrapper{Store: db}

		for _, filter := range filters {
			localQueryResults := make([]*nostr.Event, 0, len(events))
			for _, evt := range events {
				if filter.Matches(&evt) {
					localQueryResults = append(localQueryResults, &evt)
				}
			}

			results, err := w.QuerySync(ctx, nostr.Filter{})
			require.NoError(t, err)
			require.ElementsMatch(t,
				localQueryResults,
				results,
			)
		}
	})
}
