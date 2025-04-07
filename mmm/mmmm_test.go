package mmm

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"testing"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/nbd-wtf/go-nostr"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestMultiLayerIndexing(t *testing.T) {
	// Create a temporary directory for the test
	tmpDir := "/tmp/eventstore-mmm-test"
	os.RemoveAll(tmpDir)

	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr})

	// initialize MMM with three layers:
	// 1. odd timestamps layer
	// 2. even timestamps layer
	// 3. all events layer
	mmm := &MultiMmapManager{
		Dir:    tmpDir,
		Logger: &logger,
	}

	err := mmm.Init()
	require.NoError(t, err)
	defer mmm.Close()

	// create layers
	err = mmm.EnsureLayer("odd", &IndexingLayer{
		MaxLimit: 100,
		ShouldIndex: func(ctx context.Context, evt *nostr.Event) bool {
			return evt.CreatedAt%2 == 1
		},
	})
	require.NoError(t, err)
	err = mmm.EnsureLayer("even", &IndexingLayer{
		MaxLimit: 100,
		ShouldIndex: func(ctx context.Context, evt *nostr.Event) bool {
			return evt.CreatedAt%2 == 0
		},
	})
	require.NoError(t, err)
	err = mmm.EnsureLayer("all", &IndexingLayer{
		MaxLimit: 100,
		ShouldIndex: func(ctx context.Context, evt *nostr.Event) bool {
			return true
		},
	})
	require.NoError(t, err)

	// create test events
	ctx := context.Background()
	baseTime := nostr.Timestamp(0)
	sk := "945e01e37662430162121b804d3645a86d97df9d256917d86735d0eb219393eb"
	events := make([]*nostr.Event, 10)
	for i := 0; i < 10; i++ {
		evt := &nostr.Event{
			CreatedAt: baseTime + nostr.Timestamp(i),
			Kind:      1,
			Tags:      nostr.Tags{},
			Content:   "test content",
		}
		evt.Sign(sk)
		events[i] = evt
		stored, err := mmm.StoreGlobal(ctx, evt)
		require.NoError(t, err)
		require.True(t, stored)
	}

	{
		// query odd layer
		oddResults, err := mmm.layers[0].QueryEvents(ctx, nostr.Filter{
			Kinds: []int{1},
		})
		require.NoError(t, err)

		oddCount := 0
		for evt := range oddResults {
			require.Equal(t, evt.CreatedAt%2, nostr.Timestamp(1))
			oddCount++
		}
		require.Equal(t, 5, oddCount)
	}

	{
		// query even layer
		evenResults, err := mmm.layers[1].QueryEvents(ctx, nostr.Filter{
			Kinds: []int{1},
		})
		require.NoError(t, err)

		evenCount := 0
		for evt := range evenResults {
			require.Equal(t, evt.CreatedAt%2, nostr.Timestamp(0))
			evenCount++
		}
		require.Equal(t, 5, evenCount)
	}

	{
		// query all layer
		allResults, err := mmm.layers[2].QueryEvents(ctx, nostr.Filter{
			Kinds: []int{1},
		})
		require.NoError(t, err)

		allCount := 0
		for range allResults {
			allCount++
		}
		require.Equal(t, 10, allCount)
	}

	// delete some events
	err = mmm.layers[0].DeleteEvent(ctx, events[1]) // odd timestamp
	require.NoError(t, err)
	err = mmm.layers[1].DeleteEvent(ctx, events[2]) // even timestamp

	// verify deletions
	{
		oddResults, err := mmm.layers[0].QueryEvents(ctx, nostr.Filter{
			Kinds: []int{1},
		})
		require.NoError(t, err)
		oddCount := 0
		for range oddResults {
			oddCount++
		}
		require.Equal(t, 4, oddCount)
	}

	{
		evenResults, err := mmm.layers[1].QueryEvents(ctx, nostr.Filter{
			Kinds: []int{1},
		})
		require.NoError(t, err)
		evenCount := 0
		for range evenResults {
			evenCount++
		}
		require.Equal(t, 4, evenCount)
	}

	{
		allResults, err := mmm.layers[2].QueryEvents(ctx, nostr.Filter{
			Kinds: []int{1},
		})
		require.NoError(t, err)
		allCount := 0
		for range allResults {
			allCount++
		}
		require.Equal(t, 10, allCount)
	}

	// save events directly to layers regardless of timestamp
	{
		oddEvent := &nostr.Event{
			CreatedAt: baseTime + 100, // even timestamp
			Kind:      1,
			Content:   "forced odd",
		}
		oddEvent.Sign(sk)
		err = mmm.layers[0].SaveEvent(ctx, oddEvent) // save even timestamp to odd layer
		require.NoError(t, err)

		// it is added to the odd il
		oddResults, err := mmm.layers[0].QueryEvents(ctx, nostr.Filter{
			Kinds: []int{1},
		})
		require.NoError(t, err)
		oddCount := 0
		for range oddResults {
			oddCount++
		}
		require.Equal(t, 5, oddCount)

		// it doesn't affect the event il
		evenResults, err := mmm.layers[1].QueryEvents(ctx, nostr.Filter{
			Kinds: []int{1},
		})
		require.NoError(t, err)
		evenCount := 0
		for range evenResults {
			evenCount++
		}
		require.Equal(t, 4, evenCount)
	}

	// test replaceable events
	for _, layer := range mmm.layers {
		replaceable := &nostr.Event{
			CreatedAt: baseTime + 0,
			Kind:      0,
			Content:   fmt.Sprintf("first"),
		}
		replaceable.Sign(sk)
		err := layer.ReplaceEvent(ctx, replaceable)
		require.NoError(t, err)
	}

	// replace events alternating between layers
	for i := range mmm.layers {
		content := fmt.Sprintf("last %d", i)

		newEvt := &nostr.Event{
			CreatedAt: baseTime + 1000,
			Kind:      0,
			Content:   content,
		}
		newEvt.Sign(sk)

		layer := mmm.layers[i]
		err = layer.ReplaceEvent(ctx, newEvt)
		require.NoError(t, err)

		// verify replacement in the layer that did it
		results, err := layer.QueryEvents(ctx, nostr.Filter{
			Kinds: []int{0},
		})
		require.NoError(t, err)

		count := 0
		for evt := range results {
			require.Equal(t, content, evt.Content)
			count++
		}
		require.Equal(t, 1, count)

		// verify other layers still have the old version
		for j := 0; j < 3; j++ {
			if mmm.layers[j] == layer {
				continue
			}
			results, err := mmm.layers[j].QueryEvents(ctx, nostr.Filter{
				Kinds: []int{0},
			})
			require.NoError(t, err)

			count := 0
			for evt := range results {
				if i < j {
					require.Equal(t, "first", evt.Content)
				} else {
					require.Equal(t, evt.Content, fmt.Sprintf("last %d", j))
				}
				count++
			}

			require.Equal(t, 1, count, "%d/%d", i, j)
		}
	}
}

func TestLayerReferenceTracking(t *testing.T) {
	// Create a temporary directory for the test
	tmpDir, err := os.MkdirTemp("", "mmm-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr})

	// initialize MMM with three layers
	mmm := &MultiMmapManager{
		Dir:    tmpDir,
		Logger: &logger,
	}

	err = mmm.Init()
	require.NoError(t, err)
	defer mmm.Close()

	// create three layers
	err = mmm.EnsureLayer("layer1", &IndexingLayer{
		MaxLimit:    100,
		ShouldIndex: func(ctx context.Context, evt *nostr.Event) bool { return true },
	})
	require.NoError(t, err)
	err = mmm.EnsureLayer("layer2", &IndexingLayer{
		MaxLimit:    100,
		ShouldIndex: func(ctx context.Context, evt *nostr.Event) bool { return true },
	})
	require.NoError(t, err)
	err = mmm.EnsureLayer("layer3", &IndexingLayer{
		MaxLimit:    100,
		ShouldIndex: func(ctx context.Context, evt *nostr.Event) bool { return true },
	})
	require.NoError(t, err)
	err = mmm.EnsureLayer("layer4", &IndexingLayer{
		MaxLimit:    100,
		ShouldIndex: func(ctx context.Context, evt *nostr.Event) bool { return true },
	})
	require.NoError(t, err)

	// create test events
	ctx := context.Background()
	sk := "945e01e37662430162121b804d3645a86d97df9d256917d86735d0eb219393eb"
	evt1 := &nostr.Event{
		CreatedAt: 1000,
		Kind:      1,
		Tags:      nostr.Tags{},
		Content:   "event 1",
	}
	evt1.Sign(sk)

	evt2 := &nostr.Event{
		CreatedAt: 2000,
		Kind:      1,
		Tags:      nostr.Tags{},
		Content:   "event 2",
	}
	evt2.Sign(sk)

	// save evt1 to layer1
	err = mmm.layers[0].SaveEvent(ctx, evt1)
	require.NoError(t, err)

	// save evt1 to layer2
	err = mmm.layers[1].SaveEvent(ctx, evt1)
	require.NoError(t, err)

	// save evt1 to layer4
	err = mmm.layers[0].SaveEvent(ctx, evt1)
	require.NoError(t, err)

	// delete evt1 from layer1
	err = mmm.layers[0].DeleteEvent(ctx, evt1)
	require.NoError(t, err)

	// save evt2 to layer3
	err = mmm.layers[2].SaveEvent(ctx, evt2)
	require.NoError(t, err)

	// save evt2 to layer4
	err = mmm.layers[3].SaveEvent(ctx, evt2)
	require.NoError(t, err)

	// save evt2 to layer3 again
	err = mmm.layers[2].SaveEvent(ctx, evt2)
	require.NoError(t, err)

	// delete evt1 from layer4
	err = mmm.layers[3].DeleteEvent(ctx, evt1)
	require.NoError(t, err)

	// verify the state of the indexId database
	err = mmm.lmdbEnv.View(func(txn *lmdb.Txn) error {
		cursor, err := txn.OpenCursor(mmm.indexId)
		if err != nil {
			return err
		}
		defer cursor.Close()

		count := 0
		for k, v, err := cursor.Get(nil, nil, lmdb.First); err == nil; k, v, err = cursor.Get(nil, nil, lmdb.Next) {
			count++
			if hex.EncodeToString(k) == evt1.ID[:16] {
				// evt1 should only reference layer2
				require.Equal(t, 14, len(v), "evt1 should have one layer reference")
				layerRef := binary.BigEndian.Uint16(v[12:14])
				require.Equal(t, mmm.layers[1].id, layerRef, "evt1 should reference layer2")
			} else if hex.EncodeToString(k) == evt2.ID[:16] {
				// evt2 should references to layer3 and layer4
				require.Equal(t, 16, len(v), "evt2 should have two layer references")
				layer3Ref := binary.BigEndian.Uint16(v[12:14])
				require.Equal(t, mmm.layers[2].id, layer3Ref, "evt2 should reference layer3")
				layer4Ref := binary.BigEndian.Uint16(v[14:16])
				require.Equal(t, mmm.layers[3].id, layer4Ref, "evt2 should reference layer4")
			} else {
				t.Errorf("unexpected event in indexId: %x", k)
			}
		}
		require.Equal(t, 2, count, "should have exactly two events in indexId")
		return nil
	})
	require.NoError(t, err)
}
