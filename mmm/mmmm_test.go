package mmm

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/nbd-wtf/go-nostr"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestMultiLayerIndexing(t *testing.T) {
	// Create a temporary directory for the test
	tmpDir, err := os.MkdirTemp("", "mmm-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr})

	// initialize MMM with two layers:
	// 1. odd timestamps layer
	// 2. even timestamps layer
	mmm := &MultiMmapManager{
		Dir:    tmpDir,
		Logger: &logger,
		LayerBuilder: func(name string, b *MultiMmapManager) *IndexingLayer {
			return &IndexingLayer{
				dbpath:   filepath.Join(tmpDir, name),
				mmmm:     b,
				MaxLimit: 100,
				ShouldIndex: func(ctx context.Context, evt *nostr.Event) bool {
					switch name {
					case "odd":
						return evt.CreatedAt%2 == 1
					case "even":
						return evt.CreatedAt%2 == 0
					}
					return false
				},
			}
		},
	}

	err = mmm.Init()
	require.NoError(t, err)
	defer mmm.Close()

	// create odd timestamps layer
	err = mmm.CreateLayer("odd")
	require.NoError(t, err)

	// create even timestamps layer
	err = mmm.CreateLayer("even")
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

	// query odd layer
	oddResults, err := mmm.layers[0].QueryEvents(ctx, nostr.Filter{
		Kinds: []int{1},
		Since: &baseTime,
	})
	require.NoError(t, err)

	oddCount := 0
	for evt := range oddResults {
		require.Equal(t, evt.CreatedAt%2, nostr.Timestamp(1))
		oddCount++
	}
	require.Equal(t, 5, oddCount)

	// query even layer
	evenResults, err := mmm.layers[1].QueryEvents(ctx, nostr.Filter{
		Kinds: []int{1},
		Since: &baseTime,
	})
	require.NoError(t, err)

	evenCount := 0
	for evt := range evenResults {
		require.Equal(t, evt.CreatedAt%2, nostr.Timestamp(0))
		evenCount++
	}
	require.Equal(t, 5, evenCount)
}
