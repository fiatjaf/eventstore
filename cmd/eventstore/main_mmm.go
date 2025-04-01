//go:build !windows

package main

import (
	"context"
	"os"
	"path/filepath"

	"github.com/fiatjaf/eventstore"
	"github.com/fiatjaf/eventstore/mmm"
	"github.com/nbd-wtf/go-nostr"
	"github.com/rs/zerolog"
)

func doMmmInit(path string) (eventstore.Store, error) {
	logger := zerolog.New(zerolog.NewConsoleWriter(func(w *zerolog.ConsoleWriter) {
		w.Out = os.Stderr
	}))
	mmmm := mmm.MultiMmapManager{
		Dir:    filepath.Dir(path),
		Logger: &logger,
	}
	if err := mmmm.Init(); err != nil {
		return nil, err
	}
	il := &mmm.IndexingLayer{
		ShouldIndex: func(ctx context.Context, e *nostr.Event) bool { return false },
	}
	if err := mmmm.EnsureLayer(filepath.Base(path), il); err != nil {
		return nil, err
	}
	return il, nil
}
