package eventstore

import (
	"context"

	"github.com/nbd-wtf/go-nostr"
)

// Store is a persistence layer for nostr events handled by a relay.
type Store interface {
	// Init is called at the very beginning by [Server.Start], after [Relay.Init],
	// allowing a storage to initialize its internal resources.
	Init() error

	// Close must be called after you're done using the store, to free up resources and so on.
	Close()

	// QueryEvents should return a channel with the events as they're recovered from a database.
	//   the channel should be closed after the events are all delivered.
	QueryEvents(context.Context, nostr.Filter) (chan *nostr.Event, error)
	// DeleteEvent just deletes an event, no side-effects.
	DeleteEvent(context.Context, *nostr.Event) error
	// SaveEvent just saves an event, no side-effects.
	SaveEvent(context.Context, *nostr.Event) error
	// ReplaceEvent atomically replaces a replaceable or addressable event.
	// Conceptually it is like a Query->Delete->Save, but streamlined.
	ReplaceEvent(context.Context, *nostr.Event) error
}

type Counter interface {
	CountEvents(context.Context, nostr.Filter) (int64, error)
}

// VanishPubkey is an optional interface that stores can implement to support NIP-62.
// It deletes all events from a pubkey up to a given timestamp.
type VanishPubkey interface {
	// VanishPubkey deletes all events from the given pubkey created before or at the given timestamp.
	// This includes all event kinds, and the deleted events should not be re-broadcastable.
	VanishPubkey(ctx context.Context, pubkey string, until int64) error
}
