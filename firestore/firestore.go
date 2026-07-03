package firestore

import (
	"encoding/json"
	"sync"

	"cloud.google.com/go/firestore"
	"github.com/fiatjaf/eventstore"
	"github.com/nbd-wtf/go-nostr"
)

var (
	_ eventstore.Store   = (*FirestoreBackend)(nil)
	_ eventstore.Counter = (*FirestoreBackend)(nil)
)

// FirestoreBackend is an eventstore.Store backed by Google Cloud Firestore
// (Native mode).
//
// Firestore's query language is much more restrictive than SQL: a single query
// may only carry one disjunctive ("in" / "array-contains-any") clause and one
// range field, and every composite of equality/array + range + order-by needs a
// pre-declared composite index (see firestore.indexes.json). Because of that
// this backend pushes down only the created_at range plus the single most
// selective dimension of a nostr.Filter and enforces the remaining constraints
// client-side with filter.Matches, so results are always correct while the
// push-down keeps the number of billed document reads down.
type FirestoreBackend struct {
	sync.Mutex
	*firestore.Client

	// ProjectID is the Google Cloud project that owns the Firestore database.
	// Required. Credentials come from Application Default Credentials, or from
	// the FIRESTORE_EMULATOR_HOST environment variable when set.
	ProjectID string
	// Collection is the Firestore collection events are stored in. Defaults to
	// "events".
	Collection string
	// QueryLimit caps how many events a single QueryEvents call may return.
	QueryLimit int
	// PageSize is how many documents are fetched per Firestore round-trip while
	// paging towards QueryLimit. Larger values mean fewer round-trips but more
	// documents read when client-side filtering discards many of them.
	PageSize int
}

// firestoreEvent is the document shape stored in Firestore. tags holds the full
// tag array (as JSON) for faithful reconstruction and is index-exempt, while
// tagvalues is the flattened "<letter>:<value>" list used by array-contains-any
// tag queries.
type firestoreEvent struct {
	ID        string   `firestore:"id"`
	PubKey    string   `firestore:"pubkey"`
	CreatedAt int64    `firestore:"created_at"`
	Kind      int      `firestore:"kind"`
	Tags      string   `firestore:"tags"`
	TagValues []string `firestore:"tagvalues"`
	Content   string   `firestore:"content"`
	Sig       string   `firestore:"sig"`
}

func (fe *firestoreEvent) toEvent() (*nostr.Event, error) {
	evt := &nostr.Event{
		ID:        fe.ID,
		PubKey:    fe.PubKey,
		CreatedAt: nostr.Timestamp(fe.CreatedAt),
		Kind:      fe.Kind,
		Content:   fe.Content,
		Sig:       fe.Sig,
	}
	if fe.Tags != "" {
		if err := json.Unmarshal([]byte(fe.Tags), &evt.Tags); err != nil {
			return nil, err
		}
	}
	if evt.Tags == nil {
		evt.Tags = nostr.Tags{}
	}
	return evt, nil
}

func (b *FirestoreBackend) Close() {
	if b.Client != nil {
		b.Client.Close()
	}
}
