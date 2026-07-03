# firestore

An [`eventstore.Store`](../store.go) backend backed by Google Cloud Firestore
(Native mode). It also implements `eventstore.Counter`.

## Usage

```go
store := &firestore.FirestoreBackend{
	ProjectID:  "my-gcp-project",
	Collection: "events", // optional, defaults to "events"
}
if err := store.Init(); err != nil {
	log.Fatal(err)
}
defer store.Close()
```

Credentials come from [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials).
Set `FIRESTORE_EMULATOR_HOST` to run against the local emulator.

## Composite indexes (required)

Firestore rejects most non-trivial queries unless the matching composite index
exists. Deploy the indexes in [`firestore.indexes.json`](firestore.indexes.json)
before using the backend:

```sh
gcloud firestore indexes create --index-file=firestore.indexes.json
# or, with the Firebase CLI:
firebase deploy --only firestore:indexes
```

The `fieldOverrides` also disable single-field indexing on `content`, `sig` and
the raw `tags` field — they are never queried directly, and exempting them cuts
index storage (and therefore cost) substantially.

## How queries map to Firestore

Firestore allows only **one** disjunctive (`in` / `array-contains-any`) clause
and one range field per query, and cannot `OR` across different fields. A
`nostr.Filter` is richer than that, so the backend:

1. Pushes down the `created_at` range plus the single most selective dimension,
   chosen in the order **ids → tag → authors → kinds**.
2. Streams the resulting documents (paging with cursors up to the query limit)
   and enforces every remaining constraint client-side with `filter.Matches`.

The push-down is purely a cost optimization — correctness always comes from
`filter.Matches`, so results are never wrong, only potentially more expensive
when a filter can't be fully expressed as a single Firestore query.

Tags are indexed only for **single-letter** tag names (per NIP-01), stored in a
`tagvalues` array as `"<letter>:<value>"` entries.

### `CountEvents`

When a filter fits entirely in the pushed-down query (at most one disjunctive
dimension, no search term), `CountEvents` uses Firestore's native aggregation,
which is billed per index scan rather than per document. Otherwise it falls back
to reading and matching candidates, which is more expensive.

## Cost note

Firestore bills primarily per **document read**, not per stored gigabyte. A
filter that can't be pushed down (e.g. several tag constraints at once) reads
every candidate document and filters in memory, so it is best suited to archival
or low-query-volume relays. High-traffic relays should measure read volume
before committing.

## Tests

The unit tests cover the pure filter-translation logic and need no Firestore
connection:

```sh
go test ./firestore/
```
