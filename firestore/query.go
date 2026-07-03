package firestore

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/firestore"
	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/nbd-wtf/go-nostr"
	"google.golang.org/api/iterator"
)

// buildFilteredQuery translates a nostr.Filter into a Firestore query.
//
// Firestore permits at most one disjunctive ("in" / "array-contains-any")
// clause per query, so only the single most selective dimension is pushed down
// (ids > tag > authors > kinds), together with the created_at range. Any
// remaining constraints are left for filter.Matches to enforce client-side; the
// push-down is a read-count optimization, never the source of correctness.
func (b *FirestoreBackend) buildFilteredQuery(filter nostr.Filter) firestore.Query {
	q := b.Client.Collection(b.Collection).Query

	switch {
	case len(filter.IDs) > 0 && len(filter.IDs) <= inLimit:
		q = q.Where("id", "in", toInterfaces(filter.IDs))
	case hasSmallTag(filter):
		key, values := firstSmallTag(filter)
		q = q.Where("tagvalues", "array-contains-any", tagValueInterfaces(key, values))
	case len(filter.Authors) > 0 && len(filter.Authors) <= inLimit:
		q = q.Where("pubkey", "in", toInterfaces(filter.Authors))
	case len(filter.Kinds) > 0 && len(filter.Kinds) <= inLimit:
		q = q.Where("kind", "in", toIntInterfaces(filter.Kinds))
	}

	if filter.Since != nil {
		q = q.Where("created_at", ">=", int64(*filter.Since))
	}
	if filter.Until != nil {
		q = q.Where("created_at", "<=", int64(*filter.Until))
	}

	return q
}

func (b *FirestoreBackend) QueryEvents(ctx context.Context, filter nostr.Filter) (chan *nostr.Event, error) {
	limit := filter.Limit
	if limit < 1 || limit > b.QueryLimit {
		limit = b.QueryLimit
	}

	base := b.buildFilteredQuery(filter).OrderBy("created_at", firestore.Desc)
	ch := make(chan *nostr.Event)

	go func() {
		defer close(ch)

		emitted := 0
		var last *firestore.DocumentSnapshot

		for emitted < limit {
			q := base.Limit(b.PageSize)
			if last != nil {
				q = q.StartAfter(last)
			}

			docs, err := q.Documents(ctx).GetAll()
			if err != nil {
				log.Printf("firestore: query failed: %v", err)
				return
			}
			if len(docs) == 0 {
				return
			}

			for _, doc := range docs {
				last = doc

				var fe firestoreEvent
				if err := doc.DataTo(&fe); err != nil {
					log.Printf("firestore: failed to decode %s: %v", doc.Ref.ID, err)
					continue
				}
				evt, err := fe.toEvent()
				if err != nil {
					log.Printf("firestore: failed to build event %s: %v", fe.ID, err)
					continue
				}
				// the push-down is only an approximation of the filter; Matches
				// is what actually guarantees the returned events are correct.
				if !filter.Matches(evt) {
					continue
				}

				select {
				case ch <- evt:
					emitted++
					if emitted >= limit {
						return
					}
				case <-ctx.Done():
					return
				}
			}

			// a short page means the underlying query is exhausted.
			if len(docs) < b.PageSize {
				return
			}
		}
	}()

	return ch, nil
}

func (b *FirestoreBackend) CountEvents(ctx context.Context, filter nostr.Filter) (int64, error) {
	base := b.buildFilteredQuery(filter)

	// When the whole filter fits in the pushed-down query we can use Firestore's
	// native aggregation, which is billed per index scan rather than per document
	// and therefore far cheaper than reading every event.
	if filterIsExact(filter) {
		result, err := base.NewAggregationQuery().WithCount("all").Get(ctx)
		if err != nil {
			return 0, err
		}
		value, ok := result["all"]
		if !ok {
			return 0, fmt.Errorf("firestore: count missing from aggregation result")
		}
		count, ok := value.(*pb.Value)
		if !ok {
			return 0, fmt.Errorf("firestore: unexpected aggregation result type %T", value)
		}
		return count.GetIntegerValue(), nil
	}

	// Otherwise there are constraints Firestore can't express (e.g. multiple tag
	// filters or a search term), so we must read and match each candidate.
	var count int64
	iter := base.Documents(ctx)
	defer iter.Stop()
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, err
		}
		var fe firestoreEvent
		if err := doc.DataTo(&fe); err != nil {
			continue
		}
		evt, err := fe.toEvent()
		if err != nil {
			continue
		}
		if filter.Matches(evt) {
			count++
		}
	}
	return count, nil
}

// filterIsExact reports whether buildFilteredQuery captures the filter entirely,
// so that no client-side Matches step is needed. That holds only when there is
// at most one disjunctive dimension (all of which fit within Firestore's value
// limit) and no full-text search term.
func filterIsExact(filter nostr.Filter) bool {
	if filter.Search != "" {
		return false
	}

	disjunctions := 0
	if n := len(filter.IDs); n > 0 {
		if n > inLimit {
			return false
		}
		disjunctions++
	}
	if n := len(filter.Authors); n > 0 {
		if n > inLimit {
			return false
		}
		disjunctions++
	}
	if n := len(filter.Kinds); n > 0 {
		if n > inLimit {
			return false
		}
		disjunctions++
	}
	for key, values := range filter.Tags {
		if len(key) != 1 || len(values) == 0 || len(values) > inLimit {
			return false
		}
		disjunctions++
	}

	return disjunctions <= 1
}

func hasSmallTag(filter nostr.Filter) bool {
	key, _ := firstSmallTag(filter)
	return key != ""
}

// firstSmallTag returns an indexable, push-downable tag filter (single-letter
// name with a value set that fits Firestore's disjunction limit), or "" if none.
func firstSmallTag(filter nostr.Filter) (string, []string) {
	for key, values := range filter.Tags {
		if len(key) == 1 && len(values) > 0 && len(values) <= inLimit {
			return key, values
		}
	}
	return "", nil
}

func toInterfaces(values []string) []interface{} {
	out := make([]interface{}, len(values))
	for i, v := range values {
		out[i] = v
	}
	return out
}

func toIntInterfaces(values []int) []interface{} {
	out := make([]interface{}, len(values))
	for i, v := range values {
		out[i] = v
	}
	return out
}

func tagValueInterfaces(key string, values []string) []interface{} {
	out := make([]interface{}, len(values))
	for i, v := range values {
		out[i] = key + ":" + v
	}
	return out
}
