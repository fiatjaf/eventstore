package slicestore

import (
	"context"
	"strings"

	"github.com/fiatjaf/eventstore"
	"github.com/nbd-wtf/go-nostr"
	"golang.org/x/exp/slices"
)

var _ eventstore.Store = (*SliceStore)(nil)

type SliceStore struct {
	internal []*nostr.Event

	MaxLimit int
}

func (b *SliceStore) Init() error {
	b.internal = make([]*nostr.Event, 0, 5000)
	if b.MaxLimit == 0 {
		b.MaxLimit = 500
	}
	return nil
}

func (b *SliceStore) Close() {}

func (b *SliceStore) QueryEvents(ctx context.Context, filter nostr.Filter) (chan *nostr.Event, error) {
	ch := make(chan *nostr.Event)
	if filter.Limit > b.MaxLimit || filter.Limit == 0 {
		filter.Limit = b.MaxLimit
	}

	// efficiently determine where to start and end
	start := 0
	end := len(b.internal)
	if filter.Until != nil {
		start, _ = slices.BinarySearchFunc(b.internal, *filter.Until, eventTimestampComparator)
	}
	if filter.Since != nil {
		end, _ = slices.BinarySearchFunc(b.internal, *filter.Since, eventTimestampComparator)
	}

	// ham
	if end < start {
		close(ch)
		return ch, nil
	}

	count := 0
	go func() {
		for _, event := range b.internal[start:end] {
			if count == filter.Limit {
				break
			}

			if filter.Matches(event) {
				select {
				case ch <- event:
				case <-ctx.Done():
					return
				}
				count++
			}
		}
		close(ch)
	}()
	return ch, nil
}

func (b *SliceStore) CountEvents(ctx context.Context, filter nostr.Filter) (int64, error) {
	var val int64
	for _, event := range b.internal {
		if filter.Matches(event) {
			val++
		}
	}
	return val, nil
}

func (b *SliceStore) SaveEvent(ctx context.Context, evt *nostr.Event) error {
	idx, found := slices.BinarySearchFunc(b.internal, evt, eventComparator)
	if found {
		return eventstore.ErrDupEvent
	}
	// let's insert at the correct place in the array
	b.internal = append(b.internal, evt) // bogus
	copy(b.internal[idx+1:], b.internal[idx:])
	b.internal[idx] = evt

	return nil
}

func (b *SliceStore) DeleteEvent(ctx context.Context, evt *nostr.Event) error {
	idx, found := slices.BinarySearchFunc(b.internal, evt, eventComparator)
	if !found {
		// we don't have this event
		return nil
	}

	// we have it
	copy(b.internal[idx:], b.internal[idx+1:])
	b.internal = b.internal[0 : len(b.internal)-1]
	return nil
}

func eventTimestampComparator(e *nostr.Event, t nostr.Timestamp) int {
	return int(t) - int(e.CreatedAt)
}

func eventComparator(a *nostr.Event, b *nostr.Event) int {
	c := int(b.CreatedAt) - int(a.CreatedAt)
	if c != 0 {
		return c
	}
	return strings.Compare(b.ID, a.ID)
}
