package slicestore

import (
	"context"
	"testing"

	"github.com/nbd-wtf/go-nostr"
)

func TestVanishPubkey(t *testing.T) {
	ctx := context.Background()
	ss := &SliceStore{}
	ss.Init()
	defer ss.Close()

	const target = "aaaa000000000000000000000000000000000000000000000000000000000000"
	const other = "bbbb000000000000000000000000000000000000000000000000000000000000"

	// distinct ids so nothing collides on insert
	id := func(n int) string {
		const hexdigits = "0123456789abcdef"
		s := make([]byte, 64)
		for i := range s {
			s[i] = '0'
		}
		s[0] = hexdigits[n%16]
		s[1] = hexdigits[(n/16)%16]
		return string(s)
	}

	events := []*nostr.Event{
		{ID: id(1), PubKey: target, Kind: 1, CreatedAt: 100},  // deleted
		{ID: id(2), PubKey: target, Kind: 1, CreatedAt: 150},  // deleted (== until)
		{ID: id(3), PubKey: target, Kind: 1, CreatedAt: 200},  // kept (newer than until)
		{ID: id(4), PubKey: target, Kind: 62, CreatedAt: 120}, // kept (vanish request)
		{ID: id(5), PubKey: other, Kind: 1, CreatedAt: 100},   // kept (other pubkey)
	}
	for _, evt := range events {
		if err := ss.SaveEvent(ctx, evt); err != nil {
			t.Fatalf("save failed: %v", err)
		}
	}

	if err := ss.VanishPubkey(ctx, target, 150); err != nil {
		t.Fatalf("vanish failed: %v", err)
	}

	ch, _ := ss.QueryEvents(ctx, nostr.Filter{})
	remaining := map[string]bool{}
	for evt := range ch {
		remaining[evt.ID] = true
	}

	if len(remaining) != 3 {
		t.Fatalf("expected 3 events remaining, got %d", len(remaining))
	}
	if remaining[id(1)] || remaining[id(2)] {
		t.Fatal("target events up to `until` should have been deleted")
	}
	if !remaining[id(3)] {
		t.Fatal("target event newer than `until` should have been kept")
	}
	if !remaining[id(4)] {
		t.Fatal("kind 62 vanish request should have been kept")
	}
	if !remaining[id(5)] {
		t.Fatal("event from another pubkey should have been kept")
	}
}
