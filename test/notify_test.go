package test

import (
	"context"
	"strings"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/fiatjaf/eventstore/postgresql"
	"github.com/nbd-wtf/go-nostr"
)

func TestPostgresNotifier(t *testing.T) {
	postgres := embeddedpostgres.NewDatabase()
	if err := postgres.Start(); err != nil {
		t.Fatalf("failed to start embedded postgres: %s", err)
	}
	defer postgres.Stop()

	url := "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"

	// two backends on the same database stand in for two relay processes
	a := &postgresql.PostgresBackend{DatabaseURL: url}
	b := &postgresql.PostgresBackend{DatabaseURL: url}
	for _, db := range []*postgresql.PostgresBackend{a, b} {
		if err := db.Init(); err != nil {
			t.Fatal(err)
		}
		defer db.Close()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fromA, err := a.Notifications(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fromB, err := b.Notifications(ctx)
	if err != nil {
		t.Fatal(err)
	}

	recv := func(ch <-chan *nostr.Event) *nostr.Event {
		t.Helper()
		select {
		case evt := <-ch:
			return evt
		case <-time.After(5 * time.Second):
			t.Fatal("no notification received")
			return nil
		}
	}

	// a small event travels inline
	small := &nostr.Event{Kind: 1, Content: "hello", CreatedAt: nostr.Now(), Tags: nostr.Tags{}}
	small.Sign(sk3)
	if err := a.SaveEvent(ctx, small); err != nil {
		t.Fatal(err)
	}
	if err := a.Notify(ctx, small); err != nil {
		t.Fatal(err)
	}
	for _, ch := range []<-chan *nostr.Event{fromA, fromB} {
		if got := recv(ch); got.ID != small.ID || got.Content != small.Content {
			t.Errorf("expected %s, got %s", small.ID, got.ID)
		}
	}

	// an event above the pg_notify limit is announced by id and read back
	big := &nostr.Event{Kind: 1, Content: strings.Repeat("x", 9000), CreatedAt: nostr.Now(), Tags: nostr.Tags{}}
	big.Sign(sk3)
	if err := a.SaveEvent(ctx, big); err != nil {
		t.Fatal(err)
	}
	if err := a.Notify(ctx, big); err != nil {
		t.Fatal(err)
	}
	for _, ch := range []<-chan *nostr.Event{fromA, fromB} {
		if got := recv(ch); got.ID != big.ID || got.Content != big.Content {
			t.Errorf("expected %s with full content, got %s (%d bytes)", big.ID, got.ID, len(got.Content))
		}
	}

	// an ephemeral event is not saved but still travels
	eph := &nostr.Event{Kind: 20001, Content: "ephemeral", CreatedAt: nostr.Now(), Tags: nostr.Tags{}}
	eph.Sign(sk3)
	if err := a.Notify(ctx, eph); err != nil {
		t.Fatal(err)
	}
	for _, ch := range []<-chan *nostr.Event{fromA, fromB} {
		if got := recv(ch); got.ID != eph.ID {
			t.Errorf("expected %s, got %s", eph.ID, got.ID)
		}
	}

	// cancelling the context closes the channels
	cancel()
	for _, ch := range []<-chan *nostr.Event{fromA, fromB} {
		select {
		case _, ok := <-ch:
			if ok {
				t.Error("expected channel to be closed")
			}
		case <-time.After(5 * time.Second):
			t.Error("channel not closed after cancel")
		}
	}
}
