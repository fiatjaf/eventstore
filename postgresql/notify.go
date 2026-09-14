package postgresql

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/fiatjaf/eventstore"
	"github.com/lib/pq"
	"github.com/nbd-wtf/go-nostr"
)

const (
	defaultNotifyChannel = "nostr_events"

	// pg_notify refuses payloads above this size, so bigger events are
	// announced by id and fetched back from the table by the receiver.
	maxNotifyPayload = 8000
)

var _ eventstore.Notifier = (*PostgresBackend)(nil)

func (b *PostgresBackend) notifyChannel() string {
	if b.NotifyChannel != "" {
		return b.NotifyChannel
	}
	return defaultNotifyChannel
}

// Notify sends evt on the NotifyChannel with NOTIFY, so that every process
// listening through Notifications receives it.
func (b *PostgresBackend) Notify(ctx context.Context, evt *nostr.Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	if len(payload) > maxNotifyPayload {
		payload = []byte(evt.ID)
	}
	_, err = b.DB.ExecContext(ctx, `SELECT pg_notify($1, $2)`, b.notifyChannel(), string(payload))
	return err
}

// Notifications listens on the NotifyChannel and delivers every event sent
// with Notify by any process connected to the same database. It needs
// DatabaseURL, since LISTEN takes over a dedicated connection.
func (b *PostgresBackend) Notifications(ctx context.Context) (<-chan *nostr.Event, error) {
	if b.DatabaseURL == "" {
		return nil, fmt.Errorf("DatabaseURL is required to listen for notifications")
	}

	listener := pq.NewListener(b.DatabaseURL, 10*time.Second, time.Minute, nil)
	if err := listener.Listen(b.notifyChannel()); err != nil {
		listener.Close()
		return nil, err
	}

	ch := make(chan *nostr.Event)
	go func() {
		defer close(ch)
		defer listener.Close()
		for {
			select {
			case n, ok := <-listener.Notify:
				if !ok {
					return
				}
				if n == nil {
					// the listener reconnected; notifications sent meanwhile are lost
					continue
				}
				evt := b.decodeNotification(ctx, n.Extra)
				if evt == nil {
					continue
				}
				select {
				case ch <- evt:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

// decodeNotification turns a payload sent by Notify back into an event: either
// the event itself or, when it was too big for pg_notify, its id.
func (b *PostgresBackend) decodeNotification(ctx context.Context, payload string) *nostr.Event {
	if len(payload) > 0 && payload[0] == '{' {
		var evt nostr.Event
		if err := json.Unmarshal([]byte(payload), &evt); err != nil {
			return nil
		}
		return &evt
	}

	// too big to be sent inline; ephemeral events are not stored, so those are lost
	ch, err := b.QueryEvents(ctx, nostr.Filter{IDs: []string{payload}, Limit: 1})
	if err != nil {
		return nil
	}
	var evt *nostr.Event
	for e := range ch {
		evt = e
	}
	return evt
}
