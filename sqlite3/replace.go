package sqlite3

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/fiatjaf/eventstore/internal"
	"github.com/nbd-wtf/go-nostr"
)

func (b *SQLite3Backend) ReplaceEvent(ctx context.Context, evt *nostr.Event) error {
	txn, err := b.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("fail to start txn: %w", err)
	}
	defer txn.Rollback()

	filter := nostr.Filter{Limit: 1, Kinds: []int{evt.Kind}, Authors: []string{evt.PubKey}}
	if nostr.IsAddressableKind(evt.Kind) {
		// when addressable, add the "d" tag to the filter
		filter.Tags = nostr.TagMap{"d": []string{evt.Tags.GetD()}}
	}

	query, params, err := b.queryEventsSql(filter, false)
	if err != nil {
		return err
	}

	rows, err := txn.QueryContext(ctx, query, params...)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to fetch older events using query %q: %w", query, err)
	}

	shouldStore := true
	defer rows.Close()
	for rows.Next() {
		var previous nostr.Event
		var timestamp int64
		err := rows.Scan(&previous.ID, &previous.PubKey, &timestamp,
			&previous.Kind, &previous.Tags, &previous.Content, &previous.Sig)
		if err != nil {
			return fmt.Errorf("failed to scan older event row: %w", err)
		}
		previous.CreatedAt = nostr.Timestamp(timestamp)
		if internal.IsOlder(&previous, evt) {
			if _, err := txn.ExecContext(ctx, "DELETE FROM event WHERE id = $1", evt.ID); err != nil {
				return fmt.Errorf("failed to delete event %s for replacing: %w", previous.ID, err)
			}
		} else {
			// there is a newer event already stored, so we won't store this
			shouldStore = false
		}
	}

	if shouldStore {
		tagsj, _ := json.Marshal(evt.Tags)
		_, err = txn.ExecContext(ctx, `
        INSERT OR IGNORE INTO event (id, pubkey, created_at, kind, tags, content, sig)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
    `, evt.ID, evt.PubKey, evt.CreatedAt, evt.Kind, tagsj, evt.Content, evt.Sig)
	}

	return err
}
