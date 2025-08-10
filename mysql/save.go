package mysql

import (
	"context"
	"encoding/json"

	"github.com/fiatjaf/eventstore"
	"github.com/nbd-wtf/go-nostr"
)

func (b *MySQLBackend) SaveEvent(ctx context.Context, evt *nostr.Event) error {
	deleteQuery, deleteParams, shouldDelete := deleteBeforeSaveSql(evt)
	if shouldDelete {
		_, _ = b.DB.ExecContext(ctx, deleteQuery, deleteParams...)
	}

	sql, params, _ := saveEventSql(evt)
	res, err := b.DB.ExecContext(ctx, sql, params...)
	if err != nil {
		return err
	}

	nr, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if nr == 0 {
		return eventstore.ErrDupEvent
	}

	return nil
}

func deleteBeforeSaveSql(evt *nostr.Event) (string, []any, bool) {
	// react to different kinds of events
	var (
		query        = ""
		params       []any
		shouldDelete bool
	)
	if evt.Kind == nostr.KindProfileMetadata || evt.Kind == nostr.KindFollowList || (10000 <= evt.Kind && evt.Kind < 20000) {
		// delete past events from this user
		query = `DELETE FROM event WHERE pubkey = ? AND kind = ?`
		params = []any{evt.PubKey, evt.Kind}
		shouldDelete = true
	} else if evt.Kind == nostr.KindRecommendServer {
		// delete past recommend_server events equal to this one
		query = `DELETE FROM event WHERE pubkey = ? AND kind = ? AND content = ?`
		params = []any{evt.PubKey, evt.Kind, evt.Content}
		shouldDelete = true
	} else if evt.Kind >= 30000 && evt.Kind < 40000 {
		// NIP-33
		d := evt.Tags.GetFirst([]string{"d"})
		if d != nil {
			query = `DELETE FROM event WHERE pubkey = ? AND kind = ? AND tags LIKE ?`
			params = []any{evt.PubKey, evt.Kind, `["d": "`+d.Value()+`"]`}
			shouldDelete = true
		}
	}

	return query, params, shouldDelete
}

func saveEventSql(evt *nostr.Event) (string, []any, error) {
	const query = `INSERT INTO event (
	id, pubkey, created_at, kind, tags, content, sig)
	VALUES (?, ?, ?, ?, ?, ?, ?)`

	var (
		tagsj, _ = json.Marshal(evt.Tags)
		params   = []any{evt.ID, evt.PubKey, evt.CreatedAt, evt.Kind, tagsj, evt.Content, evt.Sig}
	)

	return query, params, nil
}
