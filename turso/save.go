package turso

import (
	"context"
	"encoding/json"

	"github.com/fiatjaf/eventstore"
	"github.com/nbd-wtf/go-nostr"
)

func (b *TursoBackend) SaveEvent(ctx context.Context, evt *nostr.Event) error {
	// insert
	tagsj, _ := json.Marshal(evt.Tags)
	// NOTE: the libsql driver treats $N placeholders as named parameters, which
	// don't bind to the positional args passed by database/sql, so use ? here.
	res, err := b.DB.ExecContext(ctx, `
        INSERT OR IGNORE INTO event (id, pubkey, created_at, kind, tags, content, sig)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    `, evt.ID, evt.PubKey, evt.CreatedAt, evt.Kind, tagsj, evt.Content, evt.Sig)
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
