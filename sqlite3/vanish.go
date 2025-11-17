package sqlite3

import (
	"context"
)

func (b *SQLite3Backend) VanishPubkey(ctx context.Context, pubkey string, until int64) error {
	_, err := b.DB.ExecContext(ctx, 
		"DELETE FROM event WHERE pubkey = ? AND created_at <= ? AND kind != 62", 
		pubkey, until)
	return err
}
