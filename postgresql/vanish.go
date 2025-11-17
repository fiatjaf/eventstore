package postgresql

import (
	"context"
)

func (b *PostgresBackend) VanishPubkey(ctx context.Context, pubkey string, until int64) error {
	_, err := b.DB.ExecContext(ctx, 
		"DELETE FROM event WHERE pubkey = $1 AND created_at <= $2 AND kind != 62", 
		pubkey, until)
	return err
}
