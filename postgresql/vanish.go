package postgresql

import (
	"context"
)

// kindRequestToVanish is the NIP-62 "request to vanish" event kind. These
// events are kept for bookkeeping instead of being deleted.
const kindRequestToVanish = 62

func (b *PostgresBackend) VanishPubkey(ctx context.Context, pubkey string, until int64) error {
	_, err := b.DB.ExecContext(ctx,
		"DELETE FROM event WHERE pubkey = $1 AND created_at <= $2 AND kind != $3",
		pubkey, until, kindRequestToVanish)
	return err
}
