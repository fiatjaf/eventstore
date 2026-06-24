package mysql

import (
	"context"
)

// kindRequestToVanish is the NIP-62 "request to vanish" event kind. These
// events are kept for bookkeeping instead of being deleted.
const kindRequestToVanish = 62

func (b *MySQLBackend) VanishPubkey(ctx context.Context, pubkey string, until int64) error {
	_, err := b.DB.ExecContext(ctx,
		"DELETE FROM event WHERE pubkey = ? AND created_at <= ? AND kind != ?",
		pubkey, until, kindRequestToVanish)
	return err
}
