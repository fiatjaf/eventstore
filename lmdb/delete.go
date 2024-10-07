package lmdb

import (
	"context"
	"encoding/hex"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/nbd-wtf/go-nostr"
)

func (b *LMDBBackend) DeleteEvent(ctx context.Context, evt *nostr.Event) error {
	err := b.lmdbEnv.Update(func(txn *lmdb.Txn) error {
		idPrefix8, _ := hex.DecodeString(evt.ID[0 : 8*2])
		idx, err := txn.Get(b.indexId, idPrefix8)
		if operr, ok := err.(*lmdb.OpError); ok && operr.Errno == lmdb.NotFound {
			// we already do not have this
			return nil
		}
		if err != nil {
			return err
		}

		// calculate all index keys we have for this event and delete them
		for k := range b.getIndexKeysForEvent(evt) {
			err := txn.Del(k.dbi, k.key, idx)
			k.free()
			if err != nil {
				return err
			}
		}

		// delete the raw event
		return txn.Del(b.rawEventStore, idx, nil)
	})
	if err != nil {
		return err
	}

	return nil
}
