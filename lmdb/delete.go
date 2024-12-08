package lmdb

import (
	"context"
	"encoding/hex"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/nbd-wtf/go-nostr"
)

func (b *LMDBBackend) DeleteEvent(ctx context.Context, evt *nostr.Event) error {
	err := b.lmdbEnv.Update(func(txn *lmdb.Txn) error {
		return b.delete(txn, evt)
	})
	if err != nil {
		return err
	}

	return nil
}

func (b *LMDBBackend) delete(txn *lmdb.Txn, evt *nostr.Event) error {
	idPrefix8, _ := hex.DecodeString(evt.ID[0 : 8*2])
	idx, err := txn.Get(b.indexId, idPrefix8)
	if lmdb.IsNotFound(err) {
		// we already do not have this
		return nil
	}
	if err != nil {
		return err
	}

	// calculate all index keys we have for this event and delete them
	for k := range b.getIndexKeysForEvent(evt) {
		err := txn.Del(k.dbi, k.key, idx)
		if err != nil {
			return err
		}
	}

	// delete the raw event
	return txn.Del(b.rawEventStore, idx, nil)
}
