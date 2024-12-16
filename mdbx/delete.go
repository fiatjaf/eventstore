package mdbx

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/erigontech/mdbx-go/mdbx"
	"github.com/nbd-wtf/go-nostr"
)

func (b *MDBXBackend) DeleteEvent(ctx context.Context, evt *nostr.Event) error {
	return b.mdbxEnv.Update(func(txn *mdbx.Txn) error {
		return b.delete(txn, evt)
	})
}

func (b *MDBXBackend) delete(txn *mdbx.Txn, evt *nostr.Event) error {
	idPrefix8, _ := hex.DecodeString(evt.ID[0 : 8*2])
	idx, err := txn.Get(b.indexId, idPrefix8)
	if mdbx.IsNotFound(err) {
		// we already do not have this
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get current idx for deleting %x: %w", evt.ID[0:8*2], err)
	}

	// calculate all index keys we have for this event and delete them
	for k := range b.getIndexKeysForEvent(evt) {
		err := txn.Del(k.dbi, k.key, idx)
		if err != nil {
			return fmt.Errorf("failed to delete index entry %s for %x: %w", b.keyName(k), evt.ID[0:8*2], err)
		}
	}

	// delete the raw event
	if err := txn.Del(b.rawEventStore, idx, nil); err != nil {
		return fmt.Errorf("failed to delete raw event %x (idx %x): %w", evt.ID[0:8*2], idx, err)
	}

	return nil
}
