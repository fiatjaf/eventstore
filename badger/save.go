package badger

import (
	"context"

	"github.com/dgraph-io/badger/v4"
	"github.com/nbd-wtf/go-nostr"
	nostr_binary "github.com/nbd-wtf/go-nostr/binary"
)

func (b *BadgerBackend) SaveEvent(ctx context.Context, evt *nostr.Event) error {
	return b.Update(func(txn *badger.Txn) error {
		bin, err := nostr_binary.Marshal(evt)
		if err != nil {
			return err
		}

		idx := b.Serial()
		// raw event store
		if err := txn.Set(idx, bin); err != nil {
			return err
		}

		for _, k := range getIndexKeysForEvent(evt, idx[1:]) {
			if err := txn.Set(k, nil); err != nil {
				return err
			}
		}

		return nil
	})
}
