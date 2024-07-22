package bolt

import (
	"context"
	"encoding/hex"

	"github.com/nbd-wtf/go-nostr"
	bolt "go.etcd.io/bbolt"
)

func (b *BoltBackend) DeleteEvent(ctx context.Context, evt *nostr.Event) error {
	return b.db.Update(func(txn *bolt.Tx) error {
		idPrefix8, _ := hex.DecodeString(evt.ID[0 : 8*2])

		// check if we already do not have this
		bucket := txn.Bucket(bucketId)
		seqb := bucket.Get(idPrefix8)
		if seqb == nil {
			return nil
		}

		// calculate all index keys we have for this event and delete them
		for _, k := range getIndexKeysForEvent(evt) {
			bucket := txn.Bucket(k.bucket)
			bucket.Delete(append(k.key, seqb...))
		}

		// delete the raw event
		return txn.Bucket(bucketRaw).Delete(seqb)
	})
}
