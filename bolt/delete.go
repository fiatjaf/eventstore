package bolt

import (
	"context"
	"encoding/hex"

	"github.com/nbd-wtf/go-nostr"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/exp/slices"
)

func (b *BoltBackend) DeleteEvent(ctx context.Context, evt *nostr.Event) error {
	return b.db.Update(func(txn *bolt.Tx) error {
		idPrefix8, _ := hex.DecodeString(evt.ID[0 : 8*2])
		bucket := txn.Bucket(bucketId)

		// check if we already do not have this
		c := bucket.Cursor()
		key, _ := c.Seek(idPrefix8)
		if key == nil || !slices.Equal(key[0:8], idPrefix8) {
			// already do not have it
			return nil
		}

		// seqb is the key where this event is stored at bucketRaw
		seqb := key[8:]

		// calculate all index keys we have for this event and delete them
		for _, k := range getIndexKeysForEvent(evt) {
			bucket := txn.Bucket(k.bucket)
			bucket.Delete(append(k.key, seqb...))
		}

		// delete the raw event
		return txn.Bucket(bucketRaw).Delete(seqb)
	})
}
