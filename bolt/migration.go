package bolt

import (
	"encoding/binary"

	bolt "go.etcd.io/bbolt"
)

const (
	DB_VERSION byte = 'v'
)

func (b *BoltBackend) runMigrations() error {
	return b.db.Update(func(txn *bolt.Tx) error {
		var version uint16
		v := txn.Bucket(bucketSettings).Get([]byte{DB_VERSION})
		if v == nil {
			version = 0
		} else {
			version = binary.BigEndian.Uint16(v)
		}

		// do the migrations in increasing steps (there is no rollback)
		//

		if version < 0 {
			// ...
		}

		return nil
	})
}
