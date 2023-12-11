package lmdb

import (
	"encoding/binary"
	"fmt"

	"github.com/PowerDNS/lmdb-go/lmdb"
)

const (
	DB_VERSION byte = 'v'
)

func (b *LMDBBackend) runMigrations() error {
	return b.lmdbEnv.Update(func(txn *lmdb.Txn) error {
		var version uint16
		v, err := txn.Get(b.settingsStore, []byte{DB_VERSION})
		if err != nil {
			if lmdbErr, ok := err.(*lmdb.OpError); ok && lmdbErr.Errno == lmdb.NotFound {
				version = 0
			} else if v == nil {
				return fmt.Errorf("failed to read database version: %w", err)
			}
		} else {
			version = binary.BigEndian.Uint16(v)
		}

		// do the migrations in increasing steps (there is no rollback)
		//

		// the 4 first migrations go to trash because on version 3 we need to export and import all the data anyway
		if version < 4 {
			// if there is any data in the relay we will stop and notify the user,
			// otherwise we just set version to 3 and proceed

			cursor, err := txn.OpenCursor(b.indexId)
			if err != nil {
				return fmt.Errorf("failed to open cursor in migration 4: %w", err)
			}
			defer cursor.Close()

			hasAnyEntries := false
			_, _, err = cursor.Get(nil, nil, lmdb.First)
			for err == nil {
				hasAnyEntries = true
				break
			}

			if hasAnyEntries {
				return fmt.Errorf("your database is at version %d, but in order to migrate up to version 4 you must manually export all the events and then import again: run an old version of this software, export the data, then delete the database files, run the new version, import the data back in.", version)
			}

			b.bumpVersion(txn, 4)
		}

		if version < 5 {
			// ...
		}

		return nil
	})
}

func (b *LMDBBackend) bumpVersion(txn *lmdb.Txn, version uint16) error {
	buf, err := txn.PutReserve(b.settingsStore, []byte{DB_VERSION}, 4, 0)
	binary.BigEndian.PutUint16(buf, version)
	return err
}
