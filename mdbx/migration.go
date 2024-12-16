package mdbx

import (
	"encoding/binary"
	"fmt"

	"github.com/erigontech/mdbx-go/mdbx"
)

const (
	DB_VERSION byte = 'v'
)

func (b *MDBXBackend) runMigrations() error {
	return b.mdbxEnv.Update(func(txn *mdbx.Txn) error {
		var version uint16
		v, err := txn.Get(b.settingsStore, []byte{DB_VERSION})
		if err != nil {
			if mdbxErr, ok := err.(*mdbx.OpError); ok && mdbxErr.Errno == mdbx.NotFound {
				version = 0
			} else if v == nil {
				return fmt.Errorf("failed to read database version: %w", err)
			}
		} else {
			version = binary.BigEndian.Uint16(v)
		}

		if version < 0 {
			// if there is any data in the relay we will just set the version to the max without saying anything
			cursor, err := txn.OpenCursor(b.rawEventStore)
			if err != nil {
				return fmt.Errorf("failed to open cursor in migration: %w", err)
			}
			defer cursor.Close()

			hasAnyEntries := false
			_, _, err = cursor.Get(nil, nil, mdbx.First)
			for err == nil {
				hasAnyEntries = true
				break
			}

			if !hasAnyEntries {
				b.setVersion(txn, 0)
				return nil
			}
		}

		// do the migrations in increasing steps (there is no rollback)
		//

		if version < 0 {
		}

		return nil
	})
}

func (b *MDBXBackend) setVersion(txn *mdbx.Txn, version uint16) error {
	buf, err := txn.PutReserve(b.settingsStore, []byte{DB_VERSION}, 4, 0)
	binary.BigEndian.PutUint16(buf, version)
	return err
}
