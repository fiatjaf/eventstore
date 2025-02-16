package mmm

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"slices"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/nbd-wtf/go-nostr"
)

func (il *IndexingLayer) DeleteEvent(ctx context.Context, evt *nostr.Event) error {
	return il.mmmm.lmdbEnv.Update(func(mmmtxn *lmdb.Txn) error {
		return il.lmdbEnv.Update(func(iltxn *lmdb.Txn) error {
			return il.delete(mmmtxn, iltxn, evt)
		})
	})
}

func (il *IndexingLayer) delete(mmmtxn *lmdb.Txn, iltxn *lmdb.Txn, evt *nostr.Event) error {
	zeroRefs := false
	b := il.mmmm

	b.Logger.Debug().Str("layer", il.name).Uint16("il", il.id).Msg("deleting")

	// first in the mmmm txn we check if we have the event still
	idPrefix8, _ := hex.DecodeString(evt.ID[0 : 8*2])
	val, err := mmmtxn.Get(b.indexId, idPrefix8)
	if err != nil {
		if lmdb.IsNotFound(err) {
			// we already do not have this anywhere
			return nil
		}
		return fmt.Errorf("failed to check if we have the event %x: %w", idPrefix8, err)
	}

	// we have this, but do we have it in the current layer?
	// val is [posb][il_idx][il_idx...]
	pos := positionFromBytes(val[0:12])

	// check references
	currentLayer := binary.BigEndian.AppendUint16(nil, il.id)
	for i := 12; i < len(val); i += 2 {
		if slices.Equal(val[i:i+2], currentLayer) {
			// we will remove the current layer if it's found
			nextval := make([]byte, len(val)-2)
			copy(nextval, val[0:i])
			copy(nextval[i:], val[i+2:])

			if err := mmmtxn.Put(b.indexId, idPrefix8, nextval, 0); err != nil {
				return fmt.Errorf("failed to update references for %x: %w", idPrefix8, err)
			}

			// if there are no more layers we will delete everything later
			zeroRefs = len(nextval) == 12

			break
		}
	}

	// calculate all index keys we have for this event and delete them
	for k := range il.getIndexKeysForEvent(evt) {
		if err := iltxn.Del(k.dbi, k.key, val[0:12]); err != nil && !lmdb.IsNotFound(err) {
			return fmt.Errorf("index entry %v/%x deletion failed: %w", k.dbi, k.key, err)
		}
	}

	// if there are no more refs we delete the event from the id index and mmap
	if zeroRefs {
		if err := b.purge(mmmtxn, idPrefix8, pos); err != nil {
			panic(err)
		}
	}

	return nil
}
