package mmm

import (
	"bytes"
	"fmt"
	"os"

	"github.com/PowerDNS/lmdb-go/lmdb"
)

func (b *MultiMmapManager) purge(txn *lmdb.Txn, idPrefix8 []byte, pos position) error {
	b.Logger.Debug().Hex("event", idPrefix8).Stringer("pos", pos).Msg("purging")

	// delete from index
	if err := txn.Del(b.indexId, idPrefix8, nil); err != nil {
		return err
	}

	// will add the current range to free ranges, which means it is "deleted" (or merge with existing)
	isAtEnd := b.mergeNewFreeRange(pos)

	if isAtEnd {
		// when at the end, truncate the mmap
		// [new_pos_to_be_freed][end_of_file] -> shrink file!
		pos.size = 0 // so we don't try to add this some lines below
		if err := os.Truncate(b.mmapfPath, int64(pos.start)); err != nil {
			panic(fmt.Errorf("error decreasing %s: %w", b.mmapfPath, err))
		}
		b.mmapfEnd = pos.start
	} else {
		// this is for debugging -------------
		copy(b.mmapf[pos.start:], bytes.Repeat([]byte{'!'}, int(pos.size)))
	}

	return b.saveFreeRanges(txn)
}
