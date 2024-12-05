package mmm

import (
	"fmt"
	"slices"

	"github.com/PowerDNS/lmdb-go/lmdb"
)

func (b *MultiMmapManager) mergeNewFreeRange(pos position) (isAtEnd bool) {
	// before adding check if we can merge this with some other range
	// (to merge means to delete the previous and add a new one)
	toDelete := make([]int, 0, 2)
	for f, fr := range b.freeRanges {
		if pos.start+uint64(pos.size) == fr.start {
			// [new_pos_to_be_freed][existing_fr] -> merge!
			toDelete = append(toDelete, f)
			pos.size = pos.size + fr.size
		} else if fr.start+uint64(fr.size) == pos.start {
			// [existing_fr][new_pos_to_be_freed] -> merge!
			toDelete = append(toDelete, f)
			pos.start = fr.start
			pos.size = fr.size + pos.size
		}
	}
	slices.SortFunc(toDelete, func(a, b int) int { return b - a })
	for _, idx := range toDelete {
		b.freeRanges = slices.Delete(b.freeRanges, idx, idx+1)
	}

	// when we're at the end of a file we just delete everything and don't add new free ranges
	// the caller will truncate the mmap file and adjust the position accordingly
	if pos.start+uint64(pos.size) == b.mmapfEnd {
		return true
	}

	b.addNewFreeRange(pos)
	return false
}

func (b *MultiMmapManager) addNewFreeRange(pos position) {
	// update freeranges slice in memory
	idx, _ := slices.BinarySearchFunc(b.freeRanges, pos, func(item, target position) int {
		if item.size > target.size {
			return 1
		} else if target.size > item.size {
			return -1
		} else if item.start > target.start {
			return 1
		} else {
			return -1
		}
	})
	b.freeRanges = slices.Insert(b.freeRanges, idx, pos)
}

func (b *MultiMmapManager) saveFreeRanges(txn *lmdb.Txn) error {
	// save to database
	valReserve, err := txn.PutReserve(b.stuff, FREERANGES_KEY, len(b.freeRanges)*12, 0)
	if err != nil {
		return fmt.Errorf("on put freeranges: %w", err)
	}
	for f, fr := range b.freeRanges {
		bytesFromPosition(valReserve[f*12:], fr)
	}

	return nil
}
