package mmm

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"runtime"
	"slices"
	"syscall"
	"unsafe"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/fiatjaf/eventstore/mmm/betterbinary"
	"github.com/nbd-wtf/go-nostr"
)

func (b *MultiMmapManager) Store(ctx context.Context, evt *nostr.Event) (stored bool, err error) {
	// sanity checking
	if evt.CreatedAt > maxuint32 || evt.Kind > maxuint16 {
		return false, fmt.Errorf("event with values out of expected boundaries")
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// do this just so it's cleaner, we're already locking the thread and the mutex anyway
	txn, err := b.lmdbEnv.BeginTxn(nil, 0)
	if err != nil {
		return false, fmt.Errorf("failed to begin global transaction: %w", err)
	}
	txn.RawRead = true

	// this ensures we'll commit all transactions or rollback them
	txnsToClose := make([]*lmdb.Txn, 1, 1+len(b.layers))
	txnsToClose[0] = txn
	defer func() {
		if err != nil {
			for _, txn := range txnsToClose {
				txn.Abort()
			}
		} else {
			for _, txn := range txnsToClose {
				txn.Commit()
			}
		}
	}()

	// check if we already have this id
	idPrefix8, _ := hex.DecodeString(evt.ID[0 : 8*2])
	val, err := txn.Get(b.indexId, idPrefix8)
	if err == nil {
		// we found the event, which means it is already indexed by every layer who wanted to index it
		return false, nil
	}

	// we will only proceed if we get a NotFound -- for anything else we will error
	if !lmdb.IsNotFound(err) {
		return false, fmt.Errorf("error storing: %w", err)
	}

	// prepare value to be saved in the id index
	// val: [posb][layerIdRefs...]
	val = make([]byte, 12, 12+2*len(b.layers))

	// these are the bytes we must fill in with the position once we have it
	reservedResults := make([][]byte, 0, len(b.layers))

	// ask if any of the indexing layers want this
	someoneWantsIt := false
	for _, il := range b.layers {
		if il.ShouldIndex(ctx, evt) {
			// start a txn here and close it at the end only as we will have to use the valReserve arrays later
			iltxn, err := il.lmdbEnv.BeginTxn(nil, 0)
			if err != nil {
				return false, fmt.Errorf("failed to start txn on %s: %w", il.name, err)
			}
			defer iltxn.Commit()

			for k := range il.getIndexKeysForEvent(evt) {
				valReserve, err := iltxn.PutReserve(k.dbi, k.key, 12, 0)
				if err != nil {
					b.Logger.Warn().Str("name", il.name).Msg("failed to index event on layer")
				}
				reservedResults = append(reservedResults, valReserve)
			}
			val = binary.BigEndian.AppendUint16(val, il.id)
			someoneWantsIt = true
		}
	}
	if !someoneWantsIt {
		// no one wants it
		return false, fmt.Errorf("not wanted")
	}

	// find a suitable place for this to be stored in
	pos := position{
		size: uint32(betterbinary.Measure(*evt)),
	}
	appendToMmap := true
	for f, fr := range b.freeRanges {
		if fr.size >= pos.size {
			// found the smallest possible place that can fit this event
			appendToMmap = false
			pos.start = fr.start

			// modify the free ranges we're keeping track of
			// (i.e. delete the current and add a new freerange with the remaining space)
			b.freeRanges = slices.Delete(b.freeRanges, f, f+1)

			if pos.size != fr.size {
				b.addNewFreeRange(position{
					start: fr.start + uint64(pos.size),
					size:  fr.size - pos.size,
				})
			}

			if err := b.saveFreeRanges(txn); err != nil {
				return false, fmt.Errorf("failed to save modified free ranges: %w", err)
			}

			break
		}
	}

	if appendToMmap {
		// no free ranges found, so write to the end of the mmap file
		pos.start = b.mmapfEnd
		mmapfNewSize := int64(b.mmapfEnd) + int64(pos.size)
		if err := os.Truncate(b.mmapfPath, mmapfNewSize); err != nil {
			return false, fmt.Errorf("error increasing %s: %w", b.mmapfPath, err)
		}
		b.mmapfEnd = uint64(mmapfNewSize)
	}

	// write to the mmap
	betterbinary.Marshal(*evt, b.mmapf[pos.start:])

	// this is what we will write in the indexes
	posb := make([]byte, 12)
	binary.BigEndian.PutUint32(posb[0:4], pos.size)
	binary.BigEndian.PutUint64(posb[4:12], pos.start)

	// the id index (it already had the refcounts, but was missing the pos)
	copy(val[0:12], posb)

	// each index that was reserved above for the different Layers
	for _, valReserve := range reservedResults {
		copy(valReserve, posb)
	}

	// store the id index with the refcounts
	if err := txn.Put(b.indexId, idPrefix8, val, 0); err != nil {
		panic(fmt.Errorf("failed to store %x by id: %w", idPrefix8, err))
	}

	// msync
	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC,
		uintptr(unsafe.Pointer(&b.mmapf[0])), uintptr(len(b.mmapf)), syscall.MS_SYNC)
	if errno != 0 {
		panic(fmt.Errorf("msync failed: %w", syscall.Errno(errno)))
	}

	return true, nil
}
