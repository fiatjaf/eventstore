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

func (b *MultiMmapManager) StoreGlobal(ctx context.Context, evt *nostr.Event) (stored bool, err error) {
	someoneWantsIt := false

	b.mutex.Lock()
	defer b.mutex.Unlock()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// do this just so it's cleaner, we're already locking the thread and the mutex anyway
	mmmtxn, err := b.lmdbEnv.BeginTxn(nil, 0)
	if err != nil {
		return false, fmt.Errorf("failed to begin global transaction: %w", err)
	}
	mmmtxn.RawRead = true

	iltxns := make([]*lmdb.Txn, 0, len(b.layers))
	ils := make([]*IndexingLayer, 0, len(b.layers))

	// ask if any of the indexing layers want this
	for _, il := range b.layers {
		if il.ShouldIndex != nil && il.ShouldIndex(ctx, evt) {
			someoneWantsIt = true

			iltxn, err := il.lmdbEnv.BeginTxn(nil, 0)
			if err != nil {
				mmmtxn.Abort()
				for _, txn := range iltxns {
					txn.Abort()
				}
				return false, fmt.Errorf("failed to start txn on %s: %w", il.name, err)
			}

			ils = append(ils, il)
			iltxns = append(iltxns, iltxn)
		}
	}

	if !someoneWantsIt {
		// no one wants it
		mmmtxn.Abort()
		return false, fmt.Errorf("not wanted")
	}

	stored, err = b.storeOn(mmmtxn, ils, iltxns, evt)
	if stored {
		mmmtxn.Commit()
		for _, txn := range iltxns {
			txn.Commit()
		}
	} else {
		mmmtxn.Abort()
		for _, txn := range iltxns {
			txn.Abort()
		}
	}

	return stored, err
}

func (il *IndexingLayer) SaveEvent(ctx context.Context, evt *nostr.Event) error {
	il.mmmm.mutex.Lock()
	defer il.mmmm.mutex.Unlock()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// do this just so it's cleaner, we're already locking the thread and the mutex anyway
	mmmtxn, err := il.mmmm.lmdbEnv.BeginTxn(nil, 0)
	if err != nil {
		return fmt.Errorf("failed to begin global transaction: %w", err)
	}
	mmmtxn.RawRead = true

	iltxn, err := il.lmdbEnv.BeginTxn(nil, 0)
	if err != nil {
		mmmtxn.Abort()
		return fmt.Errorf("failed to start txn on %s: %w", il.name, err)
	}

	if _, err := il.mmmm.storeOn(mmmtxn, []*IndexingLayer{il}, []*lmdb.Txn{iltxn}, evt); err != nil {
		mmmtxn.Abort()
		if iltxn != nil {
			iltxn.Abort()
		}
		return err
	}

	mmmtxn.Commit()
	iltxn.Commit()
	return nil
}

func (b *MultiMmapManager) storeOn(
	mmmtxn *lmdb.Txn,
	ils []*IndexingLayer,
	iltxns []*lmdb.Txn,
	evt *nostr.Event,
) (stored bool, err error) {
	// sanity checking
	if evt.CreatedAt > maxuint32 || evt.Kind > maxuint16 {
		return false, fmt.Errorf("event with values out of expected boundaries")
	}

	// check if we already have this id
	idPrefix8, _ := hex.DecodeString(evt.ID[0 : 8*2])
	val, err := mmmtxn.Get(b.indexId, idPrefix8)
	if err == nil {
		// we found the event, now check if it is already indexed by the layers that want to store it
		for i := len(ils) - 1; i >= 0; i-- {
			for s := 12; s < len(val); s += 2 {
				ilid := binary.BigEndian.Uint16(val[s : s+2])
				if ils[i].id == ilid {
					// swap delete this il, but keep the deleted ones at the end
					// (so the caller can successfully finalize the transactions)
					ils[i], ils[len(ils)-1] = ils[len(ils)-1], ils[i]
					ils = ils[0 : len(ils)-1]
					iltxns[i], iltxns[len(iltxns)-1] = iltxns[len(iltxns)-1], iltxns[i]
					iltxns = iltxns[0 : len(iltxns)-1]
					break
				}
			}
		}
	} else if !lmdb.IsNotFound(err) {
		// now if we got an error from lmdb we will only proceed if we get a NotFound -- for anything else we will error
		return false, fmt.Errorf("error checking existence: %w", err)
	}

	// if all ils already have this event indexed (or no il was given) we can end here
	if len(ils) == 0 {
		return false, nil
	}

	// get event binary size
	pos := position{
		size: uint32(betterbinary.Measure(*evt)),
	}
	if pos.size >= 1<<16 {
		return false, fmt.Errorf("event too large to store, max %d, got %d", 1<<16, pos.size)
	}

	// find a suitable place for this to be stored in
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

			if err := b.saveFreeRanges(mmmtxn); err != nil {
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
	if err := betterbinary.Marshal(*evt, b.mmapf[pos.start:]); err != nil {
		return false, fmt.Errorf("error marshaling to %d: %w", pos.start, err)
	}

	// prepare value to be saved in the id index (if we didn't have it already)
	// val: [posb][layerIdRefs...]
	if val == nil {
		val = make([]byte, 12, 12+2*len(b.layers))
		binary.BigEndian.PutUint32(val[0:4], pos.size)
		binary.BigEndian.PutUint64(val[4:12], pos.start)
	}

	// each index that was reserved above for the different layers
	for i, il := range ils {
		iltxn := iltxns[i]

		for k := range il.getIndexKeysForEvent(evt) {
			if err := iltxn.Put(k.dbi, k.key, val[0:12] /* pos */, 0); err != nil {
				b.Logger.Warn().Str("name", il.name).Msg("failed to index event on layer")
			}
		}

		val = binary.BigEndian.AppendUint16(val, il.id)
	}

	// store the id index with the refcounts
	if err := mmmtxn.Put(b.indexId, idPrefix8, val, 0); err != nil {
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
