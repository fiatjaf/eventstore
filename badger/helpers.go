package badger

import (
	"cmp"
	"encoding/binary"
	"encoding/hex"
	"iter"
	"math"
	"strconv"
	"strings"

	mergesortedslices "fiatjaf.com/lib/merge-sorted-slices"
	"github.com/nbd-wtf/go-nostr"
	"golang.org/x/exp/slices"
)

func getTagIndexPrefix(tagValue string) ([]byte, int) {
	var k []byte   // the key with full length for created_at and idx at the end, but not filled with these
	var offset int // the offset -- i.e. where the prefix ends and the created_at and idx would start

	if kind, pkb, d := getAddrTagElements(tagValue); len(pkb) == 32 {
		// store value in the new special "a" tag index
		k = make([]byte, 1+2+8+len(d)+4+4)
		k[0] = indexTagAddrPrefix
		binary.BigEndian.PutUint16(k[1:], kind)
		copy(k[1+2:], pkb[0:8])
		copy(k[1+2+8:], d)
		offset = 1 + 2 + 8 + len(d)
	} else if vb, _ := hex.DecodeString(tagValue); len(vb) == 32 {
		// store value as bytes
		k = make([]byte, 1+8+4+4)
		k[0] = indexTag32Prefix
		copy(k[1:], vb[0:8])
		offset = 1 + 8
	} else {
		// store whatever as utf-8
		k = make([]byte, 1+len(tagValue)+4+4)
		k[0] = indexTagPrefix
		copy(k[1:], tagValue)
		offset = 1 + len(tagValue)
	}

	return k, offset
}

func (b *BadgerBackend) getIndexKeysForEvent(evt *nostr.Event, idx []byte) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		{
			// ~ by id
			idPrefix8, _ := hex.DecodeString(evt.ID[0 : 8*2])
			k := make([]byte, 1+8+4)
			k[0] = indexIdPrefix
			copy(k[1:], idPrefix8)
			copy(k[1+8:], idx)
			if !yield(k) {
				return
			}
		}

		{
			// ~ by pubkey+date
			pubkeyPrefix8, _ := hex.DecodeString(evt.PubKey[0 : 8*2])
			k := make([]byte, 1+8+4+4)
			k[0] = indexPubkeyPrefix
			copy(k[1:], pubkeyPrefix8)
			binary.BigEndian.PutUint32(k[1+8:], uint32(evt.CreatedAt))
			copy(k[1+8+4:], idx)
			if !yield(k) {
				return
			}
		}

		{
			// ~ by kind+date
			k := make([]byte, 1+2+4+4)
			k[0] = indexKindPrefix
			binary.BigEndian.PutUint16(k[1:], uint16(evt.Kind))
			binary.BigEndian.PutUint32(k[1+2:], uint32(evt.CreatedAt))
			copy(k[1+2+4:], idx)
			if !yield(k) {
				return
			}
		}

		{
			// ~ by pubkey+kind+date
			pubkeyPrefix8, _ := hex.DecodeString(evt.PubKey[0 : 8*2])
			k := make([]byte, 1+8+2+4+4)
			k[0] = indexPubkeyKindPrefix
			copy(k[1:], pubkeyPrefix8)
			binary.BigEndian.PutUint16(k[1+8:], uint16(evt.Kind))
			binary.BigEndian.PutUint32(k[1+8+2:], uint32(evt.CreatedAt))
			copy(k[1+8+2+4:], idx)
			if !yield(k) {
				return
			}
		}

		// ~ by tagvalue+date
		customIndex := b.IndexLongerTag != nil
		customSkip := b.SkipIndexingTag != nil

		for i, tag := range evt.Tags {
			if len(tag) < 2 || len(tag[0]) != 1 || len(tag[1]) == 0 || len(tag[1]) > 100 {
				if !customIndex || !b.IndexLongerTag(evt, tag[0], tag[1]) {
					// not indexable
					continue
				}
			}

			firstIndex := slices.IndexFunc(evt.Tags, func(t nostr.Tag) bool { return len(t) >= 2 && t[1] == tag[1] })
			if firstIndex != i {
				// duplicate
				continue
			}

			if customSkip && b.SkipIndexingTag(evt, tag[0], tag[1]) {
				// purposefully skipped
				continue
			}

			// get key prefix (with full length) and offset where to write the last parts
			k, offset := getTagIndexPrefix(tag[1])

			// write the last parts (created_at and idx)
			binary.BigEndian.PutUint32(k[offset:], uint32(evt.CreatedAt))
			copy(k[offset+4:], idx)
			if !yield(k) {
				return
			}
		}

		{
			// ~ by date only
			k := make([]byte, 1+4+4)
			k[0] = indexCreatedAtPrefix
			binary.BigEndian.PutUint32(k[1:], uint32(evt.CreatedAt))
			copy(k[1+4:], idx)
			if !yield(k) {
				return
			}
		}
	}
}

func getAddrTagElements(tagValue string) (kind uint16, pkb []byte, d string) {
	spl := strings.Split(tagValue, ":")
	if len(spl) == 3 {
		if pkb, _ := hex.DecodeString(spl[1]); len(pkb) == 32 {
			if kind, err := strconv.ParseUint(spl[0], 10, 16); err == nil {
				return uint16(kind), pkb, spl[2]
			}
		}
	}
	return 0, nil, ""
}

// mergeSortMultipleBatches takes the results of multiple iterators, which are already sorted,
// and merges them into a single big sorted slice
func mergeSortMultiple(batches [][]iterEvent, limit int, dst []iterEvent) []iterEvent {
	// clear up empty lists here while simultaneously computing the total count.
	// this helps because if there are a bunch of empty lists then this pre-clean
	//   step will get us in the faster 'merge' branch otherwise we would go to the other.
	// we would have to do the cleaning anyway inside it.
	// and even if we still go on the other we save one iteration by already computing the
	//   total count.
	total := 0
	for i := len(batches) - 1; i >= 0; i-- {
		if len(batches[i]) == 0 {
			batches = swapDelete(batches, i)
		} else {
			total += len(batches[i])
		}
	}

	if limit == -1 {
		limit = total
	}

	// this amazing equation will ensure that if one of the two sides goes very small (like 1 or 2)
	//   the other can go very high (like 500) and we're still in the 'merge' branch.
	// if values go somewhere in the middle then they may match the 'merge' branch (batches=20,limit=70)
	//   or not (batches=25, limit=60)
	if math.Log(float64(len(batches)*2))+math.Log(float64(limit)) < 8 {
		if dst == nil {
			dst = make([]iterEvent, limit)
		} else if cap(dst) < limit {
			dst = slices.Grow(dst, limit-len(dst))
		}
		dst = dst[0:limit]
		return mergesortedslices.MergeFuncNoEmptyListsIntoSlice(dst, batches, compareIterEvent)
	} else {
		if dst == nil {
			dst = make([]iterEvent, total)
		} else if cap(dst) < total {
			dst = slices.Grow(dst, total-len(dst))
		}
		dst = dst[0:total]

		// use quicksort in a dumb way that will still be fast because it's cheated
		lastIndex := 0
		for _, batch := range batches {
			copy(dst[lastIndex:], batch)
			lastIndex += len(batch)
		}

		slices.SortFunc(dst, compareIterEvent)

		for i, j := 0, total-1; i < j; i, j = i+1, j-1 {
			dst[i], dst[j] = dst[j], dst[i]
		}

		if limit < len(dst) {
			return dst[0:limit]
		}
		return dst
	}
}

// batchSizePerNumberOfQueries tries to make an educated guess for the batch size given the total filter limit and
// the number of abstract queries we'll be conducting at the same time
func batchSizePerNumberOfQueries(totalFilterLimit int, numberOfQueries int) int {
	if numberOfQueries == 1 || totalFilterLimit*numberOfQueries < 50 {
		return totalFilterLimit
	}

	return int(
		math.Ceil(
			math.Pow(float64(totalFilterLimit), 0.80) / math.Pow(float64(numberOfQueries), 0.71),
		),
	)
}

func filterMatchesTags(ef *nostr.Filter, event *nostr.Event) bool {
	for f, v := range ef.Tags {
		if v != nil && !event.Tags.ContainsAny(f, v) {
			return false
		}
	}
	return true
}

func swapDelete[A any](arr []A, i int) []A {
	arr[i] = arr[len(arr)-1]
	return arr[:len(arr)-1]
}

func compareIterEvent(a, b iterEvent) int {
	if a.Event == nil {
		if b.Event == nil {
			return 0
		} else {
			return -1
		}
	} else if b.Event == nil {
		return 1
	}

	if a.CreatedAt == b.CreatedAt {
		return strings.Compare(a.ID, b.ID)
	}
	return cmp.Compare(a.CreatedAt, b.CreatedAt)
}
