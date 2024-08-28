package badger

import (
	"encoding/binary"
	"encoding/hex"

	"github.com/fiatjaf/eventstore"
	"github.com/nbd-wtf/go-nostr"
	"golang.org/x/exp/slices"
)

func getTagIndexPrefix(tagValue string) ([]byte, int) {
	var k []byte   // the key with full length for created_at and idx at the end, but not filled with these
	var offset int // the offset -- i.e. where the prefix ends and the created_at and idx would start

	if kind, pkb, d := eventstore.GetAddrTagElements(tagValue); len(pkb) == 32 {
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

func (b BadgerBackend) getIndexKeysForEvent(evt *nostr.Event, idx []byte) [][]byte {
	keys := make([][]byte, 0, 18)

	// indexes
	{
		// ~ by id
		idPrefix8, _ := hex.DecodeString(evt.ID[0 : 8*2])
		k := make([]byte, 1+8+4)
		k[0] = indexIdPrefix
		copy(k[1:], idPrefix8)
		copy(k[1+8:], idx)
		keys = append(keys, k)
	}

	{
		// ~ by pubkey+date
		pubkeyPrefix8, _ := hex.DecodeString(evt.PubKey[0 : 8*2])
		k := make([]byte, 1+8+4+4)
		k[0] = indexPubkeyPrefix
		copy(k[1:], pubkeyPrefix8)
		binary.BigEndian.PutUint32(k[1+8:], uint32(evt.CreatedAt))
		copy(k[1+8+4:], idx)
		keys = append(keys, k)
	}

	{
		// ~ by kind+date
		k := make([]byte, 1+2+4+4)
		k[0] = indexKindPrefix
		binary.BigEndian.PutUint16(k[1:], uint16(evt.Kind))
		binary.BigEndian.PutUint32(k[1+2:], uint32(evt.CreatedAt))
		copy(k[1+2+4:], idx)
		keys = append(keys, k)
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
		keys = append(keys, k)
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
		keys = append(keys, k)
	}

	{
		// ~ by date only
		k := make([]byte, 1+4+4)
		k[0] = indexCreatedAtPrefix
		binary.BigEndian.PutUint32(k[1:], uint32(evt.CreatedAt))
		copy(k[1+4:], idx)
		keys = append(keys, k)
	}

	return keys
}
