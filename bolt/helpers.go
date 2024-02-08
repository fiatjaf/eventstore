package bolt

import (
	"encoding/binary"
	"encoding/hex"

	"github.com/fiatjaf/eventstore"
	"github.com/nbd-wtf/go-nostr"
	"golang.org/x/exp/slices"
)

// returns
// - the bucket id where this will be saved
// - the key with full length for created_at and idx at the end, but not filled with these
// - the offset -- i.e. where the prefix ends and the created_at and idx would start
func getTagIndexPrefix(tagValue string) (bucket []byte, key []byte, offset int) {
	if kind, pkb, d := eventstore.GetAddrTagElements(tagValue); len(pkb) == 32 {
		// store value in the new special "a" tag index
		key = make([]byte, 2+8+len(d)+4)
		binary.BigEndian.PutUint16(key[1:], kind)
		copy(key[2:], pkb[0:8])
		copy(key[2+8:], d)
		offset = 2 + 8 + len(d)
		bucket = bucketTagAddr
	} else if vb, _ := hex.DecodeString(tagValue); len(vb) == 32 {
		// store value as bytes
		key = make([]byte, 8+4)
		copy(key, vb[0:8])
		offset = 8
		bucket = bucketTag32
	} else {
		// store whatever as utf-8
		key = make([]byte, len(tagValue)+4)
		copy(key, tagValue)
		offset = len(tagValue)
		bucket = bucketTag
	}

	return bucket, key, offset
}

type keymeta struct {
	bucket []byte
	key    []byte
}

func getIndexKeysForEvent(evt *nostr.Event) []keymeta {
	keys := make([]keymeta, 0, 24)

	// indexes
	{
		// ~ by id
		idPrefix8, _ := hex.DecodeString(evt.ID[0 : 8*2])
		k := idPrefix8
		keys = append(keys, keymeta{bucket: bucketId, key: k})
	}

	{
		// ~ by pubkey+date
		pubkeyPrefix8, _ := hex.DecodeString(evt.PubKey[0 : 8*2])
		k := make([]byte, 8+4)
		copy(k[:], pubkeyPrefix8)
		binary.BigEndian.PutUint32(k[8:], uint32(evt.CreatedAt))
		keys = append(keys, keymeta{bucket: bucketPubkey, key: k})
	}

	{
		// ~ by kind+date
		k := make([]byte, 2+4)
		binary.BigEndian.PutUint16(k[:], uint16(evt.Kind))
		binary.BigEndian.PutUint32(k[2:], uint32(evt.CreatedAt))
		keys = append(keys, keymeta{bucket: bucketKind, key: k})
	}

	{
		// ~ by pubkey+kind+date
		pubkeyPrefix8, _ := hex.DecodeString(evt.PubKey[0 : 8*2])
		k := make([]byte, 8+2+4)
		copy(k[:], pubkeyPrefix8)
		binary.BigEndian.PutUint16(k[8:], uint16(evt.Kind))
		binary.BigEndian.PutUint32(k[8+2:], uint32(evt.CreatedAt))
		keys = append(keys, keymeta{bucket: bucketPubkeyKind, key: k})
	}

	// ~ by tagvalue+date
	for i, tag := range evt.Tags {
		if len(tag) < 2 || len(tag[0]) != 1 || len(tag[1]) == 0 || len(tag[1]) > 100 {
			// not indexable
			continue
		}
		firstIndex := slices.IndexFunc(evt.Tags, func(t nostr.Tag) bool { return len(t) >= 2 && t[1] == tag[1] })
		if firstIndex != i {
			// duplicate
			continue
		}

		// get key prefix (with full length) and offset where to write the created_at
		bucket, k, offset := getTagIndexPrefix(tag[1])

		// write the created_at
		binary.BigEndian.PutUint32(k[offset:], uint32(evt.CreatedAt))

		keys = append(keys, keymeta{bucket: bucket, key: k})
	}

	{
		// ~ by date only
		k := make([]byte, 4)
		binary.BigEndian.PutUint32(k[:], uint32(evt.CreatedAt))
		keys = append(keys, keymeta{bucket: bucketCreatedAt, key: k})
	}

	return keys
}
