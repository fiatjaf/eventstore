package badger

import (
	"encoding/binary"
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/nbd-wtf/go-nostr"
)

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

func getTagIndexPrefix(tagValue string) ([]byte, int) {
	var k []byte   // the key with full length for created_at and idx at the end, but not filled with these
	var offset int // the offset -- i.e. where the prefix ends and the created_at and idx would start

	if kind, pkb, d := getAddrTagElements(tagValue); len(pkb) == 32 {
		// store value in the new special "a" tag index
		k = make([]byte, 1+2+32+len(d)+4+4)
		k[0] = indexTagAddrPrefix
		binary.BigEndian.PutUint16(k[1:], kind)
		copy(k[1+2:], pkb)
		copy(k[1+2+32:], d)
		offset = 1 + 2 + 32 + len(d)
	} else if vb, _ := hex.DecodeString(tagValue); len(vb) == 32 {
		// store value as bytes
		k = make([]byte, 1+32+4+4)
		k[0] = indexTag32Prefix
		copy(k[1:], vb)
		offset = 1 + 32
	} else {
		// store whatever as utf-8
		k = make([]byte, 1+len(tagValue)+4+4)
		k[0] = indexTagPrefix
		copy(k[1:], tagValue)
		offset = 1 + len(tagValue)
	}

	return k, offset
}

func getIndexKeysForEvent(evt *nostr.Event, idx []byte) [][]byte {
	keys := make([][]byte, 0, 18)

	// indexes
	{
		// ~ by id
		id, _ := hex.DecodeString(evt.ID)
		k := make([]byte, 1+32+4)
		k[0] = indexIdPrefix
		copy(k[1:], id)
		copy(k[1+32:], idx)
		keys = append(keys, k)
	}

	{
		// ~ by pubkey+date
		pubkey, _ := hex.DecodeString(evt.PubKey)
		k := make([]byte, 1+32+4+4)
		k[0] = indexPubkeyPrefix
		copy(k[1:], pubkey)
		binary.BigEndian.PutUint32(k[1+32:], uint32(evt.CreatedAt))
		copy(k[1+32+4:], idx)
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
		pubkey, _ := hex.DecodeString(evt.PubKey)
		k := make([]byte, 1+32+2+4+4)
		k[0] = indexPubkeyKindPrefix
		copy(k[1:], pubkey)
		binary.BigEndian.PutUint16(k[1+32:], uint16(evt.Kind))
		binary.BigEndian.PutUint32(k[1+32+2:], uint32(evt.CreatedAt))
		copy(k[1+32+2+4:], idx)
		keys = append(keys, k)
	}

	// ~ by tagvalue+date
	for _, tag := range evt.Tags {
		if len(tag) < 2 || len(tag[0]) != 1 || len(tag[1]) == 0 || len(tag[1]) > 100 {
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
