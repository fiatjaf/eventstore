package lmdb

import (
	"encoding/binary"
	"encoding/hex"
	"iter"
	"strconv"
	"strings"
	"sync"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/nbd-wtf/go-nostr"
	"golang.org/x/exp/slices"
)

var indexKeyPool = sync.Pool{
	New: func() any { return make([]byte, 2+8+4+30) },
}

type key struct {
	dbi lmdb.DBI
	key []byte
}

func (key key) free() {
	indexKeyPool.Put(key.key)
}

func (b *LMDBBackend) getIndexKeysForEvent(evt *nostr.Event) iter.Seq[key] {
	return func(yield func(key) bool) {
		{
			// ~ by id
			k := indexKeyPool.Get().([]byte)
			hex.Decode(k[0:8], []byte(evt.ID[0:8*2]))
			if !yield(key{dbi: b.indexId, key: k[0:8]}) {
				return
			}
		}

		{
			// ~ by pubkey+date
			k := indexKeyPool.Get().([]byte)
			hex.Decode(k[0:8], []byte(evt.PubKey[0:8*2]))
			binary.BigEndian.PutUint32(k[8:8+4], uint32(evt.CreatedAt))
			if !yield(key{dbi: b.indexPubkey, key: k[0 : 8+4]}) {
				return
			}
		}

		{
			// ~ by kind+date
			k := indexKeyPool.Get().([]byte)
			binary.BigEndian.PutUint16(k[0:2], uint16(evt.Kind))
			binary.BigEndian.PutUint32(k[2:2+4], uint32(evt.CreatedAt))
			if !yield(key{dbi: b.indexKind, key: k[0 : 2+4]}) {
				return
			}
		}

		{
			// ~ by pubkey+kind+date
			k := indexKeyPool.Get().([]byte)
			hex.Decode(k[0:8], []byte(evt.PubKey[0:8*2]))
			binary.BigEndian.PutUint16(k[8:8+2], uint16(evt.Kind))
			binary.BigEndian.PutUint32(k[8+2:8+2+4], uint32(evt.CreatedAt))
			if !yield(key{dbi: b.indexPubkeyKind, key: k[0 : 8+2+4]}) {
				return
			}
		}

		// ~ by tagvalue+date
		// ~ by p-tag+kind+date
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
			dbi, k, offset := b.getTagIndexPrefix(tag[1])
			binary.BigEndian.PutUint32(k[offset:], uint32(evt.CreatedAt))
			if !yield(key{dbi: dbi, key: k}) {
				return
			}

			// now the p-tag+kind+date
			if dbi == b.indexTag32 && tag[0] == "p" {
				k := indexKeyPool.Get().([]byte)
				hex.Decode(k[0:8], []byte(tag[1][0:8*2]))
				binary.BigEndian.PutUint16(k[8:8+2], uint16(evt.Kind))
				binary.BigEndian.PutUint32(k[8+2:8+2+4], uint32(evt.CreatedAt))
				dbi := b.indexPTagKind
				if !yield(key{dbi: dbi, key: k[0 : 8+2+4]}) {
					return
				}
			}
		}

		{
			// ~ by date only
			k := indexKeyPool.Get().([]byte)
			binary.BigEndian.PutUint32(k[0:4], uint32(evt.CreatedAt))
			if !yield(key{dbi: b.indexCreatedAt, key: k[0:4]}) {
				return
			}
		}
	}
}

func (b *LMDBBackend) getTagIndexPrefix(tagValue string) (lmdb.DBI, []byte, int) {
	var k []byte   // the key with full length for created_at and idx at the end, but not filled with these
	var offset int // the offset -- i.e. where the prefix ends and the created_at and idx would start
	var dbi lmdb.DBI

	k = indexKeyPool.Get().([]byte)

	// if it's 32 bytes as hex, save it as bytes
	if len(tagValue) == 64 {
		// but we actually only use the first 8 bytes
		if _, err := hex.Decode(k[0:8], []byte(tagValue[0:8*2])); err == nil {
			offset = 8
			dbi = b.indexTag32
			return dbi, k[0 : offset+4], offset
		}
	}

	// if it looks like an "a" tag, index it in this special format
	spl := strings.Split(tagValue, ":")
	if len(spl) == 3 && len(spl[1]) == 64 {
		if _, err := hex.Decode(k[2:2+8], []byte(tagValue[0:8*2])); err == nil {
			if kind, err := strconv.ParseUint(spl[0], 10, 16); err == nil {
				k[0] = byte(kind >> 8)
				k[1] = byte(kind)
				// limit "d" identifier to 30 bytes (so we don't have to grow our byte slice)
				n := copy(k[2+8:2+8+30], spl[2])
				offset = 2 + 8 + n
				return dbi, k[0 : offset+4], offset
			}
		}
	}

	// index whatever else as utf-8, but limit it to 40 bytes
	n := copy(k[0:40], tagValue)
	offset = n
	dbi = b.indexTag

	return dbi, k[0 : n+4], offset
}
