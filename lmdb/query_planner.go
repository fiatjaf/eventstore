package lmdb

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/nbd-wtf/go-nostr"
)

func (b *LMDBBackend) prepareQueries(filter nostr.Filter) (
	queries []query,
	extraAuthors [][32]byte,
	extraKinds [][2]byte,
	extraTagKey string,
	extraTagValues []string,
	since uint32,
	err error,
) {
	// we will apply this to every query we return
	defer func() {
		var until uint32 = 4294967295
		if filter.Until != nil {
			if fu := uint32(*filter.Until); fu < until {
				until = fu + 1
			}
		}
		for i, q := range queries {
			queries[i].startingPoint = binary.BigEndian.AppendUint32(q.prefix, uint32(until))
			queries[i].results = make(chan *nostr.Event, 12)
		}
	}()

	if filter.IDs != nil {
		// when there are ids we ignore everything else
		queries = make([]query, len(filter.IDs))
		for i, idHex := range filter.IDs {
			if len(idHex) != 64 {
				return nil, nil, nil, "", nil, 0, fmt.Errorf("invalid id '%s'", idHex)
			}
			prefix, _ := hex.DecodeString(idHex[0 : 8*2])
			queries[i] = query{i: i, dbi: b.indexId, prefix: prefix, prefixSize: 8, timestampSize: 0}
		}
		return queries, nil, nil, "", nil, 0, nil
	}

	// this is where we'll end the iteration
	if filter.Since != nil {
		if fs := uint32(*filter.Since); fs > since {
			since = fs
		}
	}

	if len(filter.Tags) > 0 {
		// we will select ONE tag to query for and ONE extra tag to do further narrowing, if available
		tagKey, tagValues, goodness := chooseNarrowestTag(filter)
		if goodness <= 2 && (len(filter.Authors) > 0 || len(filter.Kinds) > 0) {
			// we won't use a tag index for this as long as we have something else to match with
			goto pubkeyMatching
		}

		// will use a tag index
		queries = make([]query, len(tagValues))
		for i, value := range tagValues {
			// get key prefix (with full length) and offset where to write the created_at
			dbi, k, offset := b.getTagIndexPrefix(value)
			// remove the last parts part to get just the prefix we want here
			prefix := k[0:offset]
			queries[i] = query{i: i, dbi: dbi, prefix: prefix, prefixSize: len(prefix), timestampSize: 4}
			i++
		}

		if filter.Authors != nil {
			extraAuthors = make([][32]byte, len(filter.Authors))
			for i, pk := range filter.Authors {
				hex.Decode(extraAuthors[i][:], []byte(pk))
			}
		}
		if filter.Kinds != nil {
			extraKinds = make([][2]byte, len(filter.Kinds))
			for i, kind := range filter.Kinds {
				extraKinds[i][0] = byte(kind >> 8)
				extraKinds[i][1] = byte(kind)
			}
		}

		delete(filter.Tags, tagKey)
		if len(filter.Tags) > 0 {
			extraTagKey, extraTagValues, _ = chooseNarrowestTag(filter)
		}

		return queries, extraAuthors, extraKinds, extraTagKey, extraTagValues, since, nil
	}

pubkeyMatching:
	if len(filter.Authors) > 0 {
		if len(filter.Kinds) == 0 {
			// will use pubkey index
			queries = make([]query, len(filter.Authors))
			for i, pubkeyHex := range filter.Authors {
				if len(pubkeyHex) != 64 {
					return nil, nil, nil, "", nil, 0, fmt.Errorf("invalid pubkey '%s'", pubkeyHex)
				}
				prefix, _ := hex.DecodeString(pubkeyHex[0 : 8*2])
				queries[i] = query{i: i, dbi: b.indexPubkey, prefix: prefix, prefixSize: 8, timestampSize: 4}
			}
		} else {
			// will use pubkeyKind index
			queries = make([]query, len(filter.Authors)*len(filter.Kinds))
			i := 0
			for _, pubkeyHex := range filter.Authors {
				for _, kind := range filter.Kinds {
					if len(pubkeyHex) != 64 {
						return nil, nil, nil, "", nil, 0, fmt.Errorf("invalid pubkey '%s'", pubkeyHex)
					}
					pubkey, _ := hex.DecodeString(pubkeyHex[0 : 8*2])
					prefix := binary.BigEndian.AppendUint16(pubkey, uint16(kind))
					queries[i] = query{i: i, dbi: b.indexPubkeyKind, prefix: prefix, prefixSize: 10, timestampSize: 4}
					i++
				}
			}
		}

		// potentially with an extra useless tag filtering
		extraTagKey, extraTagValues, _ = chooseNarrowestTag(filter)
		return queries, nil, nil, extraTagKey, extraTagValues, since, nil
	}

	if len(filter.Kinds) > 0 {
		// will use a kind index
		queries = make([]query, len(filter.Kinds))
		for i, kind := range filter.Kinds {
			prefix := make([]byte, 2)
			binary.BigEndian.PutUint16(prefix[:], uint16(kind))
			queries[i] = query{i: i, dbi: b.indexKind, prefix: prefix, prefixSize: 2, timestampSize: 4}
		}

		// potentially with an extra useless tag filtering
		tagKey, tagValues, _ := chooseNarrowestTag(filter)
		return queries, nil, nil, tagKey, tagValues, since, nil
	}

	// if we got here our query will have nothing to filter with
	queries = make([]query, 1)
	prefix := make([]byte, 0)
	queries[0] = query{i: 0, dbi: b.indexCreatedAt, prefix: prefix, prefixSize: 0, timestampSize: 4}
	return queries, nil, nil, "", nil, since, nil
}

func chooseNarrowestTag(filter nostr.Filter) (key string, values []string, goodness int) {
	var tagKey string
	var tagValues []string
	for key, values := range filter.Tags {
		switch key {
		case "e", "E", "q":
			// 'e' and 'q' are the narrowest possible, so if we have that we will use it and that's it
			tagKey = key
			tagValues = values
			break
		case "a", "A", "i", "I", "g", "r":
			// these are second-best as they refer to relatively static things
			goodness = 9
			tagKey = key
			tagValues = values
		case "d":
			// this is as good as long as we have an "authors"
			if len(filter.Authors) != 0 && goodness < 8 {
				goodness = 8
				tagKey = key
				tagValues = values
			} else if goodness < 4 {
				goodness = 4
				tagKey = key
				tagValues = values
			}
		case "h", "t", "l", "k", "K":
			// these things denote "categories", so they are a little more broad
			goodness = 7
			tagKey = key
			tagValues = values
		case "p":
			// this is broad and useless for a pure tag search, but we will still prefer it over others
			// for secondary filtering
			if goodness < 0 {
				goodness = 2
				tagKey = key
				tagValues = values
			}
		default:
			// all the other tags are probably too broad and useless
			if goodness == 0 {
				tagKey = key
				tagValues = values
			}
		}
	}

	return tagKey, tagValues, goodness
}
