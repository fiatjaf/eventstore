package betterbinary

import (
	"encoding/binary"
	"slices"
)

func TagMatches(evtb []byte, key string, vals []string) bool {
	matches := make([][]byte, 0, len(vals))
	for _, val := range vals {
		match := append([]byte{1, 0, key[0], uint8(len(val)), 0}, val...)
		matches = append(matches, match)
	}

	ntags := binary.LittleEndian.Uint16(evtb[137:])
	var t uint16
	for t = 0; t < ntags; t++ {
		offset := int(binary.LittleEndian.Uint16(evtb[139+t*2:]))
		nitems := evtb[135+offset]
		if nitems >= 2 {
			for _, match := range matches {
				if slices.Equal(evtb[135+offset+1:135+offset+1+len(match)], match) {
					return true
				}
			}
		}
	}
	return false
}

func KindMatches(evtb []byte, kind uint16) bool {
	return binary.LittleEndian.Uint16(evtb[1:3]) == kind
}
