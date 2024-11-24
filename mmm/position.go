package mmm

import (
	"encoding/binary"
	"fmt"
)

type position struct {
	start uint64
	size  uint32
}

func (pos position) String() string {
	return fmt.Sprintf("<%d|%d|%d>", pos.start, pos.size, pos.start+uint64(pos.size))
}

func positionFromBytes(posb []byte) position {
	return position{
		size:  binary.BigEndian.Uint32(posb[0:4]),
		start: binary.BigEndian.Uint64(posb[4:12]),
	}
}

func bytesFromPosition(out []byte, pos position) {
	binary.BigEndian.PutUint32(out[0:4], pos.size)
	binary.BigEndian.PutUint64(out[4:12], pos.start)
}
