package mmm

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"unsafe"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/fiatjaf/eventstore/mmm/betterbinary"
	"github.com/nbd-wtf/go-nostr"
	"github.com/rs/zerolog"
)

type mmap []byte

func (_ mmap) String() string { return "<memory-mapped file>" }

type MultiMmapManager struct {
	Dir          string
	Logger       *zerolog.Logger
	LayerBuilder func(name string, b *MultiMmapManager) *IndexingLayer

	layers []*IndexingLayer
	lastId uint16

	mmapfPath string
	mmapf     mmap
	mmapfEnd  uint64

	lmdbEnv     *lmdb.Env
	stuff       lmdb.DBI
	knownLayers lmdb.DBI
	indexId     lmdb.DBI

	freeRanges []position

	mutex sync.Mutex
}

const (
	MMAP_INFINITE_SIZE = 1 << 40
	maxuint16          = 65535
	maxuint32          = 4294967295
)

var FREERANGES_KEY = []byte{'F'}

func (b *MultiMmapManager) Init() error {
	// create directory if it doesn't exist
	dbpath := filepath.Join(b.Dir, "mmmm")
	if err := os.MkdirAll(dbpath, 0755); err != nil {
		return err
	}

	// open a huge mmapped file
	b.mmapfPath = filepath.Join(b.Dir, "events")
	file, err := os.OpenFile(b.mmapfPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open events file at %s: %w", b.mmapfPath, err)
	}
	mmapf, err := syscall.Mmap(int(file.Fd()), 0, MMAP_INFINITE_SIZE,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("failed to mmap events file at %s: %w", b.mmapfPath, err)
	}
	b.mmapf = mmapf

	if stat, err := os.Stat(b.mmapfPath); err != nil {
		return err
	} else {
		b.mmapfEnd = uint64(stat.Size())
	}

	// open lmdb
	env, err := lmdb.NewEnv()
	if err != nil {
		return err
	}

	env.SetMaxDBs(3)
	env.SetMaxReaders(1000)
	env.SetMapSize(1 << 38) // ~273GB

	err = env.Open(dbpath, lmdb.NoTLS, 0644)
	if err != nil {
		return fmt.Errorf("failed to open lmdb at %s: %w", dbpath, err)
	}
	b.lmdbEnv = env

	if err := b.lmdbEnv.Update(func(txn *lmdb.Txn) error {
		if dbi, err := txn.OpenDBI("stuff", lmdb.Create); err != nil {
			return err
		} else {
			b.stuff = dbi
		}

		// this just keeps track of all the layers we know (just their names)
		// they will be instantiated by the application after their name is read from the database.
		// new layers created at runtime will be saved here.
		if dbi, err := txn.OpenDBI("layers", lmdb.Create); err != nil {
			return err
		} else {
			b.knownLayers = dbi
		}

		// this is a global index of events by id that also keeps references
		// to all the layers that may be indexing them -- such that whenever
		// an event is deleted from all layers it can be deleted from global
		if dbi, err := txn.OpenDBI("id-references", lmdb.Create); err != nil {
			return err
		} else {
			b.indexId = dbi
		}

		// load all free ranges into memory
		{
			data, err := txn.Get(b.stuff, FREERANGES_KEY)
			if err != nil && !lmdb.IsNotFound(err) {
				return fmt.Errorf("on freeranges: %w", err)
			}
			b.freeRanges = make([]position, len(data)/12)
			logOp := b.Logger.Debug()
			for f := range b.freeRanges {
				pos := positionFromBytes(data[f*12 : (f+1)*12])
				b.freeRanges[f] = pos
				logOp = logOp.Uint32(fmt.Sprintf("%d", pos.start), pos.size)
			}
			slices.SortFunc(b.freeRanges, func(a, b position) int { return int(a.size - b.size) })
			logOp.Msg("loaded free ranges")
		}

		// initialize layers from what we have stored
		{
			logOp := b.Logger.Debug()

			cursor, err := txn.OpenCursor(b.knownLayers)
			if err != nil && !lmdb.IsNotFound(err) {
				return fmt.Errorf("on layers: %w", err)
			}

			b.layers = make([]*IndexingLayer, 0, 20)
			for k, v, err := cursor.Get(nil, nil, lmdb.First); err == nil; k, v, err = cursor.Get(nil, nil, lmdb.Next) {
				name := string(k)
				id := binary.BigEndian.Uint16(v)
				logOp.Str("name", name).Msg("loaded layer")

				il := b.LayerBuilder(name, b)
				if il == nil {
					logOp.Str("name", name).Msg("layer was deleted")
					if err := txn.Del(b.knownLayers, k, nil); err != nil {
						return fmt.Errorf("on delete '%s': %w", name, err)
					}

					// remove all events that were only referenced by this layer
					if err := b.removeAllReferencesFromLayer(txn, binary.BigEndian.Uint16(v)); err != nil {
						return fmt.Errorf("on removing references: %w", err)
					}

					continue
				}

				il.name = name
				il.id = id

				if b.lastId < id {
					b.lastId = id
				}

				il.Init()
				b.layers = append(b.layers, il)
			}
		}

		return nil
	}); err != nil {
		return fmt.Errorf("failed to open and load db data: %w", err)
	}

	return nil
}

func (b *MultiMmapManager) CreateLayer(name string) error {
	il := b.LayerBuilder(name, b)
	if il == nil {
		return fmt.Errorf("tried to create layer %s, but got a nil", name)
	}

	il.name = name

	b.lastId++
	if b.lastId == 0 {
		return fmt.Errorf("reached end of available layer ids, mmm needs a refactor to be able to reuse ids or something like that")
	}
	il.id = b.lastId

	il.Init()

	err := b.lmdbEnv.Update(func(txn *lmdb.Txn) error {
		if err := il.runThroughEvents(txn); err != nil {
			return err
		}
		return txn.Put(b.knownLayers, []byte(name), nil, 0)
	})
	if err != nil {
		return err
	}

	b.layers = append(b.layers, il)
	return nil
}

func (b *MultiMmapManager) Close() {
	b.lmdbEnv.Close()
	for _, il := range b.layers {
		il.Close()
	}
}

func (b *MultiMmapManager) Load(pos position, eventReceiver *nostr.Event) error {
	return betterbinary.Unmarshal(b.mmapf[pos.start:pos.start+uint64(pos.size)], eventReceiver)
}

func (b *MultiMmapManager) String() string {
	return fmt.Sprintf("<MultiMmapManager on %s with %d layers @ %v>", b.Dir, len(b.layers), unsafe.Pointer(b))
}

func (b *MultiMmapManager) removeAllReferencesFromLayer(txn *lmdb.Txn, layerId uint16) error {
	cursor, err := txn.OpenCursor(b.indexId)
	if err != nil {
		return fmt.Errorf("when opening cursor on %v: %w", b.indexId, err)
	}
	defer cursor.Close()

	for {
		idPrefix8, val, err := cursor.Get(nil, nil, lmdb.Next)
		if lmdb.IsNotFound(err) {
			break
		}
		if err != nil {
			return fmt.Errorf("when moving the cursor: %w", err)
		}

		var zeroRefs bool
		var update bool

		needle := binary.BigEndian.AppendUint16(nil, layerId)
		for s := 12; s < len(val); s += 2 {
			if slices.Equal(val[s:s+2], needle) {
				val = slices.Delete(val, s, s+2)
				update = true

				// we must erase this event if its references reach zero
				zeroRefs = len(val) == 12

				break
			}
		}

		if zeroRefs {
			posb := val[0:12]
			pos := positionFromBytes(posb)

			if err := b.purge(txn, idPrefix8, pos); err != nil {
				return fmt.Errorf("failed to purge unreferenced event %x: %w", idPrefix8, err)
			}
		} else if update {
			if err := txn.Put(b.indexId, idPrefix8, val, 0); err != nil {
				return fmt.Errorf("failed to put updated index+refs: %w", err)
			}
		}
	}

	return nil
}
