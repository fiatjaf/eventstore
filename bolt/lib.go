package bolt

import (
	"sync/atomic"

	"github.com/boltdb/bolt"
	"github.com/fiatjaf/eventstore"
)

const (
	maxuint16 = 65535
	maxuint32 = 4294967295
)

var (
	bucketSettings   = []byte{99}
	bucketRaw        = []byte{1}
	bucketCreatedAt  = []byte{2}
	bucketId         = []byte{3}
	bucketKind       = []byte{4}
	bucketPubkey     = []byte{5}
	bucketPubkeyKind = []byte{6}
	bucketTag        = []byte{7}
	bucketTag32      = []byte{8}
	bucketTagAddr    = []byte{9}
)

var _ eventstore.Store = (*BoltBackend)(nil)

type BoltBackend struct {
	Path     string
	MaxLimit int

	db *bolt.DB

	lastId atomic.Uint32
}

func (b *BoltBackend) Init() error {
	if b.MaxLimit == 0 {
		b.MaxLimit = 500
	}

	// open boltdb
	db, err := bolt.Open(b.Path, 0644, nil)
	if err != nil {
		return err
	}
	b.db = db

	// open each bucket
	if err := b.db.Update(func(txn *bolt.Tx) error {
		if _, err := txn.CreateBucket(bucketSettings); err != nil && err != bolt.ErrBucketExists {
			return err
		}
		if _, err := txn.CreateBucket(bucketRaw); err != nil && err != bolt.ErrBucketExists {
			return err
		}
		if _, err := txn.CreateBucket(bucketCreatedAt); err != nil && err != bolt.ErrBucketExists {
			return err
		}
		if _, err := txn.CreateBucket(bucketId); err != nil && err != bolt.ErrBucketExists {
			return err
		}
		if _, err := txn.CreateBucket(bucketKind); err != nil && err != bolt.ErrBucketExists {
			return err
		}
		if _, err := txn.CreateBucket(bucketPubkey); err != nil && err != bolt.ErrBucketExists {
			return err
		}
		if _, err := txn.CreateBucket(bucketPubkeyKind); err != nil && err != bolt.ErrBucketExists {
			return err
		}
		if _, err := txn.CreateBucket(bucketTag); err != nil && err != bolt.ErrBucketExists {
			return err
		}
		if _, err := txn.CreateBucket(bucketTag32); err != nil && err != bolt.ErrBucketExists {
			return err
		}
		if _, err := txn.CreateBucket(bucketTagAddr); err != nil && err != bolt.ErrBucketExists {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	return b.runMigrations()
}

func (b *BoltBackend) Close() {
	b.db.Close()
}
