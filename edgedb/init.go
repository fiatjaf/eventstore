package edgedb

import (
	"context"

	"github.com/edgedb/edgedb-go"
	"github.com/fiatjaf/eventstore"
)

var _ eventstore.Store = (*EdgeDBBackend)(nil)

const (
	queryLimit        = 100
	queryIDsLimit     = 500
	queryAuthorsLimit = 500
	queryKindsLimit   = 10
	queryTagsLimit    = 10
)

// Init implements the Init method of the eventstore.Store inteface.
// It establishes the connection with Edgedb
func (b *EdgeDBBackend) Init() error {
	opts := edgedb.Options{}
	if b.TLSSkipVerify {
		opts.TLSOptions = edgedb.TLSOptions{SecurityMode: edgedb.TLSModeInsecure}
	}
	dbConn, err := edgedb.CreateClientDSN(context.Background(), b.DatabaseURI, opts)
	if err != nil {
		return err
	}
	b.Client = dbConn
	if b.QueryAuthorsLimit == 0 {
		b.QueryAuthorsLimit = queryAuthorsLimit
	}
	if b.QueryLimit == 0 {
		b.QueryLimit = queryLimit
	}
	if b.QueryIDsLimit == 0 {
		b.QueryIDsLimit = queryIDsLimit
	}
	if b.QueryKindsLimit == 0 {
		b.QueryKindsLimit = queryKindsLimit
	}
	if b.QueryTagsLimit == 0 {
		b.QueryTagsLimit = queryTagsLimit
	}
	return nil
}
