package edgedb

import (
	"context"

	"github.com/edgedb/edgedb-go"
	"github.com/fiatjaf/eventstore"
)

var _ eventstore.Store = (*EdgeDBBackend)(nil)

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
	return nil
}
