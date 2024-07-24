package edgedb

import "github.com/edgedb/edgedb-go"

type EdgeDBBackend struct {
	*edgedb.Client
	DatabaseURI   string
	TLSSkipVerify bool
}

// Close implements the Close method of the eventstore.Store interface
func (b *EdgeDBBackend) Close() {
	b.Client.Close()
}
