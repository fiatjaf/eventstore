package edgedb

import "github.com/edgedb/edgedb-go"

type EdgeDBBackend struct {
	*edgedb.Client
	DatabaseURI       string
	TLSSkipVerify     bool
	QueryIDsLimit     int
	QueryAuthorsLimit int
	QueryKindsLimit   int
	QueryTagsLimit    int
	QueryLimit        int
}

// Close implements the Close method of the eventstore.Store interface
func (b *EdgeDBBackend) Close() {
	b.Client.Close()
}
