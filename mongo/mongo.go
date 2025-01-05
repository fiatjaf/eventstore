package mongo

import (
	"context"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type MongoDBBackend struct {
	*mongo.Client
	ctx               context.Context
	DatabaseURL       string
	QueryLimit        int
	QueryIDsLimit     int
	QueryAuthorsLimit int
	QueryKindsLimit   int
	QueryTagsLimit    int
}

func (m *MongoDBBackend) Close() {
	m.Client.Disconnect(m.ctx)
}
