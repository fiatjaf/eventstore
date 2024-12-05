package mongo

import (
	"context"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"net/url"
)

const (
	queryLimit        = 100
	queryIDsLimit     = 500
	queryAuthorsLimit = 500
	queryKindsLimit   = 10
	queryTagsLimit    = 10
)

func (m *MongoDBBackend) Init() error {
	m.ctx = context.Background()
	client, err := mongo.Connect(options.Client().ApplyURI(url.QueryEscape(m.DatabaseURL)))
	if err != nil {
		return err
	}
	m.Client = client
	if m.QueryAuthorsLimit == 0 {
		m.QueryAuthorsLimit = queryAuthorsLimit
	}
	if m.QueryLimit == 0 {
		m.QueryLimit = queryLimit
	}
	if m.QueryIDsLimit == 0 {
		m.QueryIDsLimit = queryIDsLimit
	}
	if m.QueryKindsLimit == 0 {
		m.QueryKindsLimit = queryKindsLimit
	}
	if m.QueryTagsLimit == 0 {
		m.QueryTagsLimit = queryTagsLimit
	}
	return nil
}
