package firestore

import (
	"context"
	"errors"

	"cloud.google.com/go/firestore"
)

const (
	defaultCollection = "events"
	queryLimit        = 500
	pageSize          = 100
	// inLimit is Firestore's maximum number of values allowed in a single
	// "in" / "array-contains-any" disjunction.
	inLimit = 30
)

func (b *FirestoreBackend) Init() error {
	if b.ProjectID == "" {
		return errors.New("firestore: ProjectID is required")
	}
	if b.Collection == "" {
		b.Collection = defaultCollection
	}
	if b.QueryLimit == 0 {
		b.QueryLimit = queryLimit
	}
	if b.PageSize == 0 {
		b.PageSize = pageSize
	}

	client, err := firestore.NewClient(context.Background(), b.ProjectID)
	if err != nil {
		return err
	}
	b.Client = client
	return nil
}
