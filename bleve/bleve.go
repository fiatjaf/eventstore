package bleve

import (
	"fmt"

	"github.com/blevesearch/bleve"
	"github.com/fiatjaf/eventstore"
)

const (
	fieldPubkey    = "PubKey"
	fieldCreatedAt = "CreatedAt"
	fieldKind      = "Kind"
	fieldContent   = "Content"
)

var _ eventstore.Store = (*BleveBackend)(nil)

type BleveBackend struct {
	Path          string
	RawEventStore eventstore.Store
	index         bleve.Index
}

func (b *BleveBackend) Close() {
	b.index.Close()
}
func (b *BleveBackend) Init() error {
	if b.Path == "" {
		return fmt.Errorf("missing Path")
	}
	if b.RawEventStore == nil {
		return fmt.Errorf("missing RawEventStore")
	}

	m := bleve.NewIndexMapping()
	m.DefaultMapping = bleve.NewDocumentStaticMapping()
	m.DefaultMapping.AddFieldMappingsAt(fieldPubkey, bleve.NewTextFieldMapping())
	m.DefaultMapping.AddFieldMappingsAt(fieldCreatedAt, bleve.NewNumericFieldMapping())
	m.DefaultMapping.AddFieldMappingsAt(fieldKind, bleve.NewTextFieldMapping())
	m.DefaultMapping.AddFieldMappingsAt(fieldContent, bleve.NewTextFieldMapping())

	index, err := bleve.New(b.Path, m)
	if err != nil {
		if err != bleve.ErrorIndexPathExists {
			return err
		}
		index, err = bleve.Open(b.Path)
		if err != nil {
			return err
		}

	}
	b.index = index
	return nil
}

type IndexedEvent struct {
	PubKey    string
	CreatedAt float64
	Kind      string
	Content   string
}
