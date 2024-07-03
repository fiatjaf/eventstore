package bleve

import (
	"context"
	"strconv"

	"github.com/blevesearch/bleve"
	"github.com/nbd-wtf/go-nostr"
)

func (b *BleveBackend) QueryEvents(ctx context.Context, filter nostr.Filter) (chan *nostr.Event, error) {
	ch := make(chan *nostr.Event)

	if len(filter.Search) < 2 {
		close(ch)
		return ch, nil
	}

	searchQ := bleve.NewMatchQuery(filter.Search)
	searchQ.SetField(fieldContent)

	complicatedQuery := bleve.NewBooleanQuery()
	complicatedQuery.AddMust(searchQ)

	if len(filter.Kinds) > 0 {
		eitherKind := bleve.NewBooleanQuery()
		for _, kind := range filter.Kinds {
			kindQ := bleve.NewTermQuery(strconv.Itoa(kind))
			kindQ.SetField(fieldKind)
			eitherKind.AddShould(kindQ)
		}
		eitherKind.SetMinShould(1)
		complicatedQuery.AddMust(eitherKind)
	}

	if len(filter.Authors) > 0 {
		eitherPubkey := bleve.NewBooleanQuery()
		for _, pubkey := range filter.Authors {
			if len(pubkey) != 64 {
				continue
			}
			pubkeyQ := bleve.NewTermQuery(pubkey[56:])
			pubkeyQ.SetField(fieldPubkey)
			eitherPubkey.AddShould(pubkeyQ)
		}
		eitherPubkey.SetMinShould(1)
		complicatedQuery.AddMust(eitherPubkey)
	}

	if filter.Since != nil || filter.Until != nil {
		min := 0.0
		if filter.Since != nil {
			min = float64(*filter.Since)
		}
		max := float64(nostr.Now())
		if filter.Until != nil {
			max = float64(*filter.Until)
		}
		trueRef := true
		dateRangeQ := bleve.NewNumericRangeInclusiveQuery(&min, &max, &trueRef, &trueRef)
		dateRangeQ.SetField(fieldCreatedAt)
		complicatedQuery.AddMust(dateRangeQ)
	}

	limit := 40
	if filter.Limit != 0 {
		limit = filter.Limit
		if filter.Limit > 150 {
			limit = 150
		}
	}

	req := bleve.NewSearchRequestOptions(complicatedQuery, limit, 0, false)

	go func() {
		defer close(ch)

		res, err := b.index.SearchInContext(ctx, req)
		if err != nil {
			return
		}

		for _, hit := range res.Hits {
			rawch, err := b.RawEventStore.QueryEvents(ctx, nostr.Filter{IDs: []string{hit.ID}})
			if err != nil {
				return
			}
			for evt := range rawch {
				ch <- evt
			}
		}
	}()

	return ch, nil
}
