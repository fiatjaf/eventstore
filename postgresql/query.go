package postgresql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/nbd-wtf/go-nostr"
)

func (b *PostgresBackend) QueryEvents(ctx context.Context, filter nostr.Filter) (ch chan *nostr.Event, err error) {
	query, params, err := b.queryEventsSql(filter, false)
	if err != nil {
		return nil, err
	}

	rows, err := b.DB.QueryContext(ctx, query, params...)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to fetch events using query %q: %w", query, err)
	}

	ch = make(chan *nostr.Event)
	go func() {
		defer rows.Close()
		defer close(ch)
		for rows.Next() {
			var evt nostr.Event
			var timestamp int64
			err := rows.Scan(&evt.ID, &evt.PubKey, &timestamp,
				&evt.Kind, &evt.Tags, &evt.Content, &evt.Sig)
			if err != nil {
				return
			}
			evt.CreatedAt = nostr.Timestamp(timestamp)
			select {
			case ch <- &evt:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}

func (b *PostgresBackend) CountEvents(ctx context.Context, filter nostr.Filter) (int64, error) {
	query, params, err := b.queryEventsSql(filter, true)
	if err != nil {
		return 0, err
	}

	var count int64
	if err = b.DB.QueryRowContext(ctx, query, params...).Scan(&count); err != nil && err != sql.ErrNoRows {
		return 0, fmt.Errorf("failed to fetch events using query %q: %w", query, err)
	}
	return count, nil
}

func makePlaceHolders(n int) string {
	return strings.TrimRight(strings.Repeat("?,", n), ",")
}

var (
	TooManyIDs       = errors.New("too many ids")
	TooManyAuthors   = errors.New("too many authors")
	TooManyKinds     = errors.New("too many kinds")
	TooManyTagValues = errors.New("too many tag values")
	EmptyTagSet      = errors.New("empty tag set")
)

func (b *PostgresBackend) queryEventsSql(filter nostr.Filter, doCount bool) (string, []any, error) {
	conditions := make([]string, 0, 7)
	params := make([]any, 0, 20)
	unsatisfiable := false

	if len(filter.IDs) > 0 {
		if len(filter.IDs) > b.QueryIDsLimit {
			// too many ids, fail everything
			return "", nil, TooManyIDs
		}

		for _, v := range filter.IDs {
			params = append(params, v)
		}
		conditions = append(conditions, ` id IN (`+makePlaceHolders(len(filter.IDs))+`)`)
	}

	if len(filter.Authors) > 0 {
		if len(filter.Authors) > b.QueryAuthorsLimit {
			// too many authors, fail everything
			return "", nil, TooManyAuthors
		}

		for _, v := range filter.Authors {
			params = append(params, v)
		}
		conditions = append(conditions, ` pubkey IN (`+makePlaceHolders(len(filter.Authors))+`)`)
	}

	if len(filter.Kinds) > 0 {
		if len(filter.Kinds) > b.QueryKindsLimit {
			// too many kinds, fail everything
			return "", nil, TooManyKinds
		}

		// kind is a 32-bit integer column, so a kind outside that range can
		// never match a stored row. Drop those rather than binding a value the
		// column cannot hold (which fails the whole query with "out of range
		// for type integer"); if none remain, nothing can match.
		kinds := make([]any, 0, len(filter.Kinds))
		for _, v := range filter.Kinds {
			if v >= math.MinInt32 && v <= math.MaxInt32 {
				kinds = append(kinds, v)
			}
		}
		if len(kinds) == 0 {
			unsatisfiable = true
		} else {
			params = append(params, kinds...)
			conditions = append(conditions, `kind IN (`+makePlaceHolders(len(kinds))+`)`)
		}
	}

	totalTags := 0
	for tagKey, values := range filter.Tags {
		if len(values) == 0 {
			// any tag set to [] is wrong
			return "", nil, EmptyTagSet
		}

		totalTags += len(values)
		if totalTags > b.QueryTagsLimit {
			// too many tags, fail everything
			return "", nil, TooManyTagValues
		}

		for _, tagValue := range values {
			params = append(params, strings.TrimPrefix(tagKey, "#")+":"+tagValue)
		}

		// each separate tag key is an independent condition
		conditions = append(conditions, `tagvalues && ARRAY[`+makePlaceHolders(len(values))+`]`)
	}

	// created_at is a 32-bit integer column with the same overflow: a since
	// above its max (or until below its min) can never match, while a since
	// below its min (or until above its max) constrains nothing and is dropped.
	if filter.Since != nil {
		switch since := int64(*filter.Since); {
		case since > math.MaxInt32:
			unsatisfiable = true
		case since >= math.MinInt32:
			conditions = append(conditions, `created_at >= ?`)
			params = append(params, filter.Since)
		}
	}
	if filter.Until != nil {
		switch until := int64(*filter.Until); {
		case until < math.MinInt32:
			unsatisfiable = true
		case until <= math.MaxInt32:
			conditions = append(conditions, `created_at <= ?`)
			params = append(params, filter.Until)
		}
	}
	if filter.Search != "" {
		config := b.FullTextSearchConfig
		if config == "" {
			config = "simple"
		}
		column := b.FullTextSearchColumn
		if column == "" {
			column = "content"
		}

		var contentExpr string
		if b.FullTextSearchMaxLength > 0 {
			contentExpr = fmt.Sprintf("LEFT(%s, %d)", column, b.FullTextSearchMaxLength)
		} else {
			contentExpr = column
		}

		conditions = append(conditions, `to_tsvector(?, `+contentExpr+`) @@ plainto_tsquery(?, ?)`)
		params = append(params, config, config, filter.Search)
	}

	if unsatisfiable {
		// a bound the column cannot satisfy: match nothing, but with a valid
		// query that returns no rows rather than a failed one. Any conditions
		// and params accumulated above are moot.
		conditions = []string{"false"}
		params = params[:0]
	}

	if len(conditions) == 0 {
		// fallback
		conditions = append(conditions, `true`)
	}

	if filter.Limit < 1 || filter.Limit > b.QueryLimit {
		params = append(params, b.QueryLimit)
	} else {
		params = append(params, filter.Limit)
	}

	var query string
	if doCount {
		query = sqlx.Rebind(sqlx.BindType("postgres"), `SELECT
          COUNT(*)
        FROM event WHERE `+
			strings.Join(conditions, " AND ")+
			" LIMIT ?")
	} else {
		query = sqlx.Rebind(sqlx.BindType("postgres"), `SELECT
          id, pubkey, created_at, kind, tags, content, sig
        FROM event WHERE `+
			strings.Join(conditions, " AND ")+
			" ORDER BY created_at DESC, id LIMIT ?")
	}

	return query, params, nil
}
