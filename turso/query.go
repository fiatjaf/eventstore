package turso

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/nbd-wtf/go-nostr"
)

func (b *TursoBackend) QueryEvents(ctx context.Context, filter nostr.Filter) (ch chan *nostr.Event, err error) {
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

func (b *TursoBackend) CountEvents(ctx context.Context, filter nostr.Filter) (int64, error) {
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

// CountEventsFilters counts the events matching any of the filters, counting an
// event that matches more than one of them only once. NIP-45 OR's the filters
// together into a single count, so summing a per-filter count would report
// overlapping filters more than once. The union is evaluated by the database:
// pulling the ids back to deduplicate them here would cost one id per stored
// event.
func (b *TursoBackend) CountEventsFilters(ctx context.Context, filters nostr.Filters) (int64, error) {
	switch len(filters) {
	case 0:
		return 0, nil
	case 1:
		return b.CountEvents(ctx, filters[0])
	}

	selects := make([]string, 0, len(filters))
	params := make([]any, 0, len(filters)*20)
	for _, filter := range filters {
		conditions, filterParams, err := b.filterConditions(filter)
		if err != nil {
			return 0, err
		}
		selects = append(selects, "SELECT id FROM event WHERE "+strings.Join(conditions, " AND "))
		params = append(params, filterParams...)
	}

	// UNION rather than UNION ALL: the duplicates are exactly the events this
	// must not count twice. Rebind once, over the whole statement, so the
	// placeholders are numbered across all the filters.
	query := sqlx.Rebind(sqlx.QUESTION,
		"SELECT COUNT(*) FROM ("+strings.Join(selects, " UNION ")+") AS matched")

	var count int64
	if err := b.DB.QueryRowContext(ctx, query, params...).Scan(&count); err != nil && err != sql.ErrNoRows {
		return 0, fmt.Errorf("failed to count events using query %q: %w", query, err)
	}
	return count, nil
}

var (
	TooManyIDs       = errors.New("too many ids")
	TooManyAuthors   = errors.New("too many authors")
	TooManyKinds     = errors.New("too many kinds")
	TooManyTagValues = errors.New("too many tag values")
	EmptyTagSet      = errors.New("empty tag set")
)

func makePlaceHolders(n int) string {
	return strings.TrimRight(strings.Repeat("?,", n), ",")
}

// filterConditions renders a filter as WHERE conditions and their
// parameters, without a LIMIT, so that a caller can put the filter
// somewhere other than a standalone query -- a UNION, say.
func (b *TursoBackend) filterConditions(filter nostr.Filter) ([]string, []any, error) {
	conditions := make([]string, 0, 7)
	params := make([]any, 0, 20)

	if len(filter.IDs) > 0 {
		if len(filter.IDs) > 500 {
			// too many ids, fail everything
			return nil, nil, TooManyIDs
		}

		for _, v := range filter.IDs {
			params = append(params, v)
		}
		conditions = append(conditions, `id IN (`+makePlaceHolders(len(filter.IDs))+`)`)
	}

	if len(filter.Authors) > 0 {
		if len(filter.Authors) > b.QueryAuthorsLimit {
			// too many authors, fail everything
			return nil, nil, TooManyAuthors
		}

		for _, v := range filter.Authors {
			params = append(params, v)
		}
		conditions = append(conditions, `pubkey IN (`+makePlaceHolders(len(filter.Authors))+`)`)
	}

	if len(filter.Kinds) > 0 {
		if len(filter.Kinds) > b.QueryKindsLimit {
			// too many kinds, fail everything
			return nil, nil, TooManyKinds
		}

		for _, v := range filter.Kinds {
			params = append(params, v)
		}
		conditions = append(conditions, `kind IN (`+makePlaceHolders(len(filter.Kinds))+`)`)
	}

	// tags
	totalTags := 0
	// we use a very bad implementation in which we only check the tag values and ignore the tag names
	for _, values := range filter.Tags {
		if len(values) == 0 {
			// any tag set to [] is wrong
			return nil, nil, EmptyTagSet
		}

		orTag := make([]string, len(values))
		for i, tagValue := range values {
			orTag[i] = `tags LIKE ? ESCAPE '\'`
			params = append(params, `%`+strings.ReplaceAll(tagValue, `%`, `\%`)+`%`)
		}

		// each separate tag key is an independent condition
		conditions = append(conditions, "("+strings.Join(orTag, "OR ")+")")

		totalTags += len(values)
		if totalTags > b.QueryTagsLimit {
			// too many tags, fail everything
			return nil, nil, TooManyTagValues
		}
	}

	if filter.Since != nil {
		conditions = append(conditions, `created_at >= ?`)
		params = append(params, filter.Since)
	}
	if filter.Until != nil {
		conditions = append(conditions, `created_at <= ?`)
		params = append(params, filter.Until)
	}
	if filter.Search != "" {
		conditions = append(conditions, `content LIKE ? ESCAPE '\'`)
		params = append(params, `%`+strings.ReplaceAll(filter.Search, `%`, `\%`)+`%`)
	}

	if len(conditions) == 0 {
		// fallback
		conditions = append(conditions, `true`)
	}

	return conditions, params, nil
}

func (b *TursoBackend) queryEventsSql(filter nostr.Filter, doCount bool) (string, []any, error) {
	conditions, params, err := b.filterConditions(filter)
	if err != nil {
		return "", nil, err
	}

	if filter.Limit < 1 || filter.Limit > b.QueryLimit {
		params = append(params, b.QueryLimit)
	} else {
		params = append(params, filter.Limit)
	}

	var query string
	if doCount {
		query = sqlx.Rebind(sqlx.QUESTION, `SELECT
          COUNT(*)
        FROM event WHERE `+
			strings.Join(conditions, " AND ")+
			" LIMIT ?")
	} else {
		query = sqlx.Rebind(sqlx.QUESTION, `SELECT
          id, pubkey, created_at, kind, tags, content, sig
        FROM event WHERE `+
			strings.Join(conditions, " AND ")+
			" ORDER BY created_at DESC, id LIMIT ?")
	}

	return query, params, nil
}
