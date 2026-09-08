package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/nbd-wtf/go-nostr"
)

func (b *MySQLBackend) QueryEvents(ctx context.Context, filter nostr.Filter) (ch chan *nostr.Event, err error) {
	ch = make(chan *nostr.Event)

	query, params, err := b.queryEventsSql(filter, false)
	if err != nil {
		close(ch)
		return nil, err
	}

	rows, err := b.DB.QueryContext(ctx, query, params...)
	if err != nil && err != sql.ErrNoRows {
		close(ch)
		return nil, fmt.Errorf("failed to fetch events using query %q: %w", query, err)
	}

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

func (b *MySQLBackend) CountEvents(ctx context.Context, filter nostr.Filter) (int64, error) {
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
func (b *MySQLBackend) CountEventsFilters(ctx context.Context, filters nostr.Filters) (int64, error) {
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
	query := sqlx.Rebind(sqlx.BindType("mysql"),
		"SELECT COUNT(*) FROM ("+strings.Join(selects, " UNION ")+") AS matched")

	var count int64
	if err := b.DB.QueryRowContext(ctx, query, params...).Scan(&count); err != nil && err != sql.ErrNoRows {
		return 0, fmt.Errorf("failed to count events using query %q: %w", query, err)
	}
	return count, nil
}

func makePlaceHolders(n int) string {
	return strings.TrimRight(strings.Repeat("?,", n), ",")
}

func escapeLikeString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	s = strings.ReplaceAll(s, `%`, `\%`)
	return s
}

// filterConditions renders a filter as WHERE conditions and their
// parameters, without a LIMIT, so that a caller can put the filter
// somewhere other than a standalone query -- a UNION, say.
func (b *MySQLBackend) filterConditions(filter nostr.Filter) ([]string, []any, error) {
	conditions := make([]string, 0, 7)
	params := make([]any, 0, 20)
	unsatisfiable := false

	if len(filter.IDs) > 0 {
		if len(filter.IDs) > b.QueryIDsLimit {
			// too many ids, fail everything
			return nil, nil, nil
		}

		for _, v := range filter.IDs {
			params = append(params, v)
		}
		conditions = append(conditions, ` id IN (`+makePlaceHolders(len(filter.IDs))+`)`)
	}

	if len(filter.Authors) > 0 {
		if len(filter.Authors) > b.QueryAuthorsLimit {
			// too many authors, fail everything
			return nil, nil, nil
		}

		for _, v := range filter.Authors {
			params = append(params, v)
		}
		conditions = append(conditions, ` pubkey IN (`+makePlaceHolders(len(filter.Authors))+`)`)
	}

	if len(filter.Kinds) > 0 {
		if len(filter.Kinds) > b.QueryKindsLimit {
			// too many kinds, fail everything
			return nil, nil, nil
		}

		// kind is a 32-bit integer column, so a kind outside that range can
		// never match a stored row. Drop those rather than binding a value the
		// column cannot hold (which fails the whole query with "out of range");
		// if none remain, nothing can match.
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
	// we use a very bad implementation in which we only check the tag values and ignore the tag names
	for key, values := range filter.Tags {
		if len(values) == 0 {
			// any tag set to [] is wrong
			return nil, nil, nil
		}

		orTag := make([]string, 0, len(values))
		for _, tagValue := range values {
			orTag = append(orTag, `tags LIKE ?`)
			params = append(params, `%["`+escapeLikeString(key)+`","`+escapeLikeString(tagValue)+`"%`)
		}

		// each separate tag key is an independent condition
		conditions = append(conditions, `(`+strings.Join(orTag, " OR ")+`)`)

		totalTags += len(values)
		if totalTags > b.QueryTagsLimit {
			// too many tags, fail everything
			return nil, nil, nil
		}
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
		conditions = append(conditions, `content LIKE ?`)
		params = append(params, `%`+escapeLikeString(filter.Search)+`%`)
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

	return conditions, params, nil
}

func (b *MySQLBackend) queryEventsSql(filter nostr.Filter, doCount bool) (string, []any, error) {
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
		query = sqlx.Rebind(sqlx.BindType("mysql"), `SELECT
          COUNT(*)
        FROM event WHERE `+
			strings.Join(conditions, " AND ")+
			" LIMIT ?")
	} else {
		query = sqlx.Rebind(sqlx.BindType("mysql"), `SELECT
          id, pubkey, created_at, kind, tags, content, sig
        FROM event WHERE `+
			strings.Join(conditions, " AND ")+
			" ORDER BY created_at DESC, id LIMIT ?")
	}

	return query, params, nil
}
