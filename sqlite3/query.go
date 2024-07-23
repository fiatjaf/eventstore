package sqlite3

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/nbd-wtf/go-nostr"
)

func (b SQLite3Backend) QueryEvents(ctx context.Context, filter nostr.Filter) (ch chan *nostr.Event, err error) {
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

func (b SQLite3Backend) CountEvents(ctx context.Context, filter nostr.Filter) (int64, error) {
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

func (b SQLite3Backend) queryEventsSql(filter nostr.Filter, doCount bool) (string, []any, error) {
	var conditions []string
	var params []any

	if len(filter.IDs) > 0 {
		if len(filter.IDs) > 500 {
			// too many ids, fail everything
			return "", nil, nil
		}

		for _, v := range filter.IDs {
			params = append(params, v)
		}
		conditions = append(conditions, `id IN (`+makePlaceHolders(len(filter.IDs))+`)`)
	}

	if len(filter.Authors) > 0 {
		if len(filter.Authors) > b.QueryAuthorsLimit {
			// too many authors, fail everything
			return "", nil, nil
		}

		for _, v := range filter.Authors {
			params = append(params, v)
		}
		conditions = append(conditions, `pubkey IN (`+makePlaceHolders(len(filter.Authors))+`)`)
	}

	if len(filter.Kinds) > 0 {
		if len(filter.Kinds) > 10 {
			// too many kinds, fail everything
			return "", nil, nil
		}

		for _, v := range filter.Kinds {
			params = append(params, v)
		}
		conditions = append(conditions, `kind IN (`+makePlaceHolders(len(filter.Kinds))+`)`)
	}

	tagQuery := make([]string, 0, 1)
	for _, values := range filter.Tags {
		if len(values) == 0 {
			// any tag set to [] is wrong
			return "", nil, nil
		}

		// add these tags to the query
		tagQuery = append(tagQuery, values...)

		if len(tagQuery) > 10 {
			// too many tags, fail everything
			return "", nil, nil
		}
	}

	// we use a very bad implementation in which we only check the tag values and
	// ignore the tag names
	if len(tagQuery) > 0 {
		orTag := make([]string, len(tagQuery))
		for i, tagValue := range tagQuery {
			orTag[i] = `tags LIKE ? ESCAPE '\'`
			params = append(params, `%`+strings.ReplaceAll(tagValue, `%`, `\%`)+`%`)
		}
		conditions = append(conditions, "("+strings.Join(orTag, "OR ")+")")
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

	if filter.Limit < 1 || filter.Limit > b.QueryLimit {
		params = append(params, b.QueryLimit)
	} else {
		params = append(params, filter.Limit)
	}

	var query string
	if doCount {
		query = sqlx.Rebind(sqlx.BindType("sqlite3"), `SELECT
          COUNT(*)
        FROM event WHERE `+
			strings.Join(conditions, " AND ")+
			" LIMIT ?")
	} else {
		query = sqlx.Rebind(sqlx.BindType("sqlite3"), `SELECT
          id, pubkey, created_at, kind, tags, content, sig
        FROM event WHERE `+
			strings.Join(conditions, " AND ")+
			" ORDER BY created_at DESC LIMIT ?")
	}

	return query, params, nil
}
