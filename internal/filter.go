package internal

import "github.com/nbd-wtf/go-nostr"

// SanitizeFilter reconciles a filter with NIP-01 (and nostr.Filter.Matches)
// before a backend translates it into its own query, which usually only knows
// "no list" and "one or more values":
//
//   - a non-nil but empty ids, authors, kinds or tag value list can never be
//     satisfied, so the filter matches nothing and the caller must return no
//     events instead of treating the list as absent;
//   - a nil tag value list (e.g. {"#e": null}) is no constraint at all, so it is
//     dropped.
func SanitizeFilter(filter nostr.Filter) (sanitized nostr.Filter, unsatisfiable bool) {
	if (filter.IDs != nil && len(filter.IDs) == 0) ||
		(filter.Authors != nil && len(filter.Authors) == 0) ||
		(filter.Kinds != nil && len(filter.Kinds) == 0) {
		return filter, true
	}

	hasNil := false
	for _, values := range filter.Tags {
		if values == nil {
			hasNil = true
		} else if len(values) == 0 {
			return filter, true
		}
	}
	if hasNil {
		tags := make(nostr.TagMap, len(filter.Tags))
		for key, values := range filter.Tags {
			if values != nil {
				tags[key] = values
			}
		}
		filter.Tags = tags
	}

	return filter, false
}

// SanitizeFilters applies SanitizeFilter to each filter and leaves out the
// unsatisfiable ones, which cannot add anything to a union of the filters.
func SanitizeFilters(filters nostr.Filters) nostr.Filters {
	sanitized := make(nostr.Filters, 0, len(filters))
	for _, filter := range filters {
		if filter, unsatisfiable := SanitizeFilter(filter); !unsatisfiable {
			sanitized = append(sanitized, filter)
		}
	}
	return sanitized
}

// NoEvents returns an already closed channel, for a query that matches nothing.
func NoEvents() chan *nostr.Event {
	ch := make(chan *nostr.Event)
	close(ch)
	return ch
}
