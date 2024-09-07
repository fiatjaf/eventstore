package eventstore

import (
	"strings"

	"github.com/nbd-wtf/go-nostr"
)

func isOlder(previous, next *nostr.Event) bool {
	return previous.CreatedAt < next.CreatedAt ||
		(previous.CreatedAt == next.CreatedAt && previous.ID > next.ID)
}

func MakePlaceHolders(n int) string {
	return strings.TrimRight(strings.Repeat("?,", n), ",")
}
