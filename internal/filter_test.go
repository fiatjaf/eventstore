package internal

import (
	"encoding/json"
	"testing"

	"github.com/nbd-wtf/go-nostr"
)

func TestSanitizeFilter(t *testing.T) {
	for _, tt := range []struct {
		json          string
		unsatisfiable bool
		tags          int
	}{
		{`{"kinds":[1]}`, false, 0},
		{`{"ids":[]}`, true, 0},
		{`{"authors":[]}`, true, 0},
		{`{"kinds":[]}`, true, 0},
		{`{"#e":[]}`, true, 0},
		{`{"#t":["x"],"#e":[]}`, true, 0},
		{`{"#t":["x"],"#e":null}`, false, 1},
		{`{"#e":null}`, false, 0},
	} {
		var filter nostr.Filter
		if err := json.Unmarshal([]byte(tt.json), &filter); err != nil {
			t.Fatalf("%s: %v", tt.json, err)
		}
		sanitized, unsatisfiable := SanitizeFilter(filter)
		if unsatisfiable != tt.unsatisfiable {
			t.Errorf("%s: unsatisfiable = %v, want %v", tt.json, unsatisfiable, tt.unsatisfiable)
		}
		if !unsatisfiable && len(sanitized.Tags) != tt.tags {
			t.Errorf("%s: %d tags left, want %d", tt.json, len(sanitized.Tags), tt.tags)
		}
	}

	filters := SanitizeFilters(nostr.Filters{{Kinds: []int{}}, {Kinds: []int{1}}, {IDs: []string{}}})
	if len(filters) != 1 || filters[0].Kinds[0] != 1 {
		t.Errorf("SanitizeFilters kept %v, want only the kinds:[1] filter", filters)
	}
}
