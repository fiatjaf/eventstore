package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/nbd-wtf/go-nostr"
	"github.com/urfave/cli/v2"
)

var queryOrPut = &cli.Command{
	Hidden: true,
	Name:   "query-or-put",
	Action: func(c *cli.Context) error {
		line := getStdin()

		ee := &nostr.EventEnvelope{}
		re := &nostr.ReqEnvelope{}
		e := &nostr.Event{}
		f := &nostr.Filter{}
		if json.Unmarshal([]byte(line), ee) == nil && ee.Event.ID != "" {
			e = &ee.Event
			return doPut(c, line, e)
		}
		if json.Unmarshal([]byte(line), e) == nil && e.ID != "" {
			return doPut(c, line, e)
		}
		if json.Unmarshal([]byte(line), re) == nil && len(re.Filters) > 0 {
			f = &re.Filters[0]
			return doQuery(c, f)
		}
		if json.Unmarshal([]byte(line), f) == nil && len(f.String()) > 2 {
			return doQuery(c, f)
		}

		return fmt.Errorf("couldn't parse input '%s'", line)
	},
}

func doPut(c *cli.Context, line string, e *nostr.Event) error {
	if err := db.SaveEvent(c.Context, e); err != nil {
		return fmt.Errorf("failed to save event '%s': %s", line, err)
	}
	fmt.Fprintf(os.Stderr, "saved %s", e.ID)
	return nil
}

func doQuery(c *cli.Context, f *nostr.Filter) error {
	ch, err := db.QueryEvents(c.Context, *f)
	if err != nil {
		return fmt.Errorf("error querying: %w", err)
	}

	for evt := range ch {
		fmt.Println(evt)
	}
	return nil
}
