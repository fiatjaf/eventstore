package main

import (
	"fmt"

	"github.com/mailru/easyjson"
	"github.com/nbd-wtf/go-nostr"
	"github.com/urfave/cli/v2"
)

var query = &cli.Command{
	Name:        "query",
	Usage:       "queries an eventstore for events",
	Description: ``,
	Action: func(c *cli.Context) error {
		for line := range getStdinLinesOrBlank() {
			filter := nostr.Filter{}
			if err := easyjson.Unmarshal([]byte(line), &filter); err != nil {
				lineProcessingError(c, "invalid filter '%s' received from stdin: %s", line, err)
				continue
			}

			ch, err := db.QueryEvents(c.Context, filter)
			if err != nil {
				lineProcessingError(c, "error querying: %w", err)
				continue
			}

			for evt := range ch {
				fmt.Println(evt)
			}
		}

		exitIfLineProcessingError(c)
		return nil
	},
}
