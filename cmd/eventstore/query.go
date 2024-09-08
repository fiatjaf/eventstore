package main

import (
	"context"
	"fmt"
	"os"

	"github.com/fiatjaf/cli/v3"
	"github.com/mailru/easyjson"
	"github.com/nbd-wtf/go-nostr"
)

var query = &cli.Command{
	Name:        "query",
	Usage:       "queries an eventstore for events, takes a Nostr filter as argument",
	Description: "unless specified to be smaller, up to a million results will be returned",
	UsageText:   "eventstore query <nostr-filter>",
	Action: func(ctx context.Context, c *cli.Command) error {
		hasError := false
		for line := range getStdinLinesOrFirstArgument(c) {
			filter := nostr.Filter{}
			if err := easyjson.Unmarshal([]byte(line), &filter); err != nil {
				fmt.Fprintf(os.Stderr, "invalid filter '%s': %s\n", line, err)
				hasError = true
				continue
			}

			ch, err := db.QueryEvents(ctx, filter)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error querying: %s\n", err)
				hasError = true
				continue
			}

			for evt := range ch {
				fmt.Println(evt)
			}
		}

		if hasError {
			os.Exit(123)
		}
		return nil
	},
}
