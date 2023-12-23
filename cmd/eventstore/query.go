package main

import (
	"context"
	"fmt"
	"os"

	"github.com/mailru/easyjson"
	"github.com/nbd-wtf/go-nostr"
	"github.com/urfave/cli/v3"
)

var query = &cli.Command{
	Name:        "query",
	Usage:       "queries an eventstore for events",
	Description: ``,
	Action: func(ctx context.Context, c *cli.Command) error {
		hasError := false
		for line := range getStdinLinesOrFirstArgument(c) {
			filter := nostr.Filter{}
			if err := easyjson.Unmarshal([]byte(line), &filter); err != nil {
				fmt.Fprintf(os.Stderr, "invalid filter '%s' received from stdin: %s", line, err)
				hasError = true
				continue
			}

			ch, err := db.QueryEvents(ctx, filter)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error querying: %s", err)
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
