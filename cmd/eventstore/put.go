package main

import (
	"context"
	"fmt"
	"os"

	"github.com/mailru/easyjson"
	"github.com/nbd-wtf/go-nostr"
	"github.com/urfave/cli/v3"
)

var put = &cli.Command{
	Name:        "put",
	Usage:       "saves an event to an eventstore",
	Description: ``,
	Action: func(ctx context.Context, c *cli.Command) error {
		hasError := false
		for line := range getStdinLinesOrFirstArgument(c) {
			var event nostr.Event
			if err := easyjson.Unmarshal([]byte(line), &event); err != nil {
				fmt.Fprintf(os.Stderr, "invalid event '%s' received from stdin: %s", line, err)
				hasError = true
				continue
			}

			if err := db.SaveEvent(ctx, &event); err != nil {
				fmt.Fprintf(os.Stderr, "failed to save event '%s': %s", line, err)
				hasError = true
				continue
			}

			fmt.Fprintf(os.Stderr, "saved %s", event.ID)
		}

		if hasError {
			os.Exit(123)
		}
		return nil
	},
}
