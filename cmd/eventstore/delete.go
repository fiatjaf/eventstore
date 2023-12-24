package main

import (
	"context"
	"fmt"
	"os"

	"github.com/nbd-wtf/go-nostr"
	"github.com/urfave/cli/v3"
)

var delete_ = &cli.Command{
	Name:        "delete",
	Usage:       "deletes an event and all its associated index entries",
	Description: ``,
	Action: func(ctx context.Context, c *cli.Command) error {
		hasError := false
		for line := range getStdinLinesOrFirstArgument(c) {
			f := nostr.Filter{IDs: []string{line}}
			ch, err := db.QueryEvents(ctx, f)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error querying for %s: %s", f, err)
				hasError = true
			}
			for evt := range ch {
				if err := db.DeleteEvent(ctx, evt); err != nil {
					fmt.Fprintf(os.Stderr, "error deleting")
					hasError = true
				}
			}
		}

		if hasError {
			os.Exit(123)
		}
		return nil
	},
}
