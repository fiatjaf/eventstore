package main

import (
	"fmt"
	"os"

	"github.com/mailru/easyjson"
	"github.com/nbd-wtf/go-nostr"
	"github.com/urfave/cli/v2"
)

var put = &cli.Command{
	Name:        "put",
	Usage:       "saves an event to an eventstore",
	Description: ``,
	Action: func(c *cli.Context) error {
		for line := range getStdinLinesOrBlank() {
			var event nostr.Event
			if err := easyjson.Unmarshal([]byte(line), &event); err != nil {
				lineProcessingError(c, "invalid event '%s' received from stdin: %s", line, err)
				continue
			}

			if err := db.SaveEvent(c.Context, &event); err != nil {
				lineProcessingError(c, "failed to save event '%s': %s", line, err)
				continue
			}

			fmt.Fprintf(os.Stderr, "saved %s", event.ID)
		}

		exitIfLineProcessingError(c)
		return nil
	},
}
