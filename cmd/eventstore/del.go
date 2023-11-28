package main

import (
	"fmt"

	"github.com/urfave/cli/v2"
)

var del = &cli.Command{
	Name:        "del",
	Usage:       "deletes an event",
	Description: ``,
	Action: func(c *cli.Context) error {
		for line := range getStdinLinesOrBlank() {
			fmt.Println(line)
		}

		exitIfLineProcessingError(c)
		return nil
	},
}
