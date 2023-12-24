package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/fiatjaf/eventstore"
	"github.com/fiatjaf/eventstore/badger"
	"github.com/fiatjaf/eventstore/elasticsearch"
	"github.com/fiatjaf/eventstore/lmdb"
	"github.com/fiatjaf/eventstore/mysql"
	"github.com/fiatjaf/eventstore/postgresql"
	"github.com/fiatjaf/eventstore/sqlite3"
	"github.com/urfave/cli/v3"
)

var db eventstore.Store

var app = &cli.Command{
	Name:      "eventstore",
	Usage:     "a CLI for all the eventstore backends",
	UsageText: "eventstore -d ./data/sqlite <query|save|delete> ...",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "store",
			Aliases:  []string{"d"},
			Usage:    "path to the database file or directory or database connection uri",
			Required: true,
		},
		&cli.StringFlag{
			Name:    "type",
			Aliases: []string{"t"},
			Usage:   "store type ('sqlite', 'lmdb', 'badger', 'postgres', 'mysql', 'elasticsearch')",
		},
	},
	Before: func(ctx context.Context, c *cli.Command) error {
		path := c.String("store")
		typ := c.String("type")
		if typ != "" {
			// bypass automatic detection
			// this also works for creating disk databases from scratch
		} else {
			// try to detect based on url scheme
			switch {
			case strings.HasPrefix(path, "postgres://"), strings.HasPrefix(path, "postgresql://"):
				typ = "postgres"
			case strings.HasPrefix(path, "mysql://"):
				typ = "mysql"
			case strings.HasPrefix(path, "https://"):
				// if we ever add something else that uses URLs we'll have to modify this
				typ = "elasticsearch"
			default:
				// try to detect based on the form and names of disk files
				dbname, err := detect(path)
				if err != nil {
					if os.IsNotExist(err) {
						return fmt.Errorf(
							"'%s' does not exist, to create a store there specify the --type argument", path)
					}
					return fmt.Errorf("failed to detect store type: %w", err)
				}
				typ = dbname
			}
		}

		switch typ {
		case "sqlite":
			db = &sqlite3.SQLite3Backend{
				DatabaseURL:       path,
				QueryLimit:        1_000_000,
				QueryAuthorsLimit: 1_000_000,
				QueryKindsLimit:   1_000_000,
				QueryIDsLimit:     1_000_000,
				QueryTagsLimit:    1_000_000,
			}
		case "lmdb":
			db = &lmdb.LMDBBackend{Path: path, MaxLimit: 1_000_000}
		case "badger":
			db = &badger.BadgerBackend{Path: path, MaxLimit: 1_000_000}
		case "postgres", "postgresql":
			db = &postgresql.PostgresBackend{
				DatabaseURL:       path,
				QueryLimit:        1_000_000,
				QueryAuthorsLimit: 1_000_000,
				QueryKindsLimit:   1_000_000,
				QueryIDsLimit:     1_000_000,
				QueryTagsLimit:    1_000_000,
			}
		case "mysql":
			db = &mysql.MySQLBackend{
				DatabaseURL:       path,
				QueryLimit:        1_000_000,
				QueryAuthorsLimit: 1_000_000,
				QueryKindsLimit:   1_000_000,
				QueryIDsLimit:     1_000_000,
				QueryTagsLimit:    1_000_000,
			}
		case "elasticsearch":
			db = &elasticsearch.ElasticsearchStorage{URL: path}
		case "":
			return fmt.Errorf("couldn't determine store type, you can use --type to specify it manually")
		default:
			return fmt.Errorf("'%s' store type is not supported by this CLI", typ)
		}

		return db.Init()
	},
	Commands: []*cli.Command{
		queryOrSave,
		query,
		save,
		delete_,
	},
	DefaultCommand: "query-or-save",
}

func main() {
	if err := app.Run(context.Background(), os.Args); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
