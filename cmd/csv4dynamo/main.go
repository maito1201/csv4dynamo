package main

import (
	"github.com/maito1201/csv4dynamo/csv2dynamo"
	"github.com/maito1201/csv4dynamo/dynamo2csv"
	"github.com/urfave/cli/v2"
	"log"
	"os"
)

func main() {
	app := &cli.App{
		Name:  "csv4dynamo",
		Usage: "export and import csv for DynamoDB",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "table-name",
				Aliases:  []string{"t"},
				Usage:    "[import export] target dynamo db tabe name (required)",
				Required: true,
			},
			&cli.StringFlag{
				Name:    "endpoint",
				Aliases: []string{"e"},
				Usage:   "[import export] endpoint of DynamoDB",
			},
			&cli.StringFlag{
				Name:    "profile",
				Aliases: []string{"p"},
				Usage:   "[import export] profile of aws cli",
			},
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"out", "o"},
				Usage:   "[import export] target output (default: stdout, e.g. ./out.txt), no file will be created if execute option is enabled",
			},
			&cli.StringFlag{
				Name:    "csv",
				Aliases: []string{"c", "file", "f"},
				Usage:   "[import] file to import (e.g. ./tablename.csv)",
			},
			&cli.BoolFlag{
				Name:  "execute",
				Usage: "[import] is directly execute import command",
			},
			&cli.StringFlag{
				Name:    "filter-expression",
				Aliases: []string{"fex"},
				Usage:   "[export] filter-expression to export (e.g. 'contains(#ts, :s)')",
			},
			&cli.StringFlag{
				Name:    "expression-attribute-values",
				Aliases: []string{"exp-values", "xav"},
				Usage:   `[export] expression-attribute-values to export (e.g. '{":s":{"S":"15:00:00Z"}}')`,
			},
			&cli.StringFlag{
				Name:    "expression-attribute-names",
				Aliases: []string{"exp-names", "xan"},
				Usage:   `[export] expression-attribute-names to export (e.g. '{"#ts":"timestamp"}')`,
			},
		},
		Commands: []*cli.Command{
			{
				Name:    "import",
				Aliases: []string{"csv2dynamo", "i"},
				Usage:   "import csv to DynamoDB",
				Action: func(c *cli.Context) error {
					return csv2dynamo.Execute(c)
				},
			},
			{
				Name:    "export",
				Aliases: []string{"dynamo2csv", "x"},
				Usage:   "export csv from DynamoDB",
				Action: func(c *cli.Context) error {
					return dynamo2csv.Export(c)
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
