package main

import (
	"os"

	"github.com/itsubaki/hermes-lambda/cmd/cost"
	"github.com/itsubaki/hermes-lambda/cmd/reservation"
	"github.com/urfave/cli/v2"
)

func New() *cli.App {
	app := cli.NewApp()
	app.Name = "hermes-lambda"

	// flags
	project := cli.StringFlag{
		Name:    "project",
		Aliases: []string{"pid"},
		EnvVars: []string{"PROJECT_ID"},
	}

	dataset := cli.StringFlag{
		Name:    "dataset",
		Aliases: []string{"dsn"},
		EnvVars: []string{"DATASET_NAME"},
	}

	dsloc := cli.StringFlag{
		Name:    "location",
		Aliases: []string{"loc"},
		EnvVars: []string{"DATASET_LOCATION"},
		Value:   "asia-northeast1",
	}

	period := cli.StringFlag{
		Name:    "period",
		Aliases: []string{"p"},
		EnvVars: []string{"PERIOD"},
		Value:   "1d",
	}

	// subcommands
	costcmd := cli.Command{
		Name:    "cost",
		Aliases: []string{"c"},
		Subcommands: []*cli.Command{
			{
				Name:    "fetch",
				Aliases: []string{"f"},
				Action:  cost.Fetch,
				Flags: []cli.Flag{
					&project,
					&dataset,
					&dsloc,
					&period,
				},
			},
		},
	}

	rsvcmd := cli.Command{
		Name:    "reservation",
		Aliases: []string{"u"},
		Subcommands: []*cli.Command{
			{
				Name:    "fetch",
				Aliases: []string{"f"},
				Action:  reservation.Fetch,
				Flags: []cli.Flag{
					&project,
					&dataset,
					&dsloc,
					&period,
				},
			},
		},
	}

	app.Commands = []*cli.Command{
		&costcmd,
		&rsvcmd,
	}

	return app
}

func main() {
	if err := New().Run(os.Args); err != nil {
		panic(err)
	}
}
