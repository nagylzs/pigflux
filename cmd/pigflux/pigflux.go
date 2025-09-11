package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jessevdk/go-flags"
	"github.com/lmittmann/tint"
	"github.com/mattn/go-isatty"
	"github.com/nagylzs/pigflux/internal/config"
	"github.com/nagylzs/pigflux/internal/signal"
	"github.com/nagylzs/pigflux/internal/version"
)

func main() {
	var args = config.PigfluxCLIArgs{
		CLIArgs: config.CLIArgs{
			Debug:   false,
			Verbose: false,
		},
	}
	posArgs, err := flags.ParseArgs(&args, os.Args)
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}

	if args.ShowVersion {
		version.PrintVersion()
		os.Exit(0)
	}

	// Set loglevel
	var programLevel = new(slog.LevelVar)
	if args.Debug {
		programLevel.Set(slog.LevelDebug)
	} else if args.Verbose {
		programLevel.Set(slog.LevelInfo)
	} else {
		programLevel.Set(slog.LevelWarn)
	}

	lw := os.Stderr
	h := slog.New(
		tint.NewHandler(lw, &tint.Options{
			NoColor: !isatty.IsTerminal(lw.Fd()),
			Level:   programLevel,
		}),
	)
	slog.SetDefault(h)

	signal.SetupSignalHandler()

	go func() {
		err = runMain(args, posArgs)
		if err != nil {
			signal.Stop(1)
		}
	}()

	for !signal.IsStopping() {
		time.Sleep(time.Second)
	}

	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func runMain(args config.PigfluxCLIArgs, posArgs []string) error {
	cfg, err := config.LoadConfig(args.ConfigFile)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
	println(fmt.Sprintf("%v", cfg))

	for !signal.IsStopping() {
		time.Sleep(time.Second)
	}
	return nil
}
