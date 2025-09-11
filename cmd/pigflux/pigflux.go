package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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
	} else if args.Silent {
		programLevel.Set(slog.LevelError)
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
	if args.ConfigFiles == nil || len(args.ConfigFiles) == 0 {
		args.ConfigFiles = make([]string, 0)
	}
	if args.ConfigDirs != nil || len(args.ConfigDirs) > 0 {
		for _, cd := range args.ConfigDirs {
			cfs, err := listConfigFiles(cd)
			if err != nil {
				return err
			}
			args.ConfigFiles = append(args.ConfigFiles, cfs...)
		}
	}
	if len(args.ConfigFiles) == 0 {
		return errors.New("no config files specified")
	}

	configs := make([]config.Config, len(args.ConfigFiles))
	for _, cf := range args.ConfigFiles {
		cfg, err := config.LoadConfig(cf)
		if err != nil {
			return fmt.Errorf("error loading config %s: %w", cf, err)
		}
		configs = append(configs, cfg)
	}

	println(fmt.Sprintf("%v", configs))

	for !signal.IsStopping() {
		time.Sleep(time.Second)
	}
	return nil
}

func listConfigFiles(cwd string) ([]string, error) {
	entries, err := os.ReadDir(cwd)
	if err != nil {
		return nil, fmt.Errorf("could not list config files in %s", cwd)
	}
	result := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".yml") && !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}
		fullPath, err := filepath.Abs(filepath.Join(cwd, entry.Name()))
		if err != nil {
			return nil, err
		}
		result = append(result, fullPath)
	}
	return result, nil
}
