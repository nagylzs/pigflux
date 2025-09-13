package main

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/jessevdk/go-flags"
	"github.com/lmittmann/tint"
	"github.com/mattn/go-isatty"
	"github.com/nagylzs/pigflux/internal/config"
	"github.com/nagylzs/pigflux/internal/signal"
	"github.com/nagylzs/pigflux/internal/version"
	"github.com/nagylzs/set"
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

	cnt := 0
	if args.Debug {
		cnt++
	}
	if args.Verbose {
		cnt++
	}
	if args.Silent {
		cnt++
	}
	if cnt > 1 {
		println("Only one of --verbose, --debug, or --silent can be specified")
		os.Exit(1)
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
	wait, err := time.ParseDuration(args.Wait)
	if err != nil {

		return fmt.Errorf("cannot parse wait time: %v", err.Error())
	}

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

	err = parseConfigs(&configs)
	if err != nil {
		return err
	}

	// println(fmt.Sprintf("%v", configs))
	index := 0
	for args.Count < 0 || index < args.Count {
		if signal.IsStopping() {
			break
		}
		if args.Count != 1 {
			slog.Info(fmt.Sprintf("Pass %d started", index+1))
		}
		started := time.Now()
		for _, cf := range configs {
			err := runConfig(cf)
			if err != nil {
				slog.Error(err.Error())
			}
		}
		if signal.IsStopping() {
			break
		}
		elapsed := time.Since(started)
		index += 1
		isLast := (args.Count > 0) && (index == args.Count)
		if !isLast {
			remaining := wait - elapsed
			if remaining <= 0 {
				slog.Info(fmt.Sprintf("Pass %d elapsed %v", index, elapsed))
				continue
			}
			slog.Info(fmt.Sprintf("Pass %d elapsed %v waiting %s for next", index, elapsed, remaining))
			time.Sleep(remaining)
		}
	}

	signal.Stop(0)
	return nil
}

func parseConfigs(configs *[]config.Config) error {
	for i := 0; i < len(*configs); i++ {
		err := parseConfig(&(*configs)[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func parseConfig(config *config.Config) error {
	// Test for circular references, inherit properites
	used := set.NewSet[string]()
	for name := range config.Tests {
		err := inheritProps(config, name, used)
		if err != nil {
			return err
		}
	}
	for name := range config.Tests {
		err := config.Tests[name].Check(config)
		if err != nil {
			return fmt.Errorf("test '%s': %w", name, err)
		}
	}
	return nil
}

func inheritProps(config *config.Config, name string, used *set.Set[string]) error {
	test := config.Tests[name]
	ref := test.InheritFrom
	if ref == "" {
		return nil
	}
	if used.Contains(ref) {
		return fmt.Errorf("circular reference tests.%s.inherit_from=%s (used=%v)", name, ref, used)
	}
	ihf, ok := config.Tests[ref]
	if !ok {
		return fmt.Errorf("invalid reference tests.%s.inherit_from=%s", name, ref)
	}
	used.Add(ref)
	err := inheritProps(config, ref, used)
	if err != nil {
		return err
	}
	used.Remove(ref)

	if test.Databases == nil || len(test.Databases) == 0 {
		test.Databases = ihf.Databases
	}
	if test.Influxes == nil || len(test.Influxes) == 0 {
		test.Influxes = ihf.Influxes
	}
	if test.Influxes2 == nil || len(test.Influxes2) == 0 {
		test.Influxes2 = ihf.Influxes2
	}
	if test.Tags == nil || len(test.Tags) == 0 {
		test.Tags = ihf.Tags
	}
	if test.Fields == nil || len(test.Fields) == 0 {
		test.Fields = ihf.Fields
	}
	if test.Measurement == "" {
		test.Measurement = ihf.Measurement
	}
	if test.SQL == "" {
		test.SQL = ihf.SQL
	}
	if test.Order == 0 {
		test.Order = ihf.Order
	}
	config.Tests[name] = test
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

func runConfig(cf config.Config) error {
	// map order values to config names
	ord := make(map[int][]string)
	for name, test := range cf.Tests {
		if test.IsTemplate {
			continue
		}
		_, ok := ord[test.Order]
		if !ok {
			ord[test.Order] = make([]string, 0)
		}
		ord[test.Order] = append(ord[test.Order], name)
	}
	order := slices.Sorted(maps.Keys(ord))
	for _, o := range order {
		for _, name := range ord[o] {
			err := runTest(cf, name)
			if err != nil {
				slog.Error(fmt.Sprintf("Error running test %s: %v", name, err))
			}
		}
	}
	return nil
}

func runTest(cf config.Config, testName string) error {
	test := cf.Tests[testName]
	for _, dbname := range test.Databases {
		slog.Info(fmt.Sprintf("Running test %s on database %s", testName, dbname))
		/*
		   cur = conn.cursor()
		   q_started = time.time()
		   cur.execute(test.sql)
		   row = cur.fetchone()
		   q_elapsed = time.time() - q_started
		   column_map = {desc[0]: idx for idx, desc in enumerate(cur.description)}
		   fields = {field: row[column_map[field]] for field in test.fields}
		   fields["q_elapsed"] = q_elapsed
		   tags = {"database_name": database_name}
		   tags.update(test.tags)
		*/
	}
	return nil
}
