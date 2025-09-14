package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
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
	for dbname, db := range config.Databases {
		if db.Driver == "" {
			return fmt.Errorf("database %s: driver is not given/empty", dbname)
		}
		if db.Driver != "pgx" && db.Driver != "mysql" {
			return fmt.Errorf("database %s: driver %s not supported, only pgx and mysql are available", dbname, db.Driver)
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
	if test.Influxes3 == nil || len(test.Influxes3) == 0 {
		test.Influxes3 = ihf.Influxes3
	}
	if test.TargetDatabases == nil || len(test.TargetDatabases) == 0 {
		test.TargetDatabases = ihf.TargetDatabases
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
	/*
		   points = {influx_name: [] for influx_name in config.influxes}
		   points2 = {influx_name: [] for influx_name in config.influxes2}

			TODO: create points for influxv1 and influxv2, possiblty for influxv3 too
			see: https://github.com/influxdata/influxdb/tree/1.8/client#getting-started
			see: https://github.com/influxdata/influxdb-client-go?tab=readme-ov-file#basic-example
			see: https://github.com/InfluxCommunity/influxdb3-go
	*/
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
		//ctx, cancel := context.WithTimeout(context.Background(), test.Timeout)
		started := time.Now()
		fields, err := fetchTest(cf, dbname, test)
		//cancel()
		if err != nil {
			return err
		}
		elapsed := time.Since(started)
		fields["q_elapsed"] = elapsed.Seconds()
		tags := make(map[string]string)
		tags["database_name"] = dbname
		for name, tag := range test.Tags {
			tags[name] = tag
		}
		slog.Debug(fmt.Sprintf("Test %s on database %s: fields=%v+ tags=%v+", testName, dbname, fields, tags))
		/*
		   if test.influxes:
		       point = dict(measurement=test.measurement, tags=tags, fields=fields)
		       for influx_name in test.influxes:
		           if influx_name in points:
		               points[influx_name].append(point)
		   if test.influxes2:
		       point2 = influxdb_client.Point(test.measurement)
		       for name, value in tags.items():
		           point2.tag(name, value)
		       for name, value in fields.items():
		           point2.field(name, value)
		       for influx_name in test.influxes2:
		           if influx_name in points2:
		               points2[influx_name].append(point2)

		*/
	}
	return nil
}

func fetchTest(cf config.Config, dbname string, test config.Test) (map[string]interface{}, error) {
	db := cf.Databases[dbname]
	conn, err := sql.Open(db.Driver, db.DSN)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database %s: %w", dbname, err)
	}
	defer conn.Close()

	rows, err := conn.Query(test.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))

	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	if rows.Next() {
		err = rows.Scan(valuePtrs...)
		if err != nil {
			return nil, err
		}

		result := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]

			// Convert []byte to string for readability
			if b, ok := val.([]byte); ok {
				result[col] = string(b)
			} else {
				result[col] = val
			}
		}

		return result, nil
	}

	return nil, sql.ErrNoRows

}
