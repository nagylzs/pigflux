package pigflux

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	influxdb2 "github.com/influxdata/influxdb-client-go"
	"github.com/influxdata/influxdb-client-go/api/write"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/nagylzs/pigflux/internal/config"
)

func SendTestResultsV1(ctx context.Context, cf config.Config, name string, results []TestResult) {
	test := cf.Tests[name]
	conns := ConnectInfluxes(cf, test.Influxes)
	defer CloseInfluxes(conns)
	for _, conn := range conns {
		// TODO do these parallel
		bp, err := client.NewBatchPoints(client.BatchPointsConfig{Database: conn.Cfg.Database})
		if err != nil {
			slog.Error("could not create batch points", "type", "influx", "name", conn.Name, "error", err)
			continue
		}
		for _, result := range results {
			pt, err := client.NewPoint(
				result.Measurement,
				result.Tags,
				result.Fields,
				time.Now(),
			)
			if err != nil {
				slog.Error("could not create point", "type", "influx", "name", conn.Name, "measurement", result.Measurement, "error", err)
			}
			bp.AddPoint(pt)
		}
		if err := conn.Client.Write(bp); err != nil {
			slog.Error("could not write batch points", "type", "influx", "name", conn.Name, "error", err)
		}
	}
}

func SendTestResultsV2(ctx context.Context, cf config.Config, name string, results []TestResult) {
	test := cf.Tests[name]
	conns := ConnectInfluxes2(cf, test.Influxes2)
	defer CloseInfluxes2(conns)
	for _, conn := range conns {
		points := make([]*write.Point, 0, len(results))
		for _, result := range results {
			p := influxdb2.NewPoint(result.Measurement,
				result.Tags,
				result.Fields,
				time.Now())
			points = append(points, p)
		}
		err := conn.WriteAPI.WritePoint(ctx, points...)
		if err != nil {
			slog.Error("could not write batch points", "type", "influx2", "name", conn.Name, "error", err)
		}
	}
}

func SendTestResultsV3(ctx context.Context, cf config.Config, name string, results []TestResult) {
	test := cf.Tests[name]
	conns := ConnectInfluxes3(cf, test.Influxes3)
	defer CloseInfluxes3(conns)
	for _, conn := range conns {
		points := make([]*influxdb3.Point, 0, len(results))
		for _, result := range results {
			p := influxdb3.NewPoint(
				result.Measurement,
				result.Tags,
				result.Fields,
				time.Now(),
			)
			points = append(points, p)
		}
		err := conn.Conn.WritePoints(ctx, points)
		if err != nil {
			slog.Error("could not write batch points", "type", "influx3", "name", conn.Name, "error", err)
		}
	}
}

func SendTestResultsDb(ctx context.Context, cf config.Config, name string, results []TestResult) {
	test := cf.Tests[name]
	conns := ConnectDatabases(cf, test.TargetDatabases)
	defer CloseDatabases(conns)
	for _, conn := range conns {
		for _, result := range results {
			sql, params, err := genInsertSQL(conn.Cfg.InsertSQL, result)
			if err != nil {
				slog.Error("could not generate insert sql", "type", "database", "name", conn.Name, "error", err)
				continue
			}
			_, err = conn.Conn.Exec(sql, params...)
			if err != nil {
				slog.Error("could execute insert sql", "type", "database", "name", conn.Name, "error", err)
			}
		}
	}
}

func genInsertSQL(sql string, result TestResult) (string, []interface{}, error) {
	tokens := SplitIntoTokens(sql)
	params := make([]interface{}, 0)
	sql = ""
	appendParam := func(value interface{}) {
		params = append(params, value)
		sql += fmt.Sprintf("$%d", len(params))
	}

	for _, t := range tokens {
		// "{MEASUREMENT}"("time",{FIELDNAMES},{TAGNAMES}) VALUES (now(), {FIELDVALUES} ,{TAGVALUES}
		if t == "{MEASUREMENT}" {
			appendParam(result.Measurement)
		} else if t == "{MEASUREMENT_NAME}" {
			sql += result.Measurement
		} else if t == "{FIELDNAMES}" {
			fnames := slices.Sorted(maps.Keys(result.Fields))
			sql += strings.Join(fnames, ",")
		} else if t == "{TAGNAMES}" {
			fnames := slices.Sorted(maps.Keys(result.Tags))
			sql += strings.Join(fnames, ",")
		} else if t == "{FIELDVALUES}" {
			fnames := slices.Sorted(maps.Keys(result.Fields))
			for i, field := range fnames {
				if i > 0 {
					sql += ","
				}
				appendParam(result.Fields[field])
			}
		} else if t == "{TAGVALUES}" {
			fnames := slices.Sorted(maps.Keys(result.Tags))
			for i, field := range fnames {
				if i > 0 {
					sql += ","
				}
				appendParam(result.Tags[field])
			}
		} else if t == "{FIELDS_JSON}" {
			js, err := json.Marshal(result.Fields)
			if err != nil {
				return "", nil, err
			}
			appendParam(string(js))
		} else if t == "{TAGS_JSON}" {
			js, err := json.Marshal(result.Tags)
			if err != nil {
				return "", nil, err
			}
			appendParam(string(js))
		} else if t == "{FIELDS_RAW}" {
			appendParam(result.Fields)
		} else if t == "{TAGS_RAW}" {
			appendParam(result.Tags)
		} else {
			sql += t
		}
	}
	return sql, params, nil
}

func SplitIntoTokens(input string) []string {
	// Regex to match {VARIABLE}
	re := regexp.MustCompile(`\{[^{}]+\}`)

	tokens := []string{}
	lastIndex := 0

	// Find all matches
	matches := re.FindAllStringIndex(input, -1)

	for _, match := range matches {
		start, end := match[0], match[1]

		// Add text before the match as a token
		if start > lastIndex {
			tokens = append(tokens, input[lastIndex:start])
		}

		// Add the matched variable as a token
		tokens = append(tokens, input[start:end])

		lastIndex = end
	}

	// Add any remaining text after the last match
	if lastIndex < len(input) {
		tokens = append(tokens, input[lastIndex:])
	}

	return tokens
}
