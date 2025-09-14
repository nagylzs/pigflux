package pigflux

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/nagylzs/pigflux/internal/config"
	"github.com/nagylzs/set"
)

type TestResult struct {
	Measurement string
	Fields      map[string]interface{}
	Tags        map[string]string
}

func RunTest(cf config.Config, testName string) error {
	test := cf.Tests[testName]
	results := make([]TestResult, 0)
	for _, dbname := range test.Databases {
		slog.Info(fmt.Sprintf("Running test %s on database %s", testName, dbname))
		//ctx, cancel := context.WithTimeout(context.Background(), test.Timeout)
		started := time.Now()
		fields, tags, err := fetchTest(cf, dbname, test)
		//cancel()
		if err != nil {
			return err
		}
		elapsed := time.Since(started)
		fields["q_elapsed"] = elapsed.Seconds()
		tags["database_name"] = dbname
		for name, tag := range test.Tags {
			tags[name] = tag
		}
		slog.Debug(fmt.Sprintf("Test %s on database %s: fields=%v+ tags=%v+", testName, dbname, fields, tags))
		results = append(results, TestResult{
			Measurement: test.Measurement,
			Fields:      fields,
			Tags:        tags,
		})
	}

	// TODO make these parallel
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	SendTestResultsV1(ctx, cf, testName, results)
	SendTestResultsV2(ctx, cf, testName, results)
	SendTestResultsV3(ctx, cf, testName, results)
	SendTestResultsDb(ctx, cf, testName, results)

	return nil
}

func fetchTest(cf config.Config, dbname string, test config.Test) (map[string]interface{}, map[string]string, error) {
	// TODO use QueryTimeout here!
	db := cf.Databases[dbname]
	conn, err := sql.Open(db.Driver, db.DSN)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to connect to database %s: %w", dbname, err)
	}
	defer func() {
		err := conn.Close()
		if err != nil {
			slog.Warn("could not close connection", "dbname", dbname, "error", err.Error())
		}
	}()

	rows, err := conn.Query(test.SQL)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		err := rows.Close()
		if err != nil {
			slog.Warn("could not close rows", "dbname", dbname, "error", err.Error())
		}
	}()

	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	for _, column := range columns {
		if !config.IsIdentifierLike(column) {
			return nil, nil, fmt.Errorf("invalid column: %s, only [a-zA-Z][a-zA-Z0-9]* is supported", column)
		}
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))

	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	if rows.Next() {
		err = rows.Scan(valuePtrs...)
		if err != nil {
			return nil, nil, err
		}

		fields := make(map[string]interface{})
		tags := make(map[string]string)
		fs := set.FromArray(test.Fields)
		got := set.NewSet[string]()
		for i, col := range columns {
			val := values[i]

			if fs.Contains(col) {
				// Convert []byte to string for readability
				if b, ok := val.([]byte); ok {
					fields[col] = string(b)
				} else {
					fields[col] = val
				}
				got.Add(col)
			} else {
				tags[col] = fmt.Sprintf("%v", val)
			}
		}
		missing := fs.Difference(got)
		if !missing.Empty() {
			return nil, nil, fmt.Errorf("missing fields: %v (specified in 'fields' but missing from result", missing)
		}

		return fields, tags, nil
	}

	return nil, nil, sql.ErrNoRows

}
