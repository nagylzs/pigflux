package pigflux

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
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
	testResults := make([]TestResult, 0)
	for _, dbname := range test.Databases {
		slog.Info(fmt.Sprintf("Running test %s on database %s", testName, dbname))
		//ctx, cancel := context.WithTimeout(context.Background(), test.Timeout)
		started := time.Now()
		fetchResults, err := fetchTest(cf, dbname, test)
		//cancel()
		if err != nil {
			return err
		}
		elapsed := time.Since(started)
		slog.Debug(fmt.Sprintf("Test %s on database %s returned %d data point(s)", testName, dbname, len(fetchResults)))
		for idx, fr := range fetchResults {
			fr.Fields["q_elapsed"] = elapsed.Seconds()
			fr.Tags["database_name"] = dbname
			for name, tag := range test.Tags {
				fr.Tags[name] = tag
			}
			slog.Debug(fmt.Sprintf("Test %s point #%d on database %s: fields=%v+ tags=%v+",
				testName, idx, dbname, fr.Fields, fr.Tags))
			testResults = append(testResults, TestResult{
				Measurement: test.Measurement,
				Fields:      fr.Fields,
				Tags:        fr.Tags,
			})
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	wg := &sync.WaitGroup{}
	wg.Add(4)
	go SendTestResultsV1(ctx, cf, testName, testResults, wg)
	go SendTestResultsV2(ctx, cf, testName, testResults, wg)
	go SendTestResultsV3(ctx, cf, testName, testResults, wg)
	go SendTestResultsDb(ctx, cf, testName, testResults, wg)
	wg.Wait()

	return nil
}

type FetchResult struct {
	Fields map[string]interface{}
	Tags   map[string]string
}

func fetchTest(cf config.Config, dbname string, test config.Test) ([]FetchResult, error) {
	// TODO use QueryTimeout here!
	db := cf.Databases[dbname]
	conn, err := sql.Open(db.Driver, db.DSN)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database %s: %w", dbname, err)
	}
	defer func() {
		err := conn.Close()
		if err != nil {
			slog.Warn("could not close connection", "dbname", dbname, "error", err.Error())
		}
	}()

	rows, err := conn.Query(test.SQL)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := rows.Close()
		if err != nil {
			slog.Warn("could not close rows", "dbname", dbname, "error", err.Error())
		}
	}()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	for _, column := range columns {
		if !config.IsIdentifierLike(column) {
			return nil, fmt.Errorf("invalid column: %s, only [a-zA-Z][a-zA-Z0-9]* is supported", column)
		}
	}

	result := make([]FetchResult, 0)
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		err = rows.Scan(valuePtrs...)
		if err != nil {
			return nil, err
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
			return nil, fmt.Errorf("missing fields: %v (specified in 'fields' but missing from result", missing)
		}
		result = append(result, FetchResult{Fields: fields, Tags: tags})
	}
	return result, nil

}
