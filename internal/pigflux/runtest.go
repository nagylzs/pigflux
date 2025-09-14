package pigflux

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/nagylzs/pigflux/internal/config"
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

func fetchTest(cf config.Config, dbname string, test config.Test) (map[string]interface{}, error) {
	// TODO use QueryTimeout here!
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
	for _, column := range columns {
		if !config.IsIdentifierLike(column) {
			return nil, fmt.Errorf("invalid column: %s, only [a-zA-Z][a-zA-Z0-9]* is supported", column)
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
