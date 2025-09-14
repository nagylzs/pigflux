package config

import (
	"fmt"
	"regexp"

	"github.com/nagylzs/set"
)

func (cf *Config) ParseConfig() error {
	// Test identifiers
	for name := range cf.Influxes {
		if !IsIdentifierLike(name) {
			return fmt.Errorf("invalid influx name: %s", name)
		}
	}
	for name := range cf.Influxes2 {
		if !IsIdentifierLike(name) {
			return fmt.Errorf("invalid influx2 name: %s", name)
		}
	}
	for name := range cf.Influxes3 {
		if !IsIdentifierLike(name) {
			return fmt.Errorf("invalid influx3 name: %s", name)
		}
	}
	for name := range cf.Databases {
		if !IsIdentifierLike(name) {
			return fmt.Errorf("invalid database name: %s", name)
		}
	}
	for name := range cf.Tests {
		if !IsIdentifierLike(name) {
			return fmt.Errorf("invalid test name: %s", name)
		}
	}

	// Test for circular references, inherit properites
	used := set.NewSet[string]()
	for name := range cf.Tests {
		err := cf.inheritProps(name, used)
		if err != nil {
			return err
		}
	}
	for dbname, db := range cf.Databases {
		if db.Driver == "" {
			return fmt.Errorf("database %s: driver is not given/empty", dbname)
		}
		if db.Driver != "pgx" && db.Driver != "mysql" && db.Driver != "sqlserver" {
			return fmt.Errorf("database %s: driver %s not supported, only pgx, mysql, sqlserver are available", dbname, db.Driver)
		}
	}
	for name := range cf.Tests {
		err := cf.Tests[name].Check(cf)
		if err != nil {
			return fmt.Errorf("test '%s': %w", name, err)
		}
	}
	return nil
}

func IsIdentifierLike(s string) bool {
	ok, err := regexp.Match("[a-zA-Z][a-zA-Z0-9]*", []byte(s))
	if err != nil {
		panic(err)
	}
	return ok
}

func (cf *Config) inheritProps(name string, used *set.Set[string]) error {
	test := cf.Tests[name]
	ref := test.InheritFrom
	if ref == "" {
		return nil
	}
	if used.Contains(ref) {
		return fmt.Errorf("circular reference tests.%s.inherit_from=%s (used=%v)", name, ref, used)
	}
	ihf, ok := cf.Tests[ref]
	if !ok {
		return fmt.Errorf("invalid reference tests.%s.inherit_from=%s", name, ref)
	}
	used.Add(ref)
	err := cf.inheritProps(ref, used)
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
	cf.Tests[name] = test
	return nil
}
