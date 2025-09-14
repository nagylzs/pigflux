package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Databases map[string]Database `yaml:"databases"`
	Influxes  map[string]Influx   `yaml:"influxes"`
	Influxes2 map[string]Influx2  `yaml:"influxes2"`
	Influxes3 map[string]Influx3  `yaml:"influxes3"`
	Tests     map[string]Test     `yaml:"tests"`
}

type Database struct {
	DSN       string `yaml:"dsn"`
	Driver    string `yaml:"driver"`
	InsertSQL string `yaml:"insert_sql"`
}

type Influx struct {
	URL       string `yaml:"host"`
	VerifySSL bool   `yaml:"verify_ssl" default:"true"`
	Database  string `yaml:"database"`
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
}

type Influx2 struct {
	Url    string `yaml:"url"`
	Org    string `yaml:"org"`
	Bucket string `yaml:"bucket"`
	Token  string `yaml:"token"`
}

type Influx3 struct {
	Url string `yaml:"url"`
}

type Test struct {
	IsTemplate      bool              `yaml:"is_template"`
	Databases       []string          `yaml:"databases"`
	Influxes        []string          `yaml:"influxes"`
	Influxes2       []string          `yaml:"influxes2"`
	Influxes3       []string          `yaml:"influxes3"`
	TargetDatabases []string          `yaml:"target_databases"`
	Tags            map[string]string `yaml:"tags"`
	Fields          []string          `yaml:"fields"`
	Order           int               `yaml:"order"`
	Timeout         time.Duration     `yaml:"timeout"`
	Measurement     string            `yaml:"measurement"`
	InheritFrom     string            `yaml:"inherit_from"`
	SQL             string            `yaml:"sql"`
}

func (t Test) Check(config *Config) error {
	if t.IsTemplate {
		return nil
	}
	if t.Databases == nil || len(t.Databases) == 0 {
		return fmt.Errorf("no databases specified")
	}
	for _, dbname := range t.Databases {
		_, ok := config.Databases[dbname]
		if !ok {
			return fmt.Errorf("database '%s' does not exist", dbname)
		}
	}

	if len(t.Influxes) > 0 {
		for _, influx := range t.Influxes {
			_, ok := config.Influxes[influx]
			if !ok {
				return fmt.Errorf("influx '%s' does not exist", influx)
			}
		}
	}
	if len(t.Influxes2) > 0 {
		for _, influx2 := range t.Influxes2 {
			_, ok := config.Influxes2[influx2]
			if !ok {
				return fmt.Errorf("influx2 '%s' does not exist", influx2)
			}
		}
	}
	if len(t.Influxes3) > 0 {
		for _, influx3 := range t.Influxes3 {
			_, ok := config.Influxes3[influx3]
			if !ok {
				return fmt.Errorf("influx3 '%s' does not exist", influx3)
			}
		}
	}
	if len(t.TargetDatabases) > 0 {
		for _, dbname := range t.TargetDatabases {
			db, ok := config.Databases[dbname]
			if !ok {
				return fmt.Errorf("database '%s' does not exist", db)
			}
			if db.InsertSQL == "" {
				return fmt.Errorf("database '%s' used as a target database, but insert_sql is empty", db)
			}
		}
	}
	if len(t.Influxes) == 0 && len(t.Influxes2) == 0 && len(t.Influxes3) == 0 && len(t.TargetDatabases) == 0 {
		return fmt.Errorf("no targets specified (influxes, influxes2, influxes3 and targetdatabase are all empty)")
	}
	return nil
}

func LoadConfig(path string) (Config, error) {
	var result Config
	if path == "" {
		return result, fmt.Errorf("config file path is not specified")
	}
	yf, err := os.ReadFile(path)
	if err != nil {
		return result, fmt.Errorf("cannot read instance config file %s: %v", path, err.Error())
	}
	err = yaml.Unmarshal(yf, &result)
	if err != nil {
		return result, fmt.Errorf("cannot parse YAML file %s: %v", path, err.Error())
	}

	return result, nil
}
