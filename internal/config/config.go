package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Databases map[string]Database `yaml:"databases"`
	Influxes  map[string]Influx   `yaml:"influxes"`
	Influxes2 map[string]Influx2  `yaml:"influxes2"`
	Tests     map[string]Test     `yaml:"tests"`
}

type Database struct {
	Host     string `yaml:"host"`
	Port     uint16 `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	Driver   string `yaml:"driver"`
}

type Influx struct {
	Host      string `yaml:"host"`
	Port      uint16 `yaml:"port"`
	SSL       bool   `yaml:"ssl" default:"true"`
	VerifySSL bool   `yaml:"verify_ssl" default:"true"`
	Database  string `yaml:"database"`
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
}

type Influx2 struct {
	Url    string `yaml:"url"`
	Org    string `yaml:"org"`
	Bucket string `yaml:"bucket"`
	Token  string
}

type Test struct {
	IsTemplate  bool              `yaml:"is_template"`
	Databases   []string          `yaml:"databases"`
	Influxes    []string          `yaml:"influxes"`
	Influxes2   []string          `yaml:"influxes2"`
	Tags        map[string]string `yaml:"tags"`
	Fields      []string          `yaml:"fields"`
	Order       int               `yaml:"order"`
	Measurement string            `yaml:"measurement"`
	InheritFrom string            `yaml:"inherit_from"`
	SQL         string            `yaml:"sql"`
}

func (t Test) Check(config *Config) error {
	if t.IsTemplate {
		return nil
	}
	if t.Databases == nil || len(t.Databases) == 0 {
		return fmt.Errorf("no databases specified")
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
	if len(t.Influxes) == 0 && len(t.Influxes2) == 0 {
		return fmt.Errorf("no influxes or influxes2 specified")
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
