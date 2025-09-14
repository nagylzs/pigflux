package config

import (
	_ "embed"
	"fmt"
)

//go:embed pigflux_example.yml
var ExampleYaml string

func ShowConfigExample() {
	fmt.Println(ExampleYaml)
}
