package readme

import (
	_ "embed"
	"fmt"
)

//go:embed README.md
var Readme string

func ShowReadme() {
	fmt.Println(Readme)
}
