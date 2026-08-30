package main

import (
	"io"

	"github.com/picatz/flowstate/internal/schemaifacepilot/reference"
)

func main() {
	if err := reference.GenerateGet(io.Discard); err != nil {
		panic(err)
	}
}
