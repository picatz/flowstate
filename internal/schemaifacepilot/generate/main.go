package main

import (
	"fmt"
	"io"
	"os"

	"github.com/picatz/flowstate/internal/schemaifacepilot"
	"github.com/picatz/flowstate/internal/schemaifacepilot/reference"
)

func main() {
	if err := write("get_static_generated.go", schemaifacepilot.GenerateStaticGet); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := write("testdata/get-fields.md", reference.GenerateGet); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func write(path string, generate func(io.Writer) error) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := generate(file); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}
