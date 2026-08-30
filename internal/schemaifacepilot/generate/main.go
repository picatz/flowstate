package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

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
	dir := filepath.Dir(path)
	file, err := os.CreateTemp(dir, "."+filepath.Base(path)+"-*")
	if err != nil {
		return err
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	if err := generate(file); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}
