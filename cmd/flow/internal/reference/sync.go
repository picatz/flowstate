//go:build ignore

// Command sync rewrites docs/DSL.md's table of contents and mirror/ from the
// documents it mirrors.
//
// Run it with `go generate ./cmd/flow/internal/reference` after editing
// docs/DSL.md or adding an example. TestTheMirrorMatchesTheRepository is what
// makes forgetting the mirror a failure rather than a silently stale answer,
// and TestDSLTOCHasNoDrift is the same guarantee for the contents list.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/picatz/flowstate/cmd/flow/internal/reference"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "sync:", err)
		os.Exit(1)
	}
}

func run() error {
	// Relative to this package's directory, which is where `go generate` runs.
	const repo = "../../../.."

	if err := os.RemoveAll("mirror"); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join("mirror", "examples"), 0o755); err != nil {
		return err
	}

	dslPath := filepath.Join(repo, "docs", "DSL.md")

	dsl, err := os.ReadFile(dslPath)
	if err != nil {
		return err
	}

	// The contents list is derived from the document's own headings, so it is
	// regenerated here rather than copied verbatim — and written back to
	// docs/DSL.md itself, not only the mirror, since the mirror's whole job is
	// to be a byte-identical copy of the source, and a TOC that only existed
	// in the copy is the "second source of truth" this package's own doc
	// comment warns about.
	dsl, err = reference.SyncTOC(dsl)
	if err != nil {
		return fmt.Errorf("regenerating docs/DSL.md's table of contents: %w", err)
	}
	if err := os.WriteFile(dslPath, dsl, 0o644); err != nil {
		return err
	}

	if err := os.WriteFile(filepath.Join("mirror", "DSL.md"), dsl, 0o644); err != nil {
		return err
	}

	// Every directory under examples/ holding a workflow.yaml, flattened to
	// <name>.yaml: the directory name is the whole of an example's identity on
	// the resource surface, and a nested path would put a second thing in the
	// URI that nothing reads.
	sources, err := filepath.Glob(filepath.Join(repo, "examples", "*", "workflow.yaml"))
	if err != nil {
		return err
	}
	sort.Strings(sources)

	if len(sources) == 0 {
		return fmt.Errorf("no examples found under %s/examples: refusing to write an empty mirror", repo)
	}

	for _, source := range sources {
		name := filepath.Base(filepath.Dir(source))

		data, err := os.ReadFile(source)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join("mirror", "examples", name+".yaml"), data, 0o644); err != nil {
			return err
		}
	}

	fmt.Fprintf(os.Stderr, "sync: mirrored docs/DSL.md and %d examples\n", len(sources))

	return nil
}
