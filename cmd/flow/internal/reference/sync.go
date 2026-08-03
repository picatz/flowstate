//go:build ignore

// Command sync rewrites mirror/ from the documents it mirrors.
//
// Run it with `go generate ./cmd/flow/internal/reference` after editing
// docs/DSL.md or adding an example. TestTheMirrorMatchesTheRepository is what
// makes forgetting a failure rather than a silently stale answer.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
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

	dsl, err := os.ReadFile(filepath.Join(repo, "docs", "DSL.md"))
	if err != nil {
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
