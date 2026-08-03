// Package reference holds the documents `flow mcp` serves as MCP resources.
//
// An agent asking this binary what the language is must get an answer from the
// binary, not from wherever it happens to be standing. `flow` is installed with
// `go install` and run from a home directory, a container, or a CI job with no
// checkout anywhere near it — so a resource handler that read ../../docs/DSL.md
// would serve the reference on a maintainer's laptop and a "not found" for
// everybody else. The documents are therefore compiled in.
//
// The go:embed directive cannot reach outside the package directory, so the
// compiled-in copies live here in mirror/ rather than being embedded from docs/
// and examples/ in place. That is a value written down twice, which this
// repository treats as a defect wherever it is left unchecked — so it is
// checked: `go generate ./...` rewrites the mirror from the originals, and
// TestTheMirrorMatchesTheRepository fails when it has drifted, which is the same
// mechanism `buf generate` plus `git diff --exit-code` uses on the generated
// protobuf code. The copy is the artifact; the originals stay the source.
//
// The tradeoff that remains, and it is real: what is served is frozen at build
// time. A binary from March answers with March's DSL.md. That is the honest
// shape of the alternative — an answer that is wrong for the *binary* is worse
// than one that is old, since the reference an agent reads should describe the
// engine it is about to call, and the engine is compiled in too.
package reference

import (
	"embed"
	"io/fs"
	"path"
	"slices"
	"strings"
)

//go:generate go run sync.go

// mirror holds the copied documents. See the package doc for why they are copies.
//
//go:embed mirror
var mirror embed.FS

// DSLPath and examplesDir are where the mirror keeps each kind, named once so
// the generator and the readers cannot disagree about a path.
const (
	dslPath     = "mirror/DSL.md"
	examplesDir = "mirror/examples"
)

// DSL returns the Flowfile language reference — the content of docs/DSL.md as of
// the build.
func DSL() string {
	data, err := mirror.ReadFile(dslPath)
	if err != nil {
		// Unreachable: the file is embedded, so a failure here is a build that
		// should not have linked.
		panic("flow: the embedded DSL reference is missing: " + err.Error())
	}

	return string(data)
}

// ExampleNames returns the embedded examples' names — the directory name each one
// has under examples/ — sorted, so a listing is stable between runs.
func ExampleNames() []string {
	entries, err := fs.ReadDir(mirror, examplesDir)
	if err != nil {
		panic("flow: the embedded examples are missing: " + err.Error())
	}

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		names = append(names, strings.TrimSuffix(entry.Name(), ".yaml"))
	}
	slices.Sort(names)

	return names
}

// Example returns one example's workflow.yaml, and whether there is one by that
// name.
//
// A name carrying a path separator or a dot segment is refused rather than
// cleaned, because the caller is a URI an agent composed: the reads here are
// from an embedded filesystem with nothing else in it, so the worst case is a
// miss, but a lookup that quietly rewrites what it was asked for is the habit
// that becomes a traversal the day it is pointed at a disk.
func Example(name string) (string, bool) {
	if name == "" || strings.ContainsAny(name, "/\\") || name == "." || name == ".." {
		return "", false
	}

	data, err := mirror.ReadFile(path.Join(examplesDir, name+".yaml"))
	if err != nil {
		return "", false
	}

	return string(data), true
}
