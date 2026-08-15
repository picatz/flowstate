package flowstatev1_test

import (
	"go/parser"
	"go/token"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// selfPrefix is the import path prefix of this package's own subpackages.
const selfPrefix = "github.com/picatz/flowstate/pkg/flowstate/v1/"

// allowedSelfImports records every package under pkg/flowstate/v1/ that
// pkg/flowstate/v1 itself imports, and the non-test files that introduce it.
//
// The invariant this table is walking towards is one sentence: **the schema
// package imports nothing under pkg/flowstate/v1/**. It is not true today, and
// this table is the honest statement of by how much — a ratchet rather than an
// assertion of success. Adding an edge fails here and has to be argued for in
// the diff that adds it; removing one fails here too, so the table shrinks as
// the debt is paid instead of quietly keeping stale entries.
//
// Why the property is worth mechanising: pkg/flowstate/v1 is the package
// everything else must be able to import, so each dependency it takes is
// inherited by every importer — pkg/flowstate/embed, plugin hosts, WASM builds.
// Today that inheritance includes connectrpc, a JOSE implementation and an IDNA
// table, none of which describing a workflow requires.
//
// The remaining edges and what each would take to remove (#406):
//
//   - netpolicy — the built-in http task's egress policy. The only edge that is
//     purely the task library's, and the only one whose removal drops a
//     dependency outright. It cannot leave on its own: [HTTPTaskDef] and
//     [DefaultEgressPolicy] name *netpolicy.Policy in exported signatures, so
//     relocating the task means relocating those, which means this package no
//     longer registers its own built-ins.
//   - auth — entity.go (the namespace grammar, for a bound checked at compile
//     time) and taskruntime.go.
//   - secrets — taskruntime.go and webhookverify.go. secrets imports auth, so
//     this edge carries that one with it.
//   - nearest — one did-you-mean suggestion in constraints.go. A leaf with no
//     dependencies of its own; the cheapest edge to remove and the one that
//     buys the least.
//
// taskruntime.go is the load-bearing one and the reason this is not a single
// change: eval.go — the local interpreter, which lives in this package —
// resolves secrets and derives per-step authority through it, so the auth and
// secrets edges do not leave until the interpreter does.
var allowedSelfImports = map[string][]string{
	"auth": {
		"entity.go",
		"eval_task_http_run.go",
		"taskruntime.go",
	},
	"nearest": {
		"constraints.go",
	},
	"netpolicy": {
		"eval_task_http_def.go",
		"eval_task_http_run.go",
	},
	"secrets": {
		"eval_task_http_run.go",
		"taskruntime.go",
		"webhookverify.go",
	},
}

// TestSchemaPackageImportsAreRatcheted pins which of its own subpackages
// pkg/flowstate/v1 imports, and from where.
//
// It reads the package's non-test sources directly rather than shelling out to
// `go list`, so it holds in a sandbox with no toolchain and no module cache to
// warm, and it reports the offending file rather than only the edge — which is
// the fact a reader needs to decide whether a new edge is justified.
func TestSchemaPackageImportsAreRatcheted(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading package directory: %v", err)
	}

	got := map[string][]string{}

	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}

		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}

		for _, spec := range file.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatalf("%s: unquoting import %s: %v", name, spec.Path.Value, err)
			}

			sub, ok := strings.CutPrefix(path, selfPrefix)
			if !ok {
				continue
			}

			got[sub] = append(got[sub], name)
		}
	}

	for _, files := range got {
		slices.Sort(files)
	}

	for _, sub := range slices.Sorted(maps.Keys(got)) {
		want, allowed := allowedSelfImports[sub]
		if !allowed {
			t.Errorf("pkg/flowstate/v1 has a new import of %s%s, from %v.\n"+
				"The schema package is meant to sit at the bottom of the graph: every dependency it takes is inherited by every importer.\n"+
				"If the edge is genuinely necessary, add it to allowedSelfImports with the reason; otherwise put the code that needs it in a package above this one.",
				selfPrefix, sub, got[sub])
			continue
		}
		if !slices.Equal(got[sub], want) {
			t.Errorf("files importing %s%s changed:\n got %v\nwant %v\n"+
				"Update allowedSelfImports if this is intended — the list is what makes the debt readable.",
				selfPrefix, sub, got[sub], want)
		}
	}

	for _, sub := range slices.Sorted(maps.Keys(allowedSelfImports)) {
		if _, still := got[sub]; !still {
			t.Errorf("pkg/flowstate/v1 no longer imports %s%s — remove it from allowedSelfImports so the ratchet tightens (#406).",
				selfPrefix, sub)
		}
	}
}
