package main

import (
	"reflect"
	"testing"
)

// TestBuildPlan is the file-to-leg mapping, with the changed-file list
// injected so no git repository is needed.
func TestBuildPlan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		changed []string
		want    plan
	}{
		{
			name:    "empty diff plans nothing conditional",
			changed: nil,
			want:    plan{reasons: map[string]string{}},
		},
		{
			name:    "a package edit maps to its directory and no conditional leg",
			changed: []string{"pkg/flowstate/v1/engine/policy.go"},
			want: plan{
				goFiles:  []string{"pkg/flowstate/v1/engine/policy.go"},
				fileDirs: []string{"pkg/flowstate/v1/engine"},
				reasons:  map[string]string{},
			},
		},
		{
			// A proto-only edit is also a docs edit: `flow docs
			// generate` reads a task's field names, types and
			// required-ness from the protovalidate rules on the
			// schema-backed descriptors, and builds the MCP tool
			// list by walking the service descriptor. Firing only
			// the proto leg here would let the gate pass with
			// stale docs/reference/.
			name:    "a proto-only edit fires the proto leg AND the docs leg",
			changed: []string{"proto/flowstate/v1/flowstate.proto"},
			want: plan{
				fileDirs: []string{"proto/flowstate/v1"},
				proto:    true,
				docs:     true,
				reasons: map[string]string{
					"proto": "proto/flowstate/v1/flowstate.proto",
					"docs":  "proto/flowstate/v1/flowstate.proto",
				},
			},
		},
		{
			name:    "buf config fires the proto leg without living under proto/",
			changed: []string{"buf.gen.yaml"},
			want: plan{
				fileDirs: []string{"."},
				proto:    true,
				docs:     true,
				reasons: map[string]string{
					"proto": "buf.gen.yaml",
					"docs":  "buf.gen.yaml",
				},
			},
		},
		{
			name:    "DSL.md fires the docs leg",
			changed: []string{"docs/DSL.md"},
			want: plan{
				fileDirs: []string{"docs"},
				docs:     true,
				reasons:  map[string]string{"docs": "docs/DSL.md"},
			},
		},
		{
			name:    "an example workflow fires examples and the docs mirror",
			changed: []string{"examples/call-a-workflow/workflow.yaml"},
			want: plan{
				fileDirs: []string{"examples/call-a-workflow"},
				docs:     true,
				examples: true,
				reasons: map[string]string{
					"docs":     "examples/call-a-workflow/workflow.yaml",
					"examples": "examples/call-a-workflow/workflow.yaml",
				},
			},
		},
		{
			name:    "an example test file fires examples but not the mirror",
			changed: []string{"examples/call-a-workflow/workflow.test.yaml"},
			want: plan{
				fileDirs: []string{"examples/call-a-workflow"},
				examples: true,
				reasons:  map[string]string{"examples": "examples/call-a-workflow/workflow.test.yaml"},
			},
		},
		{
			name:    "the cobra and MCP surfaces fire the docs leg",
			changed: []string{"cmd/flow/internal/docsgen/cli.go"},
			want: plan{
				goFiles:    []string{"cmd/flow/internal/docsgen/cli.go"},
				fileDirs:   []string{"cmd/flow/internal/docsgen"},
				docs:       true,
				appearance: true,
				reasons: map[string]string{
					"docs":       "cmd/flow/internal/docsgen/cli.go",
					"appearance": "cmd/flow/internal/docsgen/cli.go",
				},
			},
		},
		{
			// The goldens embed a sample Flowfile stamped with
			// CurrentEdition, so an edition bump moves recorded
			// appearance without touching styling code. #483 shipped
			// exactly that and found out on CI.
			name:    "an edition bump fires the appearance notice",
			changed: []string{"pkg/flowstate/v1/flowfile/edition.go"},
			want: plan{
				goFiles:    []string{"pkg/flowstate/v1/flowfile/edition.go"},
				fileDirs:   []string{"pkg/flowstate/v1/flowfile"},
				appearance: true,
				reasons:    map[string]string{"appearance": "pkg/flowstate/v1/flowfile/edition.go"},
			},
		},
		{
			name:    "styled output and help text fire the appearance notice",
			changed: []string{"cmd/flow/internal/ui/style.go"},
			want: plan{
				goFiles:    []string{"cmd/flow/internal/ui/style.go"},
				fileDirs:   []string{"cmd/flow/internal/ui"},
				docs:       true,
				appearance: true,
				reasons: map[string]string{
					"docs":       "cmd/flow/internal/ui/style.go",
					"appearance": "cmd/flow/internal/ui/style.go",
				},
			},
		},
		{
			name:    "an engine change does not fire the appearance notice",
			changed: []string{"pkg/flowstate/v1/engine/policy.go"},
			want: plan{
				goFiles:  []string{"pkg/flowstate/v1/engine/policy.go"},
				fileDirs: []string{"pkg/flowstate/v1/engine"},
				reasons:  map[string]string{},
			},
		},
		{
			name:    "the registry package fires the docs leg",
			changed: []string{"pkg/flowstate/v1/catalog_functions.go"},
			want: plan{
				goFiles:  []string{"pkg/flowstate/v1/catalog_functions.go"},
				fileDirs: []string{"pkg/flowstate/v1"},
				docs:     true,
				reasons:  map[string]string{"docs": "pkg/flowstate/v1/catalog_functions.go"},
			},
		},
		{
			name:    "a registry subpackage does not fire the docs leg",
			changed: []string{"pkg/flowstate/v1/flowfile/validate.go"},
			want: plan{
				goFiles:  []string{"pkg/flowstate/v1/flowfile/validate.go"},
				fileDirs: []string{"pkg/flowstate/v1/flowfile"},
				reasons:  map[string]string{},
			},
		},
		{
			name:    "go.mod flips the module-wide switch",
			changed: []string{"go.mod", "go.sum"},
			want: plan{
				moduleWide: true,
				reasons:    map[string]string{"module": "go.mod"},
			},
		},
		{
			name:    "plugin files land in plugins, not fileDirs",
			changed: []string{"plugins/openai/main.go", "plugins/openai/go.mod"},
			want: plan{
				plugins: []string{"plugins/openai"},
				reasons: map[string]string{},
			},
		},
		{
			name: "a mixed diff fires each leg once with the first trigger recorded",
			changed: []string{
				"proto/flowstate/v1/flowstate.proto",
				"proto/flowstate/v1/plugin.proto",
				"docs/DSL.md",
				"examples/hello/workflow.yaml",
				"pkg/flowstate/v1/engine/policy.go",
				"CLAUDE.md",
			},
			want: plan{
				goFiles: []string{"pkg/flowstate/v1/engine/policy.go"},
				fileDirs: []string{
					".",
					"docs",
					"examples/hello",
					"pkg/flowstate/v1/engine",
					"proto/flowstate/v1",
				},
				proto:    true,
				docs:     true,
				examples: true,
				reasons: map[string]string{
					"proto": "proto/flowstate/v1/flowstate.proto",
					// The schema is a docs source too, and it
					// is first in this diff, so it is the
					// trigger recorded rather than DSL.md.
					"docs":     "proto/flowstate/v1/flowstate.proto",
					"examples": "examples/hello/workflow.yaml",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := buildPlan(tt.changed)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildPlan(%v)\n got %+v\nwant %+v", tt.changed, got, tt.want)
			}
		})
	}
}

// TestResolveDirs is the file-to-package half: directories resolve upward to
// the package that owns them, with the go list index injected.
func TestResolveDirs(t *testing.T) {
	t.Parallel()

	index := map[string]string{
		"pkg/flowstate/v1":          modulePath + "/pkg/flowstate/v1",
		"pkg/flowstate/v1/flowfile": modulePath + "/pkg/flowstate/v1/flowfile",
		"cmd/flow":                  modulePath + "/cmd/flow",
	}

	tests := []struct {
		name string
		dirs []string
		want []string
	}{
		{
			name: "a package directory resolves to itself",
			dirs: []string{"pkg/flowstate/v1/flowfile"},
			want: []string{modulePath + "/pkg/flowstate/v1/flowfile"},
		},
		{
			name: "testdata resolves upward to the package whose tests read it",
			dirs: []string{"pkg/flowstate/v1/flowfile/testdata/fuzz"},
			want: []string{modulePath + "/pkg/flowstate/v1/flowfile"},
		},
		{
			name: "a directory with no package ancestor resolves to nothing",
			dirs: []string{"docs/plans", ".github/workflows", "."},
			want: nil,
		},
		{
			name: "duplicates collapse",
			dirs: []string{"cmd/flow", "cmd/flow/testdata"},
			want: []string{modulePath + "/cmd/flow"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := resolveDirs(tt.dirs, index)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("resolveDirs(%v) = %v, want %v", tt.dirs, got, tt.want)
			}
		})
	}
}

// TestAffectedPackages checks the reverse-dependency expansion on a small
// injected graph, including the two directions that matter: test imports
// reach a change (affected), and importing an affected package's code does
// not inherit its test-only affectedness.
func TestAffectedPackages(t *testing.T) {
	t.Parallel()

	const m = modulePath
	pkgs := []pkgMeta{
		{ImportPath: m + "/a"},                                                // changed
		{ImportPath: m + "/b", Deps: []string{m + "/a"}},                      // imports a
		{ImportPath: m + "/c", Deps: []string{m + "/b", m + "/a"}},            // imports b (Deps transitive)
		{ImportPath: m + "/d", TestImports: []string{m + "/testsupport"}},     // tests import testsupport
		{ImportPath: m + "/testsupport", Deps: []string{m + "/a"}},            // testsupport imports a
		{ImportPath: m + "/e", Deps: []string{m + "/d"}},                      // imports d's code, not its tests
		{ImportPath: m + "/f", XTestImports: []string{m + "/a"}},              // external tests import a
		{ImportPath: m + "/g", Deps: []string{"github.com/spf13/cobra"}},      // unrelated
		{ImportPath: m + "/h", TestImports: []string{"github.com/x/unknown"}}, // unknown test import
	}
	changed := map[string]bool{m + "/a": true}

	got := affectedPackages(pkgs, changed)
	want := []string{
		m + "/a",           // changed itself
		m + "/b",           // direct importer
		m + "/c",           // transitive importer
		m + "/d",           // its tests reach the change through testsupport
		m + "/f",           // its external tests import the change
		m + "/testsupport", // direct importer
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("affectedPackages = %v, want %v", got, want)
	}

	if affectedPackages(pkgs, nil) != nil {
		t.Error("no changes must mean no affected packages")
	}
}

func TestNeedsOrdering(t *testing.T) {
	t.Parallel()

	if needsOrdering([]string{modulePath + "/pkg/flowstate/v1/engine"}) {
		t.Error("ordering leg must not fire when flowtest is unaffected")
	}
	if !needsOrdering([]string{modulePath + "/pkg/flowstate/v1/engine", flowtestPkg}) {
		t.Error("ordering leg must fire when flowtest is affected")
	}
}

// TestNeedsDocs pins the authoritative docs trigger: `flow docs generate`
// runs the cmd/flow binary, so anything reaching that binary can move its
// output, whether or not a path rule in buildPlan names it.
func TestNeedsDocs(t *testing.T) {
	t.Parallel()

	if needsDocs([]string{modulePath + "/pkg/flowstate/v1/flowtest"}) {
		t.Error("docs leg must not fire on the affected set alone when cmd/flow is unaffected")
	}
	if !needsDocs([]string{modulePath + "/pkg/flowstate/v1", cmdFlowPkg}) {
		t.Error("docs leg must fire when cmd/flow is affected")
	}
}

// TestProtoChangeReachesTheDocsBinary is the same claim one level down: a
// change to the generated code a proto edit produces reaches cmd/flow
// through the package graph, so even a plan that failed to fire the docs
// leg on the path rule would still be caught by needsDocs.
func TestProtoChangeReachesTheDocsBinary(t *testing.T) {
	t.Parallel()

	const m = modulePath
	pkgs := []pkgMeta{
		{ImportPath: m + "/pkg/flowstate/v1"},
		{ImportPath: cmdFlowPkg, Deps: []string{m + "/pkg/flowstate/v1"}},
		{ImportPath: m + "/pkg/flowstate/v1/flowtest", Deps: []string{m + "/pkg/flowstate/v1"}},
	}
	// The .pb.go a proto edit rewrites lives in pkg/flowstate/v1.
	affected := affectedPackages(pkgs, map[string]bool{m + "/pkg/flowstate/v1": true})
	if !needsDocs(affected) {
		t.Errorf("a schema change must reach the docs binary; affected = %v", affected)
	}
}
