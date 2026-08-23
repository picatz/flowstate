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
			changed: []string{"proto/flowstate/v1/workflow.proto"},
			want: plan{
				fileDirs: []string{"proto/flowstate/v1"},
				proto:    true,
				docs:     true,
				reasons: map[string]string{
					"proto": "proto/flowstate/v1/workflow.proto",
					"docs":  "proto/flowstate/v1/workflow.proto",
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
			// The example plugin's schema is a second buf module with
			// a descriptor set of its own, which is what carries that
			// plugin's field comments to an editor (#723). It rides
			// the proto leg, so that a .proto edit re-pins the
			// artifact built from it — but not the docs leg, since
			// nothing under docs/reference/ is derived from a
			// plugin's schema.
			name:    "the example plugin's schema fires the proto leg and not the docs leg",
			changed: []string{examplePluginProtoDir + "example/v1/example.proto"},
			want: plan{
				fileDirs: []string{examplePluginProtoDir + "example/v1"},
				proto:    true,
				reasons: map[string]string{
					"proto": examplePluginProtoDir + "example/v1/example.proto",
				},
			},
		},
		{
			// The artifact as well as its source: a pin over a file
			// nothing rebuilt is not a pin.
			name:    "the example plugin's descriptor set fires the proto leg",
			changed: []string{examplePluginProse},
			want: plan{
				fileDirs: []string{"pkg/flowstate/v1/plugin/examples/flowstate-plugin-example"},
				proto:    true,
				reasons: map[string]string{
					"proto": examplePluginProse,
				},
			},
		},
		{
			name:    "DSL.md fires the docs leg",
			changed: []string{"docs/DSL.md"},
			want: plan{
				fileDirs:      []string{"docs"},
				docs:          true,
				repoTestData:  true,
				repoDataRoots: []string{"docs"},
				reasons: map[string]string{
					"docs": "docs/DSL.md",
					"test": "docs/DSL.md",
				},
			},
		},
		{
			// Every Markdown file under docs/ is test data, because
			// cmd/flow/docsindex_test.go is about the *set* of them:
			// a page added, renamed or removed without docs/README.md
			// moving with it fails there, and nothing else would run
			// (#708). This one reaches no other leg at all.
			name:    "a documentation page fires the test job and nothing else",
			changed: []string{"docs/DEPLOYMENT.md"},
			want: plan{
				fileDirs:      []string{"docs"},
				repoTestData:  true,
				repoDataRoots: []string{"docs"},
				reasons:       map[string]string{"test": "docs/DEPLOYMENT.md"},
			},
		},
		{
			name:    "an internal plan is test data too, for its banner",
			changed: []string{"docs/plans/factory.md"},
			want: plan{
				fileDirs:      []string{"docs/plans"},
				repoTestData:  true,
				repoDataRoots: []string{"docs"},
				reasons:       map[string]string{"test": "docs/plans/factory.md"},
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
			// The plugin's own module (a separate `go list` walk this
			// gate does not perform) reads this exact file at test
			// time with os.ReadFile, the same #589 shape one module
			// further out; buildPlan records the plugin name so run()
			// can fold it into the plugins skip notice once it has
			// checked plugins/vcs/go.mod actually exists.
			name:    "an examples/plugins/ change fires examples and records the plugin name",
			changed: []string{"examples/plugins/vcs/workflow.yaml"},
			want: plan{
				fileDirs:           []string{"examples/plugins/vcs"},
				docs:               true,
				examples:           true,
				examplePluginNames: []string{"vcs"},
				reasons: map[string]string{
					"docs":     "examples/plugins/vcs/workflow.yaml",
					"examples": "examples/plugins/vcs/workflow.yaml",
				},
			},
		},
		{
			// examples/plugins/greet has no corresponding plugins/
			// module — it is pkg/flowstate/v1/plugin's in-repo
			// fixture — but buildPlan has no filesystem to know that
			// with, so it records the name regardless; run() is where
			// the existence check happens.
			name:    "examples/plugins/greet records a name buildPlan cannot verify",
			changed: []string{"examples/plugins/greet/workflow.yaml"},
			want: plan{
				fileDirs:           []string{"examples/plugins/greet"},
				docs:               true,
				examples:           true,
				examplePluginNames: []string{"greet"},
				reasons: map[string]string{
					"docs":     "examples/plugins/greet/workflow.yaml",
					"examples": "examples/plugins/greet/workflow.yaml",
				},
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
				"proto/flowstate/v1/workflow.proto",
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
				proto:         true,
				docs:          true,
				examples:      true,
				repoTestData:  true,
				repoDataRoots: []string{"docs"},
				reasons: map[string]string{
					"proto": "proto/flowstate/v1/workflow.proto",
					// The schema is a docs source too, and it
					// is first in this diff, so it is the
					// trigger recorded rather than DSL.md.
					"docs":     "proto/flowstate/v1/workflow.proto",
					"examples": "examples/hello/workflow.yaml",
					"test":     "docs/DSL.md",
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

// TestHasUnresolvedGoDir is the regression for a Codex P1 on #688: a diff
// that deletes the last .go file a package had leaves that package's
// directory absent from `go list`'s output entirely, which resolveDirs
// (walking upward for the nearest ancestor) cannot distinguish from a file
// that was never part of a package at all — both silently drop out of the
// changed set, and whatever else in the tree still imports the now-vanished
// package would build broken with the test job never having run.
func TestHasUnresolvedGoDir(t *testing.T) {
	t.Parallel()

	index := map[string]string{
		"pkg/flowstate/v1":          modulePath + "/pkg/flowstate/v1",
		"pkg/flowstate/v1/flowfile": modulePath + "/pkg/flowstate/v1/flowfile",
		"cmd/flow":                  modulePath + "/cmd/flow",
	}

	tests := []struct {
		name string
		go_  []string
		want bool
	}{
		{
			name: "every changed .go file's own directory is still a package",
			go_:  []string{"cmd/flow/main.go", "pkg/flowstate/v1/flowfile/parse.go"},
			want: false,
		},
		{
			name: "a deleted package's last file leaves its directory unresolved",
			go_:  []string{"pkg/flowstate/v1/deleted/last.go"},
			want: true,
		},
		{
			name: "one resolvable file alongside one unresolvable one still reports true",
			go_:  []string{"cmd/flow/main.go", "pkg/flowstate/v1/deleted/last.go"},
			want: true,
		},
		{
			name: "no changed .go files resolves to nothing missing",
			go_:  nil,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := hasUnresolvedGoDir(tt.go_, index); got != tt.want {
				t.Errorf("hasUnresolvedGoDir(%v) = %v, want %v", tt.go_, got, tt.want)
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

// TestExampleDataDepPackages is a direct test of the pattern match, with the
// two literal shapes found across the repository's example-reading tests:
// an inline relative path, and the bare "examples" segment handed to
// filepath.Join alongside the rest of the path. A package whose tests never
// mention examples/ at all must not match.
func TestExampleDataDepPackages(t *testing.T) {
	t.Parallel()

	const m = modulePath
	testSrc := map[string][]byte{
		// pkg/flowstate/v1/flowfile/validate_switch_domain_internal_test.go's
		// actual shape (#589): filepath.Join with a bare "examples" token.
		m + "/pkg/flowstate/v1/flowfile": []byte(
			`data, err := os.ReadFile(filepath.Join(repoRoot(), "examples", "approval-gate", "workflow.yaml"))`),
		// cmd/flow/unreachable_test.go's actual shape: an inline relative path.
		m + "/cmd/flow": []byte(
			`refusedStart("../../examples/hello-world/workflow.yaml", "hello-world", nil, server, unavailable())`),
		// A package with no example-shaped tokens at all.
		m + "/pkg/flowstate/v1/engine": []byte(`func TestPolicy(t *testing.T) {}`),
	}

	got := exampleDataDepPackages(testSrc)
	want := []string{m + "/cmd/flow", m + "/pkg/flowstate/v1/flowfile"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("exampleDataDepPackages = %v, want %v", got, want)
	}
}

// TestExamplesChangeReachesAPackageThatOnlyReadsExamplesAtRuntime reproduces
// #589: a change confined to examples/approval-gate/workflow.yaml touches no
// .go file and so no package's directory, but
// pkg/flowstate/v1/flowfile/validate_switch_domain_internal_test.go reads
// that exact file with os.ReadFile at test time
// (TestApprovalGateDomainIsInferable pins the values it produces). The
// import-graph mapper alone has nothing to expand from; seeding the changed
// set with exampleDataDepPackages before calling affectedPackages is what
// makes the flowfile package show up.
func TestExamplesChangeReachesAPackageThatOnlyReadsExamplesAtRuntime(t *testing.T) {
	t.Parallel()

	const m = modulePath
	flowfilePkg := m + "/pkg/flowstate/v1/flowfile"

	changed := []string{"examples/approval-gate/workflow.yaml"}
	p := buildPlan(changed)
	if !p.examples {
		t.Fatal("a change under examples/ must set p.examples")
	}

	// What run() does: resolve changed file directories to packages (here,
	// none — examples/approval-gate is not a Go package), then seed in
	// whatever exampleDataDepPackages finds among the affected-set input.
	changedSet := map[string]bool{}
	for _, ip := range resolveDirs(p.fileDirs, map[string]string{}) {
		changedSet[ip] = true
	}
	if len(changedSet) != 0 {
		t.Fatalf("examples/approval-gate has no Go package ancestor; changedSet = %v", changedSet)
	}

	testSrc := map[string][]byte{
		flowfilePkg: []byte(
			`data, err := os.ReadFile(filepath.Join(repoRoot(), "examples", "approval-gate", "workflow.yaml"))`),
		m + "/pkg/flowstate/v1/secrets": []byte(`func TestUnrelated(t *testing.T) {}`),
	}
	for _, ip := range exampleDataDepPackages(testSrc) {
		changedSet[ip] = true
	}

	pkgs := []pkgMeta{
		{ImportPath: flowfilePkg},
		{ImportPath: m + "/pkg/flowstate/v1/secrets"},
		{ImportPath: m + "/pkg/flowstate/v1/engine", Deps: []string{flowfilePkg}},
	}
	affected := affectedPackages(pkgs, changedSet)
	if !contains(affected, flowfilePkg) {
		t.Errorf("flowfile must be affected by an examples/approval-gate change; affected = %v", affected)
	}
	// And it carries the rest of the import graph with it, same as any
	// other changed package would.
	if !contains(affected, m+"/pkg/flowstate/v1/engine") {
		t.Errorf("a package importing flowfile must also be affected; affected = %v", affected)
	}
	if contains(affected, m+"/pkg/flowstate/v1/secrets") {
		t.Errorf("an unrelated package must not be swept in; affected = %v", affected)
	}
}

// TestExampleDataDepPackagesIsNotEveryPackage is the negative direction: the
// data-dependency seed only ever adds packages whose own test files reach
// into examples/, never the whole package universe. An over-broad fix that
// marked everything affected on any examples/ change would pass the
// reproduction test above while destroying the reason the gate is
// diff-scoped at all.
func TestExampleDataDepPackagesIsNotEveryPackage(t *testing.T) {
	t.Parallel()

	const m = modulePath
	testSrc := map[string][]byte{
		m + "/pkg/flowstate/v1/flowfile": []byte(`os.ReadFile(filepath.Join(root, "examples", "approval-gate", "workflow.yaml"))`),
	}
	// A large, otherwise-unrelated package universe: none of these have any
	// test source in testSrc, so none of them may be pulled in.
	pkgs := []pkgMeta{
		{ImportPath: m + "/pkg/flowstate/v1/flowfile"},
		{ImportPath: m + "/pkg/flowstate/v1/secrets"},
		{ImportPath: m + "/pkg/flowstate/v1/engine"},
		{ImportPath: m + "/pkg/flowstate/v1/auth"},
		{ImportPath: m + "/cmd/flow"},
		{ImportPath: m + "/pkg/flowstate/v1/netpolicy"},
	}

	deps := exampleDataDepPackages(testSrc)
	if len(deps) != 1 || deps[0] != m+"/pkg/flowstate/v1/flowfile" {
		t.Fatalf("exampleDataDepPackages = %v, want exactly the flowfile package", deps)
	}

	changedSet := map[string]bool{}
	for _, ip := range deps {
		changedSet[ip] = true
	}
	affected := affectedPackages(pkgs, changedSet)
	want := []string{m + "/pkg/flowstate/v1/flowfile"}
	if !reflect.DeepEqual(affected, want) {
		t.Errorf("seeding from examples/ data deps must not sweep in unrelated packages; affected = %v, want %v",
			affected, want)
	}
}

// TestDocsOnlyDiffReachesTheDocumentationReaders is the #708 shape of the same
// defect, found in review of #820: docs/README.md maps to no Go package, so
// resolveDirs yields nothing and the local gate — which scopes its vet and test
// legs to the affected set — ran neither, while the tests that would have
// caught the diff (TestTheDocsIndexListsEveryDocument and
// TestInternalDocumentsSayTheyAreInternal, both in cmd/flow) sat there unrun.
// An index whose completeness check only ever runs in CI is exactly the round
// trip the pre-push tier exists to remove.
//
// The literal below is the real one: cmd/flow/docsindex_test.go's docsDir.
func TestDocsOnlyDiffReachesTheDocumentationReaders(t *testing.T) {
	t.Parallel()

	const m = modulePath
	cmdFlow := m + "/cmd/flow"

	p := buildPlan([]string{"docs/README.md"})
	if !p.repoTestData {
		t.Fatal("a change to a Markdown file under docs/ must set p.repoTestData")
	}

	changedSet := map[string]bool{}
	for _, ip := range resolveDirs(p.fileDirs, map[string]string{}) {
		changedSet[ip] = true
	}
	if len(changedSet) != 0 {
		t.Fatalf("docs/ has no Go package ancestor; changedSet = %v", changedSet)
	}

	testSrc := map[string][]byte{
		cmdFlow:                        []byte(`const docsDir = "../../docs"`),
		m + "/pkg/flowstate/v1/engine": []byte(`func TestPolicy(t *testing.T) {}`),
	}
	for _, ip := range repoDataDepPackages(testSrc, p.repoDataRoots) {
		changedSet[ip] = true
	}

	pkgs := []pkgMeta{
		{ImportPath: cmdFlow},
		{ImportPath: m + "/pkg/flowstate/v1/engine"},
	}
	affected := affectedPackages(pkgs, changedSet)
	if !contains(affected, cmdFlow) {
		t.Errorf("a docs/README.md-only diff must reach cmd/flow, which holds the index tests; affected = %v", affected)
	}
	if contains(affected, m+"/pkg/flowstate/v1/engine") {
		t.Errorf("an unrelated package must not be swept in; affected = %v", affected)
	}
}

// TestRepoDataDepPackages is a direct test of the pattern, over the literal
// shapes actually in the tree, plus the two ways it must not match: the word
// in the middle of another path, and a package that merely mentions a document
// in prose.
func TestRepoDataDepPackages(t *testing.T) {
	t.Parallel()

	const m = modulePath
	testSrc := map[string][]byte{
		// cmd/flow/docsindex_test.go and cmd/flow/docs_test.go.
		m + "/cmd/flow": []byte("const docsDir = \"../../docs\"\nconst referenceDir = \"../../docs/reference\""),
		// pkg/flowstate/v1/flowfile/readme_test.go.
		m + "/pkg/flowstate/v1/flowfile": []byte(`os.ReadFile(filepath.Join(root, "docs", "ARCHITECTURE.md"))`),
		// pkg/flowstate/v1/agentsmd_test.go.
		m + "/pkg/flowstate/v1": []byte(`os.ReadFile("../../../AGENTS.md")`),
		// A doc comment naming a document, with no test reading one:
		// prose is not a data dependency.
		m + "/pkg/flowstate/v1/engine": []byte(`// The retry defaults docs/DSL.md describes.`),
		// "docs" as a segment in the middle of an unrelated path, the
		// shape that made exampleDataDepPattern anchor to the quote.
		m + "/pkg/flowstate/v1/netpolicy": []byte(`golden := "testdata/docs/fixture.json"`),
	}

	got := repoDataDepPackages(testSrc, []string{"AGENTS.md", "README.md", "docs"})
	want := []string{
		m + "/cmd/flow",
		m + "/pkg/flowstate/v1",
		m + "/pkg/flowstate/v1/flowfile",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("repoDataDepPackages = %v, want %v", got, want)
	}
}

// TestRepoDataDepPackagesIsNotEveryPackage is the negative direction, the same
// property TestExampleDataDepPackagesIsNotEveryPackage pins for examples/: a
// documentation change must not turn the diff-scoped gate into a full run.
func TestRepoDataDepPackagesIsNotEveryPackage(t *testing.T) {
	t.Parallel()

	const m = modulePath
	testSrc := map[string][]byte{
		m + "/cmd/flow":                   []byte(`const docsDir = "../../docs"`),
		m + "/pkg/flowstate/v1/secrets":   []byte(`func TestSecrets(t *testing.T) {}`),
		m + "/pkg/flowstate/v1/engine":    []byte(`func TestPolicy(t *testing.T) {}`),
		m + "/pkg/flowstate/v1/auth":      []byte(`func TestAuth(t *testing.T) {}`),
		m + "/pkg/flowstate/v1/netpolicy": []byte(`func TestNet(t *testing.T) {}`),
	}

	deps := repoDataDepPackages(testSrc, []string{"docs"})
	if len(deps) != 1 || deps[0] != m+"/cmd/flow" {
		t.Fatalf("repoDataDepPackages = %v, want exactly cmd/flow", deps)
	}
}

// TestRepoDataRootsScopeTheSeed is why the roots are recorded per diff rather
// than folded into one fixed pattern. pkg/flowstate/v1's tests read AGENTS.md,
// and most of this module imports that package — so seeding AGENTS.md's readers
// on a documentation change would expand a docs edit into a near-full run for a
// dependency it does not have. Each root reaches its own readers and no others.
func TestRepoDataRootsScopeTheSeed(t *testing.T) {
	t.Parallel()

	const m = modulePath
	testSrc := map[string][]byte{
		m + "/cmd/flow":         []byte(`const docsDir = "../../docs"`),
		m + "/pkg/flowstate/v1": []byte(`os.ReadFile("../../../AGENTS.md")`),
	}

	for _, tc := range []struct {
		changed string
		root    string
		want    string
	}{
		{changed: "docs/DEPLOYMENT.md", root: "docs", want: m + "/cmd/flow"},
		{changed: "AGENTS.md", root: "AGENTS.md", want: m + "/pkg/flowstate/v1"},
	} {
		t.Run(tc.changed, func(t *testing.T) {
			p := buildPlan([]string{tc.changed})
			if !reflect.DeepEqual(p.repoDataRoots, []string{tc.root}) {
				t.Fatalf("buildPlan(%q).repoDataRoots = %v, want [%q]", tc.changed, p.repoDataRoots, tc.root)
			}

			deps := repoDataDepPackages(testSrc, p.repoDataRoots)
			if !reflect.DeepEqual(deps, []string{tc.want}) {
				t.Errorf("repoDataDepPackages = %v, want [%q]", deps, tc.want)
			}
		})
	}
}

// TestPluginSkipNotices covers the second #589 shape: a plugin module's own
// tests read its shipped example under examples/plugins/<name>/, one Go
// module further out than this gate's `go list` walk reaches, so the notice
// has to come from checking the name against a real module rather than the
// import graph.
func TestPluginSkipNotices(t *testing.T) {
	t.Parallel()

	exists := func(mod string) bool { return mod == "plugins/vcs" || mod == "plugins/sql" }

	t.Run("a real module's example is folded in with its own reason", func(t *testing.T) {
		t.Parallel()
		p := plan{examplePluginNames: []string{"vcs"}}
		got := pluginSkipNotices(p, exists)
		want := []string{"plugins/vcs (via examples/plugins/vcs data dependency, not a change under plugins/)"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("pluginSkipNotices = %v, want %v", got, want)
		}
	})

	t.Run("a name with no matching module is dropped, not guessed at", func(t *testing.T) {
		t.Parallel()
		// examples/plugins/greet has no plugins/greet module.
		p := plan{examplePluginNames: []string{"greet"}}
		got := pluginSkipNotices(p, exists)
		if len(got) != 0 {
			t.Errorf("pluginSkipNotices = %v, want none: greet names no real plugin module", got)
		}
	})

	t.Run("a module already named by a direct change is not duplicated", func(t *testing.T) {
		t.Parallel()
		p := plan{plugins: []string{"plugins/vcs"}, examplePluginNames: []string{"vcs"}}
		got := pluginSkipNotices(p, exists)
		want := []string{"plugins/vcs"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("pluginSkipNotices = %v, want %v (no duplicate entry)", got, want)
		}
	})

	t.Run("no plugin signal at all yields no notices", func(t *testing.T) {
		t.Parallel()
		if got := pluginSkipNotices(plan{}, exists); len(got) != 0 {
			t.Errorf("pluginSkipNotices = %v, want none", got)
		}
	})
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
