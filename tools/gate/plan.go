package main

import (
	"path"
	"regexp"
	"sort"
	"strings"
)

// modulePath is this repository's module, used to name in-module packages.
// A repo-local tool may know the repo it lives in; nothing outside this
// module ever runs it.
const modulePath = "github.com/picatz/flowstate"

// flowtestPkg is the package whose every claim is an ordering claim (see
// CLAUDE.md on `-cpu=1`): when it is affected, the gate runs the ordering leg.
const flowtestPkg = modulePath + "/pkg/flowstate/v1/flowtest"

// cmdFlowPkg is the CLI whose binary generates docs/reference/: when it is
// affected, its output can have moved. See needsDocs.
const cmdFlowPkg = modulePath + "/cmd/flow"

// plan is what the changed-file list alone decides: which conditional legs
// fire, which files gofmt checks, and which directories feed the
// file-to-package resolution. It is pure so the mapping is testable without
// git or a Go build list; everything needing `go list` happens afterwards in
// resolveDirs and affectedPackages.
type plan struct {
	// goFiles are the changed .go files, repo-relative slash paths, for
	// `gofmt -l`. Deleted files are filtered at run time, not here.
	goFiles []string

	// fileDirs are the directories holding changed files, deduplicated,
	// for resolution to packages. Plugin-module files are excluded: they
	// belong to other Go modules this gate does not walk.
	fileDirs []string

	// moduleWide means go.mod or go.sum changed, so the dependency graph
	// itself moved and every package is treated as affected.
	moduleWide bool

	// ciWide means a file changed that decides what the verification tiers
	// themselves do: a workflow, the Makefile, or this gate's own source.
	// A plan cannot reason about the effect of a change to the thing
	// computing the plan, so CI runs everything. It is deliberately
	// separate from moduleWide: that one widens the *affected package set*
	// and is a fact about the Go build graph, while this one widens the
	// *job set* and is a fact about the harness. A workflow edit affects no
	// Go package at all, which is exactly why it needs its own answer.
	ciWide bool

	// Conditional legs, keyed on changed paths.
	proto    bool // buf lint/breaking/generate + descriptorset pin
	docs     bool // reference mirror + generated docs drift
	examples bool // flow fix --check, flow test, flow breaking

	// repoTestData means a repository-level file some test reads with
	// os.ReadFile, rather than imports, changed: README.md, AGENTS.md, or
	// any Markdown under docs/. None is a Go source file and none is under
	// examples/, so nothing above puts any of them in front of the import
	// graph or the #589 data-dependency seeding — but
	// cmd/flow/commands_test.go reads README.md's command table,
	// pkg/flowstate/v1/flowfile/readme_test.go compiles the Flowfiles
	// embedded in README.md and docs/ARCHITECTURE.md,
	// pkg/flowstate/v1/agentsmd_test.go reads AGENTS.md, and
	// cmd/flow/docs_test.go reads and validates every file under
	// docs/reference/. A change to any of them can make the test that reads
	// it fail or go stale without moving a single Go file, the same shape
	// #589 already named for examples/, one file further out than that fix
	// reached.
	//
	// It covers docs/ as a whole rather than the two pages named above
	// because the documentation set now has a test about the *set*:
	// cmd/flow/docsindex_test.go fails when a document under docs/ is
	// added, renamed or removed without docs/README.md moving with it
	// (#708). Every Markdown file in that tree is therefore test data, and
	// enumerating the ones that happen to be read today is how this rule
	// would go stale the next time a page is added.
	//
	// It widens ciDecisions' testRun — the CI job wide enough to run all of
	// these — and, since #820's review, seeds the local tier's affected set
	// as well, through the packages repoDataRoots names. It has no leg of
	// its own in either tier.
	repoTestData bool

	// repoDataRoots names which of those roots this diff actually touched
	// ("docs", "README.md", "AGENTS.md"), so the local tier can seed the
	// packages that read *those* rather than every package that reads any
	// of them. Without it a docs/DEPLOYMENT.md edit would pull in
	// pkg/flowstate/v1 because its tests read AGENTS.md — true, unrelated,
	// and expensive, since most of the module imports it. Sorted and
	// deduplicated; empty exactly when repoTestData is false.
	repoDataRoots []string

	// appearance means the diff could move a recorded golden: styled
	// output and help text live in cmd/flow, and the goldens embed a
	// sample Flowfile stamped with CurrentEdition, so an edition bump
	// moves them without touching a line of styling code (#483).
	appearance bool

	// plugins are the plugin modules with changes (e.g. "plugins/openai").
	// They are separate Go modules, reported rather than gated here.
	plugins []string

	// examplePluginNames are the second path segments of any changed
	// examples/plugins/<name>/ file (e.g. "vcs" for
	// examples/plugins/vcs/workflow.yaml). Each plugin module's own
	// *_test.go reads its shipped example the same way the root module's
	// tests read examples/ (plugins/vcs/reachable/reachable_test.go opens
	// examples/plugins/vcs/workflow.yaml with os.ReadFile), so this is the
	// same #589 data dependency, one Go module further out than this gate
	// can walk with `go list`. run() checks each name against the plugin
	// modules that actually exist before folding it into the plugins skip
	// notice — not every examples/plugins/<name> names a plugin module
	// (examples/plugins/greet is pkg/flowstate/v1/plugin's in-repo fixture,
	// not a separate module), and buildPlan has no filesystem to check that
	// with.
	examplePluginNames []string

	// reasons maps a leg name to the first changed path that triggered it,
	// so the output can say why a leg ran.
	reasons map[string]string
}

// buildPlan maps changed files (repo-relative slash paths, as git prints
// them) to the plan above.
func buildPlan(changed []string) plan {
	p := plan{reasons: map[string]string{}}
	dirSeen := map[string]bool{}
	pluginSeen := map[string]bool{}
	rootSeen := map[string]bool{}

	reason := func(leg, path string) {
		if _, ok := p.reasons[leg]; !ok {
			p.reasons[leg] = path
		}
	}

	for _, f := range changed {
		f = path.Clean(f)

		// The harness files decide what runs. Noted before anything else
		// and without `continue`, because a workflow change is also an
		// ordinary file change: tools/gate/ci.go is a Go file in a Go
		// package, and the gate's own tests should still run for it.
		if strings.HasPrefix(f, ".github/workflows/") || f == "Makefile" || strings.HasPrefix(f, "tools/gate/") {
			p.ciWide = true
			reason("ci", f)
		}

		// The module files move the whole graph.
		if f == "go.mod" || f == "go.sum" {
			p.moduleWide = true
			reason("module", f)
			continue
		}

		// Plugin modules are separate modules: `go list ./...` from the
		// root cannot see them, so mapping their files to root-module
		// packages would silently resolve to a wrong ancestor.
		if strings.HasPrefix(f, "plugins/") {
			parts := strings.SplitN(f, "/", 3)
			if len(parts) >= 2 {
				mod := parts[0] + "/" + parts[1]
				if !pluginSeen[mod] {
					pluginSeen[mod] = true
					p.plugins = append(p.plugins, mod)
				}
			}
			continue
		}

		if strings.HasSuffix(f, ".go") {
			p.goFiles = append(p.goFiles, f)
		}

		if d := path.Dir(f); !dirSeen[d] {
			dirSeen[d] = true
			p.fileDirs = append(p.fileDirs, d)
		}

		// proto/: the schema is a public contract; regenerate and re-pin.
		if strings.HasPrefix(f, "proto/") || f == "buf.gen.yaml" || f == "buf.work.yaml" {
			p.proto = true
			reason("proto", f)

			// And the schema is a *docs* source too, which is not
			// obvious and is why this is spelled out rather than
			// left to the switch below. `flow docs generate`
			// derives a task's field names, types, required-ness
			// and bounds from the protovalidate rules on the
			// schema-backed descriptors, and derives the MCP tool
			// list by walking the service descriptor. So a
			// proto-only edit that adds a field, tightens a
			// constraint or adds an RPC moves docs/reference/
			// without touching a single .go file. Triggering only
			// the proto leg there would let the gate pass with
			// stale reference docs.
			p.docs = true
			reason("docs", f)
		}

		// The derived-docs surfaces: docs/DSL.md and the example
		// workflows feed the reference mirror (`go generate
		// ./cmd/flow/internal/reference`); the cobra tree, the MCP tool
		// table and docsgen live in cmd/flow; the task registry lives in
		// the top-level pkg/flowstate/v1 package. Any of them changing
		// can move docs/reference/ or the mirror, so the docs leg
		// regenerates both and pins the result.
		//
		// These path rules are a fast, testable approximation. The
		// authoritative trigger is in run(): `flow docs generate` runs
		// the cmd/flow binary, so anything in that binary's transitive
		// dependency closure is a docs source, and run() fires this leg
		// whenever cmd/flow lands in the affected set.
		switch {
		case f == "docs/DSL.md":
			p.docs = true
			reason("docs", f)
		case strings.HasPrefix(f, "examples/") && strings.HasSuffix(f, "/workflow.yaml"):
			p.docs = true
			reason("docs", f)
		case strings.HasPrefix(f, "cmd/flow/") && strings.HasSuffix(f, ".go"):
			p.docs = true
			reason("docs", f)
		case path.Dir(f) == "pkg/flowstate/v1" && strings.HasSuffix(f, ".go"):
			p.docs = true
			reason("docs", f)
		}

		// Repository-level files read directly by tests rather than
		// imported, the same #589 shape examples/ has — see
		// p.repoTestData's doc.
		if root := repoDataRoot(f); root != "" {
			p.repoTestData = true
			reason("test", f)
			if !rootSeen[root] {
				rootSeen[root] = true
				p.repoDataRoots = append(p.repoDataRoots, root)
			}
		}

		if strings.HasPrefix(f, "examples/") {
			p.examples = true
			reason("examples", f)
		}

		// examples/plugins/<name>/... is a plugin module's shipped
		// example, read by that module's own tests the same way
		// exampleDataDepPattern's root-module targets are — see
		// examplePluginNames' doc comment.
		if strings.HasPrefix(f, "examples/plugins/") {
			parts := strings.SplitN(f, "/", 4)
			if len(parts) >= 3 && !pluginSeen["examples/plugins/"+parts[2]] {
				pluginSeen["examples/plugins/"+parts[2]] = true
				p.examplePluginNames = append(p.examplePluginNames, parts[2])
			}
		}

		// Anything that could change what the CLI prints, which the
		// appearance goldens record.
		if strings.HasPrefix(f, "cmd/flow/") && strings.HasSuffix(f, ".go") ||
			f == "pkg/flowstate/v1/flowfile/edition.go" {
			p.appearance = true
			reason("appearance", f)
		}
	}

	sort.Strings(p.goFiles)
	sort.Strings(p.fileDirs)
	sort.Strings(p.examplePluginNames)
	sort.Strings(p.plugins)
	sort.Strings(p.repoDataRoots)
	return p
}

// resolveDirs maps each changed-file directory to the import path of the
// package that owns it, walking upward so a file under a package's testdata/
// (or any non-package subdirectory) lands on the package whose tests read it.
// A directory with no package ancestor (docs/, .github/, the repo root) maps
// to nothing. pkgByDir indexes repo-relative slash directory to import path;
// injecting it is what makes this testable without running `go list`.
func resolveDirs(dirs []string, pkgByDir map[string]string) []string {
	seen := map[string]bool{}
	var out []string
	for _, d := range dirs {
		for d != "" && d != "/" {
			if ip, ok := pkgByDir[d]; ok {
				if !seen[ip] {
					seen[ip] = true
					out = append(out, ip)
				}
				break
			}
			if d == "." {
				break
			}
			d = path.Dir(d)
		}
	}
	sort.Strings(out)
	return out
}

// hasUnresolvedGoDir reports whether any of goFiles' own directories is
// missing from byDir — a changed .go file whose package `go list` cannot find
// on the tree as it was checked out.
//
// Deliberately narrower than [resolveDirs]: that function walks upward from a
// changed path looking for the nearest package ancestor, which is right for a
// file under testdata/ but wrong for asking whether *this exact directory* is
// still a package. A .go file's own directory has to be its package's
// directory — Go does not let one recurse into a subdirectory — so a direct,
// non-walking lookup is what actually answers "did this file's package
// disappear," most often because this diff deleted the last source file a
// directory had. See the caller in analyse() for why that case is treated the
// same conservative way p.moduleWide already is.
func hasUnresolvedGoDir(goFiles []string, byDir map[string]string) bool {
	for _, f := range goFiles {
		if _, ok := byDir[path.Dir(f)]; !ok {
			return true
		}
	}
	return false
}

// pkgMeta is the slice of `go list -json` this gate reads.
type pkgMeta struct {
	ImportPath   string
	Dir          string
	Deps         []string
	TestImports  []string
	XTestImports []string
}

// affectedPackages returns the import paths of every listed package whose
// build or tests can see a changed package.
//
// Mechanism: one `go list -e -json=... ./...` walk feeds this (a single
// process), not one `go list -deps` per package. `.Deps` is already the
// transitive closure of a package's non-test imports, so a single membership
// check per package covers every import chain. Test imports are direct-only,
// so each direct test import T is checked the same way: P is affected when T
// itself changed or T's transitive Deps reach a change. Affectedness does not
// then propagate further through T: a package importing P's *code* does not
// run P's tests, so nothing more than this one test-import hop exists to
// close over.
func affectedPackages(pkgs []pkgMeta, changed map[string]bool) []string {
	if len(changed) == 0 {
		return nil
	}
	byPath := make(map[string]*pkgMeta, len(pkgs))
	for i := range pkgs {
		byPath[pkgs[i].ImportPath] = &pkgs[i]
	}
	anyChanged := func(paths []string) bool {
		for _, p := range paths {
			if changed[p] {
				return true
			}
		}
		return false
	}
	reaches := func(ip string) bool {
		if changed[ip] {
			return true
		}
		if m, ok := byPath[ip]; ok {
			return anyChanged(m.Deps)
		}
		return false
	}

	var out []string
	for i := range pkgs {
		p := &pkgs[i]
		hit := reaches(p.ImportPath)
		if !hit {
			for _, t := range p.TestImports {
				if reaches(t) {
					hit = true
					break
				}
			}
		}
		if !hit {
			for _, t := range p.XTestImports {
				if reaches(t) {
					hit = true
					break
				}
			}
		}
		if hit {
			out = append(out, p.ImportPath)
		}
	}
	sort.Strings(out)
	return out
}

// needsOrdering reports whether the affected set includes the flowtest
// package, whose ordering claims get the dedicated `-cpu=1 -count=20` leg.
func needsOrdering(affected []string) bool {
	return contains(affected, flowtestPkg)
}

// needsStyle reports whether the affected set reaches the cmd/flow package,
// which is what decides the style lint over examples/ as well.
//
// The same package question needsDocs asks, and it is a separate function
// because the two legs answer different questions that happen to share a
// trigger today: `flow lint` runs the binary, so its real source set is that
// binary's dependency closure — a check added under
// pkg/flowstate/v1/flowfile moves what the leg reports without touching a
// single file under examples/, and a path rule naming examples/ alone would
// skip the leg on precisely the diff that changed the rules.
func needsStyle(affected []string) bool {
	return contains(affected, cmdFlowPkg)
}

// needsDocs reports whether the affected set reaches the cmd/flow package.
//
// This is the authoritative docs trigger, and it is a package question
// rather than a path one because `flow docs generate` *runs the binary*:
// the true source set of docs/reference/ is cmd/flow's transitive
// dependency closure, which affectedPackages already computes. Any change
// that reaches the binary can move its output, including ones no path rule
// would think to name — a task registered from a subpackage, a diagnostic
// code's text, a generated .pb.go that changed a field's required-ness.
// buildPlan's path rules stay as the fast approximation and as what the
// unit tests pin; this closes over everything they cannot see.
func needsDocs(affected []string) bool {
	return contains(affected, cmdFlowPkg)
}

// exampleDataDepPattern matches a Go string literal that opens onto
// examples/ at the repository root: a leading double quote, zero or more
// "../" segments climbing back to the repository root, then "examples" and
// either a path separator or the closing quote. That covers both literal
// shapes found across this repository's example-reading tests — an inline
// relative path ("../../../../examples/approval-gate/workflow.yaml") and
// the bare "examples" argument a test hands to filepath.Join alongside the
// rest of the path (filepath.Join(root, "examples", "approval-gate",
// "workflow.yaml")) — which is a data dependency the Go import graph cannot
// see, because nothing is ever imported (#589: a changed value inside
// examples/approval-gate/workflow.yaml broke
// TestApprovalGateDomainIsInferable in pkg/flowstate/v1/flowfile, and the
// import-graph mapper had no changed package to expand from).
//
// The quote anchor is deliberate, not decorative: an earlier version of
// this pattern matched the bare substring "examples/" anywhere, which
// caught doc comments quoting an unrelated path in prose
// (pkg/flowstate/v1/flowtest's comment about
// "examples/call-a-workflow/workflow.test.yaml" describing a bug, with no
// code anywhere near it that reads the real corpus) and a path that merely
// contains "examples/" as a directory named *inside* something else
// (tools/hooks/genguard's fixture path
// "cmd/flow/internal/reference/mirror/examples/hello.yaml", a directory
// literally named "examples" one level under the reference mirror, not the
// repository's example corpus at all). Anchoring the match to right after
// the opening quote — allowing only "../" between — accepts a path that
// *starts* at examples/ from wherever the test's working directory is, and
// rejects one where "examples" is a substring in the middle of an unrelated
// path.
var exampleDataDepPattern = regexp.MustCompile(`"(\.\./)*examples(/|")`)

// exampleDataDepPackages reports which of testSrc's packages have a test
// file reaching into examples/, matched by exampleDataDepPattern. testSrc
// maps an import path to the concatenated contents of that package's
// _test.go files (both white-box and black-box); production builds it from
// disk in readTestSources, and it is injected here (the same shape
// resolveDirs takes pkgByDir) so the mapping is testable without a
// filesystem walk.
//
// This only ever adds packages to a changed set the caller already knows is
// examples-triggered (buildPlan's p.examples); it does not run on every
// diff, so it cannot turn an unrelated change into "everything affected" —
// see TestExampleDataDepPackagesIsNotEveryPackage.
func exampleDataDepPackages(testSrc map[string][]byte) []string {
	return dataDepPackages(testSrc, exampleDataDepPattern)
}

// repoDataRoot reports which repository-level data root a changed path belongs
// to, or "" for a path that is not test data of this kind. The three roots are
// the ones p.repoTestData's doc lists; docs/ is a tree, the other two are
// single files.
func repoDataRoot(f string) string {
	switch {
	case f == "README.md", f == "AGENTS.md":
		return f
	case strings.HasPrefix(f, "docs/") && strings.HasSuffix(f, ".md"):
		return "docs"
	default:
		return ""
	}
}

// repoDataDepPattern builds the literal matcher for the roots a diff actually
// touched. Same anchoring rule as exampleDataDepPattern, and for the same
// reason — a literal that *opens* onto a root (allowing only "../" segments
// climbing back to the repository root) is a test reading it, while the same
// word in the middle of another path, or in a doc comment, is not.
//
// The literal shapes it has to cover are the ones in the tree today:
// cmd/flow/docsindex_test.go's `const docsDir = "../../docs"`,
// cmd/flow/docs_test.go's `"../../docs/reference"`, and the README.md and
// AGENTS.md paths cmd/flow/commands_test.go,
// pkg/flowstate/v1/flowfile/readme_test.go and
// pkg/flowstate/v1/agentsmd_test.go open.
//
// It is per-root rather than one fixed pattern because the roots have very
// different blast radii: pkg/flowstate/v1's tests read AGENTS.md, and most of
// this module imports that package, so folding AGENTS.md's readers into a
// docs-only diff would turn a documentation edit into a near-full run for a
// dependency it does not have.
//
// This is the seeding half of what p.repoTestData already did for CI. Before
// #708's docs index there was nothing under docs/ whose *content* a test
// asserted about beyond docs/reference/, so widening CI's test job was
// enough. TestTheDocsIndexListsEveryDocument changed that: a diff adding
// docs/guides/thing.md and not listing it in docs/README.md fails that test,
// maps to no Go package at all, and so — without this — reached no local gate
// leg, leaving the author to find out from CI what the pre-push gate exists
// to tell them.
func repoDataDepPattern(roots []string) *regexp.Regexp {
	alternatives := make([]string, 0, len(roots))
	for _, root := range roots {
		if root == "docs" {
			// A tree: "docs" itself (the bare segment handed to
			// filepath.Join) or anything under it.
			alternatives = append(alternatives, `docs(/|")`)
			continue
		}

		alternatives = append(alternatives, regexp.QuoteMeta(root)+`"`)
	}

	return regexp.MustCompile(`"(\.\./)*(` + strings.Join(alternatives, "|") + `)`)
}

// repoDataDepPackages reports which of testSrc's packages read the
// repository-level data roots this diff touched. Like exampleDataDepPackages it
// only ever runs on a diff the plan already flagged, so it cannot widen an
// unrelated change — see TestRepoDataDepPackagesIsNotEveryPackage.
func repoDataDepPackages(testSrc map[string][]byte, roots []string) []string {
	if len(roots) == 0 {
		return nil
	}

	return dataDepPackages(testSrc, repoDataDepPattern(roots))
}

// dataDepPackages is the shared loop: every package in testSrc whose test
// sources match pat, sorted so the notice a gate prints is stable.
func dataDepPackages(testSrc map[string][]byte, pat *regexp.Regexp) []string {
	var out []string
	for ip, src := range testSrc {
		if pat.Match(src) {
			out = append(out, ip)
		}
	}
	sort.Strings(out)
	return out
}

// pluginSkipNotices builds the "plugins" leg's skip message entries: every
// plugin module p.plugins already names, plus (per p.examplePluginNames)
// any plugin module whose shipped example changed without the module's own
// files changing. moduleExists reports whether "plugins/<name>" is a real
// module (checked against plugins/<name>/go.mod on disk in production,
// injected here so this is testable without a filesystem) — not every
// examples/plugins/<name> does; examples/plugins/greet names
// pkg/flowstate/v1/plugin's in-repo fixture, not a separate module.
//
// The two sources are distinguished in the text (see main's call site) so
// the same "say why" rule the affected-set notice follows also applies
// here: a reader can tell a module changed directly from one whose only
// signal was its shipped example moving.
func pluginSkipNotices(p plan, moduleExists func(mod string) bool) []string {
	notices := append([]string{}, p.plugins...)
	for _, name := range p.examplePluginNames {
		mod := "plugins/" + name
		if contains(notices, mod) {
			continue
		}
		if moduleExists(mod) {
			notices = append(notices,
				mod+" (via examples/plugins/"+name+" data dependency, not a change under plugins/)")
		}
	}
	return notices
}

func contains(paths []string, want string) bool {
	for _, ip := range paths {
		if ip == want {
			return true
		}
	}
	return false
}
