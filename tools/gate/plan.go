package main

import (
	"path"
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

	// Conditional legs, keyed on changed paths.
	proto    bool // buf lint/breaking/generate + descriptorset pin
	docs     bool // reference mirror + generated docs drift
	examples bool // flow fix --check, flow test, flow breaking

	// appearance means the diff could move a recorded golden: styled
	// output and help text live in cmd/flow, and the goldens embed a
	// sample Flowfile stamped with CurrentEdition, so an edition bump
	// moves them without touching a line of styling code (#483).
	appearance bool

	// plugins are the plugin modules with changes (e.g. "plugins/openai").
	// They are separate Go modules, reported rather than gated here.
	plugins []string

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

	reason := func(leg, path string) {
		if _, ok := p.reasons[leg]; !ok {
			p.reasons[leg] = path
		}
	}

	for _, f := range changed {
		f = path.Clean(f)

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

		if strings.HasPrefix(f, "examples/") {
			p.examples = true
			reason("examples", f)
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
	sort.Strings(p.plugins)
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

func contains(paths []string, want string) bool {
	for _, ip := range paths {
		if ip == want {
			return true
		}
	}
	return false
}
