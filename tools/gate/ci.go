package main

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
)

// v1Pkg is the root v1 package. It keeps a name here because tests name it
// directly.
const v1Pkg = modulePath + "/pkg/flowstate/v1"

// decision is one job in .github/workflows/ci.yml and whether this diff can
// reach it. Why is carried for both answers, because a skip nobody can read is
// indistinguishable from a gap — the same rule the local tier's leg lines follow.
type decision struct {
	// Job is the job's key in ci.yml. ciJobsInWorkflow (ci_test.go) pins
	// this list against the workflow file, so a job added to one and not
	// the other fails a test here rather than passing silently in CI.
	Job string

	// Output is the name this decision is published under in
	// $GITHUB_OUTPUT. Job names may contain '-'; workflow expressions read
	// `needs.plan.outputs.x` as a property access and would parse a hyphen
	// as subtraction, so the two names are not always the same string.
	Output string

	Run bool
	Why string
}

// ciDecisions maps a diff to the CI jobs it can reach.
//
// This is the same computation the local tier runs — buildPlan's path rules and
// affectedPackages' import-graph expansion — pointed at ci.yml's jobs instead of
// the local legs. That reuse is the whole point: CI recomputing "what can this
// diff reach" a second way, in YAML `paths:` filters, is one value written down
// twice, and the copy in YAML is the one that cannot be tested and drifts in
// silence.
//
// force, when non-empty, is a reason every job must run regardless of the diff:
// an event that is not a pull request (a push to main is the record of whether a
// commit was good when it landed), or a change to something that decides what CI itself does. A plan
// cannot reason about a change to the thing computing the plan.
func ciDecisions(p plan, affected []string, force string) []decision {
	goAffected := len(affected) > 0

	// test: the widest job, and the critical path. It builds, vets and tests
	// the root module, regenerates the reference docs, and runs the three
	// example checks and the compose parse — so anything Go, anything under
	// examples/ (which is where the compose file lives), the schema, and
	// the derived-docs sources all reach it.
	//
	// p.repoTestData is in the OR for the same #589 shape p.examples already
	// covers, one file further out: README.md and docs/ARCHITECTURE.md are
	// read directly by cmd/flow/commands_test.go and
	// pkg/flowstate/v1/flowfile/readme_test.go with os.ReadFile rather than
	// imported, so a change to either can make one of those tests fail or go
	// stale without moving a Go file, examples/, or proto/.
	//
	// A plugin-only diff is not in this OR any more. It used to be, because
	// this job was the only one that ran `make test-plugins`; that step has
	// a job of its own below, and this one — the five-minute root suite — is
	// what a plugin change should not have to wait for (#1726).
	testRun := goAffected || p.examples || p.docs || p.proto || p.repoTestData
	testWhy := "no Go package is affected, and nothing under examples/ or proto/, and none of the derived-docs sources or repository-level test data changed"
	switch {
	case goAffected:
		testWhy = fmt.Sprintf("%d affected package(s)", len(affected))
	case p.examples:
		testWhy = p.reasons["examples"] + " changed"
	case p.proto:
		testWhy = p.reasons["proto"] + " changed"
	case p.docs:
		testWhy = p.reasons["docs"] + " changed"
	case p.repoTestData:
		testWhy = p.reasons["test"] + " changed, and it is read directly by tests rather than imported"
	}

	// test-plugins: `make test-plugins` and `make plugin-examples`, off the
	// critical path since #1726 but on every trigger the root suite has bar
	// the docs and repository-data ones, plus the one only it has.
	//
	// p.plugins is here for the reason a path filter could not express: a
	// plugin module is a separate Go module, so a diff touching only
	// plugins/<name>/ never lands in affected (go list ./... from the root
	// cannot see it) and touches none of examples/ or proto/ either. Before
	// this arm existed on the test job, exactly that diff reached no job at
	// all, and verdict accepted the skip: a plugin that does not compile could
	// merge on a PR whose only change was to that plugin.
	//
	// goAffected is here because the boundary runs the other way too: every
	// plugin module replaces github.com/picatz/flowstate with ../.., so a
	// root package that moved is a package a plugin may be compiled against,
	// and the root module's import graph cannot say which. p.examples and
	// p.proto for what plugin-examples reads: the reviewed catalog and the
	// examples it validates live under examples/plugins/, and the schema is
	// the contract the plugins' descriptors are compiled from. examples/ as a
	// whole rather than examples/plugins/ alone is a deliberate over-run — the
	// job is two minutes off the critical path, and the narrower rule is a
	// second path spelling for a directory the plan already names.
	pluginsRun := goAffected || len(p.plugins) > 0 || p.examples || p.proto
	pluginsWhy := "no Go package is affected, no plugin module changed, and nothing under examples/ or proto/ changed"
	switch {
	case len(p.plugins) > 0:
		pluginsWhy = strings.Join(p.plugins, ", ") + " changed, and make test-plugins is the only thing that builds/vets/tests it"
	case goAffected:
		pluginsWhy = fmt.Sprintf("%d affected package(s), which the plugin modules compile against through their replace directives", len(affected))
	case p.examples:
		pluginsWhy = p.reasons["examples"] + " changed, and plugin-examples validates examples/plugins/ against the reviewed catalog"
	case p.proto:
		pluginsWhy = p.reasons["proto"] + " changed, and the schema is the contract the plugins compile against"
	}

	// test-ordering: the flowtest package under -cpu=1, on the same trigger
	// the local tier's ordering leg has. p.examples is the #589 data
	// dependency the import graph cannot see — the package's fuzz seeds walk
	// examples/ off disk — which the local tier reaches by seeding the affected
	// set and CI, which does not seed, reaches by naming it here.
	orderingRun := needsOrdering(affected) || p.examples
	orderingWhy := "the flowtest package is not affected and nothing under examples/ changed"
	switch {
	case needsOrdering(affected):
		orderingWhy = "the flowtest package is affected, and its every claim is an ordering claim"
	case p.examples:
		orderingWhy = p.reasons["examples"] + " changed, and the flowtest package reads examples/ off disk"
	}

	// fuzz-smoke runs only where the plan is forced: a push to main, and a
	// change to CI, the gate or the module graph. A pull request that merely
	// touches code does not pay for 30s of mutation per target: every seed
	// corpus already runs as an ordinary test in the test lanes (`go test`
	// executes f.Add seeds without -fuzz), and main and deep.yml run the
	// mutation. The fuzz target files (tools/fuzztargets, tools/fuzzrun) are CI
	// configuration whose own change forces the full job set, so the job is
	// proved by the change that reconfigures it.
	fuzzWhy := "pull requests do not pay for mutation: seed corpora run in the test lanes, and main and the weekly deep tier fuzz"
	if force != "" {
		fuzzWhy = "the full set runs: " + force
	}

	decisions := []decision{
		{Job: "test", Output: "test", Run: testRun, Why: testWhy},
		{Job: "test-plugins", Output: "test_plugins", Run: pluginsRun, Why: pluginsWhy},
		{Job: "test-ordering", Output: "test_ordering", Run: orderingRun, Why: orderingWhy},

		// regen: `make regen-check`, which runs the binary and the plugin
		// builds, so it needs everything the root suite does; testRun already
		// includes p.proto, which is what the schema-only proto job reads.
		{Job: "regen", Output: "regen", Run: testRun, Why: testWhy},

		{Job: "proto", Output: "proto",
			Run: p.proto,
			Why: pick(p.proto, p.reasons["proto"]+" changed", "no changes under proto/ or to buf config")},

		// govulncheck and staticcheck both analyse ./... from the root
		// *and* walk each plugin module, so a diff that moves no Go
		// package and no plugin module cannot move what either reports.
		// What *can* move govulncheck's answer without a diff is the
		// advisory database, which it fetches when it runs — and that is
		// exactly why this skip is safe here and nowhere else: every push
		// to main runs the full set (see force),
		// and deep.yml runs govulncheck weekly on a schedule. A new
		// advisory therefore still arrives on a calendar rather than
		// waiting for someone to touch a .go file; it just stops being
		// reported against the pull request that renamed a heading.
		{Job: "vulncheck", Output: "vulncheck",
			Run: goAffected || len(p.plugins) > 0,
			Why: pick(goAffected || len(p.plugins) > 0, vulncheckWhy(affected, p),
				"no Go package is affected and no plugin module changed; main and the weekly deep tier still scan against a freshly fetched advisory database")},

		{Job: "staticcheck", Output: "staticcheck",
			Run: goAffected || len(p.plugins) > 0,
			Why: pick(goAffected || len(p.plugins) > 0, staticcheckWhy(affected, p),
				"no Go package is affected and no plugin module changed")},

		{Job: "fuzz-smoke", Output: "fuzz_smoke",
			Run: force != "",
			Why: fuzzWhy},

		{Job: "appearance", Output: "appearance",
			Run: needsAppearance(p, affected),
			Why: pick(needsAppearance(p, affected),
				appearanceWhy(p, affected),
				"no change reaches the binary whose printed output the goldens record")},
	}

	if force != "" {
		for i := range decisions {
			decisions[i].Run = true
			decisions[i].Why = force
		}
	}
	return decisions
}

// testLane is one entry of the test job's matrix in ci.yml. owns is tested in
// testLanes' order and rest owns everything, so every package of the root
// module lands in exactly one lane by construction: a package added anywhere
// runs in rest until it is given a lane of its own, and none is dropped.
type testLane struct {
	Name string
	owns func(importPath string) bool
}

// testLanes is the test matrix. Each named lane is a package, or a few, whose
// own run is long enough to want a runner (the measurements are in docs/CI.md);
// TestTestLanesPartitionTheModule pins the names against ci.yml's matrix and
// the partition against `go list ./...`.
var testLanes = []testLane{
	{"engine", func(ip string) bool { return ip == v1Pkg+"/engine" }},
	{"v1", func(ip string) bool { return ip == v1Pkg }},
	{"cli", func(ip string) bool { return ip == cmdFlowPkg }},
	{"heavy", func(ip string) bool {
		return ip == v1Pkg+"/flowfile" || strings.HasPrefix(ip, v1Pkg+"/flowfile/") ||
			ip == v1Pkg+"/server" || ip == v1Pkg+"/plugin" || ip == modulePath+"/internal/strictyaml"
	}},
	{"rest", func(string) bool { return true }},
}

// ciTestLanes assigns the packages each test lane runs, and says why.
//
// A pull request runs the affected set plus the tree readers (packages whose
// tests walk the source tree or run the go command, so a change they do not
// import can still fail them; see treeReaderPattern). Everything else — a
// forced plan, the data dependencies the import graph cannot see (examples/,
// the repository-level files tests read, and proto/, which the MCP schema
// tests read off disk), and a change in unowned, a directory no root package
// owns — runs every package, as it did before the lanes were narrowed. What
// is left unguarded is a test reading another package's files off disk.
//
// Every lane is always present in the result, as a list that may be empty, so
// the Test step can tell a lane the diff does not reach from a plan output it
// failed to read.
func ciTestLanes(p plan, all, affected, treeReaders []string, unowned, force string) (map[string][]string, string) {
	full := ""
	switch {
	case force != "":
		full = force
	case p.examples:
		full = p.reasons["examples"] + " changed, and tests read examples/ off disk"
	case p.repoTestData:
		full = p.reasons["test"] + " changed, and it is read directly by tests rather than imported"
	case p.proto:
		full = p.reasons["proto"] + " changed, and tests read the schema off disk"
	case unowned != "":
		full = "a file in " + unowned + " changed, which no Go package owns, so no import says which tests read it"
	}

	selected := all
	why := "every package: " + full
	if full == "" {
		set := map[string]bool{}
		for _, ip := range affected {
			set[ip] = true
		}
		for _, ip := range treeReaders {
			set[ip] = true
		}
		selected = slices.DeleteFunc(slices.Clone(all), func(ip string) bool { return !set[ip] })
		why = fmt.Sprintf("%d affected package(s) and %d tree reader(s), %d of %d package(s)",
			len(affected), len(treeReaders), len(selected), len(all))
	}

	lanes := make(map[string][]string, len(testLanes))
	for _, l := range testLanes {
		lanes[l.Name] = []string{}
	}
	for _, ip := range selected {
		lanes[laneOf(ip)] = append(lanes[laneOf(ip)], ip)
	}
	return lanes, why
}

// laneOf names the one lane that owns ip.
func laneOf(ip string) string {
	for _, l := range testLanes {
		if l.owns(ip) {
			return l.Name
		}
	}
	panic("rest owns every package")
}

// needsAppearance reports whether this diff can move a recorded golden.
//
// Two triggers, the same pair needsDocs has and for the same reason. buildPlan's
// path rules are the fast, unit-tested approximation; the authoritative question
// is a package one, because the goldens record what the cmd/flow *binary* prints,
// so its whole dependency closure is a source of that output. A diagnostic's
// wording, a task's rendered example, a width computed three packages down: none
// of those is a path rule anyone would think to write, and all of them change
// what a recording contains.
func needsAppearance(p plan, affected []string) bool {
	return p.appearance || contains(affected, cmdFlowPkg)
}

func appearanceWhy(p plan, affected []string) string {
	if p.appearance {
		return p.reasons["appearance"] + " changed"
	}
	if contains(affected, cmdFlowPkg) {
		return "cmd/flow is affected, so the binary whose output the goldens record may print differently"
	}
	return ""
}

func vulncheckWhy(affected []string, p plan) string {
	switch {
	case len(affected) > 0 && len(p.plugins) > 0:
		return fmt.Sprintf("%d affected package(s) and %s changed", len(affected), strings.Join(p.plugins, ", "))
	case len(affected) > 0:
		return fmt.Sprintf("%d affected package(s)", len(affected))
	default:
		return strings.Join(p.plugins, ", ") + " changed"
	}
}

func staticcheckWhy(affected []string, p plan) string {
	return vulncheckWhy(affected, p)
}

func pick(cond bool, yes, no string) string {
	if cond {
		return yes
	}
	return no
}

// ciForceReason reports why this run must ignore the diff and run everything,
// or "" when the diff decides.
//
// event is the GitHub event name. Only a pull request gets a diff-scoped run:
//   - a push to main is the record of whether that commit was good when it
//     landed, and a record with holes in it is not a record;
//
// A change to the workflows, the Makefile, or this gate itself forces the same,
// on any event: those decide what CI runs, and a plan cannot reason about a
// change to the thing computing the plan.
//
// base is the merge-base [resolveBase] found, and empty means it found none.
// That is the third forcing and the bluntest: with no base there is no diff,
// so there is nothing for a plan to be a plan *of*. It belongs here rather than
// at the caller because this function is where "must this ignore the diff"
// is answered, and an answer to that question living in two places is the one
// thing docs/CI.md says must never happen.
func ciForceReason(event, base string, p plan) string {
	if base == "" {
		return "no merge-base with origin/main could be established, so the diff cannot be measured and is not trusted to narrow anything"
	}
	if event != "" && event != "pull_request" {
		return "event is " + event + ", not a pull request: the full set runs"
	}
	if p.ciWide {
		return p.reasons["ci"] + " changed, which decides what CI runs: the full set runs"
	}
	if p.moduleWide {
		return p.reasons["module"] + " changed, so every package is affected"
	}
	return ""
}

// writeCIDecisions publishes the decisions three ways: one line per job on
// stdout (the same "say why" shape the local tier prints), a `name=value` pair
// per job — and per further output a job carries — plus a `decisions` JSON
// object in $GITHUB_OUTPUT, and a table in $GITHUB_STEP_SUMMARY so the answer
// is readable from the run page without opening a log.
//
// The JSON object is what the verdict job reads. It is deliberately the *same*
// object the `if:` expressions are driven from, so the two cannot disagree about
// which jobs were meant to run.
//
// lanes, from ciTestLanes, is published beside them as one `test_lanes` JSON
// object (lane name to import paths), which the test matrix's Test step reads.
func writeCIDecisions(decisions []decision, lanes map[string][]string, lanesWhy string) error {
	for _, l := range testLanes {
		fmt.Printf("plan: test lane %s: %d package(s)\n", l.Name, len(lanes[l.Name]))
	}
	fmt.Printf("plan: test lanes run %s\n", lanesWhy)
	encodedLanes, err := json.Marshal(lanes)
	if err != nil {
		return err
	}

	obj := map[string]bool{}
	for _, d := range decisions {
		obj[d.Job] = d.Run
		verb := "skipped"
		if d.Run {
			verb = "runs"
		}
		fmt.Printf("plan: %s: %s (%s)\n", d.Job, verb, d.Why)
	}
	encoded, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	if path := os.Getenv("GITHUB_OUTPUT"); path != "" {
		var b strings.Builder
		for _, d := range decisions {
			fmt.Fprintf(&b, "%s=%t\n", d.Output, d.Run)
		}
		fmt.Fprintf(&b, "decisions=%s\n", encoded)
		fmt.Fprintf(&b, "test_lanes=%s\n", encodedLanes)
		if err := appendFile(path, b.String()); err != nil {
			return err
		}
	}

	if path := os.Getenv("GITHUB_STEP_SUMMARY"); path != "" {
		var b strings.Builder
		b.WriteString("### What this diff can reach\n\n")
		b.WriteString("| job | | why |\n|---|---|---|\n")
		sorted := append([]decision(nil), decisions...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i].Job < sorted[j].Job })
		for _, d := range sorted {
			mark := "skipped"
			if d.Run {
				mark = "**runs**"
			}
			fmt.Fprintf(&b, "| `%s` | %s | %s |\n", d.Job, mark, d.Why)
		}
		fmt.Fprintf(&b, "\nThe test lanes run %s:", lanesWhy)
		for _, l := range testLanes {
			fmt.Fprintf(&b, " `%s` %d", l.Name, len(lanes[l.Name]))
		}
		b.WriteString(".\n")
		b.WriteString("\nA skipped job here is not a check that passed: the `verdict` job re-reads this " +
			"same plan and fails unless every job it selected actually succeeded.\n")
		if err := appendFile(path, b.String()); err != nil {
			return err
		}
	}
	return nil
}

func appendFile(path, content string) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(content); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
