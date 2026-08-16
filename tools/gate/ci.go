package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

// The packages whose affectedness decides a CI job that is narrower than "the
// Go code changed". cmdFlowPkg and flowtestPkg live in plan.go beside the legs
// they already decide.
const (
	authPkg     = modulePath + "/pkg/flowstate/v1/auth"
	flowfilePkg = modulePath + "/pkg/flowstate/v1/flowfile"
	pluginPkg   = modulePath + "/pkg/flowstate/v1/plugin"
)

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
// commit was good when it landed, and a merge group is the last gate before
// main), or a change to something that decides what CI itself does. A plan
// cannot reason about a change to the thing computing the plan.
func ciDecisions(p plan, affected []string, force string) []decision {
	goAffected := len(affected) > 0

	// test: the widest job. It builds, vets, tests, walks the plugin
	// modules, regenerates the reference docs, and runs the three example
	// checks and the compose parse — so anything Go, anything under
	// examples/ (which is where the compose file lives), the schema, and
	// the derived-docs sources all reach it.
	//
	// p.plugins is in this OR for the reason a path filter could not express:
	// a plugin module is a separate Go module, so a diff touching only
	// plugins/<name>/ never lands in affected (go list ./... from the root
	// cannot see it) and touches none of examples/, proto/ or the derived-docs
	// sources either. Without this arm testRun was false for exactly that
	// diff, the test job — the only job that runs `make test-plugins`, since
	// nothing else in this workflow walks a plugin module at all — was
	// skipped, and verdict accepted the skip: a plugin that does not compile
	// could merge on a PR whose only change was to that plugin.
	// p.repoTestData is in the OR for the same #589 shape p.examples already
	// covers, one file further out: README.md and docs/ARCHITECTURE.md are
	// read directly by cmd/flow/commands_test.go and
	// pkg/flowstate/v1/flowfile/readme_test.go with os.ReadFile rather than
	// imported, so a change to either can make one of those tests fail or go
	// stale without moving a Go file, examples/, or proto/.
	testRun := goAffected || p.examples || p.docs || p.proto || len(p.plugins) > 0 || p.repoTestData
	testWhy := "no Go package is affected, and nothing under examples/ or proto/, none of the derived-docs sources or repository-level test data, and no plugin module changed"
	switch {
	case goAffected:
		testWhy = fmt.Sprintf("%d affected package(s)", len(affected))
	case p.examples:
		testWhy = p.reasons["examples"] + " changed"
	case p.proto:
		testWhy = p.reasons["proto"] + " changed"
	case p.docs:
		testWhy = p.reasons["docs"] + " changed"
	case len(p.plugins) > 0:
		testWhy = strings.Join(p.plugins, ", ") + " changed, and make test-plugins is the only thing that builds/vets/tests it"
	case p.repoTestData:
		testWhy = p.reasons["test"] + " changed, and it is read directly by tests rather than imported"
	}

	decisions := []decision{
		{Job: "test", Output: "test", Run: testRun, Why: testWhy},

		{Job: "proto", Output: "proto",
			Run: p.proto,
			Why: pick(p.proto, p.reasons["proto"]+" changed", "no changes under proto/ or to buf config")},

		// govulncheck and staticcheck both analyse ./..., so a diff that
		// moves no Go package cannot move what either reports about this
		// tree. What *can* move govulncheck's answer without a diff is the
		// advisory database, which it fetches when it runs — and that is
		// exactly why this skip is safe here and nowhere else: every push
		// to main and every merge group runs the full set (see force),
		// and deep.yml runs govulncheck weekly on a schedule. A new
		// advisory therefore still arrives on a calendar rather than
		// waiting for someone to touch a .go file; it just stops being
		// reported against the pull request that renamed a heading.
		{Job: "vulncheck", Output: "vulncheck",
			Run: goAffected,
			Why: pick(goAffected, fmt.Sprintf("%d affected package(s)", len(affected)),
				"no Go package is affected; main, the merge queue and the weekly deep tier still scan against a freshly fetched advisory database")},

		{Job: "staticcheck", Output: "staticcheck",
			Run: goAffected,
			Why: pick(goAffected, fmt.Sprintf("%d affected package(s)", len(affected)),
				"no Go package is affected")},

		// federation runs one test, in one package, against the real
		// issuer. Anything that reaches that package can change what it
		// asserts; nothing else can.
		{Job: "federation", Output: "federation",
			Run: contains(affected, authPkg),
			Why: pick(contains(affected, authPkg), "the auth package is affected", "the auth package is not affected")},

		// fuzz-smoke's five targets live in three packages. A target's
		// behaviour is its package's behaviour, so the affected set
		// decides this the same way it decides the ordering leg.
		{Job: "fuzz-smoke", Output: "fuzz_smoke",
			Run: needsFuzz(affected),
			Why: pick(needsFuzz(affected),
				"a package holding a fuzz target is affected",
				"no package holding a fuzz target (flowfile, cmd/flow, plugin) is affected")},

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

// needsFuzz reports whether any package holding a fuzz target is affected.
func needsFuzz(affected []string) bool {
	return contains(affected, flowfilePkg) || contains(affected, cmdFlowPkg) || contains(affected, pluginPkg)
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
//   - a merge group is the prospective merge — the thing #489 says was missing —
//     and it is the last gate before main, so it is also the place where being
//     wrong about the plan is unrecoverable. Running the full set there means
//     main's protection never rests on this file being right.
//
// A change to the workflows, the Makefile, or this gate itself forces the same,
// on any event: those decide what CI runs, and a plan cannot reason about a
// change to the thing computing the plan.
func ciForceReason(event string, p plan) string {
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
// per job plus a `decisions` JSON object in $GITHUB_OUTPUT, and a table in
// $GITHUB_STEP_SUMMARY so the answer is readable from the run page without
// opening a log.
//
// The JSON object is what the verdict job reads. It is deliberately the *same*
// object the `if:` expressions are driven from, so the two cannot disagree about
// which jobs were meant to run.
func writeCIDecisions(decisions []decision) error {
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
