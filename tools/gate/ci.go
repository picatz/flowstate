package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/picatz/flowstate/tools/fuzztargets"
)

// The packages whose affectedness decides a CI job that is narrower than "the
// Go code changed". cmdFlowPkg and flowtestPkg live in plan.go beside the legs
// they already decide.
//
// The packages holding fuzz targets are not among them any more:
// affectedSmokeTargets below reads them from tools/fuzztargets/targets.txt,
// because that file is where the targets themselves are written and "which
// packages hold one" is a fact about that list rather than a second thing to
// remember (#857). They used to be hand-kept here — flowfilePkg, pluginPkg,
// v1Pkg, lspPkg, each with a comment naming the target it existed for — while
// the same list was hand-kept in three other places, which is how the weekly
// deep tier came to run four of the ten targets the smoke tier ran.
const (
	// v1Pkg is the root v1 package: home of FuzzWebhookEventBinding (#799)
	// and FuzzCELEvaluate (#403), alongside every non-fuzz test already
	// reading webhook.go, eval.go and the rest of this package's own files.
	// It keeps a name here because tests name it directly; the fuzz decision
	// no longer reads it from here.
	v1Pkg = modulePath + "/pkg/flowstate/v1"
)

// fuzzTargetsOutput is the plan output naming the smoke targets the fuzz-smoke
// job runs, space-separated in targets.txt order. The job hands it to
// `make fuzz-smoke` as FUZZ_SMOKE_TARGETS; ci_test.go pins that wiring.
const fuzzTargetsOutput = "fuzz_targets"

// affectedSmokeTargets is the smoke-tier targets whose package the diff
// reaches, in targets.txt order. It is the *target* list rather than a package
// set because that is what the job spends: thirteen targets at 30s each is the
// better part of the job's budget, and a flowfile change can move seven of them
// while an engine change moves none in this tier (#1726). A target's behaviour
// is its package's behaviour, so the affected set decides it the same way it
// decides the ordering leg.
//
// The subtlety the hand-kept package list had to state and this one gets for
// free is why the language server's targets are selected on their own despite
// sitting underneath flowfile: the affected set is computed over the import
// graph and not over the directory tree. lsp imports flowfile, so a flowfile
// change reaches lsp, and a change to lsp reaches nothing above it — a diff
// touching only the language server would skip the targets that fuzz it if
// this were a prefix match.
//
// It is the smoke tier's targets and not every tier's, which is a change from
// the package set this replaced. A deep-only target's package used to reach the
// job so that promoting the target to smoke did not silently alter which diffs
// reach it; with the job running only the targets it was handed, the same diff
// would run a job with nothing in it, and the promotion itself is an edit to
// targets.txt, which buildPlan already forces wide.
func affectedSmokeTargets(affected []string) []string {
	var out []string
	for _, t := range fuzztargets.InTier(fuzztargets.TierSmoke) {
		if contains(affected, t.ImportPath(modulePath)) {
			out = append(out, t.Name)
		}
	}
	return out
}

// allSmokeTargets is every smoke-tier target's name, in targets.txt order: what
// a forced run fuzzes, and what `make fuzz-smoke` runs when handed no list.
func allSmokeTargets() []string {
	var out []string
	for _, t := range fuzztargets.InTier(fuzztargets.TierSmoke) {
		out = append(out, t.Name)
	}
	return out
}

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

	// Outputs are the further `name=value` pairs this job reads beside its
	// boolean, published to $GITHUB_OUTPUT under the same rules as Output:
	// the name must be a legal identifier in a workflow expression, and the
	// plan job's `outputs:` block must forward it. fuzz-smoke's target list
	// is the one today.
	Outputs map[string]string
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

	// fuzz-smoke: the smoke targets whose package the diff reaches, published
	// as their own output beside the boolean so the job runs those and not
	// the tier. A forced run fuzzes the whole tier, which is what the local
	// `make fuzz-smoke` does when handed no list.
	smokeTargets := affectedSmokeTargets(affected)
	if force != "" {
		smokeTargets = allSmokeTargets()
	}
	fuzzWhy := "no package holding a smoke-tier fuzz target is affected"
	if len(smokeTargets) > 0 {
		fuzzWhy = fmt.Sprintf("%d smoke target(s) live in affected packages: %s", len(smokeTargets), strings.Join(smokeTargets, ", "))
	}

	decisions := []decision{
		{Job: "test", Output: "test", Run: testRun, Why: testWhy},
		{Job: "test-plugins", Output: "test_plugins", Run: pluginsRun, Why: pluginsWhy},
		{Job: "test-ordering", Output: "test_ordering", Run: orderingRun, Why: orderingWhy},

		{Job: "proto", Output: "proto",
			Run: p.proto,
			Why: pick(p.proto, p.reasons["proto"]+" changed", "no changes under proto/ or to buf config")},

		// govulncheck and staticcheck both analyse ./... from the root
		// *and* walk each plugin module, so a diff that moves no Go
		// package and no plugin module cannot move what either reports.
		// What *can* move govulncheck's answer without a diff is the
		// advisory database, which it fetches when it runs — and that is
		// exactly why this skip is safe here and nowhere else: every push
		// to main and every merge group runs the full set (see force),
		// and deep.yml runs govulncheck weekly on a schedule. A new
		// advisory therefore still arrives on a calendar rather than
		// waiting for someone to touch a .go file; it just stops being
		// reported against the pull request that renamed a heading.
		{Job: "vulncheck", Output: "vulncheck",
			Run: goAffected || len(p.plugins) > 0,
			Why: pick(goAffected || len(p.plugins) > 0, vulncheckWhy(affected, p),
				"no Go package is affected and no plugin module changed; main, the merge queue and the weekly deep tier still scan against a freshly fetched advisory database")},

		{Job: "staticcheck", Output: "staticcheck",
			Run: goAffected || len(p.plugins) > 0,
			Why: pick(goAffected || len(p.plugins) > 0, staticcheckWhy(affected, p),
				"no Go package is affected and no plugin module changed")},

		// See affectedSmokeTargets: the job runs exactly the targets the
		// output names, and it runs at all only when there is one.
		{Job: "fuzz-smoke", Output: "fuzz_smoke",
			Run:     len(smokeTargets) > 0,
			Why:     fuzzWhy,
			Outputs: map[string]string{fuzzTargetsOutput: strings.Join(smokeTargets, " ")}},

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
//   - a merge group is the prospective merge — the thing #489 says was missing —
//     and it is the last gate before main, so it is also the place where being
//     wrong about the plan is unrecoverable. Running the full set there means
//     main's protection never rests on this file being right.
//
// A change to the workflows, the Makefile, or this gate itself forces the same,
// on any event: those decide what CI runs, and a plan cannot reason about a
// change to the thing computing the plan.
//
// base is the merge-base [resolveBase] found, and empty means it found none.
// That is the fourth forcing and the bluntest: with no base there is no diff,
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
			for _, name := range sortedKeys(d.Outputs) {
				fmt.Fprintf(&b, "%s=%s\n", name, d.Outputs[name])
			}
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
		for _, d := range sorted {
			for _, name := range sortedKeys(d.Outputs) {
				fmt.Fprintf(&b, "\n`%s` reads `%s`: %s\n", d.Job, name, pick(d.Outputs[name] != "", d.Outputs[name], "(empty)"))
			}
		}
		if err := appendFile(path, b.String()); err != nil {
			return err
		}
	}
	return nil
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
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
