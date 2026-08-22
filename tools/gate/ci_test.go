package main

import (
	"fmt"
	"os"
	"sort"
	"testing"

	yaml "github.com/goccy/go-yaml"
)

// decide is a small reader for the slice ciDecisions returns.
func decide(t *testing.T, changed []string, affected []string, event string) map[string]decision {
	t.Helper()
	p := buildPlan(changed)
	out := map[string]decision{}
	for _, d := range ciDecisions(p, affected, ciForceReason(event, p)) {
		out[d.Job] = d
	}
	return out
}

func mustRun(t *testing.T, ds map[string]decision, jobs ...string) {
	t.Helper()
	for _, j := range jobs {
		d, ok := ds[j]
		if !ok {
			t.Fatalf("no decision for job %q", j)
		}
		if !d.Run {
			t.Errorf("job %q should run, but was skipped: %s", j, d.Why)
		}
	}
}

func mustSkip(t *testing.T, ds map[string]decision, jobs ...string) {
	t.Helper()
	for _, j := range jobs {
		d, ok := ds[j]
		if !ok {
			t.Fatalf("no decision for job %q", j)
		}
		if d.Run {
			t.Errorf("job %q should be skipped, but runs: %s", j, d.Why)
		}
	}
}

// TestAMarkdownOnlyDiffReachesNothing is the case this whole mechanism exists
// for: PR #659 changed CLAUDE.md alone and ran appearance, fuzz-smoke, proto,
// staticcheck and test. None of those can be affected by that file.
func TestAMarkdownOnlyDiffReachesNothing(t *testing.T) {
	ds := decide(t, []string{"CLAUDE.md"}, nil, "pull_request")
	mustSkip(t, ds, "test", "proto", "vulncheck", "staticcheck", "fuzz-smoke", "appearance")
}

// TestEveryDecisionSaysWhy holds both answers to the same standard the local
// tier's leg lines are held to. A skip nobody can read is indistinguishable
// from a gap, which is the whole reason this is not a `paths:` filter.
func TestEveryDecisionSaysWhy(t *testing.T) {
	for _, tc := range [][]string{
		{"CLAUDE.md"},
		{"pkg/flowstate/v1/auth/auth.go"},
		{"proto/flowstate/v1/flowstate.proto"},
		{".github/workflows/ci.yml"},
	} {
		for _, d := range ciDecisions(buildPlan(tc), nil, ciForceReason("pull_request", buildPlan(tc))) {
			if d.Why == "" {
				t.Errorf("%v: job %q has no reason recorded", tc, d.Job)
			}
		}
	}
}

// TestDocsOnlySourcesStillReachTheTestJob: docs/DSL.md changes no Go package,
// but the test job is what regenerates the reference mirror and pins it. A
// skip there would let the mirror drift, which is exactly the class
// TestTheMirrorMatchesTheRepository exists to catch.
func TestDocsOnlySourcesStillReachTheTestJob(t *testing.T) {
	ds := decide(t, []string{"docs/DSL.md"}, nil, "pull_request")
	mustRun(t, ds, "test")
	mustSkip(t, ds, "proto", "vulncheck", "staticcheck", "fuzz-smoke", "appearance")
}

// TestAnExampleOnlyChangeReachesTheTestJob: examples/ holds the corpus the
// three `flow fix`/`test`/`breaking` steps read, and the observability compose
// file the last step parses — none of it imported by anything.
func TestAnExampleOnlyChangeReachesTheTestJob(t *testing.T) {
	ds := decide(t, []string{"examples/observability/docker-compose.yaml"}, nil, "pull_request")
	mustRun(t, ds, "test")
	mustSkip(t, ds, "staticcheck", "vulncheck")
}

// TestAPluginOnlyChangeStillReachesTheTestJob is the regression for the gap
// Codex's review of #688 found: a diff touching only plugins/<name>/ never
// lands in the root module's affected-package set (go list ./... from the
// root cannot see a separate module) and touches none of examples/, proto/
// or the derived-docs sources either — so before p.plugins was in testRun's
// OR, this diff reached no job at all, and the test job is the only one
// that runs `make test-plugins`, the sole thing in this workflow that
// builds, vets or tests a plugin module. Skipping it here is the gate
// failing open on exactly the PRs whose whole point is to change a plugin.
func TestAPluginOnlyChangeStillReachesTheTestJob(t *testing.T) {
	ds := decide(t, []string{"plugins/openai/main.go"}, nil, "pull_request")
	mustRun(t, ds, "test")
	mustSkip(t, ds, "proto", "vulncheck", "staticcheck", "fuzz-smoke", "appearance")
}

// TestReadmeOrArchitectureOnlyStillReachesTheTestJob is the regression for a
// Codex P2 on #688: cmd/flow/commands_test.go reads README.md's command
// table, pkg/flowstate/v1/flowfile/readme_test.go compiles the Flowfiles
// embedded in README.md and docs/ARCHITECTURE.md, pkg/flowstate/v1/agentsmd_test.go
// reads AGENTS.md, and cmd/flow/docs_test.go reads and validates every file
// under docs/reference/ — five files read with os.ReadFile rather than an
// import, so a diff touching only one of them moved neither a Go package,
// examples/, nor proto/, and reached no job at all before p.repoTestData
// existed (extended to the last three by a fresh Codex finding on the same
// PR). A PR could introduce stale command documentation, an invalid embedded
// Flowfile, an AGENTS.md that drifted from CLAUDE.md, or a stale generated
// doc while verdict accepted the skip.
//
// Widened again by #708, which gave the documentation set a test about the
// *set*: cmd/flow/docsindex_test.go fails when a page under docs/ is added,
// renamed or removed without docs/README.md moving with it, and when a page
// under docs/plans/ loses its internal-only banner. Every Markdown file in that
// tree is test data now, so the rule covers docs/ rather than enumerating the
// pages that happen to be read today — the enumeration is what would go stale
// the next time a page is added.
func TestReadmeOrArchitectureOnlyStillReachesTheTestJob(t *testing.T) {
	for _, f := range []string{
		"README.md",
		"docs/ARCHITECTURE.md",
		"AGENTS.md",
		"docs/reference/tasks.md",
		"docs/README.md",
		"docs/DEPLOYMENT.md",
		"docs/plans/factory.md",
	} {
		t.Run(f, func(t *testing.T) {
			ds := decide(t, []string{f}, nil, "pull_request")
			mustRun(t, ds, "test")
			mustSkip(t, ds, "proto", "vulncheck", "staticcheck", "fuzz-smoke", "appearance")
		})
	}
}

// TestTheNarrowJobsFollowTheAffectedSet pins the two jobs whose trigger is a
// package rather than a path: fuzz-smoke's targets live in four, and the
// appearance goldens record what the cmd/flow binary prints.
func TestTheNarrowJobsFollowTheAffectedSet(t *testing.T) {
	changed := []string{"pkg/flowstate/v1/engine/policy.go"}

	// A diff that reaches the engine and nothing else. pkg/flowstate/v1/engine
	// is a different package from the root pkg/flowstate/v1 v1Pkg names below,
	// so this must still skip.
	ds := decide(t, changed, []string{modulePath + "/pkg/flowstate/v1/engine"}, "pull_request")
	mustRun(t, ds, "test", "vulncheck", "staticcheck")
	mustSkip(t, ds, "fuzz-smoke", "appearance", "proto")

	// The same diff, in a tree where the engine is on cmd/flow's import path —
	// which is what affectedPackages actually computes.
	ds = decide(t, changed, []string{
		modulePath + "/pkg/flowstate/v1/engine",
		cmdFlowPkg,
	}, "pull_request")
	mustRun(t, ds, "fuzz-smoke", "appearance")
}

// TestAWebhookOnlyChangeReachesFuzzSmoke is the regression for #799:
// FuzzWebhookEventBinding lives in the root pkg/flowstate/v1 package
// (webhook.go's own directory), not in one of the three packages fuzz-smoke's
// affectedness check already knew about — so a diff touching only webhook.go
// used to compute an affected set with none of flowfilePkg, cmdFlowPkg or
// pluginPkg in it, and the plan would have skipped fuzz-smoke on the one kind
// of change most likely to move what that target exercises.
func TestAWebhookOnlyChangeReachesFuzzSmoke(t *testing.T) {
	ds := decide(t, []string{"pkg/flowstate/v1/webhook.go"}, []string{v1Pkg}, "pull_request")
	mustRun(t, ds, "fuzz-smoke")
}

// TestTheFullSetRunsWhereBeingWrongIsUnrecoverable. Three forcing conditions,
// each for its own reason: a push to main is the record and a record with holes
// is not one; a merge group is the last gate before main; and a change to the
// harness is a change to the thing computing the plan, which the plan cannot
// reason about.
func TestTheFullSetRunsWhereBeingWrongIsUnrecoverable(t *testing.T) {
	all := []string{"test", "proto", "vulncheck", "staticcheck", "fuzz-smoke", "appearance"}

	for _, tc := range []struct {
		name    string
		changed []string
		event   string
	}{
		{"a push to main", []string{"CLAUDE.md"}, "push"},
		{"a merge group", []string{"CLAUDE.md"}, "merge_group"},
		{"a workflow change", []string{".github/workflows/ci.yml"}, "pull_request"},
		{"a Makefile change", []string{"Makefile"}, "pull_request"},
		{"a change to the gate itself", []string{"tools/gate/ci.go"}, "pull_request"},
		{"a module graph change", []string{"go.sum"}, "pull_request"},
		// The fuzz target list is CI configuration: it decides which
		// targets each tier runs and which packages reach the fuzz job.
		// Promoting a deep-only target to smoke moves no Go package at
		// all, so a diff of this file alone would otherwise skip the very
		// job it reconfigures — including fuzz-smoke, whose new target
		// would then first run somewhere nobody is watching.
		{"a fuzz target list change", []string{"tools/fuzztargets/targets.txt"}, "pull_request"},
		{"a change to how the list is read", []string{"tools/fuzztargets/list.sh"}, "pull_request"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mustRun(t, decide(t, tc.changed, nil, tc.event), all...)
		})
	}
}

// TestAGateChangeIsStillAnOrdinaryGoChange. ciWide is recorded without
// `continue`, unlike the module-file case, because tools/gate/ci.go is also a
// Go file in a Go package: forcing the CI job set must not stop the gate's own
// package from being resolved and its own tests from running.
func TestAGateChangeIsStillAnOrdinaryGoChange(t *testing.T) {
	p := buildPlan([]string{"tools/gate/ci.go"})
	if !p.ciWide {
		t.Fatal("a change under tools/gate/ should force the full CI job set")
	}
	if !contains(p.goFiles, "tools/gate/ci.go") {
		t.Error("a change under tools/gate/ should still be gofmt-checked")
	}
	if !contains(p.fileDirs, "tools/gate") {
		t.Error("a change under tools/gate/ should still resolve to its package")
	}
}

// ── The drift pin ────────────────────────────────────────────────────────────

// ciWorkflow is the slice of .github/workflows/ci.yml this test reads.
type ciWorkflow struct {
	Jobs map[string]struct {
		Needs any    `yaml:"needs"`
		If    string `yaml:"if"`
	} `yaml:"jobs"`
}

// TestTheWorkflowAndThePlanDecideTheSameJobs is the check that keeps this from
// becoming the defect it was written to avoid.
//
// The verdict job asserts, at run time, that every job the plan named produced
// the right result and that every job it can see was named by the plan. What it
// cannot see is a job added to ci.yml and left out of *both* the plan and the
// verdict's own `needs:` — that job would run, or not run, with nothing obliged
// to care. This test closes that, before a push rather than after: the job set
// in the file, the job set the plan decides, and the set the verdict waits on
// must be the same three sets.
//
// It also pins each job's `if:` to the plan output that decides it, because an
// `if:` naming the wrong output is a job gated on somebody else's answer — and
// the verdict, comparing plan against results, would report that agreement as
// correct.
func TestTheWorkflowAndThePlanDecideTheSameJobs(t *testing.T) {
	data, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatalf("reading the workflow: %v", err)
	}
	var wf ciWorkflow
	if err := yaml.Unmarshal(data, &wf); err != nil {
		t.Fatalf("parsing the workflow: %v", err)
	}

	// buildPlan(nil) is enough: the decision *values* depend on the diff,
	// but the set of jobs decided does not.
	decided := map[string]decision{}
	for _, d := range ciDecisions(buildPlan(nil), nil, "") {
		decided[d.Job] = d
	}

	var inFile []string
	for name := range wf.Jobs {
		if name == "plan" || name == "verdict" {
			continue
		}
		inFile = append(inFile, name)
	}
	sort.Strings(inFile)

	var inPlan []string
	for name := range decided {
		inPlan = append(inPlan, name)
	}
	sort.Strings(inPlan)

	if fmt.Sprint(inFile) != fmt.Sprint(inPlan) {
		t.Fatalf("ci.yml decides %v but tools/gate decides %v;\n"+
			"a job in the file and not the plan runs unconditionally or not at all with nothing requiring it,\n"+
			"and a job in the plan and not the file makes the verdict fail on every run", inFile, inPlan)
	}

	// The verdict must wait on the plan and on every decided job; anything
	// it does not name is invisible to it at run time.
	verdict, ok := wf.Jobs["verdict"]
	if !ok {
		t.Fatal("ci.yml has no verdict job; it is the required check that keeps a skip from being read as a pass")
	}
	needs := map[string]bool{}
	switch n := verdict.Needs.(type) {
	case []any:
		for _, v := range n {
			needs[fmt.Sprint(v)] = true
		}
	case string:
		needs[n] = true
	default:
		t.Fatalf("verdict's needs: is %T, which this test cannot read", verdict.Needs)
	}
	if !needs["plan"] {
		t.Error("verdict does not need the plan job, so it cannot tell a skip from a pass")
	}
	for _, name := range inPlan {
		if !needs[name] {
			t.Errorf("verdict does not need %q, so that job's result is invisible to the check that decides", name)
		}
	}
	if verdict.If != "always()" {
		t.Errorf("verdict's if: is %q, not always(); a required check that can itself be skipped is the failure mode this design exists to remove", verdict.If)
	}

	// Each job gated on the plan output that actually decides it.
	for _, name := range inPlan {
		job := wf.Jobs[name]
		want := fmt.Sprintf("needs.plan.outputs.%s == 'true'", decided[name].Output)
		if job.If != want {
			t.Errorf("job %q has if: %q, want %q", name, job.If, want)
		}
		if fmt.Sprint(job.Needs) != "plan" {
			t.Errorf("job %q has needs: %v, want plan", name, job.Needs)
		}
	}
}

// TestPlanOutputNamesAreLegalInWorkflowExpressions. A job name may contain a
// hyphen; `needs.plan.outputs.fuzz-smoke` parses as a subtraction and silently
// evaluates to an empty string, which compares unequal to 'true' — so the job
// would skip on every run, and the verdict would then fail every run. Cheap to
// assert, and impossible to see by reading the YAML.
func TestPlanOutputNamesAreLegalInWorkflowExpressions(t *testing.T) {
	for _, d := range ciDecisions(buildPlan(nil), nil, "") {
		for _, r := range d.Output {
			if !(r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
				t.Errorf("job %q publishes output %q, which is not a legal identifier in a workflow expression", d.Job, d.Output)
				break
			}
		}
	}
}
