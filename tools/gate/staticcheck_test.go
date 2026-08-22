package main

import (
	"os"
	"regexp"
	"strings"
	"testing"

	yaml "github.com/goccy/go-yaml"
)

// The staticcheck leg exists because the gate promised something it was not
// keeping. CLAUDE.md states the general form — "the local gate cannot pass a
// commit the required job rejects" — and it held for build, vet, tests and the
// fuzz smoke while staticcheck, required in CI since its advisory window
// closed, was absent from the leg list entirely. #878 is what that costs: `go
// run ./tools/gate` passed, CI failed on SA1019, and the author paid a push,
// a wait, a read and a second push for a diagnostic that takes seconds
// locally (#879).
//
// The two tests below are the two halves of "same check, narrower scope":
// the same tool, and the same trigger.

// staticcheckWorkflow is the slice of ci.yml these tests read through YAML.
//
// Deliberately not including `env:`. goccy/go-yaml resolves an unquoted
// `2026.1` as a float before handing it to a string field, and a float has no
// memory of how it was written: a STATICCHECK_VERSION of `2026.10` decodes to
// the string "2026.1" — measured, not assumed. A pin that silently reads a
// different version than the file says is worse than no pin, so the version is
// taken from the file's own bytes instead (see workflowEnv).
type staticcheckWorkflow struct {
	Jobs map[string]struct {
		Steps []struct {
			Run string `yaml:"run"`
		} `yaml:"steps"`
	} `yaml:"jobs"`
}

// workflowEnvLine matches one `KEY: value` entry in the workflow's top-level
// `env:` block, keeping the value exactly as written.
var workflowEnvLine = regexp.MustCompile(`(?m)^  ([A-Z_]+): +(\S+)\s*$`)

func workflowEnv(t *testing.T, data []byte) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, m := range workflowEnvLine.FindAllStringSubmatch(string(data), -1) {
		out[m[1]] = m[2]
	}
	return out
}

// expandWorkflowExpr substitutes the `${{ env.NAME }}` references a run line
// carries, which is the only expansion GitHub does to these two commands.
var workflowExpr = regexp.MustCompile(`\$\{\{\s*env\.([A-Z_]+)\s*\}\}`)

func expandWorkflowExpr(t *testing.T, s string, env map[string]string) string {
	t.Helper()
	return workflowExpr.ReplaceAllStringFunc(s, func(m string) string {
		name := workflowExpr.FindStringSubmatch(m)[1]
		v, ok := env[name]
		if !ok {
			t.Fatalf("the workflow references env.%s, which its env: block does not define", name)
		}
		return v
	})
}

func readWorkflow(t *testing.T) ([]byte, staticcheckWorkflow) {
	t.Helper()
	data, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatalf("reading the workflow: %v", err)
	}
	var wf staticcheckWorkflow
	if err := yaml.Unmarshal(data, &wf); err != nil {
		t.Fatalf("parsing the workflow: %v", err)
	}
	return data, wf
}

// TestTheGateAndCIRunTheSameStaticcheck is the drift pin on the tool itself.
//
// The gate is allowed exactly one difference from the required job: scope. It
// analyses the affected packages where CI analyses ./..., because that is what
// "diff-scoped locally, full in the queue" means. Everything else — the
// analyser, its release, and the GOTOOLCHAIN it runs under — has to be
// identical, because a local pass under a different rule set is not a
// prediction of the required job's answer, it is a second opinion wearing the
// gate's clothes.
//
// So this compares whole command lines: the one the workflow runs, with its
// `${{ env.* }}` expanded, against the one the gate's own staticcheck() builds
// for the module-wide case. Bumping STATICCHECK_VERSION in ci.yml without
// moving staticcheckVersion fails here, before a push.
func TestTheGateAndCIRunTheSameStaticcheck(t *testing.T) {
	data, wf := readWorkflow(t)

	job, ok := wf.Jobs["staticcheck"]
	if !ok {
		t.Fatal("ci.yml has no staticcheck job; if it was removed, this leg and its pins should go with it")
	}
	var want string
	for _, s := range job.Steps {
		if strings.Contains(s.Run, "staticcheck@") {
			want = expandWorkflowExpr(t, strings.TrimSpace(s.Run), workflowEnv(t, data))
			break
		}
	}
	if want == "" {
		t.Fatal("the staticcheck job runs no step invoking staticcheck@<version>")
	}

	got := staticcheck("./...").display()
	if got != want {
		t.Errorf("the gate runs a different staticcheck than the required job:\n"+
			"  gate:   %s\n"+
			"  ci.yml: %s\n"+
			"the two differ only in scope by design; a different version or toolchain means a local pass\n"+
			"predicts nothing about the job it exists to predict (update staticcheckVersion/staticcheckToolchain in main.go)",
			got, want)
	}
}

// TestTheStaticcheckLegAndJobShareATrigger is the drift pin on the trigger.
//
// A tool pinned to CI's is still no use if the two tiers disagree about *when*
// to run it: a leg that skips where the job runs is the same hole #879
// reported, moved one level down. The leg's condition is therefore a named
// function, and this asserts it answers what ciDecisions answers for the job,
// over the diffs that make the answer interesting — including the one the
// whole mechanism exists for, a markdown-only change reaching no Go package.
func TestTheStaticcheckLegAndJobShareATrigger(t *testing.T) {
	for _, tc := range []struct {
		name     string
		changed  []string
		affected []string
	}{
		{"a markdown-only diff reaches no package", []string{"CLAUDE.md"}, nil},
		{"an ordinary Go change", []string{"pkg/flowstate/v1/engine/policy.go"}, []string{modulePath + "/pkg/flowstate/v1/engine"}},
		{"a diff whose only Go reach is through the import graph", []string{"proto/flowstate/v1/signal.proto"}, []string{modulePath + "/pkg/flowstate/v1"}},
		{"an examples-only change that still seeds a package", []string{"examples/http/flow.yaml"}, []string{modulePath + "/pkg/flowstate/v1/flowfile"}},
		{"an examples-only change reaching no package at all", []string{"examples/README.md"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ds := decide(t, tc.changed, tc.affected, "pull_request")
			job, ok := ds["staticcheck"]
			if !ok {
				t.Fatal("no CI decision for the staticcheck job")
			}
			if leg := staticcheckRuns(tc.affected); leg != job.Run {
				t.Errorf("the local leg %s but the CI job %s (%s);\n"+
					"a leg that skips where the job runs is the gap #879 reported, one level down",
					ranOrSkipped(leg), ranOrSkipped(job.Run), job.Why)
			}
		})
	}
}

func ranOrSkipped(run bool) string {
	if run {
		return "runs"
	}
	return "skips"
}
