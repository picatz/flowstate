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
// function, and this asserts it answers what ciDecisions answers for the job
// across every shape above — including the two forcing conditions, which is
// the direction that fails badly, since a diff affecting no Go package still
// runs the required job over ./....
func TestTheStaticcheckLegAndJobShareATrigger(t *testing.T) {
	for _, tc := range forcingCases {
		t.Run(tc.name, func(t *testing.T) {
			ds := decide(t, tc.changed, tc.affected, "pull_request")
			job, ok := ds["staticcheck"]
			if !ok {
				t.Fatal("no CI decision for the staticcheck job")
			}
			if leg := scopedLegRuns(buildPlan(tc.changed), tc.affected); leg != job.Run {
				t.Errorf("the local leg %s but the CI job %s (%s);\n"+
					"a leg that skips where the job runs is the gap #879 reported, one level down",
					ranOrSkipped(leg), ranOrSkipped(job.Run), job.Why)
			}
		})
	}
}

// TestAForcedStaticcheckLegAnalysesTheWholeModule is the drift pin on scope,
// which is the half a trigger test cannot see.
//
// Agreeing that the leg *runs* is not parity if it then analyses four packages
// while the job analyses the module: a finding in any package outside the
// affected set is one the gate passed and the required job will reject. So
// where the harness or the module graph forces CI wide, the leg's argv must
// end in ./... — and where nothing forces, it must not, because analysing the
// module on every ordinary diff is the cost this tier exists to avoid.
func TestAForcedStaticcheckLegAnalysesTheWholeModule(t *testing.T) {
	for _, tc := range forcingCases {
		t.Run(tc.name, func(t *testing.T) {
			p := buildPlan(tc.changed)
			if !scopedLegRuns(p, tc.affected) {
				return // asserted by the trigger test above
			}

			why := forcedWide(p)
			if tc.wide && why == "" {
				t.Fatalf("nothing forces this diff wide, but CI analyses ./... for it;\n"+
					"the leg would analyse only %v, and a finding anywhere else passes here and fails there", tc.affected)
			}
			if !tc.wide && why != "" {
				t.Fatalf("this diff is forced wide (%s), which makes every ordinary run analyse the module", why)
			}

			var got cmdSpec
			if why != "" {
				got = staticcheck("./...")
			} else {
				got = staticcheck(tc.affected...)
			}
			last := got.argv[len(got.argv)-1]
			if tc.wide && last != "./..." {
				t.Errorf("forced wide, but the leg analyses %q; the required job analyses ./...", last)
			}
			if !tc.wide && last == "./..." {
				t.Errorf("not forced, but the leg analyses ./...; this tier is diff-scoped by design")
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
