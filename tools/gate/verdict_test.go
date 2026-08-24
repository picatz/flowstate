package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"

	yaml "github.com/goccy/go-yaml"
)

// The verdict job is the one required check that decides a pull request, so
// the interesting question about it is not whether it passes a good run — it
// is whether it can be made to pass a bad one. Every case below is a way a
// conditional CI design fails *open*: a check reported as satisfied on
// something nobody looked at.
//
// The script is not copied here. It is read out of .github/workflows/ci.yml and
// executed, because a copy of a script is a thing that drifts and this is
// precisely the file whose drift the rest of this package exists to prevent.

// verdictScript extracts the verdict job's single `run:` block from the
// workflow. Reading it rather than restating it is the same rule
// TestTheWorkflowAndThePlanDecideTheSameJobs follows one level up.
func verdictScript(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatalf("reading the workflow: %v", err)
	}
	var wf struct {
		Jobs map[string]struct {
			Steps []struct {
				Run string `yaml:"run"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &wf); err != nil {
		t.Fatalf("parsing the workflow: %v", err)
	}
	steps := wf.Jobs["verdict"].Steps
	if len(steps) != 1 || strings.TrimSpace(steps[0].Run) == "" {
		t.Fatalf("expected the verdict job to be one run: step, found %d", len(steps))
	}
	return steps[0].Run
}

// runVerdict executes the script the way Actions would: PLAN_RESULT is the plan
// job's result, PLAN is the decisions object it published, and RESULTS is
// toJSON(needs) — a map from job name to an object with a `result` field.
func runVerdict(t *testing.T, planResult string, plan map[string]bool, results map[string]string) (bool, string) {
	t.Helper()

	planJSON, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	needs := map[string]map[string]string{}
	for job, result := range results {
		needs[job] = map[string]string{"result": result}
	}
	resultsJSON, err := json.Marshal(needs)
	if err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command("bash", "-c", verdictScript(t))
	cmd.Env = append(os.Environ(),
		"PLAN_RESULT="+planResult,
		"PLAN="+string(planJSON),
		"RESULTS="+string(resultsJSON),
	)
	out, err := cmd.CombinedOutput()
	return err == nil, string(out)
}

// fullResults is a run where everything the plan selected succeeded and
// everything it did not select skipped — the shape a correct run has.
func fullResults(plan map[string]bool) map[string]string {
	out := map[string]string{"plan": "success"}
	for job, run := range plan {
		if run {
			out[job] = "success"
		} else {
			out[job] = "skipped"
		}
	}
	return out
}

// samplePlan is a realistic diff-scoped answer: a change reaching the engine
// but not the fuzz targets or the CLI.
func samplePlan() map[string]bool {
	return map[string]bool{
		"test": true, "vulncheck": true, "staticcheck": true,
		"proto": false, "fuzz-smoke": false, "appearance": false,
	}
}

func TestTheVerdictPassesACorrectRun(t *testing.T) {
	plan := samplePlan()
	ok, out := runVerdict(t, "success", plan, fullResults(plan))
	if !ok {
		t.Fatalf("a correct run should pass:\n%s", out)
	}
}

// TestASelectedJobThatSkippedFailsTheVerdict is the fail-open case itself.
//
// GitHub reports a job skipped by its `if:` with the conclusion `skipped`, and
// a required status check treats `skipped` as satisfied. If the conditional
// jobs were the required checks, a plan that wrongly skipped `test` would show
// a green tick on a pull request nothing tested. Here the plan selected it, so
// a skip is a contradiction and the verdict says so.
func TestASelectedJobThatSkippedFailsTheVerdict(t *testing.T) {
	plan := samplePlan()
	results := fullResults(plan)
	results["test"] = "skipped"

	ok, out := runVerdict(t, "success", plan, results)
	if ok {
		t.Fatalf("a job the plan selected but which did not run must fail the verdict:\n%s", out)
	}
	if !strings.Contains(out, "test was selected by the plan") {
		t.Errorf("the failure should name the job and why:\n%s", out)
	}
}

// TestEveryNonSuccessResultFailsTheVerdict. `success` is the only result that
// means a job looked at this diff and was satisfied. The others each have a
// plausible-looking story — cancelled by concurrency, skipped because an
// upstream job failed — and none of them is evidence about the code.
func TestEveryNonSuccessResultFailsTheVerdict(t *testing.T) {
	for _, result := range []string{"failure", "cancelled", "skipped", "", "absent"} {
		plan := samplePlan()
		results := fullResults(plan)
		if result == "absent" {
			delete(results, "staticcheck")
		} else {
			results["staticcheck"] = result
		}
		if ok, out := runVerdict(t, "success", plan, results); ok {
			t.Errorf("result %q for a selected job passed the verdict:\n%s", result, out)
		}
	}
}

// TestAJobThatRanUnselectedFailsTheVerdict. This is the safe direction — a job
// that ran anyway proved *more* than was asked — and it still fails, because
// the only way it happens is that the plan and that job's `if:` disagree about
// which rule decides. One of them is then not the rule, and the next
// disagreement will not be in the safe direction.
func TestAJobThatRanUnselectedFailsTheVerdict(t *testing.T) {
	plan := samplePlan()
	results := fullResults(plan)
	results["proto"] = "success"

	ok, out := runVerdict(t, "success", plan, results)
	if ok {
		t.Fatalf("a job that ran without being selected must fail the verdict:\n%s", out)
	}
	if !strings.Contains(out, "disagree") {
		t.Errorf("the failure should say the plan and the job's condition disagree:\n%s", out)
	}
}

// TestAJobThePlanNeverDecidedFailsTheVerdict is the drift case at run time:
// somebody adds a job to ci.yml and to the verdict's needs, but not to
// ciDecisions. Nothing would otherwise be obliged to care what it did.
func TestAJobThePlanNeverDecidedFailsTheVerdict(t *testing.T) {
	plan := samplePlan()
	results := fullResults(plan)
	results["brandnew"] = "failure"

	ok, out := runVerdict(t, "success", plan, results)
	if ok {
		t.Fatalf("a job the plan does not decide must fail the verdict:\n%s", out)
	}
	if !strings.Contains(out, "brandnew") {
		t.Errorf("the failure should name the undecided job:\n%s", out)
	}
}

// TestAPlanThatDidNotSucceedFailsTheVerdict. Every skip below rests on the
// plan's answer, so a plan that failed, was cancelled, or was itself skipped
// leaves the verdict with nothing to conclude — and "nothing to conclude" is a
// red check, never a green one. An empty decisions object is the same failure
// wearing a success: a plan that established no obligations would otherwise
// vacuously satisfy every one of them.
func TestAPlanThatDidNotSucceedFailsTheVerdict(t *testing.T) {
	for _, result := range []string{"failure", "cancelled", "skipped"} {
		plan := samplePlan()
		if ok, out := runVerdict(t, result, plan, fullResults(plan)); ok {
			t.Errorf("plan result %q passed the verdict:\n%s", result, out)
		}
	}
	if ok, out := runVerdict(t, "success", map[string]bool{}, map[string]string{"plan": "success"}); ok {
		t.Errorf("an empty plan passed the verdict vacuously:\n%s", out)
	}
}
