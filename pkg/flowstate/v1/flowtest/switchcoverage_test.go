package flowtest_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// Switch-arm coverage (issue #801), which is #420's own stated measurement rule
// for switches finally implemented: "the matched case is in the transcript, so
// the harness reads it rather than inferring it."
//
// Every test here is written in the direction the house rules insist on — that a
// suite which does *not* exercise an arm is reported — because the opposite
// direction was already green before any of this existed. #452's step universe
// folds a switch's case bodies into the enclosing one, so an arm was "covered"
// whenever its body steps happened to run, and an arm with no body steps at all
// could never be anything but covered.
//
// Mutation-proven as a set: replacing markArmTaken's body with the inference
// #452 used — credit every arm of a switch whose step ran at all — fails five of
// the tests below, and none of the step-coverage tests beside them notice.

// armWorkflow is the shape the defect lives in, and all three of its arms are
// the ones the step universe cannot measure:
//
//   - `case: [closed, merged]` is two literals sharing one body, so `archive`
//     running says nothing about which of them matched;
//   - `case: synchronize` has an empty body — `steps: []`, the documented
//     spelling of written-down ignoring — so it contributes no step at all;
//   - `default:` runs `unhandled`, which is a step, but only the record says the
//     default is what ran rather than some other path to the same id.
const armWorkflow = `edition: v2026.3
name: arms
inputs:
  action:
    type: string
    required: true
steps:
  - id: on_event
    switch:
      value: ${inputs.action}
      cases:
        - case: [closed, merged]
          steps:
            - id: archive
              log:
                message: archiving
        - case: synchronize
          steps: []
      default:
        steps:
          - id: unhandled
            log:
              message: unhandled
`

// armCase is one `tests:` entry driving the switch with the action given.
func armCase(name, action string) string {
	return `
  - name: ` + name + `
    workflow: ./workflow.yaml
    inputs:
      action: ` + action + `
    stubs:
      - task: log
        returns: {}
    expect:
      failed: false
`
}

// writeArmFixture writes armWorkflow and a suite of the cases named, and returns
// the `*.test.yaml` path.
func writeArmFixture(t *testing.T, cases string, extra string) string {
	t.Helper()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", armWorkflow)
	path := dir + "/arms.test.yaml"
	writeFile(t, path, "edition: v2026.3\ntests:"+cases+extra)

	return path
}

// armByKey finds one arm of a coverage result by its key.
func armByKey(t *testing.T, coverage *flowtest.Coverage, key string) *flowtest.SwitchArm {
	t.Helper()

	for _, arm := range coverage.Arms {
		if arm.Key == key {
			return arm
		}
	}
	t.Fatalf("no arm %q in %d arms", key, len(coverage.Arms))

	return nil
}

// TestAnEmptyArmIsUncoverableWithoutTheRecord is the finding #801 opens with,
// asserted as the pair of facts that made it invisible: every *step* is reached,
// and an arm is not.
//
// A suite that drives `closed` and an unenumerated value runs `archive` and
// `unhandled`, which is the whole step universe, so step coverage reports 2/2
// and `--coverage-required` is satisfied. The `synchronize` arm — a legal,
// documented, empty body — was taken by nothing, and before this there was no
// unit in which to say so.
func TestAnEmptyArmIsUncoverableWithoutTheRecord(t *testing.T) {
	t.Parallel()

	path := writeArmFixture(t,
		armCase("the list case", "closed")+armCase("the default", "labeled"), "")

	_, coverages := flowtest.RunFileWithCoverage(path)
	require.Len(t, coverages, 1)
	coverage := coverages[0]

	// The half that hid it: every step this workflow has was run.
	assert.Empty(t, coverage.Unreached, "every step ran, which is exactly why the arm was invisible")
	assert.Empty(t, coverage.Gaps())

	// The half that finds it.
	empty := armByKey(t, coverage, "on_event:case[1]")
	assert.Equal(t, `case "synchronize"`, empty.Label)
	assert.False(t, empty.Reached, "no case drove the switch with `synchronize`")

	gaps := coverage.ArmGaps()
	require.Len(t, gaps, 2, "the unexercised member of the list case is a gap too")
	assert.Equal(t, "on_event:case[0][1]", gaps[0].Key)
	assert.Equal(t, "on_event:case[1]", gaps[1].Key)
}

// TestAMultiLiteralCaseIsMeasuredPerLiteral is the second half of the finding:
// `case: [closed, merged]` is one body, so a suite exercising only `closed`
// leaves `merged` untested while every step still runs. The transcript names the
// member that matched, so the two are distinguishable — and they are matched
// through [v1.SwitchLiteralsEqual], the same function the engine used to pick the
// arm, rather than through a second spelling of equality here.
func TestAMultiLiteralCaseIsMeasuredPerLiteral(t *testing.T) {
	t.Parallel()

	path := writeArmFixture(t,
		armCase("only closed", "closed")+
			armCase("the empty arm", "synchronize")+
			armCase("the default", "labeled"), "")

	_, coverages := flowtest.RunFileWithCoverage(path)
	require.Len(t, coverages, 1)
	coverage := coverages[0]

	assert.True(t, armByKey(t, coverage, "on_event:case[0][0]").Reached, "`closed` was driven")
	assert.False(t, armByKey(t, coverage, "on_event:case[0][1]").Reached, "`merged` was not")
	assert.True(t, armByKey(t, coverage, "on_event:case[1]").Reached)
	assert.True(t, armByKey(t, coverage, "on_event:default").Reached)

	assert.Equal(t, 3, coverage.ArmsReached())
	require.Len(t, coverage.ArmGaps(), 1)
	assert.Equal(t, `case "merged"`, coverage.ArmGaps()[0].Label)
}

// TestEveryArmExercisedLeavesNoGap is the positive direction, which matters here
// for one reason: a measurement that reports a gap on a suite that really does
// cover everything is a false diagnostic, and this file's whole subject is a
// measurement that was wrong in the other direction.
func TestEveryArmExercisedLeavesNoGap(t *testing.T) {
	t.Parallel()

	path := writeArmFixture(t,
		armCase("closed", "closed")+
			armCase("merged", "merged")+
			armCase("synchronize", "synchronize")+
			armCase("labeled", "labeled"), "")

	_, coverages := flowtest.RunFileWithCoverage(path)
	require.Len(t, coverages, 1)
	coverage := coverages[0]

	require.Len(t, coverage.Arms, 4)
	assert.Equal(t, 4, coverage.ArmsReached())
	assert.Empty(t, coverage.ArmGaps())
	assert.Empty(t, coverage.Stale)
}

// TestAnArmCarriesThePositionItWasWrittenAt is issue #801's part B, and the
// reason it is a prerequisite rather than a polish: an arm has no id. `archive`
// can be found by grepping for it; `on_event:case[1]` exists nowhere in the file
// and can only be found by the line and column the diagnostic names.
//
// The position is taken from [flowfile.Positions] through the same address the
// validator's own case diagnostics use ([flowfile.SwitchCaseField]), so a
// coverage line and a `flow validate` squiggle point at the same token.
func TestAnArmCarriesThePositionItWasWrittenAt(t *testing.T) {
	t.Parallel()

	path := writeArmFixture(t, armCase("the default", "labeled"), "")

	_, coverages := flowtest.RunFileWithCoverage(path)
	require.Len(t, coverages, 1)
	coverage := coverages[0]

	// armWorkflow's own line numbers: `- case: [closed, merged]` is line 12,
	// `- case: synchronize` is line 17, and the `default:` mapping starts at the
	// `steps:` on line 20 — the node whose span the parser records for it.
	list := armByKey(t, coverage, "on_event:case[0][0]")
	require.True(t, list.Where.IsValid(), "an arm parsed from a file must carry a position")
	assert.Equal(t, 12, list.Where.Start.Line)

	// The two members of one `case: [a, b]` are distinct tokens on that line, so
	// their columns differ — which is what makes a per-literal gap something an
	// author can actually be pointed at.
	other := armByKey(t, coverage, "on_event:case[0][1]")
	assert.Equal(t, 12, other.Where.Start.Line)
	assert.NotEqual(t, list.Where.Start.Column, other.Where.Start.Column,
		"two literals of one case are two positions, or the diagnostic points at the wrong one")

	empty := armByKey(t, coverage, "on_event:case[1]")
	assert.Equal(t, 17, empty.Where.Start.Line)

	def := armByKey(t, coverage, "on_event:default")
	require.True(t, def.Where.IsValid())
	assert.Equal(t, 20, def.Where.Start.Line)
}

// TestAnArmCanBeRecordedAndTheRecordGoesStale is `coverage.allow_unreached`
// answering for arms the way it answers for steps — one record, one question —
// including the half that keeps the record honest: a reason kept past the arm it
// explained is a false statement about the suite, and fails the same way a gap
// does.
func TestAnArmCanBeRecordedAndTheRecordGoesStale(t *testing.T) {
	t.Parallel()

	t.Run("a recorded arm is an accepted residual", func(t *testing.T) {
		t.Parallel()

		path := writeArmFixture(t,
			armCase("closed", "closed")+armCase("merged", "merged")+armCase("labeled", "labeled"),
			"\ncoverage:\n  allow_unreached:\n    on_event:case[1]: the provider only sends this in production; nothing to rehearse.\n")

		_, coverages := flowtest.RunFileWithCoverage(path)
		require.Len(t, coverages, 1)
		coverage := coverages[0]

		empty := armByKey(t, coverage, "on_event:case[1]")
		assert.False(t, empty.Reached)
		assert.Contains(t, empty.Reason, "only sends this in production")
		assert.Empty(t, coverage.ArmGaps(), "a recorded arm is a decision, not a hole")
		assert.Empty(t, coverage.Stale)
	})

	t.Run("a record for an arm every case takes is stale", func(t *testing.T) {
		t.Parallel()

		path := writeArmFixture(t,
			armCase("closed", "closed")+armCase("merged", "merged")+
				armCase("synchronize", "synchronize")+armCase("labeled", "labeled"),
			"\ncoverage:\n  allow_unreached:\n    on_event:case[1]: stale; a case drives this now.\n")

		_, coverages := flowtest.RunFileWithCoverage(path)
		require.Len(t, coverages, 1)
		coverage := coverages[0]

		require.Len(t, coverage.Stale, 1, "a record for a reached arm must be reported stale")
		assert.Contains(t, coverage.Stale[0], "on_event:case[1]")
	})

	t.Run("a record for an arm no workflow has is stale", func(t *testing.T) {
		t.Parallel()

		path := writeArmFixture(t,
			armCase("closed", "closed")+armCase("merged", "merged")+
				armCase("synchronize", "synchronize")+armCase("labeled", "labeled"),
			"\ncoverage:\n  allow_unreached:\n    on_event:case[9]: names an arm that does not exist.\n")

		_, coverages := flowtest.RunFileWithCoverage(path)
		require.Len(t, coverages, 1)
		coverage := coverages[0]

		require.Len(t, coverage.Stale, 1)
		assert.Contains(t, coverage.Stale[0], "on_event:case[9]")
		assert.Contains(t, coverage.Stale[0], "switch arm")
	})
}

// TestTheDefaultArmIsReadFromTheRecordAndNotFromItsBody is the third shape the
// step universe gets wrong, and the quietest: `unhandled` running is not
// evidence the default ran, because an id is an id. Reading
// [v1.SwitchCaseOutput] — null exactly when no case matched, which the validator
// guarantees by refusing a null `case:` — is what makes "the default ran" a fact
// rather than an inference.
func TestTheDefaultArmIsReadFromTheRecordAndNotFromItsBody(t *testing.T) {
	t.Parallel()

	path := writeArmFixture(t,
		armCase("closed", "closed")+armCase("merged", "merged")+armCase("synchronize", "synchronize"), "")

	_, coverages := flowtest.RunFileWithCoverage(path)
	require.Len(t, coverages, 1)
	coverage := coverages[0]

	def := armByKey(t, coverage, "on_event:default")
	assert.False(t, def.Reached, "no case supplied a value the cases do not enumerate")
	assert.Equal(t, "default", def.Label)

	// And its body step is genuinely unreached too, so the two agree here — the
	// point is that the arm is decided by the record, not by the step.
	assert.Equal(t, []string{"unhandled"}, coverage.Unreached)
}

// TestASwitchInALoopBodyIsMeasuredToo pins the walk rather than the rule. A
// switch's outputs travel inside the enclosing loop's `results` list as a CEL map
// literal rather than at the top level, so a walk that only reads the top-level
// transcript reports every arm of it unreached — a false diagnostic on a file
// that covers everything, which is worse than the miss this feature fixes.
func TestASwitchInALoopBodyIsMeasuredToo(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `edition: v2026.3
name: looped-arms
steps:
  - id: each
    for_each:
      items: ${['a', 'b']}
      as: item
      steps:
        - id: route
          switch:
            value: ${item}
            cases:
              - case: a
                steps:
                  - id: did_a
                    log:
                      message: a
              - case: b
                steps: []
            default:
              steps:
                - id: fell_through
                  log:
                    message: other
`)
	path := dir + "/looped.test.yaml"
	writeFile(t, path, `edition: v2026.3
tests:
  - name: both items are routed
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      failed: false
`)

	_, coverages := flowtest.RunFileWithCoverage(path)
	require.Len(t, coverages, 1)
	coverage := coverages[0]

	assert.True(t, armByKey(t, coverage, "route:case[0]").Reached, "iteration `a` took the first arm")
	assert.True(t, armByKey(t, coverage, "route:case[1]").Reached,
		"iteration `b` took the empty arm, which lives only in the loop's results")
	assert.False(t, armByKey(t, coverage, "route:default").Reached, "no item fell through")
}
