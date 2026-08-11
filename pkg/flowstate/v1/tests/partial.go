package tests

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// PartialTranscriptCase is a workflow that fails, paired with the record its
// driver must hand back alongside the failure ([v1.PartialTranscript]).
//
// A separate type from [Case] because what it asserts is the *failed* run's
// outputs, and [Case.ExpectedOutputs] is explicitly ignored when
// [Case.ExpectFailure] is set — a case whose point is the failure had nothing to
// compare, which is exactly the gap issue #453 is about.
type PartialTranscriptCase struct {
	// Name of the case, used for test identification.
	Name string

	// Workflow is the definition to run. Every one of these fails on purpose.
	Workflow *v1.Workflow

	// Expected is the whole transcript the failed run must hand back: every step
	// it recorded outputs for, and the step it stopped on. Compared exactly rather
	// than as a subset, so a driver that carries *more* than the other one fails
	// here too — the direction a "reached at least these" assertion cannot see, and
	// the one that would let a rehearsal credit a branch production never records.
	Expected *v1.Workflow_StepOutputs
}

// PartialTranscriptCases cover what a run that fails hands back about what it
// did, which both drivers must answer identically (invariant 3).
//
// Neither driver used to answer at all: a failed run returned no step outputs, so
// `flow test`'s coverage counted a case whose whole point was `expect.failed:
// true` as reaching none of the steps it had just exercised, and an author had to
// record a `coverage.allow_unreached` reason for a branch that really ran (issue
// #453). The record itself was never missing — both drivers accumulate it as they
// walk — only unreturned.
//
// The three shapes here are the three questions a transcript can get wrong:
//
//   - what ran before the failure, and the failing step itself. The step that
//     ended the run is recorded through [v1.FailedStepOutputs], the same shape a
//     step tolerated by `continue_on_error:` is recorded in, because it is the
//     same fact: this step ran and failed. Recording it one step short of the
//     truth is the version of this bug that survives a naive fix.
//   - what never ran. A step skipped by its `if:` before the failure, and every
//     step after the failure, are absent — the transcript is a record of what
//     happened, and absence is how it says a step did not.
//   - what a nesting that did not finish contributes, which is nothing. A loop's
//     per-iteration outputs only reach the transcript when the loop node completes
//     and writes its own `results`, so a body step that ran inside the iteration
//     that failed is *not* in the record. Both drivers keep it that way, and a
//     driver that reached into the unfinished nesting would be inventing a record
//     the other one does not have.
//
// The failure in each is a step's own `vars:` indexing past the end of a list:
// it compiles, it fails when evaluated, it carries no TaskError, and it needs no
// server — so the recorded sentence is the same one [ToleratedStepFailureCases]
// already pins for the tolerated version of the identical failure.
func PartialTranscriptCases() []PartialTranscriptCase {
	const oops = "['a'][5]"
	const recorded = `var "bad": evaluate expression: index out of bounds: 5`

	// boom is a step that always fails and is never tolerated, so it is always the
	// step that ends the run it appears in.
	boom := func(id string) *v1.Node {
		return withVars(says(id, "unreachable"), map[string]*v1.Value{
			"bad": v1.NewExpr(oops),
		})
	}

	return []PartialTranscriptCase{
		{
			Name: "the steps before an untolerated failure are recorded, and so is the step that failed",
			Workflow: &v1.Workflow{
				Name: "partial-before-failure",
				Steps: []*v1.Node{
					says("first", "ran"),
					says("second", "ran"),
					boom("boom"),
					// Never reached, so never recorded: the assertion that the
					// transcript stops where the run did.
					says("after", "unreachable"),
				},
			},
			Expected: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"first":  {},
				"second": {},
				"boom":   v1.FailedStepOutputs(recorded),
			}},
		},
		{
			Name: "a step skipped by its condition before the failure stays absent",
			Workflow: &v1.Workflow{
				Name: "partial-skipped-branch",
				Steps: []*v1.Node{
					says("taken", "ran"),
					guarded("not_taken", "false", "unreachable"),
					boom("boom"),
				},
			},
			Expected: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"taken": {},
				"boom":  v1.FailedStepOutputs(recorded),
			}},
		},
		{
			Name: "a loop that fails mid-body contributes only the loop step, never the body",
			Workflow: &v1.Workflow{
				Name: "partial-unfinished-loop",
				Steps: []*v1.Node{
					says("before", "ran"),
					{
						Id: "each",
						Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
							Items: v1.NewLiteralList("one"),
							Body: []*v1.Node{
								// This one genuinely runs, inside an iteration that
								// never finishes — so it is not in the record, and
								// that is the claim.
								says("inside", "ran"),
								boom("inside_boom"),
							},
						}},
					},
				},
			},
			Expected: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"before": {},
				// The position the failure passed out through, prefixed by the loop
				// level exactly as a tolerated `for_each` body failure is.
				"each": v1.FailedStepOutputs(`iteration 0: step "inside_boom": ` + recorded),
			}},
		},
	}
}
