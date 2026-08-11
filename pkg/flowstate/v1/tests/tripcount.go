package tests

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// forEachTripCeiling mirrors [v1.MaxForEachItems] the same way
// [forEachResultsByteBound] mirrors the byte bound: the real constant is
// exported, and reading it here keeps these cases straddling whatever it is,
// while the arithmetic below (exactly at it, exactly one past it) is what pins
// the boundary itself rather than some number near it.
const forEachTripCeiling = v1.MaxForEachItems

// ForEachTripCountCases are the shared cases holding both drivers to the
// `for_each` trip-count ceiling, [v1.MaxForEachItems].
//
// The gap they close: a `for_each` was bounded by `max_parallel:` (how many
// iterations at once), by [v1.MaxLoopResultsBytes] (what the iterations
// accumulate) and by the run's step budget (which suspends rather than refuses),
// and by nothing at all on the one quantity that decides how much work the step
// is: how many items the list has. A body that reports little or nothing never
// reaches the byte bound however many times it runs, which is exactly the shape
// the body below has.
//
// Run by both the local driver ([flowstatev1] eval_test.go's
// TestRunWorkflowForEachTripCount) and the durable driver (engine
// workflow_test.go's identically-named test), which is what makes "a list at the
// ceiling runs and a list one past it is refused" something the two cannot
// disagree about.
//
// Three claims, in the shape CLAUDE.md's List lesson asks for:
//
//   - A list of exactly [v1.MaxForEachItems] items runs, and the case asserts the
//     step reported exactly that many iterations. Asserting the count is what
//     makes this a claim that the bound was *reached* rather than merely not
//     exceeded: a driver that gave up part way through, or that refused one item
//     early, fails here, where an assertion that the run merely succeeded would
//     pass quietly.
//   - A list of exactly one more is refused, on both drivers. Each driver's half
//     asserts the sentence names the step, the observed count and the ceiling.
//   - The refusal lands before any iteration runs, which is why the over-ceiling
//     case expects an outright failure rather than a short `results`.
//
// The body is a single step held back by `if: ${false}`, so an iteration costs no
// activity on either driver. That is deliberate twice over: it keeps a run of a
// thousand iterations an ordinary test rather than a slow one, and it is the
// honest shape of the gap, since a workload that accumulates nothing is one the
// byte bound cannot see at all.
func ForEachTripCountCases() []Case {
	return []Case{
		{
			Name:     "a for_each at the trip-count ceiling runs every item",
			Workflow: iteratesN("at-trip-ceiling", forEachTripCeiling),
			ExpectedOutputsPredicate: func(out *v1.Workflow_StepOutputs) bool {
				return forEachResultsLen(out, "fan") == forEachTripCeiling
			},
		},
		{
			Name:          "a for_each one item past the trip-count ceiling is refused",
			Workflow:      iteratesN("past-trip-ceiling", forEachTripCeiling+1),
			ExpectFailure: true,
		},
	}
}

// iteratesN builds a one-step `for_each` over a list of n items whose body does
// no work: the step inside is held back by `if: ${false}`, so the iteration is
// real (it is counted, and it records an entry in `results`) while costing no
// activity on either driver.
//
// The list is [rangeExpr]'s, the same one [fetchesBytesEach] fans out over, so
// the two bound-shaped case sets differ in what they measure rather than in how
// they produce items.
func iteratesN(name string, n int) *v1.Workflow {
	return &v1.Workflow{
		Name:    name,
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{
			{
				Id: "fan",
				Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
					Items: v1.NewExpr(rangeExpr(n)),
					Body: []*v1.Node{
						{
							Id:        "body",
							Condition: v1.NewExpr("false"),
							Kind: &v1.Node_Task{Task: &v1.Task{
								Name: "http",
								Inputs: map[string]*v1.Value{
									"method": v1.NewLiteral("GET"),
									// Never reached: the condition above holds the
									// step back on every iteration, so a case that
									// somehow ran the body would fail loudly here
									// rather than quietly making a request.
									"url": v1.NewLiteral("http://127.0.0.1:1/never-reached"),
								},
							}},
						},
					},
				}},
			},
		},
	}
}
