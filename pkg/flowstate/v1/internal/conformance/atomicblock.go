package conformance

import (
	"strconv"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// atomicActivityCeiling mirrors [v1.MaxAtomicBlockActivities] the way
// [forEachTripCeiling] mirrors the trip-count bound: the real constant is
// exported and read here, and the arithmetic below straddles whatever it is.
const atomicActivityCeiling = v1.MaxAtomicBlockActivities

// atomicBodySteps is the per-iteration body size these cases fan out over.
// Chosen so that the ceiling divides evenly: atomicActivityCeiling /
// atomicBodySteps items lands the product exactly at the ceiling, and one
// item more is the smallest over-ceiling product this body shape can express
// — the product's granularity is the body size, so "one past" in the bound's
// own unit is one more item.
const atomicBodySteps = 20

// atomicCeilingItems is the trip count that lands items × body exactly on the
// ceiling.
const atomicCeilingItems = atomicActivityCeiling / atomicBodySteps

// ForEachAtomicBlockCases are the shared cases holding both drivers to the
// suspension-opaque fan-out ceiling, [v1.MaxAtomicBlockActivities]: the bound
// on the items × body-activities product of a `for_each` that runs with no
// Continue-As-New seam inside it.
//
// The gap they close is the concurrent half of the one [ForEachTripCountCases]
// closed. A sequential top-level `for_each` is paced — every body step counts
// against the step budget and every iteration boundary is a seam — but a
// `for_each` with `max_parallel:` above one, or one written inside a
// `parallel:` branch, runs its whole fan-out as one atomic stretch of
// history, and 1,000 items over a 60-step body — every number inside the
// bounds this repository enforces — is more history than Temporal will store:
// the server force-terminates the run mid-loop, skipping the compensation log
// entirely. So the product is weighed before the first iteration is
// dispatched, on both drivers, through [v1.CheckAtomicBlockActivities].
//
// Run by both the local driver (eval_test.go's
// TestRunWorkflowAtomicBlockBound) and the durable driver (engine
// workflow_test.go's identically-named test). Four claims, in the shape
// CLAUDE.md's List lesson asks for — the bound is asserted *reached* as well
// as not exceeded, in both placements the engine treats as atomic:
//
//   - A concurrent `for_each` whose product is exactly the ceiling runs, and
//     the case asserts every iteration was recorded.
//   - The same loop with one more item is refused, before any iteration runs,
//     with a sentence naming the step, the item count, the per-iteration
//     count and the ceiling — asserted identically by both drivers' halves.
//   - A sequential `for_each` inside a `parallel:` branch — atomic because a
//     branch cannot suspend, not because of `max_parallel:` — is held to the
//     identical ceiling: exactly-at runs, one item past is refused.
//
// The body is [atomicBodySteps] task steps each held back by `if: ${false}`,
// for [ForEachTripCountCases]'s two reasons: an iteration costs no activity
// on either driver, and a guard the walk cannot evaluate is the honest shape
// of the bound — [v1.WorstCaseBodyActivities] counts a task whatever its
// `if:` would decide, so these cases also pin that the weighing is static
// worst case rather than a count of what ran.
func ForEachAtomicBlockCases() []Case {
	return []Case{
		{
			Name:     "a concurrent for_each at the atomic-activity ceiling runs every item",
			Workflow: fansOutAtomically("at-atomic-ceiling", atomicCeilingItems, 2, false),
			ExpectedOutputsPredicate: func(out *v1.Workflow_StepOutputs) bool {
				return forEachResultsLen(out, "fan") == atomicCeilingItems
			},
		},
		{
			Name:          "a concurrent for_each one item past the atomic-activity ceiling is refused",
			Workflow:      fansOutAtomically("past-atomic-ceiling", atomicCeilingItems+1, 2, false),
			ExpectFailure: true,
		},
		{
			Name:     "a for_each inside a parallel branch at the atomic-activity ceiling runs every item",
			Workflow: fansOutAtomically("at-atomic-ceiling-in-branch", atomicCeilingItems, 0, true),
			ExpectedOutputsPredicate: func(out *v1.Workflow_StepOutputs) bool {
				return forEachResultsLen(out, "fan") == atomicCeilingItems
			},
		},
		{
			Name:          "a for_each inside a parallel branch one item past the atomic-activity ceiling is refused",
			Workflow:      fansOutAtomically("past-atomic-ceiling-in-branch", atomicCeilingItems+1, 0, true),
			ExpectFailure: true,
		},
	}
}

// AtomicBlockRefusalSubstrings are what both drivers' failing halves assert
// the composed sentence carries: the item count observed, the body's
// per-iteration worst case, and the ceiling. The step id is asserted
// separately by each runner, because the two placements compose it
// differently (`step "fan"` bare, or under `branch 0:`).
func AtomicBlockRefusalSubstrings() []string {
	return []string{
		strconv.Itoa(atomicCeilingItems + 1),
		strconv.Itoa(atomicBodySteps),
		strconv.Itoa(atomicActivityCeiling),
	}
}

// fansOutAtomically builds a `for_each` with id "fan" over n items whose body
// is [atomicBodySteps] task steps all held back by `if: ${false}`, declaring
// maxParallel when it is above zero, and wrapped as the sole branch of a
// `parallel:` when inBranch is set — the two placements the engine runs with
// no Continue-As-New seam.
//
// The items come from [rangeExpr], the same list [iteratesN] fans out over,
// so the atomic cases differ from the trip-count cases in what they measure
// rather than in how they produce items.
func fansOutAtomically(name string, n int, maxParallel int32, inBranch bool) *v1.Workflow {
	body := make([]*v1.Node, 0, atomicBodySteps)
	for i := 0; i < atomicBodySteps; i++ {
		body = append(body, &v1.Node{
			Id:        "body-" + strconv.Itoa(i),
			Condition: v1.NewExpr("false"),
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name: "http",
				Inputs: map[string]*v1.Value{
					"method": v1.NewLiteral("GET"),
					// Never reached: the condition holds every step back on
					// every iteration, so a case that somehow ran a body would
					// fail loudly here rather than quietly making requests.
					"url": v1.NewLiteral("http://127.0.0.1:1/never-reached"),
				},
			}},
		})
	}

	fan := &v1.Node{
		Id: "fan",
		Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
			Items:       v1.NewExpr(rangeExpr(n)),
			MaxParallel: maxParallel,
			Body:        body,
		}},
	}

	steps := []*v1.Node{fan}
	if inBranch {
		steps = []*v1.Node{
			{
				Id: "block",
				Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{
					Branches: []*v1.Parallel_Branch{{Steps: []*v1.Node{fan}}},
				}},
			},
		}
	}

	return &v1.Workflow{
		Name:    name,
		Profile: v1.CurrentProfile,
		Steps:   steps,
	}
}
