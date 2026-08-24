package flowstatev1

import "fmt"

// MaxAtomicBlockActivities bounds how many task activities one
// suspension-opaque stretch of a run may schedule: the ceiling on the
// items × body product of a `for_each` that runs with no Continue-As-New
// seam inside it.
//
// A sequential top-level `for_each` needs no such bound, because it is paced:
// every body step counts against the run's step budget, and the loop offers a
// Continue-As-New seam at every iteration boundary, where the engine also
// consults Temporal's own history-pressure hint. Set `max_parallel:` on the
// same loop — or write it inside a `parallel:` branch, another loop's body, or
// a `switch:` arm, all of which run at a suspend depth where no seam exists —
// and all of that pacing disappears at once: the whole fan-out runs as one
// atomic segment of history. Nothing else bounds what that segment
// accumulates. [MaxForEachItems] bounds the trip count at 1,000 and
// [MaxLoopResultsBytes] bounds the result bytes, but 1,000 items over a
// 60-step body — every number at or under a bound this repository enforces —
// is 60,000 activities in one execution, roughly 180,000 history events
// against the 51,200-event hard cap at which Temporal force-terminates the
// run. Termination delivers no workflow task, so the run's compensation log
// never executes: a saga that provisioned resources across the completed part
// of the block simply stops, with registered undos it will never run. Failing
// a little early with a diagnosis beats that ending by the whole of size.go's
// argument, so the product is weighed before the first iteration is
// dispatched.
//
// The number is sized from the history math, spending headroom in the safe
// direction exactly as size.go's reserves do — and sized for the *worst*
// dispatch shape the atomic placements admit, which is sequential. Temporal
// terminates an execution at 51,200 history events. An activity costs three
// of its own (scheduled, started, completed), and a sequential atomic
// stretch — a plain `for_each` inside a `parallel:` branch, a loop body or a
// `switch:` arm — pays a workflow-task triplet per activity on top, because
// each completion wakes the workflow to dispatch the next: the checked-in
// replay recording of three sequential tasks
// (engine/testdata/replay/2026-08-08/multi-step-tasks.json) is 23 events,
// seven-ish per activity. 5,000 activities at that ratio is roughly 35,000
// events — under the cap with room for whatever the segment ran before the
// loop, and far past any fan-out that should be one atomic block rather
// than a paged workload.
//
// A compiled-in constant rather than configuration, for [MaxRunStateBytes]'s
// reason: it is read in workflow code, so it is a determinism input. And a
// hard ceiling rather than a raisable default, for [MaxForEachItems]'s
// reason: the author has said only how long a list and a body are, and a
// workload genuinely this large pages its items across runs — the shape
// examples/paged-fan-out demonstrates — rather than holding one history
// segment open across all of them.
const MaxAtomicBlockActivities = 5_000

// atomicBlockSaturated is the value the worst-case walk saturates at: one
// past the ceiling, because every count past the ceiling is refused the same
// way and capping there keeps the multiplications below from overflowing —
// nested loops multiply their ceilings, and a few levels of that overflows
// int long before the walk's node bound stops it.
const atomicBlockSaturated = MaxAtomicBlockActivities + 1

// AtomicBlockActivitiesError is the failure a `for_each` reports when it
// would run as one suspension-opaque stretch of history and the product of
// its resolved items and its body's worst-case activity count is past
// [MaxAtomicBlockActivities].
//
// One constructor called by both drivers at the point each learns the
// resolved list's length, the same discipline [ForEachItemCountError] holds
// its cross-driver sentence to — and like it, the step's id is added by each
// driver's runNodes on the way out rather than spelled here. What the
// composed sentence must carry, and what both drivers' shared cases assert
// it carries, is the step, the item count, the per-iteration count, and the
// ceiling.
//
// Not retryable, for [ForEachItemCountError]'s reason: the same items
// expression over the same body counts the same on every replay, so there is
// no attempt that would succeed where the last one failed.
func AtomicBlockActivitiesError(items, perIteration, max int) error {
	return fmt.Errorf(
		"for_each would run all %d of its iterations as one atomic stretch of history — it declares "+
			"`max_parallel:`, or runs inside a `parallel:` branch, another loop's body, or a `switch:` arm, "+
			"where no Continue-As-New seam exists — and its body can schedule up to %d activities per "+
			"iteration, which does not fit the ceiling of %d activities one such stretch may schedule; "+
			"run the loop sequentially at the top level so it paces itself across history segments, narrow "+
			"what `items:` produces, shrink the body, or page the work across several runs",
		items, perIteration, max)
}

// CheckAtomicBlockActivities refuses a `for_each` whose resolved items,
// multiplied by its body's worst-case activity count, cannot fit one
// suspension-opaque history segment.
//
// Both drivers call it at the point they call [CheckForEachItems] — right
// after [ResolveItems], before a single iteration is dispatched — and only
// for a `for_each` that will run atomically: one that declares
// `max_parallel:` greater than one, or one reached at a suspend depth above
// zero (inside a `parallel:` branch, another loop's body, or a `switch:`
// arm). A sequential top-level `for_each` keeps its existing behavior,
// because it suspends between iterations and already reads the server's
// Continue-As-New suggestion there. The local driver enforces it too, though
// it runs iterations sequentially and has no history to protect, because a
// fan-out the rehearsal admits and production refuses is a disagreement
// between the drivers about what the file means — [CheckForEachItems]'s
// exact reasoning.
//
// The body is weighed at its static worst case: every task counts whatever
// its `if:` would decide — twice when it declares an `undo:`, since the
// compensation is a second activity the same execution schedules when the
// run unwinds — a wait counts one, because a durable timer's events are
// history too, a `switch:` counts its widest arm, a `call:` counts its
// callee's steps, and a nested loop counts its iteration ceiling times its
// own body ([MaxForEachItems] for a nested `for_each`, whose trip count is
// an expression this walk cannot evaluate; [LoopMaxIterations] for a nested
// `loop:`). That refuses some specifications whose taken branches
// would have fit, which is size.go's trade made on size.go's grounds: the
// alternative to refusing a little early is not "it fits", it is a
// termination that skips compensation. Fails closed in the other direction
// too — a walk that exhausts its node bound reports the saturated count
// rather than whatever it had seen so far.
func CheckAtomicBlockActivities(items int, body []*Node) error {
	per := WorstCaseBodyActivities(body)
	if satMul(items, per) > MaxAtomicBlockActivities {
		return AtomicBlockActivitiesError(items, per, MaxAtomicBlockActivities)
	}
	return nil
}

// WorstCaseBodyActivities counts how many task activities one pass over a
// node list can schedule, at static worst case, saturating at one past
// [MaxAtomicBlockActivities] — every count past the ceiling is refused
// identically, and saturating is what keeps nested ceilings from
// overflowing when they multiply.
//
// Bounded the way [CheckLoopNesting]'s walk is, and for the same reason —
// it runs over a [Workflow] an untrusted caller composed: by
// [maxStructureWalkNodes] total nodes visited across every callee entered
// (exhaustion saturates, which fails closed), and by [MaxCallDepth] for how
// many calls deep it follows, since a call past that depth is refused at
// execution by [CheckCallDepth] before anything under it could run.
func WorstCaseBodyActivities(body []*Node) int {
	nodesLeft := maxStructureWalkNodes
	return worstCaseActivities(body, &nodesLeft, 0)
}

func worstCaseActivities(nodes []*Node, nodesLeft *int, callDepth int) int {
	total := 0
	for _, node := range nodes {
		if *nodesLeft <= 0 {
			return atomicBlockSaturated
		}
		*nodesLeft--

		switch kind := node.GetKind().(type) {
		case *Node_Task:
			// Counted whatever the step's `if:` would decide: a condition is
			// evaluated per iteration against a scope this walk cannot see,
			// so the worst case is that every guard holds. A task declaring
			// `undo:` counts twice, because its compensation is a second
			// activity scheduled in the same execution when the run unwinds
			// — and the unwind is precisely what this bound exists to keep
			// reachable: a fan-out sized so that only its forward half fits
			// recreates the termination-skips-compensation ending during the
			// rollback itself.
			total = satAdd(total, 1)
			if node.GetUndo() != nil {
				total = satAdd(total, 1)
			}
		case *Node_Wait:
			// Not an activity, but not free history either: a durable sleep
			// is a Temporal timer (started and fired are both events), and a
			// signal wait records what wakes it. Counted as one so a body of
			// waits cannot multiply an unbounded number of timers into one
			// atomic stretch under a bound that assumed fixed headroom
			// covered them.
			total = satAdd(total, 1)
		case *Node_ForEach:
			// A nested for_each's trip count is an expression, unknowable
			// here, so its own ceiling is the worst case. The nested loop is
			// also weighed against this same bound when its own items
			// resolve — it runs at a suspend depth above zero — but by then
			// the enclosing loop has already dispatched, which is why the
			// enclosing product has to assume the worst now.
			total = satAdd(total, satMul(MaxForEachItems, worstCaseActivities(kind.ForEach.GetBody(), nodesLeft, callDepth)))
		case *Node_Loop:
			total = satAdd(total, satMul(LoopMaxIterations(kind.Loop), worstCaseActivities(kind.Loop.GetBody(), nodesLeft, callDepth)))
		case *Node_Parallel:
			for _, branch := range kind.Parallel.GetBranches() {
				total = satAdd(total, worstCaseActivities(branch.GetSteps(), nodesLeft, callDepth))
			}
		case *Node_Switch:
			// Exactly one body runs per pass, so the widest arm is the worst
			// case — through [SwitchBodies], so the default arm is weighed
			// alongside the cases rather than quietly skipped.
			widest := 0
			for _, body := range SwitchBodies(kind.Switch) {
				if n := worstCaseActivities(body, nodesLeft, callDepth); n > widest {
					widest = n
				}
			}
			total = satAdd(total, widest)
		case *Node_Call:
			callee := kind.Call.GetWorkflow()
			if callee == nil {
				continue
			}
			nextDepth := callDepth + 1
			if CheckCallDepth(nextDepth) != nil {
				// Refused at execution by CheckCallDepth itself, so what sits
				// beneath it is work nothing will ever schedule.
				continue
			}
			// A callee declaring `vars:` costs one activity of its own on the
			// durable driver — the engine evaluates them through a
			// WorkflowVars activity on every fresh call — so a call inside a
			// loop body multiplies that activity by the trip count exactly as
			// it multiplies the callee's steps.
			if len(callee.GetVars()) > 0 {
				total = satAdd(total, 1)
			}
			// A callee runs atomically at the caller's suspend level — the
			// fact CheckLoopNesting's walk records the same way — so its
			// steps join the block being weighed.
			total = satAdd(total, worstCaseActivities(callee.GetSteps(), nodesLeft, nextDepth))
		}
		// A value node counts nothing: it is an expression evaluated in
		// workflow code, writing no command into history.

		if total >= atomicBlockSaturated {
			return atomicBlockSaturated
		}
	}
	return total
}

// satAdd and satMul are the walk's arithmetic, saturating at
// [atomicBlockSaturated] so nested ceilings cannot overflow however they
// compose — everything past the refusal point behaves the same, so nothing
// above it needs representing.
func satAdd(a, b int) int {
	if s := a + b; s < atomicBlockSaturated {
		return s
	}
	return atomicBlockSaturated
}

func satMul(a, b int) int {
	if a == 0 || b == 0 {
		return 0
	}
	if a > atomicBlockSaturated/b {
		return atomicBlockSaturated
	}
	if p := a * b; p < atomicBlockSaturated {
		return p
	}
	return atomicBlockSaturated
}
