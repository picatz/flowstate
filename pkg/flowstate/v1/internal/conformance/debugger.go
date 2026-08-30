package conformance

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// DebuggerCase is a workflow, the answer it must produce, and the step
// boundaries a debugger is offered while it runs.
//
// The two halves are deliberately asymmetric, because the drivers are. See
// [DebuggerCases].
type DebuggerCase struct {
	Case

	// Offered is the step ids a debugger is asked about, in the order it is
	// asked — one entry per *offer*, so a loop body appears once per
	// iteration rather than once.
	//
	// A sequence rather than a set, and every case here runs its steps
	// sequentially so that the sequence is a fact rather than a race. A
	// `for_each:` with `max_parallel:` above one offers its body from several
	// goroutines and the order is the schedule's business; asserting one would
	// be asserting something neither driver promises.
	Offered []string

	// Held is the step ids a *durable* debug lease can hold this run at, in
	// order — #928's stage 2.
	//
	// Always a subsequence of [Offered], never a superset, and the two callers
	// hold it to that from opposite sides: the local one asserts every id here
	// was really offered, and the durable one asserts a lease takes effect
	// where this says it does. A durable driver holding somewhere the local
	// driver does not stop would be the drivers disagreeing about which
	// boundaries a run has, which is exactly what this corpus exists to catch.
	//
	// # Why it is ever shorter
	//
	// A lease names one holder and one position, and the durable driver takes
	// one only where the run *has* a single representable position — the
	// `susp == 0` boundaries, the same set Continue-As-New may suspend at and
	// the same set `RunProgress` answers about. A `switch:` arm, a `parallel:`
	// branch and a loop body are all at several positions at once as far as
	// suspension is concerned, and [v1.DebugPosition] carries no `path` for
	// precisely that reason ("a position that needed one would be a run held in
	// two places, which is not a state this seam can be in", debug.proto).
	//
	// The local driver has no such constraint: it holds a goroutine, so it can
	// stop each branch where that branch is. So the asymmetry is a real
	// property of the two mechanisms rather than a gap, and it is written down
	// here — beside the list it differs from — rather than left to be
	// rediscovered by whoever notices the two counts do not match.
	Held []string
}

// DebuggerCases hold both drivers to one answer about a run that is being
// debugged, and hold the local driver to which boundaries it offers.
//
// # Why this corpus is shaped unlike its neighbours
//
// [v1.Debugger] is a local-driver seam and will stay one until #928's second
// slice: pausing a durable run is a different mechanism for a different reason,
// and a per-process callback would hold one worker's goroutine while the run
// itself is free to continue on another ([v1.Debugger] says so at length).
// `observe.go` states the rule that makes that legitimate rather than a
// violation — the both-drivers rule governs what a *workflow* can observe, and
// no workflow can observe its observer.
//
// So the corpus asks each driver the question it can answer:
//
//   - **Both drivers** run these workflows and must produce
//     [Case.ExpectedOutputs]. That is the claim that matters and the one a
//     debugger could break: a session may hold a run, and may end it, but it
//     may never change what the run computes. `debugger.go` names that as the
//     one thing a debugger must never do — a debugger turning a red case green
//     — and the local caller runs each case *twice*, once plain and once with a
//     session stepping through every boundary, asserting the same outputs both
//     times. The durable driver runs the same workflows with no debugger at
//     all, which is what makes "the same answer" a cross-driver fact rather
//     than one driver agreeing with itself.
//   - **The local driver alone** is held to [DebuggerCase.Offered].
//
// Written now rather than during slice 2, and that is the whole argument for
// this file existing before the feature it describes. The conformance package
// is where an agreement between the drivers is *stated*; an asymmetry that is
// nowhere written down is indistinguishable from an oversight, and the cost of
// discovering during slice 2 that the two drivers had quietly disagreed about
// which boundaries exist is the cost this package was built to avoid. #1111
// item 12.
func DebuggerCases() []DebuggerCase {
	return []DebuggerCase{
		{
			// The ordinary path, and the baseline the other two are read
			// against: a debugger sees each step where its author wrote it.
			Case: Case{
				Name: "steps are offered in the order they are written",
				Workflow: &v1.Workflow{
					Name:    "debug-order",
					Profile: v1.CurrentProfile,
					Steps: []*v1.Node{
						says("first", "one"),
						says("second", "two"),
						says("third", "three"),
					},
				},
				ExpectedOutputs: held("first", "second", "third"),
			},
			Offered: []string{"first", "second", "third"},
			Held:    []string{"first", "second", "third"},
		},
		{
			// The sharp one. [v1.Debugger.BeforeStep] is documented as being
			// called *after* the condition decided the step runs, and the call
			// site honours it — `eval.go` evaluates the condition, `continue`s
			// on a skip, and offers the boundary only below that.
			//
			// A driver that offered a skipped step would stop an author at a
			// step that is not going to run, and `inspect` there would answer
			// about a scope no work will ever be done in. It is also the
			// difference between "the debugger shows the workflow" and "the
			// debugger shows the run", and only the second is useful.
			Case: Case{
				Name: "a step the condition skipped is never offered",
				Workflow: &v1.Workflow{
					Name:    "debug-skip",
					Profile: v1.CurrentProfile,
					Steps: []*v1.Node{
						says("before", "one"),
						guarded("skipped", "false", "never"),
						says("after", "two"),
					},
				},
				// Absent rather than present and empty, which is the ordinary
				// `if:` rule this corpus inherits rather than restates.
				ExpectedOutputs: held("before", "after"),
			},
			Offered: []string{"before", "after"},

			// The same two, because a skipped step is not a boundary on either
			// driver: the condition decides first, and only a step that is
			// going to run reaches the seam. A lease held at a step nothing
			// will do would stop a run at a place it is not.
			Held: []string{"before", "after"},
		},
		{
			// Once per iteration, which is the claim a set could not make and
			// a count of distinct steps would get wrong. A session stepping
			// through a loop stops three times in a three-item loop, because
			// that is what the run does — and a driver offering the body once
			// would be describing the *text* rather than the execution.
			//
			// `max_parallel: 1` so the sequence is deterministic; see
			// [DebuggerCase.Offered].
			Case: Case{
				Name: "a loop is offered once and its body once per iteration",
				Workflow: &v1.Workflow{
					Name:    "debug-loop",
					Profile: v1.CurrentProfile,
					Steps: []*v1.Node{
						{
							Id: "each",
							Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
								Items:       v1.NewExpr(`["a", "b", "c"]`),
								MaxParallel: 1,
								Body:        []*v1.Node{says("touch", "visited")},
							}},
						},
					},
				},
				// A loop records one entry per iteration under
				// [v1.LoopResultsField], each holding that iteration's own step
				// outputs — so a three-item loop over one `log:` step is three
				// maps of one empty entry.
				//
				// Stated exactly rather than loosely, even though this corpus is
				// about boundaries and not about loop encoding. The encoding is a
				// cross-driver contract like any other, and a case in this package
				// that declined to pin it would be the one place the two drivers
				// could quietly diverge while a test watched.
				ExpectedOutputs: withStep(held(), "each", map[string]*v1.Value{
					v1.LoopResultsField: v1.NewLiteralList(
						map[string]any{"touch": map[string]any{}},
						map[string]any{"touch": map[string]any{}},
						map[string]any{"touch": map[string]any{}},
					),
				}),
			},
			Offered: []string{"each", "touch", "touch", "touch"},

			// The loop, and not its body. A `for_each:` body runs at a deeper
			// suspend level than the step that declares it — the engine will
			// not Continue-As-New inside one — so it has no representable
			// position for a lease to name. The local driver stops three times
			// here and a durable lease stops once, which is the asymmetry
			// [DebuggerCase.Held] exists to state rather than leave for
			// somebody to find.
			Held: []string{"each"},
		},
		{
			// The asymmetry with nothing else going on, so that the difference
			// between the two lists is the whole of what this case is about.
			//
			// A `switch:` is the sequential member of the family — one arm
			// runs, deterministically, so the offers are a fact rather than a
			// race — and its body is at a deeper suspend level exactly as a
			// loop's is. Written with a `parallel:` this case could not state
			// an order at all; written with a `for_each:` it would repeat the
			// one above.
			Case: Case{
				Name: "a switch is a boundary and the arm it takes is not",
				Workflow: &v1.Workflow{
					Name:    "debug-switch",
					Profile: v1.CurrentProfile,
					Steps: []*v1.Node{
						says("before", "one"),
						{
							Id: "route",
							Kind: &v1.Node_Switch{Switch: &v1.Switch{
								Value: v1.NewLiteral("go"),
								Cases: []*v1.Switch_Case{{
									Values: []*v1.Value{v1.NewLiteral("go")},
									Steps:  []*v1.Node{says("chosen", "two")},
								}},
							}},
						},
					},
				},
				// A `switch:` records which arm it took beside the value it
				// matched, so the step that is not a pause point still has
				// outputs. Stated exactly rather than loosely, for the reason
				// the loop case above states its encoding: this corpus is
				// about boundaries, and a case that declined to pin what a
				// step produced would be a place the two drivers could
				// quietly diverge while a test watched.
				ExpectedOutputs: withStep(held("before", "chosen"), "route", map[string]*v1.Value{
					"value": v1.NewLiteral("go"),
					"case":  v1.NewLiteral("go"),
				}),
			},
			Offered: []string{"before", "route", "chosen"},
			Held:    []string{"before", "route"},
		},
	}
}
