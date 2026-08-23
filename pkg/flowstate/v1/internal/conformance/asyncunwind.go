package conformance

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The two defects #477 slice 0's schedule search found, pinned by name.
//
// The search found them and the search must not be what guards them. A bounded
// random walk over seeds is the right instrument for *discovering* an
// interleaving that breaks an invariant and the wrong one for *keeping* it:
// change this corpus, change the default seed, or reorder a case, and the
// schedule that exposed a defect may never be generated again. The regression
// would come back silently while the simulation reported green, having explored
// somewhere else entirely.
//
// So each defect gets a case that states the claim directly, and a way to reach
// the path every time with no seed involved. Both drivers run these. The local
// driver runs them a second time under [v1.AdversarialOrder] — the fixed
// schedule furthest from written order — because both defects lived on a path
// this driver only takes when something departs from written order, and a
// deterministic departure is what makes the pin a pin.

// AsyncUnwindCases are the shared cases for what a scope owes the work it
// started when it is on its way out failing.
//
// [UndoCase] like the rest of the saga corpus, asserted through
// [AssertRecorded] against a real recording server, because both claims are
// about effects and their order rather than about the engine's account of
// itself. [UndoCase.UnorderedPrefix] draws the same line it always does: which
// concurrent request reaches the server first is the schedule's to choose, and
// what comes after the prefix is the claim.
func AsyncUnwindCases(base string) []UndoCase {
	return []UndoCase{
		{
			// Defect 1. A scope leaving on a failure has to finish the async work
			// it started, or an implementation free to hold that work back never
			// runs it: an effect the file launched that never happened, and a
			// compensation that never registered because the step that would have
			// registered it never succeeded.
			//
			// The durable driver has always owed this and says so in
			// asyncStep.wait — the work must be finished before the scope can be
			// left. The local driver owed it the moment its own async launch became
			// a scheduling choice, and did not do it.
			//
			// Deterministic without a seed on both drivers. Durably the coroutine
			// is genuinely running and the scope's exit has to wait for it.
			// Locally under [v1.AdversarialOrder] the work is held at its launch
			// without exception, so `boom` fails with `a` outstanding every time
			// rather than whenever a seed says so.
			Name: "a scope failing with async work outstanding still runs it and takes it back",
			Workflow: &v1.Workflow{Name: "undo-async-unwind-drain", Profile: v1.CurrentProfile, Steps: []*v1.Node{
				async(undoing(records("a", base, "a"), base, "/do/undo")),
				fails("boom", base, "boom"),
			}},
			Fails: true, Summary: `; compensation ran in reverse order: undid "a"`,
			// The two requests before the unwind are a set: `a` is outstanding
			// while `boom` runs, so which arrives first is exactly the freedom
			// `async:` grants. That `a` arrived at all, and that its compensation
			// followed, is the claim.
			Recorded:        []string{"a", "boom", "undo-a"},
			UnorderedPrefix: 2,
		},
		{
			// Defect 2. A `parallel:` block's branches each keep a private undo
			// log, and the order those logs join the enclosing one decides the
			// order the block's compensations unwind in. Joining them as the
			// branches *finish* builds the log in completion order, and #479
			// settled that the unwind is reverse written order and never reverse
			// completion order.
			//
			// Invisible while a driver ran its branches in declaration order,
			// because then the two orders are the same string. They are not the
			// same rule, and this pins the rule: `second` is declared last, so its
			// compensation comes off first, whichever branch actually finished
			// first.
			//
			// Durably the branches are coroutines and the finishing order is
			// Temporal's. Locally under [v1.AdversarialOrder] the last branch runs
			// first, every time — so a driver that appended in finishing order
			// unwinds `first` before `second` here and fails, with no seed asked
			// to arrange it.
			Name: "parallel branches' compensations unwind in reverse declaration order",
			Workflow: &v1.Workflow{Name: "undo-parallel-declaration-order", Profile: v1.CurrentProfile, Steps: []*v1.Node{
				{Id: "both", Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{Branches: []*v1.Parallel_Branch{
					{Steps: []*v1.Node{undoing(records("first", base, "first"), base, "/do/undo")}},
					{Steps: []*v1.Node{undoing(records("second", base, "second"), base, "/do/undo")}},
				}}}},
				fails("boom", base, "boom"),
			}},
			Fails: true,
			Summary: `; compensation ran in reverse order: undid "second", ` +
				`undid "first"`,
			Recorded:        []string{"first", "second", "boom", "undo-second", "undo-first"},
			UnorderedPrefix: 2,
		},
	}
}
