package flowstatev1

import "fmt"

// Structured concurrency, in the half both drivers must agree about.
//
// `async: true` is the one marker that lets execution depart from written order
// (issue #418). What an author can *see* about that has to be identical on both
// drivers — which steps may carry it, which later step joins which outstanding
// work, and how wide a scope may get — so each of those is written once here, in
// the package both drivers already import. The half that is legitimately
// different is the only half left out: launching work is a goroutine locally and
// a `workflow.Go` coroutine durably.
//
// The rules are deliberately scope-shaped rather than step-shaped. A join is a
// property of the step that reads, not of the step that was marked; the end of a
// step list joins whatever that list started; and the width bound counts what one
// scope has outstanding. That is what keeps `async:` a local annotation on the
// step it appears on, with no graph anywhere to reconstruct.

// MaxAsyncInFlight is how many async steps one scope may have outstanding at
// once.
//
// The resource this bounds is *simultaneously outstanding work* — goroutines
// locally, scheduled activities durably — and that is not the same resource as
// the specification's own step cap, which is why it is its own number rather
// than a comment pointing at `Workflow.steps`. A scope's steps are capped at 100
// per workflow, but a step list reached through a `call:` is a different
// workflow's list, and a `loop:` body runs its list again per iteration. The
// count that matters is what is in flight at one moment in one scope, so that is
// what is counted, at the moment work is launched.
//
// A hundred matches the step cap so that a scope whose every step is async is
// still expressible: a bound that refused a legal specification would be a
// bound that changes what the language accepts, rather than one that stops a
// specification from spending an unbounded amount of a worker.
const MaxAsyncInFlight = 100

// CheckAsyncPlacement reports whether a node may be marked `async:`.
//
// # A task step only
//
// Async marks work that can overlap other work. Control flow has none of its own:
// a `for_each`, a `parallel`, a `loop:`, a `switch:` and a `call:` are each a
// position in the file where other steps run, and marking one async would be a
// claim about steps that have their own answer to the question. A `value:` is the
// opposite shape and refused for the opposite reason — it is a pure expression
// evaluated in workflow code, so there is nothing to wait for and nothing to
// overlap.
//
// A wait gets its own sentence, because it is the refusal with a real hazard
// behind it rather than a tidiness argument: a `wait_for_signal:` marked async is
// a parked step that nothing joins until the end of the scope, so a run would sit
// on a signal that the author cannot see anybody waiting for. The design pass on
// #418 refused it in this slice for exactly that reason.
//
// # A sequential placement only
//
// [UndoScopeConcurrent] — a `for_each` body or a `parallel` branch — refuses it.
// The width of one scope is bounded by [MaxAsyncInFlight]; the width of a scope
// running once per iteration or once per branch is that number multiplied by a
// factor the enclosing construct chooses, and nothing in this slice answers what
// that product should be. Refusing is the fail-closed direction and stays
// additive: opening it later breaks no file, where accepting it now and narrowing
// later would.
//
// Both drivers call this before a step runs, and `flow validate` calls it with a
// position, which is where an author actually meets it.
func CheckAsyncPlacement(node *Node, placement UndoScope) error {
	if !node.GetAsync() {
		return nil
	}

	if node.GetTask() == nil {
		if _, isWait := node.GetKind().(*Node_Wait); isWait {
			return fmt.Errorf(
				"`async:` is only supported on a step that runs a task, and step %q is a wait; "+
					"a wait marked async would be parked with nothing joining it until the end of "+
					"the scope, which is a deadlock nothing in the file shows — write the wait in "+
					"written order, and mark the work either side of it",
				node.GetId())
		}

		if _, isValue := node.GetKind().(*Node_Value); isValue {
			return fmt.Errorf(
				"`async:` is only supported on a step that runs a task, and step %q is a `value:`; "+
					"a value is an expression evaluated in place, so there is nothing to overlap — "+
					"remove the `async:`",
				node.GetId())
		}

		return fmt.Errorf(
			"`async:` is only supported on a step that runs a task, and step %q is control flow; "+
				"a block has no work of its own to overlap — mark the task steps inside it instead",
			node.GetId())
	}

	if placement == UndoScopeConcurrent {
		return fmt.Errorf(
			"`async:` is not supported inside a `for_each` body or a `parallel` branch, and step %q "+
				"is in one; that scope is already concurrent, and how many steps may be in flight "+
				"at once when each of its copies starts its own is not yet decided — write the step "+
				"in written order here, or lift it out of the block",
			node.GetId())
	}

	return nil
}

// CheckAsyncWidth reports whether one more step may be launched into a scope
// already holding this many.
//
// Separate from [CheckAsyncPlacement] because it is a bound on a running scope
// rather than a property of the file: a specification the validator accepts can
// still ask a scope for more outstanding work than [MaxAsyncInFlight], through a
// step list reached from a `call:` inside a `loop:` body, and the count that
// matters is only known while the scope runs. Refused rather than silently
// serialised, per the house rule that a run which cannot do what it was asked
// fails rather than quietly doing less.
func CheckAsyncWidth(inFlight int, id string) error {
	if inFlight < MaxAsyncInFlight {
		return nil
	}

	return fmt.Errorf(
		"step %q cannot start: %d async steps are already in flight in this scope, which is the "+
			"limit (%d); join some of them by referencing their outputs before starting more",
		id, inFlight, MaxAsyncInFlight)
}

// AsyncJoinTargets returns which of the outstanding async steps a node mentions,
// in the order they were started — which is written order, since a scope starts
// them where they are written.
//
// # Every mention, not every read
//
// The walk is [CollectNodeRefs], the same one Continue-As-New compaction uses to
// decide which outputs a remaining step still needs, and the reason to share it
// is that both questions are "what does this node name". It is deliberately an
// over-approximation in one direction: `steps` named bare, or handed to a macro,
// marks every step, so a node that could conceivably read an async step joins it.
// Being wrong that way costs latency; being wrong the other way costs a step
// resolving against an output that is not there yet.
//
// That totality is the semantics rather than an implementation detail. A
// `has(steps.build.artifact)` that could answer "not finished yet" would make
// completion order observable through a guard, which is select/first-of-N wearing
// a guard's clothes — #418's design pass foreclosed it, so the guard joins like
// any other mention.
//
// # Why the outstanding steps are handed in
//
// [CollectNodeRefs] recognises a bare `a.said` as a step reference by asking
// whether `a` is a key of the outputs it is given, and an outstanding async step
// is precisely one whose outputs are *not* in scope yet. So the outputs it walks
// against are the visible ones plus a placeholder per outstanding step: without
// that, the bare spelling of a reference would silently fail to join while the
// rooted `steps.a.said` spelling joined, and two spellings of one reference would
// mean different things.
func AsyncJoinTargets(node *Node, inFlight []string, visible *Workflow_StepOutputs) []string {
	if node == nil || len(inFlight) == 0 {
		return nil
	}

	known := &Workflow_StepOutputs{StepValues: map[string]*Node_Outputs{}}
	for id, outputs := range visible.GetStepValues() {
		known.StepValues[id] = outputs
	}
	for _, id := range inFlight {
		if _, seen := known.StepValues[id]; !seen {
			known.StepValues[id] = &Node_Outputs{}
		}
	}

	refs := map[string]map[string]struct{}{}
	CollectNodeRefs(node, known, refs)

	// Ordered by when the scope started them rather than by the map's iteration
	// order: the joins a node performs are themselves a sequence of waits, and a
	// sequence whose order depends on a Go map would be a place scheduling could
	// leak into which failure a scope reports first.
	joins := make([]string, 0, len(inFlight))
	for _, id := range inFlight {
		if _, mentioned := refs[id]; mentioned {
			joins = append(joins, id)
		}
	}

	return joins
}
