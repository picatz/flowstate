package flowstatev1

// PartialTranscript is what a *failed* run hands back alongside its failure: the
// record of what it did before it stopped.
//
// A run that fails used to hand back nothing at all, and the cost of that landed
// on the two places that read a transcript to find out what ran, `flow test`'s
// `expect.ran`/`expect.skipped`, and its step coverage (issues #420, #453). A case
// whose whole point is that the run fails would contribute its workflow's steps to
// the coverage universe and reach none of them, so an author testing an error
// branch had to record every step the branch actually exercised under
// `coverage.allow_unreached`, a reason written down for something that was not
// true.
//
// # What it contains
//
// Every step the run recorded outputs for, plus the step whose failure ended the
// run, recorded through [FailedStepOutputs] exactly as a step tolerated by
// `continue_on_error:` is. Both drivers accumulate that same record as they walk
// this only names it and decides what a failure is entitled to hand back of it.
//
// # What it deliberately does not contain
//
//   - `run_outputs`. A run's declared outputs are the answer it was asked for, and
//     a run that failed has no answer. Clearing them here rather than trusting them
//     to be unset is what keeps a failure *after* [EvalRunOutputs] has run from
//     reporting an answer nobody may act on.
//   - anything from inside a `parallel` block or a loop body that did not finish.
//     Neither driver merges a failed parallel's branch outputs into the enclosing
//     scope, and a loop's per-iteration outputs only reach the transcript when the
//     loop node completes and writes its own `results` — or when it exhausts its
//     iteration budget, which is the one failure whose every recorded iteration
//     *did* finish: an exhausted loop's entry carries the `results` that ran
//     beside its `error` ([LoopExhaustedError]), because there the account is
//     whole and dropping it would erase the failed-versus-never-attempted line
//     the record exists to draw. A transcript that reached into an unfinished
//     nesting would be one driver inventing a record the other does not keep,
//     which is the direction invariant 3 exists to prevent.
//
// # Why a copy
//
// The run's own scope keeps pointing at the map this is built from, compensations
// run after the failure, and the caller may hold this long after. A copy is one map
// header per failed run and removes the question entirely.
//
// The values inside are shared, not cloned: they are the same immutable
// [Node_Outputs] the successful path hands back, and nothing that reaches this
// point writes through one.
func PartialTranscript(accumulated *Workflow_StepOutputs) *Workflow_StepOutputs {
	if accumulated == nil {
		return nil
	}

	out := &Workflow_StepOutputs{
		StepValues: make(map[string]*Node_Outputs, len(accumulated.GetStepValues())),
	}
	for id, values := range accumulated.GetStepValues() {
		out.StepValues[id] = values
	}

	return out
}

// StepFailureRecord is a failure that carries the account its own step's
// transcript entry keeps in place of the bare `error` text: the iterations an
// exhausted loop ran ([LoopExhaustedError]), or the arm a switch had already
// selected when its body failed ([SwitchBodyError]).
//
// Both drivers ask for it by *direct* type assertion at the one site each records
// a failed step — the local driver's failureRecord, the durable driver's failedAt
// — and never through an unwrap chain. That is the containment [LoopExhaustedError]
// spells out: the account belongs to the entry of the step that owns it, and the
// same failure propagating out of a call or an enclosing for_each is an ordinary
// failure at that level.
//
// One interface rather than a list of assertions per driver, because the list is
// the thing that would drift: a third container that owns an account would
// otherwise be added to one driver and forgotten in the other, which is invariant
// 3's failure in its usual clothes.
//
// The rendered failure text is passed *in* rather than derived here, for the
// reason [FailedStepOutputs] takes one too: a failure reaches the durable driver
// wrapped in that engine's own words and reaches the local driver bare, so only
// each driver can shed its own envelope. An account that rendered its own text
// would record `engine: flowstate run failed: …` durably and the plain sentence
// locally — one value with one meaning, written down twice, which is the shape
// every driver disagreement found so far has had.
type StepFailureRecord interface {
	error

	// Record is the outputs to store under the failing step's own id, given the
	// failure text its driver rendered.
	Record(text string) *Node_Outputs
}
