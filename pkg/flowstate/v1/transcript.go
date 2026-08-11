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
//     loop node completes and writes its own `results`. A transcript that reached
//     into an unfinished nesting would be one driver inventing a record the other
//     does not keep, which is the direction invariant 3 exists to prevent.
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
