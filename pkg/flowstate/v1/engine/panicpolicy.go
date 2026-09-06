package engine

import "go.temporal.io/sdk/worker"

// WorkerWorkflowPanicPolicy is the workflow-panic policy every flowstate
// worker runs with, production and test alike, beside
// [v1.WorkerDeadlockDetectionTimeout] as the second half of one decision about
// what happens when workflow-side code misbehaves.
//
// The SDK's default is [worker.BlockWorkflow]: a workflow task that panics —
// a Go panic inside [Run], a detected non-determinism, or the deadlock
// detector's own TMPRL1101 for a goroutine that did not yield within the
// budget — fails the task, the server schedules it again, and the same code
// runs the same task again. For a panic whose cause is a *deployment* (the
// interpreter changed under a run's history) that is the right loop: fix the
// worker and the run resumes. For a panic whose cause is the *run* — an
// expression that spends the deadlock budget, deterministically, on every
// attempt — it is an infinite loop that no operator is told about. #1769
// measured it: a file of four `value:` steps pegged a workflow-task slot
// indefinitely, the worker logged a panic every few seconds, and nothing
// reached the run's status, the timeline, or the author, while `flow run
// local` completed the same file.
//
// [worker.FailWorkflow] makes the run fail instead, with the panic's own text
// as the cause ([temporal.PanicError] under an ApplicationError), which the
// server reads back into `flow get` as an [v1.ErrorKindInternal] failure. A
// run that fails is recoverable and audited; a task that panics forever is
// neither. The SDK offers no "fail after N attempts" between the two, and the
// workflow-task attempt is not visible to workflow code, so the choice is
// binary; failing on the first panic is chosen because the class of panic
// this system can produce from a run is deterministic by construction —
// invariant 4 keeps I/O and nondeterminism out of workflow code — so a second
// attempt of the same task cannot end differently.
//
// The cost, stated rather than discovered: a worker deployed with an
// interpreter change that does not replay a run's history fails that run
// rather than parking it until the worker is rolled back. Flowstate pins a
// run to the interpreter it started on and upgrades only at Continue-As-New
// (versioning.go), which is what makes that cost an operator error with a
// visible failure rather than a routine deployment hazard. Compensation
// (`undo:`) does not run for a run failed this way — the workflow goroutine
// it would run on is the one that panicked — which is the same as before,
// when the run never ended at all.
const WorkerWorkflowPanicPolicy = worker.FailWorkflow
