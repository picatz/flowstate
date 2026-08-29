// Package engine is the durable execution driver: it runs a compiled workflow
// specification as a Temporal workflow, so a run survives process crashes,
// deploys, and waits measured in days.
//
// It is one of two drivers over one execution model. The other is the local
// interpreter in the parent package (RunWithInputs), which executes the same
// specification in-process with no Temporal at all. Both dispatch through the
// same step executor, and anything observable that differs between them is a
// defect, because local runs exist to tell an author what production will do.
// Shared behavior cases live in pkg/flowstate/v1/internal/conformance and both drivers run
// them; add a case there rather than in one driver's own tests, and check that
// both drivers actually call the set it joins. A value with one meaning
// belongs in the parent package, which both drivers import, so one constant
// cannot disagree with itself.
//
// The package sits between two boundaries. Below it are the generated
// flowstate.v1 types: the specification it executes, the RunState it suspends
// into, and the outputs it reports are all schema messages, and nothing here
// redefines their shape. Above it is the Temporal SDK: [Register] registers
// [Run] as the single pinned workflow type together with its activities, and
// everything Temporal already does well (timers, retries, signals,
// Continue-As-New, schedules) is surfaced rather than reimplemented.
//
// The invariants below constrain every change here. They are stated fully,
// with their reasons, in docs/ARCHITECTURE.md; read that before a structural
// change, because a change that violates one is a bug even when the tests
// pass.
//
//   - Workflow-side code is pure and pinned. [Run] executes under replay, so
//     anything nondeterministic, version-sensitive, or I/O-bound belongs in an
//     activity, never in workflow code. CEL evaluation is the one deliberate
//     exception, accepted because the interpreter is version-pinned per run:
//     a run finishes on the interpreter it started on and takes the current
//     version only at Continue-As-New.
//   - The workflow's own vars are an activity ([WorkflowVars]), and not for
//     the reason above: Continue-As-New is the one seam replay does not
//     cover, so an inline evaluation there could change value mid-run.
//   - RunState is a wire contract between interpreter versions. One version
//     writes it at Continue-As-New and a different version reads it back, so
//     it obeys the rules a published message obeys.
//   - Secrets never enter workflow history. A secret crosses this package
//     only as a reference; the activity that needs the value resolves it
//     through the capabilities in [TaskRuntimeConfig], and errors are
//     scrubbed before Temporal can persist them as failures.
//   - A run that cannot continue must fail, not hang. RunState is weighed
//     before suspending (v1.CheckRunStateSize), because a payload past
//     Temporal's blob limit fails the workflow task, which is retried
//     forever in silence.
package engine
