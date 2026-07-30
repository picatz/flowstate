package engine

import (
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// Versioning, for a system where one workflow type runs every definition.
//
// Most Temporal applications have a workflow type per business process, so a
// deployment changes some processes and leaves the rest alone. Flowstate has
// exactly one: [Run] is an interpreter, and every workload anybody has ever
// written is a *value* it executes. That inverts what a deployment means. Shipping
// a fix to loop compaction, to how a `wait` consumes a carried signal, or to the
// order two parallel branches are scheduled in changes the behaviour of every
// in-flight run at once — including a run that is a month old and half finished.
//
// Temporal replays a workflow's history through the code the worker is running
// now. Interpreter behaviour is therefore a determinism input in the same way a
// clock read is: history says which activity was scheduled at each point, and code
// that would now schedule a different one does not replay. The difference is that
// a clock read is a mistake in one workflow, and this is a correct improvement to
// the interpreter that every run in the fleet is exposed to.
//
// So the posture is:
//
//   - **Pinned within a run.** A run finishes on the interpreter it started on.
//     Deploying does not touch anything in flight.
//   - **Upgraded at Continue-As-New.** Each new run picks up the current version.
//
// The second half is what makes the first half survivable. Pinned alone would
// hold a long workload on its original interpreter across every Continue-As-New,
// for as long as the workload lives — and a version with runs still on it cannot
// be drained, so an operator could never retire one. Continue-As-New is the moment
// where changing is safe, and it is the only such moment: the next run replays
// nothing, because it starts from [v1.RunState] rather than from history.
//
// That buys a precondition, and it is a real one. Version N writes the RunState
// that version N+1 reads, so the two must agree about it — see the invariant in
// docs/ARCHITECTURE.md. It is a proto message and the usual proto rules give it to
// us: add fields, never renumber, never repurpose, and treat a field the previous
// version did not set as absent rather than as a default that means something.
//
// None of this is required to run Flowstate. A deployment without Worker
// Versioning gets the behaviour it has always had: whatever is deployed executes
// whatever is in flight. What it does not get is the guarantee — and one thing
// depends on the guarantee rather than merely benefiting from it, which is
// recorded in docs/DSL.md: expression evaluation may move into workflow code only
// where versioning pins the interpreter, because cel-go's own implementation is
// pinned by the binary and by nothing else.

// Register installs the interpreter on a worker.
//
// Every process that runs Flowstate workloads calls this rather than registering
// by hand. There were four hand-written copies of these five lines before it
// existed, which is four places to forget the versioning behaviour, and four
// places to forget that [TaskWithPrev] still has to be registered for runs that
// predate scopes. A registration list is a thing to get exactly right once.
//
// The parameter is [worker.Registry] rather than [worker.Worker] so that anything
// that can hold registrations can be passed one — including a test environment
// wrapping a real worker.
func Register(w worker.Registry) {
	w.RegisterWorkflowWithOptions(Run, workflow.RegisterOptions{
		// Pinned, so an in-flight run is never handed to a different interpreter
		// than the one that has been executing it. On a worker that has not opted
		// into versioning this is inert: the SDK records it and the server has no
		// deployment to pin to.
		VersioningBehavior: workflow.VersioningBehaviorPinned,
	})

	w.RegisterActivity(Task)
	w.RegisterActivity(TaskInScope)
	w.RegisterActivity(WorkflowVars)

	// Registered so a run started before scopes existed can still complete. It has
	// no callers in current code and is not dead: history written by an older
	// interpreter names it, and a name history contains is a name a worker must
	// still answer to.
	w.RegisterActivity(TaskWithPrev)
}

// DeploymentOptions builds the worker's versioning configuration.
//
// Returned rather than applied so the caller can see what it is opting into, and
// so the zero value — versioning off — is what an unconfigured deployment gets.
// Both a deployment name and a build ID are required to turn it on: a version is
// the pair, and honouring half of it would produce a worker that claims a version
// it cannot be addressed by.
//
// Empty in, empty out. The SDK panics rather than errors on a contradictory worker
// configuration, so the contradictions are made unrepresentable here instead.
func DeploymentOptions(deployment, buildID string) worker.DeploymentOptions {
	if deployment == "" || buildID == "" {
		return worker.DeploymentOptions{}
	}

	return worker.DeploymentOptions{
		UseVersioning: true,
		Version: worker.WorkerDeploymentVersion{
			DeploymentName: deployment,
			BuildID:        buildID,
		},

		// Deliberately not set. A default versioning behaviour is what a workflow
		// type falls back to when it declares none, and [Run] declares Pinned —
		// so setting one here could only ever mask the day somebody removes that
		// declaration. Without it the SDK panics at registration, which is the
		// failure we want: loud, immediate, and before any run exists.
	}
}
