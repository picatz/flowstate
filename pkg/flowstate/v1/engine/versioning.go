package engine

import (
	"fmt"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"go.temporal.io/sdk/activity"
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
// A deployment without Worker Versioning gets the behaviour Temporal has always
// had: whatever is deployed executes whatever is in flight. What it does not get
// is the guarantee — and one thing depends on the guarantee rather than merely
// benefiting from it, which is argued in docs/DSL.md: expression evaluation
// belongs in workflow code only where versioning pins the interpreter, because
// cel-go's own implementation is pinned by the binary and by nothing else.
//
// That is not a future condition. The interpreter already evaluates CEL inline in
// workflow code — step conditions, a loop's `items:`, a step's own `vars:`, and
// every task input that does not declare `needs_prev_outputs` — so every worker
// this package registers depends on it today. `flow worker` therefore refuses to
// start unversioned unless the operator says in the command line that they accept
// the exposure (`--allow-unversioned-interpreter`); see cmd/flow/main.go. The
// refusal lives at the command rather than here because this package is also the
// registration path for test workers and embedded hosts, and a library that
// refuses to be constructed is a library that gets worked around.
//
// # An activity gains a parameter by appending one, never by gaining a name
//
// The other way this interpreter changes is that an activity needs to carry one
// more fact than it used to. That looks like a versioned change and mostly is
// not, because of what the determinism check actually reads. For a scheduled
// activity the SDK compares the activity id and the activity *type name* and
// nothing else — go.temporal.io/sdk@v1.47.0 internal/internal_task_handlers.go
// :1660-1663. Input payloads are never compared. So a signature that grows is
// invisible to replay, and a name that changes is the one edit that is not.
//
// Both halves of a rollout have to survive, and they are protected by different
// things, which is the part worth being exact about:
//
//   - **A workflow replaying.** History says the interpreter scheduled `Task`
//     here; the interpreter running now must produce `Task` here. Nothing in
//     [Register] affects this — an activity is never executed during a replay,
//     only named — so continued registration of an old name buys replay exactly
//     nothing. What protects it is that the name did not change. Where a name
//     must change, the tools are `workflow.GetVersion` or the deliberate
//     retirement of a corpus entry; see the package comment in replay_test.go.
//   - **An activity task already scheduled.** It was written with the payload
//     count the older interpreter sent, and a worker running current code
//     decodes it into the current function's parameters. `FromPayloads` walks
//     the payloads that are present and leaves every parameter past them at its
//     zero value (converter/composite_data_converter.go:59-80), so an appended
//     parameter arrives empty rather than failing to decode. *This* is what
//     continued registration is for, and it is the whole of [TaskWithPrev]'s
//     job.
//
// Two consequences. The appended parameter goes **last**, always: inserted in
// the middle it would decode the payload meant for its neighbour, which is a
// silent wrong value rather than a loud absence. And its zero value has to be a
// legible "not supplied" — `stepID == ""` is, because [v1.StartTaskSpan] omits
// the attribute rather than exporting a blank one.
//
// The precedent is #756, which appended `continueOnError` to both [Task] and
// [TaskInScope] under their existing names. Every history in the replay corpus
// was recorded before it, still carries the shorter payload list, and still
// replays.
//
// Worth naming what this avoids, because the alternative looks tidier. A `V2`
// suffix on the activity name is a determinism break on every run already in
// flight. On a versioned deployment the pinning above would hide it — the run
// finishes on the interpreter it started on. On one running with
// `--allow-unversioned-interpreter` nothing hides it: whatever is deployed
// executes whatever is in flight, so the next workflow task replays old history
// through new code, disagrees at the first scheduled task, and wedges the run.
// Appending is safe on both, so the exposure never has to be priced.

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
func Register(w worker.Registry, runtime ...TaskRuntimeConfig) {
	RegisterWorkflows(w)

	w.RegisterActivity(Task)
	w.RegisterActivity(TaskInScope)
	configured := TaskRuntimeConfig{}
	if len(runtime) > 0 {
		configured = runtime[0]
	}
	// Frozen when this worker is registered, after its built-ins and plugin
	// tasks have been installed. Activities compare against this worker-owned
	// snapshot rather than reading a mutable process registry during replay.
	configured.taskNames = v1.DefaultRegistry().Names()
	authorized := taskActivities{configured: configured}
	w.RegisterActivityWithOptions(authorized.TaskAuthorized, activity.RegisterOptions{Name: "TaskAuthorized"})
	w.RegisterActivityWithOptions(authorized.TaskInScopeAuthorized, activity.RegisterOptions{Name: "TaskInScopeAuthorized"})
	w.RegisterActivity(WorkflowVars)

	// The worker's admission check. Registered here and not conditionally on a
	// worker having plugins, because the run that needs refusing is precisely the
	// one arriving at a worker that has none: an unregistered activity fails with
	// "unknown activity type", which is a true refusal for the wrong reason and
	// reads as a broken worker rather than as a rollout that is half done.
	//
	// On the receiver, and named explicitly, for both of the reasons the two
	// authorized activities above are: the answer it gives is this worker's
	// rather than the process's (#777), and pinning the name is what keeps that
	// move invisible to history. The SDK would otherwise derive `CheckPlugins-fm`
	// from a method value, and a history that says `CheckPlugins` would replay
	// against a worker answering to a name nothing ever scheduled.
	w.RegisterActivityWithOptions(authorized.CheckPlugins, activity.RegisterOptions{Name: checkPluginsActivity})

	// The equivalent segment admission for task availability. Old histories do
	// not schedule it because their workflow carries no resolved task snapshot;
	// current runs use this stable name before any task activity is dispatched.
	w.RegisterActivityWithOptions(authorized.CheckTaskCapabilities,
		activity.RegisterOptions{Name: checkTaskCapabilitiesActivity})

	// Registered so a run started before scopes existed can still complete. It has
	// no callers in current code and is not dead: history written by an older
	// interpreter names it, and a name history contains is a name a worker must
	// still answer to.
	w.RegisterActivity(TaskWithPrev)
}

// WorkflowRegistry is the workflow half of [worker.Registry].
//
// It exists because the other thing that has to hold this package's workflow
// registration is not a worker at all: [worker.WorkflowReplayer], which replays a
// recorded history against current code and therefore registers workflows and
// nothing else — an activity is never executed during a replay, only named by the
// history. Narrowing the parameter is what lets the replay corpus in
// replay_test.go register [Run] the way a real worker does rather than by hand,
// which matters more here than it looks: replaying against different registration
// options than production uses would make the gate answer a question nobody asked.
type WorkflowRegistry interface {
	RegisterWorkflowWithOptions(w any, options workflow.RegisterOptions)
}

// RegisterWorkflows installs the interpreter workflow, and only the workflow.
//
// The single caller that is not [Register] is the replay gate. Keeping the
// options here, in one function both reach, is the same argument [Register]'s own
// doc makes about its five lines: a registration is a thing to get exactly right
// once.
//
// One *static* type, and never Temporal's dynamic workflow registration, which
// answers the same "one handler, many workloads" need by selecting a fallback
// handler on the workflow type name the caller started. Here the workload arrives
// as a typed [v1.RunState] argument instead, which is what gives one type to pin
// versioning to for the whole fleet, one name the replay corpus can register
// against (replay_test.go registers through this very function), and one place
// determinism is enforced rather than one per workload. That is why
// recordingRegistry's RegisterDynamicWorkflow in versioning_test.go is empty:
// nothing is registered there on purpose. The trade is that every run's
// WorkflowType is "Run" — see docs/ARCHITECTURE.md, "One interpreter, not a
// workflow type per workload", for what carries a workload's own name instead.
func RegisterWorkflows(r WorkflowRegistry) {
	r.RegisterWorkflowWithOptions(Run, workflow.RegisterOptions{
		// Pinned, so an in-flight run is never handed to a different interpreter
		// than the one that has been executing it. On a worker that has not opted
		// into versioning this is inert: the SDK records it and the server has no
		// deployment to pin to.
		VersioningBehavior: workflow.VersioningBehaviorPinned,
	})
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
//
// Half in is an error, and used not to be. Dropping silently to unversioned is the
// worst of the three answers: the operator asked for versioning, the worker starts,
// nothing says otherwise, and the guarantee they configured is simply absent — a
// fail-open on the exact posture the interpreter depends on. Somebody who set one
// flag meant to set both, so the missing half is named and the command stops.
func DeploymentOptions(deployment, buildID string) (worker.DeploymentOptions, error) {
	switch {
	case deployment == "" && buildID == "":
		return worker.DeploymentOptions{}, nil
	case buildID == "":
		return worker.DeploymentOptions{}, fmt.Errorf(
			"worker deployment %q has no build id: a version is the pair, so set --build-id "+
				"(or FLOWSTATE_BUILD_ID) to something unique per build, such as the commit", deployment)
	case deployment == "":
		return worker.DeploymentOptions{}, fmt.Errorf(
			"build id %q has no worker deployment: a version is the pair, so set --deployment-name "+
				"(or FLOWSTATE_DEPLOYMENT_NAME) to the deployment this worker belongs to", buildID)
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
	}, nil
}
