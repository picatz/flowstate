package engine

import (
	"context"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

// WorkflowVars is a Temporal activity that evaluates a workflow's `vars:` block.
//
// # Why an activity, for expressions with no side effects at all
//
// The reason usually given is that evaluating CEL in workflow code is not deterministic
// in the sense replay needs: a language profile pins which *functions* exist and not how
// cel-go implements them, so an upstream bug fix changes a result under an unchanged
// profile. That is true, and it is not the reason — because the executor evaluates CEL
// in workflow code all over the place and always has. A step's condition, a loop's
// `items:`, a step's own `vars:` block, and the inputs of every task that does not
// declare NeedsPrevOutputs are all resolved inline, in workflow code, and their results
// reach history as the arguments of the activities they schedule.
//
// So that exposure is accepted rather than avoided, mitigated where Worker Versioning
// pins the interpreter and named here so nobody concludes from this activity that it
// was solved. Routing each of those through an activity would be a round trip per
// condition.
//
// What makes the workflow's `vars:` different is Continue-As-New, which versioning does
// not reach. A later segment *replays nothing* — it starts from RunState rather than
// from history, which is exactly what makes suspending cheap — so a `vars:` block
// evaluated inline would be evaluated again at the top of every segment, against
// whatever cel-go that worker has. A value that changed halfway through a run is a
// worse failure than a replay mismatch, because nothing detects it. Evaluated once in
// an activity and carried in RunState, it cannot.
//
// Not the same reason TaskInScope exists, which is easy to assume from the two sitting
// side by side. That activity carries a *scope* to the worker because the expressions it
// evaluates name things the workflow does not have — a loop's binding, and the
// `response.*` of a request that has not been made yet — and it is reached only by tasks
// declaring NeedsPrevOutputs, which today is `http` and plugins asking for a scope. See
// its own doc.
//
// docs/DSL.md holds open the faster path — workflow-side evaluation where Worker
// Versioning pins the interpreter — but that would not help here: versioning pins the
// interpreter within a run's *history*, and the next segment has none.
//
// Takes and returns a [v1.Scope] rather than the specification: the vars go in
// unevaluated and come back evaluated, alongside the profile they are evaluated
// against, and nothing else about the workflow is needed or shipped.
func WorkflowVars(ctx context.Context, declared *v1.Scope) (*v1.Scope, error) {
	vars, err := v1.EvalVars(ctx, declared.GetProfile(), declared.GetAmbientVars())
	if err != nil {
		// Not retryable: a var that does not evaluate will not evaluate on the second
		// attempt either. Its expression is fixed in the specification and it reads
		// nothing that changes — no step outputs, no clock, no network. Retrying would
		// turn an author's mistake into a run that takes its whole retry budget to
		// report the same sentence.
		return nil, temporal.NewNonRetryableApplicationError(
			err.Error(), "InvalidWorkflowVars", err)
	}

	return &v1.Scope{AmbientVars: vars, Profile: declared.GetProfile()}, nil
}

// checkTaskDispatchPolicy is the one call every task-executing activity entry
// point makes before running a task's Fn — the shared helper CLAUDE.md's
// "Both execution drivers must agree" asks for once a value (here, "how is
// the deployment's task-shape policy checked") has more than one caller: one
// function that fails closed the same way everywhere, rather than N copies
// of the same four lines that can silently drift apart, which is exactly how
// this repo's own #187 slice 1 first shipped — three of what turned out to
// be five entry points carried the check, and the other two (TaskAuthorized,
// TaskInScopeAuthorized) were exactly the ones a deployment's policy most
// needs to reach: the arms that resolve secrets and act under the run's own
// identity (see runtime.go's own "Four activities... on two axes" comment on
// [executor.dispatch] for why those two exist at all).
//
// A sixth task-executing entry point, if one is ever added, has exactly one
// line to add here and one pattern to match — enumerate every `func` in this
// file and runtime.go, and every activity in versioning.go's [Register], to
// confirm each task-executing one calls this before evaluating the task.
func checkTaskDispatchPolicy(ctx context.Context, span trace.Span, task *v1.Task, identity *v1.WorkloadIdentity) error {
	if err := v1.CheckTaskPolicy(ctx, task.GetName(), identity); err != nil {
		recordTaskOutcome(span, err)
		// Never benign: a deployment's task-shape policy denying dispatch is
		// not the failure `continue_on_error:` describes — it is the
		// deployment refusing to run the task at all, which is exactly the
		// kind of thing an operator's alerting should still see regardless
		// of what the step's own policy tolerates.
		return activityError(task.GetName(), err, false)
	}
	return nil
}

// Task is a Temporal activity that executes a single task.
//
// The workflow pre-resolves expression inputs to literals before scheduling this
// activity, which keeps the payload small and avoids carrying growing prior
// outputs across every step.
//
// identity is the run's own attested [v1.WorkloadIdentity] (or nil, for a
// local run or a run that predates the field), threaded in from
// [executor.dispatch]'s `e.identity` the same way [taskActivities.TaskAuthorized]
// already receives it — one source, one spelling, for every entry point that
// can carry identity at all. Added as a parameter rather than read from
// anywhere ambient, because this activity — unlike [TaskInScope] — never
// receives a [v1.Scope], so there is nothing else on this call to carry it.
//
// continueOnError is the step's own `continue_on_error:` (#750,
// [v1.StepPolicy.GetContinueOnError]), threaded the identical way: read at
// [executor.dispatch] from the node's policy, which is workflow-side and
// already in hand there, and carried across the activity boundary as a
// parameter because that is the only way it reaches [activityError], which
// runs here on the worker. It only ever categorizes the *Temporal* error this
// activity returns — see [activityError]'s own doc for why that is a
// heuristic and not a correctness signal.
func Task(ctx context.Context, task *v1.Task, identity *v1.WorkloadIdentity, continueOnError bool) (*v1.Node_Outputs, error) {
	ctx, span := startTaskSpan(ctx, task, "")
	defer span.End()

	// The deployment's task-shape policy (#187), checked once per activity
	// entry — the durable driver's half of "once per dispatch," matching
	// where the local driver checks, above its own retry loop
	// (`eval.go`'s runStepWithPolicy).
	if err := checkTaskDispatchPolicy(ctx, span, task, identity); err != nil {
		return nil, err
	}

	// Installed on all three entry points for the reason the logger bridge below
	// is: which activity carries a step is decided by whether the task evaluates
	// its own inputs, which is a property of the task and not of how long it takes.
	ctx, stop := withHeartbeat(ctx)
	defer stop()

	// Inputs are pre-resolved by the workflow, so no scope is needed; task
	// implementations read the supplied literals.
	//
	// Installed on all three entry points rather than on the one task that reads it
	// today, because which activity carries a `log:` step is decided by whether the
	// task evaluates its own inputs — a property of the task, not of logging — and a
	// bridge present on two of three paths is a message that vanishes for a reason
	// nobody would connect to it.
	//
	// #187 gave this entry point an identity parameter for the task-shape
	// policy check above, which closes #235's one remaining gap for free: a
	// plugin task that needs neither a scope nor secret authority is scheduled
	// here, and until identity had a reason to reach this activity at all
	// there was nothing for [plugin.NewContextWithIdentity] to install it
	// from. Same call, same helper, same reasoning as [TaskInScope]'s.
	out, err := task.Eval(plugin.NewContextWithIdentity(withActivityLogger(ctx), orEmptyIdentity(identity)), nil)
	recordTaskOutcome(span, err)

	return out, activityError(task.GetName(), err, continueOnError)
}

// TaskWithPrev executes a task that evaluates expressions itself and therefore
// needs the outputs of earlier steps.
//
// Retained so that a workflow started before scopes existed continues to run; new
// runs schedule TaskInScope instead — [Register]'s own comment states this is not
// dead code, only uncalled by anything currently scheduling activities: a run
// whose history already recorded a `TaskWithPrev` schedule (before [TaskInScope]
// existed) is replayed against exactly the arguments that were persisted, so this
// activity's signature is frozen at what a pre-scope run could have recorded —
// unlike [Task], it cannot gain an identity parameter without breaking every such
// run still in flight the day this deploys. identity therefore stays nil here,
// deliberately: no pre-scope run ever carried one to lose, and [v1.WorkloadIdentity]
// nil reads as every field empty, exactly as an absent one always has.
//
// The same freeze rules out a continueOnError parameter (#750): a pre-scope
// run's history never recorded one, so it stays uncategorized here rather
// than benign — the frozen signature has no way to carry it, and inventing a
// value this activity was never told would be a guess dressed as a fact.
func TaskWithPrev(ctx context.Context, task *v1.Task, prev *v1.Workflow_StepOutputs) (*v1.Node_Outputs, error) {
	ctx, span := startTaskSpan(ctx, task, "")
	defer span.End()

	if err := checkTaskDispatchPolicy(ctx, span, task, nil); err != nil {
		return nil, err
	}

	ctx, stop := withHeartbeat(ctx)
	defer stop()

	// No identity parameter to read, per this function's own doc — but a
	// plugin task replayed through this legacy entry point still deserves the
	// same explicit-empty caller every other path gives one, rather than the
	// context simply never having a value at all.
	out, err := task.Eval(plugin.NewContextWithIdentity(withActivityLogger(ctx), orEmptyIdentity(nil)), prev)
	recordTaskOutcome(span, err)

	return out, activityError(task.GetName(), err, false)
}

// TaskInScope executes a task that evaluates expressions itself, against the scope
// those expressions resolve against.
//
// The scope carries both earlier step outputs and any variables bound by enclosing
// control flow — a loop's current item, a name a step's own `vars:` block declared.
// Sending it is what lets a task inside a loop body evaluate an expression naming one
// of those, since that evaluation happens here on the worker rather than in workflow
// code.
//
// The `http` task is the one that needs it today: `expect:` and `outputs:` are checked
// against a response that does not exist until the request has been made, so they
// cannot be resolved before the activity is scheduled — and they may still name a
// binding from the loop the step sits in.
//
// continueOnError carries the step's `continue_on_error:` across the activity
// boundary the same way it does on [Task] — see that parameter's doc.
func TaskInScope(ctx context.Context, task *v1.Task, scope *v1.Scope, continueOnError bool) (*v1.Node_Outputs, error) {
	ctx, span := startTaskSpan(ctx, task, "")
	defer span.End()

	// The deployment's task-shape policy (#187), checked once per activity
	// entry against the run's real attested identity — this entry point is
	// the one that carries a [v1.Scope], so unlike [Task]/[TaskWithPrev]
	// identity here is whatever the run was actually started as (see
	// varsScope in workflow.go).
	if err := checkTaskDispatchPolicy(ctx, span, task, scope.GetIdentity()); err != nil {
		return nil, err
	}

	ctx, stop := withHeartbeat(ctx)
	defer stop()

	// The scope this activity was scheduled with already carries the run's
	// identity — execute.go copies RunState.Identity into it for every task
	// that needs previous outputs, whether or not this invocation also needs
	// [v1.TaskNeedsAuthority]'s identity-aware activity. A plugin task that
	// declares it needs a scope gets its caller from here rather than from
	// [ContextWithTaskRuntime], which this unauthorized entry point never
	// installs — see runtime.go's taskActivities.context for the path that
	// does.
	out, err := task.EvalInScope(plugin.NewContextWithIdentity(withActivityLogger(ctx), orEmptyIdentity(scope.GetIdentity())), scope)
	recordTaskOutcome(span, err)

	return out, activityError(task.GetName(), err, continueOnError)
}

// activityError translates a task failure into an error carrying Temporal's
// retry semantics.
//
// This translation is what makes the retry policy work at all. Temporal decides
// retryability from the error's application type, so an unclassified error is
// retried until the attempt budget is exhausted — which for a deterministic
// failure wastes the budget, and for a non-idempotent request repeats an
// operation that already took effect. Classification happens in the
// execution-independent layer; this function only maps it onto the substrate.
//
// continueOnError is the step's own `continue_on_error:` (#750), threaded in
// from every caller's own parameter of the same name — see [Task]'s doc for
// where it originates and why it has to cross the activity boundary as an
// argument rather than being read from anywhere ambient. When set, every
// branch below adds [temporal.ApplicationErrorCategoryBenign], which is what
// lets an operator's category-aware metrics or alerting distinguish a
// failure the workflow author already told the system to tolerate from one
// that needs a human.
//
// This is a heuristic, not a verdict on the failure's cause: `continue_on_error`
// is a property of the *step*, decided before any particular attempt runs, so
// it categorizes every failure that step produces as benign — including one
// the author did not anticipate. A step tolerant of a flaky upstream's 5xxs
// still marks a genuine bug in its own input expression benign by the same
// flag. That is the same shape the SDK's own docs describe the category for
// (logging and metrics behavior, not correctness), so nothing downstream of
// this function should treat the category as validating what went wrong —
// only as "this step's author said failures here are expected."
func activityError(taskName string, err error, continueOnError bool) error {
	if err == nil {
		return nil
	}

	kind := v1.ClassifyError(err)

	// The application error's message is how a tolerated failure's recorded text
	// crosses the activity boundary: the workflow side reads exactly this back
	// out of Temporal's envelope (see recordedStepError). Rendering it here, from
	// the one renderer both drivers share, is what keeps `${steps.<id>.error}`
	// the same sentence under either driver.
	message := v1.StepErrorText(err)

	var category temporal.ApplicationErrorCategory
	if continueOnError {
		category = temporal.ApplicationErrorCategoryBenign
	}

	if kind.Retryable() {
		// A failure that told us when to come back gets that carried to the
		// substrate, which schedules the next attempt. The alternative — sleeping
		// where the failure happened — would hold a worker slot for the duration.
		//
		// This belongs on the retryable path and only here: a delay on a
		// non-retryable error is inert, because there is no next attempt to delay.
		if delay := v1.RetryAfter(err); delay > 0 {
			return temporal.NewApplicationErrorWithOptions(message, kind.String(),
				temporal.ApplicationErrorOptions{
					Cause:          err,
					NextRetryDelay: delay,
					Category:       category,
				})
		}

		// An application error with no NonRetryable option is retryable, which is
		// what returning the error unchanged used to rely on. It is built
		// explicitly now so the message carries the canonical text; the retry
		// semantics are the same, and the type names the classification.
		return temporal.NewApplicationErrorWithOptions(message, kind.String(),
			temporal.ApplicationErrorOptions{Cause: err, Category: category})
	}

	// NewNonRetryableApplicationError has no Options variant that also takes a
	// category, so a tolerated non-retryable failure is built the same way the
	// retryable arms are, with NonRetryable pinned explicitly instead of relying
	// on the constructor that cannot also carry Category.
	return temporal.NewApplicationErrorWithOptions(message, kind.String(),
		temporal.ApplicationErrorOptions{Cause: err, NonRetryable: true, Category: category})
}

// The first-party spans, and the two rules that decide their whole shape.
//
// **Activity side only.** Temporal's tracing interceptor opens the workflow and
// activity spans, and this adds one inside each activity naming what the step is
// actually doing. Nothing here is reachable from workflow code, which is
// invariant 4: a span minted during replay is minted again on every replay, and
// the only code allowed to know when that is happening is Temporal's own
// interceptor. Every caller below is an activity function.
//
// **No value ever becomes an attribute.** Invariant 7 is not "be careful with
// secrets", it is that a span is exported to a collector, indexed, and read by
// people and systems with no relationship to the run — so the *only* things
// written here are names and classifications the schema already treats as
// public: the task's name, the step's id, the attempt number, and the scheme and
// name of a secret *reference*. Never an input, never an output, never a
// response body, and — the one that is easy to get wrong — never an error
// message, because a task's error can quote what it was given. The failure
// status therefore carries the error's *classification* and nothing else, and
// the error is not recorded as a span event, since RecordError writes the
// message into one.

// tracerName is the instrumentation scope these spans are attributed to.
const tracerName = "github.com/picatz/flowstate/pkg/flowstate/v1/engine"

// startTaskSpan opens the span covering one task execution.
//
// The provider is read per call rather than captured in a package variable, for
// the reason cmd/flow keeps rediscovering: an instrument built before telemetry
// is configured holds the no-op provider forever, and a worker's registration
// happens at whatever moment the process assembles itself.
//
// stepID is empty on the entry points that do not carry one — the pre-scope
// activities take the task alone — so the attribute is omitted rather than
// written blank. An empty attribute is worse than a missing one: it reads as a
// step whose id is the empty string.
func startTaskSpan(ctx context.Context, task *v1.Task, stepID string) (context.Context, trace.Span) {
	ctx, span := otel.GetTracerProvider().Tracer(tracerName).Start(ctx,
		"flowstate.task/"+task.GetName(), trace.WithSpanKind(trace.SpanKindInternal))

	if !span.IsRecording() {
		// Nothing configured a provider, so the cheapest possible path: no
		// attribute built, no task walked. This is the zero-config case, which
		// is every first run.
		return ctx, span
	}

	attrs := []attribute.KeyValue{attribute.String("flowstate.task.name", task.GetName())}

	if stepID != "" {
		attrs = append(attrs, attribute.String("flowstate.step.id", stepID))
	}

	// The attempt is the substrate's, so it is asked of the substrate rather
	// than threaded through: a retried activity is a second span, and without
	// this they are indistinguishable in a trace. Guarded because these
	// functions are ordinary Go functions the local driver could one day call,
	// and activity.GetInfo panics outside an activity context.
	if activity.IsActivity(ctx) {
		attrs = append(attrs, attribute.Int("flowstate.attempt", int(activity.GetInfo(ctx).Attempt)))
	}

	attrs = append(attrs, secretReferenceAttributes(task)...)
	span.SetAttributes(attrs...)

	return ctx, span
}

// secretReferenceAttributes names the secrets a task will resolve, without
// resolving anything.
//
// This is the observability that secret resolution can honestly have from here.
// A reference is what the worker is handed; the value is produced deep inside
// the task's own evaluation, in a package this one does not own, and is held in
// a closure precisely so nothing can reach it by reflection. Naming the
// reference answers the question a trace is actually asked — *which* secret did
// this step read, and did the one that was denied get asked for at all — and
// answering it costs nothing that can leak, because a [v1.SecretRef] is a scheme
// and a name and contains no material by construction.
//
// Sorted, because the inputs are a map and a set of attributes that reorders
// between two runs of the same step is a diff for anyone comparing traces.
func secretReferenceAttributes(task *v1.Task) []attribute.KeyValue {
	// v1.SecretRefsIn walks structures too — since Value.Structure landed, a
	// reference may sit nested inside a header map or json body, and a
	// top-level look would name some of a step's secrets and not others. The
	// walk visits references and structure entries only, never a literal's
	// contents, which is the walk that would leak.
	refs := v1.SecretRefsIn(task)

	if len(refs) == 0 {
		return nil
	}

	return []attribute.KeyValue{
		attribute.StringSlice("flowstate.secret.refs", refs),
		attribute.Int("flowstate.secret.ref.count", len(refs)),
	}
}

// recordTaskOutcome marks a failed span with what kind of failure it was.
//
// The classification and not the message, and not [trace.Span.RecordError],
// which would write the message into an exception event. `${steps.<id>.error}`
// is rendered from whatever the task said, and a task can say a great deal —
// an http task's error names the URL it called, a plugin's names whatever the
// plugin wrote. That text belongs in the run's own history, which is read by
// somebody holding the run, and not in a span, which is read by a collector.
//
// The kind is the same one [activityError] hands Temporal, so a span's status
// and the retry decision cannot disagree about what happened.
func recordTaskOutcome(span trace.Span, err error) {
	if err == nil || !span.IsRecording() {
		return
	}

	span.SetStatus(codes.Error, v1.ClassifyError(err).String())
}
