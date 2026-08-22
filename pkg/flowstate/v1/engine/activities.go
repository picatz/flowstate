package engine

import (
	"context"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
	"go.opentelemetry.io/otel/attribute"
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
	// local is always false here: the durable driver always has a server in
	// front of it, even one attesting an anonymous caller, so a dispatch
	// through this activity is never the rehearsal `local` is meant to name
	// — see engine/workflow.go's varsScope, "Never Local", and
	// [v1.CheckTaskPolicy]'s own doc for what this parameter can and cannot
	// affect.
	if err := v1.CheckTaskPolicy(ctx, task.GetName(), identity, false); err != nil {
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

// The first-party task span is [v1.StartTaskSpan], two packages up, and what is
// left here is the one thing only this driver can say.
//
// It lived in this file until #523's gap 3, which is precisely why a local run
// produced no `flowstate.*` span at all: the span and the driver were the same
// code. The vocabulary — the span's name, its attribute keys, and the rule that
// no value ever becomes one of them — now lives in `pkg/flowstate/v1`, which
// both drivers import, so neither can drift from the other's spelling. Read
// [v1.StartTaskSpan]'s doc for the two rules that decide the shape; the
// activity-side-only one still binds every caller in this package (invariant 4:
// a span minted during replay is minted again on every replay).

// startTaskSpan opens the task span and adds the attempt, which is this driver's
// alone.
//
// The attempt is the substrate's, so it is asked of the substrate rather than
// threaded through: a retried activity is a second span, and without this they
// are indistinguishable in a trace. Guarded because [v1.StartTaskSpan] is an
// ordinary Go function the local driver does call, and activity.GetInfo panics
// outside an activity context — and because the local driver deliberately does
// *not* write this key from its own retry counter, which counts something else.
// See [v1.StartTaskSpan]'s note on it.
func startTaskSpan(ctx context.Context, task *v1.Task, stepID string) (context.Context, trace.Span) {
	ctx, span := v1.StartTaskSpan(ctx, task, stepID)

	if span.IsRecording() && activity.IsActivity(ctx) {
		span.SetAttributes(attribute.Int(v1.SpanAttributeAttempt, int(activity.GetInfo(ctx).Attempt)))
	}

	return ctx, span
}

// recordTaskOutcome marks a failed span with what kind of failure it was, through
// the one renderer both drivers share — see [v1.RecordTaskOutcome] for why the
// classification and never the message.
func recordTaskOutcome(span trace.Span, err error) {
	v1.RecordTaskOutcome(span, err)
}
