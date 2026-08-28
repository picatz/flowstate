package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type ErrRunFailed struct {
	Message string

	// Recorded is the driver-independent text this failure records as the step's
	// `error` output when `continue_on_error` tolerates it — rendered by
	// [v1.StepErrorText], and deliberately not Message.
	//
	// Message formats the whole cause, Temporal's envelope included, for the
	// run-level failure a person reads. Recorded is the value an author's
	// expression compares, so it has to be the same sentence the local driver
	// records for the same failure.
	//
	// It travels as a field rather than as a wrapped cause because this type has
	// no Unwrap on purpose: Temporal's failure converter walks the unwrap chain
	// into the failure it persists, and what this deliberately flattens must stay
	// flattened.
	Recorded string

	// recordedFromTask reports whether Recorded came from a classified task
	// failure.
	//
	// A classified failure renders canonically — `task "http" failed (Kind): …` —
	// and absorbs the structural prefixes around it, because the local driver's
	// errors.As reaches straight past `iteration 0:` and `step "x":` to the
	// TaskError inside. Anything else keeps those prefixes, because there the
	// local driver renders the wrapped error's own words and the position is part
	// of them. This bit is what lets the durable driver make the same choice
	// without re-deriving it from a chain it has already flattened.
	recordedFromTask bool

	// recordedOwn is the outputs a step that owns an account of its own records
	// in place of the bare `error` text ([v1.StepFailureRecord]): an exhausted
	// loop's failure sentence plus the `results` that ran before the budget was
	// spent, or a failed switch's sentence plus the arm it had selected.
	//
	// Set by [failedAt] only when the error it is handed *is* that failure,
	// raised bare at the step that owns it — never read back out of an inner
	// [ErrRunFailed] the way Recorded is. That asymmetry is the containment: the
	// account belongs to that step's own transcript entry, and a failure
	// propagating out of a call or an enclosing for_each must record as a plain
	// failure at that level rather than filing the inner step's account under a
	// step that did not run it. The local driver draws the identical line with a
	// direct type assertion in its failureRecord.
	recordedOwn *v1.Node_Outputs

	// Kind classifies the failure the same way [v1.ClassifyError] would,
	// carried alongside Message and Recorded for the same reason both of those
	// are: whatever wrapping this driver adds between where the failure
	// happened and where the run finally reports it must not be what a caller
	// reads back. See [recordedStepKind], which computes it, and
	// [classifyRunError], which is what puts it where a client can read it —
	// this field only carries it there.
	Kind v1.ErrorKind
}

func (e *ErrRunFailed) Error() string {
	return fmt.Sprintf("engine: flowstate run failed: %s", e.Message)
}

// errorKind reports e's classification, defaulting to [v1.ErrorKindInternal]
// when nothing along the way classified it — the same default
// [v1.ClassifyError] uses for a non-nil error it does not recognize, since an
// unclassified run failure is a gap in Flowstate rather than a statement that
// nothing is known about the failure.
func (e *ErrRunFailed) errorKind() v1.ErrorKind {
	if e.Kind != "" {
		return e.Kind
	}

	return v1.ErrorKindInternal
}

// stepFailed reports a step failure, except when the run is being cancelled.
//
// Temporal decides whether a closed run reads as CANCELED or FAILED from the
// error the workflow function returns, and it recognises cancellation by type.
// [ErrRunFailed] carries a message and nothing else, so wrapping a cancellation
// in one formats the type away: the run stops, and it is recorded as having
// failed.
//
// That is not a cosmetic difference. `flow cancel` is the verb for stopping a
// workload on purpose, and a workload somebody stopped on purpose reporting a
// failure sends whoever finds it later looking for a fault that never happened —
// while a real failure at the same moment is indistinguishable from it.
//
// So a cancellation propagates untouched and everything else becomes a run
// failure. Cancellation reaches here from anything that blocks on the workflow's
// context — an activity, a timer, a signal channel — which is why this is a
// helper rather than a check at one site.
// The format names only the *position* — `step %q`, `iteration %d` — and not the
// cause: this composes both strings from it, so the message a person reads and
// the text an expression compares cannot drift apart at a call site that
// remembered to interpolate the error into one of them and not the other.
//
// A position belongs to the level a failure is passing *out of*, never to the
// level that raised it. `step %q` is therefore added by [executor.runNodes] on
// the propagating path — where the local driver adds its own — and not by the
// call that raised the failure. Adding it at the raising site put the tolerating
// step's own id into the text it recorded for itself: `${steps.gate.error}` read
// `step "gate": evaluating items: …` durably and `evaluating items: …` locally,
// for the same file. Which step it is, is what the key already says.
func stepFailed(err error, format string, args ...any) error {
	if temporal.IsCanceledError(err) {
		return err
	}

	return failedAt(err, fmt.Sprintf(format, args...))
}

// nodeFailed reports a failure raised by a node itself — its `vars:`, its task's
// inputs, its loop's items, its wait — carrying no position of its own.
//
// The position is added if and when this leaves the node, so that a node
// tolerating its own failure records the same sentence the local driver records
// for it. See [stepFailed].
func nodeFailed(err error) error {
	if temporal.IsCanceledError(err) {
		return err
	}

	return failedAt(err, "")
}

// failedAt builds the run failure, optionally prefixed by a position.
func failedAt(err error, position string) error {
	recorded, fromTask := recordedStepError(err)

	// Composed from the same envelope-free extraction Recorded uses, rather
	// than from err.Error() — which, for a failure that just crossed the
	// activity boundary, is Temporal's own words: the whole cause including
	// scheduledEventID, startedEventID and a worker identity. That was the
	// bug (#788): recordedStepError already existed to shed exactly this for
	// the recorded ${steps.<id>.error} value, and Message read err.Error()
	// one line below it instead of the same extraction.
	//
	// Message still diverges from Recorded in the one way it always has: a
	// classified task failure's Recorded text absorbs every structural prefix
	// around it (so an author's `if:` compares the identical sentence the
	// local driver would record), while Message accumulates a position at
	// every level, because a person reading `flow get` benefits from the path
	// to the failure and an expression comparing a value does not. That is
	// why this does not simply assign message = recorded and stop: the
	// unconditional prepend below, versus Recorded's fromTask-gated one, is
	// what keeps that divergence intact.
	message := recorded
	if message == "" {
		message = err.Error()
	}

	var inner *ErrRunFailed
	switch {
	case errors.As(err, &inner):
		// A nested failure already carries Message built the identical way one
		// level down — recursing into inner.Message rather than inner.Recorded
		// is what lets Message keep accumulating position while Recorded does
		// not (see above).
		message = inner.Message
	default:
		// Not yet wrapped, so this is the one place a translation attached
		// directly at the failure site — see [durableStepTimeoutMessage] — can
		// still be read off err itself rather than out of a chain a later
		// wrap would hide it in.
		if friendly, ok := err.(interface{ runMessageText() string }); ok {
			message = friendly.runMessageText()
		}
	}

	if position != "" {
		message = position + ": " + message

		// A structural position is prepended for the same failures the local
		// driver prepends it for, and dropped for the same ones it drops it for. A
		// tolerated `for_each` whose body raised a runtime CEL error records
		// `iteration 0: step "child": no such key: field` under either driver;
		// a tolerated body whose *task* failed records the canonical task sentence
		// under either, because that is what errors.As finds locally.
		if !fromTask && recorded != "" {
			recorded = position + ": " + recorded
		}
	}

	// The step's own account — an exhausted loop's iterations, a failed switch's
	// selection — read off the raw error at the one site it is raised (that
	// step's own nodeFailed). A propagating re-wrap hands this function an
	// [ErrRunFailed], which has no Unwrap on purpose, so the assertion fails
	// there and the record stays with the entry it belongs to.
	var recordedOwn *v1.Node_Outputs
	if account, ok := err.(v1.StepFailureRecord); ok {
		// The envelope-free text this driver already extracted, never the
		// error's own words: see [v1.StepFailureRecord].
		recordedOwn = account.Record(recorded)
	}

	return &ErrRunFailed{
		Message:          message,
		Recorded:         recorded,
		recordedFromTask: fromTask,
		recordedOwn:      recordedOwn,
		Kind:             recordedStepKind(err),
	}
}

// recordedStepError extracts the text a tolerated failure records as the step's
// `error` output, from whatever shape the failure reached this driver in.
//
// The text itself has exactly one renderer, [v1.StepErrorText]; this only
// recovers its output from the wrapping the durable driver adds in transit. A
// task failure arrives from an activity inside a failure envelope — scheduled
// event ids, a worker identity, the classification restated at every level of
// the cause chain — and the application error within carries the rendered text
// as its message, put there by activityError on the worker. A failure from a
// nested level (a loop iteration, a parallel branch) arrives as an
// [ErrRunFailed] already carrying the text it extracted, so the innermost task's
// sentence is what propagates outward. Anything else never crossed a wire, and
// its own words are already what the local driver would record.
// The second return says whether the text came from a classified task failure,
// which decides whether an enclosing position is prepended to it — see
// [stepFailed].
func recordedStepError(err error) (string, bool) {
	var run *ErrRunFailed
	if errors.As(err, &run) && run.Recorded != "" {
		return run.Recorded, run.recordedFromTask
	}

	// Every application error reaching a step's tolerance came from
	// activityError, which builds it from a classified task failure and puts the
	// canonical text in the message.
	var app *temporal.ApplicationError
	if errors.As(err, &app) {
		return app.Message(), true
	}

	// An activity failure with no application error inside — a timeout, say —
	// still sheds the envelope: the cause is the failure, the envelope is only
	// how it travelled. Not a classified failure, so a position still applies.
	var activity *temporal.ActivityError
	if errors.As(err, &activity) && activity.Unwrap() != nil {
		return activity.Unwrap().Error(), false
	}

	var taskErr *v1.TaskError
	return v1.StepErrorText(err), errors.As(err, &taskErr)
}

// recordedStepKind extracts the [v1.ErrorKind] a failure was classified as,
// from whatever shape it reached this driver in — the companion to
// [recordedStepError], for the same reason: a value that has to agree with the
// local driver cannot be read back out of a wrapper the local driver never
// added.
//
// An inner [ErrRunFailed] is checked first so a classification made deeper in
// the walk — a loop iteration, a parallel branch — is not overwritten by a
// weaker guess made where it is caught. Failing that, an application error's
// Type is exactly [v1.ErrorKind.String] as [activityError] set it, so parsing
// it back recovers the same classification the worker made; anything else
// falls through to [v1.ClassifyError], whose own default (Internal, for a
// non-nil error it does not otherwise recognize) is the right answer for a
// failure that crossed the boundary in a shape nothing here expected.
//
// A [temporal.TimeoutError] is checked before the application error, and the
// order is the same line [durableStepTimeoutMessage] draws for the same reason,
// stated there at length: a schedule-to-close budget that expires after a
// retryable failure wraps the last attempt's [temporal.ApplicationError] as the
// timeout's own cause, and `errors.As` walks straight through to find it — so
// asking about an application error first answers with the stale prior
// attempt's classification and hides that the budget is what ended the step.
// The message and the kind must name the same fact, and they now decide it the
// same way round.
//
// Every timeout type is one answer, not only the two a step's policy names.
// [durableStepTimeoutMessage] returns err untranslated for schedule-to-start
// and heartbeat because there is no per-step budget value it could quote for
// those — but "which budget" is a question about the sentence, not about the
// classification: a worker that never picked the activity up and a worker that
// stopped reporting progress both ended the attempt on time rather than on a
// fault, and neither is a defect in Flowstate (#915).
func recordedStepKind(err error) v1.ErrorKind {
	var run *ErrRunFailed
	if errors.As(err, &run) && run.Kind != "" {
		return run.Kind
	}

	var timeoutErr *temporal.TimeoutError
	if errors.As(err, &timeoutErr) {
		return v1.ErrorKindTimeout
	}

	var app *temporal.ApplicationError
	if errors.As(err, &app) {
		if kind, ok := v1.ParseErrorKind(app.Type()); ok {
			return kind
		}
	}

	var activity *temporal.ActivityError
	if errors.As(err, &activity) && activity.Unwrap() != nil {
		return v1.ClassifyError(activity.Unwrap())
	}

	return v1.ClassifyError(err)
}

// defaultMaxStepsPerRun defines how many steps to execute before
// continuing-as-new when no label overrides are provided.
const defaultMaxStepsPerRun = 200

// Run is the durable workflow entrypoint that supports Continue-As-New.
// It executes from the provided state and yields final step outputs when done.
//
// A thin wrapper around [runWorkflow] rather than the whole body itself, so
// that the one thing every terminal failure needs — being reported as a
// [temporal.ApplicationError] whose Type is the run's [v1.ErrorKind] — happens
// at a single choke point instead of at each of runWorkflow's several return
// statements. [classifyRunError] is what does that, and it is careful to
// leave a Continue-As-New error and a cancellation untouched; see its own
// comment for why touching either would be wrong.
//
// Registered as the workflow function itself (not runWorkflow), and passed by
// this same name to Continue-As-New below — both matter, because Temporal
// resumes a continued run by looking up the registered function by the value
// passed to NewContinueAsNewErrorWithOptions, and a workflow's own dispatch
// table always points at the registered name.
func Run(ctx workflow.Context, st *v1.RunState) (*v1.Workflow_StepOutputs, error) {
	workflowName := st.GetWorkflow().GetName()

	// #917's run-lifecycle metrics. Recorded here, around the whole of this
	// segment, rather than inside runWorkflow: this is the one function every
	// entry to a segment and every exit from one already passes through — the
	// choke point [classifyRunError]'s own doc names for the identical reason
	// — so it is the one place a start and a completion can be counted without
	// adding a second seam runWorkflow's several return statements would each
	// have to remember. workflow.Now is the deterministic clock every other
	// duration in this package uses; see recordRunStart/recordRunCompletion
	// for why a Continue-As-New segment records at most one of the two.
	recordRunStart(ctx, workflowName)
	started := workflow.Now(ctx)

	outputs, err := runWorkflow(ctx, st)

	// Both halves pass through unchanged, including the partial transcript a failed
	// run carries: Temporal drops the result when the error is non-nil, so this is
	// the honest shape rather than a value worth suppressing here.
	err = classifyRunError(err)

	recordRunCompletion(ctx, workflowName, err, workflow.Now(ctx).Sub(started))

	return outputs, err
}

// classifyRunError puts a terminal run failure's [v1.ErrorKind] where a client
// reading the failed workflow can recover it — [ApplicationError.Type], the
// same field [activityError] already uses to carry a task's own classification
// across the activity boundary — rather than leaving it to be reconstructed
// from Temporal's generic wrapping of an arbitrary Go error, which classifies
// by the error's *Go type name* and would report every run failure as
// "*engine.ErrRunFailed" regardless of what actually happened.
//
// Only an [*ErrRunFailed] is rewrapped. A Continue-As-New error must reach
// Temporal exactly as [workflow.NewContinueAsNewErrorWithOptions] built it —
// it is recognized by type, and wrapping it would turn a suspension into a
// reported failure — and neither it nor a cancellation is ever an
// [*ErrRunFailed], so errors.As already excludes both without a special case
// for either, the same way [stepFailed] excludes a cancellation from
// [failedAt] by never routing one through it.
func classifyRunError(err error) error {
	if err == nil {
		return nil
	}

	var run *ErrRunFailed
	if !errors.As(err, &run) {
		return err
	}

	return temporal.NewApplicationErrorWithOptions(run.Error(), run.errorKind().String(),
		temporal.ApplicationErrorOptions{Cause: err})
}

// runWorkflow is [Run]'s whole implementation, wrapped by it rather than
// registered directly — see [Run]'s comment for why.
func runWorkflow(ctx workflow.Context, st *v1.RunState) (*v1.Workflow_StepOutputs, error) {
	if st == nil || st.Workflow == nil || len(st.Workflow.Steps) == 0 {
		return nil, fmt.Errorf("workflow cannot be nil or empty")
	}

	// Captured before any of the reassignments below, for [executor.detailsCtx]'s
	// reason: this is the SDK's own root context, the one whose WorkflowOptions
	// pointer the built-in `__temporal_workflow_metadata` query reads — and
	// `withSignalDeliveryCompat` two lines down forks a new one, which
	// [workflow.SetCurrentDetails] would then write to invisibly.
	detailsCtx := ctx

	ctx = workflow.WithActivityOptions(ctx, defaultActivityOptions())

	// Every signal channel this run ever opens — here and for the rest of the
	// function, including drainSignals below — must be able to decode a signal
	// sent in either wire shape #194 straddles. See withSignalDeliveryCompat.
	ctx = withSignalDeliveryCompat(ctx)

	logger := workflow.GetLogger(ctx)

	// Registered before anything else happens, including the vars activity below.
	// Temporal fails a query whose handler is not installed yet, and a run in its
	// first moments is exactly when somebody asks what it is doing — "nowhere yet"
	// is a better answer than an error that reads like a broken worker.
	position := &progress{}

	// Registered here, beside the position, because one query answers both:
	// what a run is parked on is asked in the same breath as where it has got
	// to. See [setProgressQuery] and [waitRegistry].
	parked := &waitRegistry{}
	if err := setProgressQuery(ctx, position, parked); err != nil {
		return nil, fmt.Errorf("register progress query: %w", err)
	}
	if err := setStateQuery(ctx, position); err != nil {
		return nil, fmt.Errorf("register state query: %w", err)
	}

	// Before anything this segment does, including the vars activity below: a
	// worker that may not run this workload must not evaluate its `vars:` either.
	// See engine/plugins.go for why the check is split the way it is, and why this
	// is per segment rather than per run: Continue-As-New is where a run can land
	// on a worker that was not in the fleet when it started.
	if err := admitPlugins(ctx, st.GetWorkflow()); err != nil {
		return nil, err
	}

	// And before any expression can read it: a trigger recorded with a kind this
	// build cannot produce is refused rather than carried, because every
	// `${trigger.kind == "..."}` in the file would then compare against a word
	// nothing can ever answer with — an authorization-shaped hole opened by a
	// value nobody checked. Deterministic and pure, so it is safe in workflow
	// code and gives the same answer on replay.
	if err := v1.CheckTriggerContext(st.GetTrigger()); err != nil {
		return nil, &ErrRunFailed{Message: err.Error()}
	}

	// Initialize step outputs with carried-over minimal subset if present.
	stepOutputs := st.Outputs
	if stepOutputs == nil {
		stepOutputs = &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{}}
	} else if stepOutputs.StepValues == nil {
		stepOutputs.StepValues = map[string]*v1.Node_Outputs{}
	}

	// Determine step budget from state (injected by server) or default.
	stepsBudget := int(st.StepsBudget)
	if stepsBudget <= 0 {
		// Use default when not injected.
		stepsBudget = defaultMaxStepsPerRun
	}

	// The workflow's `vars:`, evaluated once for the run.
	//
	// Through an activity, and only when this segment does not already carry them.
	// Not because CEL in workflow code is unsafe — the executor below evaluates every
	// condition, every loop's `items:`, every step's own `vars:` and most task inputs
	// inline, and always has. The reason is Continue-As-New, which is the one seam
	// replay does not cover: a later segment starts from RunState rather than from
	// history, so a `vars:` block evaluated inline would be evaluated again at the top
	// of every segment against whatever cel-go that worker carries. See [WorkflowVars]
	// for the whole of it.
	//
	// A run that continued as new arrives holding the answer, so this is one round
	// trip per *run* rather than per segment.
	vars := st.GetVars()
	if len(vars) == 0 && len(st.GetWorkflow().GetVars()) > 0 {
		var evaluated v1.Scope
		if err := workflow.ExecuteActivity(withSummary(ctx, runVarsSummary), WorkflowVars, &v1.Scope{
			AmbientVars: st.GetWorkflow().GetVars(),
			Profile:     st.GetWorkflow().GetProfile(),

			// The run's own identity, for no reason this activity itself reads:
			// it evaluates expressions and asks nobody who is running. It is here
			// so the scope this activity travels in says whose work it is, which
			// is what [TenantInterceptor]'s activity guard checks. A scope that
			// named no tenant would be the one activity a wrong-tenant worker
			// could take without being refused.
			Identity: st.GetIdentity(),
		}).Get(ctx, &evaluated); err != nil {
			return nil, err
		}
		vars = evaluated.GetAmbientVars()
		st.Vars = vars
	}
	position.setVars(vars)

	// Execute through the recursive executor, which handles nested control flow
	// and records where to resume if the run has to be continued as new.
	exec := &executor{
		ctx:      ctx,
		spec:     st.Workflow,
		curSpec:  st.Workflow,
		identity: st.GetIdentity(),
		runID:    workflow.GetInfo(ctx).WorkflowExecution.RunID,
		// The profile comes from the spec in RunState, not from this build. A run
		// that suspended and continued as new is picked up by whichever worker takes
		// the next task, and that worker must evaluate against the vocabulary the
		// spec was compiled with rather than its own current one — otherwise a
		// deployment mid-rollout runs one workload against two dialects.
		scope:  varsScope(st.GetWorkflow().GetProfile(), stepOutputs, vars, st.GetInputs(), st.GetIdentity(), runAddress(ctx), st.GetTrigger()),
		budget: stepsBudget,
		resume: resumeFrames(st),

		// Signals that arrived before their step was reached, carried from the
		// run that suspended. A wait consumes from here before it blocks.
		signals: &signalCarry{pending: st.GetPendingSignals()},

		// The run's debug lease, if anybody takes one. Built per segment and
		// holding no lease to begin with, which is the honest starting state
		// rather than a gap: a *held* lease and a Continue-As-New cannot
		// coexist — the boundary that holds is visited before a step and the
		// budget is checked after one — so a segment always begins unheld. What
		// can survive the seam is an ask the run accepted delivery of and has
		// not yet acted on, and that travels in `PendingSignals` exactly as an
		// early-arriving approval does. See debuglease.go.
		debug: &debugControl{
			run: runAddress(ctx),

			// Read from the run's own specification rather than from the memo,
			// because workflow code cannot see a memo — and it does not need
			// to: this is the presence question, and the two copies agree about
			// presence by construction. See [debugControl.declared].
			declared: st.GetWorkflow().GetDebug() != nil,
		},

		progress:   position,
		detailsCtx: detailsCtx,
		waits:      parked,

		// The compensations registered by segments that already ran, oldest first.
		// A saga is exactly the workload that outlives one segment — provision,
		// suspend, fail — so the run that fails is usually not the run that did the
		// work it has to take back.
		undo: v1.NewUndoLog(st.GetPendingUndo()),

		// The run's own top level. Explicit rather than relied on as the zero
		// value: every other construction site sets this deliberately, and this
		// one should read the same way.
		undoScope: v1.UndoScopeTopLevel,
	}

	err := exec.runNodes(st.Workflow.GetSteps(), 0, 0)
	// WaitForCancellation lets an activity that wins the cancellation race report
	// success. The workflow context is still cancelled in that case, and success
	// must not turn an operator's cancellation into a completed run or bypass the
	// compensation the activity just registered.
	if err == nil {
		err = ctx.Err()
	}
	switch {
	case err == nil:
		// The declared outputs, evaluated once — here, inline, in workflow code.
		//
		// # Why not an activity, when `vars:` is one
		//
		// The workflow's `vars:` go through an activity for one reason, and it is not
		// that CEL in workflow code is unsafe: this executor evaluates every
		// condition, every loop's `items:`, every step's own `vars:` and most task
		// inputs inline and always has (invariant 4 says so explicitly). The reason is
		// Continue-As-New — a later segment starts from `RunState` rather than from
		// history, so an inline `vars:` would be *re-evaluated* at the top of every
		// segment against whatever cel-go that worker carries, and a value that changes
		// halfway through a run is what [WorkflowVars] exists to prevent.
		//
		// These are evaluated in the segment that finishes the run, exactly once,
		// and never again — there is no later segment to re-evaluate them in, and a
		// replay of *this* segment runs on the interpreter this segment is pinned to.
		// So the seam that argument is about does not exist here, and paying a round
		// trip per run to avoid a hazard that cannot arise would be paying for the
		// shape of the answer rather than for the answer.
		//
		// Failing the run when an output cannot be computed is deliberate: an output
		// is the answer the caller asked for, and a run that cannot produce it has not
		// succeeded.
		runOutputs, outputsErr := v1.EvalRunOutputs(context.Background(), st.GetWorkflow(), exec.scope)
		if outputsErr == nil {
			stepOutputs.RunOutputs = runOutputs
			if sizeErr := v1.CheckRunResultSize(stepOutputs); sizeErr != nil {
				logger.Error("cannot complete run", "error", sizeErr.Error())
				outputsErr = sizeErr
			}
		}
		if outputsErr != nil {
			// Out through [compensate], the way every other failure leaves this
			// function. A run that executed every step and then could not report
			// them has failed, and a failed saga takes back what it did:
			// reporting FAILED while leaving in place every resource those steps
			// created is the outcome compensation exists to prevent, and it does
			// not become acceptable because the failure arrived after the last
			// step rather than during one.
			return v1.PartialTranscript(stepOutputs),
				compensate(ctx, exec, &ErrRunFailed{Message: outputsErr.Error()})
		}

		return stepOutputs, nil

	case errors.Is(err, errContinueAsNew):
		carry := compactOutputsForFrames(st.Workflow, exec.frames, stepOutputs)

		// Drained before suspending, and this is the whole reason it happens
		// here: a run that continues as new drops whatever is still buffered on
		// a signal channel it never read. A workload whose approval arrived
		// while it was on an earlier step would otherwise resume with the
		// approval gone and wait forever.
		pending := drainSignals(ctx, st.Workflow, exec.signals.pending)

		// And the two channels the engine owns rather than the specification.
		// A pause ask buffered when this segment ran out of budget would
		// otherwise be dropped at the seam — a `flow signal` that reported
		// success and did nothing — which is precisely the failure drainSignals
		// exists to prevent, on a channel [v1.SignalNames] cannot know about.
		pending = drainDebugAsks(ctx, pending, exec.debug)

		next := &v1.RunState{
			Workflow:    st.Workflow,
			Outputs:     carry,
			StepsBudget: int32(stepsBudget),
			Frames:      exec.frames,

			PendingSignals: pending,

			// Evaluated once for the whole run, not once per segment. A continued
			// run takes whichever interpreter version is current (invariant 10), so
			// re-evaluating here could hand later steps a different answer than
			// earlier ones saw — a value changing under a workload halfway through,
			// for no cause visible in the file.
			Vars: st.GetVars(),

			// Checked and defaulted once, at submit, and carried unchanged — for the
			// reason `Vars` above is carried, and one more: a run started before this
			// field existed carries nothing here, which reads as a run with no
			// arguments, which is exactly what it is. No compatibility arm needed
			// (invariant 10).
			Inputs: st.GetInputs(),

			// Identity must survive Continue-As-New. A long workload spans
			// several runs, and a step in the last one acts on behalf of the
			// same caller as a step in the first; dropping it would silently
			// turn an authenticated workload into an anonymous one partway
			// through.
			Identity: st.Identity,

			// How the run started must survive Continue-As-New for the reason
			// Identity does, and for one that is specific to it: this field is what
			// makes `${trigger.kind}` replay-safe at all. Dropping it at the seam
			// would give a workload one answer before it suspended and another
			// after — a nightly sweep that stopped being a nightly sweep halfway
			// through, with nothing in the file to explain it, which is precisely
			// the drift `now`'s placement exists to prevent for the clock.
			Trigger: st.GetTrigger(),

			// What the run has still to take back if it fails later. Carried rather
			// than re-derived, because it *cannot* be re-derived: which steps
			// succeeded is not a property of the specification, and the values a
			// compensation was resolved with belong to outputs the compaction above
			// is entitled to have dropped.
			//
			// Weighed by CheckRunStateSize below along with everything else, which
			// measures the whole message rather than the fields somebody remembered
			// — so this was bounded on the day it was added.
			PendingUndo: exec.undo.Pending(),
		}
		// Refused here rather than left to Temporal, because Temporal's refusal is
		// not an outcome — it fails the workflow task, and a failed workflow task
		// is retried indefinitely. The run stays RUNNING, climbs an attempt count
		// nobody is watching, and occupies a worker on every attempt. It never
		// completes and it never fails, which is the one state a durable system
		// must not leave a workload in.
		//
		// proto.Size is pure arithmetic over a value the workflow already holds, so
		// it is safe in workflow code — no clock, no I/O, and the same answer on
		// replay.
		//
		// A run that reaches this has usually made a step's output too large to
		// carry, which is why the message names both halves of the total: the
		// specification is the part an author sized deliberately, and the outputs
		// are the part that grew.
		if err := v1.CheckRunStateSize(next); err != nil {
			logger.Error("cannot continue as new", "error", err.Error())
			// Out through [compensate], the identical reason the completion-time
			// size failure above does: a run whose carried state cannot fit is a
			// failed run, and a failed saga takes back what it did. Returning
			// ErrRunFailed directly here would report FAILED while leaving every
			// effect this segment's steps performed in place — the saga-contract
			// violation the original fix on this path existed to close, reopened
			// by measuring the bound correctly and hitting it on a run this exact
			// check used to let through.
			return v1.PartialTranscript(stepOutputs), compensate(ctx, exec, &ErrRunFailed{Message: err.Error()})
		}

		logger.Info("continuing as new",
			"frames", len(exec.frames), "carried_signals", len(pending))

		// The one moment a run may change interpreter version.
		//
		// [Run] is registered Pinned, so a run executes to its end on the
		// interpreter it started on — history is replayed through code, and
		// changing the interpreter under a run in flight is the same hazard as
		// reading a clock in workflow code. Pinned alone, though, would hold a
		// long workload on its original version across every Continue-As-New for
		// as long as it lives, and a version with runs still on it cannot be
		// drained: an operator could never retire one.
		//
		// Here that is safe, and this is the only place it is. The next run
		// replays nothing — it starts from the state below rather than from
		// history — so the interpreter that resumes it need not be the one that
		// suspended it. What the two must agree about is RunState itself, which
		// is why its compatibility is an invariant rather than a convention (see
		// docs/ARCHITECTURE.md).
		//
		// Inert where versioning is off: with no deployment on the task queue
		// there is no version to move to. See engine/versioning.go.
		return nil, workflow.NewContinueAsNewErrorWithOptions(ctx, workflow.ContinueAsNewErrorOptions{
			InitialVersioningBehavior: workflow.ContinueAsNewVersioningBehaviorAutoUpgrade,
		}, Run, next)

	default:
		// The transcript accompanies the failure, exactly as it does in the local
		// driver's eval, see [v1.PartialTranscript] for what it is entitled to
		// contain. Temporal drops a workflow's result when the workflow returns an
		// error, so this reaches an in-process caller of this function and no client;
		// what it buys today is that the two drivers hand the same record to the same
		// question, which is what invariant 3 asks of them. Putting it in front of a
		// remote client would need a carrier bounded in bytes, since an unbounded
		// payload on a failure path fails the workflow *task* rather than the run.
		return v1.PartialTranscript(stepOutputs), compensate(ctx, exec, err)
	}
}

// compensate takes back what the run already did, and returns the failure it will
// report.
//
// # A cancellation compensates too, in a scope it cannot reach
//
// `flow cancel` is the verb whose whole promise is that a workload gets to release
// what it holds on the way out, so a cancelled saga takes itself back. What makes
// this the harder half is that the context the cancellation arrives on is the one
// every compensation would be scheduled on, and Temporal refuses an activity
// scheduled on a cancelled context immediately. Compensating on `ctx` would report
// a run that could not undo anything, having never attempted any of it.
//
// So the compensations run on `workflow.NewDisconnectedContext`, which the
// cancellation does not reach, bounded by [v1.UndoBudget] — a run told to stop must
// not then work indefinitely, and the local driver reads the same constant for the
// same reason. The bound is spent across the compensations together: what is left
// of it narrows each activity's own ceiling, and an entry the budget leaves no room
// for is recorded as [v1.ErrUndoBudget] rather than dropped.
//
// `flow terminate` still runs none of this, and that is the distinction the two
// verbs already carried: Temporal terminate executes no workflow code at all, so
// the CLI's two spellings land exactly on the two semantics with no flag invented
// for it.
//
// [stepFailed] refuses to wrap a cancellation for the neighbouring reason —
// Temporal decides CANCELED from the error's type — so what arrives here as a
// cancellation is exactly what arrives there as one, and what leaves here must
// still be one. That is why this does not reach for [v1.UndoRunError] the way the
// failure path does: `fmt.Errorf("%w…")` would keep the type findable by
// `errors.As` and *also* throw the summary away, because a cancelled workflow is
// closed with a CancelWorkflowExecution command whose only payload is the error's
// details. Returning a fresh cancellation carrying the summary as its details puts
// it in history, where `flow get` reads it.
//
// # Failing after compensating is still failing
//
// The run reports FAILED, with the summary appended to the failure that caused it.
// A compensated run has not succeeded: the work it was asked to do did not happen.
// What compensation changes is the state of the world, not the outcome of the run,
// and that is why there is no third status — see docs/DSL.md, and invariant 10 for
// what a new one would cost every reader of a closed run.
func compensate(ctx workflow.Context, exec *executor, err error) error {
	if exec.undo.Len() == 0 {
		return err
	}

	if temporal.IsCanceledError(err) {
		return compensateCancelled(ctx, exec)
	}

	// Two decisions, and they are not the same question — the trap eval.go's
	// local driver documents at [failRun]. *Which context compensates* turns
	// only on whether ctx is already done: a dead context refuses every entry
	// regardless of what the run failed of. *What the run reports* turns on
	// what err actually is. Reaching this line with a non-cancellation err and
	// an already-cancelled ctx is completion racing a stop: the last activity
	// returned, output evaluation or [v1.CheckRunResultSize] failed afterward,
	// and only then does ctx.Err() read as done. Compensating on ctx in that
	// case has Temporal refuse every entry before it is attempted — a run that
	// reports it could not undo work it never tried to.
	var results []v1.UndoResult
	if ctx.Err() != nil {
		results = compensateOnDisconnectedContext(ctx, exec)
	} else {
		workflow.GetLogger(ctx).Info("compensating a failed run",
			"pending", exec.undo.Len(), "error", err.Error())

		results = v1.RunUndoLog(exec.undo, func(entry *v1.PendingUndo) error {
			return exec.runUndoTask(ctx, entry, 0)
		})
	}

	// Composed from the inner failure's own message, exactly as failedAt does, so
	// the run's failure reads as one sentence rather than restating this driver's
	// preamble in the middle of it. What is appended is [v1.UndoSummary]'s output
	// and nothing else, which is the string the local driver appends to its own
	// failure — the one value that has to be identical for a local run to rehearse
	// what a compensated production run will say.
	var inner *ErrRunFailed
	if errors.As(err, &inner) {
		return &ErrRunFailed{
			Message:          inner.Message + v1.UndoSummary(results),
			Recorded:         inner.Recorded,
			recordedFromTask: inner.recordedFromTask,
			Kind:             inner.Kind,
		}
	}

	return v1.UndoRunError(err, results)
}

// compensateCancelled takes back what a cancelled run did, and returns the
// cancellation it still reports.
//
// The deadline is computed from `workflow.Now`, which is the replay-safe clock:
// on a replay it answers with the recorded time rather than the wall clock, so the
// same compensations are attempted and the same ones are skipped. `time.Now` here
// would be a straightforward invariant-4 violation — the second-worst kind, since
// it would only diverge on the replays of runs that were cancelled.
//
// The remaining budget is re-read before each entry rather than divided up in
// advance. A compensation that finishes quickly leaves its unused share to the
// ones behind it, which is what makes the number a budget for the run rather than
// a quota per step.
func compensateCancelled(ctx workflow.Context, exec *executor) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("compensating a cancelled run", "pending", exec.undo.Len())

	results := compensateOnDisconnectedContext(ctx, exec)
	summary := v1.UndoSummary(results)
	logger.Info("compensated a cancelled run", "summary", summary)

	// A fresh cancellation rather than the one that arrived, carrying the summary
	// as its details — see this function's caller for why wrapping cannot work
	// here. The run still closes CANCELED, which is decided by `errors.As` finding
	// a `*temporal.CanceledError`, and this is one.
	return temporal.NewCanceledError(summary)
}

// compensateOnDisconnectedContext runs every pending undo on a context the
// cancellation that triggered it does not reach, bounded by [v1.UndoBudget].
//
// Factored out of [compensateCancelled] because it is not only that function's
// scenario: [compensate] reaches here too, whenever the run's context is
// already done but the *error being reported* is not itself a cancellation —
// the last activity completing just as a stop arrives, with output evaluation
// or the size check failing afterward. Both callers need the identical
// disconnected, budgeted context Temporal will actually schedule the undo
// activities on; only what each caller does with the resulting summary
// differs.
func compensateOnDisconnectedContext(ctx workflow.Context, exec *executor) []v1.UndoResult {
	// The scope the cancellation does not reach. The cancel func is deliberately
	// discarded: nothing here should be able to cancel this context, and the
	// deadline below is what ends it.
	uctx, _ := workflow.NewDisconnectedContext(ctx)
	deadline := workflow.Now(uctx).Add(v1.UndoBudget)

	return v1.RunUndoLogWithin(exec.undo,
		func() time.Duration { return deadline.Sub(workflow.Now(uctx)) },
		func(entry *v1.PendingUndo, within time.Duration) error {
			return exec.runUndoTask(uctx, entry, within)
		})
}

// resumeFrames returns the position a run resumes from.
//
// A run started before frames existed carries only next_step, so that is
// translated into an equivalent single frame rather than being ignored — an
// in-flight workload must not restart from the beginning because the state model
// grew a field.
func resumeFrames(st *v1.RunState) []*v1.Frame {
	if frames := st.GetFrames(); len(frames) > 0 {
		return frames
	}
	if st.GetNextStep() > 0 {
		return []*v1.Frame{{NextNode: st.GetNextStep()}}
	}
	return nil
}

// compactOutputsForFrames carries forward only the outputs the remaining work can
// still reference.
//
// With nested control flow the remaining work is no longer a suffix of the
// top-level steps, so this is conservative: it keeps everything reachable from the
// top-level step the run will resume at. Carrying an output that turns out to be
// unnecessary costs payload; dropping one that is needed breaks the run.
func compactOutputsForFrames(spec *v1.Workflow, frames []*v1.Frame, outputs *v1.Workflow_StepOutputs) *v1.Workflow_StepOutputs {
	if len(frames) == 0 {
		return outputs
	}
	from := int(frames[0].GetNextNode())

	// Mid-loop suspension resumes inside the step at frames[0], so that step's
	// own inputs still matter.
	if len(frames) > 1 && from > 0 {
		from--
	}
	return compactOutputsForRemainingSteps(spec.GetSteps(), from, outputs, spec.GetDeclaredOutputs())
}

// failedStepOutputs records a failure as a step's outputs.
//
// A step allowed to continue past its own failure still has to leave something
// behind, so that a later step can branch on whether it worked. Reporting the
// failure under [v1.StepErrorOutput] makes that expressible as
// `${steps.<id>.error}`, and its absence means the step succeeded.
//
// The text recorded is the extracted, driver-independent one rather than
// err.Error(): what reaches here is wrapped in this driver's transport, and the
// local driver records the same failure without any of it.
func failedStepOutputs(err error) *v1.Node_Outputs {
	// Some steps' entries are richer than the text ([v1.StepFailureRecord]): an
	// exhausted loop's is `error` plus the `results` that ran, so the transcript
	// distinguishes an iteration that ran and failed from one the spent budget
	// never let start, and a failed switch's is `error` plus the arm it selected,
	// so the branch that ran is still on the record. Each was extracted by
	// failedAt at the one site that failure is raised, and is deliberately not
	// propagated into the wrappers built above it — see the recordedOwn field —
	// so only that step's own entry ever carries it, which is the same
	// containment the local driver's failureRecord gets from its direct type
	// assertion.
	var run *ErrRunFailed
	if errors.As(err, &run) && run.recordedOwn != nil {
		return run.recordedOwn
	}

	recorded, _ := recordedStepError(err)

	return v1.FailedStepOutputs(recorded)
}

// collectNodeRefs, collectValueRefs and neededOutputs moved to
// pkg/flowstate/v1/refs.go (v1.CollectNodeRefs, v1.CollectValueRefs,
// v1.NeededOutputs — along with the wholeStep marker itself, v1.WholeStep),
// because #229's static loop-results suppression needs the identical walk the
// local driver would otherwise have to reimplement — one CEL-reference walker
// for both drivers rather than a third one drifting from this one. These names
// stay as thin wrappers so the tests that call them by their engine-package
// name (walkers_guard_test.go, compactvars_internal_test.go) are unaffected.
func collectNodeRefs(node *v1.Node, prev *v1.Workflow_StepOutputs, refs map[string]map[string]struct{}) {
	v1.CollectNodeRefs(node, prev, refs)
}

func collectValueRefs(value *v1.Value, prev *v1.Workflow_StepOutputs, refs map[string]map[string]struct{}) {
	v1.CollectValueRefs(value, prev, refs)
}

func neededOutputs(full *v1.Node_Outputs, fields map[string]struct{}) *v1.Node_Outputs {
	return v1.NeededOutputs(full, fields)
}

// compactPrevOutputsForTask returns a reduced view of prev outputs that includes
// only the step outputs and fields referenced by the given task's inputs.
//
// A task's inputs are resolved before the activity is scheduled, so what reaches here
// is usually already values and the reduced view is empty — which is the point: the
// activity is handed what the step names and not the whole run.
//
// It used to also parse a literal string as CEL, for the one task whose main input was
// an expression carried as text rather than as a parsed one. That task retired at
// edition v2026.2, and with it the only reason a *literal* could hold a reference.
func compactPrevOutputsForTask(task *v1.Task, prev *v1.Workflow_StepOutputs) *v1.Workflow_StepOutputs {
	if prev == nil || task == nil || len(task.Inputs) == 0 {
		return prev
	}
	refs := map[string]map[string]struct{}{}

	for _, value := range task.Inputs {
		v1.CollectValueRefs(value, prev, refs)
	}

	if len(refs) == 0 {
		return &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{}}
	}

	trimmed := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{}}
	for stepID, fields := range refs {
		full, ok := prev.StepValues[stepID]
		if !ok {
			continue
		}
		// See the matching comment in compactOutputsForRemainingSteps: full may be
		// nil or empty and the key still has to survive a real reference.
		trimmed.StepValues[stepID] = neededOutputs(full, fields)
	}
	return trimmed
}

// compactOutputsForRemainingSteps examines the remaining steps and returns a
// minimal subset of step outputs required to evaluate their inputs.
//
// The workflow's declared outputs count as remaining work, which is why they are a
// parameter rather than something a caller may forget. They are evaluated after the
// *last* step, in the run's own scope, so a `${steps.deploy.url}` written there is a
// reference that outlives every step between here and the end — and pruning it
// would fail the run at the one moment there is nothing left to retry, on a
// specification that never changed. It is the same shape as the `vars:` block this
// walk once missed, and the same shape as a reference in map-key position: a site
// the language has and the compactor did not know about.
func compactOutputsForRemainingSteps(
	steps []*v1.Node,
	from int,
	prev *v1.Workflow_StepOutputs,
	declaredOutputs []*v1.OutputDeclaration,
) *v1.Workflow_StepOutputs {
	if prev == nil || prev.StepValues == nil || (from >= len(steps) && len(declaredOutputs) == 0) {
		return prev
	}
	refs := map[string]map[string]struct{}{}
	for i := from; i < len(steps); i++ {
		collectNodeRefs(steps[i], prev, refs)
	}
	for _, declaration := range declaredOutputs {
		collectValueRefs(declaration.GetValue(), prev, refs)
	}

	if len(refs) == 0 {
		return &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{}}
	}
	trimmed := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{}}
	for stepID, fields := range refs {
		full, ok := prev.StepValues[stepID]
		if !ok {
			continue
		}
		// full may be nil, or non-nil with a nil/empty NamedValues map — both are
		// legitimate shapes for a step that ran and produced no named outputs (a
		// wait step, or a continue_on_error task with none declared). Both getters
		// below are nil-receiver safe, and the key still has to survive: see the
		// comment on neededOutputs.
		trimmed.StepValues[stepID] = neededOutputs(full, fields)
	}
	return trimmed
}

// varsScope builds the run's initial scope with its evaluated vars in place.
//
// A named function rather than two lines at the call site, so that "the scope every
// step starts from" is one thing with one definition. The local driver reaches the
// same state through v1.EvalWorkflowVars; what must not differ between them is the
// scope a first step sees, and that is easier to compare when each driver has exactly
// one place that builds it.
func varsScope(profile string, outputs *v1.Workflow_StepOutputs, vars, inputs map[string]*v1.Value, identity *v1.WorkloadIdentity, address *v1.RunAddress, trigger *v1.TriggerContext) *v1.Scope {
	scope := v1.NewScope(profile, outputs)
	scope.AmbientVars = vars

	// Taken from `RunState` rather than derived from the specification's
	// declarations, and that is the whole of the durable driver's part in this: the
	// checking and the defaulting happened once, at submit, where a caller was still
	// there to be refused. Re-deriving them here would run at the top of *every*
	// segment, so a declaration edited between deploys could change an argument
	// underneath a run in flight — the class of thing invariant 10 exists to stop.
	scope.Inputs = inputs

	// The run's own starter identity, established once when the run was requested
	// (server.go) and carried in RunState across every Continue-As-New — see
	// [v1.RunState.identity]. Never Local: the durable driver always has a server
	// in front of it, even when that server attests an anonymous caller because no
	// identity provider is configured. That is still an attestation, not the
	// absence of one, and [v1.Scope.local] exists precisely so the two are never
	// confused.
	scope.Identity = identity

	// The run's own address. Derived from Temporal's own view of this execution
	// rather than carried in `RunState`, which is the one place this differs in
	// kind from the four fields above: a run id in `RunState` would be a value
	// written by one interpreter version and read by another (invariant 10) for
	// no benefit, because the substrate can always answer the question directly
	// and answers it identically on every replay. See [runAddress].
	scope.Address = address

	// How the run started, taken from `RunState` rather than from the substrate,
	// which is the one place this differs in kind from the address above: no
	// cluster can answer "why is this run happening". It was established once, by
	// whichever entry path admitted the submission, and is carried across every
	// Continue-As-New — which is exactly what makes reading it replay-safe, and
	// what separates it from a clock read (invariant 4).
	scope.Trigger = trigger

	return scope
}

// runAddress is the address this run reports under `run.workflow_id` and
// `run.run_id`.
//
// Both halves are read from `workflow.GetInfo`, which is replay-safe: the same
// values come back on every replay of the same execution, so an expression that
// embeds the address in a callback URL computes the same URL after a worker
// crash as before it.
func runAddress(ctx workflow.Context) *v1.RunAddress {
	info := workflow.GetInfo(ctx)

	return RunAddressFrom(info.WorkflowExecution.ID, info.FirstRunID, info.WorkflowExecution.RunID)
}

// RunAddressFrom builds a run's address from the three things Temporal knows
// about an execution, and choosing between the last two is its whole substance.
//
// Continue-As-New starts a *new* Temporal execution with the same workflow id
// and a fresh run id, and this engine continues as new on its own schedule — a
// step budget an author never sees and cannot predict. A workload that handed
// out `currentRunID` would therefore report one address before it suspended and
// a different one after, with nothing in the file to explain it. `firstRunID`
// (Temporal's `FirstRunID`) is preserved along the whole chain of continued
// executions, so it names the run an author believes they wrote. See
// [v1.RunAddress.run_id].
//
// The fallback is for the one source that does not offer the first id at all:
// Temporal's own workflow test environment leaves it unset. Falling back to the
// current execution is correct there rather than merely convenient, because a
// run that has never continued as new has one execution and the two ids are the
// same value.
//
// Exported and taking plain strings so the choice can be tested directly. It is
// exactly the kind of rule a test environment cannot exercise — it never
// populates the field the rule is about — and a bound nothing reaches is a bound
// nothing tests.
func RunAddressFrom(workflowID, firstRunID, currentRunID string) *v1.RunAddress {
	runID := firstRunID
	if runID == "" {
		runID = currentRunID
	}

	return &v1.RunAddress{WorkflowId: workflowID, RunId: runID}
}
