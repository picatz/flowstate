package engine

import (
	"errors"
	"fmt"
	"slices"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

type ErrRunFailed struct {
	Message string
}

func (e *ErrRunFailed) Error() string {
	return fmt.Sprintf("engine: flowstate run failed: %s", e.Message)
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
func stepFailed(err error, format string, args ...any) error {
	if temporal.IsCanceledError(err) {
		return err
	}
	return &ErrRunFailed{Message: fmt.Sprintf(format, args...)}
}

const RunTaskQueueName = "flowstate-run-task-queue"

// defaultMaxStepsPerRun defines how many steps to execute before
// continuing-as-new when no label overrides are provided.
const defaultMaxStepsPerRun = 200

// Run is the durable workflow entrypoint that supports Continue-As-New.
// It executes from the provided state and yields final step outputs when done.
func Run(ctx workflow.Context, st *v1.RunState) (*v1.Workflow_StepOutputs, error) {
	if st == nil || st.Workflow == nil || len(st.Workflow.Steps) == 0 {
		return nil, fmt.Errorf("workflow cannot be nil or empty")
	}

	ctx = workflow.WithActivityOptions(ctx, defaultActivityOptions())

	logger := workflow.GetLogger(ctx)

	// Registered before anything else happens, including the vars activity below.
	// Temporal fails a query whose handler is not installed yet, and a run in its
	// first moments is exactly when somebody asks what it is doing — "nowhere yet"
	// is a better answer than an error that reads like a broken worker.
	position := &progress{}
	if err := setProgressQuery(ctx, position); err != nil {
		return nil, fmt.Errorf("register progress query: %w", err)
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
		if err := workflow.ExecuteActivity(ctx, WorkflowVars, &v1.Scope{
			AmbientVars: st.GetWorkflow().GetVars(),
			Profile:     st.GetWorkflow().GetProfile(),
		}).Get(ctx, &evaluated); err != nil {
			return nil, err
		}
		vars = evaluated.GetAmbientVars()
		st.Vars = vars
	}

	// Execute through the recursive executor, which handles nested control flow
	// and records where to resume if the run has to be continued as new.
	exec := &executor{
		ctx:      ctx,
		spec:     st.Workflow,
		identity: st.GetIdentity(),
		runID:    workflow.GetInfo(ctx).WorkflowExecution.RunID,
		// The profile comes from the spec in RunState, not from this build. A run
		// that suspended and continued as new is picked up by whichever worker takes
		// the next task, and that worker must evaluate against the vocabulary the
		// spec was compiled with rather than its own current one — otherwise a
		// deployment mid-rollout runs one workload against two dialects.
		scope:  varsScope(st.GetWorkflow().GetProfile(), stepOutputs, vars),
		budget: stepsBudget,
		resume: resumeFrames(st),

		// Signals that arrived before their step was reached, carried from the
		// run that suspended. A wait consumes from here before it blocks.
		signals:  &signalCarry{pending: st.GetPendingSignals()},
		progress: position,
	}

	err := exec.runNodes(st.Workflow.GetSteps(), 0)
	switch {
	case err == nil:
		return stepOutputs, nil

	case errors.Is(err, errContinueAsNew):
		carry := compactOutputsForFrames(st.Workflow, exec.frames, stepOutputs)

		// Drained before suspending, and this is the whole reason it happens
		// here: a run that continues as new drops whatever is still buffered on
		// a signal channel it never read. A workload whose approval arrived
		// while it was on an earlier step would otherwise resume with the
		// approval gone and wait forever.
		pending := drainSignals(ctx, st.Workflow, exec.signals.pending)

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
			// Identity must survive Continue-As-New. A long workload spans
			// several runs, and a step in the last one acts on behalf of the
			// same caller as a step in the first; dropping it would silently
			// turn an authenticated workload into an anonymous one partway
			// through.
			Identity: st.Identity,
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
			return nil, &ErrRunFailed{Message: err.Error()}
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
		return nil, err
	}
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
	return compactOutputsForRemainingSteps(spec.GetSteps(), from, outputs)
}

// failedStepOutputs records a failure as a step's outputs.
//
// A step allowed to continue past its own failure still has to leave something
// behind, so that a later step can branch on whether it worked. Reporting the
// failure under a well-known `error` output makes that expressible as
// `${steps.<id>.error}`, and its absence means the step succeeded.
func failedStepOutputs(err error) *v1.Node_Outputs {
	return &v1.Node_Outputs{
		NamedValues: map[string]*v1.Value{
			"error": v1.NewLiteral(err.Error()),
		},
	}
}

// rootedStepRef reads a reference written under the steps root, returning the
// step it names and the output selected on it.
//
// Two shapes resolve, and a third deliberately does not:
//
//	steps.a.result         -> step "a", output "result"
//	steps.a.result.field   -> step "a", output "result"  (the outer select is CEL's)
//	steps.a                -> step "a", every output
//	steps                  -> not a reference to any one step
//
// The last is why this returns false rather than the whole root: an expression
// naming `steps` bare needs all of them, and saying so is the caller's job.
func rootedStepRef(sel *expr.Expr_Select) (step, field string, ok bool) {
	// Walked to the base rather than matched at a fixed depth, because the depth is
	// not fixed: `steps.a.result.code` is three selects over the root and reaching
	// only two of them leaves the reference unrecognised — which prunes every
	// output rather than one, silently.
	var fields []string
	node := sel
	for node != nil {
		fields = append(fields, node.GetField())
		operand := node.GetOperand()
		if ident := operand.GetIdentExpr(); ident != nil {
			if ident.GetName() != v1.StepsRoot {
				return "", "", false
			}
			break
		}
		node = operand.GetSelectExpr()
	}
	if node == nil {
		return "", "", false
	}

	// Collected outermost first, so the step is last and its output second to last.
	slices.Reverse(fields)
	switch len(fields) {
	case 0:
		return "", "", false
	case 1:
		// `steps.a` — the whole step.
		return fields[0], "", true
	default:
		// Anything deeper selects into the output, which is CEL's business. Only the
		// output itself has to be kept.
		return fields[0], fields[1], true
	}
}

// collectRefsFromExpr recursively walks a CEL expression and returns a map of
// step IDs to the set of fields referenced on that step. If a step is
// referenced without a field (e.g., just `a`), the field set will be empty to
// indicate that the whole step's outputs are required.
// wholeStep marks a step whose every output is needed, in the field set that
// otherwise names the ones that are.
//
// An empty set used to mean the same thing, and could not survive company. A step
// referenced whole by one expression and by field from another — `${steps.a}` in a
// step's `vars:` beside `${steps.a.foo}` in its input — recorded the empty set
// first and then had `foo` put into it, after which "everything" and "just foo"
// were the same value. The resumed segment got `foo` alone and failed on the next
// field the whole reference reached for.
//
// The empty string is the marker because it cannot collide: an output name is
// `min_len: 1` in the schema, so no field can ever be spelled this way.
const wholeStep = ""

// markWholeStep records that every output of a step is needed.
func markWholeStep(refs map[string]map[string]struct{}, step string) {
	fields, seen := refs[step]
	if !seen {
		fields = map[string]struct{}{}
		refs[step] = fields
	}
	fields[wholeStep] = struct{}{}
}

// markStepField records that one named output of a step is needed.
func markStepField(refs map[string]map[string]struct{}, step, field string) {
	fields, seen := refs[step]
	if !seen {
		fields = map[string]struct{}{}
		refs[step] = fields
	}
	fields[field] = struct{}{}
}

// neededOutputs returns the outputs to carry for one step, or the whole set when
// anything asked for it whole.
func neededOutputs(full *v1.Node_Outputs, fields map[string]struct{}) *v1.Node_Outputs {
	if _, whole := fields[wholeStep]; whole || len(fields) == 0 {
		return full
	}

	nv := map[string]*v1.Value{}
	for field := range fields {
		if value, has := full.GetNamedValues()[field]; has {
			nv[field] = value
		}
	}
	if len(nv) == 0 {
		return nil
	}

	return &v1.Node_Outputs{NamedValues: nv}
}

func collectRefsFromExpr(e *expr.Expr, prev *v1.Workflow_StepOutputs, refs map[string]map[string]struct{}) {
	if e == nil {
		return
	}
	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_IdentExpr:
		name := kind.IdentExpr.GetName()
		if prev == nil || prev.StepValues == nil {
			return
		}
		if _, ok := prev.StepValues[name]; ok {
			markWholeStep(refs, name)

			return
		}
		// The root named on its own — `has(steps.a)`, or handed to a macro — asks
		// for all of them, and there is no way to narrow that from here. Kept whole
		// rather than pruned to nothing, since being wrong in the other direction
		// costs history size and being wrong this way costs the run.
		if name == v1.StepsRoot {
			for id := range prev.StepValues {
				markWholeStep(refs, id)
			}
		}
	case *expr.Expr_SelectExpr:
		// A reference rooted at `steps` names its step one level further in:
		// `steps.a.result` is Select(Select(Ident("steps"), "a"), "result"), so the
		// step is the *field* of the inner select rather than an ident at all.
		//
		// Handled before the bare form, and handled here rather than left to the
		// walk below, because the walk cannot see it: it looks for an ident naming
		// a step, finds `steps`, matches nothing, and records nothing — after which
		// the caller prunes every output and the activity is handed an empty map.
		// No error anywhere, and the run only fails after a Continue-As-New.
		if step, field, ok := rootedStepRef(kind.SelectExpr); ok {
			if prev != nil && prev.StepValues != nil {
				if _, known := prev.StepValues[step]; known {
					if field == "" {
						markWholeStep(refs, step)
					} else {
						markStepField(refs, step, field)
					}
				}
			}
			return
		}

		// Handle a.b[.c...] by walking the operand chain down to the root ident.
		// We record the first field selected after the step ident.
		// For nested selects, this still captures the top-level output field.
		// E.g., a.result.subfield -> keep `result` from step `a`.
		// Traverse to find the base ident name.
		// Recursed into only when the operand is something other than the step
		// ident itself, which the loop below reads directly.
		//
		// The distinction is what `a` *means* here. Visited on its own it is a
		// whole-step reference and marks the step whole; visited as the operand of
		// `a.result` it is the root of a field reference, and marking it whole
		// there would carry every output of every step any expression touched —
		// the opposite mistake to the one [wholeStep] exists to prevent, and one
		// that shows up only as history nobody can explain.
		//
		// This used to work by accident: the ident case created an *empty* set,
		// the loop below then put a field into it, and empty-means-whole was
		// quietly overwritten. Making the marker explicit made the accident visible
		// as two failing tests.
		if _, bare := kind.SelectExpr.GetOperand().GetExprKind().(*expr.Expr_IdentExpr); !bare {
			collectRefsFromExpr(kind.SelectExpr.GetOperand(), prev, refs)
		}
		// Try to find the base step ident name.
		base := kind.SelectExpr.GetOperand()
		for base != nil {
			switch b := base.GetExprKind().(type) {
			case *expr.Expr_IdentExpr:
				name := b.IdentExpr.GetName()
				if prev != nil && prev.StepValues != nil {
					if _, ok := prev.StepValues[name]; ok {
						if field := kind.SelectExpr.GetField(); field != "" {
							markStepField(refs, name, field)
						} else {
							markWholeStep(refs, name)
						}
					}
				}
				base = nil
			case *expr.Expr_SelectExpr:
				base = b.SelectExpr.GetOperand()
			case *expr.Expr_CallExpr:
				// could be obj.method() => method select on call target
				base = b.CallExpr.GetTarget()
			default:
				base = nil
			}
		}
	case *expr.Expr_CallExpr:
		if kind.CallExpr.GetTarget() != nil {
			collectRefsFromExpr(kind.CallExpr.GetTarget(), prev, refs)
		}
		for _, a := range kind.CallExpr.GetArgs() {
			collectRefsFromExpr(a, prev, refs)
		}
	case *expr.Expr_ListExpr:
		for _, e := range kind.ListExpr.GetElements() {
			collectRefsFromExpr(e, prev, refs)
		}
	case *expr.Expr_StructExpr:
		for _, e := range kind.StructExpr.GetEntries() {
			collectRefsFromExpr(e.GetValue(), prev, refs)
		}
	case *expr.Expr_ComprehensionExpr:
		c := kind.ComprehensionExpr
		collectRefsFromExpr(c.GetIterRange(), prev, refs)
		collectRefsFromExpr(c.GetAccuInit(), prev, refs)
		collectRefsFromExpr(c.GetLoopCondition(), prev, refs)
		collectRefsFromExpr(c.GetLoopStep(), prev, refs)
		collectRefsFromExpr(c.GetResult(), prev, refs)
	default:
		// literals and unknown kinds carry no references
	}
}

// collectRefsFromParsedExpr extracts references from a ParsedExpr.
func collectRefsFromParsedExpr(pe *expr.ParsedExpr, prev *v1.Workflow_StepOutputs, refs map[string]map[string]struct{}) {
	if pe == nil {
		return
	}
	collectRefsFromExpr(pe.GetExpr(), prev, refs)
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

	for _, v := range task.Inputs {
		if kind, isExpr := v.GetKind().(*v1.Value_Expr); isExpr {
			collectRefsFromParsedExpr(kind.Expr, prev, refs)
		}
	}

	if len(refs) == 0 {
		return &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{}}
	}

	trimmed := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{}}
	for stepID, fields := range refs {
		full, ok := prev.StepValues[stepID]
		if !ok || full == nil || full.NamedValues == nil {
			continue
		}
		if needed := neededOutputs(full, fields); needed != nil {
			trimmed.StepValues[stepID] = needed
		}
	}
	return trimmed
}

// collectNodeRefs records every step output a node could still need.
//
// Every expression site in the node counts, not only a task's inputs: a step's
// condition, a step's own `vars:`, a loop's item list and everything in its body,
// every branch of a parallel block, and a wait's own expression. Dropping an
// output one of those needs is a correctness failure — the resumed run fails on an
// unresolved reference — while keeping one it turns out not to need only costs
// payload. So when in doubt this keeps more.
//
// `vars:` was missing from that list and from the switch below, and it is the site
// the language most encourages: `examples/http-json` names a parsed body in a
// step's `vars:` precisely so the parse is written once, and a run of that shape
// that continued as new resumed without the output and failed on a reference to a
// step that had already succeeded. Nothing running the examples could catch it,
// because only the durable driver continues as new.
func collectNodeRefs(node *v1.Node, prev *v1.Workflow_StepOutputs, refs map[string]map[string]struct{}) {
	if node == nil {
		return
	}

	// A condition decides whether the step runs at all, so it is evaluated before
	// anything else and its references are needed just as much.
	collectValueRefs(node.GetCondition(), prev, refs)

	// Outside the switch, with the condition, because both are step *properties*
	// rather than parts of one kind of work: a `for_each` and a `wait` carry
	// `vars:` exactly as a task step does. Putting it in the Task arm would have
	// fixed the shape that was reported and left the other two.
	for _, value := range node.GetVars() {
		collectValueRefs(value, prev, refs)
	}

	switch kind := node.GetKind().(type) {
	case *v1.Node_Task:
		task := kind.Task
		for _, value := range task.GetInputs() {
			collectValueRefs(value, prev, refs)
		}

	case *v1.Node_ForEach:
		collectValueRefs(kind.ForEach.GetItems(), prev, refs)
		for _, inner := range kind.ForEach.GetBody() {
			collectNodeRefs(inner, prev, refs)
		}

	case *v1.Node_Parallel:
		for _, branch := range kind.Parallel.GetBranches() {
			for _, inner := range branch.GetSteps() {
				collectNodeRefs(inner, prev, refs)
			}
		}

	case *v1.Node_Wait:
		// A `wait_until` expression can name a step's output — "wait until the
		// deadline the previous step computed" — and a run that suspended before
		// the wait needs that output to still be there when it resumes.
		collectValueRefs(kind.Wait.GetUntil(), prev, refs)
	}
}

// collectValueRefs records the step outputs one value references.
//
// Only an expression can reference anything. It used to take the task and input name
// too, because one task carried its expression as a literal string and had to be
// parsed here to be seen at all; that task retired at edition v2026.2, and a literal
// is now a literal everywhere.
func collectValueRefs(value *v1.Value, prev *v1.Workflow_StepOutputs, refs map[string]map[string]struct{}) {
	if kind, isExpr := value.GetKind().(*v1.Value_Expr); isExpr {
		collectRefsFromParsedExpr(kind.Expr, prev, refs)
	}
}

// compactOutputsForRemainingSteps examines the remaining steps and returns a
// minimal subset of step outputs required to evaluate their inputs.
func compactOutputsForRemainingSteps(steps []*v1.Node, from int, prev *v1.Workflow_StepOutputs) *v1.Workflow_StepOutputs {
	if prev == nil || prev.StepValues == nil || from >= len(steps) {
		return prev
	}
	refs := map[string]map[string]struct{}{}
	for i := from; i < len(steps); i++ {
		collectNodeRefs(steps[i], prev, refs)
	}

	if len(refs) == 0 {
		return &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{}}
	}
	trimmed := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{}}
	for stepID, fields := range refs {
		full, ok := prev.StepValues[stepID]
		if !ok || full == nil || full.NamedValues == nil {
			continue
		}
		if needed := neededOutputs(full, fields); needed != nil {
			trimmed.StepValues[stepID] = needed
		}
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
func varsScope(profile string, outputs *v1.Workflow_StepOutputs, vars map[string]*v1.Value) *v1.Scope {
	scope := v1.NewScope(profile, outputs)
	scope.AmbientVars = vars

	return scope
}
