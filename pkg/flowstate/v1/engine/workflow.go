package engine

import (
	"errors"
	"fmt"

	"github.com/google/cel-go/cel"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"go.temporal.io/sdk/workflow"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

type ErrRunFailed struct {
	Message string
}

func (e *ErrRunFailed) Error() string {
	return fmt.Sprintf("engine: flowstate run failed: %s", e.Message)
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

	// Execute through the recursive executor, which handles nested control flow
	// and records where to resume if the run has to be continued as new.
	exec := &executor{
		ctx:    ctx,
		spec:   st.Workflow,
		scope:  v1.NewScope(stepOutputs),
		budget: stepsBudget,
		resume: resumeFrames(st),
	}

	err := exec.runNodes(st.Workflow.GetSteps(), 0)
	switch {
	case err == nil:
		return stepOutputs, nil

	case errors.Is(err, errContinueAsNew):
		carry := compactOutputsForFrames(st.Workflow, exec.frames, stepOutputs)
		next := &v1.RunState{
			Workflow:    st.Workflow,
			Outputs:     carry,
			StepsBudget: int32(stepsBudget),
			Frames:      exec.frames,
			// Identity must survive Continue-As-New. A long workload spans
			// several runs, and a step in the last one acts on behalf of the
			// same caller as a step in the first; dropping it would silently
			// turn an authenticated workload into an anonymous one partway
			// through.
			Identity: st.Identity,
		}
		logger.Info("continuing as new", "frames", len(exec.frames))
		return nil, workflow.NewContinueAsNewError(ctx, Run, next)

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
// `${step.error}`, and its absence means the step succeeded.
func failedStepOutputs(err error) *v1.Node_Outputs {
	return &v1.Node_Outputs{
		NamedValues: map[string]*v1.Value{
			"error": v1.NewLiteral(err.Error()),
		},
	}
}

// collectRefsFromExpr recursively walks a CEL expression and returns a map of
// step IDs to the set of fields referenced on that step. If a step is
// referenced without a field (e.g., just `a`), the field set will be empty to
// indicate that the whole step's outputs are required.
func collectRefsFromExpr(e *expr.Expr, prev *v1.Workflow_StepOutputs, refs map[string]map[string]struct{}) {
	if e == nil {
		return
	}
	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_IdentExpr:
		name := kind.IdentExpr.GetName()
		if prev != nil && prev.StepValues != nil {
			if _, ok := prev.StepValues[name]; ok {
				if _, exists := refs[name]; !exists {
					refs[name] = make(map[string]struct{})
				}
			}
		}
	case *expr.Expr_SelectExpr:
		// Handle a.b[.c...] by walking the operand chain down to the root ident.
		// We record the first field selected after the step ident.
		// For nested selects, this still captures the top-level output field.
		// E.g., a.result.subfield -> keep `result` from step `a`.
		// Traverse to find the base ident name.
		// First, collect from operand recursively to ensure ident is seen.
		collectRefsFromExpr(kind.SelectExpr.GetOperand(), prev, refs)
		// Try to find the base step ident name.
		base := kind.SelectExpr.GetOperand()
		for base != nil {
			switch b := base.GetExprKind().(type) {
			case *expr.Expr_IdentExpr:
				name := b.IdentExpr.GetName()
				if prev != nil && prev.StepValues != nil {
					if _, ok := prev.StepValues[name]; ok {
						if _, exists := refs[name]; !exists {
							refs[name] = make(map[string]struct{})
						}
						field := kind.SelectExpr.GetField()
						if field != "" {
							refs[name][field] = struct{}{}
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

// parseCELString parses a CEL expression string to a ParsedExpr for static analysis.
func parseCELString(s string) (*expr.ParsedExpr, error) {
	env, err := cel.NewEnv()
	if err != nil {
		return nil, err
	}
	ast, issues := env.Parse(s)
	if issues != nil && issues.Err() != nil {
		return nil, issues.Err()
	}
	return cel.AstToParsedExpr(ast)
}

// compactPrevOutputsForTask returns a reduced view of prev outputs that includes
// only the step outputs and fields referenced by the given task's inputs. For
// non-CEL tasks where inputs are pre-resolved, this typically results in an
// empty set. For CEL tasks, this minimizes the payload we pass to the activity.
func compactPrevOutputsForTask(task *v1.Task, prev *v1.Workflow_StepOutputs) *v1.Workflow_StepOutputs {
	if prev == nil || task == nil || len(task.Inputs) == 0 {
		return prev
	}
	refs := map[string]map[string]struct{}{}

	// Walk all inputs.
	for k, v := range task.Inputs {
		switch kind := v.GetKind().(type) {
		case *v1.Value_Expr:
			collectRefsFromParsedExpr(kind.Expr, prev, refs)
		case *v1.Value_Literal:
			// Special-case CEL task main `expr` which is a literal string containing CEL.
			if task.GetName() == "cel" && k == "expr" {
				if s := kind.Literal.GetStringValue(); s != "" {
					if pe, err := parseCELString(s); err == nil {
						collectRefsFromParsedExpr(pe, prev, refs)
					}
				}
			}
			// Other literals don't carry references.
		default:
			// ignore
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
		// If no specific fields recorded, include whole step.
		if len(fields) == 0 {
			trimmed.StepValues[stepID] = full
			continue
		}
		// Otherwise include only referenced fields.
		nv := map[string]*v1.Value{}
		for f := range fields {
			if v, ok := full.NamedValues[f]; ok {
				nv[f] = v
			}
		}
		if len(nv) > 0 {
			trimmed.StepValues[stepID] = &v1.Node_Outputs{NamedValues: nv}
		}
	}
	return trimmed
}

// compactOutputsForRemainingSteps examines the remaining steps and returns a
// minimal subset of step outputs required to evaluate their inputs.
func compactOutputsForRemainingSteps(steps []*v1.Node, from int, prev *v1.Workflow_StepOutputs) *v1.Workflow_StepOutputs {
	if prev == nil || prev.StepValues == nil || from >= len(steps) {
		return prev
	}
	refs := map[string]map[string]struct{}{}
	for i := from; i < len(steps); i++ {
		n := steps[i]
		t := n.GetTask()
		if t == nil {
			continue
		}
		for k, v := range t.Inputs {
			switch kind := v.GetKind().(type) {
			case *v1.Value_Expr:
				collectRefsFromParsedExpr(kind.Expr, prev, refs)
			case *v1.Value_Literal:
				if t.GetName() == "cel" && k == "expr" {
					if s := kind.Literal.GetStringValue(); s != "" {
						if pe, err := parseCELString(s); err == nil {
							collectRefsFromParsedExpr(pe, prev, refs)
						}
					}
				}
			}
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
		if len(fields) == 0 {
			trimmed.StepValues[stepID] = full
			continue
		}
		nv := map[string]*v1.Value{}
		for f := range fields {
			if v, ok := full.NamedValues[f]; ok {
				nv[f] = v
			}
		}
		if len(nv) > 0 {
			trimmed.StepValues[stepID] = &v1.Node_Outputs{NamedValues: nv}
		}
	}
	return trimmed
}
