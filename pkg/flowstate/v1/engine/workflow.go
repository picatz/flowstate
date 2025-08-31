package engine

import (
	"fmt"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
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

const RunTaskQueueName = "flowstate-run-task-queue"

// defaultMaxStepsPerRun defines how many steps to execute before
// continuing-as-new when no label overrides are provided.
const defaultMaxStepsPerRun = 200

// Continue-As-New policy: use server suggestions or a server-injected step budget.
// The DSL no longer exposes labels to control Continue-As-New.

// Run is the durable workflow entrypoint that supports Continue-As-New.
// It executes from the provided state and yields final step outputs when done.
func Run(ctx workflow.Context, st *v1.RunState) (*v1.Workflow_StepOutputs, error) {
	if st == nil || st.Workflow == nil || len(st.Workflow.Steps) == 0 {
		return nil, fmt.Errorf("workflow cannot be nil or empty")
	}

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Second,
			BackoffCoefficient:     2.0,
			MaximumInterval:        100 * time.Second,
			MaximumAttempts:        5,
			NonRetryableErrorTypes: []string{"ErrRunFailed"},
		},
	})

	logger := workflow.GetLogger(ctx)

	// Initialize step outputs with carried-over minimal subset if present.
	stepOutputs := st.Outputs
	if stepOutputs == nil {
		stepOutputs = &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{}}
	} else if stepOutputs.StepValues == nil {
		stepOutputs.StepValues = map[string]*v1.Node_Outputs{}
	}

	stepsProcessed := 0

	// Determine step budget from state (injected by server) or default.
	stepsBudget := int(st.StepsBudget)
	if stepsBudget <= 0 {
		// Use default when not injected.
		stepsBudget = defaultMaxStepsPerRun
	}

	for i := int(st.NextStep); i < len(st.Workflow.Steps); i++ {
		node := st.Workflow.Steps[i]
		logger.Info("processing step", "id", node.Id, "kind", node.Kind)

		switch node.Kind.(type) {
		case *v1.Node_Task:
			var evalOutput v1.Node_Outputs
			t := node.GetTask()

			// Resolve any CEL expressions in task inputs before scheduling.
			if err := resolveTaskInputs(t, stepOutputs); err != nil {
				return nil, &ErrRunFailed{Message: err.Error()}
			}

			var evalErr error
			if t.GetName() == "cel" {
				compactPrev := compactPrevOutputsForTask(t, stepOutputs)
				evalErr = workflow.ExecuteActivity(ctx, TaskWithPrev, t, compactPrev).Get(ctx, &evalOutput)
			} else {
				evalErr = workflow.ExecuteActivity(ctx, Task, t).Get(ctx, &evalOutput)
			}
			if evalErr != nil {
				return nil, &ErrRunFailed{Message: evalErr.Error()}
			}

			logger.Info("task evaluated successfully", "output", &evalOutput)
			stepOutputs.StepValues[node.Id] = &evalOutput
			stepsProcessed++

			// Continue-As-New if suggested by server or we hit the step budget;
			// and if more steps remain.
			shouldCAN := stepsProcessed >= stepsBudget
			if info := workflow.GetInfo(ctx); info != nil {
				if info.GetContinueAsNewSuggested() {
					shouldCAN = true
				}
			}
			if shouldCAN && i < len(st.Workflow.Steps)-1 {
				carry := compactOutputsForRemainingSteps(st.Workflow.Steps, i+1, stepOutputs)
				next := &v1.RunState{
					Workflow:    st.Workflow,
					NextStep:    int32(i + 1),
					Outputs:     carry,
					StepsBudget: int32(stepsBudget),
				}
				return nil, workflow.NewContinueAsNewError(ctx, Run, next)
			}
		default:
			return nil, fmt.Errorf("unsupported node kind: %T", node.Kind)
		}
	}

	return stepOutputs, nil
}

// resolveTaskInputs evaluates any CEL expressions in the task inputs using the
// current step outputs and replaces them with literal values. This minimizes
// activity inputs and avoids repeatedly passing previous outputs in payloads.
func resolveTaskInputs(task *v1.Task, prev *v1.Workflow_StepOutputs) error {
	if task == nil || len(task.Inputs) == 0 {
		return nil
	}

	for k, val := range task.Inputs {
		switch kind := val.GetKind().(type) {
		case *v1.Value_Expr:
			ast := cel.ParsedExprToAst(kind.Expr)
			env, err := cel.NewEnv()
			if err != nil {
				return fmt.Errorf("failed to create CEL env for %q: %w", k, err)
			}
			prg, err := env.Program(ast)
			if err != nil {
				return fmt.Errorf("failed to compile CEL for %q: %w", k, err)
			}
			out, _, err := prg.Eval(cel.Activation(&v1.StepsOutputActivation{Prev: prev}))
			if err != nil {
				return fmt.Errorf("failed to evaluate CEL for %q: %w", k, err)
			}
			exprVal, err := cel.RefValueToValue(out)
			if err != nil {
				return fmt.Errorf("failed to convert CEL value for %q: %w", k, err)
			}
			task.Inputs[k] = &v1.Value{Kind: &v1.Value_Literal{Literal: exprVal}}
		case *v1.Value_Literal:
			// Already a literal; nothing to do.
			_ = types.DefaultTypeAdapter // avoid unused import if build tags change
		default:
			// Unknown kind; leave as-is so task library can handle or error.
		}
	}
	return nil
}

// maxStepsPerRun returns the configured step budget for Continue-As-New by
// reading an optional label on the workflow. Using labels keeps configuration
// deterministic across replays (unlike env vars which may change between runs).
//
// Label: "flowstate/max-steps-per-run" (string int > 0)
// Fallback: defaultMaxStepsPerRun

// maxHistoryLength returns an optional max history length (number of events)
// from workflow labels. When set (>0), the workflow will Continue-As-New when
// the current history length exceeds this value.
// Label: "flowstate/max-history-length"

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
