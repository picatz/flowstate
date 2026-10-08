package flowtest

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/nearest"
)

// stubReturnsMismatchAtBind holds a stub's `returns:` to the outputs the stubbed
// task declares, so a suite cannot be green against a fixture production can
// never produce (#1295). It answers in two strengths. invalid is a literal value
// the task's output schema refuses (an `http` `status_code` of 9999), which no
// run could produce and which refuses the stub. undeclared is a name the task
// does not declare, which only warns: the suite's own fixtures commonly return
// stand-in names, so `--fail-on-warning` is where a suite chooses to forbid
// them.
//
// It speaks only where it is certain. Every step the stub could answer must run
// a task whose output schema this build holds, and none of them may shape its
// outputs, because a shaped step's names belong to its author. A task-form stub
// in a workflow with a `call:` step is left alone, because the callee may run the
// task too. A task this harness knows only by name (a plugin's) is left alone,
// as is a value an expression will supply, since its value is only known when it
// answers. Both are empty when there is nothing to report.
func stubReturnsMismatchAtBind(m *compiledStub, spec *v1.Workflow, taskOfStep map[string]string, nodeOfStep map[string]*v1.Node) (undeclared, invalid string) {
	if !m.hasReturns || len(m.returns) == 0 {
		return "", ""
	}
	// A task-form stub also answers the task wherever a callee runs it, and
	// the callee's steps are not among the candidates below.
	if m.step == "" && callsAnother(spec) {
		return "", ""
	}

	var task string
	for _, step := range stubCandidateSteps(m, taskOfStep) {
		node, ok := nodeOfStep[step]
		if !ok || shapesOutputs(node) {
			return "", ""
		}
		if task != "" && task != taskOfStep[step] {
			return "", ""
		}
		task = taskOfStep[step]
	}
	if task == "" {
		return "", ""
	}

	def, ok := v1.DefaultRegistry().Lookup(task)
	if !ok || def.Outputs == nil {
		return "", ""
	}

	return undeclaredReturn(m.returns, task), literalReturnsMismatch(def, task, m.returns)
}

// undeclaredReturn names the first returned output the task does not declare.
func undeclaredReturn(returns map[string]any, task string) string {
	declared := rawOutputNames(task)
	for _, name := range slices.Sorted(maps.Keys(returns)) {
		if slices.Contains(declared, name) {
			continue
		}
		if len(declared) == 0 {
			return fmt.Sprintf("returns %q, but task %q declares no outputs", name, task)
		}
		if suggestion, ok := nearest.Name(name, declared); ok {
			return fmt.Sprintf("returns %q, which task %q does not declare as an output; did you mean %q?", name, task, suggestion)
		}

		return fmt.Sprintf("returns %q, which task %q does not declare as an output (it declares %s)",
			name, task, strings.Join(declared, ", "))
	}

	return ""
}

// literalReturnsMismatch checks the literal entries of returns against the
// task's output descriptor with the schema's own rules, reporting the first
// violation on a field that was returned. A value the schema cannot hold at all
// (a string for an integer) is reported as such.
func literalReturnsMismatch(def v1.TaskDef, task string, returns map[string]any) string {
	literals := map[string]any{}
	for name, value := range returns {
		if isLiteralReturn(value) {
			literals[name] = value
		}
	}
	if len(literals) == 0 {
		return ""
	}

	message := dynamicpb.NewMessage(def.Outputs)
	fields := def.Outputs.Fields()
	for _, name := range slices.Sorted(maps.Keys(literals)) {
		field := fields.ByName(protoreflect.Name(name))
		if field == nil {
			continue
		}
		if err := v1.SetLiteralField(message.ProtoReflect(), field, v1.NewValue(literals[name]).GetLiteral()); err != nil {
			return fmt.Sprintf("returns %q does not fit task %q's output: %v", name, task, err)
		}
	}

	var validation *v1.ValidationError
	if err := v1.Validate(message); errors.As(err, &validation) {
		for _, violation := range validation.Violations {
			if _, returned := literals[v1.ViolationRoot(violation.Field)]; returned {
				return fmt.Sprintf("returns %q, which task %q's output schema refuses: %s", violation.Field, task, violation.String())
			}
		}
	}

	return ""
}

// isLiteralReturn reports whether a compiled `returns:` value holds no
// expression anywhere, so it can be judged before the stub answers.
func isLiteralReturn(value any) bool {
	switch v := value.(type) {
	case *stubExpr:
		return false
	case []any:
		return !slices.ContainsFunc(v, func(e any) bool { return !isLiteralReturn(e) })
	case map[string]any:
		for _, e := range v {
			if !isLiteralReturn(e) {
				return false
			}
		}
	}

	return true
}

// stubCandidateSteps is every step the stub could answer: the one a step-form
// stub names, or every step running a task-form stub's task.
func stubCandidateSteps(m *compiledStub, taskOfStep map[string]string) []string {
	if m.step != "" {
		return []string{m.step}
	}

	var candidates []string
	for step, task := range taskOfStep {
		if task == m.task {
			candidates = append(candidates, step)
		}
	}
	slices.Sort(candidates)

	return candidates
}

// callsAnother reports whether the workflow has a `call:` step, whose callee
// can run tasks this workflow's own steps do not show.
func callsAnother(spec *v1.Workflow) bool {
	found := false
	walkOwnNodes(spec.GetSteps(), func(node *v1.Node) {
		found = found || node.GetCall() != nil
	})

	return found
}

// shapesOutputs reports whether a step carries any `outputs:` shaping at all,
// including one written as a single expression whose keys are not known before
// it runs, which [shapedOutputNames] cannot name.
func shapesOutputs(node *v1.Node) bool {
	task := node.GetTask()
	def, ok := v1.DefaultRegistry().Lookup(task.GetName())

	return ok && slices.Contains(def.DeferredInputs, "outputs") && task.GetInputs()["outputs"] != nil
}
