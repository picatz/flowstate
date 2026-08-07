package flowtest

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/cel-go/interpreter"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// compiledStub is a [Stub] with its `where:` parsed once, at load time —
// compiling a bad expression is a property of the test file and belongs to
// [compileStubs]'s error, not to whichever invocation happens to reach it
// first.
type compiledStub struct {
	where   *expr.ParsedExpr // nil matches unconditionally
	returns map[string]any
	fails   *StubFailure
}

// stubbedTask is every stub declared for one task name, tried in the order
// they were written — the shape a `switch` already has, and named that way in
// [Stub.Where]'s doc.
type stubbedTask struct {
	matchers []compiledStub
}

// compileStubs parses every stub's `where:` and groups them by task name.
func compileStubs(stubs []Stub) (map[string]*stubbedTask, error) {
	byTask := make(map[string]*stubbedTask)

	for i, s := range stubs {
		var parsed *expr.ParsedExpr
		if s.Where != "" {
			value := v1.NewExpr(s.Where)
			if errKind := value.GetError(); errKind != nil {
				return nil, fmt.Errorf("stub %d for task %q: where: %s", i+1, s.Task, errKind.GetMessage())
			}
			parsed = value.GetExpr()
		}

		task, ok := byTask[s.Task]
		if !ok {
			task = &stubbedTask{}
			byTask[s.Task] = task
		}
		task.matchers = append(task.matchers, compiledStub{where: parsed, returns: s.Returns, fails: s.Fails})
	}

	return byTask, nil
}

// fn builds the [v1.TaskFunc] this task's stubs answer through, in place of
// whatever the task would otherwise have done.
func (s *stubbedTask) fn(name string) v1.TaskFunc {
	return func(ctx context.Context, inputs map[string]*v1.Value, scope *v1.Scope) (*v1.Node_Outputs, error) {
		// Resolved once per invocation, before any matcher runs, so a
		// reference with no `secrets:` entry is refused regardless of
		// whether `where:` happens to mention the input carrying it — see
		// [resolveSecretInputs]'s own doc for why this cannot be left to
		// whichever matcher first reads the input.
		resolvedSecrets, err := resolveSecretInputs(ctx, inputs)
		if err != nil {
			return nil, v1.NewTaskError(name, v1.ErrorKindInvalidInput, err)
		}

		for _, m := range s.matchers {
			ok, err := m.matches(ctx, scope.GetProfile(), inputs, resolvedSecrets)
			if err != nil {
				return nil, v1.NewTaskError(name, v1.ErrorKindExpression, err)
			}
			if !ok {
				continue
			}

			if m.fails != nil {
				kind := v1.ErrorKind(m.fails.Kind)
				if kind == "" {
					kind = v1.ErrorKindUpstream
				}
				return nil, v1.NewTaskError(name, kind, errors.New(m.fails.Message))
			}

			return &v1.Node_Outputs{NamedValues: v1.NewNamedValues(m.returns)}, nil
		}

		return nil, v1.NewTaskError(name, v1.ErrorKindInvalidInput, fmt.Errorf(
			"flow test: task %q was invoked with no matching stub (%d stub(s) declared for it); "+
				"add a stub with no `where:` to answer every invocation, or one whose `where:` matches these inputs",
			name, len(s.matchers)))
	}
}

// matches reports whether a stub's `where:` holds against a task invocation's
// resolved inputs. An empty where always matches.
//
// resolvedSecrets carries the plaintext [resolveSecretInputs] already
// resolved for any input naming a `${secret(...)}` reference, keyed by input
// name — the one shape a where: clause needs to assert against a secret's
// value, since [literalToGo] has nothing to convert for a Value whose kind is
// [v1.Value_SecretRef] rather than a literal.
func (c compiledStub) matches(ctx context.Context, profile string, inputs map[string]*v1.Value, resolvedSecrets map[string]string) (bool, error) {
	if c.where == nil {
		return true, nil
	}

	native := make(map[string]any, len(inputs))
	for name, v := range inputs {
		if lit := v.GetLiteral(); lit != nil {
			value, err := literalToGo(lit)
			if err != nil {
				return false, fmt.Errorf("input %q: %w", name, err)
			}
			native[name] = value
			continue
		}
		if value, ok := resolvedSecrets[name]; ok {
			native[name] = value
			continue
		}
		// An input the task evaluates itself ([v1.TaskDef.DeferredInputs]) is
		// still an expression at this point and has nothing a `where:` clause
		// can compare against; it is simply absent from `inputs` rather than
		// resolved to something misleading.
	}

	activation, err := interpreter.NewActivation(map[string]any{"inputs": native})
	if err != nil {
		return false, fmt.Errorf("building activation: %w", err)
	}

	out, err := v1.DefaultEvaluator().EvalParsedBase(ctx, profile, c.where, activation)
	if err != nil {
		return false, fmt.Errorf("evaluating where: %w", err)
	}

	matched, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("where must evaluate to a boolean, got %s", out.Type().TypeName())
	}
	return matched, nil
}
