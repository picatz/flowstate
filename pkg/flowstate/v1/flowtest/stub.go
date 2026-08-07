package flowtest

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// compiledStub is a [Stub] with its `where:` and its `returns:` parsed once, at
// load time — compiling a bad expression is a property of the test file and
// belongs to [compileStubs]'s error, not to whichever invocation happens to
// reach it first.
type compiledStub struct {
	where *expr.ParsedExpr // nil matches unconditionally

	// returns is the declared map with every ${...} value replaced by a
	// [stubExpr] node, at any depth — see [compileReturnValue]. A stub with no
	// `returns:` at all keeps a nil map, which is what distinguishes it from
	// `returns: {}`.
	returns    map[string]any
	hasReturns bool

	fails *StubFailure
}

// stubExpr is one ${...} expression inside a stub's `returns:`, parsed at load
// time and evaluated once per invocation against that invocation's activation —
// which is what makes a single stub able to answer a loop's iterations
// differently. It is a distinct node type rather than a string so that
// [resolveReturnValue] cannot confuse an expression with text that merely looks
// like one.
type stubExpr struct {
	parsed *expr.ParsedExpr
	source string
}

// stubbedTask is every stub declared for one task name, tried in the order
// they were written — the shape a `switch` already has, and named that way in
// [Stub.Where]'s doc.
type stubbedTask struct {
	matchers []compiledStub
}

// compileStubs parses every stub's `where:` and `returns:` and groups them by
// task name.
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

		returns, err := compileReturns(s.Returns)
		if err != nil {
			return nil, fmt.Errorf("stub %d for task %q: returns: %w", i+1, s.Task, err)
		}

		task, ok := byTask[s.Task]
		if !ok {
			task = &stubbedTask{}
			byTask[s.Task] = task
		}
		task.matchers = append(task.matchers, compiledStub{
			where:      parsed,
			returns:    returns,
			hasReturns: s.Returns != nil,
			fails:      s.Fails,
		})
	}

	return byTask, nil
}

// compileReturns parses every ${...} a stub's `returns:` holds.
func compileReturns(returns map[string]any) (map[string]any, error) {
	if returns == nil {
		return nil, nil
	}

	compiled := make(map[string]any, len(returns))
	for name, v := range returns {
		value, err := compileReturnValue(v)
		if err != nil {
			return nil, fmt.Errorf("%q: %w", name, err)
		}
		compiled[name] = value
	}

	return compiled, nil
}

// compileReturnValue parses one value of a stub's `returns:`, recursing into
// maps and lists.
//
// The fence rule is the Flowfile's own, taken from [flowfile.SplitFence] rather
// than restated here: a whole-value ${...} is an expression wherever it is
// written, including nested inside a structure, and text that mixes literal
// characters with a fence is a mistake rather than a literal string. A test file
// is a document this repo's tooling authors alongside a Flowfile, so a value in
// one has to mean what the same value means in the other.
func compileReturnValue(v any) (any, error) {
	switch value := v.(type) {
	case string:
		if err := flowfile.ExprError(value); err != nil {
			return nil, err
		}
		inner, fenced := flowfile.SplitFence(value)
		if !fenced {
			return value, nil
		}
		parsed := v1.NewExpr(inner)
		if errKind := parsed.GetError(); errKind != nil {
			return nil, fmt.Errorf("invalid expression %q: %s", inner, errKind.GetMessage())
		}
		return &stubExpr{parsed: parsed.GetExpr(), source: inner}, nil
	case []any:
		list := make([]any, 0, len(value))
		for i, element := range value {
			compiled, err := compileReturnValue(element)
			if err != nil {
				return nil, fmt.Errorf("element %d: %w", i, err)
			}
			list = append(list, compiled)
		}
		return list, nil
	}

	// Reflected rather than switched on map[string]any alone, because what a
	// YAML decoder hands back for a nested mapping is its own choice: missing a
	// map here would leave a ${...} inside it as literal text, which is the
	// silent-nothing failure CLAUDE.md's "diagnostics are a feature" forbids.
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Map && rv.Type().Key().Kind() == reflect.String {
		object := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			name := iter.Key().String()
			compiled, err := compileReturnValue(iter.Value().Interface())
			if err != nil {
				return nil, fmt.Errorf("%q: %w", name, err)
			}
			object[name] = compiled
		}
		return object, nil
	}
	if rv.Kind() == reflect.Slice {
		list := make([]any, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			compiled, err := compileReturnValue(rv.Index(i).Interface())
			if err != nil {
				return nil, fmt.Errorf("element %d: %w", i, err)
			}
			list = append(list, compiled)
		}
		return list, nil
	}

	return v, nil
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

		activation, err := stubActivation(ctx, scope, inputs, resolvedSecrets)
		if err != nil {
			return nil, v1.NewTaskError(name, v1.ErrorKindExpression, err)
		}

		for _, m := range s.matchers {
			ok, err := m.matches(ctx, scope.GetProfile(), activation)
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

			returns, err := m.answer(ctx, scope.GetProfile(), activation)
			if err != nil {
				return nil, v1.NewTaskError(name, v1.ErrorKindExpression, err)
			}

			return &v1.Node_Outputs{NamedValues: v1.NewNamedValues(returns)}, nil
		}

		return nil, v1.NewTaskError(name, v1.ErrorKindInvalidInput, fmt.Errorf(
			"flow test: task %q was invoked with no matching stub (%d stub(s) declared for it); "+
				"add a stub with no `where:` to answer every invocation, or one whose `where:` matches these inputs",
			name, len(s.matchers)))
	}
}

// stubActivation builds what one invocation's `where:` and `returns:` are
// evaluated against: the scope the step itself was evaluated in, plus `inputs`
// — the task's own resolved inputs.
//
// The scope is what carries the iteration. A loop binds its `as:` name in the
// scope the body's steps run in (see [v1.Scope.WithLocal]), and that scope
// travels all the way to the task's own [v1.TaskFunc]; before this it simply
// went unread here, which is why a stub could not tell one iteration from
// another and a case over a loop had to assert what the stub distorted rather
// than what the workflow computes (#269).
//
// `inputs` is bound as a *local*, so it wins over the run's own `inputs.<name>`
// namespace for the length of a `where:` clause. That shadowing is deliberate
// and is the older meaning kept: a stub's `where:` has named the task's inputs
// since stubs existed, and the alternative — silently changing what
// `inputs.url` means in every test file in the corpus — is worse than one
// documented collision. See [Stub.Where].
func stubActivation(ctx context.Context, scope *v1.Scope, inputs map[string]*v1.Value, resolvedSecrets map[string]string) (cel.Activation, error) {
	native := make(map[string]any, len(inputs))
	for name, v := range inputs {
		if lit := v.GetLiteral(); lit != nil {
			value, err := literalToGo(lit)
			if err != nil {
				return nil, fmt.Errorf("input %q: %w", name, err)
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
		// resolved to something misleading. What the expression is *written in
		// terms of* — the loop's binding, the step's vars — is reachable
		// through the scope below, which is what makes an iteration
		// distinguishable even where its inputs are not.
	}

	return scope.ActivationWith(ctx, map[string]ref.Val{
		v1.InputsRoot: types.NewStringInterfaceMap(v1.TypeAdapter, native),
	}), nil
}

// matches reports whether a stub's `where:` holds for one invocation. An empty
// where always matches.
func (c compiledStub) matches(ctx context.Context, profile string, activation cel.Activation) (bool, error) {
	if c.where == nil {
		return true, nil
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

// answer resolves the stub's `returns:` for one invocation, evaluating every
// ${...} it holds against that invocation's activation.
func (c compiledStub) answer(ctx context.Context, profile string, activation cel.Activation) (map[string]any, error) {
	if !c.hasReturns {
		return nil, nil
	}

	resolved := make(map[string]any, len(c.returns))
	for name, v := range c.returns {
		value, err := resolveReturnValue(ctx, profile, activation, v)
		if err != nil {
			return nil, fmt.Errorf("returns %q: %w", name, err)
		}
		resolved[name] = value
	}

	return resolved, nil
}

// resolveReturnValue replaces every [stubExpr] in a compiled `returns:` value
// with what it evaluates to, recursing the way [compileReturnValue] did.
//
// An expression becomes a [v1.Value] holding a literal rather than a Go native,
// which [v1.NewValue] passes through unchanged at any depth — so a resolved
// expression nested inside a map or a list needs no second conversion that
// could disagree with the first.
func resolveReturnValue(ctx context.Context, profile string, activation cel.Activation, v any) (any, error) {
	switch value := v.(type) {
	case *stubExpr:
		out, err := v1.DefaultEvaluator().EvalParsedBase(ctx, profile, value.parsed, activation)
		if err != nil {
			return nil, fmt.Errorf("evaluating %q: %w", value.source, err)
		}
		literal, err := cel.RefValueToValue(out)
		if err != nil {
			return nil, fmt.Errorf("evaluating %q: converting result: %w", value.source, err)
		}
		return &v1.Value{Kind: &v1.Value_Literal{Literal: literal}}, nil
	case map[string]any:
		object := make(map[string]any, len(value))
		for name, element := range value {
			resolved, err := resolveReturnValue(ctx, profile, activation, element)
			if err != nil {
				return nil, fmt.Errorf("%q: %w", name, err)
			}
			object[name] = resolved
		}
		return object, nil
	case []any:
		list := make([]any, 0, len(value))
		for i, element := range value {
			resolved, err := resolveReturnValue(ctx, profile, activation, element)
			if err != nil {
				return nil, fmt.Errorf("element %d: %w", i, err)
			}
			list = append(list, resolved)
		}
		return list, nil
	default:
		return v, nil
	}
}
