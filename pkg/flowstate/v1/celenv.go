package flowstatev1

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/ext"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// Flowstate evaluates every CEL expression through this file. Expressions come
// from workflow definitions, which are untrusted input: a workflow author can
// write an expression that allocates gigabytes or runs for minutes, and without
// bounds that expression is a denial-of-service against the worker executing it.
//
// Two properties make evaluation safe, and both require going through
// [Evaluator] rather than constructing environments ad hoc:
//
//   - Cost limiting. The CEL runtime tracks the cost of an evaluation and
//     aborts it once the budget is exhausted, which bounds both time and
//     allocation.
//   - Cancellation. Evaluation periodically checks whether its context is done,
//     so a caller's deadline actually stops the work rather than being noticed
//     only after it finishes.
//
// Environments are cached because constructing one parses and type-checks every
// declaration in it, which is far too expensive to repeat per evaluation.

// DefaultCostLimit is the CEL cost budget applied to a single evaluation when
// no other limit is configured.
//
// CEL costs are abstract units roughly proportional to the work performed, not
// wall-clock time or bytes. This budget is generous enough for the data shaping
// real workflows do and small enough that a pathological expression fails fast
// instead of exhausting the worker.
const DefaultCostLimit uint64 = 1_000_000

// DefaultInterruptCheckFrequency is how many evaluation steps elapse between
// context cancellation checks.
//
// Lower values cancel more promptly at the cost of checking more often. This
// must be non-zero for context cancellation to take effect at all.
const DefaultInterruptCheckFrequency uint = 256

// Limits bound a single CEL evaluation.
//
// The zero value is not useful; call [DefaultLimits] and adjust from there, so
// that a new field added here cannot silently become unbounded.
type Limits struct {
	// Cost is the CEL cost budget for one evaluation. Zero means unlimited,
	// which should be used only in tests.
	Cost uint64

	// InterruptCheckFrequency is how many evaluation steps elapse between
	// context cancellation checks. Zero disables cancellation checking.
	InterruptCheckFrequency uint
}

// DefaultLimits returns the evaluation limits applied when a caller does not
// specify their own.
func DefaultLimits() Limits {
	return Limits{
		Cost:                    DefaultCostLimit,
		InterruptCheckFrequency: DefaultInterruptCheckFrequency,
	}
}

// programOptions returns the CEL program options that enforce l.
func (l Limits) programOptions() []cel.ProgramOption {
	var opts []cel.ProgramOption
	if l.Cost > 0 {
		// CostLimit implies OptTrackCost.
		opts = append(opts, cel.CostLimit(l.Cost))
	}
	if l.InterruptCheckFrequency > 0 {
		opts = append(opts, cel.InterruptCheckFrequency(l.InterruptCheckFrequency))
	}
	return opts
}

// An Evaluator compiles and evaluates CEL expressions under a fixed set of
// limits, caching the environments it builds.
//
// An Evaluator is safe for concurrent use by multiple goroutines, and is meant
// to be long-lived: the cache is what makes repeated evaluation affordable.
type Evaluator struct {
	limits Limits

	// envs caches environments by extension-library set. Keys are the
	// canonical library-set string produced by libsKey.
	envs sync.Map // map[string]*envResult
}

// envResult is a memoized environment construction, successful or not. Failures
// are cached too, so a workflow that repeatedly requests a broken library set
// does not repeatedly pay for the failure.
type envResult struct {
	env *cel.Env
	err error
}

// EvaluatorOption configures an [Evaluator].
type EvaluatorOption func(*Evaluator)

// WithLimits sets the evaluation limits an [Evaluator] enforces.
func WithLimits(l Limits) EvaluatorOption {
	return func(e *Evaluator) { e.limits = l }
}

// NewEvaluator returns an [Evaluator] enforcing [DefaultLimits], adjusted by
// any supplied options.
func NewEvaluator(opts ...EvaluatorOption) *Evaluator {
	e := &Evaluator{limits: DefaultLimits()}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// defaultEvaluator returns the process-wide [Evaluator] used by callers that do
// not supply one. It is constructed once, on first use.
var defaultEvaluator = sync.OnceValue(func() *Evaluator {
	return NewEvaluator()
})

// DefaultEvaluator returns the shared [Evaluator] enforcing [DefaultLimits].
//
// Task implementations and the workflow engine use this unless they need
// different limits, so that every expression in the system is bounded the same
// way and environments are cached once for the whole process.
func DefaultEvaluator() *Evaluator {
	return defaultEvaluator()
}

// Limits returns the limits e enforces.
func (e *Evaluator) Limits() Limits {
	return e.limits
}

// Env returns a CEL environment enabling the named extension libraries, which
// may be empty for the base environment.
//
// Library names are matched case-insensitively and order does not matter; the
// returned environment is cached and shared, so callers must not mutate it.
// Requesting an unknown library is an error, reported with the set of libraries
// that do exist.
func (e *Evaluator) Env(libs ...string) (*cel.Env, error) {
	key := libsKey(libs)
	if cached, ok := e.envs.Load(key); ok {
		res := cached.(*envResult)
		return res.env, res.err
	}

	env, err := buildEnv(libs)
	res := &envResult{env: env, err: err}
	actual, _ := e.envs.LoadOrStore(key, res)
	stored := actual.(*envResult)
	return stored.env, stored.err
}

// Eval evaluates ast in env against the given activation, enforcing e's limits
// and honoring ctx's cancellation.
//
// The activation may be a map[string]any or a cel.Activation, matching the CEL
// runtime's own contract.
func (e *Evaluator) Eval(ctx context.Context, env *cel.Env, ast *cel.Ast, activation any) (ref.Val, error) {
	prg, err := env.Program(ast, e.limits.programOptions()...)
	if err != nil {
		return nil, fmt.Errorf("compile expression: %w", err)
	}
	out, _, err := prg.ContextEval(ctx, activation)
	if err != nil {
		return nil, fmt.Errorf("evaluate expression: %w", err)
	}
	return out, nil
}

// EvalParsed evaluates a previously parsed expression, of the form carried in a
// compiled workflow specification, against the given activation.
func (e *Evaluator) EvalParsed(ctx context.Context, env *cel.Env, parsed *expr.ParsedExpr, activation any) (ref.Val, error) {
	if parsed == nil {
		return nil, fmt.Errorf("parsed expression is nil")
	}
	return e.Eval(ctx, env, cel.ParsedExprToAst(parsed), activation)
}

// EvalParsedBase evaluates a previously parsed expression in the base
// environment, which is the common case for resolving step inputs.
func (e *Evaluator) EvalParsedBase(ctx context.Context, parsed *expr.ParsedExpr, activation any) (ref.Val, error) {
	env, err := e.Env()
	if err != nil {
		return nil, err
	}
	return e.EvalParsed(ctx, env, parsed, activation)
}

// EvalString parses and evaluates an expression string with the named
// extension libraries enabled.
func (e *Evaluator) EvalString(ctx context.Context, exprStr string, libs []string, activation any) (ref.Val, error) {
	env, err := e.Env(libs...)
	if err != nil {
		return nil, err
	}
	ast, issues := env.Parse(exprStr)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("parse expression: %w", issues.Err())
	}
	return e.Eval(ctx, env, ast, activation)
}

// libsKey returns a canonical cache key for a set of extension libraries, so
// that requests differing only in case or order share one environment.
func libsKey(libs []string) string {
	if len(libs) == 0 {
		return ""
	}
	normalized := make([]string, 0, len(libs))
	for _, l := range libs {
		normalized = append(normalized, strings.ToLower(l))
	}
	slices.Sort(normalized)
	normalized = slices.Compact(normalized)
	return strings.Join(normalized, ",")
}

// extensionLibraries maps the library names a workflow may enable to the
// environment options that provide them.
//
// Every entry must be deterministic and free of I/O: expressions are evaluated
// during workflow execution, where nondeterminism corrupts replay.
var extensionLibraries = map[string][]cel.EnvOption{
	"bindings":       {ext.Bindings()},
	"comprehensions": {ext.TwoVarComprehensions()},
	"encoders":       {ext.Encoders()},
	"json":           {jsonLibrary()},
	"lists":          {ext.Lists()},
	"math":           {ext.Math()},
	"optional":       {cel.OptionalTypes()},
	"protos":         {ext.Protos()},
	"regex":          {cel.OptionalTypes(), ext.Regex()},
	"sets":           {ext.Sets()},
	"strings":        {ext.Strings()},
}

// ExtensionLibraries returns the sorted names of the CEL extension libraries a
// workflow may enable, for use in error messages, documentation, and editor
// completion.
func ExtensionLibraries() []string {
	names := make([]string, 0, len(extensionLibraries))
	for name := range extensionLibraries {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// buildEnv constructs a CEL environment enabling the named extension libraries.
func buildEnv(libs []string) (*cel.Env, error) {
	opts := make([]cel.EnvOption, 0, len(libs)+1)
	for _, name := range libs {
		libOpts, ok := extensionLibraries[strings.ToLower(name)]
		if !ok {
			return nil, fmt.Errorf("unknown CEL extension library %q (available: %s)",
				name, strings.Join(ExtensionLibraries(), ", "))
		}
		opts = append(opts, libOpts...)
	}
	env, err := cel.NewEnv(opts...)
	if err != nil {
		return nil, fmt.Errorf("create CEL environment: %w", err)
	}
	return env, nil
}

// jsonLibrary returns the "json" extension library, which provides json_parse
// for turning a JSON string or bytes into CEL values.
//
// This lets a workflow keep tasks minimal — an HTTP task returns a body string
// and an expression picks fields out of it — instead of requiring a dedicated
// task for every response shape.
func jsonLibrary() cel.EnvOption {
	parse := func(data []byte) ref.Val {
		var out any
		if err := json.Unmarshal(data, &out); err != nil {
			return types.NewErr("json_parse: %v", err)
		}
		return types.DefaultTypeAdapter.NativeToValue(out)
	}
	return cel.Function("json_parse",
		cel.Overload("json_parse_string",
			[]*cel.Type{cel.StringType}, cel.DynType,
			cel.UnaryBinding(func(val ref.Val) ref.Val {
				s, ok := val.Value().(string)
				if !ok {
					return types.NewErr("json_parse: expected string input, got %v", val.Type())
				}
				return parse([]byte(s))
			}),
		),
		cel.Overload("json_parse_bytes",
			[]*cel.Type{cel.BytesType}, cel.DynType,
			cel.UnaryBinding(func(val ref.Val) ref.Val {
				b, ok := val.Value().([]byte)
				if !ok {
					return types.NewErr("json_parse: expected bytes input, got %v", val.Type())
				}
				return parse(b)
			}),
		),
	)
}
