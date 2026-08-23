package flowstatev1

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/ext"
	"github.com/google/cel-go/interpreter"
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

// MaxJSONParseBytes bounds one value decoded by json_parse. It matches the
// largest specification and default HTTP response body, the two places JSON
// supplied by a workflow can originate, while making the allocation performed
// inside the CEL function finite before json.Unmarshal is entered.
const MaxJSONParseBytes = 1 << 20

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
	// CEL otherwise assigns extension functions a constant call cost. JSON
	// decoding is linear in its input, so charge its bytes against the same
	// per-evaluation budget that bounds comprehensions. Repeated json_parse calls
	// can therefore consume at most Cost bytes in aggregate.
	opts := []cel.ProgramOption{cel.CostTracking(jsonParseCostEstimator{})}
	if l.Cost > 0 {
		// CostLimit implies OptTrackCost.
		//
		// The estimator is what decides that a unit of this budget buys a
		// bounded number of *bytes* rather than one operation of any size; see
		// celcost.go for why cel-go's own size-aware pricing cannot reach a
		// parsed AST. It is installed here rather than at any call site because
		// this is the one place both execution drivers build a program.
		opts = append(opts, cel.CostLimit(l.Cost), cel.CostTracking(evaluationCostEstimator))
	}
	if l.InterruptCheckFrequency > 0 {
		opts = append(opts, cel.InterruptCheckFrequency(l.InterruptCheckFrequency))
	}
	return opts
}

type jsonParseCostEstimator struct{}

func (jsonParseCostEstimator) CallCost(_ string, overloadID string, args []ref.Val, _ ref.Val) *uint64 {
	if (overloadID != "json_parse_string" && overloadID != "json_parse_bytes") || len(args) != 1 {
		return nil
	}
	var cost uint64
	switch value := args[0].Value().(type) {
	case string:
		cost = uint64(len(value))
	case []byte:
		cost = uint64(len(value))
	default:
		return nil
	}
	return &cost
}

var _ interpreter.ActualCostEstimator = jsonParseCostEstimator{}

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
	// Refused before it can become a key, which is what bounds the cache.
	//
	// The key is derived from names a workflow supplies, and a failed
	// construction used to be memoized alongside a successful one — so every
	// distinct unknown name became a permanent entry in a process-wide map with
	// no eviction. A loop with `continue_on_error` carries on past the
	// fail-closed error, so one run could add an entry per iteration: 200,000
	// distinct names retained 165 MiB, for the life of the worker, across runs
	// and across tenants.
	//
	// Rejecting first makes the key space the 2^11 subsets of the known
	// libraries, by construction rather than by hoping nobody asks twice. It is
	// the same rule the rest of this repo applies to anything a peer controls the
	// size of: bound the resource the attacker chooses, and the attacker chooses
	// the name.
	if err := checkLibraries(libs); err != nil {
		return nil, err
	}

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

// checkLibraries refuses a library name this build does not have.
//
// Reported before anything is cached, and worded the way the runtime already
// words it, so an author reading a diagnostic from `flow validate` and an
// operator reading an error from a worker see the same sentence.
func checkLibraries(libs []string) error {
	for _, lib := range libs {
		if _, ok := extensionLibraries[strings.ToLower(strings.TrimSpace(lib))]; !ok {
			return fmt.Errorf("unknown CEL extension library %q; available libraries are %s",
				lib, strings.Join(ExtensionLibraries(), ", "))
		}
	}

	return nil
}

// Eval evaluates ast in env against the given activation, enforcing e's limits
// and honoring ctx's cancellation.
//
// The activation may be a map[string]any or a cel.Activation, matching the CEL
// runtime's own contract.
func (e *Evaluator) Eval(ctx context.Context, env *cel.Env, ast *cel.Ast, activation any) (ref.Val, error) {
	prg, err := env.Program(ast, e.limits.programOptions()...)
	if err != nil {
		return nil, &ExpressionError{Err: fmt.Errorf("compile expression: %w", err)}
	}
	out, _, err := prg.ContextEval(ctx, activation)
	if err != nil {
		return nil, &ExpressionError{Err: fmt.Errorf("evaluate expression: %w", err)}
	}
	return out, nil
}

// An ExpressionError reports that a CEL expression failed to compile or to
// evaluate, and carries nothing but that fact: [ExpressionError.Error] is its
// cause's own words, byte for byte.
//
// It exists to classify, not to phrase. [ErrorKindExpression] is documented as
// the kind for an expression that "failed to parse, exceeded its cost budget,
// or referenced something that does not exist" (errors.go), and until this type
// existed nothing outside a task ever produced it: an expression failure left
// this evaluator as a bare [fmt.Errorf] chain, so [ClassifyError] fell through
// to its default and every surface reading a kind — `flow run local --output
// json`, the `flowstate_run_local` MCP result, a durable run's
// `RunResponse.Error.kind` — reported an author's `${['a'][5]}` or a refused
// cost budget as `Internal`, which errors.go defines as "a defect in Flowstate
// itself". Two consequences, neither cosmetic: an agent branching on the kind
// was told to file a bug rather than fix the file, and `Internal` is one of the
// two *retryable* kinds, so a deterministic failure was classified as one worth
// attempting again.
//
// Deliberately not a [TaskError] with an empty Task, which renders identically
// and would have been a smaller diff. The durable driver asks
// `errors.As(err, **v1.TaskError)` to decide whether a failure came from a task
// (`engine/workflow.go`'s recordedStepError), and answers "yes" by *dropping the
// enclosing position* — `iteration 0: step "child": …` — from the text it
// records. An expression failure has no task and needs that position, since the
// expression is the author's and the iteration is how they find it. So the
// classification travels in a type of its own, the text stays what it was, and
// nothing that reads for a task finds one.
type ExpressionError struct {
	// Err is the failure this classifies, already worded for a reader.
	Err error
}

// Error implements the error interface. The cause's own words and nothing
// added: this type is a classification, and a classification that also
// prefixes the message would double every sentence it travels with — the
// garbling #184 is about.
func (e *ExpressionError) Error() string { return e.Err.Error() }

// Unwrap returns the cause, so errors.Is and errors.As reach through this the
// way they reach through every other wrapper here.
func (e *ExpressionError) Unwrap() error { return e.Err }

// EvalParsed evaluates a previously parsed expression, of the form carried in a
// compiled workflow specification, against the given activation.
func (e *Evaluator) EvalParsed(ctx context.Context, env *cel.Env, parsed *expr.ParsedExpr, activation any) (ref.Val, error) {
	if parsed == nil {
		return nil, fmt.Errorf("parsed expression is nil")
	}
	return e.Eval(ctx, env, cel.ParsedExprToAst(parsed), activation)
}

// EvalParsedBase evaluates a previously parsed expression in the workflow's
// profile environment, which is the common case for resolving step inputs.
//
// Named "base" for what it is not — it carries no task's own scope — rather than
// for a smaller vocabulary. It used to mean both: an expression resolved here saw
// no extension libraries at all, so `if:`, `items:` and every task input spoke a
// poorer dialect than the `cel` step beside them. One profile is what removes that,
// and this is where most of the file feels it.
func (e *Evaluator) EvalParsedBase(ctx context.Context, profile string, parsed *expr.ParsedExpr, activation any) (ref.Val, error) {
	env, err := e.ProfileEnv(profile)
	if err != nil {
		return nil, err
	}
	return e.EvalParsed(ctx, env, parsed, activation)
}

// ProfileEnv returns the environment a named profile describes.
//
// Goes through [Evaluator.Env], so a profile's environment is cached and cost-
// limited exactly like any other — the profile decides *membership*, and nothing
// about how an environment is built or bounded moves with it.
func (e *Evaluator) ProfileEnv(profile string) (*cel.Env, error) {
	libs, err := ProfileLibraries(profile)
	if err != nil {
		return nil, err
	}
	return e.Env(libs...)
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
		// Parsing is an expression-failure phase like compiling and evaluating
		// (which [Evaluator.Eval] already wraps): a malformed expression is the
		// author's, not a defect in Flowstate, so it classifies [ErrorKindExpression]
		// rather than falling through [ClassifyError] to the retryable Internal
		// default. Without this wrapper parsing was the one phase still mislabeled
		// (#184, #899).
		return nil, &ExpressionError{Err: fmt.Errorf("parse expression: %w", issues.Err())}
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

// stringsExtensionVersion pins the strings extension library to the version
// this build was audited against.
//
// Unpinned, `ext.Strings()` means "every function, including whatever a future
// cel-go adds" — its default version is literally MaxUint32. That is the exact
// bug the profile mechanism exists to prevent: an expression stored in a run's
// specification silently changing meaning because a dependency bump taught the
// environment a new function or altered a version-gated behaviour. The profile
// closes that door for which *libraries* are present; this closes it for what
// one library *contains*.
//
// The gap was found by auditing every CEL surface against every other. All
// three policy surfaces — netpolicy's rules, auth's assumption rules, auth's
// secret rules — had already pinned version 5 for exactly this reason, and the
// workflow core, whose expressions live longest and travel furthest, was the
// one place left open.
//
// Five is the highest version cel-go v0.29.2 implements, so pinning it changes
// nothing today and is the whole point tomorrow: a cel-go upgrade that ships a
// version 6 does not reach a workflow until this constant is raised — a
// reviewed decision with a place for its reasoning, not a side effect of
// `go get -u`. When raising it, raise the three policy pins in the same commit
// or say why not; they are the same decision spelled in four places because
// the packages cannot share a constant without an import cycle.
const stringsExtensionVersion = 5

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
	"strings":        {ext.Strings(ext.StringsVersion(stringsExtensionVersion))},
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

// A workflow speaks one dialect, and it is the same dialect everywhere in the file.
//
// It used to speak two. A `cel` step could name extension libraries with `libs:`,
// and nothing else in the file could — so `if:`, `items:`, `wait_until:` and every
// task input were evaluated in an environment without them. Two expressions one
// line apart, in one document, with different vocabularies, and no way for a
// reader to infer which was which.
//
// The workaround for that already existed, one library at a time: see
// durationLibrary above, which is unconditional precisely because "a `wait_until:`
// step has no `libs:` key to enable anything with". That is this problem, solved
// for the library somebody hit first. A profile generalises it.
//
// # Why a named set rather than "everything this build has"
//
// Because a run has to keep meaning what it meant. Adding a library to a future
// build must not change how an expression already stored in `RunState` evaluates —
// invariant 10 — and "all of them" is a set that changes underfoot. A profile
// names a fixed membership, the compiler records which one a spec was built for,
// and a worker resolves that name rather than asking what it happens to have.

// CurrentProfile is the language profile this build evaluates against.
//
// Not yet recorded per run, and the distinction is the whole of what is left to
// do. A profile *name* freezes a membership, which is what makes "pinned per run"
// possible — but nothing stores which profile a spec was compiled against, so
// every expression is evaluated against whatever profile the worker running it
// calls current. Today that is safe because there is exactly one; the day a second
// exists it stops being, and the field and the threading have to land before then.
//
// The first attempt at this added `Workflow.profile` and then hardcoded
// CurrentProfile at both evaluation sites, so the value was recorded and never
// read. That is worse than not recording it: the schema claimed a guarantee the
// engine did not honour. Backed out rather than shipped half-wired.
const CurrentProfile = "2026.1"

// OriginalProfile is what a spec compiled before profiles existed evaluates as.
//
// Deliberately its own constant rather than an alias for [CurrentProfile], and the
// difference is the whole point: CurrentProfile *moves*, and this must not. Reading
// an unrecorded profile as "whatever is current" would mean a run that predates the
// field silently acquires each new vocabulary as it is released — losing exactly the
// pinning this mechanism exists to provide, for the runs least able to survive it.
//
// It equals CurrentProfile today because there has only ever been one profile. It
// stops equalling it the moment a second is minted, which is when this matters.
const OriginalProfile = "2026.1"

// profiles is the membership of each named profile.
//
// A profile is append-only in the sense that matters: once a name has been
// recorded in a spec, its membership is frozen. Adding libraries means adding a
// *new* profile, so that a run compiled against the old one keeps the vocabulary
// it was checked against.
var profiles = map[string][]string{
	// The first profile is every library this build shipped with when profiles
	// were introduced, which is also every library that existed. That is a
	// coincidence of timing rather than a rule: the second profile will differ
	// from "everything available" the moment a library is added.
	CurrentProfile: {
		"bindings", "comprehensions", "encoders", "json", "lists",
		"math", "optional", "protos", "regex", "sets", "strings",
	},
}

// ProfileLibraries returns the libraries a named profile includes.
//
// An unknown profile is an error rather than a fallback. A worker that cannot
// resolve the vocabulary a spec was compiled against does not know what the
// expressions in it mean, and guessing is how a run quietly starts computing
// something else — the fail-closed rule, applied to the language itself.
//
// This refusal has no caller that can reach it yet, because nothing passes a
// profile a spec chose. It is here because the refusal is the hard part to add
// later under pressure, not because it is exercised today.
func ProfileLibraries(profile string) ([]string, error) {
	if profile == "" {
		// A spec compiled before this field existed, which can only have come from
		// a build whose one vocabulary was the original — so that is what it gets,
		// permanently, rather than whatever this build happens to call current.
		profile = OriginalProfile
	}

	libs, ok := profiles[profile]
	if !ok {
		return nil, fmt.Errorf(
			"unknown language profile %q; this build knows %s — the spec was compiled by a newer "+
				"build than this worker, which cannot evaluate it",
			profile, strings.Join(profileNames(), ", "))
	}
	return slices.Clone(libs), nil
}

// profileNames returns the known profile names, sorted, for diagnostics.
func profileNames() []string {
	names := make([]string, 0, len(profiles))
	for name := range profiles {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// ProfileNames returns every known language profile's name, sorted.
//
// Exported for the reference documentation in docs/reference/cel.md, which
// says what each profile admits rather than only what the current one does —
// see cmd/flow/internal/docsgen/cel.go for why that is written as a loop
// over every profile even while there is only one to loop over.
func ProfileNames() []string {
	return profileNames()
}

// buildEnv constructs a CEL environment enabling the named extension libraries.
func buildEnv(libs []string) (*cel.Env, error) {
	opts := make([]cel.EnvOption, 0, len(libs)+len(durationLibrary())+1)

	// Always present rather than opt-in, unlike the libraries below. A
	// `wait_until:` step has no `libs:` key to enable anything with — the
	// expression is the whole of the step — so a unit only reachable through opt-in
	// would be missing exactly where durations are most written.
	//
	// This also gives the two spellings of a delay the same vocabulary: the
	// Flowfile's own duration parser already accepts `sleep: 3d`, and without these
	// the expression form would have made an author reach for `duration('72h')` to
	// say the same thing.
	opts = append(opts, durationLibrary()...)

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
		if len(data) > MaxJSONParseBytes {
			return types.NewErr("json_parse: input exceeds %d bytes", MaxJSONParseBytes)
		}
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

// durationUnits are the duration constructors available to every expression,
// largest first so the list reads the way a person says a duration.
//
// Go's own parser — which CEL's duration() calls — understands ns through h and
// stops, because outside a fixed offset a "day" is a calendar question rather
// than a quantity: days differ in length across a daylight-saving boundary. What
// a wait needs is the quantity, since it is an offset to a moment and not a date
// calculation, so days(3) is exactly 72 hours and this comment is where that is
// written down.
//
// Anything genuinely calendar-shaped — "09:00 next Tuesday in Berlin" — is a
// different problem, and one that needs a timezone to even be well posed. It is
// deliberately absent rather than approximated here, because a `days` that is
// usually right is worse than one that is always a fixed offset and says so.
var durationUnits = []struct {
	name string
	per  time.Duration
}{
	{"weeks", 7 * 24 * time.Hour},
	{"days", 24 * time.Hour},
	{"hours", time.Hour},
	{"minutes", time.Minute},
	{"seconds", time.Second},
}

// durationLibrary provides the duration constructors, so an author writes
// `days(3)` rather than counting hours into `duration('72h')`.
//
// hours, minutes and seconds duplicate what duration() can already spell. They
// exist anyway, because the point is that a reader can scan `days(3) + hours(12)`
// without stopping, and a set of units with a hole in it makes them stop.
func durationLibrary() []cel.EnvOption {
	opts := make([]cel.EnvOption, 0, len(durationUnits))

	for _, unit := range durationUnits {
		// Captured per iteration so each overload closes over its own unit.
		name, per := unit.name, unit.per

		// The largest count this unit can express before int64 nanoseconds wrap.
		// An author's expression is untrusted input, and without a bound
		// days(400000) becomes a *negative* duration: a wait meant for the far
		// future would already be in the past, and would release immediately
		// rather than fail. A silent sign flip is the worst available outcome, so
		// the limit is checked rather than the result inspected afterwards.
		limit := int64(math.MaxInt64 / int64(per))

		opts = append(opts, cel.Function(name,
			cel.Overload(name+"_int",
				[]*cel.Type{cel.IntType}, cel.DurationType,
				cel.UnaryBinding(func(val ref.Val) ref.Val {
					count, ok := val.Value().(int64)
					if !ok {
						return types.NewErr("%s: expected an integer, got %v", name, val.Type())
					}
					if count > limit || count < -limit {
						return types.NewErr(
							"%s(%d) is out of range: a duration cannot exceed about 292 years, so this unit stops at %d",
							name, count, limit)
					}
					return types.DefaultTypeAdapter.NativeToValue(time.Duration(count) * per)
				}),
			),
		))
	}

	return opts
}

// DurationUnits returns the names of the duration constructors every expression
// can use, largest unit first.
//
// Derived from the same table the functions are built from, so documentation and
// editor completion cannot drift from what an expression will actually accept.
func DurationUnits() []string {
	names := make([]string, 0, len(durationUnits))
	for _, unit := range durationUnits {
		names = append(names, unit.name)
	}
	return names
}
