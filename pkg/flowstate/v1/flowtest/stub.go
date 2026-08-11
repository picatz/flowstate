package flowtest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/nearest"
)

// compiledStub is a [Stub] with its `where:` and its `returns:` parsed once, at
// load time — compiling a bad expression is a property of the test file and
// belongs to [compileStubs]'s error, not to whichever invocation happens to
// reach it first.
type compiledStub struct {
	// task and step are the two ways a stub names what it replaces, exactly one
	// set. task is filled straight from the file; step is resolved to its task
	// against the compiled workflow by [bindStubs], and stepScope carries the
	// step id forward so the matcher answers only that step's invocations.
	task      string
	step      string
	stepScope string

	where *expr.ParsedExpr // nil matches unconditionally

	// whereSource is the `where:` clause exactly as the author wrote it, kept
	// alongside the parsed form so an unmatched-stub failure can show the
	// clause that did not match rather than making the reader reconstruct it
	// from the compiled expression. Empty when where is nil.
	whereSource string

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

// compileStubs parses every stub's `where:` and `returns:` once, keeping them in
// the order they were written. It does not yet group them by task, because a
// step-form stub's task is not known until the workflow it names a step of has
// been compiled; that resolution is [bindStubs], run after the parse.
func compileStubs(stubs []Stub) ([]compiledStub, error) {
	compiled := make([]compiledStub, 0, len(stubs))

	for i, s := range stubs {
		var parsed *expr.ParsedExpr
		if s.Where != "" {
			value := v1.NewExpr(s.Where)
			if errKind := value.GetError(); errKind != nil {
				return nil, fmt.Errorf("stub %d for %s: where: %s", i+1, stubTarget(&s), errKind.GetMessage())
			}
			parsed = value.GetExpr()
		}

		returns, err := compileReturns(s.Returns)
		if err != nil {
			return nil, fmt.Errorf("stub %d for %s: returns: %w", i+1, stubTarget(&s), err)
		}

		compiled = append(compiled, compiledStub{
			task:        s.Task,
			step:        s.Step,
			where:       parsed,
			whereSource: s.Where,
			returns:     returns,
			hasReturns:  s.Returns != nil,
			fails:       s.Fails,
		})
	}

	return compiled, nil
}

// stubTaskNames is the set of task names task-form stubs replace, which is the
// only set [swapRegistry] has to pre-register a synthetic shape for: a step-form
// stub names a step of the workflow, whose task the compiler already knows, so
// it can never be the name the build is missing.
func stubTaskNames(compiled []compiledStub) []string {
	seen := map[string]bool{}
	var names []string
	for i := range compiled {
		name := compiled[i].task
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		names = append(names, name)
	}
	return names
}

// bindStubs resolves every stub to the task it answers and groups them by task
// name, preserving the order the stubs were written so a task's matchers are
// still tried as the switch-like sequence [Stub.Where] documents.
//
// A step-form stub is resolved against the compiled workflow: its step id names
// the task that step invokes, checked here rather than trusted, so an unknown id
// is refused with a did-you-mean suggestion (the same [nearest] machinery every
// other surface reads the compiled workflow through). That is what lets a stub
// reference the workflow's own name for a thing instead of retyping a value the
// step happens to carry (issue #416, principle 12).
func bindStubs(compiled []compiledStub, spec *v1.Workflow) (map[string]*stubbedTask, error) {
	taskOfStep, kindOfStep := stepTasks(spec)

	byTask := make(map[string]*stubbedTask)
	for i := range compiled {
		m := compiled[i]

		task := m.task
		if m.step != "" {
			resolved, ok := taskOfStep[m.step]
			if !ok {
				return nil, unknownStepError(m.step, kindOfStep, taskOfStep)
			}
			task = resolved
			m.stepScope = m.step
		}

		t, ok := byTask[task]
		if !ok {
			t = &stubbedTask{}
			byTask[task] = t
		}
		t.matchers = append(t.matchers, m)
	}

	return byTask, nil
}

// unknownStepError refuses a step-form stub naming a step the workflow does not
// have as a stubbable one, positioned by the step id and carrying a did-you-mean
// suggestion drawn from the workflow's own task steps.
//
// A step that exists but runs no task (a wait, a loop container, a bare
// parallel) is told apart from one that does not exist at all, because the fix
// differs: the first is a real id aimed at the wrong kind of step, the second is
// a typo.
func unknownStepError(step string, kindOfStep map[string]string, taskOfStep map[string]string) error {
	if kind, exists := kindOfStep[step]; exists {
		return fmt.Errorf("stub names step %q, which runs no task (it is a %s step) and so cannot be stubbed; "+
			"stub a task step, or the task itself with `task:` and `where:`", step, kind)
	}

	names := make([]string, 0, len(taskOfStep))
	for id := range taskOfStep {
		names = append(names, id)
	}
	sort.Strings(names)
	if suggestion, ok := nearest.Name(step, names); ok {
		return fmt.Errorf("stub names unknown step %q; did you mean %q?", step, suggestion)
	}
	return fmt.Errorf("stub names unknown step %q, which this workflow has no task step for", step)
}

// stepTasks walks a compiled workflow and returns two maps: every task step's id
// to the task it invokes, and every non-task step's id to a word naming its kind
// (`wait`, `loop`, `for_each`, `call`, `value`). Together they cover every step a
// step-form stub could name, so a stub aimed at a wait is told apart from one
// aimed at nothing.
//
// It descends into loop and for_each bodies and parallel branches for the same
// reason coverage does: those hold steps an author wrote and could name. It does
// not descend into a `call:`, whose steps belong to the callee's own file.
func stepTasks(spec *v1.Workflow) (taskOfStep map[string]string, kindOfStep map[string]string) {
	taskOfStep = map[string]string{}
	kindOfStep = map[string]string{}
	var walk func(nodes []*v1.Node)
	walk = func(nodes []*v1.Node) {
		for _, node := range nodes {
			switch kind := node.GetKind().(type) {
			case *v1.Node_Task:
				taskOfStep[node.GetId()] = kind.Task.GetName()
			case *v1.Node_Wait:
				kindOfStep[node.GetId()] = "wait"
			case *v1.Node_Call:
				kindOfStep[node.GetId()] = "call"
			case *v1.Node_Value:
				// A step that exists and runs no task, so it belongs in the map
				// that tells those apart from a typo. Without this arm a stub
				// aimed at a value step was answered with "unknown step, which
				// this workflow has no task step for", a sentence that is false
				// twice over about a step written three lines above it, and one
				// that sends an author looking for a misspelling.
				//
				// It is also the kind most likely to be aimed at by mistake: a
				// value is exactly the sort of thing a test wants to force, and
				// the honest answer is that there is nothing to stub, because
				// nothing is invoked. The expression is the value.
				kindOfStep[node.GetId()] = "value"
			case *v1.Node_Parallel:
				for _, branch := range kind.Parallel.GetBranches() {
					walk(branch.GetSteps())
				}
			case *v1.Node_ForEach:
				kindOfStep[node.GetId()] = "for_each"
				walk(kind.ForEach.GetBody())
			case *v1.Node_Loop:
				kindOfStep[node.GetId()] = "loop"
				walk(kind.Loop.GetBody())
			}
		}
	}
	walk(spec.GetSteps())
	return taskOfStep, kindOfStep
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
//
// sensitiveInputNames is the workflow's own declared-sensitive input names
// (see [v1.SensitiveInputNames]), threaded through once at registry build time
// so an unmatched-stub failure (see [unmatchedStubError]) can redact a value
// that traces back to one, the same "display etiquette" every other surface
// applies, not skipped just because this surface is new.
func (s *stubbedTask) fn(name string, sensitiveInputNames map[string]bool) v1.TaskFunc {
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

		native, secretNames, err := invocationNativeInputs(inputs, resolvedSecrets)
		if err != nil {
			return nil, v1.NewTaskError(name, v1.ErrorKindExpression, err)
		}

		activation := stubActivation(ctx, scope, native)

		var verdicts []stubVerdict
		// sawEvalErr tracks whether any tried matcher's where: failed to
		// evaluate, so the final failure keeps ErrorKindExpression rather than
		// misreporting a broken CEL expression as ErrorKindInvalidInput; see
		// below, where the loop no longer returns the moment one matcher
		// errors.
		var sawEvalErr bool

		for _, m := range s.matchers {
			// A step-form stub answers only its own step's invocations, told
			// apart by the step id the engine records on each node's context.
			// An undo call carries none (it runs off the run level context), so
			// a step stub never answers a compensation, which is what keeps the
			// forward call and its reversal stubbable separately.
			if m.stepScope != "" {
				current, ok := v1.TaskStepFromContext(ctx)
				if !ok || current != m.stepScope {
					continue
				}
			}

			ok, matchErr := m.matches(ctx, scope.GetProfile(), activation)
			verdicts = append(verdicts, stubVerdict{whereSource: m.whereSource, matched: ok, err: matchErr})
			if matchErr != nil {
				// Not returned immediately: a broken where: on one stub must
				// not hide a later stub that matches cleanly, and must not
				// make [stubVerdict.err] unreachable from the diagnostic this
				// invocation ultimately reports if nothing does match (#386
				// follow-up), recorded above and surfaced below either way.
				sawEvalErr = true
				continue
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

		// A where: that failed to evaluate is a broken expression, the same
		// kind [m.matches] itself would have reported had this returned
		// immediately; a where: that merely evaluated false is an ordinary
		// unmatched invocation. The diagnostic text is identical either way,
		// which is the point: the kind is what a caller's retry: policy
		// reads, and it must still name the real cause.
		kind := v1.ErrorKindInvalidInput
		if sawEvalErr {
			kind = v1.ErrorKindExpression
		}
		return nil, v1.NewTaskError(name, kind,
			unmatchedStubError(name, len(s.matchers), native, secretNames,
				sensitiveNativeValues(scope, sensitiveInputNames), verdicts))
	}
}

// invocationNativeInputs builds what one invocation's `where:` and `returns:`
// are evaluated against, and what an unmatched-stub failure reports back: the
// task's own resolved inputs, as native CEL values, plus the subset of names
// whose value came from a `secrets:` reference rather than the input itself.
//
// secretNames exists because the returned native map holds a resolved
// secret's plaintext in the clear: `where:` has to be able to compare
// against it, the same way it compares against any other input, and that is
// fine for evaluation, which never leaves this process. It stops being fine
// the moment a caller turns this map into text for a failure message: per
// CLAUDE.md's "secrets never enter workflow history", a resolved secret must
// never be printed, so [unmatchedStubError] redacts every name secretNames
// marks regardless of what its `where:` clauses do or do not reveal.
func invocationNativeInputs(inputs map[string]*v1.Value, resolvedSecrets map[string]any) (native map[string]any, secretNames map[string]bool, err error) {
	native = make(map[string]any, len(inputs))
	for name, v := range inputs {
		if lit := v.GetLiteral(); lit != nil {
			value, err := literalToGo(lit)
			if err != nil {
				return nil, nil, fmt.Errorf("input %q: %w", name, err)
			}
			native[name] = value
			continue
		}
		// A whole-input reference resolves to its plaintext, and a structured
		// input holding one — `headers:`, `json:`, `form:` — resolves to the
		// native map or list the task itself would see, so `where:` can match
		// on `inputs.headers.Authorization` the same way it matches on
		// `inputs.bearer`. See [resolveSecretInputs].
		if value, ok := resolvedSecrets[name]; ok {
			native[name] = value
			if secretNames == nil {
				secretNames = make(map[string]bool)
			}
			secretNames[name] = true
			continue
		}
		// An input the task evaluates itself ([v1.TaskDef.DeferredInputs]) is
		// still an expression at this point and has nothing a `where:` clause
		// can compare against; it is simply absent from `inputs` rather than
		// resolved to something misleading. What the expression is *written in
		// terms of* — the loop's binding, the step's vars — is reachable
		// through the scope, which is what makes an iteration distinguishable
		// even where its inputs are not.
	}

	return native, secretNames, nil
}

// stubActivation builds what one invocation's `where:` and `returns:` are
// evaluated against: the scope the step itself was evaluated in, plus the
// invocation's own resolved inputs already converted to native CEL values by
// [invocationNativeInputs].
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
// since stubs existed, and the alternative, silently changing what
// `inputs.url` means in every test file in the corpus, is worse than one
// documented collision. See [Stub.Where].
func stubActivation(ctx context.Context, scope *v1.Scope, native map[string]any) cel.Activation {
	return scope.ActivationWith(ctx, map[string]ref.Val{
		v1.InputsRoot: types.NewStringInterfaceMap(v1.TypeAdapter, native),
	})
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

// stubVerdict is one matcher's outcome against one invocation, kept so
// [unmatchedStubError] can show every stub's `where:` beside what it decided
// instead of making the reader re-derive it (#386). Only matchers actually
// tried for this invocation are recorded: a step-form stub skipped because
// it names a different step never reaches [compiledStub.matches] and has
// nothing useful to report here.
type stubVerdict struct {
	whereSource string // empty for a stub with no `where:`
	matched     bool
	err         error
}

// maxUnmatchedStubValueLen bounds how much of one input's rendered value an
// unmatched-stub failure prints. Inputs are attacker-adjacent-sized (an http
// task's body can be megabytes), and the author needs the discriminating
// field, not the payload, so this elides past a point well short of that
// (CLAUDE.md, "bound anything that consumes untrusted input").
const maxUnmatchedStubValueLen = 200

// sensitiveNativeValues returns, as native Go values comparable with
// [reflect.DeepEqual], the run's own values for every workflow input
// sensitiveNames marks: the values [unmatchedStubError] must not print even
// when they reach a task's inputs under a different name, since `sensitive:`
// is a property of the value's origin, not of whatever a step chose to call
// it. Returns nil when there is nothing to redact, which is the common case
// and costs one nil map read per invocation.
func sensitiveNativeValues(scope *v1.Scope, sensitiveNames map[string]bool) []any {
	if len(sensitiveNames) == 0 {
		return nil
	}

	var values []any
	for name, v := range scope.GetInputs() {
		if !sensitiveNames[name] {
			continue
		}
		lit := v.GetLiteral()
		if lit == nil {
			continue
		}
		native, err := literalToGo(lit)
		if err != nil {
			continue
		}
		values = append(values, native)
	}
	return values
}

// isSensitiveValue reports whether v is one of the run's own sensitive-input
// values, by content rather than by the name the task happened to give it:
// `message: ${inputs.token}` is caught the same as `inputs.token` itself.
func isSensitiveValue(v any, sensitiveValues []any) bool {
	for _, sv := range sensitiveValues {
		if reflect.DeepEqual(v, sv) {
			return true
		}
	}
	return false
}

// redactSensitiveTree walks v and replaces every value that [isSensitiveValue]
// recognizes, at any depth, with a marker string.
//
// A `map[string]any` or `[]any` compares as a whole against reflect.DeepEqual,
// so a top-level check alone misses the far more common shape: a sensitive
// scalar carried *inside* a structured input, such as `headers: {Authorization:
// ${inputs.token}}`. The whole headers map is never equal to the token; only
// one leaf of it is, so the leaf has to be checked on its own (#386
// follow-up). The recursion mirrors the two structured shapes an input can
// hold ([invocationNativeInputs] never produces anything else): a map keyed
// by string, and a list.
func redactSensitiveTree(v any, sensitiveValues []any) any {
	if isSensitiveValue(v, sensitiveValues) {
		return sensitiveMarker
	}

	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = redactSensitiveTree(e, sensitiveValues)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = redactSensitiveTree(e, sensitiveValues)
		}
		return out
	default:
		return v
	}
}

// sensitiveMarker is what a redacted leaf renders as inside a structure:
// deliberately not shaped like a value the workload could have produced
// itself, the same discipline [formatUnmatchedStubValue]'s own top-level
// marker follows.
const sensitiveMarker = "[redacted]"

// redactSensitiveSubstrings is the backstop for a sensitive value that
// reaches the invocation as part of a larger string rather than as the whole
// value or a map/list leaf: `"Bearer " + inputs.token` renders as one
// string, which [redactSensitiveTree] cannot see into because nothing about
// the concatenated result equals the token on its own. Every sensitive
// string value's exact text is replaced wherever it occurs in rendered, so
// the credential cannot survive by being wrapped in unrelated characters.
func redactSensitiveSubstrings(rendered string, sensitiveValues []any) string {
	for _, sv := range sensitiveValues {
		s, ok := sv.(string)
		if !ok || s == "" {
			continue
		}
		rendered = strings.ReplaceAll(rendered, s, sensitiveMarker)
	}
	return rendered
}

// truncateRuneSafe elides rendered past [maxUnmatchedStubValueLen], cutting
// at a rune boundary rather than a byte offset. A byte cut through the
// middle of a multi-byte UTF-8 sequence produces invalid UTF-8, which
// encoding/json (and so `-o json`, via protojson) refuses to encode as a
// string at all, turning one test's overlong value into every case's JSON
// report failing to marshal, not just this one line's own display (#386
// follow-up).
func truncateRuneSafe(rendered string, max int) string {
	if len(rendered) <= max {
		return rendered
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(rendered[cut]) {
		cut--
	}
	return rendered[:cut] + "...(truncated)"
}

// formatUnmatchedStubValue renders one invocation input for the failure
// message: redacted when it is a secret reference or traces back to a
// sensitive workflow input at any depth, quoted and bounded otherwise. It
// never returns a resolved secret's plaintext or a sensitive input's value in
// the clear, at the top level, nested inside a structure, or concatenated
// into a larger string; see [invocationNativeInputs]'s doc for why that
// distinction has to survive past evaluation and into anything printed.
func formatUnmatchedStubValue(name string, v any, isSecret bool, sensitiveValues []any) string {
	if isSecret || isSensitiveValue(v, sensitiveValues) {
		return fmt.Sprintf("[redacted: %s]", name)
	}

	redacted := redactSensitiveTree(v, sensitiveValues)

	var rendered string
	if s, ok := redacted.(string); ok {
		rendered = strconv.Quote(s)
	} else {
		rendered = fmt.Sprintf("%v", redacted)
	}

	rendered = redactSensitiveSubstrings(rendered, sensitiveValues)

	return truncateRuneSafe(rendered, maxUnmatchedStubValueLen)
}

// unmatchedStubError builds the diagnostic for a task invocation no declared
// stub answered: what the invocation actually carried, redacted where
// CLAUDE.md requires it, and every tried stub's `where:` beside whether it
// matched, so an author can see why in the failure output instead of having
// to reason it out from the workflow (#386, "diagnostics are a feature").
func unmatchedStubError(name string, declared int, native map[string]any, secretNames map[string]bool, sensitiveValues []any, verdicts []stubVerdict) error {
	var b strings.Builder
	fmt.Fprintf(&b, "flow test: task %q was invoked with no matching stub (%d stub(s) declared for it); "+
		"add a stub with no `where:` to answer every invocation, or one whose `where:` matches these inputs\n",
		name, declared)

	names := make([]string, 0, len(native))
	for n := range native {
		names = append(names, n)
	}
	sort.Strings(names)

	if len(names) == 0 {
		b.WriteString("  invocation carried no inputs\n")
	} else {
		b.WriteString("  invocation carried:\n")
		for _, n := range names {
			fmt.Fprintf(&b, "    %s: %s\n", n, formatUnmatchedStubValue(n, native[n], secretNames[n], sensitiveValues))
		}
	}

	if len(verdicts) == 0 {
		b.WriteString("  no stub was tried against this invocation")
	} else {
		b.WriteString("  stub verdicts:")
		for i, v := range verdicts {
			where := v.whereSource
			if where == "" {
				where = "(no where:)"
			}
			switch {
			case v.matched:
				fmt.Fprintf(&b, "\n    stub %d requires: %s   -> true", i+1, where)
			case v.err != nil:
				fmt.Fprintf(&b, "\n    stub %d requires: %s   -> error: %s", i+1, where, v.err)
			default:
				fmt.Fprintf(&b, "\n    stub %d requires: %s   -> false", i+1, where)
			}
		}
	}

	return errors.New(b.String())
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
