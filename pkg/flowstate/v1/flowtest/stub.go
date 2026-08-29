package flowtest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
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
	// workflow and step forward so the matcher answers only that exact step's
	// invocations.
	task      string
	step      string
	stepScope stubStepScope

	// ordinal is this stub's 1-based position in the case's declared order —
	// the number every diagnostic about it uses ("stub 2"), kept here because
	// [bindStubs] regroups matchers by task and the position in a task's own
	// matcher list stops being the position the author can count to.
	ordinal int

	// fromDefaults mirrors [Stub]'s provenance mark: this stub reached the
	// case through the file's `defaults:`. Read by [unusedStubWarnings],
	// which exempts an inherited catch-all from the idle-stub warning a
	// case's own stub earns (#926).
	fromDefaults bool

	// answered records that this matcher answered at least one invocation in
	// the current case — with `returns:` or with `fails:`, either is an
	// answer. Written under [stubbedTask.mu], because two parallel branches
	// can invoke one task concurrently. Per-case state: [bindStubs] builds a
	// fresh matcher list for every case, and schedule exploration re-runs the
	// whole case, so nothing here survives into another run.
	answered bool

	// times is the stub's declared answer budget, 0 for unbounded; remaining
	// counts it down per answer, under the same lock and with the same
	// per-case lifetime as answered. A drained matcher (times > 0, remaining
	// 0) is skipped with a verdict that says so, and the list falls through —
	// which is the whole mechanism of [Stub.Times]: the matcher sequence
	// becomes a script.
	times     int
	remaining int

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

	// response is [Stub.Response] compiled exactly as returns is — the same
	// fence rule at every depth — and resolved per invocation the same way.
	// What the resolved fields *mean* is the task's business
	// ([v1.TaskDef.StubResponseFn]); this package only carries them there.
	response    map[string]any
	hasResponse bool

	fails *StubFailure
}

type stubStepScope struct {
	workflow string
	step     string
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
	// mu serializes the matcher scan and the per-matcher bookkeeping against
	// concurrent invocations of one task: two `parallel:` branches invoking
	// the same task race otherwise, and the bookkeeping ([compiledStub.answered],
	// and a `times:` budget once one exists) must decide atomically with the
	// match itself. Held for the length of one invocation's scan — a test
	// harness's matcher list is short, and a lock that is obviously correct
	// beats a clever one in the package whose own claims are ordering claims.
	mu sync.Mutex

	// invoked records that any invocation of this task reached the stub set at
	// all, which is what tells "this case never invokes the task" apart from
	// "the task ran and this stub never matched" in [unusedStubWarnings].
	invoked bool

	// respond is the task's own [v1.TaskDef.StubResponseFn], resolved once at
	// bind time so a `response:` stub aimed at a task with no raw-response
	// semantics is refused before the run rather than surfacing mid-case. Nil
	// where the task defines none — which [bindStubs] has then already
	// guaranteed no matcher here needs.
	respond func(ctx context.Context, inputs map[string]*v1.Value, scope *v1.Scope, response map[string]*v1.Value) (*v1.Node_Outputs, error)

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

		response, err := compileReturns(s.Response)
		if err != nil {
			return nil, fmt.Errorf("stub %d for %s: response: %w", i+1, stubTarget(&s), err)
		}

		times := 0
		if s.Times != nil {
			times = *s.Times
		}

		compiled = append(compiled, compiledStub{
			task:         s.Task,
			step:         s.Step,
			ordinal:      i + 1,
			fromDefaults: s.fromDefaults,
			where:        parsed,
			whereSource:  s.Where,
			returns:      returns,
			hasReturns:   s.Returns != nil,
			response:     response,
			hasResponse:  s.Response != nil,
			fails:        s.Fails,
			times:        times,
			remaining:    times,
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
			m.stepScope = stubStepScope{workflow: spec.GetName(), step: m.step}
		}

		t, ok := byTask[task]
		if !ok {
			t = &stubbedTask{}
			if def, exists := v1.DefaultRegistry().Lookup(task); exists {
				t.respond = def.StubResponseFn
			}
			byTask[task] = t
		}

		// A `response:` stub needs the task to say what a raw response means
		// ([v1.TaskDef.StubResponseFn]), and only a task with deferred
		// response semantics does — refused here, before the run, with the
		// spelling that exists. A task this harness knows only by name (a
		// plugin's) has no semantics to consult, and gets the same answer.
		if m.hasResponse && t.respond == nil {
			return nil, fmt.Errorf(
				"stub %d for %s declares response:, but task %q does not evaluate a raw response — "+
					"only a task with deferred inputs (such as http's outputs: and expect:) can; "+
					"use returns: for already-shaped outputs",
				m.ordinal, describeStubTarget(&m), task)
		}

		t.matchers = append(t.matchers, m)
	}

	return byTask, nil
}

// describeStubTarget names a compiled stub the way the author aimed it, for a
// diagnostic: by step where a step was named, by task otherwise.
func describeStubTarget(m *compiledStub) string {
	if m.step != "" {
		return fmt.Sprintf("step %q", m.step)
	}
	return fmt.Sprintf("task %q", m.task)
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

	// Suggestions draw on every step the workflow has, not only the task
	// steps: a typo one letter off a `call:` or a `wait` step used to get the
	// bare "no task step" sentence below, because the candidate list stopped
	// at taskOfStep — and the author retyping the suggested id then gets the
	// kind-specific refusal above, which names the actual fix (#926).
	names := make([]string, 0, len(taskOfStep)+len(kindOfStep))
	for id := range taskOfStep {
		names = append(names, id)
	}
	for id := range kindOfStep {
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
			case *v1.Node_Switch:
				// Like a value:, there is nothing to stub on the switch itself —
				// the discriminant is the expression — but its body steps are
				// stubbable like any others, whichever branch a test drives the
				// run down.
				kindOfStep[node.GetId()] = "switch"
				for _, body := range v1.SwitchBodies(kind.Switch) {
					walk(body)
				}
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

		// The transcript's share of an answer (#929 slice 2): which matcher
		// answered, serving which step — the same numbering every stub
		// diagnostic uses, recorded so a failing case's account can say
		// `stub 2 (step: build)` beside what the step produced. Nil outside
		// `flow test`'s own runs, where nothing records.
		recordAnswer := func(m *compiledStub) {
			if recorder := runRecorderFromContext(ctx); recorder != nil {
				serving, _ := v1.TaskStepFromContext(ctx)
				recorder.stubAnswered(name, m.ordinal, m.step, serving)
			}
		}

		// The scan and its bookkeeping are one atomic decision: two parallel
		// branches invoking this task concurrently must not both read a
		// matcher's state between one another's updates. See [stubbedTask.mu].
		s.mu.Lock()
		defer s.mu.Unlock()
		s.invoked = true

		var verdicts []stubVerdict
		// sawEvalErr tracks whether any tried matcher's where: failed to
		// evaluate, so the final failure keeps ErrorKindExpression rather than
		// misreporting a broken CEL expression as ErrorKindInvalidInput; see
		// below, where the loop no longer returns the moment one matcher
		// errors.
		var sawEvalErr bool

		for i := range s.matchers {
			m := &s.matchers[i]

			// A step-form stub answers only its own step's invocations, told
			// apart by the step id the engine records on each node's context.
			// An undo call carries none (it runs off the run level context), so
			// a step stub never answers a compensation, which is what keeps the
			// forward call and its reversal stubbable separately.
			if m.stepScope.step != "" {
				current, ok := v1.TaskStepRefFromContext(ctx)
				if !ok || current.Workflow != m.stepScope.workflow || current.Step != m.stepScope.step {
					continue
				}
			}

			// A drained `times:` stub retires: the list falls through to the
			// next matcher, which is the scripting [Stub.Times] promises. The
			// verdict is recorded so an invocation that then matches nothing
			// is explained by the budget it fell past, not left mysterious.
			if m.times > 0 && m.remaining == 0 {
				verdicts = append(verdicts, stubVerdict{whereSource: m.whereSource, drained: m.times})
				continue
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
				// A `fails:` answer is an answer: the stub did its job, so the
				// unused-stub report must not name it (#926), and it spends
				// `times:` budget exactly as a `returns:` answer does.
				m.answered = true
				if m.times > 0 {
					m.remaining--
				}
				recordAnswer(m)
				kind := v1.ErrorKind(m.fails.Kind)
				if kind == "" {
					kind = v1.ErrorKindUpstream
				}
				return nil, v1.NewTaskError(name, kind, errors.New(m.fails.Message))
			}

			m.answered = true
			if m.times > 0 {
				m.remaining--
			}
			recordAnswer(m)

			// A `response:` answer hands the resolved fields to the task's own
			// raw-response evaluation ([v1.TaskDef.StubResponseFn], resolved
			// non-nil at bind time), with the invocation's full input map —
			// deferred expressions included — so the step's `outputs:` and
			// `expect:` run exactly as they would over a live response (#925).
			if m.hasResponse {
				resolved, err := resolveStubValues(ctx, scope.GetProfile(), activation, m.response)
				if err != nil {
					return nil, v1.NewTaskError(name, v1.ErrorKindExpression, fmt.Errorf("response: %w", err))
				}
				return s.respond(ctx, inputs, scope, v1.NewNamedValues(resolved))
			}

			returns, err := m.answer(ctx, scope.GetProfile(), activation)
			if err != nil {
				return nil, v1.NewTaskError(name, v1.ErrorKindExpression, err)
			}

			return &v1.Node_Outputs{NamedValues: v1.NewNamedValues(returns)}, nil
		}

		// An invocation nothing answered clears any attribution an earlier
		// attempt of the same step recorded — see [runRecorder.stubUnmatched].
		if recorder := runRecorderFromContext(ctx); recorder != nil {
			serving, _ := v1.TaskStepFromContext(ctx)
			recorder.stubUnmatched(serving)
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

	// drained is the stub's spent `times:` budget when the matcher was
	// skipped as retired rather than tried at all — the one verdict where the
	// clause is not what decided (#927). Zero otherwise.
	drained int
}

// maxUnmatchedStubValueLen bounds how much of one input's rendered value an
// unmatched-stub failure prints. Inputs are attacker-adjacent-sized (an http
// task's body can be megabytes), and the author needs the discriminating
// field, not the payload, so this elides past a point well short of that
// (CLAUDE.md, "bound anything that consumes untrusted input").
const maxUnmatchedStubValueLen = 200

// sensitiveInputs is the redaction set one unmatched-stub diagnostic is
// rendered against: what [unmatchedStubError] must not print, plus whether
// that set could be built at all.
//
// The two lists are deliberately different sizes. values is compared with
// [reflect.DeepEqual] against every node of the invocation, so it holds each
// sensitive input's whole value *and* every value nested within it.
// substrings is the textual backstop [redactSensitiveSubstrings] applies to
// the rendered line, which is a far blunter instrument — a string replaced
// everywhere it occurs — so it holds a narrower set; the walk in
// [sensitiveNativeValues] decides which strings earn a place in it.
//
// withholdAll is the fail-closed answer: when the set could not be built
// completely, no input can be shown to be safe, so every input on the
// invocation is withheld. [sensitiveNativeValues] documents the two ways that
// happens.
type sensitiveInputs struct {
	values      []any
	substrings  []string
	withholdAll bool
}

// minSensitiveSubstringRunes is the shortest *descendant* string
// [redactSensitiveSubstrings] will replace textually. A one-rune leaf is not
// a redaction, it is a shredder: replacing every `a` in the rendered line
// with the marker destroys the diagnostic while protecting nothing
// [isSensitiveValue] has not already caught by comparing that leaf on its
// own. A declared input's own value is exempt from this floor — it is the
// thing `sensitive:` names, and `"Bearer " + inputs.token` is precisely the
// shape the backstop exists for.
const minSensitiveSubstringRunes = 2

// maxSensitiveSubstringRedactionWork bounds the bytes inspected while
// redacting one rendered value. The strings being searched and the text being
// searched are both controlled by a submitted test, so bounding either one
// alone does not bound their product. When the estimate exceeds this limit,
// [redactSensitiveSubstrings] withholds the complete value instead.
const maxSensitiveSubstringRedactionWork = 1 << 20

// maxSensitiveDescendants bounds how many values one invocation's redaction
// set may hold, counting both what the walk below has collected and what it
// still has queued. The queue is counted because it is memory the same input
// controls: bounding only the result would leave a wide, shallow value free
// to push millions of entries onto the stack on the way to a bounded answer,
// which is the "bounding one resource does not bound another" failure
// CLAUDE.md names.
//
// A workflow input may legitimately carry maxListElements = 10,000 elements
// (pkg/flowstate/v1/constraints.go), and every entry in the set costs one
// [reflect.DeepEqual] per node of the invocation in [redactSensitiveTree]
// plus, for a string, a full [strings.ReplaceAll] pass over the rendered
// value. 1024 sits far above any real credential — a token, a key pair, a
// small object of headers — and far below where building a *failure* message
// costs more than the run it is reporting on.
//
// Blowing the bound is not a reason to redact less: [sensitiveNativeValues]
// answers withholdAll, so an input too large to enumerate withholds the whole
// invocation rather than printing the part of it the walk never reached. That
// is one mechanism serving two of CLAUDE.md's rules at once — bound the
// resource the attacker controls, and deny when you cannot decide.
const maxSensitiveDescendants = 1024

// sensitiveNativeValues returns, as native Go values comparable with
// [reflect.DeepEqual], every value the run's own `sensitive:` inputs carry:
// each such input's whole value and every value nested within it. These are
// what [unmatchedStubError] must not print even when they reach a task's
// inputs under a different name, since `sensitive:` is a property of the
// value's origin, not of whatever a step chose to call it.
//
// The descendants are in there because a sensitive declaration can itself be
// structured. A `creds:` input marked `sensitive: true` and read as
// `${inputs.creds.token}` puts a leaf into the invocation that is not
// [reflect.DeepEqual] to anything the scope holds, so a set of whole values
// alone prints that credential in the clear — and [redactSensitiveSubstrings]
// does not save it either, since the whole value there is a map rather than a
// string. Returns the zero value when there is nothing to redact, which is
// the common case and costs one nil map read per invocation.
//
// # The cost of matching by value, and why it is still the right rule
//
// This matches by *content*, with no provenance: nothing in
// [invocationNativeInputs] records which declaration a native value came
// from, so a descendant that happens to equal an unrelated input's value
// redacts that input too. Sensitive `creds: {enabled: false}` puts `false`
// into the set, and an ordinary `follow_redirects: false` on the same
// invocation then renders as `[redacted: follow_redirects]` — hiding one of
// the discriminating fields this diagnostic exists to show (Codex, #956).
//
// That cost is real, and it is chosen for the same reason cmd/flow's
// redactStepValues chooses the same trade at greater length. The precise
// alternative is to trace each value back to the declaration it came from,
// and a trace catches only what it can see: a sensitive leaf that reaches the
// invocation through a step's `vars:`, through another step's output, or
// concatenated into a larger string has no path back to `inputs.creds` at
// all. Such a rule would print those in the clear while implying that it
// traces sensitive data — a mechanism that looks precise and is not, which is
// worse than one that is honestly blunt, because a reader trusts the one that
// looks precise (CLAUDE.md, "fail closed").
//
// So the blunt rule stays and its cost is written down here, rather than
// being rediscovered by whoever next wonders why an unrelated `false` came
// back redacted.
func sensitiveNativeValues(scope *v1.Scope, sensitiveNames map[string]bool) sensitiveInputs {
	if len(sensitiveNames) == 0 {
		return sensitiveInputs{}
	}

	// node carries whether a queued value is the declared input itself rather
	// than something nested inside it, which only the substring floor above
	// cares about.
	type node struct {
		value any
		root  bool
	}

	var out sensitiveInputs
	for name, v := range scope.GetInputs() {
		if !sensitiveNames[name] {
			continue
		}

		// A sensitive input this cannot read is withheld whole, and takes
		// every other input on the invocation with it. Skipping it — which is
		// what a `continue` here does — drops it out of the redaction set
		// silently, so *nothing* about that input is redacted anywhere in the
		// diagnostic: an allow-on-error in the one function whose job is to
		// deny (CLAUDE.md, "fail closed": a component that allows when it
		// cannot decide will eventually allow everything).
		lit := v.GetLiteral()
		if lit == nil {
			return sensitiveInputs{withholdAll: true}
		}
		native, err := literalToGo(lit)
		if err != nil {
			return sensitiveInputs{withholdAll: true}
		}

		// Sensitivity belongs to the declared input's origin, so it follows
		// every descendant when a task selects one field or one list element
		// out of a structured value. The container is kept as well: a task may
		// carry it whole.
		pending := []node{{value: native, root: true}}
		for len(pending) > 0 {
			if len(out.values)+len(pending) > maxSensitiveDescendants {
				return sensitiveInputs{withholdAll: true}
			}

			n := pending[len(pending)-1]
			pending = pending[:len(pending)-1]
			out.values = append(out.values, n.value)

			switch value := n.value.(type) {
			case string:
				// "" is excluded whatever its origin: replacing it inserts
				// the marker between every rune of the rendered line.
				if value != "" && (n.root || utf8.RuneCountInString(value) >= minSensitiveSubstringRunes) {
					out.substrings = append(out.substrings, value)
				}
			case int64, uint64, float64, bool:
				// A non-string scalar's canonical text joins the backstop:
				// `${string(inputs.pin)}` turns the number into a string the
				// typed equality can never see (Codex, #1052). fmt.Sprint is
				// the spelling both CEL's string() of an int and this
				// package's own rendering produce; a reformatted spelling
				// (padding, precision) is past what a substring set can
				// enumerate, which is the boundary the withholdAll rule
				// already draws for sets that cannot be built at all. The
				// floor and root exemption apply exactly as for a string
				// descendant.
				text := fmt.Sprint(value)
				if n.root || utf8.RuneCountInString(text) >= minSensitiveSubstringRunes {
					out.substrings = append(out.substrings, text)
				}
			case map[string]any:
				// Keys are descendants too: sensitivity belongs to the whole
				// declared value, and a map whose *keys* carry the material —
				// account ids, say — leaks through a walk that only enqueues
				// what they map to (Codex, #1052). A key rides the queue as
				// any string descendant does, so the substring floor and the
				// descendant bound apply to it unchanged.
				for name, child := range value {
					pending = append(pending, node{value: name}, node{value: child})
				}
			case []any:
				for _, child := range value {
					pending = append(pending, node{value: child})
				}
			}
		}
	}
	return out
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
			// Keys redact by exact match at every depth, not only where a
			// renderer happens to print one at the top level: a sensitive
			// struct's key — including one below the substring floor — is as
			// much the material as the value it maps to (Codex, #1052). Two
			// sensitive keys folding into one marker entry lose a pair, which
			// is the redaction doing its job, not a collision to avoid.
			if isSensitiveValue(k, sensitiveValues) {
				k = sensitiveMarker
			}
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
// the concatenated result equals the token on its own. Each sensitive
// string's exact text is replaced wherever it occurs in rendered, so the
// credential cannot survive by being wrapped in unrelated characters.
//
// It reads [sensitiveInputs.substrings] rather than the full value set on
// purpose: this replacement is textual and unanchored, so a string short
// enough to occur by accident does more damage to the diagnostic than it
// prevents. [sensitiveNativeValues] is where that line is drawn, and
// [minSensitiveSubstringRunes] is where it is argued.
func redactSensitiveSubstrings(rendered string, substrings []string) string {
	// Every match of every sensitive string is found against the ORIGINAL
	// text, the intervals merged, and the merged spans spliced out in one
	// pass. Sequential ReplaceAll cannot be ordered into correctness: with
	// containment (`abcd` in `abcdef`) the shorter-first order splits the
	// longer into `[redacted]ef`, and with intersection (`ABCDE` and `CDEFG`
	// across `ABCDEFG`) *either* order leaves the other's fragment exposed —
	// both partial leaks, the second one whatever you sort by (Codex, #1052).
	// A union of matches has no order to get wrong. One site, so the stub
	// diagnostics and the transcript share the answer.
	// Deduplicate before accounting for work. Structured sensitive inputs may
	// contain the same leaf hundreds of times; searching once has exactly the
	// same redaction result and prevents duplicates multiplying the cost.
	unique := make(map[string]struct{}, len(substrings))
	for _, s := range substrings {
		if s == "" || len(s) > len(rendered) {
			continue
		}
		unique[s] = struct{}{}
	}
	if len(rendered) != 0 && len(unique) > maxSensitiveSubstringRedactionWork/len(rendered) {
		return sensitiveMarker
	}

	// A byte mask is bounded by the rendered value itself. Recording a span
	// for every overlapping match is not: a short repeated secret in a long
	// string can otherwise materialize one allocation per byte per secret.
	redacted := make([]bool, len(rendered))
	found := false
	for s := range unique {
		coveredEnd := 0
		for from := 0; from <= len(rendered)-len(s); {
			i := strings.Index(rendered[from:], s)
			if i < 0 {
				break
			}
			start := from + i
			// Matches for one substring arrive in start order. Only mark the
			// suffix this match adds, so a long self-overlapping substring
			// cannot turn the marking pass into quadratic work.
			for i := max(start, coveredEnd); i < start+len(s); i++ {
				redacted[i] = true
			}
			coveredEnd = max(coveredEnd, start+len(s))
			found = true
			// Advance one byte, not one match length: self-overlapping
			// matches ("aa" across "aaa") must all enter the union.
			from = start + 1
		}
	}
	if !found {
		return rendered
	}

	var b strings.Builder
	for i := 0; i < len(rendered); {
		if !redacted[i] {
			b.WriteByte(rendered[i])
			i++
			continue
		}
		b.WriteString(sensitiveMarker)
		for i < len(rendered) && redacted[i] {
			i++
		}
	}
	return b.String()
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
func formatUnmatchedStubValue(name string, v any, isSecret bool, sensitive sensitiveInputs) string {
	// sensitive.withholdAll is the fail-closed case: the redaction set could
	// not be built completely, so no input can be shown to be safe and every
	// one of them is withheld — see [sensitiveNativeValues].
	if isSecret || sensitive.withholdAll || isSensitiveValue(v, sensitive.values) {
		return fmt.Sprintf("[redacted: %s]", name)
	}

	redacted := redactSensitiveTree(v, sensitive.values)

	var rendered string
	if s, ok := redacted.(string); ok {
		rendered = strconv.Quote(s)
	} else {
		rendered = fmt.Sprintf("%v", redacted)
	}

	rendered = redactSensitiveSubstrings(rendered, sensitive.substrings)

	return truncateRuneSafe(rendered, maxUnmatchedStubValueLen)
}

// formatUnmatchedStubEvalError renders a `where:` evaluation error for the
// unmatched-stub diagnostic. CEL runtime errors can quote an operand's value
// verbatim rather than just its type or position — `timestamp(inputs.token)`
// on a non-RFC3339 string reports `invalid RFC 3339 timestamp "<the string>"`,
// and an unresolved map/list index reports `no such key: <the key's value>`
// — so this text is exactly as capable of leaking a sensitive input as the
// invocation's own inputs are, and gets the same treatment (#977, filed from
// #956, which built the redaction set this reuses).
//
// [whereSource] is exempt: it is the author's own text, never a runtime
// value, so it needs no redaction and is printed as written.
//
// The tree comparison [formatUnmatchedStubValue] uses ([redactSensitiveTree]
// and [isSensitiveValue]) is not available here — an error is a string by
// the time it reaches this function, not a value with structure to walk —
// so the only tool that applies is the textual backstop,
// [redactSensitiveSubstrings]. When the redaction set could not be built at
// all ([sensitiveInputs.withholdAll]), that backstop has nothing to check
// the text against, so the fail-closed answer is to withhold the whole
// message rather than print text nothing has cleared as safe.
func formatUnmatchedStubEvalError(err error, sensitive sensitiveInputs) string {
	if sensitive.withholdAll {
		return "[redacted: where: evaluation error]"
	}
	return redactSensitiveSubstrings(err.Error(), sensitive.substrings)
}

// unmatchedStubError builds the diagnostic for a task invocation no declared
// stub answered: what the invocation actually carried, redacted where
// CLAUDE.md requires it, and every tried stub's `where:` beside whether it
// matched, so an author can see why in the failure output instead of having
// to reason it out from the workflow (#386, "diagnostics are a feature").
func unmatchedStubError(name string, declared int, native map[string]any, secretNames map[string]bool, sensitive sensitiveInputs, verdicts []stubVerdict) error {
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
			fmt.Fprintf(&b, "    %s: %s\n", n, formatUnmatchedStubValue(n, native[n], secretNames[n], sensitive))
		}
	}

	// Every input came back redacted for a reason the reader cannot otherwise
	// see: say which one, so an author staring at a diagnostic that shows
	// nothing knows it is a refusal rather than a bug (CLAUDE.md,
	// "diagnostics are a feature").
	if sensitive.withholdAll {
		b.WriteString("  (every input above is withheld: this run's sensitive inputs could not be enumerated, " +
			"so nothing on this invocation can be shown to be safe to print)\n")
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
			case v.drained > 0:
				fmt.Fprintf(&b, "\n    stub %d requires: %s   -> drained (times: %d spent)", i+1, where, v.drained)
			case v.matched:
				fmt.Fprintf(&b, "\n    stub %d requires: %s   -> true", i+1, where)
			case v.err != nil:
				fmt.Fprintf(&b, "\n    stub %d requires: %s   -> error: %s", i+1, where,
					formatUnmatchedStubEvalError(v.err, sensitive))
			default:
				fmt.Fprintf(&b, "\n    stub %d requires: %s   -> false", i+1, where)
			}
		}
	}

	return errors.New(b.String())
}

// unusedStubWarnings reports, after one case's run, every stub the case
// declared and the run never answered through — the account a green case owes
// about its own scaffolding (#926). A shipped example asserted in prose that
// "a stub whose task is never invoked is itself reported"; before this
// existed, that sentence was false, and an unused stub sat quietly forever.
//
// Warnings, not failures: an idle stub is a hole in the case's hygiene, not
// in the run, so the case's verdict is untouched and `flow test
// --fail-on-warning` is where a suite opts in to treating one as fatal — the
// `--coverage-required` shape. Two exemptions, each a legitimate pattern
// rather than a hole:
//
//   - A stub inherited from `defaults:` ([compiledStub.fromDefaults]): a
//     file-level catch-all exists precisely to be shared by cases that may
//     not all invoke its task.
//   - Nothing at all when the run failed or never reached a verdict — the
//     caller skips this whenever the run errored, since a run that stopped
//     early leaves later stubs legitimately unanswered, and this report
//     cannot tell that apart from a genuinely idle one (the same
//     unverifiable-claim honesty `expect.skipped` applies to parallel
//     branches on a failed run).
//
// The two messages tell the two situations apart, because the fix differs: a
// task never invoked is a stub aimed at nothing, while a matcher tried and
// never matched is a `where:` (or an earlier stub) that took the traffic.
func unusedStubWarnings(byTask map[string]*stubbedTask) []*v1.Diagnostic {
	type idle struct {
		ordinal int
		message string
	}
	var found []idle

	for task, stubs := range byTask {
		for i := range stubs.matchers {
			m := &stubs.matchers[i]
			if m.answered || m.fromDefaults {
				continue
			}

			target := fmt.Sprintf("task %q", task)
			if m.step != "" {
				target = fmt.Sprintf("step %q", m.step)
			}

			var message string
			switch {
			case !stubs.invoked:
				message = fmt.Sprintf(
					"stub %d (%s) was never consulted: this case invoked no %q task at all; "+
						"delete the stub, or move it under `defaults:` if it is shared boilerplate",
					m.ordinal, target, task)
			case m.whereSource != "":
				message = fmt.Sprintf(
					"stub %d (%s) never answered an invocation: its where: (%s) matched nothing this case ran; "+
						"tighten or delete it — a matcher that answers nothing asserts nothing",
					m.ordinal, target, m.whereSource)
			default:
				message = fmt.Sprintf(
					"stub %d (%s) never answered an invocation: every call it could have answered "+
						"was taken by an earlier stub; delete it, or reorder the list it falls through",
					m.ordinal, target)
			}

			found = append(found, idle{ordinal: m.ordinal, message: message})
		}
	}

	// Ordered by the author's own numbering, not by Go's map walk: the report
	// must read identically on every run.
	sort.Slice(found, func(i, j int) bool { return found[i].ordinal < found[j].ordinal })

	warnings := make([]*v1.Diagnostic, 0, len(found))
	for _, f := range found {
		warnings = append(warnings, &v1.Diagnostic{
			Field:   "stubs",
			Message: f.message,
		})
	}
	return warnings
}

// answer resolves the stub's `returns:` for one invocation, evaluating every
// ${...} it holds against that invocation's activation.
func (c compiledStub) answer(ctx context.Context, profile string, activation cel.Activation) (map[string]any, error) {
	if !c.hasReturns {
		return nil, nil
	}

	resolved, err := resolveStubValues(ctx, profile, activation, c.returns)
	if err != nil {
		return nil, fmt.Errorf("returns %w", err)
	}
	return resolved, nil
}

// resolveStubValues resolves one compiled value map — a stub's `returns:` or
// its `response:` — for one invocation, evaluating every ${...} it holds
// against that invocation's activation. One resolver for both, so the two
// stanzas cannot disagree about what a fenced value means.
func resolveStubValues(ctx context.Context, profile string, activation cel.Activation, values map[string]any) (map[string]any, error) {
	resolved := make(map[string]any, len(values))
	for name, v := range values {
		value, err := resolveReturnValue(ctx, profile, activation, v)
		if err != nil {
			return nil, fmt.Errorf("%q: %w", name, err)
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
