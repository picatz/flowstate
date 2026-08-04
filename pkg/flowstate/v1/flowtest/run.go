package flowtest

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// registryMu serializes every case that swaps the process-wide default task
// registry ([swapRegistry]).
//
// The registry-swap pattern this package uses ([swapRegistry]'s own doc)
// mutates process-global state, the same way `allowLoopback` does in
// pkg/flowstate/v1/tests/tests.go — safe there because that suite runs its
// swaps one at a time, and safe here for the same reason: [RunFile] already
// runs a file's own cases in sequence, but nothing stops two goroutines each
// calling [RunFile] on two different files at once (`flow test`'s own
// discovery could parallelize across files; a caller embedding this package
// could too), and two cases racing on the same global registry would each see
// a mix of the other's stubs. One case's task registry mutation happens
// entirely inside this lock, so "which stub answered this call" is always
// unambiguous.
var registryMu sync.Mutex

// epoch is the moment every case's [v1.VirtualClock] starts at.
//
// Fixed and arbitrary — no case may observe *which* moment it is, only that
// time advances from it the way its `sleep:`/`wait_until:`/signal script
// says, so a case comparing an absolute timestamp out of `now` against a
// literal would be coupling to an implementation detail this value could
// change under it. It is not the Unix epoch on purpose, so a case that
// forgets this rule and asserts against `now == 0` fails immediately rather
// than by accident continuing to pass.
var epoch = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

// RunFile runs every test in a `*.test.yaml`, returning one [v1.TestReport].
//
// Cases run sequentially and each gets its own [v1.VirtualClock] and its own
// registry swap, so one case's stub or scripted signal cannot leak into the
// next — the isolation a test suite needs to be trustworthy at all.
func RunFile(path string) *v1.TestReport {
	report := &v1.TestReport{File: path}

	file, err := Load(path)
	if err != nil {
		report.Refused = err.Error()
		return report
	}

	for _, test := range file.Tests {
		report.Cases = append(report.Cases, runCase(path, &test))
	}

	return report
}

// runCase runs one test and reports its verdict.
func runCase(testFile string, test *Test) *v1.TestCase {
	started := time.Now()
	result := &v1.TestCase{Name: test.Name}
	defer func() {
		result.Duration = durationpb.New(time.Since(started))
	}()

	workflowPath := WorkflowPath(testFile, test)
	workflow, _, err := flowfile.ParseFile(workflowPath)
	if err != nil {
		result.Error = fmt.Sprintf("loading workflow %q: %v", test.Workflow, err)
		return result
	}

	stubs, err := compileStubs(test.Stubs)
	if err != nil {
		result.Error = err.Error()
		return result
	}

	registryMu.Lock()
	defer registryMu.Unlock()

	restore := swapRegistry(stubs)
	defer restore()

	clock := v1.NewVirtualClock(epoch)
	ctx := v1.NewContextWithClock(context.Background(), clock)

	signals := v1.NewLocalSignals()
	ctx = v1.NewContextWithSignalWaiter(ctx, signals)

	stopScripts, scriptErr := scriptSignals(clock, signals, test.Signals)
	defer stopScripts()
	if scriptErr != nil {
		result.Error = scriptErr.Error()
		return result
	}

	inputs := v1.NewNamedValues(test.Inputs)

	outputs, runErr := v1.RunWithInputs(ctx, workflow, inputs)

	result.Failures = assertExpectation(&test.Expect, outputs, runErr)
	result.Passed = len(result.Failures) == 0

	return result
}

// swapRegistry replaces the named tasks in [v1.DefaultRegistry] with stubs,
// and returns a func restoring what was there before.
//
// The registry-swap pattern the repo's own tests already use for the same
// reason (`allowLoopback` in pkg/flowstate/v1/tests/tests.go): the local
// driver looks tasks up through the process-wide default registry
// ([v1.LookupTask]), so replacing a task for the duration of one case means
// mutating that registry and putting it back. Test cases within one `flow
// test` invocation therefore cannot run concurrently with each other — they
// do not; [RunFile] runs them in sequence — and not concurrently with
// anything else touching the same registry in the same process.
func swapRegistry(stubs map[string]*stubbedTask) func() {
	registry := v1.DefaultRegistry()

	type saved struct {
		def     v1.TaskDef
		existed bool
	}
	originals := make(map[string]saved, len(stubs))

	for name, stubbed := range stubs {
		def, existed := registry.Lookup(name)
		originals[name] = saved{def: def, existed: existed}

		replacement := def
		if !existed {
			// A stub for a task this build does not register at all — a
			// plugin task, say — still needs a shape the engine can dispatch
			// to; a bare definition with no declared input/output schema is
			// enough for RunWithInputs to reach the stub's Fn, and stubbing a
			// name production also has no task for is the author's mistake to
			// discover from the case failing, not this package's to refuse
			// before the fact.
			replacement = v1.TaskDef{Name: name}
		}
		replacement.Fn = stubbed.fn(replacement.Name)
		_ = registry.Register(replacement)
	}

	return func() {
		for _, s := range originals {
			if s.existed {
				_ = registry.Register(s.def)
			}
			// There is no Unregister; a name that did not exist before this
			// case is left registered as a stub with nothing left to match,
			// which fails loudly (see [stubbedTask.fn]) rather than silently
			// resolving to a task that does not exist anywhere else.
		}
	}
}

// scriptSignals starts one goroutine per scripted signal, each registered as
// a [v1.ClockParticipant] so the clock cannot advance past a send it has not
// made yet, and returns a func that waits for all of them to finish (a case
// that never reaches its own wait must not leave these running past it).
func scriptSignals(clock *v1.VirtualClock, signals *v1.LocalSignals, scripts []SignalScript) (stop func(), err error) {
	if len(scripts) == 0 {
		return func() {}, nil
	}

	type job struct {
		name    string
		at      time.Duration
		payload map[string]any
	}

	jobs := make([]job, 0, len(scripts))
	for _, s := range scripts {
		at := time.Duration(0)
		if s.At != "" {
			d, err := time.ParseDuration(s.At)
			if err != nil {
				return func() {}, fmt.Errorf("signal %q: invalid `at` duration %q: %w", s.Name, s.At, err)
			}
			at = d
		}
		jobs = append(jobs, job{name: s.Name, at: at, payload: s.Payload})
	}

	done := make(chan struct{}, len(jobs))
	for _, j := range jobs {
		clock.Enter()
		go func(j job) {
			defer clock.Leave()
			clock.Advance(epoch.Add(j.at))
			_ = signals.Deliver(j.name, &v1.Node_Outputs{NamedValues: v1.NewNamedValues(j.payload)})
			done <- struct{}{}
		}(j)
	}

	return func() {
		for range jobs {
			<-done
		}
	}, nil
}

// assertExpectation compares a run's outcome against what the case declared,
// returning one diagnostic per unmet expectation.
func assertExpectation(want *Expectation, outputs *v1.Workflow_StepOutputs, runErr error) []*v1.Diagnostic {
	var failures []*v1.Diagnostic

	failed := runErr != nil
	if want.Failed != nil && *want.Failed != failed {
		failures = append(failures, &v1.Diagnostic{
			Field: "expect.failed",
			Message: fmt.Sprintf("expected the run to report failed=%t, got failed=%t (error: %v)",
				*want.Failed, failed, runErr),
		})
	}
	if want.ErrorContains != "" {
		if runErr == nil || !strings.Contains(runErr.Error(), want.ErrorContains) {
			failures = append(failures, &v1.Diagnostic{
				Field: "expect.error_contains",
				Message: fmt.Sprintf("expected the run's error to contain %q, got: %v",
					want.ErrorContains, runErr),
			})
		}
	}
	for _, step := range want.Compensated {
		marker := fmt.Sprintf("undid %q", step)
		if runErr == nil || !strings.Contains(runErr.Error(), marker) {
			failures = append(failures, &v1.Diagnostic{
				Field:   "expect.compensated",
				Value:   step,
				Message: fmt.Sprintf("expected step %q to have been compensated, but the run's account does not say so", step),
			})
		}
	}

	// A failed run has no step outputs to compare Ran/Skipped/Outputs against
	// in the ordinary sense — [v1.RunWithInputs] returns nil on failure — so
	// those assertions are skipped rather than reported as additional
	// failures once Failed has already said what needed saying.
	if failed {
		return failures
	}

	for _, step := range want.Ran {
		if _, ok := outputs.GetStepValues()[step]; !ok {
			failures = append(failures, &v1.Diagnostic{
				Step:    step,
				Field:   "expect.ran",
				Message: fmt.Sprintf("expected step %q to have run, but it produced no recorded outputs", step),
			})
		}
	}
	for _, step := range want.Skipped {
		if _, ok := outputs.GetStepValues()[step]; ok {
			failures = append(failures, &v1.Diagnostic{
				Step:    step,
				Field:   "expect.skipped",
				Message: fmt.Sprintf("expected step %q to have been skipped, but it produced recorded outputs", step),
			})
		}
	}

	if want.Outputs != nil {
		failures = append(failures, compareOutputs(want.Outputs, outputs.GetRunOutputs().GetValues())...)
	}

	return failures
}

// compareOutputs checks a workflow's declared `outputs:` against what a case
// expected, both directions: a missing key and an extra one are both
// reported, because a case naming three outputs and getting a fourth
// unexpected one is a case whose workflow no longer matches its own promise.
func compareOutputs(want map[string]any, got map[string]*v1.Value) []*v1.Diagnostic {
	var failures []*v1.Diagnostic

	for name, wantVal := range want {
		gotVal, ok := got[name]
		if !ok {
			failures = append(failures, &v1.Diagnostic{
				Field:   "expect.outputs",
				Value:   name,
				Message: fmt.Sprintf("expected output %q, but the run produced none", name),
			})
			continue
		}
		if errKind := gotVal.GetError(); errKind != nil {
			failures = append(failures, &v1.Diagnostic{
				Field:   "expect.outputs",
				Value:   name,
				Message: fmt.Sprintf("output %q: expected %v, but evaluating it failed: %s", name, wantVal, errKind.GetMessage()),
			})
			continue
		}
		gotNative, err := literalToGo(gotVal.GetLiteral())
		if err != nil {
			failures = append(failures, &v1.Diagnostic{
				Field:   "expect.outputs",
				Value:   name,
				Message: fmt.Sprintf("output %q: could not compare: %v", name, err),
			})
			continue
		}
		if !looseEqual(wantVal, gotNative) {
			failures = append(failures, &v1.Diagnostic{
				Field:   "expect.outputs",
				Value:   name,
				Message: fmt.Sprintf("output %q: expected %v, got %v", name, wantVal, gotNative),
			})
		}
	}

	names := make([]string, 0, len(got))
	for name := range got {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if _, expected := want[name]; !expected {
			failures = append(failures, &v1.Diagnostic{
				Field:   "expect.outputs",
				Value:   name,
				Message: fmt.Sprintf("unexpected output %q, which expect.outputs does not name", name),
			})
		}
	}

	return failures
}

// literalToGo converts a resolved CEL literal into a plain Go value, the same
// conversion [v1.NewValue] performs in reverse when a test file's YAML
// becomes a stub's canned outputs.
func literalToGo(v *expr.Value) (any, error) {
	switch kind := v.GetKind().(type) {
	case nil, *exprValueNull:
		return nil, nil
	case *exprValueString:
		return kind.StringValue, nil
	case *exprValueInt64:
		return kind.Int64Value, nil
	case *exprValueUint64:
		return kind.Uint64Value, nil
	case *exprValueDouble:
		return kind.DoubleValue, nil
	case *exprValueBool:
		return kind.BoolValue, nil
	case *exprValueBytes:
		return kind.BytesValue, nil
	case *exprValueList:
		list := make([]any, 0, len(kind.ListValue.GetValues()))
		for i, element := range kind.ListValue.GetValues() {
			native, err := literalToGo(element)
			if err != nil {
				return nil, fmt.Errorf("element %d: %w", i, err)
			}
			list = append(list, native)
		}
		return list, nil
	case *exprValueMap:
		object := make(map[string]any, len(kind.MapValue.GetEntries()))
		for _, entry := range kind.MapValue.GetEntries() {
			name := entry.GetKey().GetStringValue()
			native, err := literalToGo(entry.GetValue())
			if err != nil {
				return nil, fmt.Errorf("key %q: %w", name, err)
			}
			object[name] = native
		}
		return object, nil
	default:
		return nil, fmt.Errorf("a %T cannot be compared", kind)
	}
}

// looseEqual compares a YAML-decoded expectation against a value that came
// back out of CEL, tolerant of the numeric type differences the two
// encodings disagree about (YAML gives an integer literal `int`; CEL gives
// `int64`) without being tolerant of an actual type mismatch — a string
// "1" and a number 1 are still different answers.
func looseEqual(want, got any) bool {
	if wf, ok := asFloat(want); ok {
		if gf, ok := asFloat(got); ok {
			return wf == gf
		}
		return false
	}

	switch w := want.(type) {
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok || len(w) != len(g) {
			return false
		}
		for k, wv := range w {
			gv, ok := g[k]
			if !ok || !looseEqual(wv, gv) {
				return false
			}
		}
		return true
	case []any:
		g, ok := got.([]any)
		if !ok || len(w) != len(g) {
			return false
		}
		for i := range w {
			if !looseEqual(w[i], g[i]) {
				return false
			}
		}
		return true
	default:
		return want == got
	}
}

// asFloat reports whether v is one of the numeric types [looseEqual] treats
// as comparable across encodings, and its value as a float64.
func asFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}

// The concrete *expr.Value_* types, aliased so [literalToGo]'s switch reads
// against the same names eval_task_http.go's literalToNative does, without
// this package reaching into that unexported function directly.
type (
	exprValueNull   = expr.Value_NullValue
	exprValueString = expr.Value_StringValue
	exprValueInt64  = expr.Value_Int64Value
	exprValueUint64 = expr.Value_Uint64Value
	exprValueDouble = expr.Value_DoubleValue
	exprValueBool   = expr.Value_BoolValue
	exprValueBytes  = expr.Value_BytesValue
	exprValueList   = expr.Value_ListValue
	exprValueMap    = expr.Value_MapValue
)
