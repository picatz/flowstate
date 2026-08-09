package flowtest

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

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

	// Reads test.Workflow off disk, relative to the *.test.yaml itself — the
	// same rule `call:` resolves against ([WorkflowPath]'s doc) — which is
	// what makes this the bytes-vs-paths seam [runCase] shares with
	// [RunSource]: the loader is the only part of one case's run that differs
	// between a file on disk and a workflow submitted as bytes.
	for _, test := range file.Tests {
		report.Cases = append(report.Cases, runCase(&test, func() (*v1.Workflow, error) {
			workflow, _, err := flowfile.ParseFile(WorkflowPath(path, &test))
			if err != nil {
				return nil, fmt.Errorf("loading workflow %q: %w", test.Workflow, err)
			}
			return workflow, nil
		}))
	}

	return report
}

// RunSource is [RunFile] for a workflow and a `*.test.yaml` given directly as
// bytes rather than as paths — the same relationship the MCP surface's
// flowstate_run_local tool has to `flow run local`
// (see cmd/flow/mcp.go's parseFlowfileSource): same machinery, bytes instead
// of files, so an agent can rehearse a workflow's conditions, retries, and
// data flow through `flow test`'s stubbing without writing anything to disk.
//
// label names the report the way a path does for [RunFile]; a caller with no
// path of its own — the MCP tool — passes whatever it wants a reader to see
// there, such as "" or "<submitted>".
//
// workflowSource is parsed once per case, exactly as [RunFile] resolves and
// parses test.Workflow once per case, and it is the *only* workflow every
// case in testSource runs against: a case's own `workflow:` field, if it sets
// one, is accepted but never consulted here — see [LoadSource]. A workflow
// submitted this way has no file identity, so a `call:` step in it is refused
// with a diagnostic rather than resolved, the same restriction
// [flowfile.Parse] documents.
func RunSource(label string, workflowSource, testSource []byte) *v1.TestReport {
	report := &v1.TestReport{File: label}

	file, err := LoadSource(testSource)
	if err != nil {
		report.Refused = err.Error()
		return report
	}

	load := func() (*v1.Workflow, error) {
		workflow, err := flowfile.Unmarshal(workflowSource)
		if err != nil {
			return nil, fmt.Errorf("the submitted workflow source is not a valid Flowfile: %w", err)
		}
		return workflow, nil
	}

	for _, test := range file.Tests {
		report.Cases = append(report.Cases, runCase(&test, load))
	}

	return report
}

// runCase runs one test and reports its verdict. load resolves the workflow
// this case runs against — from a sibling file for [RunFile], from bytes
// submitted directly for [RunSource] — which is the entire seam between the
// two: everything below this call is oblivious to where the workflow came
// from.
func runCase(test *Test, load func() (*v1.Workflow, error)) *v1.TestCase {
	started := time.Now()
	result := &v1.TestCase{Name: test.Name}
	defer func() {
		result.Duration = durationpb.New(time.Since(started))
	}()

	stubs, err := compileStubs(test.Stubs)
	if err != nil {
		result.Error = err.Error()
		return result
	}

	// Serializes every case that swaps the process-wide default task registry
	// ([swapRegistry]) against every other such compound sequence in the
	// process — this package's own, run one at a time by [RunFile], and any
	// other package's, such as pkg/flowstate/embed's Tasks.Install. See
	// [v1.LockDefaultRegistry].
	unlockRegistry := v1.LockDefaultRegistry()
	defer unlockRegistry()

	// Swapped in before the workflow is even parsed, not just before it runs:
	// a stub may name a task this build does not otherwise register — a
	// plugin task's name — and the compiler refuses a step naming a task it
	// cannot find *at parse time*, before this function would otherwise get a
	// chance to make one up. Registering the synthetic shape first is what
	// makes stubbing a plugin task's name actually usable rather than merely
	// advertised.
	restore := swapRegistry(stubs)
	defer restore()

	workflow, err := load()
	if err != nil {
		result.Error = err.Error()
		return result
	}

	runtime, err := secretRuntime(test.Secrets)
	if err != nil {
		result.Error = err.Error()
		return result
	}

	clock := v1.NewVirtualClock(epoch)
	ctx := v1.NewContextWithClock(context.Background(), clock)
	ctx = v1.ContextWithTaskRuntime(ctx, runtime)

	// The run executes against its own registry, not the process-wide one:
	// stubs answer, everything else fails closed, and no other goroutine's
	// timing can put a real task's Fn in this run's path. See [caseRegistry].
	ctx = v1.NewContextWithRegistry(ctx, caseRegistry(stubs))

	inputs := v1.NewNamedValues(test.Inputs)

	// Resolved here, against the case's own inputs, the same way submit
	// resolves a `subject: ${inputs.x}` rule to a literal before anything is
	// enforced ([v1.ResolveSignalPolicySubjects]) — so a scripted `sender:`
	// is checked against the same literal production would check it against.
	// A bind failure here is not reported directly: [v1.RunWithInputs] below
	// performs the identical bind on the same inputs and is what the case's
	// own `expect.failed`/`expect.error_contains` are written against, so a
	// bad input surfaces as that run's ordinary failure rather than as a
	// second, differently-shaped error from this package. When binding
	// fails, policies stays nil (unpoliced) — the run is going to fail
	// before it reaches any wait step regardless, so nothing here needs to
	// enforce anything.
	var policies map[string]*v1.SignalPolicy
	if bound, bindErr := v1.BindRunInputs(workflow, inputs); bindErr == nil {
		resolved, err := v1.ResolveSignalPolicySubjects(ctx, workflow, bound)
		if err != nil {
			result.Error = fmt.Sprintf("resolving workflow %q's signal policy: %v", test.Workflow, err)
			return result
		}
		policies = resolved
	}

	// Who this case runs as, for the one question a starter answers locally:
	// what `distinct_from_starter:` compares a scripted `sender:` against.
	// [Test.Starter] names it, the way `flow run local --as-subject` names it
	// for a rehearsal on the command line; a case that names none runs as
	// nobody, exactly as every case did before that field existed.
	//
	// hasStarter true either way, and never false: a case that named no
	// starter ran as nobody, which is a known fact, because a `flow test` case has
	// no concept of "who ran this test" unless the case says: it is not a gap in a
	// record the way a durable run predating [starterMemoKey] is. Treating it
	// as unknown would make every `distinct_from_starter` policy
	// unconditionally refuse every case, including one that scripts a
	// genuinely qualifying `sender:`, the happy path this harness exists to
	// let an author exercise at all. See [v1.NewPolicedLocalSignals]'s own doc
	// comment.
	//
	// It reaches the signal policy and nothing else. `run.identity` stays
	// empty with `run.local` true, as it does for every local run including
	// `flow run local --as-subject`: a local run must never look like an
	// attested production one (eval.go's eval). See [Test.Starter].
	signals := v1.NewPolicedLocalSignals(policies, scriptedIdentity(test.Starter), true)
	ctx = v1.NewContextWithSignalWaiter(ctx, signals)

	// Hold the run's own clock participant before any scripted signal can park,
	// and tell eval (through the context) not to register a second one of its
	// own. Without this, a signal scripted for a virtual instant the run has
	// not reached yet would, in the window before the run reaches its first
	// wait, be the clock's only participant — and the clock would advance
	// straight to that instant and deliver the signal early, defeating a
	// `wait_for_signal:` timeout the signal was scripted to arrive after. Held
	// until RunWithInputs returns; released after stopScripts so signal
	// goroutines wind down first. See [v1.NewContextWithHeldRunParticipant].
	clock.Enter()
	defer clock.Leave()
	ctx = v1.NewContextWithHeldRunParticipant(ctx)

	// runFinished is closed the moment RunWithInputs returns, so a scripted
	// signal whose moment never arrives during the run does not deliver into
	// an empty room after the fact — see [scriptSignals].
	runFinished := make(chan struct{})

	stopScripts, scriptErr := scriptSignals(runFinished, clock, signals, test.Signals)
	defer stopScripts()
	if scriptErr != nil {
		result.Error = scriptErr.Error()
		return result
	}

	outputs, runErr := v1.RunWithInputs(ctx, workflow, inputs)
	close(runFinished)

	result.Failures = assertExpectation(&test.Expect, outputs, runErr)
	result.Passed = len(result.Failures) == 0

	return result
}

// swapRegistry replaces every task in [v1.DefaultRegistry] for the duration
// of one case — stubbed tasks with their stub, and every other registered
// task with a function that fails closed — and returns a func restoring what
// was there before.
//
// Every task, not just the stubbed ones. `flow test`'s whole promise is no
// network and no Temporal (#155): a task this case never bothered to stub
// must not fall through to its real Fn, because a real `http` task reaches
// the real network the instant a workflow's step reaches it — silently, on
// the one path this command exists to make sure never has to be trusted by
// accident. An omitted stub is exactly as much a test author's mistake as an
// unmatched `where:` is (see [stubbedTask.fn]), and gets the same answer: the
// case fails, naming the task, rather than doing whatever the real one does.
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

	// A stub naming a task this build does not register at all — a plugin
	// task, say — still needs a shape the *compiler* can compile a step
	// against, and the compiler reads the build's registry rather than a run's
	// (see [v1.NewContextWithRegistry] for why shapes stay a property of the
	// build). Only these synthetic names are registered globally, and only so
	// that parsing succeeds; what actually executes comes from the per-case
	// registry [caseRegistry] builds, which every real task's entry in this
	// global registry is left completely untouched by.
	for name, stub := range stubs {
		if _, already := registry.Lookup(name); already {
			continue
		}
		originals[name] = saved{existed: false}
		_ = registry.Register(v1.TaskDef{Name: name, Fn: stub.fn(name)})
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

// caseRegistry returns the registry one case executes against: every task this
// build registers, with its Fn replaced by the case's stub or by a fail-closed
// refusal, plus a synthetic definition for any stubbed name the build does not
// have.
//
// A fresh registry per case rather than a mutation of the shared one. That is
// what makes `flow test`'s central promise — no task runs for real — a
// structural property instead of a timing-dependent one: the run is handed this
// registry on its context ([v1.NewContextWithRegistry]), so nothing another
// goroutine does to [v1.DefaultRegistry] can put a real Fn in this run's path,
// and nothing this case does can leak into anyone else's. Issue #195 is what
// happens without it — a real DNS lookup escaped a supposedly stubbed http task
// under concurrency, because a swapped global is only ever swapped for a window.
func caseRegistry(stubs map[string]*stubbedTask) *v1.Registry {
	registry := v1.NewRegistry()

	// Every task this build registers, stubbed or not, which is what makes
	// "no stub, no network" a property of the whole registry rather than of
	// whichever names a case happened to mention.
	for _, def := range v1.DefaultRegistry().All() {
		replacement := def
		if stub, ok := stubs[def.Name]; ok {
			replacement.Fn = stub.fn(def.Name)
		} else {
			replacement.Fn = unstubbedTaskFn(def.Name)
		}
		_ = registry.Register(replacement)
	}

	// A stubbed name this build does not register at all — a plugin task's
	// name. A bare definition is enough for the engine to dispatch to it.
	for name, stub := range stubs {
		if _, already := registry.Lookup(name); already {
			continue
		}
		_ = registry.Register(v1.TaskDef{Name: name, Fn: stub.fn(name)})
	}

	return registry
}

// unstubbedTaskFn is what a registered task's Fn becomes for the duration of
// a case that declares no stub for it: a failure naming the task, rather than
// whatever the real one would have done.
func unstubbedTaskFn(name string) v1.TaskFunc {
	return func(ctx context.Context, inputs map[string]*v1.Value, scope *v1.Scope) (*v1.Node_Outputs, error) {
		return nil, v1.NewTaskError(name, v1.ErrorKindInvalidInput, fmt.Errorf(
			"flow test: task %q was invoked, but this case declares no stub for it; "+
				"add a `stubs:` entry naming %q — flow test never lets an unstubbed task run for real",
			name, name))
	}
}

// scriptSignals starts one goroutine per scripted signal and returns a func
// that waits for all of them to finish (a case that never reaches its own
// wait must not leave these running past it).
//
// Each goroutine is a [v1.ClockParticipant] ([v1.VirtualClock.Enter]) that
// parks on [v1.VirtualClock.After] for its own scheduled offset rather than
// forcing the clock forward itself ([v1.VirtualClock.Advance] would resolve a
// send the instant this goroutine is scheduled, in real time, regardless of
// what virtual moment the run has actually reached). Parking is what lets the
// clock's own auto-advance — the same mechanism a workload's `sleep:` and
// `wait_for_signal:` timeout already register with — decide, among every
// pending deadline across the whole case, which fires first: a signal
// scheduled after a wait's own timeout arrives *after* that wait has already
// lapsed, because the timeout's earlier deadline is what the clock advances
// to first; two signals scripted out of declaration order still arrive in
// timestamp order, because the clock always fires the earliest pending
// deadline next, never the one that happened to park first.
//
// runFinished is closed once the run itself has returned. A goroutine still
// parked on a moment later than anything the run itself ever advanced to
// stops waiting and delivers nothing — a signal scripted for after the
// workflow already finished is simply never sent, matching what a sender
// pointed at a workload that is no longer running would actually manage.
func scriptSignals(runFinished <-chan struct{}, clock *v1.VirtualClock, signals *v1.LocalSignals, scripts []SignalScript) (stop func(), err error) {
	if len(scripts) == 0 {
		return func() {}, nil
	}

	type job struct {
		name    string
		at      time.Duration
		payload map[string]any
		sender  *v1.SignalSender
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
		jobs = append(jobs, job{name: s.Name, at: at, payload: s.Payload, sender: scriptedSender(s.Sender)})
	}

	done := make(chan struct{}, len(jobs))
	for _, j := range jobs {
		clock.Enter()
		go func(j job) {
			defer clock.Leave()
			defer func() { done <- struct{}{} }()

			select {
			case <-clock.After(j.at):
			case <-runFinished:
				// The run ended before the clock ever advanced to this
				// signal's moment — nothing is left to deliver to.
				return
			}

			// The run may have finished in the instant between the clock
			// releasing this goroutine and this check — the two race
			// legitimately (advancing the clock and returning from
			// RunWithInputs happen on different goroutines) — so it is
			// checked again, non-blocking, right before the send that
			// matters: delivering into a room that is already empty is not
			// wrong, exactly, but "never delivered" should mean that outside
			// of a race this narrow too.
			select {
			case <-runFinished:
				return
			default:
			}

			_ = signals.DeliverFrom(j.name, &v1.Node_Outputs{NamedValues: v1.NewNamedValues(j.payload)}, j.sender)
		}(j)
	}

	return func() {
		for range jobs {
			<-done
		}
	}, nil
}

// scriptedSender renders a [SignalScript]'s optional `sender:` as the
// [v1.SignalSender] [v1.LocalSignals.DeliverFrom] carries: the rehearsal
// identity [v1.RehearsalSignalSender] builds when a case names one, or the
// [v1.LocalSignalSender] standing in for nobody that a script with no
// `sender:` always delivered as.
//
// # Why a rehearsal and not an attested sender (#344 slice 3)
//
// This used to build the sender by hand with `Local` left false, on the
// reading that a scripted `sender:` attests a real identity the way a durable
// signal's does. It does not: nothing authenticated it, and `flow test` has no
// server in front of it that could. [v1.SignalWaiter]'s own contract says so
// outright - every local delivery, "`flow run local --signal` or a `flow test`
// script", is marked local - and the observable consequence of getting it
// wrong is the one that matters: a gate's `sender.local` output read false,
// so a scripted sender rendered exactly like an attested production one and
// `!sender.local` stopped meaning "a server accepted this" in the one place
// an author checks it.
//
// Marked local, the shape is the same one `flow run local --signal-as-subject`
// delivers, which the durable driver refuses outright (`authorizeSignal`), and
// the policy decision is unchanged either way: [v1.SignalPolicyCheck] reads the
// identity, never the marker.
//
// AcceptedAt is left unset for the same reason, and that is a deliberate
// narrowing of what this used to report: it is when a *server* accepted a
// delivery, and no server did. A rehearsal that filled it in with the case's
// own epoch was reporting an acceptance that never happened, which is the same
// mistake in a second field.
func scriptedSender(s *ScriptedIdentity) *v1.SignalSender {
	if s == nil {
		return v1.LocalSignalSender()
	}

	return v1.RehearsalSignalSender(scriptedIdentity(s))
}

// scriptedIdentity renders a [ScriptedIdentity] as the [v1.WorkloadIdentity] a
// `signals:` policy is matched against - one conversion for both ends of that
// comparison, because `distinct_from_starter:` compares a sender's rendering
// against a starter's and two conversions could disagree about a field.
//
// A nil identity renders as an empty one rather than nil, which is what a case
// declaring no `starter:` means: this run started as nobody, recorded, rather
// than a starter nobody wrote down. See [Test.Starter] and runCase.
func scriptedIdentity(s *ScriptedIdentity) *v1.WorkloadIdentity {
	if s == nil {
		return &v1.WorkloadIdentity{}
	}

	return &v1.WorkloadIdentity{
		Subject:   s.Subject,
		Issuer:    s.Issuer,
		Namespace: s.Namespace,
		Claims:    s.Claims,
	}
}

// assertExpectation compares a run's outcome against what the case declared,
// returning one diagnostic per unmet expectation.
//
// # An unexpected failure is always a failure of the case
//
// A run erroring out is the verdict only where a case explicitly said it
// could — `expect.failed: true`. Without that, a run that errors is not a
// case with nothing to report: it is a case that asserted something about
// outputs, or steps, or nothing in particular, and never got the chance,
// because the workflow crashed, an input was invalid, or a task an author
// forgot to stub failed closed (see [unstubbedTaskFn]). Reporting nothing
// there would make a crash indistinguishable from a pass, which is the one
// failure mode a test framework may not have — a green test that should be
// red is worse than a framework that cannot run at all, because the second
// one is at least visibly broken.
func assertExpectation(want *Expectation, outputs *v1.Workflow_StepOutputs, runErr error) []*v1.Diagnostic {
	var failures []*v1.Diagnostic

	failed := runErr != nil
	switch {
	case want.Failed != nil && *want.Failed != failed:
		// An explicit expectation, in either direction, that did not hold:
		// expected to fail and did not, or expected to succeed and did not.
		failures = append(failures, &v1.Diagnostic{
			Field: "expect.failed",
			Message: fmt.Sprintf("expected the run to report failed=%t, got failed=%t (error: %v)",
				*want.Failed, failed, runErr),
		})
	case want.Failed == nil && failed:
		// No expectation named this outcome as possible, so the case gets
		// the same answer an explicit "expected to succeed" would: the run's
		// error, reported as a failure of the case rather than absorbed
		// silently because nothing was left to compare against.
		failures = append(failures, &v1.Diagnostic{
			Field: "expect.failed",
			Message: fmt.Sprintf(
				"the run failed unexpectedly, and this case's expect.failed was not set to declare that it should: %v", runErr),
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
// back out of CEL, tolerant of the numeric *type* differences the two
// encodings disagree about (YAML's decoder picks whichever integer type a
// literal happens to fit — `int`, or a narrower `uint8` for a small
// non-negative one; CEL always gives `int64`) without being tolerant of
// either an actual type mismatch (a string "1" and a number 1 are still
// different answers) or of losing precision to get there.
//
// Two integral values are compared as integers, exactly, never through
// float64 — an int64 has 63 bits of mantissa and a float64 has 52, so
// converting both sides to compare them is not a loosening, it is a second,
// silent rounding that can make two genuinely different values compare
// equal. 9007199254740992 and 9007199254740993 are both representable as
// int64 and both round to the same float64; a case pinning one and getting
// the other must fail, not pass by way of the comparison itself losing the
// difference the case exists to catch.
func looseEqual(want, got any) bool {
	if wi, ok := asInt64(want); ok {
		if gi, ok := asInt64(got); ok {
			return wi == gi
		}
		// One side is integral and the other is not representable as an
		// int64 — a genuine float on the other side, most likely — so a
		// float comparison is the honest one left, not a workaround.
		if gf, ok := asFloatOnly(got); ok {
			return float64(wi) == gf
		}
		return false
	}
	if wu, ok := asUint64(want); ok {
		// Only reached when want overflows int64 — a uint64 above 1<<63 — so
		// the comparison stays exact only when got is in the same range;
		// anything else already answers false rather than rounding through
		// a float to find out.
		gu, ok := asUint64(got)
		return ok && wu == gu
	}
	if wf, ok := asFloatOnly(want); ok {
		if gf, ok := asFloatOnly(got); ok {
			return wf == gf
		}
		if gi, ok := asInt64(got); ok {
			return wf == float64(gi)
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

// asInt64 reports whether v is an integral value representable exactly as an
// int64, and its value. Every signed width and every unsigned width narrow
// enough to always fit (uint8/uint16/uint32) qualify unconditionally; a plain
// uint or uint64 qualifies only when its value does not exceed
// [math.MaxInt64] — a large one falls through to [asUint64] instead of being
// truncated here.
func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int8:
		return int64(n), true
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case uint8:
		return int64(n), true
	case uint16:
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint:
		if uint64(n) > math.MaxInt64 {
			return 0, false
		}
		return int64(n), true
	case uint64:
		if n > math.MaxInt64 {
			return 0, false
		}
		return int64(n), true
	default:
		return 0, false
	}
}

// asUint64 reports whether v is an integral value representable as a uint64,
// and its value — reached only for a value [asInt64] refused, which today
// means an unsigned value above [math.MaxInt64].
func asUint64(v any) (uint64, bool) {
	switch n := v.(type) {
	case uint:
		return uint64(n), true
	case uint64:
		return n, true
	default:
		return 0, false
	}
}

// asFloatOnly reports whether v is a genuine floating-point value — as
// opposed to [asInt64], which never matches one, so the two together
// partition every numeric type [looseEqual] compares without an overlap that
// could pick the lossy path when an exact one was available.
func asFloatOnly(v any) (float64, bool) {
	switch n := v.(type) {
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
