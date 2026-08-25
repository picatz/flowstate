package flowtest

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/cel-go/common/types/ref"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/dst"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/nearest"
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

// RunOptions is what a caller may vary about a suite run. The zero value is
// the run every door has always performed: every case, written order only,
// report labelled by whatever the door knows.
type RunOptions struct {
	// Label names the report ([v1.TestReport.File]) — a path for a file on
	// disk, "<submitted>" or similar for bytes, whatever a Go caller wants a
	// reader to see for a built [File].
	Label string

	// Budget is the seeded-schedule exploration to run beyond the
	// written-order pass (issue #800). The zero budget explores nothing.
	Budget dst.Budget

	// Select filters which cases run, by name; nil runs every case. A case
	// filtered out is not run, not reported, and counted in
	// [RunResult.Filtered] — the number a caller's own output must surface,
	// because a green over a subset must never read as the file's green
	// (issue #929).
	Select func(name string) bool

	// Debugger, when set, holds each case's run at every step boundary so a
	// session can drive it (#928 slice 1). Installed on the run's context, so
	// loop bodies, branches and called workflows all inherit it.
	//
	// A debugger that also implements [v1.RunObserver] is given the account
	// as well, alongside the transcript's own recorder — capability
	// discovery rather than a second field, because the two are one object
	// in the only implementation that exists ([flowdebug.Session]) and an
	// implementer that only wants to pause should not have to write three
	// empty methods to say so.
	//
	// Interactive by nature: nothing here bounds how long a run is held, and
	// the caller that sets this owns that decision. `flow test --debug`
	// refuses to set it for more than one case at a time.
	Debugger v1.Debugger

	// skipTranscript disables the case account entirely. Set by
	// [RunSourceContext] and nothing else: that door discards
	// [RunResult.Transcripts], and its callers are whoever holds a token on
	// a serving surface — so recording there would be memory an untrusted
	// submission shapes, retained for an account nobody will ever read. The
	// byte and event bounds still exist for the doors that do record; this
	// removes the resource from the one door where the submitter is not the
	// reader.
	skipTranscript bool
}

// RunResult is everything one suite run produced.
type RunResult struct {
	Report    *v1.TestReport
	Coverage  []*Coverage
	Schedules *ScheduleReport

	// Transcripts holds each case's rendered account (#929 slice 2),
	// parallel to Report.Cases: Transcripts[i] is what case i's run did,
	// step by step, redacted through the same machinery the stub
	// diagnostics use. Nil for a case that never reached a run. Rendered
	// lines rather than a schema message on purpose — where a machine
	// transcript lives, if anywhere, is #923's decision.
	Transcripts [][]TranscriptLine

	// Filtered is how many cases [RunOptions.Select] excluded from this run.
	Filtered int
}

// RunFile runs every test in a `*.test.yaml`, returning one [v1.TestReport].
//
// Cases run sequentially and each gets its own [v1.VirtualClock] and its own
// registry swap, so one case's stub or scripted signal cannot leak into the
// next — the isolation a test suite needs to be trustworthy at all.
func RunFile(path string) *v1.TestReport {
	report, _ := RunFileWithCoverage(path)
	return report
}

// Run runs a loaded [File]'s cases — the door for a suite built or loaded in
// Go rather than named by path (issue #930; this is the [Run] the [File.Tests]
// doc has always cited). dir is what a case's `workflow:` and a trigger case's
// `payload:` resolve against, the way those paths resolve against the test
// file's own directory when one exists; empty refuses any case that needs a
// path resolved, exactly as [RunSourceContext]'s byte-born cases are refused.
func Run(ctx context.Context, file *File, dir string, opts RunOptions) RunResult {
	// Tables expand before defaults fold, the same order [Load] applies to a
	// parsed file and for the same reason: a row beats its entry, and an
	// entry beats `defaults:`. A built file skipping expansion would run a
	// table's template and silently never its rows — the parsed-vs-built
	// divergence #1015 is about, wearing #924's clothes. On a file Load
	// already expanded this is a no-op: no surviving entry carries `cases:`.
	expanded, err := expandTableEntries(file.Tests)
	if err != nil {
		return RunResult{Report: &v1.TestReport{File: opts.Label, Refused: err.Error()}}
	}
	shallow := *file
	shallow.Tests = expanded
	file = (&shallow).withDefaultsApplied()

	return runSuite(ctx, file, opts, func(test *Test) (loader, string) {
		identity := workflowPathIn(dir, test)

		// The refusal the doc above promises, made real: with no directory, a
		// relative path would otherwise fall through [filepath.Join]'s
		// identity on an empty prefix and resolve against the process working
		// directory — so the same suite would silently run whatever file the
		// caller's cwd happens to hold, or fail there, depending on where the
		// test binary ran from. A path-shaped fact must never depend on cwd;
		// refused per case, since a sibling case naming absolute paths is
		// fine. Reported by Codex on picatz/flowstate#1015.
		if dir == "" {
			if err := pathlessRefusal(test); err != nil {
				return loader{
					load:      func() (*v1.Workflow, error) { return nil, err },
					positions: func() *flowfile.Positions { return nil },
				}, identity
			}
		}

		var positions *flowfile.Positions
		return loader{
			load: func() (*v1.Workflow, error) {
				workflow, parsed, err := flowfile.ParseFile(identity)
				if err != nil {
					return nil, fmt.Errorf("loading workflow %q: %w", test.Workflow, err)
				}
				positions = parsed
				return workflow, nil
			},
			positions:    func() *flowfile.Positions { return positions },
			deliveryPath: deliveryPathIn(dir, test),
		}, identity
	})
}

// loader is one case's way of producing the workflow it runs against, plus
// the two facts that travel with it: the parse positions (nil for bytes,
// which have none to record) and where a trigger case's stored delivery
// lives (empty where there is no directory to resolve one against).
type loader struct {
	load         func() (*v1.Workflow, error)
	positions    func() *flowfile.Positions
	deliveryPath string
}

// runSuite is the one loop every door shares: [Run] and
// [RunFileUnderSchedules] for files, [RunSourceContext] for bytes. It owns
// case selection, coverage and schedule accounting, and attaching the
// coverage to the report — in one place, so the machine document cannot
// disagree with the richer Go-side result about what was measured.
func runSuite(ctx context.Context, file *File, opts RunOptions, loaderFor func(*Test) (loader, string)) RunResult {
	report := &v1.TestReport{File: opts.Label}

	var allowUnreached map[string]string
	if file.Coverage != nil {
		allowUnreached = file.Coverage.AllowUnreached
	}
	coverage := newCoverageAccumulator(allowUnreached)
	schedules := newScheduleAccumulator(opts.Budget)

	// Installed once for the whole suite rather than per case: the debugger
	// is a caller's object with a caller's lifetime, and runCase reads it
	// back off the context to decide whether it also wants the account.
	if opts.Debugger != nil {
		ctx = v1.NewContextWithDebugger(ctx, opts.Debugger)
	}

	filtered := 0
	var transcripts [][]TranscriptLine
	transcriptBudget := newSuiteTranscriptBudget()
	for _, test := range file.Tests {
		if opts.Select != nil && !opts.Select(test.Name) {
			filtered++
			continue
		}

		if stopped := caseStoppedBefore(ctx, &test); stopped != nil {
			report.Cases = append(report.Cases, stopped)
			transcripts = append(transcripts, nil)

			continue
		}

		l, identity := loaderFor(&test)

		result, spec, transcript, account := schedules.run(ctx,
			func(ctx context.Context) (*v1.TestCase, *v1.Workflow, *v1.Workflow_StepOutputs, []TranscriptLine, error) {
				// The account is recorded only for runs whose account is
				// kept. Under an exploring budget, [scheduleAccumulator.run]
				// retains the written-order baseline's and discards every
				// seeded one, so recording through ten thousand seeds would
				// clone and render for nothing — the discriminator is the
				// same one schedules.run keeps the baseline by. A budget that
				// explores nothing keeps its single run's account whatever
				// scheduler the *caller* put on the context ([v1.AdversarialOrder],
				// a pinned seed installed by hand), so no scheduler check
				// applies there: suppression is only ever about exploratory
				// invocations (Codex, #1052, twice).
				record := !opts.skipTranscript &&
					(!schedules.explores || v1.SchedulerFromContext(ctx) == v1.WrittenOrder)
				return runCase(ctx, &test, l.deliveryPath, l.load, record, file.Vars)
			})
		report.Cases = append(report.Cases, result)
		transcripts = append(transcripts, transcriptBudget.take(account))
		coverage.observe(identity, spec, transcript, l.positions())
	}

	out := RunResult{
		Report:      report,
		Coverage:    coverage.result(),
		Schedules:   schedules.result(),
		Transcripts: transcripts,
		Filtered:    filtered,
	}
	// Attached here, for every door, so the whole document renders through
	// protojson wherever it ends up — the CLI's machine modes and the MCP
	// tool alike carry the same account (issue #931; before this, the MCP
	// door had no accumulator at all and answered `coverage: []` forever
	// while docs/reference/mcp.md promised the CLI's own report).
	for _, c := range out.Coverage {
		report.Coverage = append(report.Coverage, c.Report())
	}
	// The schedule account rides the same way (issue #931's other half):
	// unset when nothing explored, so a report from a default run is the
	// document it always was.
	if out.Schedules != nil {
		report.Schedules = out.Schedules.Report()
	}
	return out
}

// withDefaultsApplied folds the file's `defaults:` into every case, exactly
// as [Load] folds them for a parsed file (#416) — the normalization a [File]
// built in Go otherwise never receives, which made the same logical suite
// behave differently depending on whether it was constructed or parsed:
// inherited inputs, stubs and signal senders were silently absent from the
// built one. Reported by Codex on picatz/flowstate#1015.
//
// A shallow copy with merged Tests, never a mutation of the caller's File — a
// caller may hold and re-run it. On a file [Load] already merged, folding
// again changes nothing, which is a property of [mergeDefaults]'s own rules
// rather than luck: inputs re-copy to the values the first fold chose, a
// default stub whose target key is already present is skipped (and the
// inherited copies carry those keys), and a sender fills only signals that
// have none. TestRunDoesNotDoubleMergeALoadedFile pins it.
func (f *File) withDefaultsApplied() *File {
	if f.Defaults == nil {
		return f
	}

	merged := *f
	merged.Tests = make([]Test, len(f.Tests))
	for i := range f.Tests {
		merged.Tests[i] = mergeDefaults(f.Defaults, f.Tests[i])
	}
	return &merged
}

// pathlessRefusal is what stops one case from running under [Run] with no
// directory: a relative `workflow:` or trigger `payload:` has nothing to
// resolve against, and the two honest answers are an absolute path or a
// directory. Nil when every path the case names is absolute (or absent).
func pathlessRefusal(test *Test) error {
	if !filepath.IsAbs(test.Workflow) {
		return fmt.Errorf("workflow %q is a relative path and this run has no directory to resolve it against; "+
			"pass dir to Run (flowtesting callers: WithDir), or make the path absolute", test.Workflow)
	}
	if test.Trigger != nil && test.Trigger.Payload != "" && !filepath.IsAbs(test.Trigger.Payload) {
		return fmt.Errorf("trigger payload %q is a relative path and this run has no directory to resolve it against; "+
			"pass dir to Run (flowtesting callers: WithDir), or make the path absolute", test.Trigger.Payload)
	}
	return nil
}

// workflowPathIn and deliveryPathIn are [WorkflowPath] and [DeliveryPath] for
// a caller holding the directory rather than the test file whose directory it
// is — same rule, same one spelling, minus the Dir().
func workflowPathIn(dir string, test *Test) string {
	if filepath.IsAbs(test.Workflow) {
		return test.Workflow
	}
	return filepath.Join(dir, test.Workflow)
}

func deliveryPathIn(dir string, test *Test) string {
	if test.Trigger == nil || test.Trigger.Payload == "" {
		return ""
	}
	if filepath.IsAbs(test.Trigger.Payload) {
		return test.Trigger.Payload
	}
	return filepath.Join(dir, test.Trigger.Payload)
}

// RunFileWithCoverage is [RunFile] and, alongside the report, the branch
// coverage the file's cases achieved (issue #420): which of a targeted
// workflow's steps at least one case ran, and which no case ever reached.
//
// One [Coverage] per workflow the file's cases target, sorted by workflow
// identity. A `*.test.yaml` usually tests one workflow and this is a single
// entry, but each case names its own `workflow:`, so a file may target several,
// and coverage is kept separate per workflow so a step one workflow reaches
// never masks the same step id left unreached in another (Finding 3).
//
// Nil for a file that produced no case with a compiled workflow to account for
// (a refused file, or one whose every case failed to compile), where a "0/0
// steps reached" line would say less than nothing. See [Coverage].
func RunFileWithCoverage(path string) (*v1.TestReport, []*Coverage) {
	report, coverage, _ := RunFileUnderSchedules(context.Background(), path, dst.Budget{})

	return report, coverage
}

// RunFileUnderSchedules is [RunFileWithCoverage] with two additions the command
// line reaches and nothing else needs: a caller-supplied context, and a budget
// of seeded schedules to run every case under (issue #800, #477 slice 2).
//
// The zero budget explores nothing, which is what [RunFileWithCoverage] passes
// and what `flow test` does unless `--seeds` says otherwise: each case runs once,
// under [v1.WrittenOrder], exactly as it always has. A budget that explores runs
// each case once more per seed and returns a [ScheduleReport] beside the other
// two results; the report and the coverage still describe the written-order run
// alone, so a verdict never depends on which seeds were drawn. See
// [scheduleAccumulator.run].
//
// The context is the same bound [RunSourceContext] documents, for the same
// reason and now with a second one: a case whose virtual clock can never advance
// has nothing to end it, and `--seeds N` multiplies whatever that costs by N.
func RunFileUnderSchedules(ctx context.Context, path string, budget dst.Budget) (*v1.TestReport, []*Coverage, *ScheduleReport) {
	result := RunPath(ctx, path, RunOptions{Budget: budget})
	return result.Report, result.Coverage, result.Schedules
}

// RunPath is [Run] for a suite named by path rather than already loaded: load
// the `*.test.yaml`, then run it with everything the options say, against the
// file's own directory — which is what a case's `workflow:` and `payload:`
// resolve against, the same rule `call:` resolves against ([WorkflowPath]'s
// doc). An empty [RunOptions.Label] defaults to the path. A file that does not
// load returns a refused report, the same one every path door has always
// produced, with nil coverage and schedules — no case was ever reached.
//
// This is the door `flow test` walks through (with [RunOptions.Select] behind
// `--run`, and [RunOptions.Budget] behind `--seeds`); [RunFileUnderSchedules]
// is this with only a budget to say.
func RunPath(ctx context.Context, path string, opts RunOptions) RunResult {
	if opts.Label == "" {
		opts.Label = path
	}

	file, err := Load(path)
	if err != nil {
		return RunResult{Report: &v1.TestReport{File: opts.Label, Refused: err.Error()}}
	}

	return Run(ctx, file, filepath.Dir(path), opts)
}

// caseStoppedBefore reports the case a caller's context ended before it could
// start, or nil while there is still budget to run one.
//
// Checked before the case rather than only inside the run, because most of what
// a case costs happens before its context is ever consulted: compiling its
// stubs, parsing the workflow again, binding the stubs against it. A file may
// declare [MaxTestsPerFile] cases, so a deadline that expired during an early
// one would otherwise be followed by hundreds of expensive parses — and on a
// serving surface those run while the caller's whole budget is already spent
// and, worse, while it still holds whatever lock it took (see
// cmd/flow/mcpserve.go's registry guard). The bound has to stop the *work*, not
// only the execution. Reported by Codex on picatz/flowstate#807.
func caseStoppedBefore(ctx context.Context, test *Test) *v1.TestCase {
	err := ctx.Err()
	if err == nil {
		return nil
	}

	return &v1.TestCase{
		Name:  test.Name,
		Error: fmt.Sprintf("not run: the run was stopped before this case started (%v)", err),
	}
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
	return RunSourceContext(context.Background(), label, workflowSource, testSource)
}

// RunSourceContext is [RunSource] with a caller-supplied context, which every
// case's run is started from.
//
// It exists because a run here can block forever and nothing outside it could
// say stop. The virtual clock advances when every participant is parked, so a
// `wait_for_signal:` with no timeout and no scripted signal has no deadline to
// advance to: the case never completes, and neither does the call that asked
// for it. On a developer's machine that is a hung `flow test` somebody
// interrupts. On a server whose callers are whoever holds a token — see
// cmd/flow/mcpserve.go — it is a request that never returns and a goroutine
// that never exits, arranged from a legal Flowfile, which is the shape
// CLAUDE.md's "bound anything that consumes untrusted input" names. Reported
// by Codex on picatz/flowstate#807.
//
// A context that is already done makes every case fail rather than run, with
// the reason on the case; [RunSource] and [RunFile] pass a background context
// and are unchanged by this existing.
func RunSourceContext(ctx context.Context, label string, workflowSource, testSource []byte) *v1.TestReport {
	return RunSourceWith(ctx, label, workflowSource, testSource, RunOptions{}).Report
}

// RunSourceWith is [RunSourceContext] with the caller's own [RunOptions] —
// the door a front that needs more than a verdict comes through, and today
// that is the MCP debug adapter (#928 slice 3), which needs [RunOptions.Select]
// to name one case and [RunOptions.Debugger] to drive it.
//
// Label is this door's to set, not the caller's: it is the report identity and
// the coverage identity, and both are the label argument. Everything else the
// caller brings. skipTranscript stays on for the reason it was set here at all
// — the submitter is not the reader on this door, so the case account is
// memory an untrusted submission shapes for nobody. A debugging session hears
// the same account directly through [v1.RunObserver] instead, which is what
// makes that discard cost a debug front nothing.
func RunSourceWith(ctx context.Context, label string, workflowSource, testSource []byte, opts RunOptions) RunResult {
	file, err := LoadSource(testSource)
	if err != nil {
		return RunResult{Report: &v1.TestReport{File: label, Refused: err.Error()}}
	}

	// The loader parses the one submitted workflow per case, with no delivery
	// path (bytes have no directory to resolve a fixture against — which is
	// why [checkTrigger] refuses a trigger case in this shape) and no parse
	// positions (bytes have no file for a position to point into; a switch
	// arm's coverage carries line zero here, as the schema documents). The
	// label is the coverage identity, since every case runs the same
	// submitted workflow. Coverage riding the report through [runSuite] is
	// what makes this door answer with the same account the CLI's does
	// (issue #931).
	opts.Label = label
	opts.skipTranscript = true

	return runSuite(ctx, file, opts, func(*Test) (loader, string) {
		return loader{
			load: func() (*v1.Workflow, error) {
				workflow, err := flowfile.Unmarshal(workflowSource)
				if err != nil {
					return nil, fmt.Errorf("the submitted workflow source is not a valid Flowfile: %w", err)
				}
				return workflow, nil
			},
			positions:    func() *flowfile.Positions { return nil },
			deliveryPath: "",
		}, label
	})
}

// runCase runs one test and reports its verdict. load resolves the workflow
// this case runs against — from a sibling file for [RunFile], from bytes
// submitted directly for [RunSource] — which is the entire seam between the
// two: everything below this call is oblivious to where the workflow came
// from.
// The spec and transcript it returns beside the case are what branch coverage
// is computed from ([Coverage]): spec is the workflow this case compiled and
// ran (nil when it never compiled one), and transcript is the step outputs the
// run produced, the partial one when the run failed ([v1.PartialTranscript]),
// nil only when the case never reached a run at all. Both are the same values the
// verdict itself was reached against, so coverage counts a step reached on
// exactly the evidence `expect.ran` counts on.
// deliveryPath is where a `trigger:` case's stored delivery lives, already
// resolved against the test file's own directory ([DeliveryPath]); empty for every
// other case and for [RunSource], which has no directory at all.
// runErr is the run's own failure, distinct from the verdict: a case that
// declares `expect.failed: true` passes while its run errored, and schedule
// exploration compares the error text because `expect.error_contains` is read
// out of it ([caseObservables]). Nil for a case that never reached a run.
//
// # The schedule this case runs under
//
// base decides it. Everything below builds on base with [context.WithValue], so
// a [v1.Scheduler] a caller put there reaches [v1.RunWithInputs] and answers the
// two questions the local driver asks — `parallel:` branch order and where an
// `async:` step's work happens. Nothing here installs one, deliberately: a
// scheduler is a fact about the run somebody asked for, exactly as the clock
// this function *does* install is a fact about the harness, and inventing one
// here would take the choice away from the only caller with a reason to make it
// ([RunFileUnderSchedules]). With no scheduler on base the driver takes
// [v1.WrittenOrder], which is what every `flow test` case has always run under.
func runCase(base context.Context, test *Test, deliveryPath string, load func() (*v1.Workflow, error), record bool, vars map[string]any) (result *v1.TestCase, spec *v1.Workflow, transcript *v1.Workflow_StepOutputs, account []TranscriptLine, runErr error) {
	started := time.Now()
	result = &v1.TestCase{Name: test.Name}
	defer func() {
		result.Duration = durationpb.New(time.Since(started))
	}()

	// The case's own account (#929 slice 2), rendered on every exit path
	// once the run's clock exists — a case that never got that far has
	// nothing to account for.
	var recorder *runRecorder
	defer func() {
		if recorder != nil {
			account = recorder.render()
		}
	}()

	compiled, err := compileStubs(test.Stubs)
	if err != nil {
		result.Error = err.Error()
		return
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
	// advertised. Only task-form stubs can name a missing task; a step-form
	// stub names a step whose task the compiler already knows.
	restore := swapRegistry(stubTaskNames(compiled))
	defer restore()

	workflow, err := load()
	if err != nil {
		result.Error = err.Error()
		return
	}
	// Reported to the caller for coverage: the workflow this case compiled is
	// what its steps are counted against, even when the run below then fails.
	spec = workflow

	// Resolved against the compiled workflow, not the file alone: a step-form
	// stub names a step id, and this is where that id becomes the task it
	// invokes, or an `unknown step` diagnostic with a did-you-mean, refused
	// before the run rather than surfacing as an unmatched-stub failure a whole
	// virtual day later.
	stubs, err := bindStubs(compiled, workflow)
	if err != nil {
		result.Error = err.Error()
		return
	}

	// Refused before the run for the same reason a bad stub target is: an
	// expectation naming a step the workflow does not have is the file
	// disagreeing with the workflow, and a whole virtual day of execution
	// cannot make the claim checkable. See [checkExpectationNames].
	if err := checkExpectationNames(&test.Expect, workflow); err != nil {
		result.Error = err.Error()
		return
	}

	runtime, err := secretRuntime(test.Secrets)
	if err != nil {
		result.Error = err.Error()
		return
	}

	clock := v1.NewVirtualClock(epoch)
	// base rather than context.Background(): whatever bound the caller put on
	// this run is the only thing that can end a case the virtual clock cannot
	// advance past. See [RunSourceContext].
	ctx := v1.NewContextWithClock(base, clock)
	ctx = v1.ContextWithTaskRuntime(ctx, runtime)

	// The transcript's recorder: the engine reports through [v1.RunObserver],
	// the stub functions find the same recorder on the context, and the
	// scripted-signal goroutines are handed it directly. Not built at all
	// where the account would be discarded ([RunOptions].skipTranscript) —
	// no observer on the context means the engine clones nothing either.
	if record {
		recorder = newRunRecorder(clock)
		ctx = contextWithRunRecorder(ctx, recorder)
		// What the transcript may honestly call a switch decision — from the
		// spec, never inferred from an output's name. See [runRecorder.noteSwitches].
		recorder.noteSwitches(workflow.GetSteps())
	}

	// Who hears the engine's account. Usually the transcript's recorder and
	// nobody else; under a debugging session that also observes, both, because
	// a context holds one observer and a session that could not say what a
	// step produced would be a debugger you cannot debug with. Either may be
	// absent — a discarded account records nothing, and a session is rare —
	// and where both are, nothing is installed at all, which is what keeps the
	// engine cloning nothing for a run nobody is listening to.
	if observer := observerFor(ctx, recorder); observer != nil {
		ctx = v1.NewContextWithRunObserver(ctx, observer)
	}

	// The run executes against its own registry, not the process-wide one:
	// stubs answer, everything else fails closed, and no other goroutine's
	// timing can put a real task's Fn in this run's path. See [caseRegistry].
	ctx = v1.NewContextWithRegistry(ctx, caseRegistry(stubs, v1.SensitiveInputNames(workflow)))

	inputs := v1.NewNamedValues(test.Inputs)

	// A trigger case's inputs are not stated, they are produced: the stored
	// delivery is mapped through the workflow's own `with:` expressions by the
	// same function a live receiver will call. Done here, before the clock, the
	// registry and the signal machinery are built, because a refused delivery
	// produces no run at all and none of that would be used.
	// How this case's run started, which every case has: a manual start unless
	// the case says otherwise, exactly as `flow run local` reports one. That
	// default is what makes `if: ${trigger.kind == "manual"}` a branch a case can
	// reach without arranging anything, and its opposite reachable by stating one
	// line — the whole point of this being settable at all.
	trigger := v1.NewManualTriggerContext("")

	if test.Trigger != nil && !test.Trigger.Replays() {
		// Stated outright, with no delivery involved. Checked against the closed
		// set of kinds when the file loaded ([checkTriggerContext]).
		trigger = test.Trigger.Context()
	}

	if test.Trigger != nil && test.Trigger.Replays() {
		mapped, deliveryID, failures, err := replayDelivery(test, deliveryPath, workflow)
		if err != nil {
			result.Error = err.Error()
			return
		}
		if len(failures) > 0 || mapped == nil {
			// Either the delivery was refused — which is a verdict, not an error,
			// and the whole of what a `refused: true` case asserts — or it was
			// accepted by a case that expected otherwise. No run happens either way.
			result.Failures = failures
			result.Passed = len(failures) == 0
			return
		}
		inputs = mapped

		// Derived from the replay rather than stated by the case, so a case
		// asserting on `${trigger.delivery_id}` asserts against the value the
		// receiver would really have recorded — [v1.WebhookDeliveryID] over the
		// same evaluated key, which is the function the receiver itself calls.
		//
		// The principal is left empty, honestly: a live receiver records the tenant
		// the delivery was admitted under, and `flow test` has no tenant. Filling
		// it with something invented is what would make a rehearsal lie.
		trigger = v1.NewWebhookTriggerContext(test.Trigger.Webhook, "", deliveryID)
	}

	ctx = v1.NewContextWithTrigger(ctx, trigger)

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
	var bound map[string]*v1.Value
	var sensitive sensitiveInputs
	if b, bindErr := v1.BindRunInputs(workflow, inputs); bindErr == nil {
		bound = b
		resolved, err := v1.ResolveSignalPolicySubjects(ctx, workflow, bound)
		if err != nil {
			result.Error = fmt.Sprintf("resolving workflow %q's signal policy: %v", test.Workflow, err)
			return
		}
		policies = resolved

		// The redaction set, built from the same bound inputs and the same
		// `sensitive:` declarations the stub diagnostics read
		// ([sensitiveNativeValues]) — one set, shared by the transcript's
		// recorder and the check witnesses below, so what one surface
		// refuses to print no other surface prints. A case whose bind fails
		// records no step events carrying input-derived values: the run
		// fails at the same bind before any step runs.
		sensitive = sensitiveNativeValues(&v1.Scope{Inputs: bound}, v1.SensitiveInputNames(workflow))
	}

	// The case's own `secrets:` plaintext joins the set (Codex, #1052):
	// [resolveSecretInputs] exposes those values to stub expressions, so a
	// stub can echo one into a step's outputs, and the rule is absolute — a
	// resolved secret never prints, whatever path it took (CLAUDE.md,
	// "secrets never enter workflow history"). Both lists, the pair every
	// declared sensitive value gets: the value comparison catches the whole,
	// the substring backstop catches "Bearer " + secret. An empty value is
	// skipped — replacing the empty string would mark every position in
	// every line while protecting nothing.
	for _, value := range test.Secrets {
		if value == "" {
			continue
		}
		sensitive.values = append(sensitive.values, value)
		sensitive.substrings = append(sensitive.substrings, value)
	}
	// A debugging session prints what the transcript prints, so it withholds
	// what the transcript withholds (Codex, #1109). Capability-discovered the
	// way the autopsy is: a debugger that does not implement it simply prints
	// whatever it was going to, and the only implementation that exists is
	// [flowdebug.Session]. Installed per case, because `sensitive` is this
	// case's — its inputs, its secrets — and cleared afterward so a session
	// driving a second case never carries the first one's rule.
	//
	// Installed only where it would do something, because the session reads
	// "a redactor is installed" as "this case withholds values" and says so
	// at the autopsy — a rule that redacts nothing would put that notice on
	// every failing case.
	if redact := sensitive; redact.withholdAll || len(redact.substrings) > 0 {
		if redacting, ok := v1.DebuggerFromContext(ctx).(interface {
			SetRedactor(func(string) string)
		}); ok {
			redacting.SetRedactor(func(text string) string {
				if redact.withholdAll {
					return "[withheld]\n"
				}

				return redactSensitiveSubstrings(text, redact.substrings)
			})
			defer redacting.SetRedactor(nil)
		}
	}

	if recorder != nil {
		recorder.sensitive = sensitive
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

	stopScripts, scriptErr := scriptSignals(runFinished, clock, signals, test.Signals, recorder)
	defer stopScripts()
	if scriptErr != nil {
		result.Error = scriptErr.Error()
		return
	}

	// runErr is this function's named result, assigned here rather than
	// redeclared: it is reported to the caller beside the verdict, because a
	// case can pass with a failed run and schedule exploration compares the
	// failure text ([caseObservables]).
	var outputs *v1.Workflow_StepOutputs
	outputs, runErr = v1.RunWithInputs(ctx, workflow, inputs)
	close(runFinished)

	// The transcript coverage reads is the same one the verdict does. A failed
	// run hands back the partial one ([v1.PartialTranscript]): the steps it ran
	// before it stopped, and the step it stopped on. So a case whose whole point
	// is `expect.failed: true` credits the branch it actually exercised instead
	// of contributing its workflow's steps to the universe and reaching none of
	// them (issue #453), and `expect.ran`/`expect.skipped` read the same record,
	// which is what keeps the two from disagreeing about one run.
	transcript = outputs

	// An abandoned run is not a verdict about the workflow, whatever the case
	// expected. `quit` ends the run wherever it stands, so a case declaring
	// `expect.failed: true` would otherwise be *satisfied* by the debugger's
	// own error and pass without ever reaching the failure it named — a
	// debugger turning a red case green, which is the one thing `--debug` must
	// never do (Codex, #1109). Reported as the case's error rather than as a
	// failed expectation, because nothing about the expectations was actually
	// judged.
	if errors.Is(runErr, v1.ErrDebugSessionEnded) {
		result.Error = fmt.Sprintf("the debug session ended this run before it finished, so this "+
			"case has no verdict: %v", runErr)
		result.Passed = false

		return
	}

	result.Failures = assertExpectation(&test.Expect, workflow, outputs, runErr)
	// The CEL claims (#1072), after the named fields so a report reads
	// structure first, values second — the order the file states them in.
	result.Failures = append(result.Failures, assertChecks(ctx, test.Expect.Check, workflow, bound, vars, outputs, runErr, sensitive)...)
	result.Passed = len(result.Failures) == 0

	// The autopsy (#1072 decision 4's follow-on): a failing case under a
	// debugging session stops once more, after the verdict, with the
	// failures printed and the finished run's scope still questionable —
	// the same [postRunScope] the checks were judged against, so what an
	// author inspects here and what a check would have read are one thing.
	// Capability-discovered the way an observing session is: a debugger
	// that does not implement it simply ends when the run ends, and nothing
	// here can change the verdict — it was reached above.
	if !result.Passed {
		if examiner, ok := v1.DebuggerFromContext(ctx).(interface {
			Autopsy(context.Context, *v1.Scope, map[string]ref.Val, []string)
		}); ok {
			rendered := make([]string, 0, len(result.Failures))
			for _, failure := range result.Failures {
				rendered = append(rendered, failure.GetField()+": "+failure.GetMessage())
			}
			scope := postRunScope(workflow, bound, outputs)
			examiner.Autopsy(ctx, scope, autopsyExtras(ctx, scope, vars, runErr, sensitive), rendered)
		}
	}

	// Only for a run that completed: on one that failed, a stub the run never
	// reached is legitimately unanswered, and the report cannot tell that
	// apart from a genuinely idle one — the same unverifiable-claim honesty
	// `expect.skipped` applies to parallel branches on a failed run. A case
	// that never reached a run at all (result.Error set) returns above and
	// never gets here.
	if runErr == nil {
		result.Warnings = unusedStubWarnings(stubs)
	}

	return
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
// reason (`allowLoopback` in pkg/flowstate/v1/internal/conformance/conformance.go): the local
// driver looks tasks up through the process-wide default registry
// ([v1.LookupTask]), so replacing a task for the duration of one case means
// mutating that registry and putting it back. Test cases within one `flow
// test` invocation therefore cannot run concurrently with each other — they
// do not; [RunFile] runs them in sequence — and not concurrently with
// anything else touching the same registry in the same process.
func swapRegistry(taskNames []string) func() {
	registry := v1.DefaultRegistry()

	type saved struct {
		def     v1.TaskDef
		existed bool
	}
	originals := make(map[string]saved, len(taskNames))

	// A stub naming a task this build does not register at all — a plugin
	// task, say — still needs a shape the *compiler* can compile a step
	// against, and the compiler reads the build's registry rather than a run's
	// (see [v1.NewContextWithRegistry] for why shapes stay a property of the
	// build). Only these synthetic names are registered globally, and only so
	// that parsing succeeds; what actually executes comes from the per-case
	// registry [caseRegistry] builds, which every real task's entry in this
	// global registry is left completely untouched by. The Fn is a fail-closed
	// placeholder because it is never called: a compile does not run a task,
	// and the run below is handed [caseRegistry] instead of this one.
	for _, name := range taskNames {
		if _, already := registry.Lookup(name); already {
			continue
		}
		originals[name] = saved{existed: false}
		_ = registry.Register(v1.TaskDef{Name: name, Fn: unstubbedTaskFn(name)})
	}

	return func() {
		for name, s := range originals {
			if s.existed {
				_ = registry.Register(s.def)
			} else {
				registry.Unregister(name)
			}
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
func caseRegistry(stubs map[string]*stubbedTask, sensitiveInputNames map[string]bool) *v1.Registry {
	registry := v1.NewRegistry()

	// Every task this build registers, stubbed or not, which is what makes
	// "no stub, no network" a property of the whole registry rather than of
	// whichever names a case happened to mention.
	for _, def := range v1.DefaultRegistry().All() {
		replacement := def
		if stub, ok := stubs[def.Name]; ok {
			replacement.Fn = stub.fn(def.Name, sensitiveInputNames)
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
		_ = registry.Register(v1.TaskDef{Name: name, Fn: stub.fn(name, sensitiveInputNames)})
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
func scriptSignals(runFinished <-chan struct{}, clock *v1.VirtualClock, signals *v1.LocalSignals, scripts []SignalScript, recorder *runRecorder) (stop func(), err error) {
	if len(scripts) == 0 {
		return func() {}, nil
	}

	type job struct {
		name    string
		at      time.Duration
		payload map[string]any
		sender  *v1.SignalSender

		// senderSubject is what the transcript names a delivery as sent by —
		// the script's own `sender.subject`, "" for a script that named
		// nobody.
		senderSubject string
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
		subject := ""
		if s.Sender != nil {
			subject = s.Sender.Subject
		}
		jobs = append(jobs, job{name: s.Name, at: at, payload: s.Payload, sender: scriptedSender(s.Sender), senderSubject: subject})
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

			// The send and its record are one atomic decision, and the record
			// is honest about the outcome — delivered, or refused by a
			// declared signal policy or the queue's bound. See
			// [runRecorder.deliverRecorded] for why both halves matter. A run
			// recording no account delivers plainly.
			if recorder == nil {
				_ = signals.DeliverFrom(j.name, &v1.Node_Outputs{NamedValues: v1.NewNamedValues(j.payload)}, j.sender)
				return
			}
			recorder.deliverRecorded(j.name, j.payload, j.senderSubject, func() error {
				return signals.DeliverFrom(j.name, &v1.Node_Outputs{NamedValues: v1.NewNamedValues(j.payload)}, j.sender)
			})
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

// checkExpectationNames refuses an expectation naming a step the workflow does
// not have, before the run ever starts (#926). Without it, `skipped:` naming a
// ghost — a typo, or a step renamed since the case was written — asserted
// nothing forever while reading as if it asserted something, because "absent
// from the transcript" is satisfied by a name that never existed; and `ran:`
// on the same typo failed with "produced no recorded outputs" and no hint that
// the step is not merely unreached but unreal.
//
// Refused as a case-level error rather than a failure diagnostic, exactly as a
// stub naming an unknown step is ([bindStubs]): both are the file disagreeing
// with the workflow, decided before anything runs, with the same did-you-mean
// machinery every other surface reads the compiled workflow through.
//
// Three name sets, because the honest refusal differs:
//
//   - `ran:`/`skipped:` are judged against the top-level transcript, so their
//     names must be in [topLevelStepUniverse]. A name that exists only inside
//     a loop body gets the specific sentence — its outputs travel inside the
//     loop's own results, so no top-level claim about it can ever be checked —
//     rather than a suggestion to retype what is already spelled right.
//   - `compensated:` names any step the run could have undone, so it is
//     checked against every id the workflow declares at any depth.
func checkExpectationNames(want *Expectation, spec *v1.Workflow) error {
	top := map[string]bool{}
	topLevelStepUniverse(spec.GetSteps(), top)

	all := map[string]bool{}
	collectAllStepIDs(spec.GetSteps(), all)

	candidates := func(set map[string]bool) []string {
		names := make([]string, 0, len(set))
		for id := range set {
			names = append(names, id)
		}
		sort.Strings(names)
		return names
	}
	topNames := candidates(top)
	allNames := candidates(all)

	checkTop := func(field, step string) error {
		if top[step] {
			return nil
		}
		if all[step] {
			return fmt.Errorf("expect.%s names step %q, which is a loop body step: its outputs travel "+
				"inside the loop's own results and never appear in the top-level transcript this claim "+
				"is judged against; assert the loop's results through expect.outputs", field, step)
		}
		if suggestion, ok := nearest.Name(step, topNames); ok {
			return fmt.Errorf("expect.%s names unknown step %q; did you mean %q?", field, step, suggestion)
		}
		return fmt.Errorf("expect.%s names unknown step %q, which this workflow has no step for", field, step)
	}

	for _, step := range want.Ran {
		if err := checkTop("ran", step); err != nil {
			return err
		}
	}
	for _, step := range want.Skipped {
		if err := checkTop("skipped", step); err != nil {
			return err
		}
	}
	// A `call:` registers its callee's own `undo:` steps onto this run's
	// stack under the callee's step ids (see examples/progressive-rollout,
	// whose cases name `record` and `shift` — steps of shift-traffic.yaml,
	// not of the caller), and this checker deliberately never loads a callee,
	// the same line [stepTasks] draws. So a workflow with a `call:` anywhere
	// leaves `compensated:` unchecked rather than refusing a name it cannot
	// see: a false diagnostic is worse than a missing one, the
	// ResolvableInputs abstention CLAUDE.md's diagnostics rule names.
	if !containsCallStep(spec.GetSteps()) {
		for _, step := range want.Compensated {
			if all[step] {
				continue
			}
			if suggestion, ok := nearest.Name(step, allNames); ok {
				return fmt.Errorf("expect.compensated names unknown step %q; did you mean %q?", step, suggestion)
			}
			return fmt.Errorf("expect.compensated names unknown step %q, which this workflow has no step for", step)
		}
	}

	return nil
}

// containsCallStep reports whether any step at any depth is a `call:` — the
// one node kind whose compensations run under step ids this package never
// compiles. See [checkExpectationNames].
func containsCallStep(nodes []*v1.Node) bool {
	for _, node := range nodes {
		switch kind := node.GetKind().(type) {
		case *v1.Node_Call:
			return true
		case *v1.Node_Parallel:
			for _, branch := range kind.Parallel.GetBranches() {
				if containsCallStep(branch.GetSteps()) {
					return true
				}
			}
		case *v1.Node_Switch:
			for _, body := range v1.SwitchBodies(kind.Switch) {
				if containsCallStep(body) {
					return true
				}
			}
		case *v1.Node_ForEach:
			if containsCallStep(kind.ForEach.GetBody()) {
				return true
			}
		case *v1.Node_Loop:
			if containsCallStep(kind.Loop.GetBody()) {
				return true
			}
		}
	}
	return false
}

// collectAllStepIDs records every step id the workflow declares, at any depth —
// parallel branches, switch bodies, and loop/for_each bodies included. It does
// not descend into a `call:`, whose steps belong to the callee's own file, the
// same line [stepTasks] draws.
func collectAllStepIDs(nodes []*v1.Node, out map[string]bool) {
	for _, node := range nodes {
		switch kind := node.GetKind().(type) {
		case *v1.Node_Parallel:
			for _, branch := range kind.Parallel.GetBranches() {
				collectAllStepIDs(branch.GetSteps(), out)
			}
			continue
		case *v1.Node_Switch:
			out[node.GetId()] = true
			for _, body := range v1.SwitchBodies(kind.Switch) {
				collectAllStepIDs(body, out)
			}
			continue
		case *v1.Node_ForEach:
			out[node.GetId()] = true
			collectAllStepIDs(kind.ForEach.GetBody(), out)
			continue
		case *v1.Node_Loop:
			out[node.GetId()] = true
			collectAllStepIDs(kind.Loop.GetBody(), out)
			continue
		}
		out[node.GetId()] = true
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
func assertExpectation(want *Expectation, spec *v1.Workflow, outputs *v1.Workflow_StepOutputs, runErr error) []*v1.Diagnostic {
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

	for _, step := range want.Ran {
		if _, ok := outputs.GetStepValues()[step]; !ok {
			failures = append(failures, &v1.Diagnostic{
				Step:    step,
				Field:   "expect.ran",
				Message: fmt.Sprintf("expected step %q to have run, but it produced no recorded outputs", step),
			})
		}
	}
	// On a failed run, a parallel branch step's absence is unknowable rather
	// than evidence: branch outputs merge to the top level only when the block
	// completes, and [v1.PartialTranscript] deliberately carries no unmerged
	// branch scope, so an absent branch step may have run right up to the
	// failure. The transcript cannot tell that apart from a genuine skip, and
	// fail closed (CLAUDE.md) means an unverifiable claim is refused, not
	// accepted. Judged before the absence check below so the refusal wins.
	branchSteps := map[string]bool{}
	if failed {
		parallelBranchSteps(spec.GetSteps(), branchSteps)
	}
	for _, step := range want.Skipped {
		if failed && branchSteps[step] {
			if _, ok := outputs.GetStepValues()[step]; !ok {
				failures = append(failures, &v1.Diagnostic{
					Step:  step,
					Field: "expect.skipped",
					Message: fmt.Sprintf(
						"step %q is inside a parallel block and the run failed, so its absence from "+
							"the transcript cannot be told apart from the block not finishing; assert a "+
							"top-level step, or expect the run to succeed, to make this claim checkable", step),
				})
				continue
			}
		}
		if _, ok := outputs.GetStepValues()[step]; ok {
			failures = append(failures, &v1.Diagnostic{
				Step:    step,
				Field:   "expect.skipped",
				Message: fmt.Sprintf("expected step %q to have been skipped, but it produced recorded outputs", step),
			})
		}
	}

	// `others: skipped` closes the `ran:` claim: every step the workflow has
	// that `ran:` does not name must be absent from the transcript. Computed
	// over the top-level step universe, the same set `ran:`/`skipped:` compare
	// against, so a loop body step is not miscounted (its outputs live inside
	// the loop's results, never at the top level). Deny-by-default applied to
	// expectations (CLAUDE.md, "fail closed"): a step that ran and is not named
	// is a failure, which is what makes adding a workflow step fail a closed
	// claim loudly.
	if want.Others == OthersSkipped {
		named := make(map[string]bool, len(want.Ran))
		for _, step := range want.Ran {
			named[step] = true
		}
		universe := map[string]bool{}
		topLevelStepUniverse(spec.GetSteps(), universe)
		ids := make([]string, 0, len(universe))
		for id := range universe {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			if named[id] {
				continue
			}
			if failed && branchSteps[id] {
				if _, ran := outputs.GetStepValues()[id]; !ran {
					failures = append(failures, &v1.Diagnostic{
						Step:  id,
						Field: "expect.others",
						Message: fmt.Sprintf(
							"step %q is inside a parallel block and the run failed, so `others: skipped` "+
								"cannot verify it was skipped rather than lost with the unfinished block; name "+
								"it in ran:, or expect the run to succeed, to make the closed claim checkable", id),
					})
					continue
				}
			}
			if _, ran := outputs.GetStepValues()[id]; ran {
				failures = append(failures, &v1.Diagnostic{
					Step:  id,
					Field: "expect.others",
					Message: fmt.Sprintf(
						"step %q ran, but expect.ran does not name it and expect.others is %q, "+
							"which requires every step not named in ran: to have been skipped", id, OthersSkipped),
				})
			}
		}
	}

	// `outputs:` is the one expectation a failed run still cannot answer. A run's
	// declared outputs are the answer it was asked for, and [v1.PartialTranscript]
	// deliberately carries none: a failed run has no answer, so comparing against
	// an absent one would report every declared output missing on top of the
	// failure the case already said it wanted. Ran/Skipped/Others above are a
	// different question, what the run *did*, and the partial transcript is
	// exactly the record of that.
	if failed {
		return failures
	}

	if want.Outputs != nil {
		failures = append(failures, compareOutputs(want.Outputs, outputs.GetRunOutputs().GetValues())...)
	}

	return failures
}

// topLevelStepUniverse collects every step id that can appear in a run's
// top-level transcript ([v1.Workflow_StepOutputs.StepValues]), which is the set
// `expect.ran`, `expect.skipped`, and `expect.others` are all judged against.
//
// A parallel block records nothing under its own id but its branch steps are
// merged into the enclosing scope, so this descends into branches and counts
// their steps. A loop or for_each records outputs under its own id, so the
// container is counted, but its body steps travel inside the loop's `results`
// output rather than at the top level, so this does not descend into a body: a
// body step is not a step `others: skipped` can claim was skipped. This is
// coverage's [collectStepUniverse] with the one deliberate difference that it
// stops at a loop container.

// parallelBranchSteps collects every step id declared inside a parallel
// block's branches, at any nesting depth. On a failed run these are the ids
// whose absence from the transcript is unknowable: their outputs merge to the
// top level only when their block completes.
func parallelBranchSteps(nodes []*v1.Node, out map[string]bool) {
	for _, node := range nodes {
		if parallel, ok := node.GetKind().(*v1.Node_Parallel); ok {
			for _, branch := range parallel.Parallel.GetBranches() {
				collectStepIDs(branch.GetSteps(), out)
			}
			continue
		}
	}
}

// collectStepIDs records every node id in nodes, recursing into parallel
// branches and switch bodies, so a nested block's steps are counted too — a
// switch-body step inside a branch is as unknowable on a failed run as any
// other branch step.
func collectStepIDs(nodes []*v1.Node, out map[string]bool) {
	for _, node := range nodes {
		if parallel, ok := node.GetKind().(*v1.Node_Parallel); ok {
			for _, branch := range parallel.Parallel.GetBranches() {
				collectStepIDs(branch.GetSteps(), out)
			}
			continue
		}
		if sw, ok := node.GetKind().(*v1.Node_Switch); ok {
			out[node.GetId()] = true
			for _, body := range v1.SwitchBodies(sw.Switch) {
				collectStepIDs(body, out)
			}
			continue
		}
		out[node.GetId()] = true
	}
}

func topLevelStepUniverse(nodes []*v1.Node, universe map[string]bool) {
	for _, node := range nodes {
		if parallel, ok := node.GetKind().(*v1.Node_Parallel); ok {
			for _, branch := range parallel.Parallel.GetBranches() {
				topLevelStepUniverse(branch.GetSteps(), universe)
			}
			continue
		}
		// A switch records its own id (the `case` record coverage reads) *and*
		// its taken arm's body steps at the top level — a body step is
		// assertable through `ran:`, so it has to be in the universe the
		// closed claim walks. Before this arm existed, `others: skipped` was
		// blind to a switch-body step that ran: the id was absent from the
		// universe, so the one claim built to catch an unnamed step passed
		// straight over it (#926).
		if sw, ok := node.GetKind().(*v1.Node_Switch); ok {
			universe[node.GetId()] = true
			for _, body := range v1.SwitchBodies(sw.Switch) {
				topLevelStepUniverse(body, universe)
			}
			continue
		}
		// Task, Wait, Call, ForEach, Loop: each records outputs under its own
		// id at the top level.
		universe[node.GetId()] = true
	}
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

// literalToGo converts a resolved CEL literal into a plain Go value. It is
// [v1.LiteralToGo], the one spelling of that conversion this package shares
// with [v1] itself and with `embed`, kept as a local name so the call sites
// below did not need touching when the switch moved to [v1].
func literalToGo(v *expr.Value) (any, error) {
	return v1.LiteralToGo(v)
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
