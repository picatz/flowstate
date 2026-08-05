// Package flowtest implements `flow test` (#155): self-testing of workflows
// through the local driver, with stubbed tasks and virtual time.
//
// A `*.test.yaml` file sits beside the Flowfile it tests — `deploy.test.yaml`
// next to `deploy.yaml` — naming the workflow, the arguments to run it with,
// the task responses to stub in place of the real registry, scripted signals,
// and what the run must produce. Running it never touches a network or a
// Temporal server: every task the workflow would otherwise call is replaced by
// what the test declares, and the workflow's own control flow — conditions,
// retries, loops, `undo:` — runs for real through the ordinary local driver.
// That is the registry-swap pattern the repo's own tests already use
// (`allowLoopback`, `NewUndoServer` in pkg/flowstate/v1/tests) productized for
// an author's own workflow.
//
// # Why this runs the local driver only
//
// `flow test` is not a second execution engine. It is [v1.RunWithInputs] with
// three things swapped in through the mechanisms that already exist for
// exactly this purpose: the task registry (stubs, via the registry-swap
// pattern), the clock ([v1.VirtualClock], via [v1.NewContextWithClock]), and
// the signal source ([v1.LocalSignals], via [v1.NewContextWithSignalWaiter]).
// Nothing here reaches into the durable driver, on purpose — sub-second,
// no-infrastructure feedback is the entire point (#155), and durably
// rehearsing composition, Continue-As-New, and versioning is a different
// question with a different answer (`flow run` against a dev server) that
// this package does not attempt to also be.
package flowtest

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/goccy/go-yaml"
)

// Bounds on a test file, enforced before anything in it runs.
//
// A test file is untrusted input like any Flowfile (CLAUDE.md, "bound
// anything that consumes untrusted input"): it is read from disk by `flow
// test` today, but the shape is ordinary YAML with no reason to assume
// whoever wrote it controls what a case's `where:` or `returns:` can cost —
// checked out from a fork, generated, or pulled in with a called workflow's
// own repository. The numbers are generous next to any legitimate test file;
// they exist to fail loudly on a pathological one rather than to constrain an
// ordinary one.
const (
	// MaxTestFileBytes bounds how large a single *.test.yaml may be, read
	// entirely into memory before it is parsed.
	MaxTestFileBytes = 1 << 20 // 1 MiB

	// MaxTestsPerFile bounds how many `tests:` entries one file may declare.
	MaxTestsPerFile = 500

	// MaxStubsPerTest bounds how many `stubs:` one test may declare.
	MaxStubsPerTest = 200

	// MaxSignalsPerTest bounds how many `signals:` one test may script.
	MaxSignalsPerTest = 200
)

// File is a parsed `*.test.yaml`.
type File struct {
	// Edition is accepted and otherwise unused.
	//
	// `flow fix` stamps `edition:` into any document it recognizes as a Flowfile
	// or a Flowfile test (see flowfile.Fix), and a `*.test.yaml` is the latter —
	// it has no `steps:` of its own but is still a document this repo's tooling
	// migrates forward. Before this field existed, that stamp landed on a
	// struct parsed with [yaml.Strict], and the file this build had just
	// "fixed" no longer loaded: `unknown field "edition"`, the exact failure
	// mode issue #203 records for an egress policy, reproduced here by the fix
	// for that issue's own drift example. This field exists so that migration
	// is not itself a bug — see #203's discussion of `examples/call-a-workflow/workflow.test.yaml`.
	Edition string `yaml:"edition"`

	// Tests are the cases this file declares, in the order they were written —
	// the order [Run] runs them in and reports them in, so a reader matching a
	// report back to the file does not have to search for it.
	Tests []Test `yaml:"tests"`
}

// Test is one `tests:` entry: a workflow, what to run it with, what to
// replace, and what it must produce.
type Test struct {
	// Name identifies the case in a report. Required, because "test 3" is not
	// a name a person or a CI log can act on.
	Name string `yaml:"name"`

	// Workflow is the Flowfile under test, resolved relative to the directory
	// the *.test.yaml itself lives in — the same rule `call:` resolves
	// against, and for the same reason: a test file is meant to travel with
	// the workflow it tests, not with whatever directory `flow test` happened
	// to be invoked from.
	Workflow string `yaml:"workflow"`

	// Inputs bind the workflow's declared `inputs:`, checked the same way a
	// real run's are — see [v1.RunWithInputs].
	Inputs map[string]any `yaml:"inputs"`

	// Stubs replace the task registry for the duration of this case. A task
	// this case never invokes needs no stub; a task it invokes with no
	// matching stub fails the case with a diagnostic naming which task and how
	// many stubs were declared for it, rather than reaching the real registry
	// — a stubbed test that silently made a real network request would defeat
	// the entire point of stubbing at the task boundary.
	Stubs []Stub `yaml:"stubs"`

	// Signals scripts what to deliver to a `wait_for_signal:` step, and when.
	Signals []SignalScript `yaml:"signals"`

	// Expect is what the run must have done to pass.
	Expect Expectation `yaml:"expect"`
}

// Stub replaces one task's real behavior with a canned answer.
//
// Stubbing happens at the task boundary and nothing lower (#155): control
// flow, CEL evaluation, retries, `if:`, loops, and `undo:` registration all
// run for real through the actual local driver, and only the effect —
// whatever the task would have done outside the process — is replaced.
type Stub struct {
	// Task is the task name this replaces, exactly as a step's own task key
	// names it — `http`, `log`, a plugin task's name.
	Task string `yaml:"task"`

	// Where filters which invocations of Task this stub answers, as a CEL
	// expression over the task's resolved inputs — the same language `if:`
	// and every other expression in a Flowfile use, bound to one name:
	// `inputs`. Written bare, without the `${...}` fence a Flowfile's own
	// values need: a Flowfile fences an expression to tell it apart from a
	// literal value in a position that could legally hold either, and Where
	// is never anything else, so there is nothing for a fence to
	// disambiguate. Empty matches every invocation of Task that no earlier
	// stub already matched, which is what makes a stub list read as a
	// sequence of cases tried in order — the shape a `switch` already has.
	Where string `yaml:"where"`

	// Returns is the task's outputs when Where matches, converted the way a
	// literal value anywhere in a Flowfile is — see [v1.NewValue].
	//
	// Mutually exclusive with Fails; a stub declaring both is refused when
	// the test file is loaded rather than left to pick one arbitrarily at run
	// time.
	Returns map[string]any `yaml:"returns"`

	// Fails makes the stubbed task report a classified failure instead of
	// succeeding, so a case can exercise `continue_on_error:`, `retry:`, and
	// `undo:` without a real dependency ever having to be down on purpose.
	Fails *StubFailure `yaml:"fails"`
}

// StubFailure is a canned failure a [Stub] reports instead of outputs.
type StubFailure struct {
	// Kind classifies the failure the way [v1.ErrorKind] does — "Upstream",
	// "InvalidInput", and so on — which is what a case exercising `retry:`
	// needs to get right: only some kinds are retryable, and a stub reporting
	// the wrong one would make a test pass for a reason production would not.
	// Defaults to "Upstream", the ordinary transient failure, when left empty.
	Kind string `yaml:"kind"`

	// Message is the failure text, read back through `${steps.<id>.error}`
	// exactly as a real task's would be.
	Message string `yaml:"message"`
}

// SignalScript delivers one signal at a virtual moment.
type SignalScript struct {
	// Name is the signal a `wait_for_signal:` step names.
	Name string `yaml:"name"`

	// At is when to deliver it, as a duration from the moment the run
	// started — "5m", "1h30m" — parsed by [time.ParseDuration]. Empty (or
	// "0s") delivers it immediately, which for a signal a wait reaches before
	// anything else happens is indistinguishable from an early-arriving
	// signal in production: [v1.LocalSignals] buffers it until something
	// asks.
	At string `yaml:"at"`

	// Payload is what the signal carries, read back under `${<step>.payload}`
	// exactly as `flow signal`'s would be.
	Payload map[string]any `yaml:"payload"`
}

// Expectation is what a case's run must have produced to pass.
//
// Assertions are about promises, not internals (#155): [Expectation.Outputs]
// checks the workflow's declared `outputs:`, [Expectation.Failed] and
// [Expectation.ErrorContains] the run failing outright, [Expectation.Compensated]
// the undo log's account, and [Expectation.Ran]/[Expectation.Skipped] step
// presence when a case needs that level of detail. Nothing here can reach a
// step's private intermediate values — there is no field for one — the same
// restraint the transcript-vs-outputs distinction already draws elsewhere.
type Expectation struct {
	// Outputs, when set, must equal the workflow's declared `outputs:`
	// exactly — every named output present with the expected value, and no
	// unexpected one. Ignored when Failed is true, on the same reasoning
	// [tests.Case.ExpectedOutputs] already documents: a run that fails
	// outright has no outputs to compare.
	Outputs map[string]any `yaml:"outputs"`

	// Failed asserts whether the run failed outright, as distinct from a
	// step's failure being tolerated by `continue_on_error:` — an ordinary
	// case, asserted through Outputs like any other. Nil means "no assertion
	// either way": the case is not about whether the run failed.
	Failed *bool `yaml:"failed"`

	// ErrorContains, when set, must appear in the run's failure text. Only
	// meaningful alongside Failed: true.
	ErrorContains string `yaml:"error_contains"`

	// Compensated names the steps that must have been undone, in any order —
	// this checks the *set* the undo log reports, not the reverse-registration
	// order [v1.RunUndoLog] itself already guarantees and which
	// pkg/flowstate/v1/tests/undo.go already pins for both drivers.
	Compensated []string `yaml:"compensated"`

	// Ran names steps that must have executed — present in the run's step
	// outputs, whether they succeeded or were tolerated.
	Ran []string `yaml:"ran"`

	// Skipped names steps that must not have executed — absent from the run's
	// step outputs, because their `if:` did not hold or the run never reached
	// them.
	Skipped []string `yaml:"skipped"`
}

// Load reads and parses a `*.test.yaml`, applying [MaxTestFileBytes] before
// anything is unmarshaled and [MaxTestsPerFile]/[MaxStubsPerTest]/
// [MaxSignalsPerTest] after.
func Load(path string) (*File, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if info.Size() > MaxTestFileBytes {
		return nil, fmt.Errorf("%s: %d bytes exceeds the %d byte limit for a test file",
			path, info.Size(), MaxTestFileBytes)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	// Checked against the parsed AST, before yaml.Unmarshal below is asked to
	// do anything: Unmarshal resolves every alias into the destination value
	// as it decodes, which means a billion-laughs document is already fully
	// expanded in memory by the time any bound written against the decoded
	// value could run. See [checkExpansionBounds].
	if err := checkExpansionBounds(data); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	var file File
	if err := yaml.UnmarshalWithOptions(data, &file, yaml.Strict()); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	if len(file.Tests) == 0 {
		return nil, fmt.Errorf("%s: declares no tests", path)
	}
	if len(file.Tests) > MaxTestsPerFile {
		return nil, fmt.Errorf("%s: declares %d tests, more than the limit of %d",
			path, len(file.Tests), MaxTestsPerFile)
	}

	for i, test := range file.Tests {
		if test.Name == "" {
			return nil, fmt.Errorf("%s: test %d has no name", path, i+1)
		}
		if test.Workflow == "" {
			return nil, fmt.Errorf("%s: test %q names no workflow", path, test.Name)
		}
		if len(test.Stubs) > MaxStubsPerTest {
			return nil, fmt.Errorf("%s: test %q declares %d stubs, more than the limit of %d",
				path, test.Name, len(test.Stubs), MaxStubsPerTest)
		}
		if len(test.Signals) > MaxSignalsPerTest {
			return nil, fmt.Errorf("%s: test %q declares %d signals, more than the limit of %d",
				path, test.Name, len(test.Signals), MaxSignalsPerTest)
		}
		for j, stub := range test.Stubs {
			if stub.Task == "" {
				return nil, fmt.Errorf("%s: test %q stub %d names no task", path, test.Name, j+1)
			}
			if stub.Returns != nil && stub.Fails != nil {
				return nil, fmt.Errorf(
					"%s: test %q stub %d for task %q declares both returns and fails; a stubbed call either succeeds or fails, not both",
					path, test.Name, j+1, stub.Task)
			}
		}
	}

	return &file, nil
}

// WorkflowPath resolves a test's `workflow:` relative to the *.test.yaml file
// that declared it.
func WorkflowPath(testFile string, test *Test) string {
	if filepath.IsAbs(test.Workflow) {
		return test.Workflow
	}
	return filepath.Join(filepath.Dir(testFile), test.Workflow)
}
