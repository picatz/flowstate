// Package flowtest implements `flow test` (#155): self-testing of workflows
// through the local driver, with stubbed tasks and virtual time.
//
// A `*.test.yaml` file sits beside the Flowfile it tests — `deploy.test.yaml`
// next to `deploy.yaml` — naming the workflow, the arguments to run it with,
// the task responses to stub in place of the real registry, scripted signals,
// what `${secret(...)}` references resolve to, and what the run must produce.
// Running it never touches a network or a Temporal server: every task the
// workflow would otherwise call is replaced by what the test declares, and the
// workflow's own control flow — conditions, retries, loops, `undo:` — runs for
// real through the ordinary local driver. That is the registry-swap pattern
// the repo's own tests already use (`allowLoopback`, `NewUndoServer` in
// pkg/flowstate/v1/tests) productized for an author's own workflow.
//
// # Secrets are stubbed at the reference boundary, the same way tasks are
// stubbed at the task boundary
//
// A workflow's `${secret('vault:prod/db#password')}` compiles and runs under
// `flow test` with no Vault reachable, and no Vault provider even compiled
// into this build: [Test.Secrets] binds the reference's text form directly to
// a value for the duration of one case, resolved by an in-memory provider
// [runCase] registers instead of any real backend — see [secretRuntime]. A
// reference with no matching entry is refused, naming the reference and the
// remedy, on the identical fail-closed reasoning [swapRegistry] applies to an
// unstubbed task: `flow test`'s whole promise is that nothing it runs reaches
// a real dependency, and a secret backend is exactly such a dependency.
//
// # Identities are declared, checked by the real policy, and never attested
//
// A workflow's `signals:` policy is authorization, and a case exercises it by
// naming the two parties that policy is written about: [Test.Starter] is who
// the run started as, and a [SignalScript.Sender] is who a scripted delivery
// stands in for. Both are read by [v1.SignalPolicyCheck] - the function
// `FlowstateServer.Signal` itself calls - so a rule that admits an approver in
// production admits them here, one that refuses them refuses them here, and
// `distinct_from_starter:` refuses the approver who is this run's own starter
// (#344 slice 3).
//
// Neither is an attestation, and the harness is careful to keep saying so.
// A scripted delivery carries [v1.RehearsalSignalSender] - identity populated,
// `local` true - the same shape `flow run local --signal-as-subject` delivers
// and the shape the durable driver refuses outright, so a gate's own
// `sender.local` output reads true and `!sender.local` keeps meaning "a server
// accepted this". A starter reaches the signal policy and nothing else:
// `run.identity` stays empty and `run.local` true for every case, as it does
// for every local run, because a local run must never look like an attested
// production one.
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
	"maps"
	"os"
	"path/filepath"
	"slices"

	"github.com/goccy/go-yaml"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
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

	// MaxSecretsPerTest bounds how many `secrets:` entries one test may
	// declare.
	MaxSecretsPerTest = 200
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

	// Secrets replaces the real secret backend for the duration of this case —
	// the secret sibling of Stubs, at the reference boundary rather than the
	// task boundary. Keyed by a reference's text form ("scheme:name", exactly
	// as a Flowfile's `${secret('scheme:name')}` names it — see
	// [secrets.RefString]) and bound to the plaintext value that reference
	// resolves to for this case only.
	//
	// A `${secret(...)}` a stubbed task's input carries is resolved against
	// this map, not against any real backend: no scheme needs a configured
	// provider, or even a provider compiled into this build, which is what
	// makes a workflow naming `vault:prod/db#password` testable with no Vault
	// reachable — see the package doc. A reference with no matching entry
	// here is refused the moment a stubbed task is invoked with it, naming
	// the reference and the remedy, on the same fail-closed reasoning
	// [swapRegistry] applies to an unstubbed task: resolving it to the empty
	// string, or falling through to whatever secret backend this process
	// happens to have configured, would both defeat the reason `flow test`
	// exists.
	Secrets map[string]string `yaml:"secrets"`

	// Signals scripts what to deliver to a `wait_for_signal:` step, and when.
	Signals []SignalScript `yaml:"signals"`

	// Starter is who this case runs as: the identity a `signals:` policy's
	// `distinct_from_starter:` compares a scripted [SignalScript.Sender]
	// against, exactly as `flow run local`'s `--as-subject` and its siblings
	// name it for a rehearsal on the command line (#344 slice 3).
	//
	// Without it a case runs as nobody, which is what every case did before
	// this field existed and remains the default: an empty identity that is
	// *recorded* rather than unknown, so a `distinct_from_starter:` policy
	// admits a scripted approver instead of refusing every case outright
	// (see [v1.NewPolicedLocalSignals]'s hasStarter parameter, and runCase).
	// The consequence worth stating is that "nobody" is distinct from every
	// named approver, so the refusal `distinct_from_starter:` exists to
	// produce is unreachable until a case names a starter - which is the
	// whole reason this field exists.
	//
	// It is who the run *starts as*, not who it is attested to be. Nothing
	// here is authenticated, and it deliberately does not reach
	// `run.identity`: the local driver answers that with an empty identity
	// and `run.local` true for every run, `flow run local --as-subject`
	// included, because a local run must never look like an attested
	// production one (eval.go's own eval, invariant 3). A case asserting on
	// `run.identity.subject` therefore sees "" here whatever this field says,
	// the same value `flow run local` shows it.
	Starter *ScriptedIdentity `yaml:"starter"`

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
	// expression — the same language `if:` and every other expression in a
	// Flowfile use. Written bare, without the `${...}` fence a Flowfile's own
	// values need: a Flowfile fences an expression to tell it apart from a
	// literal value in a position that could legally hold either, and Where
	// is never anything else, so there is nothing for a fence to
	// disambiguate. Empty matches every invocation of Task that no earlier
	// stub already matched, which is what makes a stub list read as a
	// sequence of cases tried in order — the shape a `switch` already has.
	//
	// It is evaluated against the scope the stubbed step itself was evaluated
	// in, plus `inputs` — the task's own resolved inputs. So it reads, with
	// the spellings a Flowfile uses: `inputs.<name>`, any name bound where the
	// step is written (a `for_each`'s `as:` name, `item` when the loop writes
	// none, the step's own `vars:` keys), `vars.<name>`, and
	// `steps.<id>.<output>`.
	//
	// The bare binding is what makes a loop testable. An input the task
	// evaluates itself ([v1.TaskDef.DeferredInputs] — `http`'s `outputs:` and
	// `expect:`, against `response`) is still an expression when a stub is
	// consulted and is absent from `inputs` rather than resolved to something
	// misleading, so where a loop body's outputs are shaped by one, `inputs`
	// is identical on every iteration and only the binding separates them
	// (#269).
	//
	// `inputs` is bound as a local, so it shadows the run's own
	// `inputs.<name>` namespace for the length of a Where clause. That is the
	// older meaning kept deliberately: a stub's `where:` has named the task's
	// inputs since stubs existed.
	Where string `yaml:"where"`

	// Returns is the task's outputs when Where matches, converted the way a
	// literal value anywhere in a Flowfile is — see [v1.NewValue].
	//
	// A value follows the Flowfile's own fence rule, at any depth: a
	// whole-value `${...}` is an expression, evaluated per invocation against
	// the same names [Stub.Where] sees, and anything else is literal. So one
	// stub can answer a loop's iterations differently — `name:
	// '${service.name}'` — rather than repeating one canned answer over a
	// whole fan-out. A value that mixes literal text with a fence is refused
	// when the test file is loaded, the same mistake reported the same way a
	// Flowfile reports it.
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

	// Sender is who this signal stands in for, checked against the workflow's
	// own declared `signals:` policy for Name exactly as
	// `FlowstateServer.Signal` checks a durable one — through
	// [v1.SignalPolicyCheck], the same function, so a policy that would
	// refuse this sender in production refuses it here too rather than
	// silently delivering anyway (#207's slice 2: local delivery not
	// enforcing policy was what made rehearsal lie in the dangerous
	// direction once a workflow's `if:` started trusting `signals:` for
	// authorization).
	//
	// It stands in for that approver; it never claims anybody authenticated
	// them. The delivery carries [v1.RehearsalSignalSender], identity
	// populated and `local` true, which is the same shape `flow run local
	// --signal-as-subject` delivers (#349) and the shape the durable driver
	// refuses outright. So a gate's own `sender.local` output reads true
	// under `flow test`, and `!sender.local` keeps meaning "a server accepted
	// this" for a workflow author: the contract [v1.SignalWaiter] states it,
	// which a scripted sender used to break by delivering with `local` false
	// and rendering exactly like an attested production sender.
	//
	// Omitted under a signal that carries no declared policy: the signal is
	// delivered as [v1.LocalSignalSender] always was, standing in for nobody.
	// Omitted under a signal that *does* declare a policy: the delivery is
	// still checked, as an empty identity — which a real policy's `allow:`
	// rule can never match (every rule requires a non-empty subject,
	// namespace, or claim; see signalpolicy.go's ruleMatchesEverySender) — so
	// an author who forgets `sender:` gets a refused delivery rather than a
	// silent pass. That is the deliberate fail-closed answer, not an
	// oversight: a scripted signal stands in for exactly as little as `flow
	// run local --signal` does unless a case says otherwise.
	//
	// A refused delivery is not reported as a diagnostic of its own, and that
	// is production's shape rather than an omission: the waiting step is
	// never told anything was sent (see [v1.LocalSignals.DeliverFrom]), so
	// the gate lapses at its own `timeout:` and the case fails against
	// whatever it expected of that. What a mistake in the *file* gets instead
	// is a refusal when the file loads - see [checkScriptedIdentity], which
	// exists precisely so a malformed identity is not rendered as a gate
	// nobody answered.
	Sender *ScriptedIdentity `yaml:"sender"`
}

// ScriptedIdentity is an identity a case names, either the sender of a
// scripted signal ([SignalScript.Sender]) or the run's own starter
// ([Test.Starter]), carrying the fields [v1.WorkloadIdentity] does that a `signals:` policy is
// matched on: subject and issuer together, never subject alone, for the
// identical multi-IdP reason `flow validate` requires a
// [v1.SignalPolicyRule.subject] to be issuer-qualified.
//
// One type for both ends on purpose. `distinct_from_starter:` compares the two
// against each other, through [v1.QualifiedSubject] on each, so a case whose
// starter and sender were spelled with two different sets of fields would be a
// comparison an author could not read - the same reasoning that made
// `--signal-as-subject` rhyme with `--as-subject` on the command line rather
// than invent a second vocabulary.
//
// Nothing here is attested, and nothing here is minted, signed, or carried
// anywhere: the values live in one process, for one case, and are discarded
// with it.
type ScriptedIdentity struct {
	// Subject is the caller this identity stands in for, matched against a
	// policy rule's `subject:` as `<issuer>#<subject>` — see
	// [v1.QualifiedSubject].
	Subject string `yaml:"subject"`

	// Issuer identifies which identity provider would have attested Subject.
	Issuer string `yaml:"issuer"`

	// Namespace is the tenant this identity belongs to, matched against a
	// policy rule's `namespace:`.
	Namespace string `yaml:"namespace"`

	// Claims are additional facts, matched against a policy rule's `claims:`
	// — every key the rule names must be present here with the same value.
	Claims map[string]string `yaml:"claims"`
}

// checkScriptedIdentity refuses an identity no policy could ever read the way
// its author meant it, when the file loads rather than when a case runs.
//
// where names the position in the file a reader has to look at - `test "x"
// starter:`, `test "x" signal 2 sender:` - because a `*.test.yaml` is
// identified by case name throughout this loader (see [parseSource]) rather
// than by line and column: every diagnostic here names the case, what is
// wrong, and what to do instead, which is the shape CLAUDE.md's "diagnostics
// are a feature" asks for and the shape the rest of this file already has.
//
// Both rules are fail-closed readings of a policy that would otherwise refuse
// silently, at a gate, a whole virtual day later:
//
//   - A subject with no issuer (or an issuer with no subject) matches
//     "#alice", which no `allow:` rule can carry: `flow validate` refuses an
//     unqualified rule `subject:` outright. `flow run local` refuses the same
//     half-pair on the command line, and for the same reason.
//   - A claim with an empty name or an empty value cannot be what an author
//     meant, and is matched literally rather than ignored - the mistake
//     `--signal-as-claim`'s own NAME=VALUE check refuses.
func checkScriptedIdentity(where string, identity *ScriptedIdentity) error {
	if identity == nil {
		return nil
	}

	if (identity.Subject == "") != (identity.Issuer == "") {
		return fmt.Errorf(
			"%s names a subject or an issuer without the other; give both, because a rule matches %q "+
				"and never a bare subject - a subject is only unique within its issuer",
			where, v1.QualifiedSubject("<issuer>", "<subject>"))
	}

	// Sorted, so a file with two bad claims reports the same one every time:
	// a diagnostic that changes between runs of the same input is one nobody
	// can write a test against, this package's own included.
	for _, name := range slices.Sorted(maps.Keys(identity.Claims)) {
		value := identity.Claims[name]

		empty := "value"
		if name == "" {
			empty = "name"
		}
		if name == "" || value == "" {
			return fmt.Errorf(
				"%s declares a claim with an empty %s; a claim is matched literally, so write it as "+
					"`name: value` with both present, or drop it",
				where, empty)
		}
	}

	return nil
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

	file, err := parseSource(data, true)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	return file, nil
}

// LoadSource is [Load] for a `*.test.yaml` given directly as bytes rather than
// read from a path — the seam [RunSource] uses to run inline test cases the
// way [RunFile] runs a file on disk, on bytes instead of a path, exactly as
// the MCP surface's flowstate_run_local is [v1.RunWithInputs] on bytes instead
// of `flow run local`'s path.
//
// The one difference bytes force: a case's `workflow:` is not required. [Load]
// requires it because [RunFile] resolves each case's workflow relative to the
// *.test.yaml's own directory ([WorkflowPath]), and there is no directory for
// bytes with no path — the same reason [Parse], unlike [ParseFile], refuses a
// `call:` step. [RunSource] is given the workflow directly instead, once, for
// every case in the file, so nothing here needs a name for it.
func LoadSource(data []byte) (*File, error) {
	if len(data) > MaxTestFileBytes {
		return nil, fmt.Errorf("%d bytes exceeds the %d byte limit for a test file", len(data), MaxTestFileBytes)
	}

	return parseSource(data, false)
}

// parseSource is the byte-parsing seam both [Load] and [LoadSource] share:
// the expansion-bound check, the strict unmarshal, and every bound in
// [MaxTestsPerFile]/[MaxStubsPerTest]/[MaxSignalsPerTest] — everything [Load]
// did after reading the file off disk, factored out so a caller with bytes
// and no path runs the identical checks rather than a second copy of them.
// requireWorkflow is false only for [LoadSource]; see its doc for why.
func parseSource(data []byte, requireWorkflow bool) (*File, error) {
	// Checked against the parsed AST, before yaml.Unmarshal below is asked to
	// do anything: Unmarshal resolves every alias into the destination value
	// as it decodes, which means a billion-laughs document is already fully
	// expanded in memory by the time any bound written against the decoded
	// value could run. See [checkExpansionBounds].
	if err := checkExpansionBounds(data); err != nil {
		return nil, err
	}

	var file File
	if err := yaml.UnmarshalWithOptions(data, &file, yaml.Strict()); err != nil {
		return nil, err
	}

	if len(file.Tests) == 0 {
		return nil, fmt.Errorf("declares no tests")
	}
	if len(file.Tests) > MaxTestsPerFile {
		return nil, fmt.Errorf("declares %d tests, more than the limit of %d",
			len(file.Tests), MaxTestsPerFile)
	}

	for i, test := range file.Tests {
		if test.Name == "" {
			return nil, fmt.Errorf("test %d has no name", i+1)
		}
		if requireWorkflow && test.Workflow == "" {
			return nil, fmt.Errorf("test %q names no workflow", test.Name)
		}
		if len(test.Stubs) > MaxStubsPerTest {
			return nil, fmt.Errorf("test %q declares %d stubs, more than the limit of %d",
				test.Name, len(test.Stubs), MaxStubsPerTest)
		}
		if len(test.Signals) > MaxSignalsPerTest {
			return nil, fmt.Errorf("test %q declares %d signals, more than the limit of %d",
				test.Name, len(test.Signals), MaxSignalsPerTest)
		}
		if len(test.Secrets) > MaxSecretsPerTest {
			return nil, fmt.Errorf("test %q declares %d secrets, more than the limit of %d",
				test.Name, len(test.Secrets), MaxSecretsPerTest)
		}
		for ref := range test.Secrets {
			// Checked while the reference is still text, so a malformed
			// `secrets:` key fails when the file loads rather than the first
			// time a case happens to invoke a task naming it — the same
			// timing [secrets.ParseRef]'s own doc gives for a Flowfile.
			if _, err := secrets.ParseRef(ref); err != nil {
				return nil, fmt.Errorf("test %q secrets: %w", test.Name, err)
			}
		}
		if err := checkScriptedIdentity(fmt.Sprintf("test %q starter:", test.Name), test.Starter); err != nil {
			return nil, err
		}
		for j, signal := range test.Signals {
			// Checked at load, alongside every other shape check in this
			// loop, rather than when the scripted goroutine delivers: a
			// delivery refused there disappears the way a production
			// PermissionDenied does (see [v1.LocalSignals.DeliverFrom]), so
			// the case would report a gate that timed out and never the
			// mistake in the file that caused it.
			if err := checkScriptedIdentity(
				fmt.Sprintf("test %q signal %d (%q) sender:", test.Name, j+1, signal.Name),
				signal.Sender,
			); err != nil {
				return nil, err
			}
		}
		for j, stub := range test.Stubs {
			if stub.Task == "" {
				return nil, fmt.Errorf("test %q stub %d names no task", test.Name, j+1)
			}
			if stub.Returns != nil && stub.Fails != nil {
				return nil, fmt.Errorf(
					"test %q stub %d for task %q declares both returns and fails; a stubbed call either succeeds or fails, not both",
					test.Name, j+1, stub.Task)
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
