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
	"path/filepath"
	"reflect"
	"slices"
	"strings"

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

	// MaxAllowUnreachedPerFile bounds how many `coverage.allow_unreached`
	// entries one file may declare. A workflow has few branches a suite cannot
	// reach, and a file recording hundreds is a record that has stopped meaning
	// anything.
	MaxAllowUnreachedPerFile = 200

	// MaxDefaultStubs bounds how many `stubs:` a file's `defaults:` block may
	// declare. Defaults exist to state a handful of things once (a `log` stub,
	// a base http answer); a block listing hundreds is a program hiding in a
	// fixture, not a default. A test file is author-controlled but still parsed
	// input (CLAUDE.md, "bound anything that consumes untrusted input"), and a
	// default is copied into every case, so its size multiplies.
	MaxDefaultStubs = 100

	// maxDefaultsDepth bounds how deeply the no-expressions scan descends into a
	// `defaults:` value before refusing it. A default is a fixture, so nesting
	// past this is a document doing something a default is not for; the bound
	// exists so the recursive scan cannot be driven to exhaust the stack by a
	// pathological file rather than to constrain an ordinary one.
	maxDefaultsDepth = 32
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

	// Defaults are the inputs, stubs, and signal sender a file states once for
	// every case, rather than pasting into each (issue #416). Each case
	// inherits them and may override them, by the boring, stated rules
	// [mergeDefaults] applies. Optional; a file that declares none behaves
	// exactly as it did before this field existed.
	Defaults *Defaults `yaml:"defaults"`

	// Tests are the cases this file declares, in the order they were written —
	// the order [Run] runs them in and reports them in, so a reader matching a
	// report back to the file does not have to search for it.
	Tests []Test `yaml:"tests"`

	// Coverage records branch-coverage decisions for this file (issue #420):
	// which of the workflow's steps no case is expected to reach, and why.
	// Optional; a file that declares none is held to the default that every
	// step should be reached by some case.
	Coverage *CoverageStanza `yaml:"coverage"`
}

// CoverageStanza is a file's record of the branches its cases deliberately do
// not reach.
//
// The precedent is `examplesWithoutTestFile`: "an entry here is a decision with
// a reason, never a gap." A step no case reaches is either a hole in the suite
// or a branch that cannot be reached from a `flow test` case at all (a gate on
// `!run.local`, which a local rehearsal can never satisfy, is the shape the
// enterprise examples hit). The first is a bug this feature exists to surface;
// the second is a fact about the workflow that belongs written down beside it,
// not silently tolerated.
type CoverageStanza struct {
	// AllowUnreached maps a step id to the reason no case reaches it. A step
	// named here is reported as an accepted residual rather than a gap, and
	// does not fail `--coverage-required`. The reason is required, because an
	// entry with none is the silent gap this record exists to refuse.
	AllowUnreached map[string]string `yaml:"allow_unreached"`
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
	//
	// Mutually exclusive with Trigger: a case either states its inputs or
	// replays a delivery that produces them, and a case doing both would be
	// asserting a mapping while overriding it.
	Inputs map[string]any `yaml:"inputs"`

	// Trigger replays a stored delivery against one of the workflow's declared
	// `triggers:`, so the argument mapping is a unit test rather than the one
	// part of a workflow debuggable only in production.
	Trigger *TriggerDelivery `yaml:"trigger"`

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

// A TriggerDelivery replays one stored arrival at a declared trigger.
//
// The case names the webhook, points at a delivery on disk, and says whether that
// delivery verified. What comes back is what the run would have started with:
// [Expectation.Inputs] asserts the mapping, and [Expectation.Refused] asserts the
// negative direction, where an unverifiable delivery produces no run at all.
//
// # What is real here and what is declared
//
// The mapping is real: the expressions under the webhook's `with:` are evaluated by
// [v1.BindWebhookTriggerInputs] — the same function a live receiver will call —
// against the stored delivery, bounded by the same CEL cost limit, and bound
// through the same [v1.BindRunInputs] a submit uses. A case that maps a field the
// payload does not carry fails here exactly as it would in production.
//
// The verification *outcome* is declared rather than computed, because there is no
// receiver yet and therefore no signature arithmetic anywhere in this repository.
// `signature: invalid` does not forge a bad signature; it says "this delivery did
// not verify" and asserts what Flowstate then does about it, which is the half that
// is a property of the file. When the receiver lands it supplies that same boolean
// from real material, and nothing about this shape changes — which is precisely why
// the refusal lives in the shared function rather than in the receiver.
type TriggerDelivery struct {
	// Webhook is the name one of the workflow's `- webhook:` entries declares.
	// An unknown name is refused when the file loads, naming what the workflow
	// does declare.
	Webhook string `yaml:"webhook"`

	// Payload is the stored delivery, resolved relative to the directory the
	// *.test.yaml lives in — the same rule [Test.Workflow] follows.
	//
	// The file is one JSON document with `headers` and `body`, because a
	// delivery is both: an idempotency key is usually a signature header, so a
	// fixture holding only a body could not exercise the key at all. It is read
	// under [v1.MaxWebhookPayloadBytes], the bound a live receiver will apply to
	// a request body.
	Payload string `yaml:"payload"`

	// Kind sets the run's trigger context directly, with no delivery involved:
	// `trigger: {kind: schedule, name: nightly}`.
	//
	// This is the other half of what a `trigger:` stanza can be, and it exists
	// because without it a branch guarded by `if: ${trigger.kind == "manual"}` is
	// conditional behaviour that only manifests in production — the thing
	// everybody hates about trigger-aware workflows elsewhere. A case says how the
	// run started and the workflow behaves accordingly, with no webhook, no
	// payload and no signature arithmetic anywhere near it.
	//
	// Mutually exclusive with [TriggerDelivery.Webhook], which sets the same
	// context from a *real* replay: kind `webhook`, the webhook's own name, and
	// the delivery id the receiver would have computed from the same key. A case
	// naming both would be stating a context while replaying one, and the two
	// could disagree.
	//
	// One of the kinds `v1.TriggerKinds` names, refused by name when the file
	// loads if it is not: a case guarding on `"schedual"` would otherwise assert
	// that a branch is never taken, and pass, forever.
	Kind string `yaml:"kind"`

	// Name is the trigger's own name for a context set directly — the schedule's,
	// or the webhook's where a case states one rather than replaying it. Empty is
	// legal and is what a manual start carries: a person is not a declared source.
	Name string `yaml:"name"`

	// Principal is who the context says started the run, read as
	// `${trigger.principal}`.
	//
	// Settable here and attested nowhere, which is the point and is also the
	// reason this must never be the shape a workflow authorizes on: a value a test
	// file can write is a value a test file can write. `flow test` says so by
	// letting a case set it freely — if a step's `if:` gates a destructive action
	// on this string, the case that fakes it passes, and the diff that made it
	// possible read as a security control. Authorization belongs on the trigger:
	// `manual: {allowed_principals: [...]}`, enforced by the server against an
	// identity it attested.
	Principal string `yaml:"principal"`

	// DeliveryID is the delivery a directly-set context names, read as
	// `${trigger.delivery_id}`. A replayed delivery computes its own through
	// [v1.WebhookDeliveryID], the same function the receiver uses, so a case that
	// replays never has to state one.
	DeliveryID string `yaml:"delivery_id"`

	// Signature says whether this delivery verified: "valid" (the default, and
	// what an omitted key means) or "invalid".
	//
	// Two words rather than a boolean, because `signature: invalid` reads as the
	// thing being described where `verified: false` reads as a switch being
	// flipped — and because there is deliberately no third value. A delivery
	// that could not be checked is refused exactly as one that failed a check
	// is; see [v1.BindWebhookTriggerInputs].
	Signature string `yaml:"signature"`
}

// The two values [TriggerDelivery.Signature] accepts.
const (
	SignatureValid   = "valid"
	SignatureInvalid = "invalid"
)

// Verified reports whether this delivery is to be treated as having verified.
func (d *TriggerDelivery) Verified() bool { return d.Signature != SignatureInvalid }

// Replays reports whether this stanza replays a stored delivery, rather than
// stating a trigger context outright.
//
// Keyed on the webhook's name rather than on the payload path, because that is
// the field that says which of the two things the stanza is: a delivery is
// addressed to a declared source, and a stated context is addressed to nobody.
func (d *TriggerDelivery) Replays() bool { return d.Webhook != "" }

// Context is the trigger context a directly-stated stanza sets on the run.
//
// Only meaningful when [TriggerDelivery.Replays] is false; a replay derives its
// own from the trigger it replayed against, so that a case asserting on
// `${trigger.name}` asserts against what the receiver would really have recorded.
func (d *TriggerDelivery) Context() *v1.TriggerContext {
	return &v1.TriggerContext{
		Kind:       d.Kind,
		Name:       d.Name,
		Principal:  d.Principal,
		DeliveryId: d.DeliveryID,
	}
}

// Defaults is what a file states once for every case (issue #416): the base
// inputs, the stubs, and the signal sender each case would otherwise repeat.
//
// A test file is a fixture, not a program, so nothing here may hold an
// expression: a `${...}` in a default is refused when the file loads, named by
// its position, rather than carried silently into a case that then computes
// something the author never wrote (CLAUDE.md, "diagnostics are a feature").
// Expressions in a *case* are unaffected; a stub's own `returns:` is still free
// to carry one, because a case is where a value is allowed to depend on the run.
//
// The merge rules [mergeDefaults] applies are deliberately boring:
//
//   - Inputs merge one level. A case's `inputs:` entry replaces only that key;
//     every other default key remains. A scalar a case sets wins over the same
//     scalar in the defaults.
//   - A case's stubs come first and the file's defaults are appended as
//     fallbacks, except that a case stub selecting the same thing the same way
//     as a default replaces it rather than doubling it. "The same way" includes
//     the `where:` filter, so a default catch-all is a true fallthrough: a
//     case's filtered stub for the same task is tried first and still fires (see
//     [stubTargetKey] and [mergeDefaults]).
//   - Sender fills in only where a case's signal omits its own `sender:`.
//     Explicit beats inherited, so a signal that names a sender keeps it.
type Defaults struct {
	// Inputs are the base bindings every case starts from, before its own
	// `inputs:` are merged over them one key at a time.
	Inputs map[string]any `yaml:"inputs"`

	// Stubs are the stubs every case starts from. A case's own stubs append,
	// unless one targets the same task or step id, which replaces the default
	// for that target.
	Stubs []Stub `yaml:"stubs"`

	// Sender is the scripted signal sender a case's signals inherit when they
	// omit their own. It is the one place a whole file's signals share an
	// approver identity rather than restating the five-line stanza per signal.
	Sender *ScriptedIdentity `yaml:"sender"`
}

// Stub replaces one task's real behavior with a canned answer.
//
// Stubbing happens at the task boundary and nothing lower (#155): control
// flow, CEL evaluation, retries, `if:`, loops, and `undo:` registration all
// run for real through the actual local driver, and only the effect —
// whatever the task would have done outside the process — is replaced.
type Stub struct {
	// Task is the task name this replaces, exactly as a step's own task key
	// names it: `http`, `log`, a plugin task's name. Mutually exclusive with
	// Step; a stub names one or the other, never both and never neither.
	Task string `yaml:"task"`

	// Step is the id of the workflow step this replaces, as an alternative to
	// naming the Task the step invokes. It is the workflow's own name for the
	// thing being stubbed, checked against the compiled workflow's step ids
	// (an unknown id is refused with a did-you-mean suggestion), so a stub
	// scoped this way survives an endpoint rename that a `where:` clause
	// retyping the step's url would not: the reference is the step, not a value
	// the step happens to carry.
	//
	// A step stub answers only invocations of that one step, matched by the
	// step id the engine records on each node's context ([v1.TaskStepFromContext]),
	// so two steps sharing a task are told apart without a `where:` at all. It
	// does not answer a compensation (`undo:`) call, which runs off the run
	// level context carrying no step id; a case exercising an undo stubs it by
	// task and `where:`, the genuine case `where:` remains for. Mutually
	// exclusive with Task.
	Step string `yaml:"step"`

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
	// still checked, as an empty identity, which a real policy's `allow:`
	// rule can never match (every rule requires a non-empty subject,
	// namespace, or claim; see signalpolicy.go's ruleMatchesEverySender), so
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
	// policy rule's `subject:` as `<issuer>#<subject>`; see
	// [v1.QualifiedSubject].
	Subject string `yaml:"subject"`

	// Issuer identifies which identity provider would have attested Subject.
	Issuer string `yaml:"issuer"`

	// Namespace is the tenant this identity belongs to, matched against a
	// policy rule's `namespace:`.
	Namespace string `yaml:"namespace"`

	// Claims are additional facts, matched against a policy rule's `claims:`
	// every key the rule names must be present here with the same value.
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

	// Inputs, when set, must equal the inputs a replayed delivery produced,
	// exactly — every named input present with the expected value, and no
	// unexpected one. Only meaningful alongside [Test.Trigger]: it is the
	// assertion about the *mapping*, which is the part of a trigger a file
	// controls.
	//
	// Compared against the bound inputs, so a declaration's `default:` shows up
	// here for an input the delivery does not carry — that is what the run will
	// see, and asserting anything else would be asserting a different run.
	Inputs map[string]any `yaml:"inputs"`

	// Refused asserts that the delivery was refused and no run happened.
	//
	// The negative direction, and the reason it is a field rather than
	// `failed: true`: a refused delivery does not produce a failed run, it
	// produces no run — which is the whole point of deciding verification before
	// anything attacker-chosen is evaluated. A case asserting this fails if the
	// delivery is accepted, whatever the run would then have done.
	Refused *bool `yaml:"refused"`

	// IdempotencyKey, when set, must equal the key the replayed delivery
	// evaluated to. Only meaningful alongside [Test.Trigger].
	//
	// Worth asserting rather than trusting: the key decides whether a
	// redelivery starts a second run, and an expression that reaches the wrong
	// header is wrong in the direction nothing else notices — every delivery
	// gets a different key, or every delivery gets the same one, and both look
	// like working software until a retry happens.
	IdempotencyKey string `yaml:"idempotency_key"`

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
	// outputs, whether they succeeded, were tolerated, or were the step whose
	// failure ended the run.
	//
	// Checked on a failed run too, unlike Outputs above: a run that fails hands
	// back the partial transcript ([v1.PartialTranscript]), which is a record of
	// what it did rather than an answer it produced. Before that existed this was
	// silently skipped whenever the run failed, so a case asserting `failed: true`
	// could claim anything at all here and be believed (issue #453).
	Ran []string `yaml:"ran"`

	// Skipped names steps that must not have executed — absent from the run's
	// step outputs, because their `if:` did not hold or the run never reached
	// them. Checked on a failed run for the reason Ran is, and a step *after* the
	// step that failed is one this can now legitimately name.
	Skipped []string `yaml:"skipped"`

	// Others, when set to "skipped", closes the `ran:` claim: every step the
	// workflow has that Ran does not name must have been skipped (issue #416).
	// It is the fail-closed, deny-by-default shape (CLAUDE.md, "fail closed")
	// applied to expectations, so a case need not hand-enumerate the complement
	// of the steps it ran, and adding a step to the workflow fails a closed
	// claim loudly rather than passing silently around the new step.
	//
	// The only accepted value is "skipped"; anything else is refused when the
	// file loads, named by its position. Empty means the claim stays open, the
	// behavior of every case before this field existed: `ran:` asserts
	// membership and says nothing about the rest.
	//
	// The complement is computed over the steps that can appear in the run's
	// top level transcript, the same set `ran:` and `skipped:` are checked
	// against, so a loop body step (whose outputs travel inside the loop's
	// results, never at the top level) is not miscounted as a step that should
	// have been skipped.
	Others string `yaml:"others"`
}

// OthersSkipped is the one accepted value of [Expectation.Others]: the whole
// point of the field is to state absence, so there is exactly one thing to say.
const OthersSkipped = "skipped"

// Load reads and parses a `*.test.yaml`, applying [MaxTestFileBytes] before
// anything is unmarshaled and [MaxTestsPerFile]/[MaxStubsPerTest]/
// [MaxSignalsPerTest] after.
//
// The byte bound is applied to the stream through [readBounded] rather than to
// what a prior [os.Stat] reported, for the reasons that function records: a
// fixture path naming `/dev/zero` stats as empty and reads without end, and a
// file swapped between the sizing and the reading is bounded by neither size.
func Load(path string) (*File, error) {
	data, err := readBounded(path, MaxTestFileBytes, "test file")
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
	if stanza := file.Coverage; stanza != nil {
		if len(stanza.AllowUnreached) > MaxAllowUnreachedPerFile {
			return nil, fmt.Errorf("coverage.allow_unreached declares %d entries, more than the limit of %d",
				len(stanza.AllowUnreached), MaxAllowUnreachedPerFile)
		}
		for step, reason := range stanza.AllowUnreached {
			if step == "" {
				return nil, fmt.Errorf("coverage.allow_unreached has an entry with no step id")
			}
			// A reason is required, because an entry with none is exactly the
			// silent gap this record exists to refuse: "a decision with a
			// reason, never a gap." Name the offending step so the fix is
			// obvious.
			if strings.TrimSpace(reason) == "" {
				return nil, fmt.Errorf("coverage.allow_unreached[%q] has no reason; "+
					"record why no case reaches this step, or remove the entry and let it be a gap", step)
			}
		}
	}

	// Validated then merged before anything below bounds or checks a case, so
	// every per-test check runs against the effective test a case actually
	// runs, not the sparse one the author wrote (issue #416). A default is a
	// fixture, so it may hold no expression; that is refused here, by position,
	// rather than carried into a case.
	if err := checkDefaults(file.Defaults); err != nil {
		return nil, err
	}
	if file.Defaults != nil {
		for i := range file.Tests {
			file.Tests[i] = mergeDefaults(file.Defaults, file.Tests[i])
		}
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
			switch {
			case stub.Task == "" && stub.Step == "":
				return nil, fmt.Errorf("test %q stub %d names neither a task nor a step; "+
					"give one, either `task: <name>` or `step: <id>`", test.Name, j+1)
			case stub.Task != "" && stub.Step != "":
				return nil, fmt.Errorf("test %q stub %d names both a task (%q) and a step (%q); "+
					"a stub targets one or the other, never both", test.Name, j+1, stub.Task, stub.Step)
			}
			if stub.Returns != nil && stub.Fails != nil {
				return nil, fmt.Errorf(
					"test %q stub %d for %s declares both returns and fails; a stubbed call either succeeds or fails, not both",
					test.Name, j+1, stubTarget(&stub))
			}
		}
		if err := checkOthers(&test); err != nil {
			return nil, err
		}
		if err := checkTrigger(&test, requireWorkflow); err != nil {
			return nil, err
		}
	}

	return &file, nil
}

// stubTarget names a stub by what it targets, for a diagnostic: `task "http"`
// or `step "debit"`. One of the two is always set by the time this is reached.
func stubTarget(s *Stub) string {
	if s.Step != "" {
		return fmt.Sprintf("step %q", s.Step)
	}
	return fmt.Sprintf("task %q", s.Task)
}

// stubTargetKey identifies a stub by what it selects, so a case stub can be said
// to replace a default stub aimed at the same thing (issue #416). A task stub
// and a step stub never collide, because the two id spaces are kept apart by the
// prefix.
//
// The `where:` filter is part of the identity, and deliberately so: a task's
// stubs are a switch tried in order (see [Stub.Where]), so two stubs for the
// same task with different filters are different cases of that switch, not one
// overriding the other. A default catch-all `- task: log` (no filter) and a
// case's `- task: log where: <asserts the message>` therefore coexist: the case
// asserts one message and the default answers every other log call, which is the
// exact shape the corpus uses. A case stub replaces a default only when it
// selects the same thing the same way, which is what "override this answer"
// means.
func stubTargetKey(s *Stub) string {
	target := "task:" + s.Task
	if s.Step != "" {
		target = "step:" + s.Step
	}
	return target + "\x00" + s.Where
}

// checkOthers refuses an `expect.others:` value that is not the one thing the
// field is allowed to say, named by its position (CLAUDE.md, "diagnostics are a
// feature"). Empty is fine: it means the `ran:` claim stays open.
func checkOthers(test *Test) error {
	switch test.Expect.Others {
	case "", OthersSkipped:
		return nil
	default:
		return fmt.Errorf("test %q expect.others: %q is not a value it accepts; the only value is %q, "+
			"which asserts every step not named in `ran:` was skipped",
			test.Name, test.Expect.Others, OthersSkipped)
	}
}

// checkTrigger refuses a trigger case that cannot mean what it says, when the
// file loads rather than when the case runs.
//
// Everything here is knowable from the test file alone; whether the workflow
// declares the named webhook is checked in [runCase], where the compiled workflow
// exists. requireWorkflow is false for [LoadSource], which has no directory to
// resolve a payload path against — the same reason a case's `workflow:` is not
// required there.
func checkTrigger(test *Test, requireWorkflow bool) error {
	trigger := test.Trigger
	if trigger == nil {
		if test.Expect.Refused != nil {
			return fmt.Errorf("test %q expects a refusal but replays no delivery; a refusal is what a "+
				"`trigger:` produces, so give the case one", test.Name)
		}
		if test.Expect.Inputs != nil {
			return fmt.Errorf("test %q expects mapped inputs but replays no delivery; `expect.inputs:` "+
				"asserts what a `trigger:` produced, and a case that states its own `inputs:` already "+
				"knows them", test.Name)
		}
		return nil
	}

	if trigger.Webhook != "" && trigger.Kind != "" {
		return fmt.Errorf("test %q trigger: names both a webhook (%q) and a kind (%q); a stanza either "+
			"replays a delivery, which decides the context, or states the context outright, which "+
			"needs no delivery — never both, because the two could disagree about the same run",
			test.Name, trigger.Webhook, trigger.Kind)
	}

	if !trigger.Replays() {
		return checkTriggerContext(test, trigger)
	}

	if trigger.Payload == "" {
		return fmt.Errorf("test %q trigger %q: names no payload; write `payload: ./testdata/<file>.json`, "+
			"a stored delivery with `headers` and `body`", test.Name, trigger.Webhook)
	}
	if !requireWorkflow {
		return fmt.Errorf("test %q trigger %q: a delivery is read relative to the test file's own "+
			"directory, and these cases were given as bytes with no directory to resolve it against",
			test.Name, trigger.Webhook)
	}
	switch trigger.Signature {
	case "", SignatureValid, SignatureInvalid:
	default:
		return fmt.Errorf("test %q trigger %q: signature: %q is not a value it accepts; write %q or %q, "+
			"which say whether this delivery verified — there is no third answer, because a delivery "+
			"that could not be checked is refused exactly as one that failed a check",
			test.Name, trigger.Webhook, trigger.Signature, SignatureValid, SignatureInvalid)
	}
	if len(test.Inputs) > 0 {
		return fmt.Errorf("test %q trigger %q: the case also states `inputs:`; a trigger case's inputs "+
			"come from the delivery, so stating them here would override the mapping the case exists "+
			"to check", test.Name, trigger.Webhook)
	}

	return nil
}

// checkTriggerContext refuses a directly-stated trigger context that cannot mean
// what it says.
//
// The kind is the whole of it, and it is checked against the closed set rather
// than accepted as free text for a reason worth being explicit about: a case
// exercising `if: ${trigger.kind == "schedule"}` by stating `kind: schedual`
// would assert that the branch is *not* taken, and it would pass — a green test
// certifying behaviour nobody wrote. A closed set turns that into a refusal
// naming the kinds, which is the difference between a harness that checks a
// belief and one that confirms it.
//
// The other three fields are free strings, because they are: a name, a subject
// and a delivery id are whatever the trigger that produced them said.
func checkTriggerContext(test *Test, trigger *TriggerDelivery) error {
	if trigger.Kind == "" {
		return fmt.Errorf("test %q trigger: says neither how the run started nor what delivery started "+
			"it; write `kind: <%s>` to state the context a run reads as `${%s.kind}`, or `webhook: "+
			"<name>` with a `payload:` to replay a stored delivery",
			test.Name, strings.Join(v1.TriggerKinds(), "|"), v1.TriggerRoot)
	}

	if !v1.KnownTriggerKind(trigger.Kind) {
		return fmt.Errorf("test %q trigger: kind: %q is not a kind Flowstate starts runs with; the kinds "+
			"are %s. A case stating one that cannot occur asserts a branch is never taken, and passes",
			test.Name, trigger.Kind, strings.Join(v1.TriggerKinds(), ", "))
	}

	if test.Expect.Inputs != nil {
		// `expect.inputs:` asserts what a *mapping* produced, and a stated context
		// maps nothing: the case's own `inputs:` are the inputs. Refused rather
		// than ignored, because an assertion nothing evaluates is a test that
		// reports success for a claim it never checked — the one outcome a harness
		// must not have.
		return fmt.Errorf("test %q trigger: states a context and expects mapped inputs; `expect.inputs:` "+
			"asserts what replaying a delivery produced, and a case that states its own `inputs:` "+
			"already knows them", test.Name)
	}

	if trigger.Payload != "" || trigger.Signature != "" {
		return fmt.Errorf("test %q trigger: states a context (`kind: %s`) and also a delivery; a payload "+
			"and a signature belong to a replay, which is written `webhook: <name>` and derives its own "+
			"context", test.Name, trigger.Kind)
	}

	if test.Expect.Refused != nil {
		return fmt.Errorf("test %q trigger: states a context and expects a refusal; a refusal is what "+
			"replaying an unverifiable *delivery* produces, and a stated context starts the run it "+
			"describes", test.Name)
	}

	return nil
}

// checkDefaults refuses a `defaults:` block that holds an expression anywhere,
// because a test file is a fixture and not a program (issue #416): a `${...}` a
// case is free to write is, in a default, a value the author did not mean to
// depend on a run they cannot see from the defaults block. It also bounds the
// block's stub list, since a default is copied into every case and so its size
// multiplies.
//
// Positioned the way the rest of this loader positions a diagnostic: by naming
// the field a reader has to look at (`defaults.inputs.version`,
// `defaults.stubs[0].returns.reference`, `defaults.sender.claims`), a
// *.test.yaml being identified throughout here by name rather than by line and
// column (see [checkScriptedIdentity]).
func checkDefaults(d *Defaults) error {
	if d == nil {
		return nil
	}
	if len(d.Stubs) > MaxDefaultStubs {
		return fmt.Errorf("defaults declares %d stubs, more than the limit of %d", len(d.Stubs), MaxDefaultStubs)
	}
	for name, v := range d.Inputs {
		if err := checkNoExpressions("defaults.inputs."+name, v, 0); err != nil {
			return err
		}
	}
	for i := range d.Stubs {
		s := d.Stubs[i]
		where := fmt.Sprintf("defaults.stubs[%d]", i)
		if err := checkNoExpressions(where+".where", s.Where, 0); err != nil {
			return err
		}
		if err := checkNoExpressions(where+".returns", s.Returns, 0); err != nil {
			return err
		}
	}
	if d.Sender != nil {
		if err := checkNoExpressions("defaults.sender", d.Sender, 0); err != nil {
			return err
		}
	}
	return nil
}

// checkNoExpressions descends a default value and refuses the first string that
// carries a `${` fence, naming its position. The bound on depth is the
// fail-closed answer to a pathological fixture rather than a constraint on an
// ordinary one (CLAUDE.md, "bound anything that consumes untrusted input"): the
// walk is recursive, so recursion gets its own bound.
func checkNoExpressions(where string, v any, depth int) error {
	if depth > maxDefaultsDepth {
		return fmt.Errorf("%s: nests more than %d levels deep, deeper than a default is meant to go",
			where, maxDefaultsDepth)
	}
	switch value := v.(type) {
	case nil:
		return nil
	case string:
		if strings.Contains(value, "${") {
			return fmt.Errorf("%s holds the expression %q; a test file's `defaults:` is a fixture, "+
				"so it may not hold an expression. Write the literal value, or move it into the case that needs it",
				where, value)
		}
		return nil
	case *ScriptedIdentity:
		if value == nil {
			return nil
		}
		for _, field := range []struct {
			name  string
			value string
		}{
			{"subject", value.Subject},
			{"issuer", value.Issuer},
			{"namespace", value.Namespace},
		} {
			if err := checkNoExpressions(where+"."+field.name, field.value, depth+1); err != nil {
				return err
			}
		}
		for _, name := range slices.Sorted(maps.Keys(value.Claims)) {
			if err := checkNoExpressions(where+".claims."+name, value.Claims[name], depth+1); err != nil {
				return err
			}
		}
		return nil
	case []any:
		for i, element := range value {
			if err := checkNoExpressions(fmt.Sprintf("%s[%d]", where, i), element, depth+1); err != nil {
				return err
			}
		}
		return nil
	}

	// A nested mapping a YAML decoder hands back is its own choice of type, so
	// it is reflected rather than switched on map[string]any alone, since missing a
	// map would let a `${...}` inside it pass as literal text, the silent
	// nothing "diagnostics are a feature" forbids. Mirrors [compileReturnValue].
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Map && rv.Type().Key().Kind() == reflect.String {
		for _, key := range rv.MapKeys() {
			if err := checkNoExpressions(where+"."+key.String(), rv.MapIndex(key).Interface(), depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	if rv.Kind() == reflect.Slice {
		for i := 0; i < rv.Len(); i++ {
			if err := checkNoExpressions(fmt.Sprintf("%s[%d]", where, i), rv.Index(i).Interface(), depth+1); err != nil {
				return err
			}
		}
		return nil
	}

	return nil
}

// mergeDefaults folds a file's `defaults:` into one case, producing the
// effective test that case runs (issue #416). The rules are the boring ones
// [Defaults] documents: inputs merge one level with the case winning per key,
// stubs append except a case stub replaces a default targeting the same task or
// step, and a default sender fills in only where a case's signal named none.
//
// It never mutates the case or the defaults in place: a case's own maps and
// slices are the author's, and a second case merging the same defaults must see
// them untouched.
func mergeDefaults(d *Defaults, test Test) Test {
	// Inputs: one level. Start from the defaults, then let the case replace
	// whole keys. A nested map under a key is replaced wholesale by the case's,
	// not deep-merged, which is the "one level" the rules promise.
	//
	// Not into a case that replays a delivery: a trigger case's inputs are
	// produced by the mapping under test, so inheriting a file's base inputs
	// would silently override what the case exists to check — and would refuse
	// the case ([checkTrigger]) for something its author never wrote.
	if len(d.Inputs) > 0 && test.Trigger == nil {
		merged := make(map[string]any, len(d.Inputs)+len(test.Inputs))
		maps.Copy(merged, d.Inputs)
		maps.Copy(merged, test.Inputs)
		test.Inputs = merged
	}

	// Stubs: the case's own come first, then the file's defaults are appended as
	// fallbacks, minus any a case stub selects the same way, which is replaced
	// rather than doubled. Defaults last is what makes a default catch-all
	// (`- task: log` with no filter) a fallthrough rather than a shadow: a case
	// asserting a specific log message with `where:` is tried first and still
	// fires, and the catch-all answers only the calls that assertion did not
	// name. Reversing this would let an unconditional default match before a
	// case's more specific stub, silently defeating the assertion (see
	// [stubTargetKey]).
	if len(d.Stubs) > 0 {
		replaced := make(map[string]bool, len(test.Stubs))
		for i := range test.Stubs {
			replaced[stubTargetKey(&test.Stubs[i])] = true
		}
		merged := make([]Stub, 0, len(d.Stubs)+len(test.Stubs))
		merged = append(merged, test.Stubs...)
		for i := range d.Stubs {
			if replaced[stubTargetKey(&d.Stubs[i])] {
				continue
			}
			merged = append(merged, d.Stubs[i])
		}
		test.Stubs = merged
	}

	// Sender: explicit beats inherited. A signal that named its own sender
	// keeps it; only one that omitted `sender:` inherits the default (the
	// resolved open question in #416).
	if d.Sender != nil && len(test.Signals) > 0 {
		signals := make([]SignalScript, len(test.Signals))
		copy(signals, test.Signals)
		for i := range signals {
			if signals[i].Sender == nil {
				signals[i].Sender = d.Sender
			}
		}
		test.Signals = signals
	}

	return test
}

// WorkflowPath resolves a test's `workflow:` relative to the *.test.yaml file
// that declared it.
func WorkflowPath(testFile string, test *Test) string {
	if filepath.IsAbs(test.Workflow) {
		return test.Workflow
	}
	return filepath.Join(filepath.Dir(testFile), test.Workflow)
}

// DeliveryPath resolves a trigger case's `payload:` the same way [WorkflowPath]
// resolves its workflow, and for the same reason: a fixture travels with the test
// that reads it, not with whatever directory `flow test` was invoked from.
//
// Empty for a case that replays nothing.
func DeliveryPath(testFile string, test *Test) string {
	if test.Trigger == nil || test.Trigger.Payload == "" {
		return ""
	}
	if filepath.IsAbs(test.Trigger.Payload) {
		return test.Trigger.Payload
	}
	return filepath.Join(filepath.Dir(testFile), test.Trigger.Payload)
}
