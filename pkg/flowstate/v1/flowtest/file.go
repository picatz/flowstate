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
// pkg/flowstate/v1/internal/conformance) productized for an author's own workflow.
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
// # What a green case proves about identity, and what it does not
//
// The sentence to read first, because it is the one an author is most likely
// to assume the other way round: **a green case says nothing about whether the
// identity it names would be allowed to do any of this in production.**
// Nothing attested a `starter:` or a `sender:` - they are what the file says
// they are - and every policy a *deployment* installs is either absent from
// this process or evaluated against somebody else entirely.
//
// What reads a [Test.Starter], exhaustively: the workflow's own `signals:`
// policy, through [v1.SignalPolicyCheck] - the function `FlowstateServer.Signal`
// itself calls - reached from [v1.NewPolicedLocalSignals] in runCase. That is
// a rule's `subject:`, `issuer:`, `namespace:` and `claims:` matching a
// scripted [SignalScript.Sender], and `distinct_from_starter:` comparing that
// sender's [v1.QualifiedSubject] against the starter's. Nothing else in this
// package passes the value anywhere.
//
// What does not read it, each for a reason worth stating separately:
//
//   - **`run.identity`.** Empty for every case, whatever `starter:` says, with
//     `run.local` true - see [Test.Starter]. An `if:` keyed on
//     `run.identity.namespace` therefore takes the empty branch here and may
//     take another one in production.
//
//   - **Task-shape policy** ([v1.TaskPolicy]). Every dispatch does reach
//     [v1.CheckTaskPolicy] - eval.go calls it at the seam both drivers share -
//     and it is handed `scope.GetIdentity()`, which is that same empty
//     identity. It is never handed a `starter:`.
//
//     Which policy it consults is a property of the *process*, not of the
//     case, and the distinction is worth stating precisely because the
//     convenient version of it is false. The `flow test` **command** installs
//     none: `--task-policy` is declared on `flow worker`, `flow run local`,
//     `flow mcp`, `flow serverdev` and `flow task run`, and deliberately not
//     on `flow test`, so under that command [v1.TaskPolicyIn] finds nothing
//     and every dispatch is allowed. But a policy is installed process-wide
//     by [v1.SetDefaultTaskPolicy], runCase does not clear it with
//     [v1.NewContextWithTaskPolicy], and `flow test`'s machinery is reachable
//     from other hosts: the `flowstate_test` MCP tool runs
//     [RunSourceContext] in whatever process serves it, so under
//     `flow mcp --task-policy` a case's dispatches *are* governed by that
//     deployment's policy. A rehearsal inherits whatever the hosting process
//     installed.
//
//     Which is its own trap, and the reason the identity clause above is not
//     a footnote: a rule that reads `identity.namespace` or
//     `identity.subject` is matched against the empty identity there, not
//     against `starter:`, however the case names one. So a policy admitting
//     only a named namespace refuses every stubbed dispatch in that host, and
//     an author reading the denial has no `starter:` to change that would
//     make any difference. [v1.TaskPolicyDeniedError] says exactly that in
//     its own message (#652 item 3): a denial from this venue names the
//     identity the rule was evaluated against, and where that identity is
//     empty it says so and says why — that a rehearsal has one only where
//     `flow run local --as-*` named one, and never under `flow test`. Which
//     is the difference between "the rule matched me" and "the rule had
//     nothing to match", and it is not one an author can otherwise derive
//     from a denial. The remedy it names is the policy *this process*
//     installed rather than a `--task-policy` flag, precisely because this
//     command does not have one.
//
//   - **Egress policy** (`netpolicy`). Never consulted, because no request is
//     made: a step that would reach the network is answered by its stub, so
//     there is no URL for a policy to refuse. `--egress-policy` is likewise
//     not offered on `flow test`.
//
//   - **Secret access policy** (`auth.SecretAccessPolicy`). Consulted, and this
//     is the surface where the gap is easiest to miss, because the mechanism is
//     real and the answer is fixed: [secretRuntime] compiles `allow: ["true"]`
//     and runs it under the constant identity `flow-test#flow-test`, not under
//     `starter:`. So every `${secret(...)}` a case binds resolves, and a
//     deployment rule keyed on `identity.namespace` is neither loaded nor
//     matched.
//
// The line all four sit on is CLAUDE.md's: report what is a property of the
// file, and stay silent about what a deployment decides. `signals:` is written
// in the Flowfile, so `flow test` owns it; task-shape, egress and secret
// access rules are installed by whoever runs the worker, and a case whose
// verdict turned on which policy file happened to be passed on the command
// line would be a test of that machine rather than of the workflow — which is
// exactly what a case gets when the hosting process installed one, so a suite
// meant to be portable should be run by a command that takes no such flag.
// A case
// that wants to exercise one of those *denials* writes it against the policy's
// own package - [v1.TaskPolicy.Check] and `auth.SecretPolicy` are pure
// functions of (thing, identity), tested that way today, and need no workflow
// at all - which is the same answer [secretRuntime]'s own doc has been giving
// for secrets since it was written. See #652 item 2, where this was decided
// and deliberately not built.
//
// That decision is now enforced rather than only recorded. `cmd/flow`'s
// TestFlowTestTakesNoDeploymentPolicyFlags asserts `flow test` declares
// neither `--task-policy` nor `--egress-policy` — while every verb that runs a
// real dispatch still declares the first, so the assertion means "deliberately
// absent here" rather than "absent everywhere" — and
// [TestACaseCannotDeclareADeploymentPolicy] asserts a case file cannot smuggle
// one in through a key of its own. A paragraph of prose beside a one-line flag
// registration is the same value written down twice, and only one of the two
// is what the program does.
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

	"github.com/goccy/go-yaml/parser"

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

	// Vars are the literal values this file states once and references
	// everywhere (#1072): a whole-value `${vars.x}` in a fixture position is
	// substituted at load, and `expect.check:` reads `vars.x` at evaluation.
	// Literals only — any `${` inside one is refused, including a reference
	// to another var — so there is no evaluation order and no cycles; see
	// vars.go for the design and the one deliberate asymmetry (a stub's
	// `vars.` stays the workflow's).
	Vars map[string]any `yaml:"vars"`

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
	// AllowUnreached maps a step id — or, since issue #801, a switch arm's key —
	// to the reason no case reaches it. An entry named here is reported as an
	// accepted residual rather than a gap, and does not fail
	// `--coverage-required`. The reason is required, because an entry with none
	// is the silent gap this record exists to refuse.
	//
	// A switch arm's key is [SwitchArm.Key]: `<step>:case[<i>]` for a case
	// holding one literal, `<step>:case[<i>][<j>]` for member j of a case listing
	// several, and `<step>:default`. `flow test` prints the key to record beside
	// the diagnostic for every arm it reports, so nobody has to derive one.
	//
	// One map for both, rather than a second stanza: an entry here answers one
	// question — "no case reaches this, and here is why" — and the answer does
	// not change with what kind of thing is unreached. An entry naming neither a
	// step nor an arm of any workflow the file targets is stale, and fails the
	// same way an unrecorded gap does.
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
	//
	// One policy surface reads it, and the others a deployment installs read
	// nothing of it at all — see the package doc's "What a green case proves
	// about identity". The short version: `signals:` is a control the
	// *workflow file* declares, so `flow test` exercises it; task-shape
	// policy, egress policy and secret access policy are controls a
	// *deployment* installs, and `flow test` neither takes them nor evaluates
	// them under this identity.
	Starter *ScriptedIdentity `yaml:"starter"`

	// Cases are the rows of a table entry (#924 slice 2): one run each, with
	// the enclosing entry standing to a row exactly as `defaults:` stands to
	// a case. An entry that declares rows does not itself run — it is the
	// template they are merged over — and a row's own value always beats the
	// entry's, the one direction every merge in this file takes.
	//
	// The house Go convention this mirrors is the charter's: "slice-of-struct
	// tables with a `name` field, one `t.Run` per case" (#405). Report
	// identity is `<entry name>/<row name>`, the two-level naming `t.Run`
	// gives a Go table, carried in the existing [v1.TestCase] name so nothing
	// reading a report needs a schema change to see it.
	//
	// One level only. A row that declares its own `cases:` is refused rather
	// than flattened, because a table of tables is a shape whose merge order
	// nobody wrote down and whose report identity has no obvious spelling.
	Cases []Test `yaml:"cases"`

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
// The verification *outcome* is computed when the case supplies the material, and
// declared when it does not (#935). A case whose `secrets:` binds every key the
// trigger's `verify:` names gets the outcome from [v1.VerifyWebhookDelivery] — the
// same arithmetic the served receiver runs (server/webhook.go), over the fixture's
// exact `body` bytes, at the virtual clock's epoch — so a fixture with a wrong
// signature fails here exactly as a live delivery would be refused. With no keys
// bound, `signature: valid|invalid` declares the boolean instead: it does not
// forge a bad signature, it says "this delivery did not verify" and asserts what
// Flowstate then does about it, which is the keyless rehearsal this stanza always
// supported. Binding the keys *and* declaring `signature:` is refused, naming
// both, because a declaration that could contradict the arithmetic is the
// two-spellings bug as a test fixture.
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
	//
	// Legal only while the case binds none of the keys the trigger's `verify:`
	// names. A case whose `secrets:` binds them all has the outcome *computed*
	// from that material instead ([v1.VerifyWebhookDelivery], the receiver's
	// own function), and declaring this beside the arithmetic is refused —
	// see the type's doc.
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
	// Workflow is the Flowfile every case runs against unless it names its
	// own (#924 slice 1) — resolved exactly as a case's own `workflow:` is,
	// relative to the test file's directory, because it becomes the case's
	// value before anything resolves. The one fact 151 of the corpus's 151
	// cases restated identically, now stated once; a case that does name a
	// workflow keeps it, per the merge rules' one direction.
	Workflow string `yaml:"workflow"`

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

	// Check holds claims every case in the file must satisfy (#1072),
	// prepended to each case's own `expect.check:` by [mergeDefaults]. Bare
	// CEL carries no `${` fence, so the #416 fixture rule — a default may
	// hold no expression *value* — is untouched: a claim is a predicate the
	// file states, not a value a case inherits.
	Check []CheckClaim `yaml:"check"`
}

// Stub replaces one task's real behavior with a canned answer.
//
// Stubbing happens at the task boundary and nothing lower (#155): control
// flow, CEL evaluation, retries, `if:`, loops, and `undo:` registration all
// run for real through the actual local driver, and only the effect —
// whatever the task would have done outside the process — is replaced.
type Stub struct {
	// fromDefaults records that this stub reached the case through the file's
	// `defaults:` rather than being written on the case itself — set by
	// [mergeDefaults], invisible to the YAML form, and read by the
	// unused-stub report (#926): a file-level catch-all is *expected* to go
	// unanswered by cases that never invoke its task, so it is exempt from
	// the warning a case's own idle stub earns.
	fromDefaults bool

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

	// Times bounds how many invocations this stub answers before it retires
	// and the list falls through to the next matcher (#927). Absent means
	// unbounded — every stub before this field existed answers forever, and
	// still does. It is what makes a stub list a *script* rather than only a
	// switch: the canonical retry test, fail once and then succeed, is two
	// stubs for one step, the first carrying `times: 1` and `fails:`, the
	// second answering the recovery — inexpressible before this existed,
	// because the first match answered every attempt and `retry:` was
	// testable only to exhaustion.
	//
	// Consumption is counted per answer, `returns:` and `fails:` alike, and
	// the budget is per case: every case (and every seeded schedule of one)
	// starts the count over. A drained stub shows up in an unmatched-stub
	// failure with its spent budget, so an invocation that fell past it is
	// explained rather than mysterious.
	//
	// One caution, and the schedule explorer enforces it rather than this
	// package hiding it: two `parallel:` branches invoking one task drain a
	// shared `times:` budget in whichever order the schedule runs them, which
	// is a real observable `--seeds` will legitimately flag. Scope such a
	// stub with `step:`, which already tells steps sharing a task apart.
	//
	// A pointer so an explicit `times: 0` is told apart from the field being
	// absent, and refused when the file loads: a stub that can answer nothing
	// asserts nothing.
	Times *int `yaml:"times"`

	// Response answers the invocation with a raw *response* instead of shaped
	// outputs, and the task then evaluates its own deferred inputs over it —
	// for `http`, the step's `outputs:` and `expect:` run for real against
	// `status_code`, `headers`, `body` (and `response.json` under
	// `parse_json: true`) exactly as they would against a live response
	// (#925). That is the difference from Returns, which supplies the
	// post-shaping outputs and leaves the mapping — the exact expression a
	// path typo lives in — unexercised by every green case.
	//
	// The fields and their meanings are the task's own: `http` takes
	// `status_code` (200 when omitted), `body` (a string verbatim, or a map
	// or list encoded as its JSON), and `headers` (name to string value), and
	// refuses a name it does not define. A task that defines no raw-response
	// semantics — `log`, or a plugin task this harness knows only by name —
	// refuses the stanza when the case binds, naming `returns:` as the
	// spelling that exists. Values follow the Flowfile fence rule at any
	// depth, exactly as Returns documents, so one stub can answer a loop's
	// iterations differently.
	//
	// Mutually exclusive with Returns and with Fails: a stub answers with a
	// response the task interprets, with outputs already shaped, or with a
	// failure — one of the three, said once.
	Response map[string]any `yaml:"response"`
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
// where names the case and the stanza a reader has to look at - `test "x"
// starter:`, `test "x" signal 2 sender:` - and r carries where that stanza was
// written, so the diagnostic names the case *and* the line. Both halves matter
// and neither replaces the other: the name is the identity a reader matches
// back to a report, and the position is what an editor underlines. This
// function used to argue that the name alone was enough, which was a doc
// comment defending `line: 0, column: 0` against the schema's own promise that
// a failure is "positioned to the test file" ([v1.TestCase]); #923 settled it
// the other way.
//
// An identity a case inherited - a signal sender folded in from `defaults:` -
// is refused with the same words and no position, because this document did not
// write it. See [document.positionOf].
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
func checkScriptedIdentity(p *problems, r site, where string, identity *ScriptedIdentity) {
	if identity == nil {
		return
	}

	if (identity.Subject == "") != (identity.Issuer == "") {
		p.report(r,
			"%s names a subject or an issuer without the other; give both, because a rule matches %q "+
				"and never a bare subject - a subject is only unique within its issuer",
			where, v1.QualifiedSubject("<issuer>", "<subject>"))
	}

	// Sorted, so a file with two bad claims reports them in the same order
	// every time: a diagnostic that changes between runs of the same input is
	// one nobody can write a test against, this package's own included.
	for _, name := range slices.Sorted(maps.Keys(identity.Claims)) {
		value := identity.Claims[name]

		empty := "value"
		if name == "" {
			empty = "name"
		}
		if name == "" || value == "" {
			p.reportKey(r.in(r.at.field("claims").field(name)),
				"%s declares a claim with an empty %s; a claim is matched literally, so write it as "+
					"`name: value` with both present, or drop it",
				where, empty)
		}
	}
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
	// [conformance.Case.ExpectedOutputs] already documents: a run that fails
	// outright has no outputs to compare.
	//
	// [conformance.Case.ExpectedOutputs]: https://pkg.go.dev/github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance#Case
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
	// pkg/flowstate/v1/internal/conformance/undo.go already pins for both drivers.
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

	// Check holds CEL claims over the finished run (#1072) — for everything
	// the named fields above cannot say. Each entry is a bare CEL predicate,
	// or `{that:, because:}` to add the sentence a failure prints. Evaluated
	// against `steps.*`, `inputs.*`, and a `run` root (`failed`, `error`,
	// `local`), whether or not the run failed — an error claim exists
	// precisely for failed runs. See [CheckClaim].
	//
	// Across defaults → entry → row the lists accumulate — every level's
	// claims all hold — where the named fields above merge by override.
	// Predicates union naturally; values cannot.
	Check []CheckClaim `yaml:"check"`
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

	return LoadSourceAt(data, path)
}

// LoadSourceAt is [Load] on bytes the caller already holds, with the semantics
// the path decides — the directory's `testdefaults.yaml` folded in, the
// workflow requirement enforced — kept exactly as [Load] applies them.
//
// It exists for a caller whose bytes are *newer* than the file at path: the
// language server checks an editor's live buffer, and an unsaved edit is
// precisely what its diagnostics have to reflect (#1110). [Load] is this
// function behind a bounded read, so the two cannot drift.
func LoadSourceAt(data []byte, path string) (*File, error) {
	if len(data) > MaxTestFileBytes {
		return nil, fmt.Errorf("%s: %d bytes exceeds the %d byte limit for a test file",
			path, len(data), MaxTestFileBytes)
	}

	// The directory's shared fixture, if the suite's own directory states
	// one — see dirdefaults.go for the chain and its boundaries.
	dd, err := loadDirDefaults(filepath.Dir(path))
	if err != nil {
		return nil, err
	}

	file, refused := parseSourceWith(data, dd, true)
	if refused != nil {
		// The path is stamped on every problem rather than prefixed onto the
		// rendered text once, so a report of several problems names the file on
		// each line — a line that travels on its own has to say which file it
		// is about. One unpositioned problem renders exactly as it did when
		// this was a `%s: %w` wrap.
		return nil, refused.inFile(path)
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

	file, refused := parseSource(data, false)
	if refused != nil {
		// No path to attribute these to: bytes are all this door was given,
		// and a file name it invented would be a fact about nothing.
		return nil, refused
	}

	return file, nil
}

// parseSource is the byte-parsing seam both [Load] and [LoadSource] share:
// the expansion-bound check, the strict unmarshal, and every bound in
// [MaxTestsPerFile]/[MaxStubsPerTest]/[MaxSignalsPerTest] — everything [Load]
// did after reading the file off disk, factored out so a caller with bytes
// and no path runs the identical checks rather than a second copy of them.
// requireWorkflow is false only for [LoadSource]; see its doc for why.
func parseSource(data []byte, requireWorkflow bool) (*File, *Diagnostics) {
	return parseSourceWith(data, nil, requireWorkflow)
}

// parseSourceWith is [parseSource] with a directory's contribution folded in
// before anything resolves or validates, so the combined suite is what every
// rule below checks.
//
// Every refusal below is collected rather than returned, and positioned where
// the document wrote the value it is about — see [problems] for why a loader
// that stopped at the first one was making an author fix a suite one run at a
// time, and [document.positionOf] for the one rule that keeps a position
// honest. The document tree is parsed once here and read twice: by the
// expansion bound, which must see it before anything resolves an alias, and by
// every diagnostic that needs a line. It is held for the length of one load
// rather than discarded after the bound, which is the one cost this adds to a
// suite that loads cleanly: a tree and the value decoded from it, both bounded
// by [MaxTestFileBytes], live at once instead of one after the other.
func parseSourceWith(data []byte, dd *dirDefaults, requireWorkflow bool) (*File, *Diagnostics) {
	// Parsed to the AST and no further. Unmarshal resolves every alias into
	// the destination value as it decodes, which means a billion-laughs
	// document is already fully expanded in memory by the time any bound
	// written against the decoded value could run.
	//
	// A parse failure is not reported here: the decode below meets the same
	// malformed document and reports it in the shape a caller already expects,
	// and reporting it twice, once from each of two parsers, would be the same
	// fact said two different ways depending on which noticed first.
	parsed, parseErr := parser.ParseBytes(data, 0)
	if parseErr == nil {
		if err := checkExpansionBoundsIn(parsed); err != nil {
			// Unpositioned on purpose: this is a property of the document as a
			// whole, and the walk that could name a node is the very walk
			// refusing to run over it.
			refused := newProblems(nil)
			refused.report(site{}, "%s", err)

			return nil, refused.err()
		}
	}

	var file File
	if err := decodeStrict(data, &file); err != nil {
		return nil, yamlProblem(err)
	}

	p := newProblems(newDocument(parsed))
	tests := at("tests")

	if len(file.Tests) == 0 {
		p.report(site{at: tests}, "declares no tests")
	}
	if len(file.Tests) > MaxTestsPerFile {
		// Reported and returned, not reported and carried on with. Collecting
		// every problem is about a file whose *size* is legal: a count bound
		// exists to stop the work it bounds, and everything below here is that
		// work — vars resolved per case, a source recorded per case, defaults
		// merged into each, then every per-case check. A file of tens of
		// thousands of entries fits well inside [MaxTestFileBytes], so a bound
		// that only annotated it would have moved the spend rather than
		// refused it (Codex, #1179). Same rule at every count bound below.
		p.report(site{at: tests}, "declares %d tests, more than the limit of %d",
			len(file.Tests), MaxTestsPerFile)

		return nil, p.err()
	}
	if stanza := file.Coverage; stanza != nil {
		allowed := at("coverage").field("allow_unreached")
		if len(stanza.AllowUnreached) > MaxAllowUnreachedPerFile {
			p.report(site{at: allowed}, "coverage.allow_unreached declares %d entries, more than the limit of %d",
				len(stanza.AllowUnreached), MaxAllowUnreachedPerFile)

			return nil, p.err()
		}
		// Sorted, so a file with two bad entries reports them in the same
		// order every time: a map's iteration order is not a thing anyone can
		// write a test against, this package's own included. Every entry is
		// judged now rather than only the first, which is the whole of the
		// change here.
		for _, step := range slices.Sorted(maps.Keys(stanza.AllowUnreached)) {
			if step == "" {
				p.reportKey(site{at: allowed.field(step)}, "coverage.allow_unreached has an entry with no step id")

				continue
			}
			// A reason is required, because an entry with none is exactly the
			// silent gap this record exists to refuse: "a decision with a
			// reason, never a gap." Name the offending step so the fix is
			// obvious.
			if strings.TrimSpace(stanza.AllowUnreached[step]) == "" {
				p.reportKey(site{at: allowed.field(step)}, "coverage.allow_unreached[%q] has no reason; "+
					"record why no case reaches this step, or remove the entry and let it be a gap", step)
			}
		}
	}

	// What the fold brought in from the sibling file, so a diagnostic about one
	// of those values names the document that holds the text rather than the
	// suite that inherited it. The counts it carries are taken before it folds,
	// because both of the collections it merges renumber: `check:` prepends and
	// `stubs:` appends, so afterwards an index alone no longer says which
	// document wrote the entry it addresses.
	moved := dd.combineInto(&file)
	p.wrote(moved.file, moved.paths)

	// Vars validate and substitute first, before tables expand and before
	// `defaults:` is checked: an inherited `${vars.x}` resolves to its
	// literal exactly once, and the fixture rule below then checks what the
	// run will actually see.
	if !checkVars(p, file.Vars) {
		return nil, p.err()
	}
	file.resolveVars(p)

	// Rows are expanded before defaults are merged, which is what makes the
	// precedence chain read the way an author expects it to: a row beats its
	// entry, and an entry beats `defaults:`. Expanding afterward would let a
	// file-level default win over an entry's own value for a row that stated
	// neither — one fact written down twice, disagreeing with itself.
	//
	// Done here rather than at run time so everything downstream — the
	// per-test checks below, coverage, `--run`, the Go subtests — keeps
	// reading one flat list of effective cases and needs no notion of a table.
	expanded, sources := expandTableEntries(p, file.Tests)
	file.Tests = expanded

	// The bound is on the runs, not on the written entries: a row is a whole
	// case, so an entry with four hundred rows costs what four hundred cases
	// cost. Checked after expansion for that reason, and the diagnostic says
	// "once its rows are counted" because the limit is otherwise confusing to
	// read in a file whose `tests:` list is three items long.
	if len(file.Tests) > MaxTestsPerFile {
		p.report(site{at: tests},
			"this file declares %d cases once its `cases:` rows are counted, more than the limit of %d",
			len(file.Tests), MaxTestsPerFile)

		return nil, p.err()
	}

	// Validated then merged before anything below bounds or checks a case, so
	// every per-test check runs against the effective test a case actually
	// runs, not the sparse one the author wrote (issue #416). A default is a
	// fixture, so it may hold no expression; that is refused here, by position,
	// rather than carried into a case.
	//
	// The stub count is the bound that has to stop the pass rather than be
	// noted: a default is copied into every case below, so an over-limit block
	// multiplies by the case count before anything checks it.
	if !checkDefaults(p, file.Defaults, moved) {
		return nil, p.err()
	}
	if file.Defaults != nil {
		for i := range file.Tests {
			file.Tests[i] = mergeDefaults(file.Defaults, file.Tests[i])
		}
	}

	for i := range file.Tests {
		// The entry in file.Tests, not a loop copy: the check-claim validation
		// below strips a tolerated whole-value fence in place, and stripping a
		// copy would leave the fence on the claim the run reads.
		test := &file.Tests[i]
		source := sources[i]
		r := site{test: test.Name, at: source.path}

		if test.Name == "" {
			p.report(r, "test %d has no name", i+1)
		}
		if requireWorkflow && test.Workflow == "" {
			p.report(r, "test %q names no workflow", test.Name)
		}
		// Each of these three stops this case rather than annotating it, for
		// the reason the file-level counts return: the work each bounds — a
		// reference parsed per secret, an identity checked per signal, five
		// shape checks per stub — is what runs immediately below, and a case
		// can hold tens of thousands of any of them inside a legal file. The
		// loop itself is bounded, because a file over [MaxTestsPerFile] never
		// reaches it.
		if len(test.Stubs) > MaxStubsPerTest {
			p.report(r.in(source.path.field("stubs")), "test %q declares %d stubs, more than the limit of %d",
				test.Name, len(test.Stubs), MaxStubsPerTest)

			continue
		}
		if len(test.Signals) > MaxSignalsPerTest {
			p.report(r.in(source.path.field("signals")), "test %q declares %d signals, more than the limit of %d",
				test.Name, len(test.Signals), MaxSignalsPerTest)

			continue
		}
		if len(test.Secrets) > MaxSecretsPerTest {
			p.report(r.in(source.path.field("secrets")), "test %q declares %d secrets, more than the limit of %d",
				test.Name, len(test.Secrets), MaxSecretsPerTest)

			continue
		}
		for _, reference := range slices.Sorted(maps.Keys(test.Secrets)) {
			// Checked while the reference is still text, so a malformed
			// `secrets:` key fails when the file loads rather than the first
			// time a case happens to invoke a task naming it — the same
			// timing [secrets.ParseRef]'s own doc gives for a Flowfile.
			if _, err := secrets.ParseRef(reference); err != nil {
				p.reportKey(r.in(source.path.field("secrets").field(reference)),
					"test %q secrets: %s", test.Name, err)
			}
		}
		checkScriptedIdentity(p, r.in(source.path.field("starter")),
			fmt.Sprintf("test %q starter:", test.Name), test.Starter)
		checkCheckClaims(p, r.in(source.path.field("expect").field("check")),
			// A case's inherited claims came from a block or an entry this
			// path does not reach, so there is no document to name for them.
			fmt.Sprintf("test %q expect", test.Name), test.Expect.Check, source.ownChecks, "")
		for j := range test.Signals {
			signal := &test.Signals[j]
			// The identity [mergeDefaults] installed on a signal that wrote no
			// `sender:` is the `defaults:` block's own, judged there a moment
			// ago — where it is addressable and where the file that wrote it is
			// known. Judging the same identity again here would report one
			// mistake once per inheriting signal, against a path no document
			// holds. Compared by pointer because that is exactly what the merge
			// installed: a signal's own sender is its own value, and is judged.
			if file.Defaults != nil && signal.Sender != nil && signal.Sender == file.Defaults.Sender {
				continue
			}
			// Checked at load, alongside every other shape check in this
			// loop, rather than when the scripted goroutine delivers: a
			// delivery refused there disappears the way a production
			// PermissionDenied does (see [v1.LocalSignals.DeliverFrom]), so
			// the case would report a gate that timed out and never the
			// mistake in the file that caused it.
			checkScriptedIdentity(p,
				r.in(source.path.field("signals").item(j).field("sender")),
				fmt.Sprintf("test %q signal %d (%q) sender:", test.Name, j+1, signal.Name),
				signal.Sender,
			)
		}
		for j := range test.Stubs {
			stub := &test.Stubs[j]
			// A stub [mergeDefaults] copied in from the `defaults:` block was
			// judged there a moment ago, where it is addressable and where the
			// file that wrote it is known. Judging the copy again would report
			// one mistake once per case, against a merged index that for a
			// directory-written stub addresses neither document — the same two
			// failures the block's claims and its sender were fixed for
			// (Codex, #1185). A stub inherited from a table entry is nobody
			// else's to judge and is judged here.
			where, judgedAtTheBlock := source.stubOrigin(j, stub, file.Defaults)
			if judgedAtTheBlock {
				continue
			}
			checkStubShape(p, r.in(where), fmt.Sprintf("test %q stub %d", test.Name, j+1), stub)
		}
		checkOthers(p, r, test)
		checkTrigger(p, r, test, requireWorkflow)
	}

	if refused := p.err(); refused != nil {
		return nil, refused
	}

	return &file, nil
}

// A caseSource is where one effective case was written, and how much of it the
// case wrote for itself.
//
// The path is not `tests[i]` for every case: a table's rows expand into the
// flat list every check below reads, so case i may have been written at
// `tests[3].cases[2]`. Recomputing that from the index afterward is impossible,
// which is why it travels from the expansion that knows it.
//
// The two counts exist because merging shifts indices. A case's own stubs come
// first in the merged list and its own check claims come last, so a merged
// index inside those runs addresses something the case wrote, and one outside
// them addresses something it inherited — which has no position in this
// document, and must not borrow the case's.
type caseSource struct {
	// path addresses the case in the source.
	path loc

	// ownStubs is how many of the merged `stubs:` the case wrote itself,
	// counted before any `defaults:` were folded in.
	ownStubs int

	// ownChecks is how many of the merged `expect.check:` claims the case
	// wrote itself, counted the same way.
	ownChecks int
}

// stubOrigin says where stub j of a merged case was written: the path this
// document addresses it at, and whether the `defaults:` block already answered
// for it.
//
// A case's own stub has a path here. An inherited one has none — a position on
// the case would underline a stub the case did not write — and the question that
// remains is which document owes the diagnostic. A stub the `defaults:` block
// holds was judged by [checkDefaults] a moment ago, where it is addressable and
// where the file that wrote it is known, so judging the copy again would report
// one mistake once per case; a stub that matches nothing there came from the
// case's table entry, which nothing else judges, and is judged in the case.
//
// The block is searched by value, and accepting a match only when the two stubs
// are the same value is the whole of the rule: a case can inherit an identical
// stub from its table entry while a file default of the same target sits unused
// beside it, and the two say the same thing about the same shape, so answering
// once at the block is the report an author can act on.
//
// The mark [Stub.fromDefaults] cannot answer that question, and finding out
// nearly cost a fail-open regression the tests now pin: [mergeRow] folds a table
// entry through [mergeDefaults] with the entry standing in as the block, so an
// entry's stubs carry the mark too — and skipping on it left an entry's
// malformed stub judged by nobody at all.
func (c caseSource) stubOrigin(j int, stub *Stub, defaults *Defaults) (loc, bool) {
	if j < c.ownStubs {
		return c.path.field("stubs").item(j), false
	}
	if defaults == nil {
		return nil, false
	}
	for k := range defaults.Stubs {
		// Compared with the provenance mark set aside, since it is what the
		// merge stamped on the copy and never something a document wrote.
		candidate := defaults.Stubs[k]
		candidate.fromDefaults = stub.fromDefaults
		if reflect.DeepEqual(candidate, *stub) {
			return nil, true
		}
	}

	return nil, false
}

// checkStubShape refuses a stub whose fields contradict each other, naming it as
// where — `test "the rollback" stub 2`, or `defaults.stubs[0]`.
//
// One list, two callers: a case's own stubs and the `defaults:` block's. Written
// out twice they would drift, and a drift here means a mistake refused in a case
// and accepted in the block every case inherits from.
//
// Answers whether the stub was found coherent enough to keep judging. The target
// decides what a stub *is*, so a stub that names none or names two is not judged
// further: every check below quotes [stubTarget], which for a targetless stub
// reads `task ""` — a second diagnostic about a stub the first one already said
// does not identify anything, spending the report's bound on a cascade (Codex,
// #1179). Same rule as the trigger stanza that names both a webhook and a kind.
func checkStubShape(p *problems, spot site, where string, stub *Stub) bool {
	switch {
	case stub.Task == "" && stub.Step == "":
		p.report(spot, "%s names neither a task nor a step; "+
			"give one, either `task: <name>` or `step: <id>`", where)

		return false
	case stub.Task != "" && stub.Step != "":
		p.report(spot, "%s names both a task (%q) and a step (%q); "+
			"a stub targets one or the other, never both", where, stub.Task, stub.Step)

		return false
	}
	if stub.Returns != nil && stub.Fails != nil {
		p.report(spot,
			"%s for %s declares both returns and fails; a stubbed call either succeeds or fails, not both",
			where, stubTarget(stub))
	}
	if stub.Response != nil && stub.Returns != nil {
		p.report(spot,
			"%s for %s declares both response and returns; a stub answers with a raw "+
				"response the task interprets, or with outputs already shaped, not both",
			where, stubTarget(stub))
	}
	if stub.Response != nil && stub.Fails != nil {
		p.report(spot,
			"%s for %s declares both response and fails; a failing response is a "+
				"response — write the status the failure would carry, and let the step's own "+
				"expect: decide",
			where, stubTarget(stub))
	}
	// `times: 0` is refused rather than read as "never answers": a stub that can
	// answer nothing asserts nothing, and an author who wrote 0 meant something —
	// most likely deleting the stub, or the unbounded default they get by writing
	// no `times:` at all. Negative is the same mistake with less ambiguity.
	if stub.Times != nil && *stub.Times <= 0 {
		p.report(spot,
			"%s for %s declares times: %d, which is a stub that never answers; "+
				"delete the stub, or drop `times:` for the unbounded default",
			where, stubTarget(stub), *stub.Times)
	}

	return true
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
func checkOthers(p *problems, r site, test *Test) {
	switch test.Expect.Others {
	case "", OthersSkipped:
		return
	default:
		p.report(r.in(r.at.field("expect").field("others")),
			"test %q expect.others: %q is not a value it accepts; the only value is %q, "+
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
//
// The one place this stops rather than collecting is the stanza that names both
// a webhook and a kind: it is two stanzas written as one, and every rule below
// reads it as whichever of the two it happens to look like. Judging its inside
// would report problems with a shape the author never wrote (see [problems]).
func checkTrigger(p *problems, r site, test *Test, requireWorkflow bool) {
	stanza := r.in(r.at.field("trigger"))

	trigger := test.Trigger
	if trigger == nil {
		if test.Expect.Refused != nil {
			p.report(r.in(r.at.field("expect").field("refused")),
				"test %q expects a refusal but replays no delivery; a refusal is what a "+
					"`trigger:` produces, so give the case one", test.Name)
		}
		if test.Expect.Inputs != nil {
			p.report(r.in(r.at.field("expect").field("inputs")),
				"test %q expects mapped inputs but replays no delivery; `expect.inputs:` "+
					"asserts what a `trigger:` produced, and a case that states its own `inputs:` already "+
					"knows them", test.Name)
		}

		return
	}

	if trigger.Webhook != "" && trigger.Kind != "" {
		p.report(stanza, "test %q trigger: names both a webhook (%q) and a kind (%q); a stanza either "+
			"replays a delivery, which decides the context, or states the context outright, which "+
			"needs no delivery — never both, because the two could disagree about the same run",
			test.Name, trigger.Webhook, trigger.Kind)

		return
	}

	if !trigger.Replays() {
		checkTriggerContext(p, r, test, trigger)

		return
	}

	if trigger.Payload == "" {
		p.report(stanza, "test %q trigger %q: names no payload; write `payload: ./testdata/<file>.json`, "+
			"a stored delivery with `headers` and `body`", test.Name, trigger.Webhook)
	}
	if !requireWorkflow {
		p.report(stanza, "test %q trigger %q: a delivery is read relative to the test file's own "+
			"directory, and these cases were given as bytes with no directory to resolve it against",
			test.Name, trigger.Webhook)
	}
	switch trigger.Signature {
	case "", SignatureValid, SignatureInvalid:
	default:
		p.report(stanza.in(stanza.at.field("signature")),
			"test %q trigger %q: signature: %q is not a value it accepts; write %q or %q, "+
				"which say whether this delivery verified — there is no third answer, because a delivery "+
				"that could not be checked is refused exactly as one that failed a check",
			test.Name, trigger.Webhook, trigger.Signature, SignatureValid, SignatureInvalid)
	}
	if len(test.Inputs) > 0 {
		p.report(r.in(r.at.field("inputs")),
			"test %q trigger %q: the case also states `inputs:`; a trigger case's inputs "+
				"come from the delivery, so stating them here would override the mapping the case exists "+
				"to check", test.Name, trigger.Webhook)
	}
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
func checkTriggerContext(p *problems, r site, test *Test, trigger *TriggerDelivery) {
	stanza := r.in(r.at.field("trigger"))

	if trigger.Kind == "" {
		// Nothing below can be judged: every remaining rule here is about what
		// a stated context may sit beside, and this stanza states none.
		p.report(stanza, "test %q trigger: says neither how the run started nor what delivery started "+
			"it; write `kind: <%s>` to state the context a run reads as `${%s.kind}`, or `webhook: "+
			"<name>` with a `payload:` to replay a stored delivery",
			test.Name, strings.Join(v1.TriggerKinds(), "|"), v1.TriggerRoot)

		return
	}

	if !v1.KnownTriggerKind(trigger.Kind) {
		p.report(stanza.in(stanza.at.field("kind")),
			"test %q trigger: kind: %q is not a kind Flowstate starts runs with; the kinds "+
				"are %s. A case stating one that cannot occur asserts a branch is never taken, and passes",
			test.Name, trigger.Kind, strings.Join(v1.TriggerKinds(), ", "))
	}

	if test.Expect.Inputs != nil {
		// `expect.inputs:` asserts what a *mapping* produced, and a stated context
		// maps nothing: the case's own `inputs:` are the inputs. Refused rather
		// than ignored, because an assertion nothing evaluates is a test that
		// reports success for a claim it never checked — the one outcome a harness
		// must not have.
		p.report(r.in(r.at.field("expect").field("inputs")),
			"test %q trigger: states a context and expects mapped inputs; `expect.inputs:` "+
				"asserts what replaying a delivery produced, and a case that states its own `inputs:` "+
				"already knows them", test.Name)
	}

	if trigger.Payload != "" || trigger.Signature != "" {
		p.report(stanza, "test %q trigger: states a context (`kind: %s`) and also a delivery; a payload "+
			"and a signature belong to a replay, which is written `webhook: <name>` and derives its own "+
			"context", test.Name, trigger.Kind)
	}

	if test.Expect.Refused != nil {
		p.report(r.in(r.at.field("expect").field("refused")),
			"test %q trigger: states a context and expects a refusal; a refusal is what "+
				"replaying an unverifiable *delivery* produces, and a stated context starts the run it "+
				"describes", test.Name)
	}
}

// checkDefaults refuses a `defaults:` block that holds an expression anywhere,
// because a test file is a fixture and not a program (issue #416): a `${...}` a
// case is free to write is, in a default, a value the author did not mean to
// depend on a run they cannot see from the defaults block. It also bounds the
// block's stub list, since a default is copied into every case and so its size
// multiplies.
//
// Named the way the rest of this loader names a diagnostic — the field a
// reader has to look at (`defaults.inputs.version`,
// `defaults.stubs[0].returns.reference`, `defaults.sender.claims`) — and
// positioned at it as well, where this document is the one that wrote it. A
// default folded in from a directory's `testdefaults.yaml` has no position in
// this file, and reports none rather than borrowing the suite's.
// Reports false when the stub count stopped it, which the loader takes as a
// refusal of the whole document rather than one more diagnostic: an
// over-limit block is copied into every case a moment later, so this is the
// one bound here whose overrun multiplies.
//
// from is what the directory's fold moved in, which is how the two collections
// it renumbers are named after the file that wrote them: see [contribution].
func checkDefaults(p *problems, d *Defaults, from contribution) bool {
	if d == nil {
		return true
	}

	base := at("defaults")
	if len(d.Stubs) > MaxDefaultStubs {
		p.report(site{at: base.field("stubs")},
			"defaults declares %d stubs, more than the limit of %d", len(d.Stubs), MaxDefaultStubs)

		return false
	}
	checkNoExpressions(p, site{at: base.field("workflow")}, "defaults.workflow", d.Workflow, 0)
	for _, name := range slices.Sorted(maps.Keys(d.Inputs)) {
		checkNoExpressions(p, site{at: base.field("inputs").field(name)},
			"defaults.inputs."+name, d.Inputs[name], 0)
	}
	for i := range d.Stubs {
		s := &d.Stubs[i]
		index, elsewhere := from.stubWrittenElsewhere(i)
		// Numbered the way the document that wrote it numbers it, and named
		// after that document: a stub the fold appended sits at an index the
		// directory's file does not use, so both the path and the prose have to
		// count from *its* list or a reader is sent to an entry that is not
		// there (Codex, #1185).
		spot := site{at: base.field("stubs").item(index)}
		if elsewhere {
			spot.file = from.file
		}
		where := fmt.Sprintf("defaults.stubs[%d]", index)
		// Judged here, once, rather than once per case that inherits it. The
		// same rule the block's claims and its sender already follow, and the
		// last field of a `defaults:` block that did not: [mergeDefaults] copies
		// a stub into every case, so judging the copies reported one mistake
		// once per case — against a merged index, which for a directory-written
		// stub addressed neither document.
		if !checkStubShape(p, spot, where, s) {
			continue
		}
		checkNoExpressions(p, spot.in(spot.at.field("where")), where+".where", s.Where, 0)
		checkNoExpressions(p, spot.in(spot.at.field("returns")), where+".returns", s.Returns, 0)
	}
	if d.Sender != nil {
		checkNoExpressions(p, site{at: base.field("sender")}, "defaults.sender", d.Sender, 0)
		// Judged here, where it is written, rather than only on the signals
		// that inherit it. [mergeDefaults] installs this one identity on every
		// signal that omits its own, so checking it there alone reported one
		// mistake once per inheriting signal — and reported it against a path
		// no document holds, which cost the sibling file its name (Codex,
		// #1185). This is the rule the block's own expression check has always
		// followed: a default is refused where an author can see it, whether or
		// not a case happens to reach it.
		checkScriptedIdentity(p, site{at: base.field("sender")}, "defaults.sender:", d.Sender)
	}
	// The inherited claims here are the directory file's own, prepended — named
	// after that file rather than addressed by an index the two documents share.
	checkCheckClaims(p, site{at: base.field("check")}, "defaults", d.Check, from.ownChecks, from.file)

	return true
}

// checkNoExpressions descends a default value and refuses every string that
// carries a `${` fence, naming and positioning each. The bound on depth is the
// fail-closed answer to a pathological fixture rather than a constraint on an
// ordinary one (CLAUDE.md, "bound anything that consumes untrusted input"): the
// walk is recursive, so recursion gets its own bound.
//
// The prose path and the source path descend together, one line apart at every
// step, so the name a reader is given and the line they are sent to cannot come
// to disagree.
func checkNoExpressions(p *problems, r site, where string, v any, depth int) {
	if depth > maxDefaultsDepth {
		// Reported once and not descended into: every level below this one
		// would report the same fact about the same value.
		p.report(r, "%s: nests more than %d levels deep, deeper than a default is meant to go",
			where, maxDefaultsDepth)

		return
	}
	switch value := v.(type) {
	case nil:
		return
	case string:
		if strings.Contains(value, "${") {
			p.report(r, "%s holds the expression %q; a test file's `defaults:` is a fixture, "+
				"so it may not hold an expression. Write the literal value, or move it into the case that needs it",
				where, value)
		}

		return
	case *ScriptedIdentity:
		if value == nil {
			return
		}
		for _, field := range []struct {
			name  string
			value string
		}{
			{"subject", value.Subject},
			{"issuer", value.Issuer},
			{"namespace", value.Namespace},
		} {
			checkNoExpressions(p, r.in(r.at.field(field.name)), where+"."+field.name, field.value, depth+1)
		}
		for _, name := range slices.Sorted(maps.Keys(value.Claims)) {
			checkNoExpressions(p, r.in(r.at.field("claims").field(name)),
				where+".claims."+name, value.Claims[name], depth+1)
		}

		return
	case []any:
		for i, element := range value {
			checkNoExpressions(p, r.in(r.at.item(i)), fmt.Sprintf("%s[%d]", where, i), element, depth+1)
		}

		return
	}

	// A nested mapping a YAML decoder hands back is its own choice of type, so
	// it is reflected rather than switched on map[string]any alone, since missing a
	// map would let a `${...}` inside it pass as literal text, the silent
	// nothing "diagnostics are a feature" forbids. Mirrors [compileReturnValue].
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Map && rv.Type().Key().Kind() == reflect.String {
		// Sorted for the reason every map walk here is sorted: the order a
		// file's problems are found in must be a property of the file.
		keys := make([]string, 0, rv.Len())
		for _, key := range rv.MapKeys() {
			keys = append(keys, key.String())
		}
		slices.Sort(keys)
		for _, key := range keys {
			checkNoExpressions(p, r.in(r.at.field(key)), where+"."+key,
				rv.MapIndex(reflect.ValueOf(key).Convert(rv.Type().Key())).Interface(), depth+1)
		}

		return
	}
	if rv.Kind() == reflect.Slice {
		for i := range rv.Len() {
			checkNoExpressions(p, r.in(r.at.item(i)), fmt.Sprintf("%s[%d]", where, i),
				rv.Index(i).Interface(), depth+1)
		}

		return
	}
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
	// Workflow: explicit beats inherited, the same one direction Sender
	// takes. Merged before anything resolves, so the inherited path is
	// resolved relative to the test file exactly as a stated one would be —
	// and idempotently, since a merged case carries the value and never
	// takes it again.
	if d.Workflow != "" && test.Workflow == "" {
		test.Workflow = d.Workflow
	}

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
			inherited := d.Stubs[i]
			// Marked on the appended copy, never on the file's own entry: a
			// second case merging the same defaults must see them untouched,
			// per this function's contract above.
			inherited.fromDefaults = true
			merged = append(merged, inherited)
		}
		test.Stubs = merged
	}

	// Sender: explicit beats inherited. A signal that named its own sender
	// keeps it; only one that omitted `sender:` inherits the default (the
	// resolved open question in #416).
	// Check accumulates rather than overriding: the file's claims and the
	// case's all hold, inherited first so a failure lists them in the order
	// a reader meets them in the file. A fresh slice — a second case merging
	// the same defaults must see them untouched — and marked, because this
	// fold runs twice on the Go door and a prepend is the one merge here
	// that is not idempotent by shape (see [CheckClaim].fromDefaults).
	if len(d.Check) > 0 && !slices.ContainsFunc(test.Expect.Check, func(c CheckClaim) bool { return c.fromDefaults }) {
		inherited := make([]CheckClaim, 0, len(d.Check)+len(test.Expect.Check))
		for _, claim := range d.Check {
			claim.fromDefaults = true
			inherited = append(inherited, claim)
		}
		test.Expect.Check = append(inherited, test.Expect.Check...)
	}

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
	return workflowPathIn(filepath.Dir(testFile), test)
}

// DeliveryPath resolves a trigger case's `payload:` the same way [WorkflowPath]
// resolves its workflow, and for the same reason: a fixture travels with the test
// that reads it, not with whatever directory `flow test` was invoked from.
//
// Empty for a case that replays nothing.
func DeliveryPath(testFile string, test *Test) string {
	return deliveryPathIn(filepath.Dir(testFile), test)
}
