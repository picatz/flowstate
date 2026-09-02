package lsp

import (
	"reflect"
	"strings"
)

// The test language's own document shape (#1110 item 8), the way dslKeys is the
// workflow's: one table per level of nesting, read by completion and (for one
// level) proved against the loader by TestTestDSLKeysMatchTheLoader.
//
// Where dslKeys cannot be derived — the workflow's document shape lives in
// unexported flowfile structs — this one mostly can. Every flowtest type below
// is exported and decoded by goccy's yaml.Strict() straight off its `yaml:"..."`
// struct tags, with no custom UnmarshalYAML in the way, so [yamlKeys] reads the
// table the loader itself consults to decide "known" from "unknown field" — for
// every field below, which is what makes the claim exact rather than merely
// usually true: each one is checked, individually, to carry an explicit
// `yaml:"..."` tag (see [yamlKeys]'s own doc on the field goccy would still
// accept that this table would not). A key added to a struct with a tag is a
// key TestTestDSLKeysMatchTheLoader learns about with no change here required
// to notice; a key added to *this* table with no matching struct field fails
// the same test. The two exceptions are cited individually, in checkClaimKeys
// and dirDefaultsTopLevelKeys below.

// yamlKeys returns the yaml key names t's exported, explicitly tagged fields
// declare, in declaration order. t must be a struct type.
//
// "Explicitly tagged" is narrower than goccy's own idea of a known key, and
// the gap matters to name: [(goccy/go-yaml).structField] falls back to a
// field's lowercased Go name when neither a `yaml` nor a `json` tag is
// present, so a field with no tag at all is still a key goccy's strict
// decoder accepts — one yamlKeys would silently omit here. It is safe for
// every struct this file reflects on only because every field of every one
// of them carries an explicit `yaml:"..."` tag today (grep finds none
// missing one in flowtest/file.go), which TestTestDSLKeysMatchTheLoader
// re-checks every time it runs by requiring yamlKeys' answer non-empty. A
// struct that later adds a tagless field would make that assumption false
// silently rather than loudly: yamlKeys would keep returning a result, just
// one short by the field nobody tagged. Skipping a field with no yaml tag or
// the literal tag `"-"` is deliberate regardless — a tagless field falling
// back to its lowercased Go name is an accident of how goccy is written, not
// a spelling this package should offer as if an author chose it.
//
// This is the derivation TestDSLKeysMatchTheDSL could not use for the
// workflow grammar (that shape has no Go struct to reflect on, only Marshal's
// rendered output); flowtest.File and its neighbors are ordinary tagged
// structs, so reflection reads the loader's own source of truth directly
// rather than through a fixture that must remember to populate every field.
func yamlKeys(t reflect.Type) []string {
	if t.Kind() != reflect.Struct {
		return nil
	}
	keys := make([]string, 0, t.NumField())
	for i := range t.NumField() {
		f := t.Field(i)
		if f.PkgPath != "" {
			// Unexported: goccy cannot set it and does not offer it as a key.
			// [flowtest.Stub.fromDefaults] and [flowtest.CheckClaim.fromDefaults]
			// are exactly this — bookkeeping the loader itself writes, invisible
			// to an author.
			continue
		}
		tag, ok := f.Tag.Lookup("yaml")
		if !ok || tag == "-" {
			continue
		}
		name, _, _ := strings.Cut(tag, ",")
		if name == "" {
			continue
		}
		keys = append(keys, name)
	}
	return keys
}

// testDocLevel names one nesting level of the test-file document shape, for
// [testDocKeys] and [testDSLCandidates] — the test-language analogue of the
// workflow's `dslKeys` level strings ("steps", "retry", ...).
type testDocLevel string

const (
	// testLevelFile is a *.test.yaml's own top level: [flowtest.File].
	testLevelFile testDocLevel = ""
	// testLevelCase is one `tests:` or `cases:` entry: [flowtest.Test]. One
	// level serves both spellings, because a `cases:` row is a [flowtest.Test]
	// like any other — see [flowtest.Test.Cases]'s own doc.
	testLevelCase testDocLevel = "test"
	// testLevelDefaults is a `defaults:` block: [flowtest.Defaults]. Shared by
	// a suite's own `defaults:` and by testdefaults.yaml's, which nests the
	// identical type (dirdefaults.go).
	testLevelDefaults testDocLevel = "defaults"
	// testLevelCoverage is the `coverage:` stanza: [flowtest.CoverageStanza].
	testLevelCoverage testDocLevel = "coverage"
	// testLevelTrigger is a case's `trigger:` stanza: [flowtest.TriggerDelivery].
	testLevelTrigger testDocLevel = "trigger"
	// testLevelStub is one `stubs:` entry: [flowtest.Stub].
	testLevelStub testDocLevel = "stub"
	// testLevelFails is a stub's `fails:` stanza: [flowtest.StubFailure].
	testLevelFails testDocLevel = "fails"
	// testLevelSignal is one `signals:` entry: [flowtest.SignalScript].
	testLevelSignal testDocLevel = "signal"
	// testLevelIdentity is a `starter:` or `sender:` stanza: both spell
	// [flowtest.ScriptedIdentity] — see that type's own doc for why one type
	// serves both ends.
	testLevelIdentity testDocLevel = "identity"
	// testLevelExpect is a case's `expect:` stanza: [flowtest.Expectation].
	testLevelExpect testDocLevel = "expect"
	// testLevelCheck is one `check:` list entry: [flowtest.CheckClaim]. The one
	// level yamlKeys cannot read — see checkClaimKeys.
	testLevelCheck testDocLevel = "check"
)

// testDocKeys is completion's document-shape table for the test language, one
// entry per [testDocLevel]. The prose is hand-written — the same call
// dslKeys makes, and for the same reason (see that variable's doc): the name
// a key is offered under has to be the DSL spelling, which for every field
// below happens to equal the Go field's own yaml tag, so nothing here risks
// naming a key the loader would reject.
//
// TestTestDSLKeysMatchTheLoader is the guard: it derives each level's real
// key set with [yamlKeys] over the matching flowtest type and asserts this
// table names exactly that set, both directions, the same shape
// TestDSLKeysMatchTheDSL already holds the workflow table to.
var testDocKeys = map[testDocLevel][]dslKey{
	testLevelFile: {
		{name: "edition", detail: "version", docs: "Accepted and otherwise unused: `flow fix` stamps `edition:` into any document it recognizes as a Flowfile or a Flowfile test, and a strict decode without the field would refuse a file the moment a migration touched it."},
		{name: "vars", detail: "map", docs: "Literal values this file states once. A whole-value `${vars.x}` in a fixture position is substituted at load, and `expect.check:` reads `vars.x` at evaluation. Literals only — any `${` inside one is refused."},
		{name: "defaults", detail: "map", docs: "The inputs, stubs, and signal sender every case in this file starts from, before its own values are merged over them (issue #416)."},
		{name: "tests", detail: "list", docs: "The cases this file declares, in the order they run and the order they are reported in. Required — a file declaring none is refused."},
		{name: "coverage", detail: "map", docs: "Which of the workflow's steps no case here is expected to reach, and why (issue #420)."},
	},
	testLevelCase: {
		{name: "name", detail: "string", docs: "Identifies the case in a report. Required."},
		{name: "workflow", detail: "path", docs: "The Flowfile under test, resolved relative to the directory the *.test.yaml itself lives in — the same rule `call:` resolves against."},
		{name: "inputs", detail: "map", docs: "Binds the workflow's declared `inputs:`, checked the same way a real run's are. Mutually exclusive with `trigger:`."},
		{name: "trigger", detail: "map", docs: "Replays a stored delivery against one of the workflow's declared `triggers:`, or states a trigger context directly. Mutually exclusive with `inputs:`."},
		{name: "stubs", detail: "list", docs: "Replaces the task registry for the duration of this case. A task this case never invokes needs no stub; one invoked with no matching stub fails the case."},
		{name: "secrets", detail: "map", docs: "Replaces the real secret backend for this case, keyed by a `${secret(...)}` reference's text form and bound to the plaintext value it resolves to."},
		{name: "signals", detail: "list", docs: "Scripts what to deliver to a `wait_for_signal:` step, and when."},
		{name: "starter", detail: "map", docs: "Who this case runs as — the identity a `signals:` policy's `distinct_from_starter:` compares a scripted sender against. Never attested, and never reaches `run.identity`."},
		{name: "cases", detail: "list", docs: "Rows of a table entry (#924 slice 2): one run each, merged over this entry the way a case merges over `defaults:`. An entry declaring rows does not itself run."},
		{name: "expect", detail: "map", docs: "What the run must have produced to pass."},
	},
	testLevelDefaults: {
		{name: "workflow", detail: "path", docs: "The Flowfile every case runs against unless it names its own (#924 slice 1), resolved exactly as a case's own `workflow:` is."},
		{name: "inputs", detail: "map", docs: "The base bindings every case starts from, before its own `inputs:` are merged over them one key at a time."},
		{name: "stubs", detail: "list", docs: "The stubs every case starts from. A case's own stubs append, unless one targets the same task or step id, which replaces the default."},
		{name: "sender", detail: "map", docs: "The scripted signal sender a case's signals inherit when they omit their own."},
		{name: "check", detail: "list", docs: "Claims every case in the file must satisfy (#1072), prepended to each case's own `expect.check:`."},
	},
	testLevelCoverage: {
		{name: "allow_unreached", detail: "map", docs: "Maps a step id — or a switch arm's key — to the reason no case reaches it. An entry here is an accepted residual rather than a gap."},
	},
	testLevelTrigger: {
		{name: "webhook", detail: "string", docs: "The name one of the workflow's `- webhook:` entries declares. An unknown name is refused when the file loads."},
		{name: "payload", detail: "path", docs: "The stored delivery, resolved relative to the directory the *.test.yaml lives in: one JSON document with `headers` and `body`."},
		{name: "kind", detail: "string", docs: "Sets the run's trigger context directly, with no delivery involved — one of the kinds the workflow's `triggers:` declares. Mutually exclusive with `webhook:`."},
		{name: "name", detail: "string", docs: "The trigger's own name for a context set directly: the schedule's, or the webhook's where a case states one rather than replaying it."},
		{name: "principal", detail: "string", docs: "Who the context says started the run, read as `${trigger.principal}`. Settable here and attested nowhere — never the shape a workflow authorizes on."},
		{name: "delivery_id", detail: "string", docs: "The delivery a directly-set context names, read as `${trigger.delivery_id}`. A replayed delivery computes its own."},
		{name: "signature", detail: "string", docs: "Whether this delivery verified: `valid` (the default) or `invalid`. Legal only while the case binds none of the keys the trigger's `verify:` names."},
	},
	testLevelStub: {
		{name: "task", detail: "string", docs: "The task name this replaces, exactly as a step's own task key names it. Mutually exclusive with `step:`."},
		{name: "step", detail: "string", docs: "The id of the workflow step this replaces, as an alternative to naming the task it invokes. An unknown id is refused with a did-you-mean suggestion."},
		{name: "where", detail: "expression", docs: "Filters which invocations this stub answers, as bare CEL — no `${...}` fence. Empty matches every invocation no earlier stub already matched."},
		{name: "returns", detail: "map", docs: "The task's outputs when `where:` matches. Mutually exclusive with `fails:` and `response:`."},
		{name: "fails", detail: "map", docs: "Makes the stubbed task report a classified failure instead of succeeding, so a case can exercise `continue_on_error:`, `retry:`, and `undo:`."},
		{name: "times", detail: "int", docs: "Bounds how many invocations this stub answers before it retires and the list falls through to the next matcher (#927). Absent means unbounded."},
		{name: "response", detail: "map", docs: "Answers with a raw response instead of shaped outputs, so the task evaluates its own deferred inputs — `outputs:`, `expect:` — over it for real."},
	},
	testLevelFails: {
		{name: "kind", detail: "string", docs: "Classifies the failure the way an [v1.ErrorKind] does — \"Upstream\", \"InvalidInput\", and so on. Defaults to \"Upstream\"."},
		{name: "message", detail: "string", docs: "The failure text, read back through `${steps.<id>.error}` exactly as a real task's would be."},
	},
	testLevelSignal: {
		{name: "name", detail: "string", docs: "The signal a `wait_for_signal:` step names."},
		{name: "at", detail: "duration", docs: "When to deliver it, as a duration from the moment the run started — \"5m\", \"1h30m\". Empty delivers it immediately."},
		{name: "payload", detail: "map", docs: "What the signal carries, read back under `${<step>.payload}` exactly as `flow signal`'s would be."},
		{name: "sender", detail: "map", docs: "Who this signal stands in for, checked against the workflow's own declared `signals:` policy exactly as a real delivery is."},
		{name: "delivery_id", detail: "string", docs: "Names the webhook delivery this signal stands in for, so a case can rehearse a redelivery. Two entries sharing one value are one delivery arriving twice: the second answers no gate."},
	},
	testLevelIdentity: {
		{name: "subject", detail: "string", docs: "The caller this identity stands in for, matched against a policy rule's `subject:` as `<issuer>#<subject>`."},
		{name: "issuer", detail: "string", docs: "Identifies which identity provider would have attested subject."},
		{name: "namespace", detail: "string", docs: "The tenant this identity belongs to, matched against a policy rule's `namespace:`."},
		{name: "claims", detail: "map", docs: "Additional facts, matched against a policy rule's `claims:` — every key the rule names must be present here with the same value."},
	},
	testLevelExpect: {
		{name: "outputs", detail: "map", docs: "Must equal the workflow's declared `outputs:` exactly. Ignored when `failed: true`."},
		{name: "inputs", detail: "map", docs: "Must equal the inputs a replayed delivery produced, exactly. Only meaningful alongside `trigger:`."},
		{name: "refused", detail: "bool", docs: "Asserts that the delivery was refused and no run happened."},
		{name: "idempotency_key", detail: "string", docs: "Must equal the key the replayed delivery evaluated to. Only meaningful alongside `trigger:`."},
		{name: "failed", detail: "bool", docs: "Asserts whether the run failed outright, as distinct from a step's failure being tolerated by `continue_on_error:`."},
		{name: "error_contains", detail: "string", docs: "Must appear in the run's failure text. Only meaningful alongside `failed: true`."},
		{name: "compensated", detail: "list", docs: "Names the steps that must have been undone, in any order."},
		{name: "ran", detail: "list", docs: "Names steps that must have executed — present in the run's step outputs, whether they succeeded, were tolerated, or ended the run."},
		{name: "skipped", detail: "list", docs: "Names steps that must not have executed — absent because their `if:` did not hold or the run never reached them."},
		{name: "others", detail: "string", docs: "The only accepted value is `skipped`, closing the `ran:` claim: every step the workflow has that `ran:` does not name must have been skipped (issue #416)."},
		{name: "check", detail: "list", docs: "CEL claims over the finished run (#1072), for everything the named fields above cannot say."},
	},
	testLevelCheck: checkClaimKeys,
}

// checkClaimKeys is [flowtest.CheckClaim]'s two keys, cited rather than
// derived: unlike every other type this file completes, CheckClaim declares
// no yaml struct tags at all — it implements UnmarshalYAML by hand precisely
// so a misspelled key is still refused (check.go's own doc on the method) —
// so [yamlKeys] finds nothing to read. The two names below are the literal
// strings that method compares against (pkg/flowstate/v1/flowtest/check.go:82-84):
// any key besides "that" and "because" is the refusal that function raises.
var checkClaimKeys = []dslKey{
	{name: "that", detail: "expression", docs: "The claim, in CEL, over `steps.*`, `inputs.*`, and a `run` root carrying `failed`, `error`, and `local`. Bare CEL — a whole-value `${...}` fence is tolerated and stripped."},
	{name: "because", detail: "string", docs: "The sentence a failure prints. Optional; the claim and its witnessed values print either way."},
}

// dirDefaultsTopLevelKeys is testdefaults.yaml's own top level — cited rather
// than derived, because the type behind it (flowtest's unexported
// `dirDefaults`) cannot even be *named* from this package, let alone passed
// to [reflect.TypeOf]: an unexported identifier is invisible across a
// package boundary at compile time, before reflection ever enters into it.
// Its own doc names the whole of what it accepts: "vars and defaults, nothing else"
// (pkg/flowstate/v1/flowtest/dirdefaults.go:35-37), plus the same accepted-but-unused
// `edition:` [flowtest.File.Edition] documents, for the identical migration
// reason (dirdefaults.go:39-42).
var dirDefaultsTopLevelKeys = []dslKey{
	{name: "edition", detail: "version", docs: "Accepted and otherwise unused, for the same reason a suite's own edition: is: `flow fix` stamps it into a document this repo's tooling migrates forward."},
	{name: "vars", detail: "map", docs: "Literal values every suite in this directory shares, folded into each suite's own `vars:` (the suite's own value wins where both name one)."},
	{name: "defaults", detail: "map", docs: "The inputs, stubs, and signal sender every suite in this directory starts from, folded beneath the suite's own `defaults:`."},
}

// testLevelChildren says which keys at one level open another level of the
// test grammar, and — by omission — which hold the author's own data.
//
// The omissions are the point, and they are what a suffix match got wrong: a
// case's `inputs:` holds whatever the workflow declares, so a fixture map in
// there named `expect` is the author's data, not the DSL's stanza — and a
// suffix check on the enclosing key offered `outputs`, `failed` and the rest
// inside it with full confidence (Codex, #1173). A walk from the root stops at
// the first data-holding key, and nothing below one is ever completed.
//
// TestTheTransitionMapNamesOnlyRealKeys holds every entry here to
// [testDocKeys]'s derived key sets, and pins the deliberate omissions by name
// so completing this map later has to argue with the reason.
var testLevelChildren = map[testDocLevel]map[string]testDocLevel{
	testLevelFile: {
		"tests":    testLevelCase,
		"defaults": testLevelDefaults,
		"coverage": testLevelCoverage,
	},
	testLevelCase: {
		"cases":   testLevelCase,
		"trigger": testLevelTrigger,
		"stubs":   testLevelStub,
		"signals": testLevelSignal,
		"starter": testLevelIdentity,
		"expect":  testLevelExpect,
	},
	testLevelDefaults: {
		"stubs":  testLevelStub,
		"sender": testLevelIdentity,
		"check":  testLevelCheck,
	},
	testLevelStub: {
		"fails": testLevelFails,
	},
	testLevelSignal: {
		"sender": testLevelIdentity,
	},
	testLevelExpect: {
		"check": testLevelCheck,
	},
}

// dirDefaultsChildren is the same map for testdefaults.yaml's narrower top
// level: `defaults:` opens the shared [flowtest.Defaults] shape, and `vars:`
// holds the author's data.
var dirDefaultsChildren = map[string]testDocLevel{
	"defaults": testLevelDefaults,
}

// testDocLevelAt walks the enclosing key chain from the document's root and
// returns the test-grammar level it lands on, or false the moment the chain
// passes through a key the grammar does not open — an unknown key, or one
// that holds the author's own data. Below either, nothing is the DSL's to
// complete.
func testDocLevelAt(kind documentKind, path []string) (testDocLevel, bool) {
	level := testLevelFile
	for i, segment := range path {
		var next testDocLevel
		var ok bool
		if i == 0 && kind == docTestDefaults {
			next, ok = dirDefaultsChildren[segment]
		} else {
			next, ok = testLevelChildren[level][segment]
		}
		if !ok {
			return "", false
		}
		level = next
	}
	return level, true
}
