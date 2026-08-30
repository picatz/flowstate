package flowfile_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `manual:` narrows, and the `trigger` root reads how a run started.
//
// The two halves are tested together here because they are two sides of one
// decision, and because the mistake worth guarding against is confusing them:
// authorization is written on the trigger, where a deployment owns it and this
// validator can see it, and behaviour is written in the body. A file that gates a
// destructive step on `${trigger.principal}` still compiles — nothing can stop it
// — which is exactly why the *other* place has to exist and has to be easy to
// reach. See [v1.CheckManualStart].

// TestAWebhookDoesNotSilentlyRefuseManualStarts is the load-bearing one, and it
// is a test about something *not* happening.
//
// `triggers:` is deliberately not exhaustive. If declaring a webhook meant "and
// nothing else may start this", then adding an integration would take `flow run`
// away from the author who added it, and testability with it. So a file with a
// webhook and no `manual:` block compiles to a workflow that
// [v1.CheckManualStart] admits, and the assertion is that admission rather than
// the absence of a diagnostic — a validator saying nothing is not the same claim.
func TestAWebhookDoesNotSilentlyRefuseManualStarts(t *testing.T) {
	t.Parallel()

	workflow := mustCompile(t, `edition: v2026.3
name: order-webhook
inputs:
  order_id: { type: string, required: true }
triggers:
  - webhook: stripe
    verify:
      stripe: ${secret('env:STRIPE_WEBHOOK_SECRET')}
    idempotency_key: ${event.headers["stripe-signature"]}
    with:
      order_id: ${event.body.order_id}
steps:
  - id: record
    log:
      message: ${'order ' + inputs.order_id}
`)

	require.Nil(t, workflow.GetTriggers().GetManual(),
		"declaring a webhook must not compile to a manual narrowing nobody wrote")
	require.NoError(t, v1.CheckManualStart(workflow, "https://issuer.example.com#anyone@example.com", ""),
		"adding a webhook silently stopped `flow run` from working, which is the one thing "+
			"a non-exhaustive `triggers:` exists to prevent")
}

// TestManualDeniedRefusesAStartAndSaysWhatDoesStartIt pins the refusal and its
// remedy: a diagnostic that names what is wrong and not what to do instead is
// half-written.
func TestManualDeniedRefusesAStartAndSaysWhatDoesStartIt(t *testing.T) {
	t.Parallel()

	workflow := mustCompile(t, `edition: v2026.3
name: payments-only
inputs:
  order_id: { type: string, required: true }
triggers:
  - webhook: payments
    verify:
      stripe: ${secret('env:STRIPE_WEBHOOK_SECRET')}
    idempotency_key: ${event.headers["stripe-signature"]}
    with:
      order_id: ${event.body.order_id}
  - manual: denied
steps:
  - id: record
    log:
      message: ${'order ' + inputs.order_id}
`)

	require.True(t, workflow.GetTriggers().GetManual().GetDenied())

	err := v1.CheckManualStart(workflow, "alice@example.com", "because I said so")
	require.Error(t, err, "`manual: denied` must refuse a manual start whatever reason accompanies it")
	assert.Contains(t, err.Error(), "payments",
		"a refusal owes the author the source that does start this workload")
}

// TestManualNarrowingRequiresAReasonAndAPrincipal covers the two narrowings that
// are not a refusal, in both directions each.
//
// The empty-subject case is the one worth writing down. A deployment with no
// identity provider attests every caller as nobody in particular, so a policy that
// admitted the empty subject would read as if it named two people and behave as if
// it named everybody — the fail-closed direction is to refuse, and say why.
func TestManualNarrowingRequiresAReasonAndAPrincipal(t *testing.T) {
	t.Parallel()

	workflow := mustCompile(t, `edition: v2026.3
name: break-glass
triggers:
  manual:
    require_reason: true
    allowed_principals:
      - https://issuer.example.com#oncall@example.com
      - https://issuer.example.com#sre@example.com
steps:
  - id: rotate
    log:
      message: rotating
`)

	manual := workflow.GetTriggers().GetManual()
	require.True(t, manual.GetRequireReason())
	require.Equal(t, []string{"https://issuer.example.com#oncall@example.com", "https://issuer.example.com#sre@example.com"}, manual.GetAllowedPrincipals())

	require.NoError(t, v1.CheckManualStart(workflow, "https://issuer.example.com#oncall@example.com", "rotating the leaked key"),
		"an allowed principal with a reason is exactly what this block permits")

	err := v1.CheckManualStart(workflow, "https://issuer.example.com#oncall@example.com", "   ")
	require.Error(t, err, "whitespace is not a reason")
	assert.Contains(t, err.Error(), "--reason")

	err = v1.CheckManualStart(workflow, "https://issuer.example.com#intern@example.com", "curious")
	require.Error(t, err, "a principal outside the set must be refused")
	assert.Contains(t, err.Error(), "https://issuer.example.com#intern@example.com")

	err = v1.CheckManualStart(workflow, "", "deploying")
	require.Error(t, err, "an unattested caller must be refused rather than admitted as nobody in particular")
	assert.Contains(t, err.Error(), "no authenticated issuer-qualified principal")
}

// TestManualContradictionsAreRefusedWithAPosition pins each diagnostic a
// `manual:` block can earn, in the words an author reads.
func TestManualContradictionsAreRefusedWithAPosition(t *testing.T) {
	t.Parallel()

	tooManyPrincipals := make([]string, 65)
	for i := range tooManyPrincipals {
		tooManyPrincipals[i] = "issuer#" + strconv.Itoa(i)
	}

	for _, test := range []struct {
		name   string
		source string
		want   string
	}{
		{
			name: "a word that is not denied",
			source: `triggers:
  manual: locked
`,
			want: "is \"locked\", which is not something a `manual:` says",
		},
		{
			name: "a denied written as a key",
			source: `triggers:
  manual:
    denied: true
`,
			want: "denied",
		},
		{
			name: "a block that narrows nothing",
			source: `triggers:
  manual: {}
`,
			want: "narrows nothing",
		},
		{
			name: "an empty principal",
			source: `triggers:
  manual:
    allowed_principals: ["", "ops@example.com"]
`,
			want: "names nobody",
		},
		{
			name: "a bare principal",
			source: `triggers:
  manual:
    allowed_principals: [ops@example.com]
`,
			want: "<issuer>#<subject>",
		},
		{
			name: "an ambiguous principal",
			source: `triggers:
  manual:
    allowed_principals: [mesh#x#y]
`,
			want: "<issuer>#<subject>",
		},
		{
			name: "too many principals",
			source: `triggers:
  manual:
    allowed_principals: [` + strings.Join(tooManyPrincipals, ", ") + `]
`,
			want: "limit of 64",
		},
		{
			name: "a principal listed twice",
			source: `triggers:
  manual:
    allowed_principals: ["https://issuer.example.com#ops@example.com", "https://issuer.example.com#ops@example.com"]
`,
			want: "twice",
		},
		{
			name: "a second manual entry",
			source: `triggers:
  - manual: denied
  - manual:
      require_reason: true
`,
			want: "is a second `- manual:`",
		},
		{
			name: "a refusal in a file nothing else starts",
			source: `triggers:
  manual: denied
`,
			want: "nothing can start it at all",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			source := "edition: v2026.3\nname: narrowed\n" + test.source + `steps:
  - id: work
    log:
      message: working
`
			diagnostics := validateTriggerSource(t, source)
			require.NotEmpty(t, diagnostics,
				"the file was accepted, so nothing told the author their `manual:` does not mean what it says")

			var messages []string
			for _, d := range diagnostics {
				messages = append(messages, d.Message)

				// Every diagnostic this package emits owes a position, which is
				// the whole of what makes it usable in an editor.
				assert.NotZero(t, d.Line,
					"diagnostic %q has no position", d.Message)
			}

			assert.Truef(t, strings.Contains(strings.Join(messages, "\n"), test.want),
				"no diagnostic said %q; got:\n%s", test.want, strings.Join(messages, "\n"))
		})
	}
}

// TestAContradictoryManualBlockIsRefusedAtSubmit covers the contradiction no
// Flowfile can spell, because a refusal has one spelling and it is a scalar.
//
// It is reachable only by a specification built by hand — the shape that never
// passed through the compiler — which is exactly why the rule also lives in
// [v1.BindRunInputs], beside the three checks already there for the same reason.
// Resolving it by precedence instead would leave [v1.CheckManualStart] deciding
// which half of an author's two sentences to believe.
func TestAContradictoryManualBlockIsRefusedAtSubmit(t *testing.T) {
	t.Parallel()

	workflow := &v1.Workflow{
		Name:    "hand-built",
		Profile: v1.CurrentProfile,
		Triggers: &v1.Triggers{Manual: &v1.ManualTrigger{
			Denied:            true,
			AllowedPrincipals: []string{"https://issuer.example.com#ops@example.com"},
		}},
		Steps: []*v1.Node{{Id: "work", Kind: &v1.Node_Value{Value: v1.NewLiteral("x")}}},
	}

	_, err := v1.BindRunInputs(workflow, nil)
	require.Error(t, err, "a contradiction must not reach durable history")
	assert.Contains(t, err.Error(), "refuses manual starts")

	// And the refusal denies rather than being ignored, which is the fail-closed
	// half: a malformed block that reached a server is a refusal, never a permit.
	require.Error(t, v1.CheckManualStart(workflow, "https://issuer.example.com#ops@example.com", "because"),
		"a `manual:` block that cannot be believed must deny")
}

// TestTriggerContextIsReadableInABodyAndClosedToPayloadData is the other half:
// what a step may read under `trigger`, and what it may not.
//
// The closed set is not tidiness. `trigger` is metadata and never data —
// everything a workflow operates on arrives through a trigger's `with:` into
// `inputs:`, where declarations exist for this validator to check against — so a
// payload field reachable here would be a second input path `flow validate` is
// blind to. The diagnostic therefore says where the value actually comes from
// rather than only that the name is wrong.
func TestTriggerContextIsReadableInABodyAndClosedToPayloadData(t *testing.T) {
	t.Parallel()

	mustCompile(t, `edition: v2026.3
name: trigger-aware
triggers:
  - manual:
      require_reason: true
steps:
  - id: notify
    if: ${trigger.kind != "schedule"}
    log:
      message: ${"started by " + trigger.kind + " " + trigger.name}
  - id: correlate
    log:
      message: ${trigger.delivery_id + trigger.principal}
`)

	diagnostics := validateTriggerSource(t, `edition: v2026.3
name: trigger-as-data
steps:
  - id: leak
    log:
      message: ${trigger.body}
`)

	require.NotEmpty(t, diagnostics, "a payload field under `trigger` was accepted")

	var found bool
	for _, d := range diagnostics {
		if strings.Contains(d.Message, `unknown field "body"`) {
			found = true
			assert.Contains(t, d.Message, "inputs",
				"the diagnostic names the name and not the path the value really arrives by")
		}
	}
	require.True(t, found, "no diagnostic named the unknown field: %v", diagnostics)
}

// TestTriggerKindTypoLiteralIsCaught is the PR #514 finding: `flow validate`
// accepted `${trigger.kind == "schedual"}` because the field name resolves —
// `kind` is real, so the loop above never sees a problem — and nothing looked
// at what it was compared to. Both drivers then evaluate the comparison as
// false forever and silently skip whatever the branch guards.
// [v1.KnownTriggerKind] already knows the closed set of three; this pins that
// the validator now asks it.
func TestTriggerKindTypoLiteralIsCaught(t *testing.T) {
	t.Parallel()

	diagnostics := validateTriggerSource(t, `edition: v2026.3
name: trigger-kind-typo
steps:
  - id: notify
    if: ${trigger.kind == "schedual"}
    log:
      message: hi
`)

	require.NotEmpty(t, diagnostics, "a comparison against an unknown trigger kind was accepted")

	var found bool
	for _, d := range diagnostics {
		if strings.Contains(d.Message, `"schedual"`) {
			found = true
			assert.Contains(t, d.Message, "schedule",
				"the diagnostic should name the kind the typo likely meant")
		}
	}
	require.True(t, found, "no diagnostic named the unknown trigger kind literal: %v", diagnostics)
}

// TestTriggerKindLiteralInequalityIsCaught pins the two variations the finding
// calls out explicitly: `!=` rather than `==`, and the literal written on the
// left of the operator rather than the right.
func TestTriggerKindLiteralInequalityIsCaught(t *testing.T) {
	t.Parallel()

	diagnostics := validateTriggerSource(t, `edition: v2026.3
name: trigger-kind-typo-ne
steps:
  - id: notify
    if: ${"webhok" != trigger.kind}
    log:
      message: hi
`)

	var found bool
	for _, d := range diagnostics {
		if strings.Contains(d.Message, `"webhok"`) {
			found = true
		}
	}
	require.True(t, found, "no diagnostic named the unknown trigger kind literal: %v", diagnostics)
}

// TestTriggerKindComparedToNonLiteralStaysSilent is one of the three negative
// directions the fix has to hold: `trigger.kind` compared against a variable,
// an input, or another field is a value this validator cannot know at
// authoring time, and reporting one would be exactly the false diagnostic
// CLAUDE.md ranks worse than a missing one. The file below compiles clean —
// [mustCompile] fails the test the moment any diagnostic appears.
func TestTriggerKindComparedToNonLiteralStaysSilent(t *testing.T) {
	t.Parallel()

	mustCompile(t, `edition: v2026.3
name: trigger-kind-dynamic
inputs:
  expected_kind: { type: string, required: true }
vars:
  wanted: ${"webhook"}
steps:
  - id: against_input
    if: ${trigger.kind == inputs.expected_kind}
    log:
      message: hi
  - id: against_var
    if: ${trigger.kind == vars.wanted}
    log:
      message: hi
  - id: against_another_field
    if: ${trigger.kind == trigger.name}
    log:
      message: hi
`)
}

// TestTriggerKindOutsideComparisonStaysSilent is the second negative
// direction: a use of `trigger.kind` that is not an `==`/`!=` comparison at
// all — interpolated into a message, or passed to a function — must not be
// reported. There is no literal to judge it against, so nothing here should
// look like the check added for the PR #514 finding.
func TestTriggerKindOutsideComparisonStaysSilent(t *testing.T) {
	t.Parallel()

	mustCompile(t, `edition: v2026.3
name: trigger-kind-outside-comparison
steps:
  - id: interpolated
    log:
      message: ${"started as " + trigger.kind}
  - id: passed_to_function
    if: ${size(trigger.kind) > 0}
    log:
      message: hi
`)
}

// TestNonTriggerKindComparisonsAreUnaffected is the third negative direction:
// anything that is not `trigger.kind` must be untouched by this check, even
// when it is compared to the exact literal a typo test above uses. A
// `trigger.name` comparison is deliberately not checked (see the PR body for
// why), and an ordinary `vars:` comparison against the same string must not
// be caught by an over-broad match on the literal rather than on the field.
func TestNonTriggerKindComparisonsAreUnaffected(t *testing.T) {
	t.Parallel()

	mustCompile(t, `edition: v2026.3
name: trigger-kind-scope
vars:
  status: ${"schedual"}
steps:
  - id: by_name
    if: ${trigger.name == "schedual"}
    log:
      message: hi
  - id: by_var
    if: ${vars.status == "schedual"}
    log:
      message: hi
`)
}

// TestATriggerCannotReadItsOwnContext pins the reverse scope: `trigger` is not
// bound where a trigger's own arguments are evaluated.
//
// Not because a trigger has no context, but because that expression *is* the
// trigger: the context is fixed at the moment this mapping produces a run, so
// reading it while computing that run's arguments is a value asking about itself.
func TestATriggerCannotReadItsOwnContext(t *testing.T) {
	t.Parallel()

	diagnostics := validateTriggerSource(t, `edition: v2026.3
name: self-referential-trigger
inputs:
  origin: { type: string, required: true }
triggers:
  - webhook: stripe
    verify:
      stripe: ${secret('env:STRIPE_WEBHOOK_SECRET')}
    idempotency_key: ${event.headers["stripe-signature"]}
    with:
      origin: ${trigger.name}
steps:
  - id: record
    log:
      message: ${inputs.origin}
`)

	var found bool
	for _, d := range diagnostics {
		if strings.Contains(d.Message, "trigger.name") {
			found = true
		}
	}
	require.True(t, found, "a trigger reading its own context was accepted: %v", diagnostics)
}

// TestTriggerIsRefusedAsANameAFileBinds covers the root's cost, which is the only
// thing adding one costs: the word itself.
//
// Refused as a step id, a loop's `as:` and a step's own `vars:` key, exactly as
// `steps`, `vars`, `inputs` and `run` already are — because all three are names
// that *win* over a root when an expression resolves, so a file taking one does
// not collide with the root, it hides it silently for everything after.
func TestTriggerIsRefusedAsANameAFileBinds(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
	}{
		{
			name: "as a step id",
			source: `steps:
  - id: trigger
    log:
      message: hi
`,
		},
		{
			name: "as a loop binding",
			source: `steps:
  - id: each
    for_each:
      items: ${["a"]}
      as: trigger
      steps:
        - id: inner
          log:
            message: ${trigger}
`,
		},
		{
			name: "as a step's own var",
			source: `steps:
  - id: work
    vars:
      trigger: hello
    log:
      message: ${trigger}
`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			diagnostics := validateTriggerSource(t, "edition: v2026.3\nname: shadowing\n"+test.source)
			require.NotEmpty(t, diagnostics,
				"a name hiding the `trigger` root was accepted, so every reference after it silently "+
					"means something else")

			var found bool
			for _, d := range diagnostics {
				if strings.Contains(d.Message, "how the run started") {
					found = true
				}
			}
			assert.True(t, found, "the refusal did not say what the root holds: %v", diagnostics)
		})
	}
}

// validateTriggerSource validates a Flowfile written inline, failing the test where the
// source will not compile at all — which is a different thing from a file with
// diagnostics, and is never what one of these tests means to be exercising.
func validateTriggerSource(t *testing.T, source string) flowfile.Diagnostics {
	t.Helper()

	diagnostics, err := flowfile.ValidateSource([]byte(source))
	if err == nil {
		return diagnostics
	}

	// A file the compiler refuses outright comes back as one error carrying every
	// diagnostic it found, positions and all, because there is no workflow left to
	// validate. Read back into the same shape, so a test asserting on wording and
	// on a position does not have to know which of the two moments reported it —
	// which is a detail of where a rule happens to live, not of what an author sees.
	return parseCompilerRefusal(t, err)
}

// parseCompilerRefusal turns the compiler's `line:column: field: message` lines
// back into diagnostics.
//
// Positions are asserted from the text rather than trusted, which is the point: a
// diagnostic reported here with no line is exactly the failure these tests are
// checking for, and a helper that quietly supplied one would hide it.
func parseCompilerRefusal(t *testing.T, err error) flowfile.Diagnostics {
	t.Helper()

	var ds flowfile.Diagnostics
	for _, line := range strings.Split(err.Error(), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		d := flowfile.Diagnostic{Message: line}
		if head, rest, ok := strings.Cut(line, ": "); ok {
			if lineText, columnText, split := strings.Cut(head, ":"); split {
				if n, convErr := strconv.Atoi(lineText); convErr == nil {
					d.Line = n
				}
				if n, convErr := strconv.Atoi(columnText); convErr == nil {
					d.Column = n
				}
				d.Message = rest
			}
		}

		ds = append(ds, d)
	}

	return ds
}

// mustCompile parses a Flowfile and fails the test with every diagnostic if it
// does not validate, so a test about one rule is never quietly passing because
// the file it was written against stopped compiling for another reason.
func mustCompile(t *testing.T, source string) *v1.Workflow {
	t.Helper()

	diagnostics := validateTriggerSource(t, source)
	require.Empty(t, diagnostics, "the file did not validate: %v", diagnostics)

	workflow, err := flowfile.Unmarshal([]byte(source))
	require.NoError(t, err)

	return workflow
}
