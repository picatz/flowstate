package flowfile_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `flow fix` bringing who-may-act into the one predicate that says it
// (docs/STYLE.md R1/R2, issue #326).
//
// Three kinds of claim are made here, and each is asserted the way it can be
// falsified:
//
//   - the bytes written, against a golden, because a rewrite that still validates
//     can mean something else (CLAUDE.md's rewriter section);
//   - the *decision*, against the engine itself: the old spelling and the one
//     the tool writes are both compiled and both asked, for every sender and
//     starter in a grid, and must answer the same;
//   - the committed examples, which must be exactly what the tool writes from the
//     spelling they were in.

const (
	policyIssuer = "https://issuer.example.com"
)

// workflowWith places a policy under the stanza it belongs to, in a workflow that
// is otherwise valid: a wait for the signal, and the inputs a predicate reads.
func workflowWith(stanza, body string) string {
	const (
		head = `edition: v2026.4
name: who-may-act
inputs:
  approver:
    type: string
    default: ""
  spec:
    type: string
    default: ""
`
		steps = `steps:
  - id: gate
    wait_for_signal:
      name: go
      timeout: 5s
`
	)
	switch stanza {
	case "signals":
		return head + "signals:\n  go:\n" + indent(body, 4) + steps
	case "debug":
		return head + "debug:\n" + indent(body, 2) + steps
	case "manual":
		return head + "triggers:\n  manual:\n" + indent(body, 4) + steps
	default:
		panic("unknown stanza " + stanza)
	}
}

func indent(body string, n int) string {
	pad := strings.Repeat(" ", n)
	var b strings.Builder
	for line := range strings.SplitSeq(strings.TrimSuffix(body, "\n"), "\n") {
		if line == "" {
			b.WriteString("\n")
			continue
		}
		b.WriteString(pad + line + "\n")
	}

	return b.String()
}

func fixAllow(t *testing.T, src string) flowfile.FixResult {
	t.Helper()

	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)

	return result
}

// TestFixWritesEachOldFormAsItsPredicate pins the text, case by case.
func TestFixWritesEachOldFormAsItsPredicate(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, stanza, old, want string
	}{
		{
			name: "one claim", stanza: "signals",
			old:  "allow:\n  - claims:\n      team: sre\n",
			want: "allow: ${sender.identity.claims.team == \"sre\"}\n",
		},
		{
			name: "subject, claims and namespace of one rule, in the order they are checked", stanza: "signals",
			old:  "allow:\n  - namespace: payments\n    claims:\n      role: on-call\n      team: sre\n    subject: \"https://issuer.example.com#bot@example.com\"\n",
			want: "allow: ${sender.identity.principal == \"https://issuer.example.com#bot@example.com\" && sender.identity.claims.role == \"on-call\" && sender.identity.claims.team == \"sre\" && sender.identity.namespace == \"payments\"}\n",
		},
		{
			name: "rules are alternatives, and a rule of several checks is parenthesized to say so", stanza: "signals",
			old:  "allow:\n  - subject: \"https://issuer.example.com#a@example.com\"\n    claims: {team: x}\n  - claims: {role: sre-lead}\n",
			want: "allow: ${(sender.identity.principal == \"https://issuer.example.com#a@example.com\" && sender.identity.claims.team == \"x\") || sender.identity.claims.role == \"sre-lead\"}\n",
		},
		{
			name: "distinct_from_starter wraps the whole disjunction", stanza: "signals",
			old:  "allow:\n  - claims: {team: x}\n  - claims: {role: y}\ndistinct_from_starter: true\n",
			want: "allow: ${(sender.identity.claims.team == \"x\" || sender.identity.claims.role == \"y\") && sender.identity.principal != run.identity.principal}\n",
		},
		{
			name: "distinct_from_starter beside one rule needs no parentheses", stanza: "signals",
			old:  "allow:\n  - claims: {team: x}\ndistinct_from_starter: true\n",
			want: "allow: ${sender.identity.claims.team == \"x\" && sender.identity.principal != run.identity.principal}\n",
		},
		{
			name: "distinct_from_starter written before the rules", stanza: "signals",
			old:  "distinct_from_starter: true\nallow:\n  - claims: {team: x}\n",
			want: "allow: ${sender.identity.claims.team == \"x\" && sender.identity.principal != run.identity.principal}\n",
		},
		{
			name: "distinct_from_starter false says nothing and is dropped", stanza: "signals",
			old:  "allow:\n  - claims: {team: x}\ndistinct_from_starter: false\n",
			want: "allow: ${sender.identity.claims.team == \"x\"}\n",
		},
		{
			name: "an interpolated subject keeps its expression and the claim that narrows it", stanza: "signals",
			old:  "allow:\n  - subject: ${\"https://issuer.example.com#\" + inputs.approver}\n    claims:\n      team: release-managers\ndistinct_from_starter: true\n",
			want: "allow: ${sender.identity.principal == \"https://issuer.example.com#\" + inputs.approver && sender.identity.claims.team == \"release-managers\" && sender.identity.principal != run.identity.principal}\n",
		},
		{
			name: "an interpolated subject that could be empty is not allowed to match the empty principal", stanza: "signals",
			old:  "allow:\n  - subject: ${inputs.spec}\n    claims: {team: x}\n",
			want: "allow: ${sender.identity.principal != \"\" && sender.identity.principal == inputs.spec && sender.identity.claims.team == \"x\"}\n",
		},
		{
			name: "an interpolated subject narrowed only by distinct_from_starter", stanza: "signals",
			old:  "allow:\n  - subject: ${inputs.spec}\ndistinct_from_starter: true\n",
			want: "allow: ${sender.identity.principal != \"\" && sender.identity.principal == inputs.spec && sender.identity.principal != run.identity.principal}\n",
		},
		{
			name: "a ternary subject is parenthesized so it stays one operand", stanza: "signals",
			old:  "allow:\n  - subject: '${inputs.approver == \"\" ? inputs.spec : \"https://issuer.example.com#\" + inputs.approver}'\n    claims: {team: x}\n",
			want: "allow: '${sender.identity.principal != \"\" && sender.identity.principal == (inputs.approver == \"\" ? inputs.spec : \"https://issuer.example.com#\" + inputs.approver) && sender.identity.claims.team == \"x\"}'\n",
		},
		{
			name: "a multi-line subject expression goes onto one line", stanza: "signals",
			old:  "allow:\n  - subject: |-\n      ${\"https://issuer.example.com#\" +\n        inputs.approver}\n    claims: {team: x}\n",
			want: "allow: ${sender.identity.principal == \"https://issuer.example.com#\" + inputs.approver && sender.identity.claims.team == \"x\"}\n",
		},
		{
			name: "claim names that are not identifiers, and values that need escaping", stanza: "signals",
			old:  "allow:\n  - claims:\n      team-name: a\n      in: b\n      \"a.b\": c\n      q: 'say \"hi\" \\ bye'\n",
			want: "allow: ${sender.identity.claims[\"team-name\"] == \"a\" && sender.identity.claims[\"in\"] == \"b\" && sender.identity.claims[\"a.b\"] == \"c\" && sender.identity.claims.q == \"say \\\"hi\\\" \\\\ bye\"}\n",
		},
		{
			name: "a predicate beside distinct_from_starter gains the clause", stanza: "signals",
			old:  "allow: ${sender.identity.claims.team == \"x\" || sender.identity.claims.role == \"y\"}\ndistinct_from_starter: true\n",
			want: "allow: ${(sender.identity.claims.team == \"x\" || sender.identity.claims.role == \"y\") && sender.identity.principal != run.identity.principal}\n",
		},
		{
			name: "debug takes the same rewrite", stanza: "debug",
			old:  "allow:\n  - claims:\n      team: sre\n",
			want: "allow: ${sender.identity.claims.team == \"sre\"}\n",
		},
		{
			name: "allowed_principals as a list", stanza: "manual",
			old:  "require_reason: true\nallowed_principals:\n  - https://issuer.example.com#a@example.com\n  - https://issuer.example.com#b@example.com\n",
			want: "require_reason: true\nallow: ${sender.identity.principal in [\"https://issuer.example.com#a@example.com\", \"https://issuer.example.com#b@example.com\"]}\n",
		},
		{
			name: "allowed_principals as one subject", stanza: "manual",
			old:  "allowed_principals: https://issuer.example.com#a@example.com\n",
			want: "allow: ${sender.identity.principal in [\"https://issuer.example.com#a@example.com\"]}\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := fixAllow(t, workflowWith(tc.stanza, tc.old))
			require.Empty(t, result.Refusals)
			require.True(t, result.Changed())
			assert.Equal(t, workflowWith(tc.stanza, tc.want), string(result.Source))

			// The result is a fixed point, still validates, and is what the
			// formatter already writes: a second run and `flow fmt` both find nothing.
			again := fixAllow(t, string(result.Source))
			assert.False(t, again.Changed(), "a second run changed it: %v", again.Changes)

			wf, _, err := flowfile.Parse(result.Source)
			require.NoError(t, err)
			assert.Empty(t, flowfile.Validate(wf))
			formatted, err := flowfile.Format(result.Source, wf)
			require.NoError(t, err)
			assert.Equal(t, string(result.Source), string(formatted))
		})
	}
}

// TestFixRefusesWhatItWouldHaveToGuess: nothing is written, the old spelling
// keeps working, and the message says why.
func TestFixRefusesWhatItWouldHaveToGuess(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, stanza, old, why string
	}{
		{"an interpolated subject nothing narrows", "signals",
			"allow:\n  - subject: ${inputs.spec}\n", "nothing beside it narrows"},
		{"an interpolated subject narrowed only in another rule", "signals",
			"allow:\n  - subject: ${inputs.spec}\n  - claims: {team: x}\n", "nothing beside it narrows"},
		{"a bare subject", "signals",
			"allow:\n  - subject: alice@example.com\n", "not \"<issuer>#<subject>\""},
		{"a rule that checks nothing", "signals",
			"allow:\n  - claims: {}\n", "matches every sender"},
		{"an empty namespace", "signals",
			"allow:\n  - namespace: \"\"\n    claims: {team: x}\n", "no constraint"},
		{"a claim that is not a string", "signals",
			"allow:\n  - claims:\n      level: 3\n", "must be a string"},
		{"a fence inside a literal", "signals",
			"allow:\n  - claims:\n      team: \"a-${inputs.spec}\"\n", "cannot be an expression"},
		{"a key a rule does not say", "signals",
			"allow:\n  - issuer: https://issuer.example.com\n    claims: {team: x}\n", "`issuer:` is none of them"},
		{"a policy in flow style", "signals",
			"{allow: [{claims: {team: x}}], distinct_from_starter: true}\n", ""},
		{"a subject expression with a comment that would swallow the rest", "signals",
			"allow:\n  - subject: |-\n      ${\"https://issuer.example.com#\" + inputs.approver // who\n      }\n    claims: {team: x}\n", "`//` comment"},
		{"a comment beside a value that cannot be told from text", "signals",
			"allow:\n  - claims:\n      team: it's # the sre team\n", "sits beside a value"},
		{"a predicate whose distinct_from_starter would make it pass narrowing", "signals",
			"allow: ${inputs.spec == \"x\"}\ndistinct_from_starter: true\n", "does not validate on its own"},
		{"both manual spellings", "manual",
			"allowed_principals: https://issuer.example.com#a@example.com\nallow: ${sender.identity.claims.team == \"x\"}\n", "both"},
		{"a manual principal that is bare", "manual",
			"allowed_principals: alice\n", "not \"<issuer>#<subject>\""},
		{"a manual principal listed twice", "manual",
			"allowed_principals: [\"https://issuer.example.com#a\", \"https://issuer.example.com#a\"]\n", "twice"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			stanza := tc.stanza
			var src string
			if strings.HasPrefix(tc.old, "{") {
				src = workflowWith(stanza, "x: 1\n")
				src = strings.Replace(src, "  go:\n    x: 1\n", "  go: "+tc.old, 1)
			} else {
				src = workflowWith(stanza, tc.old)
			}

			result := fixAllow(t, src)
			require.NotEmpty(t, result.Refusals, "expected a refusal; changes: %v", result.Changes)
			assert.False(t, result.Complete())
			assert.False(t, result.Changed())
			assert.Equal(t, src, string(result.Source), "a refused file is left byte for byte as it was")
			if tc.why != "" {
				var all []string
				for _, r := range result.Refusals {
					all = append(all, r.Message)
				}
				assert.Contains(t, strings.Join(all, "\n"), tc.why)
			}
		})
	}
}

// TestFixCarriesCommentsFromTheOldRulesAndSaysSo: a comment is the part of a file a
// tool can least afford to lose.
func TestFixCarriesCommentsFromTheOldRulesAndSaysSo(t *testing.T) {
	t.Parallel()

	result := fixAllow(t, workflowWith("signals", `# why this gate exists
allow:
  # release managers only
  - claims:
      team: release-managers  # not the whole org
  # and the on-call lead
  - claims: {role: sre-lead}
distinct_from_starter: true # never your own
`))
	require.Empty(t, result.Refusals)
	assert.Equal(t, workflowWith("signals", `# why this gate exists
# release managers only
# not the whole org
# and the on-call lead
# never your own
allow: ${(sender.identity.claims.team == "release-managers" || sender.identity.claims.role == "sre-lead") && sender.identity.principal != run.identity.principal}
`), string(result.Source))
	require.Len(t, result.Changes, 1)
	assert.Contains(t, result.Changes[0].Message, "comments written inside the old rules were moved above it")
}

// TestFixDoesNotRootAStepNamedSenderInsideThePredicate: `sender` and `run` are
// the predicate's own names, and a step of the same name is not what is written.
// The rewrite that produces these predicates came out as `steps.sender.identity`
// for such a file, and the result validated.
func TestFixDoesNotRootAStepNamedSenderInsideThePredicate(t *testing.T) {
	t.Parallel()

	src := strings.Replace(workflowWith("signals", "allow:\n  - claims: {team: x}\ndistinct_from_starter: true\n"),
		"steps:\n", "steps:\n  - id: sender\n    value: 1\n", 1)
	result := fixAllow(t, src)
	require.Empty(t, result.Refusals)
	assert.Contains(t, string(result.Source), `allow: ${sender.identity.claims.team == "x" && sender.identity.principal != run.identity.principal}`)
	assert.NotContains(t, string(result.Source), "steps.sender")

	manual := strings.Replace(workflowWith("manual", "allowed_principals: https://issuer.example.com#a@example.com\n"),
		"steps:\n", "steps:\n  - id: sender\n    value: 1\n", 1)
	result = fixAllow(t, manual)
	require.Empty(t, result.Refusals)
	assert.NotContains(t, string(result.Source), "steps.sender")
}

// ---------------------------------------------------------------------------
// Equivalence: the same decision, asked of the engine.

func ident(issuer, subject, namespace string, claims map[string]string) *v1.WorkloadIdentity {
	return &v1.WorkloadIdentity{Issuer: issuer, Subject: subject, Namespace: namespace, Claims: claims}
}

// senderGrid is every caller the cases below can tell apart, and several they
// cannot: the empty identity, an identity with one half missing, a subject that
// differs from a rule's only by issuer, claims present with the wrong or an empty
// value.
func senderGrid() []*v1.WorkloadIdentity {
	issuers := []string{"", policyIssuer, "https://other.example.com"}
	subjects := []string{"", "a@example.com", "b@example.com", "bot@example.com", "alice@example.com", "a#b"}
	namespaces := []string{"", "payments", "other"}
	claimSets := []map[string]string{
		nil,
		{"team": "x"},
		{"team": "release-managers"},
		{"team": "sre"},
		{"team": ""},
		{"role": "sre-lead"},
		{"role": "on-call", "team": "sre"},
		{"team": "x", "role": "sre-lead"},
		{"team-name": "a", "in": "b", "a.b": "c", "q": `say "hi" \ bye`},
		{"team-name": "a"},
	}

	var out []*v1.WorkloadIdentity
	for _, issuer := range issuers {
		for _, subject := range subjects {
			for _, namespace := range namespaces {
				for _, claims := range claimSets {
					out = append(out, ident(issuer, subject, namespace, claims))
				}
			}
		}
	}

	return out
}

func starterGrid() []*v1.WorkloadIdentity {
	return []*v1.WorkloadIdentity{
		nil, // unknown: the run records no starter
		ident("", "", "", nil),
		ident(policyIssuer, "a@example.com", "payments", nil),
		ident(policyIssuer, "alice@example.com", "payments", map[string]string{"team": "x"}),
		ident("https://other.example.com", "a@example.com", "", nil),
		ident("", "a@example.com", "", nil), // half an identity
	}
}

func inputsOf(m map[string]string) map[string]*v1.Value {
	out := make(map[string]*v1.Value, len(m))
	for k, v := range m {
		out[k] = v1.NewLiteral(v)
	}

	return out
}

// decide asks the engine whether one sender may act under the policy a workflow
// declares, resolving a rule list's interpolated subjects the way submit does.
//
// started is false when the run would never have been created: submit refuses a
// rule list whose interpolated subject resolves to something that is not
// "<issuer>#<subject>", before any signal could be delivered, so there is no
// delivery whose answer could be compared.
func decide(t *testing.T, stanza string, wf *v1.Workflow, sender, starter *v1.WorkloadIdentity, inputs map[string]*v1.Value) (allowed, started bool) {
	t.Helper()

	ctx := context.Background()
	hasStarter := starter != nil

	switch stanza {
	case "manual":
		return v1.CheckManualStart(ctx, wf, sender, v1.Principal(sender.GetIssuer(), sender.GetSubject()), "because", inputs) == nil, true
	case "debug":
		policy, err := v1.ResolvePolicySubjects(ctx, "debug", wf.GetDebug(), &v1.Scope{Inputs: inputs})
		if err != nil {
			return false, false
		}

		return v1.DebugPolicyCheck(ctx, policy, sender, starter, hasStarter, inputs) == nil, true
	default:
		policy, err := v1.ResolvePolicySubjects(ctx, "signals[go]", wf.GetSignals()["go"], &v1.Scope{Inputs: inputs})
		if err != nil {
			return false, false
		}

		return v1.SignalPolicyCheck(ctx, policy, sender, starter, hasStarter, inputs) == nil, true
	}
}

func halfFormed(id *v1.WorkloadIdentity) bool {
	return id != nil && (id.GetIssuer() == "") != (id.GetSubject() == "")
}

// TestFixedPredicateDecidesLikeTheRulesItReplaced replays each old spelling and the
// predicate the tool writes from it through the engine over a grid of senders,
// starters and inputs, and requires the same answer to every question.
//
// One divergence is known and asserted as exactly that: an identity with one half
// missing has principal "" ([v1.Principal]) where the rule list compared a "#"-joined
// form, so `distinct_from_starter` — the one place a principal is compared to another
// — can only be stricter, never looser, for such an identity. An authenticated
// identity has both halves, so this does not arise on a server.
func TestFixedPredicateDecidesLikeTheRulesItReplaced(t *testing.T) {
	t.Parallel()

	const (
		alice = policyIssuer + "#alice@example.com"
		bot   = policyIssuer + "#bot@example.com"
	)

	type input = map[string]string
	noInputs := []input{{}}

	for _, tc := range []struct {
		name, stanza, old string
		inputs            []input
	}{
		{"claims", "signals", "allow:\n  - claims: {team: release-managers}\n", noInputs},
		{"literal subject", "signals", "allow:\n  - subject: " + bot + "\n", noInputs},
		{"literal subject, namespace and claims", "signals",
			"allow:\n  - subject: " + bot + "\n    namespace: payments\n    claims: {team: sre}\n", noInputs},
		{"namespace and claims", "signals", "allow:\n  - namespace: payments\n    claims: {role: on-call}\n", noInputs},
		{"alternatives", "signals",
			"allow:\n  - subject: " + bot + "\n    claims: {team: sre}\n  - claims: {role: sre-lead}\n  - namespace: other\n    claims: {team: x}\n", noInputs},
		{"alternatives with distinct_from_starter", "signals",
			"allow:\n  - claims: {team: x}\n  - claims: {role: sre-lead}\n  - subject: " + bot + "\ndistinct_from_starter: true\n", noInputs},
		{"one rule with distinct_from_starter", "signals",
			"allow:\n  - claims: {team: x}\ndistinct_from_starter: true\n", noInputs},
		{"distinct_from_starter: false", "signals",
			"allow:\n  - claims: {team: x}\ndistinct_from_starter: false\n", noInputs},
		{"claim names that are not identifiers", "signals",
			"allow:\n  - claims:\n      team-name: a\n      in: b\n      \"a.b\": c\n      q: 'say \"hi\" \\ bye'\n", noInputs},
		{"interpolated subject and claims", "signals",
			"allow:\n  - subject: ${\"" + policyIssuer + "#\" + inputs.approver}\n    claims: {team: release-managers}\ndistinct_from_starter: true\n",
			[]input{{"approver": "alice@example.com"}, {"approver": "a@example.com"}, {"approver": ""}, {"approver": "a#b"}}},
		{"interpolated subject and claims, two rules", "signals",
			"allow:\n  - subject: ${\"" + policyIssuer + "#\" + inputs.approver}\n    claims: {team: x}\n  - subject: ${\"" + policyIssuer + "#\" + inputs.spec}\n    claims: {team: sre}\n",
			[]input{{"approver": "alice@example.com", "spec": "a@example.com"}, {"approver": "b@example.com", "spec": ""}}},
		{"interpolated subject narrowed by distinct_from_starter alone", "signals",
			"allow:\n  - subject: ${\"" + policyIssuer + "#\" + inputs.approver}\ndistinct_from_starter: true\n",
			[]input{{"approver": "alice@example.com"}, {"approver": "a@example.com"}, {"approver": ""}}},
		// The case the empty-principal guard exists for: the input is empty, the
		// rule list refused the run at submit, and a bare `principal == inputs.spec`
		// would have admitted every unauthenticated sender.
		{"interpolated subject that can be empty, and an anonymous sender", "signals",
			"allow:\n  - subject: ${inputs.spec}\ndistinct_from_starter: true\n",
			[]input{{"spec": alice}, {"spec": ""}, {"spec": "not-qualified"}, {"spec": bot}}},
		{"interpolated subject that can be empty, narrowed by a claim", "signals",
			"allow:\n  - subject: ${inputs.spec}\n    claims: {team: x}\n",
			[]input{{"spec": alice}, {"spec": ""}, {"spec": bot}}},
		{"ternary subject", "signals",
			"allow:\n  - subject: '${inputs.approver == \"\" ? inputs.spec : \"" + policyIssuer + "#\" + inputs.approver}'\n    claims: {team: x}\n",
			[]input{{"approver": "", "spec": alice}, {"approver": "a@example.com", "spec": ""}, {"approver": "", "spec": ""}}},
		{"predicate gaining distinct_from_starter", "signals",
			"allow: ${sender.identity.claims.team == \"x\" || sender.identity.claims.role == \"sre-lead\"}\ndistinct_from_starter: true\n", noInputs},
		{"debug claims", "debug", "allow:\n  - claims: {team: sre}\n", noInputs},
		{"debug with an interpolated subject", "debug",
			"allow:\n  - subject: ${\"" + policyIssuer + "#\" + inputs.approver}\n    claims: {team: sre}\n",
			[]input{{"approver": "alice@example.com"}, {"approver": ""}}},
		{"manual principals", "manual",
			"allowed_principals:\n  - " + alice + "\n  - " + bot + "\n", noInputs},
		{"manual one principal", "manual", "allowed_principals: " + bot + "\n", noInputs},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			oldSrc := workflowWith(tc.stanza, tc.old)
			result := fixAllow(t, oldSrc)
			require.Empty(t, result.Refusals)
			require.True(t, result.Changed(), "the case does not exercise the rewrite")

			oldWF, _, err := flowfile.Parse([]byte(oldSrc))
			require.NoError(t, err)
			require.Empty(t, flowfile.Validate(oldWF), "the old spelling must be a valid file for the comparison to mean anything")
			newWF, _, err := flowfile.Parse(result.Source)
			require.NoError(t, err)
			require.Empty(t, flowfile.Validate(newWF))

			var allowed, denied, softer, unstarted, total int
			for _, in := range tc.inputs {
				values := inputsOf(in)
				for _, sender := range senderGrid() {
					for _, starter := range starterGrid() {
						if tc.stanza == "manual" && starter != nil {
							continue // a manual start has no run, so no starter
						}

						oldAllows, started := decide(t, tc.stanza, oldWF, sender, starter, values)
						newAllows, _ := decide(t, tc.stanza, newWF, sender, starter, values)
						total++

						if !started {
							// The rule list would have refused this run at submit, so there is
							// nothing to compare, and the predicate has to be safe on its own:
							// whatever it admits is a sender with a whole principal, never an
							// unauthenticated one that an empty computed subject could equal.
							unstarted++
							if newAllows && v1.Principal(sender.GetIssuer(), sender.GetSubject()) == "" {
								t.Errorf("the run is refused by the rule list, and the predicate admits a sender with no principal: sender=%v starter=%v inputs=%v\nrewritten: %s",
									sender, starter, in, rewrittenPolicy(string(result.Source)))

								return
							}

							continue
						}

						if oldAllows {
							allowed++
						} else {
							denied++
						}

						if oldAllows == newAllows {
							continue
						}

						// The one known divergence, and only in the stricter direction.
						if oldAllows && !newAllows && (halfFormed(sender) || halfFormed(starter)) {
							softer++
							continue
						}

						t.Errorf("old=%v new=%v for sender=%v starter=%v inputs=%v\nrewritten: %s",
							oldAllows, newAllows, sender, starter, in, rewrittenPolicy(string(result.Source)))
						if t.Failed() {
							return
						}
					}
				}
			}

			assert.Positive(t, allowed, "no sender was ever allowed, so the comparison proved nothing about allowing")
			assert.Positive(t, denied, "no sender was ever denied, so the comparison proved nothing about denying")
			t.Logf("%d questions, %d allowed by the rule list, %d refused, %d for runs the rule list would not have started; %d stricter for half-formed identities",
				total, allowed, denied, unstarted, softer)
		})
	}
}

// rewrittenPolicy is the line a failure should quote.
func rewrittenPolicy(src string) string {
	for line := range strings.SplitSeq(src, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "allow:") {
			return strings.TrimSpace(line)
		}
	}

	return ""
}

// TestFixedInterpolatedSubjectKeepsTheConjunctionThatNarrowsIt: the narrowing rule
// is now syntactic over the whole predicate, so a rewrite that dropped the claim
// beside an interpolated subject would still read `run.identity` or `claims`
// elsewhere and pass. Asked of the written predicate itself.
func TestFixedInterpolatedSubjectKeepsTheConjunctionThatNarrowsIt(t *testing.T) {
	t.Parallel()

	result := fixAllow(t, workflowWith("signals", `allow:
  - subject: ${"https://issuer.example.com#" + inputs.approver}
    claims: {team: release-managers}
  - claims: {role: sre-lead}
`))
	require.Empty(t, result.Refusals)

	line := rewrittenPolicy(string(result.Source))
	// The first alternative still conjoins the subject computed from inputs with the
	// claim, and the claim is not hoisted into the second alternative's place.
	assert.Contains(t, line, `(sender.identity.principal == "https://issuer.example.com#" + inputs.approver && sender.identity.claims.team == "release-managers") || sender.identity.claims.role == "sre-lead"`)

	// And the engine agrees it is what narrows: an input-named approver who lacks
	// the claim is refused, even though another alternative reads claims.
	wf, _, err := flowfile.Parse(result.Source)
	require.NoError(t, err)
	in := inputsOf(map[string]string{"approver": "alice@example.com"})
	named := ident(policyIssuer, "alice@example.com", "", map[string]string{"team": "other"})
	allowed, _ := decide(t, "signals", wf, named, nil, in)
	assert.False(t, allowed)
	named.Claims["team"] = "release-managers"
	allowed, _ = decide(t, "signals", wf, named, nil, in)
	assert.True(t, allowed)
}

// ---------------------------------------------------------------------------
// The committed examples are what the tool writes.

// TestEveryRewrittenExampleIsExactlyWhatFixWritesFromItsOldSpelling holds each
// example that used the retired spellings to the tool's own output. testdata/fixallow
// keeps the file as it was written before the rewrite, named by its path under
// examples/ with `__` for `/`; the committed example must equal what Fix makes of it,
// byte for byte, so nobody edits the migration's result by hand and nobody leaves an
// example in a spelling the tool would still change.
func TestEveryRewrittenExampleIsExactlyWhatFixWritesFromItsOldSpelling(t *testing.T) {
	t.Parallel()

	before, err := filepath.Glob(filepath.Join("testdata", "fixallow", "*.yaml"))
	require.NoError(t, err)
	require.NotEmpty(t, before)

	for _, path := range before {
		name := strings.ReplaceAll(strings.TrimSuffix(filepath.Base(path), ".yaml"), "__", "/") + ".yaml"
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			old, err := os.ReadFile(path)
			require.NoError(t, err)
			want, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "examples", filepath.FromSlash(name)))
			require.NoError(t, err)

			result, err := flowfile.Fix(old)
			require.NoError(t, err)
			require.Empty(t, result.Refusals)
			require.True(t, result.Changed(), "the fixture holds nothing to rewrite, so it proves nothing")
			assert.Equal(t, string(want), string(result.Source), "examples/%s is not what `flow fix` writes", name)

			// And none of the retired spellings is left behind, outside the prose that
			// still talks about them.
			for line := range strings.SplitSeq(string(result.Source), "\n") {
				if strings.HasPrefix(strings.TrimSpace(line), "#") {
					continue
				}
				for _, gone := range []string{"distinct_from_starter:", "allowed_principals:", "- subject:", "- claims:", "- namespace:"} {
					assert.NotContains(t, line, gone)
				}
			}
		})
	}
}

// TestNoExampleKeepsARetiredWhoMayActSpelling is the other direction: an example
// added later in the old spelling is found here rather than by a reader.
func TestNoExampleKeepsARetiredWhoMayActSpelling(t *testing.T) {
	t.Parallel()

	root := filepath.Join("..", "..", "..", "..", "examples")
	var checked int
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".yaml") || strings.HasSuffix(path, ".test.yaml") {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		checked++

		result, err := flowfile.Fix(data)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		for _, change := range result.Changes {
			assert.NotContains(t, change.Message, "`allow:` rule list", "%s", path)
			assert.NotContains(t, change.Message, "`allowed_principals:`", "%s", path)
		}

		return nil
	})
	require.NoError(t, err)
	assert.Positive(t, checked)
}
