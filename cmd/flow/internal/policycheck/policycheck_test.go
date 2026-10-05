package policycheck_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/policycheck"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

const issuer = "https://issuer.example.com"

// gated declares one gate of every kind, each reading something different, so a
// decision can only come out right by reading the right thing:
//
//   - approve: the named approver, never the run's own starter
//   - crash: a claim (a sender without it makes the predicate error)
//   - pin: a claim compared with a sensitive input (the value must never print)
//   - note: no policy at all
const gated = `
edition: v2026.4
name: gated
inputs:
  approver:
    type: string
    default: ops@example.com
  pin:
    type: string
    default: input-secret-9f2c
    sensitive: true
signals:
  approve:
    allow: ${sender.identity.principal == "https://issuer.example.com#" + inputs.approver && sender.identity.principal != run.identity.principal}
  crash:
    allow: ${sender.identity.claims.team == "sre"}
  pin:
    allow: ${sender.identity.claims.pin == inputs.pin}
debug:
  allow: ${sender.identity.claims.team == "sre"}
triggers:
  - manual:
      require_reason: true
      allow: ${sender.identity.claims.team == "dev"}
steps:
  - id: a
    wait_for_signal: {name: approve, timeout: 1h}
  - id: b
    wait_for_signal: {name: crash, timeout: 1h}
  - id: c
    wait_for_signal: {name: pin, timeout: 1h}
  - id: d
    wait_for_signal: {name: note, timeout: 1h}
`

func compile(t *testing.T, src string) *v1.Workflow {
	t.Helper()

	wf, _, err := flowfile.Parse([]byte(src))
	require.NoError(t, err)

	return wf
}

func id(subject string, claims ...string) *flowtest.ScriptedIdentity {
	identity := &flowtest.ScriptedIdentity{Subject: subject, Issuer: issuer, Claims: map[string]string{}}
	for _, claim := range claims {
		name, value, _ := strings.Cut(claim, "=")
		identity.Claims[name] = value
	}

	return identity
}

func decide(t *testing.T, wf *v1.Workflow, gate policycheck.Gate, subject policycheck.Subject) policycheck.Decision {
	t.Helper()

	decisions, err := policycheck.Evaluate(t.Context(), wf, []policycheck.Gate{gate}, subject)
	require.NoError(t, err)
	require.Len(t, decisions, 1)

	return decisions[0]
}

func signal(name string) policycheck.Gate {
	return policycheck.Gate{Stanza: policycheck.StanzaSignals, Name: name}
}

var (
	debugGate  = policycheck.Gate{Stanza: policycheck.StanzaDebug}
	manualGate = policycheck.Gate{Stanza: policycheck.StanzaManual}
)

const (
	notSatisfied = "does not satisfy this signal's allow predicate"
	errored      = "could not be evaluated for this sender"
)

func TestEvaluateDecidesEachGateTheWayTheEngineDoes(t *testing.T) {
	t.Parallel()

	wf := compile(t, gated)
	dev := id("dev@example.com")

	tests := []struct {
		name    string
		gate    policycheck.Gate
		subject policycheck.Subject
		admits  bool
		reason  string
	}{
		{"the named approver, started by somebody else", signal("approve"),
			policycheck.Subject{Sender: id("ops@example.com"), Starter: dev}, true, ""},
		{"the starter approving their own run", signal("approve"),
			policycheck.Subject{Sender: id("ops@example.com"), Starter: id("ops@example.com")}, false, notSatisfied},
		{"somebody else entirely", signal("approve"),
			policycheck.Subject{Sender: id("mallory@example.com"), Starter: dev}, false, notSatisfied},
		{"an unknown starter fails closed for a predicate reading run.identity", signal("approve"),
			policycheck.Subject{Sender: id("ops@example.com")}, false, errored},
		{"an unauthenticated sender", signal("approve"),
			policycheck.Subject{Starter: dev}, false, notSatisfied},
		{"a starter known to be nobody does not refuse", signal("approve"),
			policycheck.Subject{Sender: id("ops@example.com"), Starter: &flowtest.ScriptedIdentity{}}, true, ""},
		{"an input narrows the approver", signal("approve"),
			policycheck.Subject{
				Sender: id("ops@example.com"), Starter: dev,
				Inputs: map[string]*v1.Value{"approver": v1.NewLiteral("someone-else@example.com")},
			}, false, notSatisfied},
		{"a predicate that errors refuses (missing claim)", signal("crash"),
			policycheck.Subject{Sender: id("ops@example.com")}, false, errored},
		{"the claim present admits", signal("crash"),
			policycheck.Subject{Sender: id("ops@example.com", "team=sre")}, true, ""},
		{"the claim present with another value refuses, not errors", signal("crash"),
			policycheck.Subject{Sender: id("ops@example.com", "team=dev")}, false, notSatisfied},
		{"debug admits its team", debugGate,
			policycheck.Subject{Sender: id("ops@example.com", "team=sre")}, true, ""},
		{"debug refuses the others", debugGate,
			policycheck.Subject{Sender: id("ops@example.com", "team=dev")}, false, "debug policy"},
		{"manual refuses an anonymous caller", manualGate,
			policycheck.Subject{Reason: "because"}, false, "anonymous start is refused"},
		{"manual refuses an issuer-less caller", manualGate,
			policycheck.Subject{Sender: &flowtest.ScriptedIdentity{Claims: map[string]string{"team": "dev"}}, Reason: "because"},
			false, "anonymous start is refused"},
		{"manual admits a dev with a reason", manualGate,
			policycheck.Subject{Sender: id("dev@example.com", "team=dev"), Reason: "because"}, true, ""},
		{"manual requires its reason", manualGate,
			policycheck.Subject{Sender: id("dev@example.com", "team=dev")}, false, "requires a reason"},
		{"manual refuses another team", manualGate,
			policycheck.Subject{Sender: id("dev@example.com", "team=sre"), Reason: "because"}, false, "refuses this manual start"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := decide(t, wf, tt.gate, tt.subject)

			require.Equal(t, tt.admits, got.Admitted, "reason: %s", got.Reason)
			require.Equal(t, !tt.admits, got.Reason != "", "a refusal carries the engine's sentence, an admission none")
			require.Contains(t, got.Reason, tt.reason)
		})
	}
}

func TestAGateNothingGovernsIsOpenAndSaysSo(t *testing.T) {
	t.Parallel()

	wf := compile(t, gated)

	open := decide(t, wf, signal("note"), policycheck.Subject{})
	require.True(t, open.Admitted, "a signal no policy governs admits any sender, as the server does")
	require.Contains(t, open.Note, "no `signals:` policy")

	bare := compile(t, `
edition: v2026.4
name: bare
steps:
  - id: a
    log: {message: hi}
`)

	manual := decide(t, bare, manualGate, policycheck.Subject{})
	require.True(t, manual.Admitted)
	require.Contains(t, manual.Note, "no `triggers.manual` block")

	debug := decide(t, bare, debugGate, policycheck.Subject{Sender: id("ops@example.com", "team=sre")})
	require.False(t, debug.Admitted, "a workflow with no `debug:` is not debuggable by anyone")
	require.Contains(t, debug.Reason, "declares no `debug:` policy")
	require.Empty(t, debug.Note)
}

func TestManualDeniedRefusesEveryone(t *testing.T) {
	t.Parallel()

	wf := compile(t, `
edition: v2026.4
name: hook-only
triggers:
  - webhook: provider
    verify:
      hmac_sha256: ${secret('env:PROVIDER_SECRET')}
    idempotency_key: ${event.body.id}
  - manual: denied
steps:
  - id: a
    log: {message: hi}
`)

	got := decide(t, wf, manualGate, policycheck.Subject{Sender: id("dev@example.com", "team=dev"), Reason: "x"})
	require.False(t, got.Admitted)
	require.Contains(t, got.Reason, "manual: denied")
}

// A refusal is the engine's fixed sentence. Nothing a sender or an input holds
// may appear in it, on any of the three ways a predicate refuses (says no,
// errors, reads a sensitive input).
func TestRefusalsNeverQuoteAValue(t *testing.T) {
	t.Parallel()

	wf := compile(t, gated)

	const (
		claimValue = "claim-secret-71ab"
		inputValue = "input-secret-9f2c" // the default of the sensitive `pin` input
	)

	subjects := map[policycheck.Gate]policycheck.Subject{
		signal("pin"):     {Sender: id("ops@example.com", "pin="+claimValue)},
		signal("crash"):   {Sender: id("ops@example.com", "pin="+claimValue, "team="+claimValue)},
		signal("approve"): {Sender: id("ops@example.com", "pin="+claimValue), Inputs: map[string]*v1.Value{"approver": v1.NewLiteral(claimValue)}},
		debugGate:         {Sender: id("ops@example.com", "team="+claimValue, "pin="+inputValue)},
		manualGate:        {Sender: id("ops@example.com", "team="+claimValue), Reason: claimValue},
	}

	for gate, subject := range subjects {
		got := decide(t, wf, gate, subject)

		require.False(t, got.Admitted, "%s", gate)
		require.NotContains(t, got.Reason, claimValue, "%s quoted a claim", gate)
		require.NotContains(t, got.Reason, inputValue, "%s quoted an input", gate)
		require.NotContains(t, got.Reason, "ops@example.com", "%s quoted the sender", gate)
	}
}

func TestEvaluateRefusesToAnswerAQuestionItCannotPut(t *testing.T) {
	t.Parallel()

	wf := compile(t, gated)

	_, err := policycheck.Evaluate(t.Context(), wf, []policycheck.Gate{signal("crash")},
		policycheck.Subject{Sender: &flowtest.ScriptedIdentity{Subject: "ops@example.com"}})
	require.ErrorContains(t, err, "names a subject or an issuer without the other",
		"a half-written identity is refused as itself, not read as a sender no policy admits")

	_, err = policycheck.Evaluate(t.Context(), wf, []policycheck.Gate{signal("crash")},
		policycheck.Subject{Sender: id("ops@example.com"), Starter: &flowtest.ScriptedIdentity{Issuer: issuer}})
	require.ErrorContains(t, err, "the starter")

	required := compile(t, `
edition: v2026.4
name: needs
inputs:
  region:
    type: string
    required: true
signals:
  go:
    allow: ${inputs.region == "eu" && sender.identity.claims.team == "ops"}
steps:
  - id: a
    wait_for_signal: {name: go, timeout: 1h}
`)

	_, err = policycheck.Evaluate(t.Context(), required, []policycheck.Gate{signal("go")},
		policycheck.Subject{Sender: id("ops@example.com")})
	require.ErrorContains(t, err, "region", "an argument the workflow requires is not an answer to guess at")

	got := decide(t, required, signal("go"), policycheck.Subject{
		Sender: id("ops@example.com", "team=ops"), Inputs: map[string]*v1.Value{"region": v1.NewLiteral("eu")}})
	require.True(t, got.Admitted, got.Reason)
}

func TestGates(t *testing.T) {
	t.Parallel()

	wf := compile(t, gated)

	tests := []struct {
		name    string
		signals []string
		debug   bool
		manual  bool
		want    []string
		wantErr string
	}{
		{name: "default is every declared signal", want: []string{"signals.approve", "signals.crash", "signals.note", "signals.pin"}},
		{name: "naming a signal checks only it", signals: []string{"pin"}, want: []string{"signals.pin"}},
		{name: "debug alone is debug alone", debug: true, want: []string{"debug"}},
		{name: "all three, ordered", signals: []string{"pin", "approve", "pin"}, debug: true, manual: true,
			want: []string{"signals.approve", "signals.pin", "debug", "triggers.manual"}},
		{name: "a typo is refused by name", signals: []string{"aprove"}, wantErr: `declares no signal "aprove"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gates, err := policycheck.Gates(wf, tt.signals, tt.debug, tt.manual)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.ErrorContains(t, err, "approve", "the refusal lists what does exist")
				return
			}
			require.NoError(t, err)

			got := make([]string, 0, len(gates))
			for _, gate := range gates {
				got = append(got, gate.String())
			}
			require.Equal(t, tt.want, got)
		})
	}

	none := compile(t, "edition: v2026.4\nname: none\nsteps:\n  - id: a\n    log: {message: hi}\n")
	_, err := policycheck.Gates(none, nil, false, false)
	require.ErrorContains(t, err, "declares no signals")

	gates, err := policycheck.Gates(none, nil, true, false)
	require.NoError(t, err)
	require.Len(t, gates, 1)
}
