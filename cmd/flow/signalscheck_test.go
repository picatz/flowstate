package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/policycheck"
)

const (
	gateWorkflow = "../../examples/approval-gate/workflow.yaml"
	gateInputs   = "../../examples/approval-gate/inputs.json"
	gateIssuer   = "https://issuer.example.com"
)

// approverFlags names the approver examples/approval-gate admits, and a starter
// who is somebody else.
func approverFlags() []string {
	return []string{
		"--signal-as-subject", "sre-lead@example.com", "--signal-as-issuer", gateIssuer,
		"--signal-as-claim", "team=release-managers",
		"--starter-subject", "dev@example.com", "--starter-issuer", gateIssuer,
	}
}

func checkGate(t *testing.T, extra ...string) flowResult {
	t.Helper()

	return runFlow(t, append([]string{"signals", "check", gateWorkflow, "--input-file", gateInputs}, extra...)...)
}

func writeFile(t *testing.T, name, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	return path
}

func TestSignalsCheckAnswersOneLinePerGate(t *testing.T) {
	t.Parallel()

	res := checkGate(t, append(approverFlags(), "--debug", "--signal", "deploy-approved")...)
	require.NoError(t, res.Err, res.Output())
	require.Zero(t, res.ExitCode)

	lines := strings.Split(strings.TrimSpace(res.Stdout), "\n")
	require.Len(t, lines, 2, res.Stdout)
	require.Regexp(t, `^signals\.deploy-approved\s+admitted$`, lines[0])
	require.Regexp(t, `^debug\s+refused: the sender does not satisfy this debug policy's allow predicate$`, lines[1],
		"a refusal is the engine's own sentence")
}

func TestSignalsCheckDefaultsToEverySignalAndExitsZeroWithoutAnExpectation(t *testing.T) {
	t.Parallel()

	// An unauthenticated sender: refused, and still exit 0, because nothing
	// asserted otherwise.
	res := checkGate(t, "--starter-subject", "dev@example.com", "--starter-issuer", gateIssuer)
	require.NoError(t, res.Err, res.Output())
	require.Zero(t, res.ExitCode)
	require.Regexp(t, `signals\.deploy-approved\s+refused`, res.Stdout)
}

func TestSignalsCheckExpectIsTheExitStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		expect string
		extra  []string
		exit   int
	}{
		{"admitted and expected", "admitted", approverFlags(), 0},
		{"admitted but refusal expected", "refused", approverFlags(), 1},
		{"refused and expected", "refused", nil, 0},
		{"refused but admission expected", "admitted", nil, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res := checkGate(t, append([]string{"--expect", tt.expect}, tt.extra...)...)
			require.Equal(t, tt.exit, res.ExitCode, res.Output())

			if tt.exit == 1 {
				require.Contains(t, res.Stdout, "(expected "+tt.expect+")",
					"the answers are printed before the failing status, and the contradicted one is marked")
				require.NotContains(t, res.Stderr, "did not match",
					"the report was already given; the exit status is the rest of it")
			}
		})
	}

	res := checkGate(t, "--expect", "allowed")
	require.Equal(t, exitCodeUsage, res.ExitCode, "an expectation that is not one of two words is a usage error")
}

// The engine's own fail-closed readings, reached through the command.
func TestSignalsCheckFailsClosedAsTheEngineDoes(t *testing.T) {
	t.Parallel()

	approver := approverFlags()[:6]
	unknownStarter := checkGate(t, approver...)
	require.Regexp(t, `signals\.deploy-approved\s+refused: .*could not be evaluated`, unknownStarter.Stdout,
		"an unknown starter makes a predicate reading run.identity error, which refuses")

	nobody := checkGate(t, append(slices.Clone(approver), "--starter-anonymous")...)
	require.Regexp(t, `signals\.deploy-approved\s+admitted`, nobody.Stdout,
		"a run known to be started by nobody is a fact, not a gap in the record")

	self := checkGate(t, append(slices.Clone(approver),
		"--starter-subject", "sre-lead@example.com", "--starter-issuer", gateIssuer)...)
	require.Regexp(t, `signals\.deploy-approved\s+refused: the sender does not satisfy`, self.Stdout,
		"the starter may not approve their own run")

	noClaims := checkGate(t, "--signal-as-subject", "sre-lead@example.com", "--signal-as-issuer", gateIssuer,
		"--starter-subject", "dev@example.com", "--starter-issuer", gateIssuer)
	require.Regexp(t, `refused: .*could not be evaluated`, noClaims.Stdout,
		"a predicate that errors (the claim is missing) refuses rather than admits")
}

func TestSignalsCheckManual(t *testing.T) {
	t.Parallel()

	path := writeFile(t, "manual.yaml", `
edition: v2026.4
name: manual-gated
triggers:
  - manual:
      allow: ${sender.identity.claims.team == "dev"}
steps:
  - id: a
    log:
      message: hi
`)

	check := func(extra ...string) flowResult {
		return runFlow(t, append([]string{"signals", "check", path, "--manual"}, extra...)...)
	}

	anonymous := check("--expect", "refused")
	require.Zero(t, anonymous.ExitCode, anonymous.Output())
	require.Contains(t, anonymous.Stdout, "triggers.manual")
	require.Contains(t, anonymous.Stdout, "anonymous start is refused",
		"manual refuses a caller with no authenticated principal, whatever the predicate says")

	// The same claim, without an issuer-qualified principal, is still anonymous.
	claimOnly := check("--signal-as-claim", "team=dev", "--expect", "refused")
	require.Zero(t, claimOnly.ExitCode, claimOnly.Output())

	named := check("--signal-as-subject", "dev@example.com", "--signal-as-issuer", gateIssuer,
		"--signal-as-claim", "team=dev", "--expect", "admitted")
	require.Zero(t, named.ExitCode, named.Output())

	wrongTeam := check("--signal-as-subject", "dev@example.com", "--signal-as-issuer", gateIssuer,
		"--signal-as-claim", "team=sre", "--expect", "admitted")
	require.Equal(t, 1, wrongTeam.ExitCode, wrongTeam.Output())
}

func TestSignalsCheckUsageErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		extra []string
		want  string
	}{
		{"an unknown signal", []string{"--signal", "aprove"}, `declares no signal "aprove"`},
		{"half a sender", []string{"--signal-as-subject", "a"}, "without the other"},
		{"half a starter", []string{"--starter-issuer", gateIssuer}, "without the other"},
		{"a malformed claim", []string{"--signal-as-claim", "team"}, "want NAME=VALUE"},
		{"a duplicate claim", []string{"--starter-claim", "a=b", "--starter-claim", "a=c"}, "duplicate --starter-claim"},
		{"an anonymous starter that is also named", []string{"--starter-anonymous", "--starter-subject", "a", "--starter-issuer", "b"}, "contradicts"},
		{"a matrix and its own senders", []string{"--matrix", "m.yaml", "--signal-as-claim", "a=b"}, "--matrix names its own senders"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res := checkGate(t, tt.extra...)
			require.Equal(t, exitCodeUsage, res.ExitCode, res.Output())
			require.Contains(t, res.Output(), tt.want)
			require.Empty(t, res.Stdout, "no answer is written for a question that could not be put")
		})
	}
}

func TestSignalsCheckNeedsACompilingFlowfileAndAnswerableInputs(t *testing.T) {
	t.Parallel()

	bad := writeFile(t, "bad.yaml", "edition: v2026.4\nname: bad\nsteps:\n  - id: a\n    nonsense: true\n")
	res := runFlow(t, "signals", "check", bad)
	require.Equal(t, exitCodeFailure, res.ExitCode, res.Output())
	require.Empty(t, res.Stdout)

	// A required input the invocation does not give is an error, not a guess.
	res = runFlow(t, "signals", "check", gateWorkflow, "--signal-as-subject", "a", "--signal-as-issuer", "b")
	require.Equal(t, exitCodeFailure, res.ExitCode, res.Output())
	require.Contains(t, res.Output(), "version")
	require.Empty(t, res.Stdout)
}

func TestSignalsCheckMatrix(t *testing.T) {
	t.Parallel()

	rows := func(leadExpect, selfExpect string) string {
		return `
identities:
  - name: sre-lead
    subject: sre-lead@example.com
    issuer: ` + gateIssuer + `
    claims: {team: release-managers}
    starter: {subject: dev@example.com, issuer: ` + gateIssuer + `}
    expect: ` + leadExpect + `
  - name: self-approval
    subject: sre-lead@example.com
    issuer: ` + gateIssuer + `
    claims: {team: release-managers}
    starter: {subject: sre-lead@example.com, issuer: ` + gateIssuer + `}
    expect: ` + selfExpect + `
  - name: wrong-approver-by-input
    subject: sre-lead@example.com
    issuer: ` + gateIssuer + `
    claims: {team: release-managers}
    starter: {subject: dev@example.com, issuer: ` + gateIssuer + `}
    inputs: {expected_approver: someone-else@example.com}
    expect: refused
  - name: nobody
    expect: refused
`
	}

	holds := writeFile(t, "holds.yaml", rows("admitted", "refused"))
	res := checkGate(t, "--matrix", holds)
	require.Zero(t, res.ExitCode, res.Output())

	for _, want := range []string{"identity", "signals.deploy-approved", "sre-lead", "self-approval",
		"wrong-approver-by-input", "nobody"} {
		require.Contains(t, res.Stdout, want)
	}
	require.Regexp(t, `sre-lead\s+admitted`, res.Stdout)
	require.Regexp(t, `self-approval\s+refused`, res.Stdout)
	require.Regexp(t, `wrong-approver-by-input\s+refused`, res.Stdout,
		"a row's own inputs reach the predicate")
	require.Regexp(t, `nobody\s+refused`, res.Stdout)

	broken := writeFile(t, "broken.yaml", rows("admitted", "admitted"))
	res = checkGate(t, "--matrix", broken)
	require.Equal(t, exitCodeFailure, res.ExitCode, "a row whose expectation does not hold fails the check")
	require.Regexp(t, `self-approval\s+refused \(expected admitted\)`, res.Stdout)

	// --expect is the default for a row that asserts nothing of its own.
	unasserted := writeFile(t, "unasserted.yaml", "identities:\n  - name: nobody\n")
	require.Equal(t, exitCodeFailure, checkGate(t, "--matrix", unasserted, "--expect", "admitted").ExitCode)
	require.Zero(t, checkGate(t, "--matrix", unasserted, "--expect", "refused").ExitCode)
	require.Zero(t, checkGate(t, "--matrix", unasserted).ExitCode)

	// -o json carries the same verdict.
	res = checkGate(t, "--matrix", broken, "-o", "json")
	require.Equal(t, exitCodeFailure, res.ExitCode)

	var report policycheck.Report
	require.NoError(t, json.Unmarshal([]byte(res.Stdout), &report), res.Stdout)
	require.False(t, report.Matches)
	require.Len(t, report.Results, 4)
}

func TestSignalsCheckMatrixIsBoundedAndStrict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		doc  string
		want string
	}{
		{"a half identity", "identities:\n  - name: a\n    subject: s\n", "without the other"},
		{"a misspelled key", "identities:\n  - name: a\n    expct: refused\n", "expct"},
		{"an expectation about a gate not checked", "identities:\n  - name: a\n    expect_by_gate: {debug: refused}\n", "does not decide"},
		{"an oversized file", "identities:\n  - name: a\n" + strings.Repeat("#", policycheck.MaxMatrixBytes), "limit"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res := checkGate(t, "--matrix", writeFile(t, "m.yaml", tt.doc))
			require.Equal(t, exitCodeFailure, res.ExitCode, res.Output())
			require.Contains(t, res.Output(), tt.want)
			require.Empty(t, res.Stdout)
		})
	}

	missing := checkGate(t, "--matrix", filepath.Join(t.TempDir(), "absent.yaml"))
	require.Equal(t, exitCodeFailure, missing.ExitCode)
}

// Nothing a sender, a starter or an input holds is written by this command: not
// in an answer, not in a refusal, not in an input error.
func TestSignalsCheckNeverEchoesAValue(t *testing.T) {
	t.Parallel()

	const (
		claimValue = "claim-secret-71ab"
		pinValue   = "pin-secret-3d9e"
	)

	path := writeFile(t, "secret.yaml", `
edition: v2026.4
name: secret-gated
inputs:
  pin:
    type: string
    sensitive: true
    default: `+pinValue+`
  attempts:
    type: int
    sensitive: true
    default: 3
  code:
    type: string
    sensitive: true
    default: a-long-enough-default-code-value
    must: this.size() > 20
signals:
  go:
    allow: ${sender.identity.claims.pin == "expected-pin" && sender.identity.claims.team == "ops"}
steps:
  - id: a
    wait_for_signal:
      name: go
      timeout: 1h
`)

	identity := []string{"--signal-as-subject", "ops@example.com", "--signal-as-issuer", gateIssuer,
		"--signal-as-claim", "pin=" + claimValue, "--signal-as-claim", "team=" + claimValue}

	run := func(extra ...string) flowResult {
		return runFlow(t, append([]string{"signals", "check", path, "--expect", "admitted"}, append(identity, extra...)...)...)
	}

	forbidden := []string{claimValue, pinValue, "ops@example.com", "9876501234", "short-secret-77"}

	for name, res := range map[string]flowResult{
		"a refusal":               run(),
		"a refusal given a pin":   run("--input", "pin="+pinValue),
		"a pin that is not text":  run("--input-file", writeFile(t, "in.json", `{"pin": 9876501234}`)),
		"an unparseable int":      run("--input", "attempts=9876501234x"),
		"a must violation":        run("--input", "code=short-secret-77"),
		"a must violation (file)": run("--input-file", writeFile(t, "in2.json", `{"code": "short-secret-77"}`)),
		"a matrix row":            runFlow(t, "signals", "check", path, "--matrix", writeFile(t, "m.yaml", "identities:\n  - name: ops\n    subject: ops@example.com\n    issuer: "+gateIssuer+"\n    claims: {pin: "+claimValue+", team: "+claimValue+"}\n    inputs: {pin: 9876501234}\n")),
		"a matrix row's int":      runFlow(t, "signals", "check", path, "--matrix", writeFile(t, "m2.yaml", "identities:\n  - name: ops\n    subject: ops@example.com\n    issuer: "+gateIssuer+"\n    claims: {pin: "+claimValue+"}\n    inputs: {attempts: 9876501234x}\n")),
	} {
		for _, word := range forbidden {
			require.NotContains(t, res.Output(), word, "%s echoed %q:\n%s", name, word, res.Output())
		}
	}

	refused := run()
	require.Equal(t, 1, refused.ExitCode, refused.Output())
	require.Contains(t, refused.Stdout, "refused (expected admitted)")
}

// The command's answers are what `flow run local` decides for the same
// identity, because both ask the same function: the delivery path `flow run
// local` takes ([withLocalSignals]) is run for the real approval-gate example,
// and its admission or refusal must be what the static check says.
func TestSignalsCheckAgreesWithARunLocalDelivery(t *testing.T) {
	t.Parallel()

	workflow, err := loadWorkflow(gateWorkflow)
	require.NoError(t, err)

	inputs, err := inputsFromFile(gateInputs, declaredInputs(workflow))
	require.NoError(t, err)

	type who struct {
		subject, issuer string
		claims          []string
	}

	var (
		lead     = who{"sre-lead@example.com", gateIssuer, []string{"team=release-managers"}}
		other    = who{"someone@example.com", gateIssuer, []string{"team=release-managers"}}
		wrongOrg = who{"sre-lead@example.com", "https://other.example.com", []string{"team=release-managers"}}
		noTeam   = who{"sre-lead@example.com", gateIssuer, []string{"team=interns"}}
		noClaims = who{"sre-lead@example.com", gateIssuer, nil}
	)

	cases := []struct {
		name    string
		sender  *who
		starter *who // nil: started by nobody, which is what `run local` models with no --as-*
		admits  bool
	}{
		{"the named approver", &lead, &other, true},
		{"the named approver, started by nobody", &lead, nil, true},
		{"the starter approving", &lead, &lead, false},
		{"an approver nobody named", &other, nil, false},
		{"the right subject at the wrong issuer", &wrongOrg, nil, false},
		{"the right subject in the wrong team", &noTeam, nil, false},
		{"the right subject with no team claim (a predicate error)", &noClaims, nil, false},
		{"an unattested delivery", nil, nil, false},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// `flow run local`'s delivery.
			local := localSignalsTestCommand(t)
			var check []string

			if tt.sender != nil {
				require.NoError(t, local.Flags().Set("signal-as-subject", tt.sender.subject))
				require.NoError(t, local.Flags().Set("signal-as-issuer", tt.sender.issuer))
				check = append(check, "--signal-as-subject", tt.sender.subject, "--signal-as-issuer", tt.sender.issuer)
				for _, claim := range tt.sender.claims {
					require.NoError(t, local.Flags().Set("signal-as-claim", claim))
					check = append(check, "--signal-as-claim", claim)
				}
			}

			if tt.starter != nil {
				require.NoError(t, local.Flags().Set("as-subject", tt.starter.subject))
				require.NoError(t, local.Flags().Set("as-issuer", tt.starter.issuer))
				check = append(check, "--starter-subject", tt.starter.subject, "--starter-issuer", tt.starter.issuer)
			} else {
				// `run local` always knows its starter, as nobody when no
				// --as-* flag names one.
				check = append(check, "--starter-anonymous")
			}

			_, deliveryErr := withLocalSignals(t.Context(), local, workflow, inputs, []string{`deploy-approved={"approved": true}`})
			res := checkGate(t, check...)
			require.NoError(t, res.Err, res.Output())

			require.Equal(t, tt.admits, deliveryErr == nil,
				"the local delivery disagrees with the case's own expectation: %v", deliveryErr)

			admitted := strings.Contains(res.Stdout, "admitted")
			require.Equal(t, deliveryErr == nil, admitted,
				"the static check and the local run's delivery decided differently\ncheck:\n%s\ndelivery: %v", res.Stdout, deliveryErr)

			// The refusal is the one sentence, from the one function.
			if deliveryErr != nil {
				_, sentence, _ := strings.Cut(res.Stdout, "refused: ")
				require.Contains(t, deliveryErr.Error(), strings.TrimSpace(sentence))
			}
		})
	}
}
