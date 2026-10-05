package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"

	flowmcp "github.com/picatz/flowstate/cmd/flow/internal/mcp"
	"github.com/picatz/flowstate/cmd/flow/internal/policycheck"
)

// callCheckPolicy calls flowstate_check_policy and returns the raw result and
// its text.
func callCheckPolicy(t *testing.T, session *mcp.ClientSession, args map[string]any) (*mcp.CallToolResult, string) {
	t.Helper()

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: flowmcp.CheckPolicyToolName, Arguments: args})
	require.NoError(t, err)
	require.NotEmpty(t, result.Content)

	return result, result.Content[0].(*mcp.TextContent).Text
}

// checkPolicyAnswer decodes the report the tool answers with.
func checkPolicyAnswer(t *testing.T, result *mcp.CallToolResult, text string) policycheck.Report {
	t.Helper()

	require.False(t, result.IsError, "the call failed: %s", text)

	var report policycheck.Report
	require.NoError(t, json.Unmarshal([]byte(text), &report), text)

	return report
}

func approvalGateSource(t *testing.T) string {
	t.Helper()

	source, err := os.ReadFile(gateWorkflow)
	require.NoError(t, err)

	return string(source)
}

func approvalGateArgs(t *testing.T) map[string]any {
	t.Helper()

	return map[string]any{
		"source": approvalGateSource(t),
		"gate":   "signal",
		"signal": "deploy-approved",
		"inputs": map[string]any{"version": "v1.4.2", "environment": "production", "expected_approver": "sre-lead@example.com"},
		"sender": map[string]any{
			"subject": "sre-lead@example.com", "issuer": gateIssuer,
			"claims": map[string]any{"team": "release-managers"},
		},
		"starter": map[string]any{"subject": "dev@example.com", "issuer": gateIssuer},
	}
}

func TestCheckPolicyToolIsRegisteredAndBounded(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	tools, err := session.ListTools(t.Context(), nil)
	require.NoError(t, err)

	var found bool
	for _, tool := range tools.Tools {
		if tool.Name != flowmcp.CheckPolicyToolName {
			continue
		}
		found = true

		schema, err := json.Marshal(tool.InputSchema)
		require.NoError(t, err)
		require.Contains(t, string(schema), `"additionalProperties":false`)
		require.Contains(t, string(schema), `"required":["source","gate"]`)
	}

	require.True(t, found, "flowstate_check_policy is not advertised")
}

func TestCheckPolicyToolAdmitsTheApprover(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	result, text := callCheckPolicy(t, session, approvalGateArgs(t))
	report := checkPolicyAnswer(t, result, text)

	require.Equal(t, []string{"signals.deploy-approved"}, report.Gates)
	require.Len(t, report.Results, 1)
	require.Len(t, report.Results[0].Decisions, 1)
	require.Equal(t, policycheck.OutcomeAdmitted, report.Results[0].Decisions[0].Outcome, text)
	require.Empty(t, report.Results[0].Decisions[0].Reason)
}

// A refusal is an answer, not a tool error, and it is the engine's own fixed
// sentence.
func TestCheckPolicyToolRefusesWithTheEnginesSentence(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	t.Run("the requester approving their own request", func(t *testing.T) {
		t.Parallel()

		args := approvalGateArgs(t)
		args["starter"] = args["sender"]

		result, text := callCheckPolicy(t, session, args)
		decision := checkPolicyAnswer(t, result, text).Results[0].Decisions[0]

		require.Equal(t, policycheck.OutcomeRefused, decision.Outcome, text)
		require.Contains(t, decision.Reason, "does not satisfy")
		require.NotContains(t, text, "sre-lead@example.com", "a refusal quoted the sender")
	})

	t.Run("a sender with the wrong claim", func(t *testing.T) {
		t.Parallel()

		args := approvalGateArgs(t)
		args["sender"] = map[string]any{
			"subject": "sre-lead@example.com", "issuer": gateIssuer,
			"claims": map[string]any{"team": "interns"},
		}

		result, text := callCheckPolicy(t, session, args)
		require.Equal(t, policycheck.OutcomeRefused, checkPolicyAnswer(t, result, text).Results[0].Decisions[0].Outcome, text)
	})

	t.Run("no sender is an unauthenticated caller", func(t *testing.T) {
		t.Parallel()

		args := approvalGateArgs(t)
		delete(args, "sender")

		result, text := callCheckPolicy(t, session, args)
		require.Equal(t, policycheck.OutcomeRefused, checkPolicyAnswer(t, result, text).Results[0].Decisions[0].Outcome, text)

		// An empty sender object is the same caller, not an authenticated one
		// with empty fields.
		args["sender"] = map[string]any{}

		result, text = callCheckPolicy(t, session, args)
		require.Equal(t, policycheck.OutcomeRefused, checkPolicyAnswer(t, result, text).Results[0].Decisions[0].Outcome, text)
	})

	t.Run("the debug gate", func(t *testing.T) {
		t.Parallel()

		args := approvalGateArgs(t)
		args["gate"] = "debug"
		delete(args, "signal")

		result, text := callCheckPolicy(t, session, args)
		report := checkPolicyAnswer(t, result, text)

		require.Equal(t, []string{"debug"}, report.Gates)
		require.Equal(t, policycheck.OutcomeRefused, report.Results[0].Decisions[0].Outcome, text)
		require.Contains(t, report.Results[0].Decisions[0].Reason, "debug policy")

		args["sender"] = map[string]any{"subject": "s@example.com", "issuer": gateIssuer, "claims": map[string]any{"team": "sre"}}

		result, text = callCheckPolicy(t, session, args)
		require.Equal(t, policycheck.OutcomeAdmitted, checkPolicyAnswer(t, result, text).Results[0].Decisions[0].Outcome, text)
	})

	t.Run("the manual gate with no block says so", func(t *testing.T) {
		t.Parallel()

		args := approvalGateArgs(t)
		args["gate"] = "manual"
		delete(args, "signal")

		result, text := callCheckPolicy(t, session, args)
		decision := checkPolicyAnswer(t, result, text).Results[0].Decisions[0]

		require.Equal(t, policycheck.OutcomeAdmitted, decision.Outcome, text)
		require.Contains(t, decision.Note, "no `triggers.manual` block")
	})
}

// A starter nobody named is unknown, and a predicate that reads `run.identity`
// refuses it: the engine's fail-closed reading, not an admission.
func TestCheckPolicyToolFailsClosedOnAnUnknownStarter(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	args := approvalGateArgs(t)
	delete(args, "starter")

	result, text := callCheckPolicy(t, session, args)
	decision := checkPolicyAnswer(t, result, text).Results[0].Decisions[0]
	require.Equal(t, policycheck.OutcomeRefused, decision.Outcome,
		"the approver was admitted with the starter unknown: %s", text)

	// Known to be nobody authenticated, the same sender is admitted: the two
	// are different facts and the tool tells them apart.
	args["starter"] = map[string]any{}

	result, text = callCheckPolicy(t, session, args)
	require.Equal(t, policycheck.OutcomeAdmitted, checkPolicyAnswer(t, result, text).Results[0].Decisions[0].Outcome, text)
}

// Every way the question cannot be put is a tool error with no verdict in it.
func TestCheckPolicyToolFailsClosedWhenTheQuestionCannotBePut(t *testing.T) {
	t.Parallel()

	session := connectMCP(t, defaultLocalRunPosture())

	with := func(t *testing.T, edit func(map[string]any)) map[string]any {
		args := approvalGateArgs(t)
		edit(args)

		return args
	}

	tests := []struct {
		name string
		args map[string]any
		want string
	}{
		{"a source that does not compile",
			map[string]any{"source": "edition: v2026.4\nname: x\nsteps:\n  - id: a\n    nope:\n      x: y\n", "gate": "debug"},
			"has problems"},
		{"a source that is not YAML", map[string]any{"source": "{{{", "gate": "debug"}, "not a valid Flowfile"},
		{"no source", map[string]any{"gate": "debug"}, "source is required"},
		{"no gate", with(t, func(a map[string]any) { delete(a, "gate") }), "gate is required"},
		{"an unknown gate", with(t, func(a map[string]any) { a["gate"] = "all" }), "gate is required"},
		{"an unknown argument", with(t, func(a map[string]any) { a["senders"] = []any{} }), "do not match"},
		{"an unknown signal", with(t, func(a map[string]any) { a["signal"] = "nope" }), `declares no signal "nope"`},
		{"a signal with another gate", with(t, func(a map[string]any) { a["gate"] = "debug" }), "only gate"},
		{"a reason with another gate", with(t, func(a map[string]any) { a["reason"] = "why" }), "reason is read only"},
		{"half an identity", with(t, func(a map[string]any) { a["sender"] = map[string]any{"subject": "a@example.com"} }), "subject or an issuer without the other"},
		{"an undeclared input", with(t, func(a map[string]any) { a["inputs"] = map[string]any{"nope": 1} }), "nope"},
		{"too many claims", with(t, func(a map[string]any) {
			claims := map[string]any{}
			for i := range maxCheckPolicyClaims + 1 {
				claims[strings.Repeat("c", i+1)] = "v"
			}
			a["sender"] = map[string]any{"claims": claims}
		}), "over the limit"},
		{"an oversized subject", with(t, func(a map[string]any) {
			a["sender"] = map[string]any{"subject": strings.Repeat("s", maxCheckPolicyIdentityField+1), "issuer": gateIssuer}
		}), "byte limit"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, text := callCheckPolicy(t, session, tt.args)
			require.True(t, result.IsError, "the question could not be put but the call answered: %s", text)
			require.Contains(t, text, tt.want)
			require.NotContains(t, text, `"outcome"`, "an error carried a verdict")
		})
	}
}

// Nothing a caller supplied is written back: not in an answer, and not in the
// refusal of an argument, `sensitive:` or not.
func TestCheckPolicyToolNeverEchoesAValue(t *testing.T) {
	t.Parallel()

	const (
		claimValue = "claim-secret-71ab"
		pinValue   = "pin-secret-3d9e"
		subject    = "ops-subject-5c2f"
	)

	source := `
edition: v2026.4
name: secret-gated
inputs:
  pin:
    type: string
    sensitive: true
    default: ` + pinValue + `
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
`

	session := connectMCP(t, defaultLocalRunPosture())

	call := func(inputs map[string]any) (*mcp.CallToolResult, string) {
		args := map[string]any{
			"source": source,
			"gate":   "signal",
			"sender": map[string]any{
				"subject": subject, "issuer": gateIssuer,
				"claims": map[string]any{"pin": claimValue, "team": claimValue},
			},
		}
		if inputs != nil {
			args["inputs"] = inputs
		}

		return callCheckPolicy(t, session, args)
	}

	forbidden := []string{claimValue, pinValue, subject, "9876501234", "short-secret-77", "a-long-enough-default-code-value", "1e999"}

	for name, inputs := range map[string]map[string]any{
		"a refusal":                 nil,
		"a refusal given a pin":     {"pin": pinValue},
		"a pin that is not text":    {"pin": 9876501234},
		"an int that is not an int": {"attempts": "9876501234x"},
		"an int too large":          {"attempts": json.Number("1e999")},
		"a must violation":          {"code": "short-secret-77"},
	} {
		t.Run(name, func(t *testing.T) {
			result, text := call(inputs)
			for _, word := range forbidden {
				require.NotContains(t, text, word, "%s echoed %q (error=%v):\n%s", name, word, result.IsError, text)
			}
		})
	}

	// The control: the first case is a real, successful answer, so the
	// assertions above were made over a decision and not only over errors.
	result, text := call(nil)
	require.Equal(t, policycheck.OutcomeRefused, checkPolicyAnswer(t, result, text).Results[0].Decisions[0].Outcome, text)

	// And a refusal of a sensitive value is still a refusal that says which
	// input, so redaction does not blind the author to what to fix.
	result, text = call(map[string]any{"code": "short-secret-77"})
	require.True(t, result.IsError, text)
	require.Contains(t, text, "code")
}
