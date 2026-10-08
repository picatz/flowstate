package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const claimCheckPolicy = `
issuers:
  - name: keycloak
    issuer: https://idp.example.com/realms/acme
    audiences: [flowstate]
    actions: []
    carry_claims:
      - {claim: team, type: string}
    groups_claim: realm_access.roles
secrets:
  allow:
    - '"sre" in identity.claims.groups'
    - 'identity.claims.cost_center == "x"'
`

const claimCheckFlowfile = `edition: v2026.4
name: claim-check
signals:
  approve:
    allow: ${sender.identity.claims.team == "ops" && "sre" in sender.identity.claims.groups && sender.identity.claims.region == "eu"}
debug:
  allow: ${sender.identity.claims.squad == "x"}
steps:
  - id: gate
    wait_for_signal:
      name: approve
      timeout: 1h
`

// TestValidateChecksIdentityReadsAgainstTheAuthPolicy is `flow validate
// --auth-policy`: every claim an identity expression reads must be one an issuer
// entry carries, and one it does not is a diagnostic naming where it is read and
// what the policy does carry.
func TestValidateChecksIdentityReadsAgainstTheAuthPolicy(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	flowfile := filepath.Join(dir, "workflow.yaml")
	policy := filepath.Join(dir, "auth.yaml")
	require.NoError(t, os.WriteFile(flowfile, []byte(claimCheckFlowfile), 0o600))
	require.NoError(t, os.WriteFile(policy, []byte(claimCheckPolicy), 0o600))

	t.Run("without the flag nothing is checked", func(t *testing.T) {
		t.Parallel()

		res := runFlow(t, "validate", flowfile)
		require.NoError(t, res.Err, res.Output())
		require.Contains(t, res.Stdout, ": ok")
	})

	t.Run("a claim no entry carries is a diagnostic", func(t *testing.T) {
		t.Parallel()

		res := runFlow(t, "validate", "--auth-policy", policy, flowfile)
		require.Error(t, res.Err)
		out := res.Output()
		require.Contains(t, out, `signals.approve.allow reads the caller's claim "region"`)
		require.Contains(t, out, `debug.allow reads the caller's claim "squad"`)
		require.Contains(t, out, "it carries groups, team", "the diagnostic names what the policy does carry")
		require.NotContains(t, out, `claim "team"`, "a carried claim is not reported")
		require.NotContains(t, out, `claim "groups"`, "groups_claim carries groups")
	})

	t.Run("the policy's own secret rules are checked against its entries", func(t *testing.T) {
		t.Parallel()

		res := runFlow(t, "validate", "--auth-policy", policy, flowfile)
		require.Error(t, res.Err)
		require.Contains(t, res.Output(), `secrets.allow[1] reads the caller's claim "cost_center"`)
	})

	t.Run("a machine report carries the same diagnostics", func(t *testing.T) {
		t.Parallel()

		res := runFlow(t, "validate", "--auth-policy", policy, flowfile, "-o", "jsonl")
		require.Error(t, res.Err)
		require.Contains(t, res.Stdout, `claim \"region\"`)
	})

	t.Run("a policy that does not load fails the command", func(t *testing.T) {
		t.Parallel()

		bad := filepath.Join(dir, "bad.yaml")
		require.NoError(t, os.WriteFile(bad, []byte("issuers:\n  - name: x\n    issuer: https://idp.example.com\n    audiences: [a]\n    actions: []\n    carry_claims: [{claim: team}]\n"), 0o600))
		res := runFlow(t, "validate", "--auth-policy", bad, flowfile)
		require.Error(t, res.Err)
		require.Contains(t, res.Output(), "carry_claims[0]")
	})

	t.Run("every claim carried is clean", func(t *testing.T) {
		t.Parallel()

		clean := filepath.Join(dir, "clean.yaml")
		require.NoError(t, os.WriteFile(clean, []byte("name: clean\nedition: v2026.4\nsignals:\n  approve:\n    allow: ${sender.identity.claims.team == \"ops\" && \"sre\" in sender.identity.claims.groups}\nsteps:\n  - id: gate\n    wait_for_signal:\n      name: approve\n      timeout: 1h\n"), 0o600))
		cleanPolicy := filepath.Join(dir, "clean-auth.yaml")
		require.NoError(t, os.WriteFile(cleanPolicy, []byte(claimCheckPolicy[:strings.Index(claimCheckPolicy, "secrets:")]), 0o600))
		res := runFlow(t, "validate", "--auth-policy", cleanPolicy, clean)
		require.NoError(t, res.Err, res.Output())
	})
}

// TestValidateChecksActorReadsAgainstTheAuthPolicy is the delegation half of the
// cross-check: a rule that reads `actors` or `delegated` can only ever see a
// delegated caller where some issuer entry has a `delegation:` stanza, and is a
// diagnostic where none has.
func TestValidateChecksActorReadsAgainstTheAuthPolicy(t *testing.T) {
	t.Parallel()

	const flowfile = `edition: v2026.4
name: actor-check
signals:
  approve:
    allow: ${!sender.identity.delegated || sender.identity.actors[0].subject == "triage-bot"}
steps:
  - id: gate
    wait_for_signal:
      name: approve
      timeout: 1h
`
	const entry = `
issuers:
  - name: agents
    issuer: https://idp.example.com
    audiences: [flowstate]
    actions: [workload.read]
`
	const stanza = `    delegation:
      actors:
        - {issuer: https://agents.example, subject: triage-bot, actions: [workload.read]}
`
	const policyRule = `secrets:
  allow:
    - 'identity.delegated'
`

	dir := t.TempDir()
	workflow := filepath.Join(dir, "workflow.yaml")
	without := filepath.Join(dir, "without.yaml")
	with := filepath.Join(dir, "with.yaml")
	require.NoError(t, os.WriteFile(workflow, []byte(flowfile), 0o600))
	require.NoError(t, os.WriteFile(without, []byte(entry+policyRule), 0o600))
	require.NoError(t, os.WriteFile(with, []byte(entry+stanza+policyRule), 0o600))

	res := runFlow(t, "validate", "--auth-policy", without, workflow)
	require.Error(t, res.Err)
	out := res.Output()
	require.Contains(t, out, "signals.approve.allow reads the caller's `actors` or `delegated`")
	require.Contains(t, out, "no issuer entry in the auth policy has a `delegation:` stanza")
	require.Contains(t, out, "secrets.allow[0] reads the caller's `actors` or `delegated`", "the policy's own rules are read the same way")

	res = runFlow(t, "validate", "--auth-policy", with, workflow)
	require.NoError(t, res.Err, res.Output())
	require.NotContains(t, res.Output(), "delegation:")
}
