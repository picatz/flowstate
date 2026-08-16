package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// #295: `flow run local --as-*` rehearsed the secret rules and the plugin
// caller and silently no-opped on the task-shape and egress policies, which
// read the run's scope rather than its task runtime. The direction is what made
// it corrosive: a rule keyed on `identity.namespace` matched nothing locally and
// matched in production, so the rehearsal *denied* what production permits and
// taught an author their correct rule was wrong.
//
// The unit-level agreement between the drivers is pinned by the shared cases
// ([conformance.TaskPolicyCases], [conformance.EgressIdentityCases]) with a caller on each.
// What those cannot see is the flag: they set the identity through the seam
// directly, and a `--as-namespace` that never reached that seam would leave them
// green. This is the path from a command line, which is what CLAUDE.md's "a
// capability is not done until it is reachable" asks for on this surface.

// writeTaskPolicy writes a task-shape policy file and returns its path.
func writeTaskPolicy(t *testing.T, body string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "tasks.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))

	// [v1.SetDefaultTaskPolicy] is process-wide — a worker installs one policy
	// and keeps it — so a test that installs one through the real command has
	// to put it back, or every later test in this package runs under it. The
	// first version of this file did not, and the next test in the package
	// failed with an http dispatch refused by a policy it had never heard of.
	t.Cleanup(func() { v1.SetDefaultTaskPolicy(nil) })

	return path
}

// tenantScopedPolicy admits the `log` task for one tenant and nobody else. An
// allowlist rather than a deny rule because that is the shape #295 reports: an
// allow rule that matches nothing refuses, which is the fail-closed reading and
// exactly what a local run used to do to every step.
const tenantScopedPolicy = `
allow:
  - 'task == "log" && identity.namespace == "team-a"'
`

// A workflow with no condition of its own, so the policy is the only thing that
// can decide whether the step runs.
const rehearsalIdentityWorkflow = `edition: v2026.3
name: rehearsal-identity
steps:
  - id: report
    log:
      message: the policy permitted this
`

func TestRunLocalRehearsesTaskPolicyAsTheNamedTenant(t *testing.T) {
	policy := writeTaskPolicy(t, tenantScopedPolicy)

	_, _, err := runLocal(t, rehearsalIdentityWorkflow,
		"--task-policy", policy, "--as-namespace", "team-a")

	require.NoError(t, err,
		"a rehearsal as the tenant the policy admits must run the step production runs")
}

// The negative direction, which is what makes the case above a test of a
// boundary rather than of a policy that says yes to everything: the same file,
// the same policy, a different tenant, refused.
func TestRunLocalRefusesTaskPolicyAsAnotherTenant(t *testing.T) {
	policy := writeTaskPolicy(t, tenantScopedPolicy)

	_, _, err := runLocal(t, rehearsalIdentityWorkflow,
		"--task-policy", policy, "--as-namespace", "team-b")

	require.Error(t, err, "the policy admits only team-a, and this rehearsal is team-b")
	assert.Contains(t, err.Error(), "task-shape policy",
		"the refusal must read as a deployment refusal rather than a mistake in the file")
}

// And the run that names no tenant at all, which is the case the fix must not
// turn into a blanket exemption: `--as-namespace` unset is an empty namespace,
// an identity-scoped allowlist matches nothing, and the dispatch is refused —
// the same answer production gives a run whose caller has no namespace.
func TestRunLocalRefusesTaskPolicyWhenNoTenantIsNamed(t *testing.T) {
	policy := writeTaskPolicy(t, tenantScopedPolicy)

	_, _, err := runLocal(t, rehearsalIdentityWorkflow, "--task-policy", policy)

	require.Error(t, err, "an identity-scoped allowlist must refuse a run that names no tenant")
	assert.Contains(t, err.Error(), "task-shape policy")
}

// The expression surface, and the honesty property alongside it: `run.identity`
// reports the identity the flags named — the same one the secret rules and the
// task-shape policy see, since there is one identity and not two — while
// `run.local` still reads true, because naming an identity is not being attested
// as one. Nothing on the command line can turn `local` off; the local driver
// sets it itself.
func TestRunLocalReportsTheRehearsalIdentityAndStaysLocal(t *testing.T) {
	const workflow = `edition: v2026.3
name: rehearsal-identity-outputs

outputs:
  local:
    value: ${run.local}
  subject:
    value: ${run.identity.subject}
  namespace:
    value: ${run.identity.namespace}

steps:
  - id: report
    log:
      message: reporting
`

	stdout, _, err := runLocal(t, workflow, "-o", "json",
		"--as-subject", "release-requester@example.com", "--as-namespace", "team-a")
	require.NoError(t, err)

	var response struct {
		RunOutputs map[string]any `json:"runOutputs"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &response))

	values := response.RunOutputs
	assert.Equal(t, "release-requester@example.com", values["subject"],
		"run.identity must report the identity --as-subject named")
	assert.Equal(t, "team-a", values["namespace"],
		"run.identity must report the tenant --as-namespace named")
	assert.Equal(t, true, values["local"],
		"a rehearsal must still say it is one: naming an identity is not being attested as one")
}
