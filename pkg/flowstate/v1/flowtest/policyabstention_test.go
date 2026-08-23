package flowtest_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// TestACaseCannotDeclareADeploymentPolicy is the case-file half of #652
// item 2's decision, which `cmd/flow`'s TestFlowTestTakesNoDeploymentPolicyFlags
// covers at the command line.
//
// The decision is that a case's verdict must not depend on deployment
// configuration, and a flag is only one of the two doors that configuration
// could come through. The other is the case file itself: `task_policy:` or
// `egress_policy:` written beside `starter:` would put the same deployment
// configuration one level deeper, where it is not merely per-invocation but
// *committed*, and where a suite checked out from a fork would carry it.
//
// Nothing new refuses these — [flowtest.Load] parses with yaml.Strict, so an
// unknown key has been refused since the parser was written. That is the point
// worth pinning rather than a gap worth filling: the abstention holds because
// the file surface is closed, and a future `Test` field added without reading
// #652 is what would open it. This fails the moment one is.
func TestACaseCannotDeclareADeploymentPolicy(t *testing.T) {
	t.Parallel()

	for _, key := range []string{"task_policy", "egress_policy", "secret_policy", "policy"} {
		t.Run(key, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			path := dir + "/policy.test.yaml"
			writeFile(t, path, `
tests:
  - name: a case that tries to bring its own deployment policy
    workflow: ./workflow.yaml
    `+key+`:
      deny:
        - 'task == "log"'
`)

			_, err := flowtest.Load(path)
			require.Error(t, err,
				"a case file must not be able to declare `%s:`; a deployment's policy is not a "+
					"property of the workflow under test, and a case carrying one would make its "+
					"verdict a test of whoever configured it (#652 item 2)", key)

			// A misspelled key must be *reported*, not ignored — CLAUDE.md's
			// diagnostics rule — so the refusal has to name the key the author
			// wrote rather than failing somewhere downstream.
			require.Contains(t, err.Error(), key,
				"the refusal must name the key the author wrote")
		})
	}
}
