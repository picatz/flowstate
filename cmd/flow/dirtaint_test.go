package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// #2080's two leaks, found by the independent review of #2066 (the #2041
// fix): a secret-holding var seeded in one suite file was not withheld in a
// sibling that only reads the value the directory's shared testdefaults.yaml
// gave it, and a load-time diagnostic — as opposed to a case's own runtime
// posture, which #2066 and #2079 already cover — had no redaction seam at
// all. Both are reproduced here exactly as the issue states them, on both
// `flow test` and `flow validate`, in text and `--json`.

// TestASuiteNamingASecretFromADirectoryVarIsRefused is #2080's first leak.
// `testdefaults.yaml` states a var every suite file in the directory reads;
// `a.test.yaml` names it from its own `secrets:`, and `b.test.yaml` only ever
// reads it — through a scripted signal name that matches no gate the workflow
// waits on. Taint is a fact about one suite's `secrets:`, so `a.test.yaml` is
// refused at load for putting a directory var on a path to a secret, naming
// the remedy: move `token` into its own `vars:`.
//
// b.test.yaml's own mismatch still prints the value, by design: no file b can
// see calls it a secret, and a.test.yaml is refused before it can. The rule
// is that a secret-holding value lives in the suite that names the secret, so
// the residual is a directory var that no loadable suite withholds.
func TestASuiteNamingASecretFromADirectoryVarIsRefused(t *testing.T) {
	t.Parallel()

	const secret = "sk-live-multifile-4471"

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow-a.yaml"), []byte(cleanWorkflow), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow-b.yaml"), []byte(gatedWorkflow), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "testdefaults.yaml"), []byte(
		"edition: v2026.3\n"+
			"vars:\n"+
			"  token: "+secret+"\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.test.yaml"), []byte(
		"tests:\n"+
			"  - name: holds the secret\n"+
			"    workflow: ./workflow-a.yaml\n"+
			"    secrets:\n"+
			"      env:TOKEN: ${vars.token}\n"+
			"    stubs:\n"+
			"      - task: log\n"+
			"        returns: {}\n"+
			"    expect:\n"+
			"      ran: [s]\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "b.test.yaml"), []byte(
		"tests:\n"+
			"  - name: the gate is signalled by the wrong name\n"+
			"    workflow: ./workflow-b.yaml\n"+
			"    signals:\n"+
			"      - name: ${vars.token}\n"+
			"        at: 1s\n"+
			"        payload: {}\n"+
			"    expect:\n"+
			"      ran: [gate]\n"), 0o600))

	for _, cmd := range []string{"test", "validate"} {
		for _, format := range []string{"text", "json"} {
			t.Run(cmd+"/"+format, func(t *testing.T) {
				t.Parallel()

				res := runFlow(t, cmd, "-o", format, dir)
				require.Error(t, res.Err, "a.test.yaml puts a directory var on a path to a secret, so it is refused")

				assert.Contains(t, res.Output(), "vars.token is stated by testdefaults.yaml",
					"the refusal names the directory's var and the file that states it (#2080)")
				assert.Contains(t, res.Output(), "move vars.token into the suite's own vars: and remove it from testdefaults.yaml",
					"the refusal names the remedy")
			})
		}
	}
}

// TestALoadTimeDiagnosticIsRedacted is #2080's second leak. `problems.report`
// — the seam every load-time refusal in `file.go` and `diagnostics.go` speaks
// through — never cleared its message against what a file's `vars:` withhold,
// which is the eighth surface `vars.go`'s own containment table did not yet
// list: a case's `secrets:` taints `vars.token`; a second case's malformed
// `trigger:` stanza (naming both a webhook and a kind, refused before either
// half of it is validated) quotes the substituted value straight into the
// diagnostic that refuses the whole file.
func TestALoadTimeDiagnosticIsRedacted(t *testing.T) {
	t.Parallel()

	const secret = "sk-live-loadleak-3310"

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(cleanWorkflow), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(
		"vars:\n"+
			"  token: "+secret+"\n"+
			"tests:\n"+
			"  - name: holds the secret\n"+
			"    workflow: ./workflow.yaml\n"+
			"    secrets:\n"+
			"      env:TOKEN: ${vars.token}\n"+
			"    expect:\n"+
			"      ran: [s]\n"+
			"  - name: a trigger stated both ways\n"+
			"    workflow: ./workflow.yaml\n"+
			"    trigger:\n"+
			"      webhook: ${vars.token}\n"+
			"      kind: manual\n"), 0o600))

	for _, cmd := range []string{"test", "validate"} {
		for _, format := range []string{"text", "json"} {
			t.Run(cmd+"/"+format, func(t *testing.T) {
				t.Parallel()

				res := runFlow(t, cmd, "-o", format, dir)
				require.Error(t, res.Err, "a trigger naming both a webhook and a kind must refuse the file")

				require.NotContains(t, res.Output(), secret,
					"a load-time diagnostic quoted a value substituted from a withheld var (#2080)")
				require.Contains(t, res.Output(), "names both a webhook",
					"the positive control: the diagnostic itself must still be reported")
				assert.Contains(t, res.Output(), "[redacted]",
					"the redaction leaves a marker rather than silently dropping the value")
			})
		}
	}
}
