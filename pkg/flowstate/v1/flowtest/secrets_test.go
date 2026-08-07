package flowtest_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// TestRunFileSecretResolvesFromTestFile is the positive case: a workflow
// putting `${secret('env:TOKEN')}` into an http header runs under `flow
// test` with a `secrets:` entry, and the stubbed task's `where:` observes the
// resolved plaintext — proof that resolution actually happened rather than
// the reference passing through untouched.
func TestRunFileSecretResolvesFromTestFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.2
name: bearer-request
steps:
  - id: call
    http:
      url: https://api.example.com/status
      bearer: ${secret('env:TOKEN')}
`)
	writeFile(t, dir+"/workflow.test.yaml", `
tests:
  - name: the stub observes the resolved secret
    workflow: ./workflow.yaml
    secrets:
      env:TOKEN: s3cr3t-value
    stubs:
      - task: http
        where: inputs.bearer == 's3cr3t-value'
        returns:
          status_code: 200
    expect:
      ran: [call]
`)

	report := flowtest.RunFile(dir + "/workflow.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)

	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "failures: %v", c.GetFailures())
}

// TestRunFileSecretWithNoEntryIsRefused is the negative case, and the point:
// the same workflow with no `secrets:` entry must be refused, naming the
// reference, rather than resolving to an empty bearer token or reaching a
// real backend.
func TestRunFileSecretWithNoEntryIsRefused(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.2
name: bearer-request
steps:
  - id: call
    http:
      url: https://api.example.com/status
      bearer: ${secret('env:TOKEN')}
`)
	writeFile(t, dir+"/workflow.test.yaml", `
tests:
  - name: no secrets entry at all
    workflow: ./workflow.yaml
    stubs:
      - task: http
        returns:
          status_code: 200
    expect:
      ran: [call]
`)

	report := flowtest.RunFile(dir + "/workflow.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)

	c := report.GetCases()[0]
	require.False(t, c.GetPassed(), "a case with no `secrets:` entry for env:TOKEN must not pass")
	require.NotEmpty(t, c.GetFailures())

	found := false
	for _, f := range c.GetFailures() {
		if f.GetField() == "expect.failed" {
			require.Contains(t, f.GetMessage(), "env:TOKEN")
			require.Contains(t, f.GetMessage(), "secrets:")
			found = true
		}
	}
	require.True(t, found, "expected an expect.failed diagnostic naming env:TOKEN and `secrets:`; got %v", c.GetFailures())
}

// TestRunFileSecretSchemeNeedNotExist proves the scheme need not exist: a
// `vault:` reference resolves purely from the test file's own `secrets:`
// block, with no Vault provider registered or even compiled into this
// build — that is what makes #250's backends testable at all.
func TestRunFileSecretSchemeNeedNotExist(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.2
name: vault-backed-call
steps:
  - id: call
    http:
      url: https://api.example.com/status
      bearer: ${secret('vault:apps/api#token')}
`)
	writeFile(t, dir+"/workflow.test.yaml", `
tests:
  - name: vault resolves purely from this file
    workflow: ./workflow.yaml
    secrets:
      vault:apps/api#token: from-the-test-file-not-a-real-vault
    stubs:
      - task: http
        where: inputs.bearer == 'from-the-test-file-not-a-real-vault'
        returns:
          status_code: 200
    expect:
      ran: [call]
`)

	report := flowtest.RunFile(dir + "/workflow.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)

	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "failures: %v", c.GetFailures())
}

// TestLoadRejectsMalformedSecretRef checks that a `secrets:` key that is not
// a well-formed "scheme:name" reference fails when the file loads, the same
// timing a malformed stub or signal already fails at.
func TestLoadRejectsMalformedSecretRef(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.2
name: whatever
steps:
  - id: noop
    log:
      message: hi
`)
	writeFile(t, dir+"/workflow.test.yaml", `
tests:
  - name: a malformed secrets key
    workflow: ./workflow.yaml
    secrets:
      not-a-reference: whatever
    stubs:
      - task: log
        returns: {}
`)

	_, err := flowtest.Load(dir + "/workflow.test.yaml")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no provider")
}

// TestLoadRejectsTooManySecrets checks the bound is reached rather than
// merely declared: MaxSecretsPerTest+1 entries is refused, and the message
// names both the count and the limit so an author knows which way to move.
// The sibling bound MaxStubsPerTest is pinned the same way in run_test.go —
// a limit nothing tests is a limit nothing enforces once someone edits the
// loader.
func TestLoadRejectsTooManySecrets(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.2
name: whatever
steps:
  - id: noop
    log:
      message: hi
`)

	var secretsBlock strings.Builder
	for i := 0; i <= flowtest.MaxSecretsPerTest; i++ {
		fmt.Fprintf(&secretsBlock, "      env:TOKEN_%d: value-%d\n", i, i)
	}

	writeFile(t, dir+"/workflow.test.yaml", `
tests:
  - name: too many secrets
    workflow: ./workflow.yaml
    secrets:
`+secretsBlock.String()+`    stubs:
      - task: log
        returns: {}
`)

	_, err := flowtest.Load(dir + "/workflow.test.yaml")
	require.Error(t, err)
	require.Contains(t, err.Error(), fmt.Sprintf("declares %d secrets", flowtest.MaxSecretsPerTest+1))
	require.Contains(t, err.Error(), fmt.Sprintf("limit of %d", flowtest.MaxSecretsPerTest))
}

// TestSecretsContainmentShapes is the redaction containment matrix CLAUDE.md
// requires for anything that behaves as a [secrets.Secret]: a test-supplied
// secret value must never appear under %v, %+v, %#v, or %s, whether printed
// directly, nested in a struct through an unexported field, or nested in a
// slice of those. This exercises flow test's own secret path end to end
// (Test.Secrets -> the in-memory provider -> [secrets.Store] ->
// [v1.ResolveSecret]) rather than constructing a [secrets.Secret] by hand, so
// it proves the containment holds for what flow test itself resolves and not
// only for the type in isolation.
func TestSecretsContainmentShapes(t *testing.T) {
	t.Parallel()

	const material = "leak-me-not-0451"

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.2
name: bearer-request
steps:
  - id: call
    http:
      url: https://api.example.com/status
      bearer: ${secret('env:TOKEN')}
`)
	writeFile(t, dir+"/workflow.test.yaml", fmt.Sprintf(`
tests:
  - name: containment
    workflow: ./workflow.yaml
    secrets:
      env:TOKEN: %s
    stubs:
      - task: http
        returns:
          status_code: 200
    expect:
      ran: [call]
`, material))

	report := flowtest.RunFile(dir + "/workflow.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "failures: %v", c.GetFailures())

	// The report itself — the only artifact this package hands back to a
	// caller — must not contain the plaintext anywhere in its rendering.
	require.NotContains(t, fmt.Sprintf("%v", report), material)
	require.NotContains(t, fmt.Sprintf("%+v", report), material)
	require.NotContains(t, fmt.Sprintf("%#v", report), material)
	require.NotContains(t, report.String(), material)

	// And directly on the type every resolution produces, on a struct
	// holding it through an unexported field, and on a slice of those — the
	// exact shapes CLAUDE.md's "Secrets never enter workflow history" names.
	secret := secrets.NewSecret(secrets.NewRef("env", "TOKEN"), material)

	require.NotContains(t, fmt.Sprintf("%v", secret), material)
	require.NotContains(t, fmt.Sprintf("%+v", secret), material)
	require.NotContains(t, fmt.Sprintf("%#v", secret), material)
	require.NotContains(t, secret.String(), material)

	type holder struct{ secret secrets.Secret }
	held := holder{secret: secret}

	require.NotContains(t, fmt.Sprintf("%v", held), material)
	require.NotContains(t, fmt.Sprintf("%+v", held), material)
	require.NotContains(t, fmt.Sprintf("%#v", held), material)
	// %s on a struct with no Stringer is a vet-flagged shape; hidden behind
	// an `any` so vet cannot see the static type and this test keeps
	// checking the exact reflection path CLAUDE.md's containment matrix
	// requires: %s falls through to the same reflect-over-fields formatting
	// %v does, which is precisely the path an unexported Secret field must
	// survive.
	var heldAny any = held
	require.NotContains(t, fmt.Sprintf("%s", heldAny), material)

	slice := []holder{held, held}

	require.NotContains(t, fmt.Sprintf("%v", slice), material)
	require.NotContains(t, fmt.Sprintf("%+v", slice), material)
	require.NotContains(t, fmt.Sprintf("%#v", slice), material)
	var sliceAny any = slice
	require.NotContains(t, fmt.Sprintf("%s", sliceAny), material)
}
