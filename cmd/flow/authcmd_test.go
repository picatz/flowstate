package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeAuthPolicy(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "auth.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`issuers:
  - name: local
    issuer: https://issuer.example.com
    audiences: [flowstate]
    require:
      - claim: repository
        any_of: [acme/service]
`), 0o600))
	return path
}

func runAuthCLI(t *testing.T, args ...string) string {
	t.Helper()
	cmd := newRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	require.NoError(t, cmd.Execute())
	return out.String()
}

func TestAuthDoctorJSONIsStableAndNamesFields(t *testing.T) {
	out := runAuthCLI(t, "auth", "doctor", "--policy", writeAuthPolicy(t), "--output", "json")
	var report authReport
	require.NoError(t, json.Unmarshal([]byte(out), &report))
	require.Equal(t, "flowstate.auth.report.v1", report.Schema)
	require.Equal(t, "issuers[0].audiences", report.Findings[0].Field)
	require.NotContains(t, out, "Bearer ")
}

func TestAuthRehearsalCoversEveryNegativeScenario(t *testing.T) {
	out := runAuthCLI(t, "auth", "rehearse", "--policy", writeAuthPolicy(t), "--output", "json")
	var report authReport
	require.NoError(t, json.Unmarshal([]byte(out), &report))
	require.True(t, report.Unattested)
	require.Len(t, report.Scenarios, 10)
	for _, scenario := range report.Scenarios {
		require.Equal(t, "pass", scenario.Status)
		require.NotEmpty(t, scenario.Field)
	}
}
