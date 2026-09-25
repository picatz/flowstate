package reachable

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// TestADefaultWorkerRefusesPostgresAndNamesTheFlag is this plugin's half of
// point 7 of #1332, through a real launched process.
//
// A worker started with no --egress-policy now grants its own default rather
// than nothing (point 6), and every other first-party plugin accepts that grant
// and reaches public hosts with it. This one does not: a database destination is
// the whole meaning of the credential the task carries, and a default policy is
// what a deployment runs under when nobody has decided anything about
// destinations. #1320 made that refusal; the grant becoming universal must not
// quietly undo it.
//
// The message has to name the flag. "Denied" alone leaves an operator guessing
// which of several controls decided, and the remedy — write a policy and pass
// --egress-policy — is not derivable from the refusal without it.
func TestADefaultWorkerRefusesPostgresAndNamesTheFlag(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a real plugin binary; skipped under -short, run in CI and by `make check`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("the Go toolchain is not available, so this plugin cannot be built")
	}

	dir := t.TempDir()
	binaryPath := filepath.Join(dir, plugin.BinaryPrefix+"sql")
	buildPlugin(t, binaryPath)

	host := openHost(t, plugin.Config{
		SearchPath:          []string{dir},
		HandshakeTimeout:    10 * time.Second,
		DescribeTimeout:     10 * time.Second,
		CallTimeout:         30 * time.Second,
		ShutdownGrace:       5 * time.Second,
		DisableHealthChecks: true,

		// What `flow worker` grants when no --egress-policy was configured:
		// the deployment default, written down and marked as the default. Read
		// from the same function the CLI calls, so this is that launch rather
		// than a document resembling it.
		EgressPolicy: flowstatev1.DefaultEgressPolicyDocument(),
	})

	var query *flowstatev1.TaskDef
	for _, def := range host.TaskDefs() {
		if def.Name == "sql.query" {
			query = &def
			break
		}
	}
	if query == nil {
		t.Fatal("the launched plugin offers no sql.query, so nothing below tests the policy")
	}

	_, err := query.Fn(flowstatev1.ContextWithTaskRuntime(t.Context(), taskRuntimeResolvingTheTestDSN(t)),
		map[string]*flowstatev1.Value{
			"dsn": {Kind: &flowstatev1.Value_SecretRef{SecretRef: &flowstatev1.SecretRef{
				Scheme: "env", Name: testDSNName,
			}}},
			"engine":   flowstatev1.NewValue("ENGINE_POSTGRES"),
			"query":    flowstatev1.NewValue("SELECT 1"),
			"max_rows": flowstatev1.NewValue(int64(1)),
		}, nil)
	if err == nil {
		t.Fatal("sql.query connected to a database on a worker whose operator authorized no destination")
	}
	if !strings.Contains(err.Error(), "--egress-policy") {
		t.Fatalf("the refusal does not name the flag that would grant it: %v", err)
	}
}

// testDSNName is the reference the call above passes; the environment variable
// behind it is FLOWSTATE_SECRET_ plus this name, the env provider's own prefix.
const testDSNName = "SQL_EGRESS_TEST_DSN"

// taskRuntimeResolvingTheTestDSN is the smallest runtime that lets a whole
// secret reference resolve, because the host refuses a literal for a required
// secret input before it dispatches — a control worth having, and an obstacle
// for a test about somewhere else.
//
// The DSN it resolves to is never connected to: the refusal under test happens
// before the driver opens anything, which is the point.
func taskRuntimeResolvingTheTestDSN(t *testing.T) flowstatev1.TaskRuntime {
	t.Helper()

	t.Setenv(secrets.DefaultEnvPrefix+testDSNName, "postgres://app@database.example:5432/app?sslmode=verify-full")

	provider, err := secrets.NewEnvProvider(secrets.WithEnvAllow(testDSNName))
	if err != nil {
		t.Fatalf("building the env secret provider: %v", err)
	}
	store, err := secrets.NewStore(provider)
	if err != nil {
		t.Fatalf("building the secret store: %v", err)
	}
	policy, err := auth.SecretAccessPolicy{
		Allow: []string{`secret.scheme == "env" && secret.name == "` + testDSNName + `"`},
	}.Compile()
	if err != nil {
		t.Fatalf("compiling the secret access policy: %v", err)
	}

	return flowstatev1.TaskRuntime{
		Store:  store,
		Policy: policy,
		Identity: auth.WorkloadIdentity{
			Subject: "worker",
			Issuer:  "https://issuer.example.com",
		},
		Step: auth.StepRef{Workflow: "sql-egress", Run: "egress-test", Step: "read"},
	}
}
