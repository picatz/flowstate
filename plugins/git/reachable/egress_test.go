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

// deniedHost is the destination the operator's policy below refuses. It is
// denied by name, so the refusal happens before any name resolution and the
// test needs no network of its own.
const deniedHost = "git.example.com"

// operatorPolicyDenying is the file an operator writes to stop this plugin
// reaching one host — the shape of policy #1321 says a deployment must be able
// to express, and could not, while this plugin built its own.
const operatorPolicyDenying = "egress:\n  deny:\n    - host == \"" + deniedHost + "\"\n"

// TestAnOperatorDenyRuleStopsAGitTask is #1321's acceptance stated as a test:
// the deployment's own --egress-policy governs a git.* task on the real
// connection path, through a real launched plugin process.
//
// Before this, the plugin built its own safe-default policy and never read the
// grant, so an operator who wrote this deny rule governed the built-in http task
// and nothing else — the deny rule reached no git task at all, which is the
// false promise the flag's help had to carry until now.
//
// The task is invoked through the host's own TaskDef rather than a Flowfile,
// because what is under test is the connection this process makes, not the
// author-facing spelling the neighbouring test already covers.
func TestAnOperatorDenyRuleStopsAGitTask(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a real plugin binary; skipped under -short, run in CI and by `make check`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("the Go toolchain is not available, so this plugin cannot be built")
	}

	dir := t.TempDir()
	binaryPath := filepath.Join(dir, plugin.BinaryPrefix+"git")
	buildPlugin(t, binaryPath)

	host := openHost(t, plugin.Config{
		SearchPath:          []string{dir},
		HandshakeTimeout:    10 * time.Second,
		DescribeTimeout:     10 * time.Second,
		CallTimeout:         30 * time.Second,
		HealthTimeout:       5 * time.Second,
		ShutdownGrace:       5 * time.Second,
		DisableHealthChecks: true,
		Logger:              testLogger(t),

		// The field, not a hand-composed Env entry: the host owns encoding the
		// grant, and this is the launch path a worker actually takes.
		EgressPolicy: []byte(operatorPolicyDenying),
	})

	var lsRemote *flowstatev1.TaskDef
	for _, def := range host.TaskDefs() {
		if def.Name == "git.ls_remote" {
			lsRemote = &def
			break
		}
	}
	if lsRemote == nil {
		t.Fatal("the launched plugin offers no git.ls_remote, so nothing below tests the policy")
	}

	_, err := lsRemote.Fn(t.Context(), map[string]*flowstatev1.Value{
		"url": flowstatev1.NewValue("https://" + deniedHost + "/an/org/repo.git"),
	}, nil)
	if err == nil {
		t.Fatal("git.ls_remote reached a host the operator's egress policy denies")
	}
	if !strings.Contains(err.Error(), "denied by egress policy") {
		t.Fatalf("git.ls_remote failed for some reason other than the operator's policy: %v", err)
	}
}

// credentialRulePolicy denies exactly the requests that carry one, and permits
// the same destination otherwise. Both halves are needed: the denial is only
// evidence about the credential if the request without it goes through.
const credentialRulePolicy = "egress:\n  deny:\n    - 'credentials'\n"

// TestACloneCarryingATokenIsMarkedAsCredentialed is the half of the grant a
// destination rule does not cover.
//
// `deny: ['credentials && !(host in [...])']` — a secret leaves only towards one
// place — is a rule an operator can write today, and it decides nothing for a
// plugin whose client never says a request carried one. go-git sets an
// Authorization header for BasicAuth, so the marking client the SDK hands out is
// what makes that rule fire for a clone; a client composed out of the policy
// alone would let the same clone through with the rule evaluating false, which
// is a rule that did not fire rather than one that allowed.
//
// The unauthenticated case is what makes the denial evidence about the token: it
// is the same task, the same host, the same policy, one input.
func TestACloneCarryingATokenIsMarkedAsCredentialed(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a real plugin binary; skipped under -short, run in CI and by `make check`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("the Go toolchain is not available, so this plugin cannot be built")
	}

	dir := t.TempDir()
	binaryPath := filepath.Join(dir, plugin.BinaryPrefix+"git")
	buildPlugin(t, binaryPath)

	host := openHost(t, plugin.Config{
		SearchPath:          []string{dir},
		HandshakeTimeout:    10 * time.Second,
		DescribeTimeout:     10 * time.Second,
		CallTimeout:         30 * time.Second,
		HealthTimeout:       5 * time.Second,
		ShutdownGrace:       5 * time.Second,
		DisableHealthChecks: true,
		Logger:              testLogger(t),
		EgressPolicy:        []byte(credentialRulePolicy),
	})

	var lsRemote *flowstatev1.TaskDef
	for _, def := range host.TaskDefs() {
		if def.Name == "git.ls_remote" {
			lsRemote = &def
			break
		}
	}
	if lsRemote == nil {
		t.Fatal("the launched plugin offers no git.ls_remote, so nothing below tests the policy")
	}

	// A token, and therefore an Authorization header on the wire. The host
	// refuses a literal for a required secret input before dispatch, so it
	// arrives as a resolvable reference.
	_, err := lsRemote.Fn(flowstatev1.ContextWithTaskRuntime(t.Context(), taskRuntimeResolvingTheTestToken(t)),
		map[string]*flowstatev1.Value{
			"url": flowstatev1.NewValue("https://" + deniedHost + "/an/org/repo.git"),
			"token": {Kind: &flowstatev1.Value_SecretRef{SecretRef: &flowstatev1.SecretRef{
				Scheme: "env", Name: testTokenName,
			}}},
		}, nil)
	if err == nil {
		t.Fatal("a clone carrying a token was not seen as carrying one; the operator's credentials rule did not fire")
	}
	if !strings.Contains(err.Error(), "denied by egress policy") {
		t.Fatalf("the clone failed for some reason other than the operator's policy: %v", err)
	}
}

// testTokenName is the reference the call above passes; the environment variable
// behind it is FLOWSTATE_SECRET_ plus this name, the env provider's own prefix.
const testTokenName = "GIT_EGRESS_TEST_TOKEN"

// taskRuntimeResolvingTheTestToken is the smallest runtime that lets a whole
// secret reference resolve, because the host refuses a literal for a required
// secret input before it dispatches — a control worth having, and an obstacle
// for a test about somewhere else.
func taskRuntimeResolvingTheTestToken(t *testing.T) flowstatev1.TaskRuntime {
	t.Helper()

	t.Setenv(secrets.DefaultEnvPrefix+testTokenName, "not-a-real-token")

	provider, err := secrets.NewEnvProvider(secrets.WithEnvAllow(testTokenName))
	if err != nil {
		t.Fatalf("building the env secret provider: %v", err)
	}
	store, err := secrets.NewStore(provider)
	if err != nil {
		t.Fatalf("building the secret store: %v", err)
	}
	policy, err := auth.SecretAccessPolicy{
		Allow: []string{`secret.scheme == "env" && secret.name == "` + testTokenName + `"`},
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
		Step: auth.StepRef{Workflow: "git-egress", Run: "egress-test", Step: "read"},
	}
}
