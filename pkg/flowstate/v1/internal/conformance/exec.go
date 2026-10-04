package conformance

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/execpolicy"
)

// Shared cases for the built-in `exec` task, run by both execution drivers:
// `flowstatev1_test.TestRunWorkflowExec` locally and `engine.TestRunWorkflowExec`
// durably.
//
// # Why these are shared, and what they pin
//
// Process execution is the capability whose two drivers could most easily
// diverge without either noticing: the local driver runs the task in the
// caller's process, the durable one in an activity, and each reaches the policy,
// the identity, the error classification and the output record by a different
// route. The cases here are the join. Every one runs a *real* program (sh) under
// a *real* policy, because a stubbed exec would prove the plumbing and none of
// the claims worth making: that an exit code is data, that an argument stays one
// argument, that the environment is built from nothing, that a bound ends the
// program, and that a denial is a denial on both.
//
// They are written in both directions on purpose. Each permitted shape has a
// refused sibling (an authored key and an unlisted one, one tenant and another),
// because a policy that said yes to everything would pass every positive case.

// ExecCase pairs a workflow with the policy it runs under and the outcome both
// drivers must reach.
type ExecCase struct {
	// Name identifies the case in test output.
	Name string

	// Workflow is the file under test.
	Workflow *v1.Workflow

	// Identity is who the run acts as, or nil for a run whose starter named
	// nobody. Each driver carries it by its own route; see [TaskPolicyCase.Identity].
	Identity *v1.WorkloadIdentity

	// NoPolicy runs the case with the built-in default: the exec task as it
	// ships, denied because no deployment has configured a policy.
	NoPolicy bool

	// Allow and Deny are CEL rules added to the case's policy.
	Allow []string
	Deny  []string

	// Timeout is the policy's time bound; zero means [ExecDefaultTimeout].
	Timeout time.Duration

	// ExpectedKind and ExpectedError describe a run that must fail outright:
	// the classification both drivers agree on, and text the failure carries.
	// Empty ExpectedKind means the run must succeed.
	ExpectedKind  v1.ErrorKind
	ExpectedError []string

	// Check asserts on the outputs of a run that must succeed. It is a function
	// rather than a literal because a process's duration is not a constant.
	Check func(tb testing.TB, out *v1.Workflow_StepOutputs)
}

const (
	// ExecDefaultTimeout is the policy's time bound unless a case says otherwise,
	// generous enough that a loaded CI machine never trips it by accident.
	ExecDefaultTimeout = 20 * time.Second

	// ExecMaxOutputBytes is the per-stream bound every case runs under, small
	// enough that one case can overrun it with a one-line program.
	ExecMaxOutputBytes = 256

	// execWorkerSecretEnv is a variable [InstallExecPolicy] sets in the worker's
	// own environment, standing in for the credentials a worker holds there.
	execWorkerSecretEnv = "CONFORMANCE_EXEC_WORKER_SECRET"
)

// ExecRoot creates the directory the cases run in and returns it with symbolic
// links resolved, which is how the policy compares it.
func ExecRoot(tb testing.TB) string {
	tb.Helper()

	root, err := filepath.EvalSymlinks(tb.TempDir())
	require.NoError(tb, err)

	return root
}

// InstallExecPolicy registers the exec task enforcing the policy the cases run
// under for the duration of the test, restoring whatever was registered before.
//
// The policy lists one program (sh, found on this machine and resolved), one
// root, one authored environment key (GREETING), nothing passed through from the
// process, and the bounds above. It mutates the process-wide registry, so it is
// serial like [InstallEgressIdentityPolicy] and for the same reason: do not call
// it from a parallel test.
func InstallExecPolicy(tb testing.TB, root string, c ExecCase) {
	tb.Helper()

	tb.Setenv(execWorkerSecretEnv, "worker-secret")
	tb.Setenv("HOME", "/nonexistent-worker-home")
	tb.Setenv("USER", "worker")

	registry := v1.DefaultRegistry()
	original, existed := registry.Lookup("exec")

	var policy *execpolicy.Policy
	if !c.NoPolicy {
		sh, err := exec.LookPath("sh")
		if err != nil {
			tb.Skipf("sh is not installed: %v", err)
		}
		sh, err = filepath.EvalSymlinks(sh)
		require.NoError(tb, err)

		timeout := c.Timeout
		if timeout == 0 {
			timeout = ExecDefaultTimeout
		}
		policy, err = execpolicy.New(execpolicy.Config{
			Executables:    map[string]string{"sh": sh},
			Roots:          []string{root},
			EnvAuthored:    []string{"GREETING"},
			Timeout:        timeout,
			MaxOutputBytes: ExecMaxOutputBytes,
			Allow:          c.Allow,
			Deny:           c.Deny,
			// Nothing is passed through, so a case can prove the worker's own
			// environment does not reach the program.
			LookupEnv: func(string) (string, bool) { return "", false },
		})
		require.NoError(tb, err, "every case's policy must itself load")
	}

	require.NoError(tb, registry.Replace(v1.ExecTaskDef(policy)))
	tb.Cleanup(func() {
		if existed {
			_ = registry.Replace(original)
		}
	})
}

// execStep is a step running argv in dir, with optional environment.
func execStep(id string, argv *v1.Value, dir string, env *v1.Value) *v1.Node {
	inputs := map[string]*v1.Value{
		"argv": argv,
		"dir":  v1.NewLiteral(dir),
	}
	if env != nil {
		inputs["env"] = env
	}

	return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{Name: "exec", Inputs: inputs}}}
}

func shArgv(script string, rest ...string) *v1.Value {
	words := []any{"sh", "-c", script}
	for _, word := range rest {
		words = append(words, word)
	}

	return v1.NewLiteralList(words...)
}

func execWorkflow(name string, steps ...*v1.Node) *v1.Workflow {
	return &v1.Workflow{Name: name, Profile: v1.CurrentProfile, Steps: steps}
}

// execField reads one named output of a step.
func execField(tb testing.TB, out *v1.Workflow_StepOutputs, step, name string) any {
	tb.Helper()

	node, ok := out.GetStepValues()[step]
	require.True(tb, ok, "step %q produced no outputs", step)
	value, ok := node.GetNamedValues()[name]
	require.True(tb, ok, "step %q has no output %q", step, name)
	native, err := v1.LiteralToGo(value.GetLiteral())
	require.NoError(tb, err)

	return native
}

// ExecCases returns the shared cases both drivers must agree on. root comes from
// [ExecRoot].
func ExecCases(root string) []ExecCase {
	teamA := &v1.WorkloadIdentity{Subject: "spiffe://acme/a", Issuer: "https://issuer.example.com", Namespace: "team-a"}
	teamB := &v1.WorkloadIdentity{Subject: "spiffe://acme/b", Issuer: "https://issuer.example.com", Namespace: "team-b"}

	denied := func(contains ...string) []string { return append([]string{"exec"}, contains...) }

	return []ExecCase{
		{
			// The default posture: the task ships in the registry and does
			// nothing. The remedy is in the sentence, because a denial an
			// operator cannot act on is no diagnostic.
			Name:          "the task is denied until a policy enables it",
			NoPolicy:      true,
			Workflow:      execWorkflow("exec-default-denied", execStep("run", shArgv("echo hi"), root, nil)),
			ExpectedKind:  v1.ErrorKindPolicyDenied,
			ExpectedError: denied("no exec policy", "--exec-policy"),
		},
		{
			// A nonzero exit is output. The step succeeds, the code is data, and
			// a later step branches on it: the same stance as an HTTP status.
			Name: "a nonzero exit code is output, not failure",
			Workflow: execWorkflow("exec-exit-code",
				execStep("run", shArgv(`printf out; printf err >&2; exit 3`), root, nil),
				&v1.Node{
					Id:        "after",
					Condition: v1.NewExpr(`steps.run.exit_code == 3 && steps.run.outcome == "ran"`),
					Kind: &v1.Node_Task{Task: &v1.Task{Name: "log", Inputs: map[string]*v1.Value{
						"message": v1.NewExpr(`"exited " + string(steps.run.exit_code)`),
					}}},
				}),
			Check: func(tb testing.TB, out *v1.Workflow_StepOutputs) {
				require.EqualValues(tb, 3, execField(tb, out, "run", "exit_code"))
				require.Equal(tb, "out", execField(tb, out, "run", "stdout"))
				require.Equal(tb, "err", execField(tb, out, "run", "stderr"))
				require.Equal(tb, "ran", execField(tb, out, "run", "outcome"))
				require.Equal(tb, "", execField(tb, out, "run", "signal"))
				require.Equal(tb, false, execField(tb, out, "run", "stdout_truncated"))
				require.Equal(tb, false, execField(tb, out, "run", "stderr_truncated"))
				require.Contains(tb, out.GetStepValues(), "after", "the exit code was readable by the next step")
			},
		},
		{
			// The injection claim. The value is a shell's worst day, and it is
			// one argument: nothing between the workflow and the program parses
			// it.
			Name: "a value inside an argument stays one argument",
			Workflow: &v1.Workflow{
				Name:    "exec-argv-is-not-a-shell-string",
				Profile: v1.CurrentProfile,
				Vars:    map[string]*v1.Value{"payload": v1.NewLiteral("x; echo hacked; $(echo hacked) `echo hacked` > pwned")},
				Steps: []*v1.Node{execStep("run",
					v1.NewExpr(`["sh", "-c", "printf %s \"$1\"", "sh", vars.payload]`), root, nil)},
			},
			Check: func(tb testing.TB, out *v1.Workflow_StepOutputs) {
				require.Equal(tb, "x; echo hacked; $(echo hacked) `echo hacked` > pwned", execField(tb, out, "run", "stdout"))
				require.EqualValues(tb, 0, execField(tb, out, "run", "exit_code"))
			},
		},
		{
			// The environment is built from nothing: the worker process has a HOME
			// and a secret of its own (set by [InstallExecPolicy]), and the program
			// must see neither. A shell invents a PATH when it has none, so PATH is
			// not a variable this case can speak to.
			Name: "the worker's environment does not reach the program",
			Workflow: execWorkflow("exec-env-from-nothing",
				execStep("run", shArgv(`printf '%s|%s|%s' "${HOME-unset}" "${USER-unset}" "${`+execWorkerSecretEnv+`-unset}"`), root, nil)),
			Check: func(tb testing.TB, out *v1.Workflow_StepOutputs) {
				require.Equal(tb, "unset|unset|unset", execField(tb, out, "run", "stdout"))
			},
		},
		{
			Name: "an authored environment key the policy names reaches the program",
			Workflow: execWorkflow("exec-env-authored",
				execStep("run", shArgv(`printf %s "$GREETING"`), root, v1.NewLiteralMap(map[string]any{"GREETING": "hello"}))),
			Check: func(tb testing.TB, out *v1.Workflow_StepOutputs) {
				require.Equal(tb, "hello", execField(tb, out, "run", "stdout"))
			},
		},
		{
			// Its refused sibling: the same step setting a key the operator did
			// not name.
			Name: "an environment key the policy does not name is denied",
			Workflow: execWorkflow("exec-env-unlisted",
				execStep("run", shArgv(`true`), root, v1.NewLiteralMap(map[string]any{"LD_PRELOAD": "/tmp/evil.so"}))),
			ExpectedKind:  v1.ErrorKindPolicyDenied,
			ExpectedError: denied("LD_PRELOAD", "env_authored"),
		},
		{
			Name: "a program the policy does not list is denied",
			Workflow: execWorkflow("exec-unlisted-program",
				execStep("run", v1.NewLiteralList("curl", "https://example.com"), root, nil)),
			ExpectedKind:  v1.ErrorKindPolicyDenied,
			ExpectedError: denied(`"curl"`, "not a program this policy lists"),
		},
		{
			// A path is refused even when it names the very file the table
			// resolves: a workflow names, it never locates.
			Name: "a path in argv[0] is denied",
			Workflow: execWorkflow("exec-path-argv0",
				execStep("run", v1.NewLiteralList("/bin/sh", "-c", "true"), root, nil)),
			ExpectedKind:  v1.ErrorKindPolicyDenied,
			ExpectedError: denied("is a path"),
		},
		{
			Name: "a directory outside every root is denied",
			Workflow: execWorkflow("exec-dir-outside",
				execStep("run", shArgv(`true`), filepath.Dir(root), nil)),
			ExpectedKind:  v1.ErrorKindPolicyDenied,
			ExpectedError: denied("not under any root"),
		},
		{
			// Output beyond the bound is dropped, not allowed to grow the run's
			// history: a thousand bytes against a 256-byte bound.
			Name: "output past the bound is truncated and said so",
			Workflow: execWorkflow("exec-output-bound",
				execStep("run", shArgv(`i=0; while [ $i -lt 100 ]; do printf aaaaaaaaaa; i=$((i+1)); done`), root, nil)),
			Check: func(tb testing.TB, out *v1.Workflow_StepOutputs) {
				stdout, _ := execField(tb, out, "run", "stdout").(string)
				require.Len(tb, stdout, ExecMaxOutputBytes)
				require.Equal(tb, true, execField(tb, out, "run", "stdout_truncated"))
				require.Equal(tb, false, execField(tb, out, "run", "stderr_truncated"))
			},
		},
		{
			// The time bound ends the program and the step fails with the limit
			// kind: permanent, because the same program would run as long again.
			Name:     "the policy's time bound ends the program",
			Timeout:  500 * time.Millisecond,
			Workflow: execWorkflow("exec-timeout", execStep("run", shArgv(`sleep 30`), root, nil)),

			ExpectedKind:  v1.ErrorKindLimitExceeded,
			ExpectedError: denied("outcome=timed_out"),
		},
		{
			Name:  "an allow rule keyed on the run's identity permits its own tenant",
			Allow: []string{`identity.namespace == "team-a" && name == "sh"`},
			Workflow: execWorkflow("exec-identity-allow",
				execStep("run", shArgv(`printf ok`), root, nil)),
			Identity: teamA,
			Check: func(tb testing.TB, out *v1.Workflow_StepOutputs) {
				require.Equal(tb, "ok", execField(tb, out, "run", "stdout"))
			},
		},
		{
			// The negative direction, which makes the case above a test of a
			// boundary rather than of a rule that says yes. The denial names the
			// rule category, and no identity claim values.
			Name:  "the same allow rule refuses another tenant",
			Allow: []string{`identity.namespace == "team-a" && name == "sh"`},
			Workflow: execWorkflow("exec-identity-allow-other",
				execStep("run", shArgv(`printf no`), root, nil)),
			Identity:      teamB,
			ExpectedKind:  v1.ErrorKindPolicyDenied,
			ExpectedError: denied("allow rules", "no allow rule matched"),
		},
		{
			Name:  "a run with no identity matches no tenant rule",
			Allow: []string{`identity.namespace == "team-a"`},
			Workflow: execWorkflow("exec-identity-absent",
				execStep("run", shArgv(`printf no`), root, nil)),
			ExpectedKind:  v1.ErrorKindPolicyDenied,
			ExpectedError: denied("allow rules"),
		},
		{
			Name: "a deny rule over argv refuses the invocation",
			Deny: []string{`argv.exists(a, a.contains("rm -rf"))`},
			Workflow: execWorkflow("exec-deny-rule",
				execStep("run", shArgv(`rm -rf /`), root, nil)),
			ExpectedKind:  v1.ErrorKindPolicyDenied,
			ExpectedError: denied("deny rule", "rm -rf"),
		},
		{
			// A rule that cannot be evaluated denies. argv[9] does not exist, and
			// the checker cannot know.
			Name: "a rule that cannot be evaluated denies",
			Deny: []string{`argv[9] == "x"`},
			Workflow: execWorkflow("exec-rule-error",
				execStep("run", shArgv(`true`), root, nil)),
			ExpectedKind:  v1.ErrorKindPolicyDenied,
			ExpectedError: denied("rule error"),
		},
	}
}

// ExecFailureMentions reports whether a failure's text carries every fragment a
// case expects, case-insensitively and across Temporal's line wrapping.
func ExecFailureMentions(text string, fragments []string) (missing string, ok bool) {
	flat := strings.Join(strings.Fields(strings.ToLower(text)), " ")
	for _, fragment := range fragments {
		if !strings.Contains(flat, strings.Join(strings.Fields(strings.ToLower(fragment)), " ")) {
			return fragment, false
		}
	}

	return "", true
}
