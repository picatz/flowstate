package engine_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/protobuf/testing/protocmp"
)

// runAuthorityCase installs test's Authority the way the durable driver
// actually does it in production — [engine.NewTaskRuntimeConfig] passed to
// [engine.Register] at worker registration — and runs the case through
// [engine.Run] on a Temporal test environment.
//
// eval_test.go's runAuthorityCase runs the identical [conformance.AuthorityCase]
// through a context value instead. The two are deliberately the same
// assertions against a different installation path: #116 existed because
// nothing compared them, and secret denial text and a JIT credential's
// containment had each only ever been proven by one driver's own test file.
func runAuthorityCase(t *testing.T, test conformance.AuthorityCase) {
	t.Helper()

	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	if test.Authority.NoRuntime {
		engine.Register(env)
	} else {
		var store *secrets.Store
		var policy *auth.SecretPolicy
		if test.Authority.HasSecrets() {
			store = test.Authority.Store(t)
			policy = test.Authority.Policy(t)
		}
		runtime, err := engine.NewTaskRuntimeConfig(store, policy, test.Authority.Broker(t))
		require.NoError(t, err)
		engine.Register(env, runtime)
	}

	env.ExecuteWorkflow(engine.Run, &v1.RunState{
		Workflow: test.Workflow,
		Identity: test.Authority.ProtoIdentity(),
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var out v1.Workflow_StepOutputs
	require.NoError(t, env.GetWorkflowResult(&out))
	require.Empty(t, cmp.Diff(test.ExpectedOutputs, &out, protocmp.Transform()))

	if test.ContainmentValue != "" {
		conformance.AssertNoLeak(t, &out, test.ContainmentValue)
	}
}

// TestAuthorityDenial runs the shared fail-closed and policy-denial cases
// against the durable driver. The local driver runs the same cases in
// eval_test.go.
func TestAuthorityDenial(t *testing.T) {
	for _, test := range conformance.AuthorityDenialCases() {
		t.Run(test.Name, func(t *testing.T) {
			runAuthorityCase(t, test)
			if test.Authority.ProviderCalls != nil {
				require.Zero(t, test.Authority.ProviderCalls.Load(),
					"the fixture provider resolved a reference the policy should have denied first")
			}
		})
	}
}

// TestAuthorityContainment runs the shared secret and JIT credential
// containment cases against the durable driver. The local driver runs the
// same cases in eval_test.go.
func TestAuthorityContainment(t *testing.T) {
	baseURL := conformance.NewHTTPServer(t)
	for _, test := range conformance.AuthorityContainmentCases(baseURL) {
		t.Run(test.Name, func(t *testing.T) {
			runAuthorityCase(t, test)
		})
	}
}
