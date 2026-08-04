package flowstatev1_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
)

type exampleSecretProvider struct{}

func (exampleSecretProvider) Scheme() string { return "env" }
func (exampleSecretProvider) Resolve(_ context.Context, req secrets.Request) (secrets.Secret, error) {
	return secrets.NewSecret(req.Ref, "example-token"), nil
}

type exampleExchanger struct{}

func (exampleExchanger) Name() string { return "example-sts" }
func (exampleExchanger) Requirement() auth.Requirement {
	return auth.Requirement{Audience: "https://api.example.com"}
}
func (exampleExchanger) Exchange(context.Context, auth.Assertion) (auth.Credential, error) {
	return auth.NewCredential(auth.CredentialBearer, time.Now().Add(time.Minute),
		map[string]string{auth.CredentialAccessToken: "example-jit-token"})
}

func exampleBroker(t *testing.T) *auth.Broker {
	t.Helper()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	key, err := auth.NewSigningKey("example", private)
	require.NoError(t, err)
	issuer, err := auth.NewIssuer("https://flowstate.example", key)
	require.NoError(t, err)
	broker, err := auth.NewBroker(issuer,
		auth.WithTarget("partner-api", exampleExchanger{}), auth.WithAssumeAllowRules("true"))
	require.NoError(t, err)
	return broker
}

// CLAUDE.md says a capability is not done until an example exercises it, "those
// run in CI, which is what keeps them honest". They did not run. Every test over
// `examples/` compiled or validated them, and the difference is not academic: an
// example shipped here that `flow validate` called ok and `flow run local`
// refused on its first step, because `expect:` was written as a mapping where the
// http task wants an expression.
//
// It got through because `expect` is a *deferred* input — evaluated by the task
// against a scope the validator cannot see, so the validator correctly declines
// to judge it. Every deferred input has that shape. Validation cannot close this;
// only running can.
//
// So this runs them. It is the cheap half of what the rule already claimed.

// unattendedGateBudget is how long this harness will sit through a gate nobody
// answers.
//
// Comfortably inside the per-example bound below, and deliberately much shorter than
// `approval-gate`'s day: an example meant to be watched lapsing says so in seconds,
// and one that would take longer is an example to answer rather than to wait out.
const unattendedGateBudget = 30 * time.Second

// TestEveryOfflineExampleRuns executes each example that needs no network.
//
// Which ones those are is derived rather than listed: a workflow reaching the
// network is one with an `http` step somewhere in it, and asking the compiled
// workflow means a new example is covered the day it is written, without anyone
// remembering to add it here.
func TestEveryOfflineExampleRuns(t *testing.T) {
	t.Parallel()

	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "examples", "*", "workflow.yaml"))
	require.NoError(t, err)
	require.NotEmpty(t, paths, "no examples found; the glob is wrong")

	var ran, lapsed int
	for _, path := range paths {
		name := filepath.Base(filepath.Dir(path))

		wf, _, err := flowfile.ParseFile(path)
		require.NoError(t, err, "%s does not compile", name)

		if tests.ReachesTheNetwork(wf.GetSteps()) {
			continue
		}

		// A gate is answered from outside the workload, which is the point of it.
		// The local driver takes an answer through `--signal`; a run without one is
		// refused, correctly, and that refusal is not something to assert here.
		//
		// A gate with a `timeout:` is the other case, and it is what `wait-timeout`
		// is about: nothing has to be sent, because the deadline passing *is* the
		// outcome — the wait reports `timed_out` and the run carries on down the
		// branch the file wrote for that. So those do run here, unattended, which is
		// the only way this corpus covers a wait at all on this driver.
		gateLapses := tests.LapsesWithin(wf.GetSteps(), unattendedGateBudget)
		if tests.WaitsForASignal(wf.GetSteps()) && !gateLapses {
			continue
		}

		// Bound once, here, from the example's own inputs.json — the file
		// `flow run local --input-file` takes, and the only answer either harness
		// has to what an example requires. An example this cannot start fails
		// naming that file rather than being skipped.
		inputs, err := tests.BindExampleInputs(t, wf, path)
		require.NoError(t, err, "%s cannot be started", name)

		ran++
		if gateLapses {
			lapsed++
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// Bounded, because an example that waits is an example that could hang
			// this suite, and a test whose failure mode is "CI times out in ten
			// minutes" is worse than one that says which example stopped.
			ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
			defer cancel()

			if gateLapses {
				// A waiter that is never given anything, which is the point rather
				// than an omission: the local driver refuses a run that waits with
				// nothing able to deliver to it at all ([v1.ErrNoSignalWaiter]), and
				// that refusal is about there being no channel rather than about
				// nothing arriving on one. What the example demonstrates is the
				// second, so it needs the first to exist.
				ctx = v1.NewContextWithSignalWaiter(ctx, v1.NewLocalSignals())
			}

			outputs, err := v1.RunWithInputs(ctx, wf, inputs)
			require.NoError(t, err, "%s validates but does not run", name)
			require.NotNil(t, outputs)

			// The run reached the end, rather than "succeeding" having done almost
			// nothing — which the error above would not catch.
			//
			// Asserted over unconditional top-level task steps only, and each
			// exclusion is a real rule rather than a way to make this pass. A step
			// behind an `if:` that is false is *meant* to produce nothing. A
			// `parallel:` reports through its branches, whose outputs merge into the
			// enclosing scope under their own ids and not the block's. A loop reports
			// through `results`.
			for _, step := range wf.GetSteps() {
				if step.GetTask() == nil || step.GetCondition() != nil {
					continue
				}
				assert.Contains(t, outputs.GetStepValues(), step.GetId(),
					"step %q produced no outputs", step.GetId())
			}
		})
	}

	// The count is asserted so that a change making every example look like it
	// needs the network — a rename, a broken predicate — fails here rather than
	// silently running nothing and reporting success.
	assert.GreaterOrEqual(t, ran, 8,
		"expected most examples to be runnable offline; only %d were, which suggests the network check is wrong", ran)

	// And the same for the half of the wait story this driver can reach. A gate that
	// lapses is the only kind of wait runnable with nothing to answer it, so if this
	// reaches zero the local corpus has stopped exercising waits entirely — which it
	// would do silently, since every other example would still pass.
	assert.Positive(t, lapsed,
		"no example exercised a gate that lapses; `wait-timeout` is the one that does, and the "+
			"local driver's only unattended path through a wait_for_signal step")
}

// TestEveryNetworkedExampleRuns runs the other half — the ones skipped above.
//
// Skipping them was not a small gap. Eight of the nineteen examples reach the
// network, and they are precisely the ones demonstrating the http task: `query`,
// `json`, `form`, `expect`, `outputs` shaping, and reading a response back out of
// `steps.<id>.json`. Everything the offline test was written to catch lived in the
// half it could not see.
//
// And one of them was broken the whole time. `http-query-and-json` ended with
// `${string(steps.search.json.args)}`, which cannot evaluate — `string()` has no
// overload for a map, and `fields:` is string-valued deliberately — so its last
// step failed on every run it ever had. It was found by counting CEL evaluations
// for something else entirely, which is not a way to find bugs.
//
// The examples name `httpbin.org`, and they should: somebody reading one wants to
// paste it and watch it work. So the run is pointed at a stand-in instead of the
// example being rewritten. What that proves is the whole of what the example
// controls — its inputs, its expressions, and what its steps do with a response of
// that shape. What it does not prove is that httpbin is up, or that its shape is
// still what the bodies below were copied from. That second one is the real limit
// and it is why they are copies of a recorded response rather than something
// invented to make the assertions pass.
//
// Nothing runs until every one of its requests is pointed at the stand-in. An
// example this cannot point is refused rather than run, because running it would
// reach the real host — which makes the suite depend on somebody else's service
// and fails outright with no egress. See [tests.PointAtStandIn] for why that is a
// refusal and not a fallback.
func TestEveryNetworkedExampleRuns(t *testing.T) {
	// Not parallel: the loopback exemption swaps a process-global registry entry
	// and restores it on cleanup, so two top-level tests holding one at once would
	// have the first one's restore land while the second still runs. Subtests may
	// still be parallel — cleanup waits for them.
	base, unserved := tests.NewExamplesHTTPServer(t)
	secretStore, err := secrets.NewStore(exampleSecretProvider{})
	require.NoError(t, err)
	secretPolicy, err := (auth.SecretAccessPolicy{Allow: []string{"true"}}).Compile()
	require.NoError(t, err)
	broker := exampleBroker(t)

	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "examples", "*", "workflow.yaml"))
	require.NoError(t, err)
	require.NotEmpty(t, paths, "no examples found; the glob is wrong")

	var ran int
	for _, path := range paths {
		name := filepath.Base(filepath.Dir(path))

		wf, _, err := flowfile.ParseFile(path)
		require.NoError(t, err, "%s does not compile", name)

		if !tests.ReachesTheNetwork(wf.GetSteps()) {
			continue
		}
		if tests.WaitsForASignal(wf.GetSteps()) {
			continue
		}

		inputs, err := tests.BindExampleInputs(t, wf, path)
		require.NoError(t, err, "%s cannot be started", name)

		ran++

		// Every request has to be pointed somewhere this test controls before the
		// example runs. A step it cannot point is not a step to run anyway: it would
		// reach the real host, which makes the suite depend on somebody else's
		// service being up and fails outright on a machine with no egress.
		require.Empty(t, tests.PointAtStandIn(wf.GetSteps(), base),
			"%s has an http step this test cannot point at the stand-in, so running it would "+
				"reach the real host; give the step a literal url, or teach PointAtStandIn the shape it uses", name)

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
			defer cancel()
			ctx = v1.ContextWithTaskRuntime(ctx, v1.TaskRuntime{
				Store: secretStore, Policy: secretPolicy, Broker: broker,
				Identity: auth.WorkloadIdentity{Subject: "examples", Issuer: "flowstate:test"},
				Step:     auth.StepRef{Workflow: wf.GetName(), Run: "example-run"},
			})

			outputs, err := v1.RunWithInputs(ctx, wf, inputs)

			// An example whose point is a run that does not succeed. One of them,
			// and the classification is shared with the durable harness so the two
			// cannot disagree about which — see [tests.ExampleFailure].
			//
			// What is asserted is that it failed *and* what the failure says, because
			// "it failed" is satisfied by an example that broke for some other reason
			// entirely, which is exactly how this kind of entry rots.
			if want, fails := tests.ExampleFailure(name); fails {
				require.Error(t, err, "%s is meant to fail and did not", name)
				require.Contains(t, err.Error(), want,
					"%s failed, but not in the way it exists to demonstrate", name)

				return
			}

			require.NoError(t, err, "%s validates but does not run", name)
			require.NotNil(t, outputs)

			for _, step := range wf.GetSteps() {
				if step.GetTask() == nil || step.GetCondition() != nil {
					continue
				}
				assert.Contains(t, outputs.GetStepValues(), step.GetId(),
					"step %q produced no outputs", step.GetId())
			}
		})
	}

	// Same reason the offline test counts: a predicate that stopped matching would
	// otherwise run nothing and report success.
	assert.GreaterOrEqual(t, ran, 7,
		"expected the networked examples to be found; only %d were, which suggests the network check is wrong", ran)

	// The stand-in answers the paths the examples ask for and nothing else, so an
	// example added against a path it does not serve fails here saying so — rather
	// than passing against a 404 its expressions never look at.
	t.Cleanup(func() {
		assert.Empty(t, unserved(), "the examples stand-in was asked for paths it does not serve")
	})
}
