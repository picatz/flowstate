package flowstatev1_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// exampleSecretProvider always resolves to "example-token", whatever name was
// asked for, under the scheme it is registered for.
//
// One provider per scheme rather than one provider answering several: [secrets.
// Provider] handles exactly one scheme, which is also the shape the CLI wiring
// takes in cmd/flow/secrets.go — a distinct provider per backend, registered
// against distinct schemes. vault-secret, keychain-secret, onepassword-secret and
// command-secret each need their own scheme resolvable here for the same reason
// http-secret's env: does: this corpus exercises the engine's own machinery
// end to end, and a step that fails to resolve its secret never reaches the http
// task that machinery is supposed to be exercising.
type exampleSecretProvider struct{ scheme string }

func (p exampleSecretProvider) Scheme() string { return p.scheme }
func (exampleSecretProvider) Resolve(_ context.Context, req secrets.Request) (secrets.Secret, error) {
	return secrets.NewSecret(req.Ref, "example-token"), nil
}

// exampleSecretSchemes are the schemes every example in the corpus may resolve
// against, each answering the same fixture value. Kept alongside the local-driver
// harness because the durable-driver harness (examples_durable_test.go) builds the
// identical set, and a scheme reachable on one driver and not the other is exactly
// the kind of disagreement this corpus exists to catch.
var exampleSecretSchemes = []string{"env", "vault", "keychain", "op", "command"}

// exampleSecretProviders builds one [exampleSecretProvider] per scheme in
// [exampleSecretSchemes].
func exampleSecretProviders() []secrets.Provider {
	providers := make([]secrets.Provider, len(exampleSecretSchemes))
	for i, scheme := range exampleSecretSchemes {
		providers[i] = exampleSecretProvider{scheme: scheme}
	}
	return providers
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

		if conformance.ReachesTheNetwork(wf.GetSteps()) {
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
		gateLapses := conformance.LapsesWithin(wf.GetSteps(), unattendedGateBudget)
		if conformance.WaitsForASignal(wf.GetSteps()) && !gateLapses {
			continue
		}

		// Bound once, here, from the example's own inputs.json — the file
		// `flow run local --input-file` takes, and the only answer either harness
		// has to what an example requires. An example this cannot start fails
		// naming that file rather than being skipped.
		inputs, err := conformance.BindExampleInputs(t, wf, path)
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
// and fails outright with no egress. See [conformance.PointAtStandIn] for why that is a
// refusal and not a fallback.
func TestEveryNetworkedExampleRuns(t *testing.T) {
	// Not parallel: the loopback exemption swaps a process-global registry entry
	// and restores it on cleanup, so two top-level tests holding one at once would
	// have the first one's restore land while the second still runs. Subtests may
	// still be parallel — cleanup waits for them.
	base, unserved := conformance.NewExamplesHTTPServer(t)
	secretStore, err := secrets.NewStore(exampleSecretProviders()...)
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

		if !conformance.ReachesTheNetwork(wf.GetSteps()) {
			continue
		}
		if conformance.WaitsForASignal(wf.GetSteps()) {
			continue
		}

		inputs, err := conformance.BindExampleInputs(t, wf, path)
		require.NoError(t, err, "%s cannot be started", name)

		ran++

		// Every request has to be pointed somewhere this test controls before the
		// example runs. A step it cannot point is not a step to run anyway: it would
		// reach the real host, which makes the suite depend on somebody else's
		// service being up and fails outright on a machine with no egress.
		require.Empty(t, conformance.PointAtStandIn(wf.GetSteps(), base),
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
			// cannot disagree about which — see [conformance.ExampleFailure].
			//
			// What is asserted is that it failed *and* what the failure says, because
			// "it failed" is satisfied by an example that broke for some other reason
			// entirely, which is exactly how this kind of entry rots.
			if want, fails := conformance.ExampleFailure(name); fails {
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

		// An example's default arguments are one path through it; a variant is
		// another, reached only by overriding an input the defaults leave alone
		// — order-fulfillment's compensation is the case this exists for, since
		// nothing about the default run ever fails. See [conformance.ExampleVariants].
		for _, variant := range conformance.ExampleVariants(name) {
			variantInputs := variant.WithOverrides(inputs)

			// Cloned rather than shared with the primary run above: both run in
			// parallel, and a spec is something an executor reads from while it
			// runs — two goroutines doing that over one pointer is exactly the
			// race `-race` exists to catch, the same reason the durable harness
			// clones per run below.
			variantSpec, ok := proto.Clone(wf).(*v1.Workflow)
			require.True(t, ok)

			t.Run(name+"/"+variant.Name, func(t *testing.T) {
				t.Parallel()

				ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
				defer cancel()
				ctx = v1.ContextWithTaskRuntime(ctx, v1.TaskRuntime{
					Store: secretStore, Policy: secretPolicy, Broker: broker,
					Identity: auth.WorkloadIdentity{Subject: "examples", Issuer: "flowstate:test"},
					Step:     auth.StepRef{Workflow: wf.GetName(), Run: "example-run-" + variant.Name},
				})

				_, err := v1.RunWithInputs(ctx, variantSpec, variantInputs)

				if variant.Fails != "" {
					require.Error(t, err, "%s/%s is meant to fail and did not", name, variant.Name)
					require.Contains(t, err.Error(), variant.Fails,
						"%s/%s failed, but not in the way it exists to demonstrate", name, variant.Name)

					return
				}

				require.NoError(t, err, "%s/%s validates but does not run", name, variant.Name)
			})
		}
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

// TestExampleWorkflowNamesMatchDirectoryAndAreUnique guards the corpus against the
// shape #231 found: five examples sharing `name: authenticated-health-check`, two
// sharing `name: simple`, and ten directories whose own `name:` disagreed with the
// directory holding them — none of it caught by anything, because nothing checked
// it.
//
// The field is not decorative. #231 records it in the run memo and projects it
// into Temporal visibility, which makes it the value a production operator
// searches by — so a corpus where that value collides between five unrelated
// workflows, or silently disagrees with the path an author would `flow run`, is
// teaching that the field does not matter. It does, and this is what keeps it
// fixed: a directory's `workflow.yaml` names itself after the directory, and no
// two directories may name themselves the same thing (which they cannot, since a
// directory name is already unique — so the second check is really "no example
// was left off the first one").
func TestExampleWorkflowNamesMatchDirectoryAndAreUnique(t *testing.T) {
	t.Parallel()

	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "examples", "*", "workflow.yaml"))
	require.NoError(t, err)
	require.NotEmpty(t, paths, "no examples found; the glob is wrong")

	seen := make(map[string]string, len(paths)) // workflow name -> directory that claimed it first
	for _, path := range paths {
		dir := filepath.Base(filepath.Dir(path))

		wf, _, err := flowfile.ParseFile(path)
		require.NoError(t, err, "%s does not compile", dir)

		name := wf.GetName()
		assert.Equal(t, dir, name,
			"examples/%s/workflow.yaml declares name %q, which does not match its own directory — "+
				"a production operator searching Temporal visibility by directory name would not find this run", dir, name)

		if owner, ok := seen[name]; ok {
			t.Errorf("examples/%s/workflow.yaml and examples/%s/workflow.yaml both declare name %q; "+
				"a run started from either is indistinguishable from the other in Temporal visibility", owner, dir, name)
			continue
		}
		seen[name] = dir
	}
}

// examplesWithoutTestFile is the allowlist [TestEveryExampleHasATestFile] reads:
// example directories permitted to have no `workflow.test.yaml`.
//
// It is empty, and that is the finding rather than an oversight. #263 nominated
// two candidates and neither survived being looked at:
//
//   - `observability` is described as a lab whose subject is telemetry rather
//     than the workload — but the file's own first sentence is that nothing in it
//     is about telemetry, which is the point of it. The workload is an ordinary
//     workload (a durable wait, a fan-out over three regions, a value carried
//     forward) and is asserted as one. What a test cannot reach is the collector
//     and the dashboards around it, and none of that is in workflow.yaml.
//   - `edition-and-descriptions` is described as executing almost nothing. Its
//     two subjects, `edition:` and `description:`, are indeed inert — but the file
//     carries a `sleep:`, a `for_each` over a roster, and a report counting it,
//     and the sentence its own `description:` writes about the loop ("one at a
//     time, in roster order") is a claim nothing else enforces.
//
// An entry here would be a decision with a reason, never a gap: it must name why
// a run of the example can assert nothing, not merely that writing the case was
// awkward. "The interesting behavior is above the stub boundary" is not one —
// examples/http-expect and examples/task-shape-policy both have that problem and
// both have test files that say so in their own headers and pin what is left.
var examplesWithoutTestFile = map[string]string{}

// TestEveryExampleHasATestFile keeps the corpus from drifting back to the shape
// #263 records: 34 of 45 example directories with no `workflow.test.yaml`, so
// `flow test` — the capability that makes a library of reusable workflows
// maintainable, and half of what `call:` is for — was demonstrated by under a
// quarter of the files that are supposed to be the product demo.
//
// This is the same shape as [TestExampleWorkflowNamesMatchDirectoryAndAreUnique]
// above and exists for the same reason: the sweep that fixed it once is a
// one-time tidy unless something checks. CLAUDE.md's rule is that a capability is
// not done until an example exercises it in CI, *because that is what keeps it
// honest* — pointed at the corpus itself, an example with no test file is an
// example checked only for the thing it happens to do on the happy path.
//
// What this cannot check is the bar #260 sets, which is the part that actually
// matters: a case whose `expect:` is only `ran: [...]` asserts that a run
// happened and nothing about what it produced. No test can tell a case that pins
// behavior from one that pins membership, so that stays a review question. This
// checks the floor.
func TestEveryExampleHasATestFile(t *testing.T) {
	t.Parallel()

	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "examples", "*", "workflow.yaml"))
	require.NoError(t, err)
	require.NotEmpty(t, paths, "no examples found; the glob is wrong")

	seen := make(map[string]bool, len(examplesWithoutTestFile))
	for _, path := range paths {
		dir := filepath.Base(filepath.Dir(path))

		if reason, ok := examplesWithoutTestFile[dir]; ok {
			seen[dir] = true
			assert.NotEmpty(t, reason,
				"examples/%s is allowlisted with no reason; an entry must be a decision, not a gap", dir)

			continue
		}

		testFile := filepath.Join(filepath.Dir(path), "workflow.test.yaml")
		_, err := os.Stat(testFile)
		assert.NoError(t, err,
			"examples/%s/workflow.yaml has no sibling workflow.test.yaml — write one asserting what the "+
				"example exists to teach (not that it ran), or add examples/%s to examplesWithoutTestFile "+
				"with the reason a run of it can assert nothing", dir, dir)
	}

	// An allowlist entry for a directory that no longer exists is a decision
	// about nothing, and would otherwise sit here outliving whatever it was for.
	for dir := range examplesWithoutTestFile {
		assert.True(t, seen[dir],
			"examples/%s is allowlisted but has no workflow.yaml; remove the entry", dir)
	}
}
