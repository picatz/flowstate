package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
)

// The examples corpus proves a capability is reachable from a file somebody writes,
// and until now it proved it of one driver. `examples_run_test.go` runs every
// example through the local driver, which executes a run start to finish in one
// process: it never continues as new, never compacts carryover, and never runs a
// loop's iterations on more than one goroutine's worth of Temporal machinery. So
// every durable-only path was outside the corpus, and two bugs this week lived
// exactly there — the compaction map-key bug, and output-declaration pruning. The
// blind spot is named in compactvars_internal_test.go:27: "the example passes CI
// because examples_run_test.go runs the local driver, which never continues as new".
//
// This runs the same files through the durable driver against a real dev server,
// and it does two things the local harness cannot:
//
//   - It forces a Continue-As-New boundary between *every* step (StepsBudget=1), so
//     an example with more than one step suspends and resumes at least once. That is
//     the entire point: compaction only decides wrongly when there is a handover for
//     it to decide at, and a bound nothing reaches is a bound nothing tests.
//   - It compares the answer against the local driver's answer for the same file.
//     Invariant 3 says local runs exist to tell an author what production will do, so
//     the honest check is not "the durable run succeeded" but "the durable run said
//     what the local run said".
//
// Both halves matter. A durable run that suspends four times and completes proves
// nothing on its own; a durable run whose outputs equal the local ones but never
// suspended proves nothing about compaction. The assertions below require both.

// exampleRunTimeout bounds a single example on either driver.
//
// Bounded because an example that waits is an example that could hang the suite, and
// a test whose failure mode is "CI times out in fifteen minutes" is worse than one
// that says which example stopped. Generous rather than tight: every step of a
// suspending run is a workflow task round trip against a real server.
const exampleRunTimeout = 3 * time.Minute

// exampleSignals answers the gates an example waits at.
//
// A gate is answered from outside the workload, which is the point of it, so an
// example that waits for a signal cannot run unattended on either driver without a
// harness supplying the answer. The local harness declines to run them at all
// (`examples_run_test.go` skips `waitsForASignal`); this one answers them, because
// a wait is the durable driver's flagship behavior and skipping it here would leave
// the whole of `approval-gate` — a sleep, a gate, and three conditional branches
// reading a sender's payload — outside every driver's example coverage.
//
// The payloads are the ones the examples themselves document, from the `flow signal`
// line in their own comments, rather than something invented to make an assertion
// pass. `flow run local --signal deploy-approved='{"approved": true, "by": ...}'` is
// the local spelling of the same answer.
//
// Missing an entry is a failure, not a skip: [TestEveryExampleRunsDurably] refuses
// an example that waits for a signal this table cannot answer, so a new gate example
// arrives as a red test naming what it needs rather than as silently dropped
// coverage.
var exampleSignals = map[string]map[string]*v1.Node_Outputs{
	"approval-gate": {
		"deploy-approved": {NamedValues: map[string]*v1.Value{
			"approved": v1.NewLiteral(true),
			"by":       v1.NewLiteral("someone@example.com"),
		}},
	},
}

// exampleDurableSkips names an example this harness genuinely cannot run durably,
// against the reason why.
//
// Deliberately empty. It exists so that a future skip has to be written down with a
// sentence justifying it, and so the count below fails when the map grows: silent
// shrinkage of coverage is the failure mode this whole file was written against, and
// a harness that quietly stopped running half the corpus would still be green.
var exampleDurableSkips = map[string]string{}

// TestEveryExampleRunsDurably runs every example through the durable driver, across
// Continue-As-New, and checks the answer against the local driver's.
func TestEveryExampleRunsDurably(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: starts a real Temporal dev server; CI runs the full suite")
	}

	// One stand-in and one dev server for the whole corpus. Starting either per
	// example would dominate the wall time — the server takes seconds to boot and
	// the examples take a second or two each — and neither has per-example state.
	base, unserved := tests.NewExamplesHTTPServer(t)

	// The worker capabilities two examples need. `http-secret` resolves
	// `${secret('env:API_TOKEN')}` and `http-federated` names a `credential:`
	// target, and on the durable driver both arrive through worker registration
	// rather than through a context value — which is the installation path this
	// harness should be exercising, since it is the one production uses.
	authority := tests.Authority{
		Scheme:       "env",
		FixtureValue: "example-token",
		Allow:        []string{"true"},
		Identity: auth.WorkloadIdentity{
			Subject: "examples",
			Issuer:  "flowstate:test",
		},
		Federation: &tests.Federation{Target: "partner-api", Token: "example-jit-token"},
	}
	runtime, err := engine.NewTaskRuntimeConfig(
		authority.Store(t), authority.Policy(t), authority.Broker(t))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	devServer, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ClientOptions: &client.Options{},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = devServer.Stop() })

	w := worker.New(devServer.Client(), engine.RunTaskQueueName, worker.Options{})
	engine.Register(w, runtime)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "..", "examples", "*", "workflow.yaml"))
	require.NoError(t, err)
	require.NotEmpty(t, paths, "no examples found; the glob is wrong")

	// Recorded per example and reported together at the end, so one run tells the
	// whole story: which examples suspended and how often. A count of zero on a
	// multi-step example is a failure inside the subtest — it means the budget seam
	// stopped forcing a handover and this harness silently became a slower copy of
	// the local one.
	var (
		mu       sync.Mutex
		crossed  = map[string]int{}
		ran      int
		skipped  []string
		answered int
	)

	for _, path := range paths {
		name := filepath.Base(filepath.Dir(path))

		data, err := os.ReadFile(path)
		require.NoError(t, err)

		wf, err := flowfile.Unmarshal(data)
		require.NoError(t, err, "%s does not compile", name)

		if reason, ok := exampleDurableSkips[name]; ok {
			skipped = append(skipped, name+": "+reason)

			continue
		}

		// Bound once, here, and handed to both drivers — which is also how a
		// deployment does it: the server checks and defaults a run's arguments at
		// submit and carries the result unchanged through every Continue-As-New
		// (workflow.go's comment on RunState.Inputs). Binding separately per driver
		// would let a default differ between them and call that a driver
		// disagreement.
		//
		// The arguments come from the example's own `inputs.json`, which is also
		// what the local harness reads and what `flow run local --input-file` takes.
		// They were a Go map here — one entry, `parameterized-deploy: service:
		// checkout` — which was a second answer to what an example needs, sitting
		// beside the file a reader actually runs. Two answers drift, and the one
		// nobody can reproduce is the harness's.
		inputs, err := tests.BindExampleInputs(t, wf, path)
		require.NoError(t, err,
			"%s cannot be started, so it is an example nobody can run", name)

		signals := exampleSignals[name]
		if tests.WaitsForASignal(wf.GetSteps()) {
			require.NotEmpty(t, signals,
				"%s waits for a signal and exampleSignals has no answer for it; add the payload the "+
					"example's own `flow signal` line documents, so the gate is exercised rather than skipped", name)
			answered++
		}

		// Every request is pointed at the stand-in before the example runs, on both
		// drivers. A step this cannot point is not a step to run: it would reach the
		// real host, which makes the suite depend on somebody else's service being up
		// and fails outright on a machine with no egress.
		if tests.ReachesTheNetwork(wf.GetSteps()) {
			require.Empty(t, tests.PointAtStandIn(wf.GetSteps(), base),
				"%s has an http step this test cannot point at the stand-in, so running it would reach "+
					"the real host; give the step a literal url, or teach PointAtStandIn the shape it uses", name)
		}

		ran++

		t.Run(name, func(t *testing.T) {
			// Parallel, and the dev server supports it: each example is its own
			// workflow id in one namespace, and the stand-in is stateless apart from
			// its unserved log, which is mutex-guarded. Sequentially this test is the
			// sum of every example's wall clock, and several of them sleep on purpose.
			t.Parallel()

			// One spec per driver. `wf` holds pointers the executors read while they
			// run, and the local run and the durable client both marshal from it —
			// cloning keeps a driver from observing anything the other did to it.
			// One spec per run. `wf` holds pointers an executor reads while it runs,
			// and each run marshals from its own copy, so nothing observes what
			// another did to it.
			wholeSpec := cloneSpec(t, wf)
			suspendingSpec := cloneSpec(t, wf)
			localSpec := cloneSpec(t, wf)

			local := runExampleLocally(t, localSpec, inputs, authority, signals)

			// Two durable runs of one file, because the two questions need different
			// budgets and answering both from one run would answer neither properly.
			//
			// The first runs the whole file in one segment, which is the run whose
			// outputs are directly comparable with the local driver's: nothing was
			// handed over, so nothing was compacted away, and any difference is a real
			// disagreement between the drivers rather than carryover trimming.
			whole, wholeCrossings := runExampleDurably(t,
				devServer.Client(), "example-whole-"+name, name, wholeSpec, inputs, signals, 0)
			assert.Zero(t, wholeCrossings,
				"%s continued as new on the default budget, so the comparison below is against a "+
					"compacted answer rather than a whole one", name)

			if diff := cmp.Diff(stableOutputs(local), stableOutputs(whole), protocmp.Transform()); diff != "" {
				t.Fatalf("%s answers differently on the two drivers, so a local run does not "+
					"tell an author what production will do (-local +durable):\n%s", name, diff)
			}

			// The second suspends between every step. Its outputs are deliberately a
			// subset — Continue-As-New carries forward only what the remaining steps
			// still need — so what is asserted is that everything it *did* carry says
			// what the local driver said, and that the run's declared outputs, which
			// are computed in the final segment out of that carryover, are identical.
			//
			// That second half is the one the local harness cannot ask at all: an
			// output declaration naming a step compaction dropped fails only after a
			// handover, which is how the output-declaration pruning bug survived
			// examples CI.
			suspended, crossings := runExampleDurably(t,
				devServer.Client(), "example-"+name, name, suspendingSpec, inputs, signals, 1)

			mu.Lock()
			crossed[name] = crossings
			mu.Unlock()

			// The point of the budget override. An example of one step has nothing to
			// hand over and is not asserted; every other one must actually have
			// suspended, or this harness proved only what the local one already did.
			if len(suspendingSpec.GetSteps()) > 1 {
				assert.Positive(t, crossings,
					"%s never continued as new, so nothing here tested compaction or resumption; "+
						"the step budget seam (RunState.StepsBudget) is no longer forcing a handover", name)
			}

			assertSurvivedCompaction(t, name, stableOutputs(local), stableOutputs(suspended))

			// And the whole-file run reached the end, rather than "succeeding" having
			// done almost nothing — which agreement alone would not catch, since two
			// drivers can agree on having done nothing.
			//
			// Asserted over unconditional top-level task steps only, and each exclusion
			// is a real rule rather than a way to make this pass: a step behind a false
			// `if:` is meant to produce nothing, a `parallel:` reports through its
			// branches under their own ids, and a loop reports through `results`.
			for _, step := range wholeSpec.GetSteps() {
				if step.GetTask() == nil || step.GetCondition() != nil {
					continue
				}
				assert.Contains(t, whole.GetStepValues(), step.GetId(),
					"step %q produced no outputs", step.GetId())
			}
		})
	}

	// A predicate that stopped matching, a rename, or a glob that went stale would
	// otherwise run nothing and report success.
	assert.GreaterOrEqual(t, ran, 18,
		"expected the whole corpus to run durably; only %d examples did, which suggests the "+
			"enumeration is wrong or examples are being skipped silently", ran)
	assert.Empty(t, skipped,
		"examples were skipped durably; each is a hole in example-level coverage of the driver "+
			"that actually runs in production: %v", skipped)
	assert.Positive(t, answered,
		"no example exercised a signal gate durably; `approval-gate` is the one example about "+
			"waiting, and a corpus that stopped covering it would still be green here")

	t.Cleanup(func() {
		assert.Empty(t, unserved(), "the examples stand-in was asked for paths it does not serve")

		mu.Lock()
		defer mu.Unlock()

		names := make([]string, 0, len(crossed))
		for name := range crossed {
			names = append(names, name)
		}
		sort.Strings(names)

		var report strings.Builder
		for _, name := range names {
			report.WriteString("\n\t" + name + ": " + strconv.Itoa(crossed[name]))
		}
		t.Logf("Continue-As-New crossings per example (StepsBudget=1):%s", report.String())
	})
}

// runExampleDurably executes one example on the dev server and returns its outputs
// and how many times it continued as new.
// A budget of one suspends between every step; a budget of zero leaves the engine's
// own default (200), which no example comes near. The seam is the one
// `server.WithMaxStepsPerRun` configures in a deployment — the server writes it into
// `RunState.StepsBudget` at submit (server.go:346) — and the harness sets the field
// directly because it submits the state itself rather than going through the service.
func runExampleDurably(
	t *testing.T,
	c client.Client,
	id, name string,
	spec *v1.Workflow,
	inputs map[string]*v1.Value,
	signals map[string]*v1.Node_Outputs,
	budget int32,
) (*v1.Workflow_StepOutputs, int) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), exampleRunTimeout)
	defer cancel()

	run, err := c.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{ID: id, TaskQueue: engine.RunTaskQueueName},
		engine.Run,
		&v1.RunState{
			Workflow:    spec,
			Inputs:      inputs,
			StepsBudget: budget,
			Identity: (&tests.Authority{
				Identity: auth.WorkloadIdentity{Subject: "examples", Issuer: "flowstate:test"},
			}).ProtoIdentity(),
		})
	require.NoError(t, err)

	// Read before the run is waited on, and that ordering is load-bearing: the SDK's
	// WorkflowRun follows the Continue-As-New chain while Get blocks, so afterwards
	// this answers with the *last* segment's id — whose history holds no handover at
	// all, and the crossing count came back zero for every example until this moved.
	firstRunID := run.GetRunID()

	// Sent immediately, before the run has reached its gate — which is the ordinary
	// case for an approval, and the interesting one here: a signal that arrives
	// early is buffered by Temporal, drained into `RunState.PendingSignals` when the
	// run suspends, and consumed by the wait in a later segment. With a budget of one
	// step, an `approval-gate` run crosses several boundaries before it reaches the
	// gate, so this exercises the carry rather than a channel receive.
	for signal, payload := range signals {
		require.NoError(t, c.SignalWorkflow(ctx, id, "", signal, payload),
			"signalling %s with %q", name, signal)
	}

	var outputs v1.Workflow_StepOutputs
	require.NoError(t, run.Get(ctx, &outputs), "%s validates but does not run durably", name)

	return &outputs, continueAsNewCrossings(t, c, id, firstRunID)
}

// cloneSpec copies a specification so one run cannot observe what another did to it.
func cloneSpec(t *testing.T, spec *v1.Workflow) *v1.Workflow {
	t.Helper()

	clone, ok := proto.Clone(spec).(*v1.Workflow)
	require.True(t, ok)

	return clone
}

// runExampleLocally executes the same example through the local driver.
//
// The same stand-in, the same fixture authority, and the same signal payloads: the
// comparison is only about the drivers if everything else is held equal, and a
// harness that pointed one driver at a different server would report a disagreement
// it caused itself.
func runExampleLocally(
	t *testing.T,
	spec *v1.Workflow,
	inputs map[string]*v1.Value,
	authority tests.Authority,
	signals map[string]*v1.Node_Outputs,
) *v1.Workflow_StepOutputs {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), exampleRunTimeout)
	defer cancel()

	ctx = v1.ContextWithTaskRuntime(ctx, v1.TaskRuntime{
		Store:    authority.Store(t),
		Policy:   authority.Policy(t),
		Broker:   authority.Broker(t),
		Identity: authority.Identity,
		Step:     auth.StepRef{Workflow: spec.GetName(), Run: "example-run"},
	})

	if len(signals) > 0 {
		waiter := v1.NewLocalSignals()
		for signal, payload := range signals {
			// Delivered before the run starts, for the reason the durable side signals
			// immediately: the local waiter buffers, so this is the local spelling of a
			// signal that arrived early.
			require.NoError(t, waiter.Deliver(signal, payload))
		}
		ctx = v1.NewContextWithSignalWaiter(ctx, waiter)
	}

	outputs, err := v1.RunWithInputs(ctx, spec, inputs)
	require.NoError(t, err, "%s validates but does not run locally", spec.GetName())

	return outputs
}

// assertSurvivedCompaction checks a suspending run against the local answer over the
// steps its carryover kept, and over the run's declared outputs in full.
//
// Written as a subset check rather than an equality one because trimming is the
// behavior, not a defect: Continue-As-New carries forward only what the remaining
// steps still name, and it trims at two levels — whole steps, and individual named
// values within a step that survived (`http-json`'s `web` arrives holding `body` and
// not `status_code`, because nothing left to run asks for the code). Asserting
// equality would fail on every multi-step example and teach nothing.
//
// What it does assert is the two things trimming must never change. A value that
// survived has to be the value the local driver produced — carrying a *wrong* one
// forward is the compaction failure that matters, and a check counting keys cannot
// see it. And `run_outputs` must be complete and identical, because those are
// evaluated in the final segment out of whatever the carryover kept: an output
// declaration naming something compaction dropped fails only after a handover, which
// is exactly how output-declaration pruning got past examples CI.
func assertSurvivedCompaction(t *testing.T, name string, local, suspended *v1.Workflow_StepOutputs) {
	t.Helper()

	for id, outputs := range suspended.GetStepValues() {
		want, ok := local.GetStepValues()[id]
		if !assert.True(t, ok,
			"%s: the suspending run reported step %q, which the local run did not report at all", name, id) {
			continue
		}

		for output, value := range outputs.GetNamedValues() {
			expected, ok := want.GetNamedValues()[output]
			if !assert.True(t, ok,
				"%s: step %q carried an output %q across Continue-As-New that the local run "+
					"never produced", name, id, output) {
				continue
			}

			if diff := cmp.Diff(expected, value, protocmp.Transform()); diff != "" {
				t.Errorf("%s: step %q carried a different %q across Continue-As-New than the local "+
					"run produced (-local +durable):\n%s", name, id, output, diff)
			}
		}
	}

	if diff := cmp.Diff(local.GetRunOutputs(), suspended.GetRunOutputs(), protocmp.Transform()); diff != "" {
		t.Errorf("%s: the run's declared outputs differ after suspending, so compaction dropped or "+
			"changed something an output declaration needs (-local +durable):\n%s", name, diff)
	}
}

// continueAsNewCrossings counts the handovers in one run's chain.
//
// Counted from history rather than inferred from the step count, because what is
// under test is precisely whether the engine still suspends where it is supposed to:
// a number derived from the specification would agree with itself no matter what the
// engine did. Each segment's history ends with a ContinuedAsNew event naming the next
// run id, so the chain is walked to its end.
func continueAsNewCrossings(t *testing.T, c client.Client, id, runID string) int {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	var crossings int
	for runID != "" {
		iter := c.GetWorkflowHistory(ctx, id, runID, false,
			enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)

		var next string
		for iter.HasNext() {
			event, err := iter.Next()
			require.NoError(t, err)

			if attrs := event.GetWorkflowExecutionContinuedAsNewEventAttributes(); attrs != nil {
				next = attrs.GetNewExecutionRunId()
			}
		}

		if next == "" {
			break
		}
		crossings++
		runID = next
	}

	return crossings
}

// stableOutputs returns a copy with the fields a particular execution owns removed,
// so the comparison is over what the file computes.
//
// The only such field today is an http step's `headers`, which carries the server's
// `Date` — a second's difference between two runs of the same file is not a driver
// disagreement. It is dropped whole rather than by key: `Content-Length` beside a
// `Date` would make the exclusion look narrower than it is, and an example asserting
// something about a header does so in its own `outputs:` expression, which this does
// not touch.
//
// Everything else is compared, including error text: a step tolerated by
// `continue_on_error` records what went wrong, and the two drivers wording that
// differently is exactly the class of disagreement invariant 3 is about
// (`marshalJSON` on one side and a bare `protojson.Marshal` on the other, per
// CLAUDE.md).
func stableOutputs(outputs *v1.Workflow_StepOutputs) *v1.Workflow_StepOutputs {
	clone, ok := proto.Clone(outputs).(*v1.Workflow_StepOutputs)
	if !ok {
		return outputs
	}

	for _, step := range clone.GetStepValues() {
		delete(step.GetNamedValues(), "headers")

		for _, value := range step.GetNamedValues() {
			sortMapEntries(value.GetLiteral())
		}
	}

	for _, value := range clone.GetRunOutputs().GetValues() {
		sortMapEntries(value.GetLiteral())
	}

	return clone
}

// sortMapEntries puts a literal's map entries in key order, in place and at every
// depth.
//
// A CEL map literal is a *list* of entries on the wire, so its order is part of the
// message even though it is not part of the value. A parsed JSON response becomes one
// of these by ranging over a Go map, and Go randomizes that — so two runs of the same
// file produce the same document in a different order, and a proto comparison calls
// that a driver disagreement. It is not one: nothing an expression can ask sees the
// order, `steps.x.json['a']` is a lookup.
//
// Sorted rather than compared with an order-insensitive matcher because the entries
// are a plain repeated field, and there is no key for a matcher to pair them by.
// Where order *is* load-bearing — a signal payload carried across Continue-As-New —
// the engine already sorts it deliberately (see [v1.SignalOutputs]), so this cannot
// hide a difference in the one place order is meant to be stable.
func sortMapEntries(value *expr.Value) {
	switch kind := value.GetKind().(type) {
	case *expr.Value_MapValue:
		entries := kind.MapValue.GetEntries()
		for _, entry := range entries {
			sortMapEntries(entry.GetValue())
		}
		sort.SliceStable(entries, func(i, j int) bool {
			return mapKeyOrder(entries[i].GetKey()) < mapKeyOrder(entries[j].GetKey())
		})

	case *expr.Value_ListValue:
		for _, element := range kind.ListValue.GetValues() {
			sortMapEntries(element)
		}
	}
}

// mapKeyOrder renders a map key as something two documents can be sorted by.
//
// Built from the key's own scalar rather than from its `String()`, because the
// protobuf runtime deliberately perturbs that rendering — so a comparator reading it
// could order one document's keys differently from the other's and manufacture the
// difference this sorting exists to remove.
func mapKeyOrder(key *expr.Value) string {
	switch kind := key.GetKind().(type) {
	case *expr.Value_StringValue:
		return "s:" + kind.StringValue
	case *expr.Value_Int64Value:
		return "i:" + strconv.FormatInt(kind.Int64Value, 10)
	case *expr.Value_Uint64Value:
		return "u:" + strconv.FormatUint(kind.Uint64Value, 10)
	case *expr.Value_BoolValue:
		return "b:" + strconv.FormatBool(kind.BoolValue)
	default:
		// CEL permits nothing else as a map key, so this is unreachable for a value
		// a run produced. Named rather than panicked on: a harness that dies on an
		// unexpected shape says less than one that sorts it consistently and lets the
		// comparison report what actually differs.
		return "?"
	}
}

// TestAnExampleWithNoInputsFileIsRefusedRatherThanCapped pins the failure that keeps
// the corpus honest about its own arguments.
//
// Both harnesses read what an example requires from an `inputs.json` beside its
// `workflow.yaml`. That answer used to exist twice — this file also carried a Go map
// naming `parameterized-deploy`'s `service` — and the map was the answer no reader
// could reproduce: a run of the example from the command line takes the file.
//
// With one answer, the way it now fails matters. An example declaring a required
// input with no file beside it must fail *naming the file*, because the two silent
// outcomes are both worse than a red test: skipping the example drops it out of
// coverage, and inventing a value tests the harness rather than the example. So this
// asserts the sentence, not merely the error — a harness that failed with "input
// "service" is required" alone would leave an author with nowhere to write the
// answer.
func TestAnExampleWithNoInputsFileIsRefusedRatherThanCapped(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", "..", "..", "examples", "parameterized-deploy", "workflow.yaml")

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	wf, err := flowfile.Unmarshal(data)
	require.NoError(t, err)

	// The example as it ships: the file beside it answers what it declares.
	bound, err := tests.BindExampleInputs(t, wf, path)
	require.NoError(t, err, "the example's own inputs.json no longer answers its declarations")
	require.Equal(t, "checkout", bound["service"].GetLiteral().GetStringValue())

	// The same specification, read from a directory holding no inputs.json — which is
	// what a new example demonstrating a required input looks like before anyone
	// writes one.
	_, err = tests.BindExampleInputs(t, wf, filepath.Join(t.TempDir(), "workflow.yaml"))
	require.Error(t, err,
		"an example whose required inputs nothing answers was accepted, so a run of it would be "+
			"skipped or invented rather than reported")
	require.ErrorContains(t, err, tests.ExampleInputsFile,
		"the refusal does not name the file convention, so an author is told what is missing "+
			"without being told where to write it")
	require.ErrorContains(t, err, `input "service" is required`)
}
