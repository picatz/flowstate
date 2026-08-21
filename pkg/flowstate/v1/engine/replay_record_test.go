package engine_test

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// The other half of the replay gate: how a history gets into the corpus.
//
// A corpus that is a one-off is a corpus that stops covering the engine the week
// after it lands, so recording is a command somebody can run rather than an
// afternoon somebody spent. [TestRecordReplayCorpus] starts a dev server, runs each
// scenario in [replayScenarios] for real, and writes what the server recorded:
//
//	FLOWSTATE_REPLAY_RECORD=1 go test -run TestRecordReplayCorpus -timeout 600s ./pkg/flowstate/v1/engine/
//
// It is off unless that variable is set, and skipped under `-short`, because it is
// the half that needs a server — and because a recorder that ran with the ordinary
// suite would rewrite the corpus on every `go test`, which would quietly destroy
// the only thing the corpus is for. A history is evidence of what an *earlier*
// engine did. Re-recording it against the current engine turns the gate back into
// the thing it was built to replace: the current engine tested against itself.
//
// For the same reason it never overwrites. A scenario whose file already exists is
// left alone and reported; recording the same scenario from a newer engine means
// recording into a new dated directory, which is an addition rather than an edit.
//
// See testdata/replay/README.md for the whole procedure, including what to read in
// the JSON before committing it.

// replayRecordEnv, when set to any non-empty value, permits recording.
const replayRecordEnv = "FLOWSTATE_REPLAY_RECORD"

// replayRecordDirEnv names the directory under testdata/replay to write into,
// overriding the default of today's UTC date.
//
// A date rather than a build id or a commit, because what the directory means is
// "the engine as it stood then" and a reader placing a failure in time is the whole
// use of the name. A commit would be more precise about an engine nobody can
// reconstruct anyway — the corpus is history, not source.
const replayRecordDirEnv = "FLOWSTATE_REPLAY_RECORD_DIR"

// recorderIdentity replaces the SDK's default client and worker identity, which is
// `<pid>@<hostname>`.
//
// History records the identity of whoever completed each workflow task, so the
// default would write the recording machine's hostname into a file this repository
// then carries forever. Fixed here rather than scrubbed afterwards: editing a
// recorded history is falsifying evidence, and the check in
// [requireNothingMachineSpecific] would then be checking the scrubber rather than
// the recording.
const recorderIdentity = "flowstate-replay-recorder"

// TestRecordReplayCorpus records the histories [TestReplayCorpus] replays.
func TestRecordReplayCorpus(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: needs the shared Temporal dev server")
	}
	if os.Getenv(replayRecordEnv) == "" {
		t.Skipf("skipping: set %s=1 to record the replay corpus (see testdata/replay/README.md)", replayRecordEnv)
	}

	dir := os.Getenv(replayRecordDirEnv)
	if dir == "" {
		dir = time.Now().UTC().Format(time.DateOnly)
	}
	target := filepath.Join(replayCorpusDir, dir)
	require.NoError(t, os.MkdirAll(target, 0o755))

	temporal := newTemporalNamespaceWithIdentity(t, recorderIdentity)

	w := worker.New(temporal, engine.RunTaskQueueName, worker.Options{Identity: recorderIdentity})
	engine.Register(w)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	for _, scenario := range replayScenarios(t) {
		t.Run(scenario.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), exampleRunTimeout)
			defer cancel()

			run, err := temporal.ExecuteWorkflow(ctx,
				client.StartWorkflowOptions{
					// The scenario's own name, so the workflow id in the
					// recorded history says what the history is of.
					ID:        "replay-corpus-" + scenario.name,
					TaskQueue: engine.RunTaskQueueName,
				},
				engine.Run, scenario.state)
			require.NoError(t, err)

			// Read before the wait below, and that is not incidental: a
			// [client.WorkflowRun] follows a Continue-As-New chain as it
			// waits, and afterwards names the execution that finished
			// rather than the one that started. Recording from there would
			// capture the last run of a chain and call it the whole thing —
			// which is what the first cut of this recorder did, silently
			// producing one file for a scenario whose entire point is the
			// three.
			firstRunID := run.GetRunID()

			// Whatever the scenario needs from outside itself before it can
			// finish — today, the signal that releases a `wait_for_signal:`.
			// Run before the wait below rather than from another goroutine,
			// because it is the only thing that will make that wait return,
			// and a recorder that waited first would sit here until the
			// context lapsed and write nothing.
			if scenario.release != nil {
				scenario.release(ctx, t, temporal, run.GetID())
			}

			// Waited on, and the error deliberately not required to be nil: a
			// scenario whose point is a failing step is still a history worth
			// replaying. What must not happen is writing a history of a run
			// that never finished.
			var outputs v1.Workflow_StepOutputs
			runErr := run.Get(ctx, &outputs)
			if scenario.expectRunFailure {
				require.Error(t, runErr, "scenario %q was expected to fail and did not", scenario.name)
			} else {
				require.NoError(t, runErr)
			}

			histories := recordRunChain(ctx, t, temporal, run.GetID(), firstRunID)
			require.NotEmpty(t, histories)

			for i, history := range histories {
				name := scenario.name
				if len(histories) > 1 {
					// One file per execution in a Continue-As-New chain.
					// Each is a separate history and each replays on its
					// own; the second and later ones are the valuable
					// ones, since they start from compacted carryover
					// rather than from the submitted specification.
					name += "-run" + strconv.Itoa(i+1)
				}
				writeRecordedHistory(t, filepath.Join(target, name+".json"), history)
			}
		})
	}
}

// recordRunChain returns the history of every execution in a Continue-As-New chain,
// starting at the given run.
//
// A chain is walked rather than only its first execution because the executions
// after the first are the ones no other test in this repository can produce: they
// begin from the [v1.RunState] compaction decided to carry, which is the state a
// change to compaction would silently alter.
func recordRunChain(ctx context.Context, tb testing.TB, c client.Client, workflowID, runID string) []*historypb.History {
	tb.Helper()

	var chain []*historypb.History
	for runID != "" {
		history := &historypb.History{}
		iter := c.GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for iter.HasNext() {
			event, err := iter.Next()
			require.NoError(tb, err)
			history.Events = append(history.Events, event)
		}
		require.NotEmpty(tb, history.GetEvents(), "run %s of %s has no history", runID, workflowID)
		chain = append(chain, history)

		last := history.GetEvents()[len(history.GetEvents())-1]
		runID = last.GetWorkflowExecutionContinuedAsNewEventAttributes().GetNewExecutionRunId()
	}

	return chain
}

// writeRecordedHistory writes one history, refusing to replace one already there.
func writeRecordedHistory(tb testing.TB, path string, history *historypb.History) {
	tb.Helper()

	// The custom marshaler rather than protojson, because [client.HistoryFromJSON]
	// reads the custom dialect — shorthand payloads and the enum spellings the CLI
	// emits — and a corpus the SDK cannot load is not a corpus.
	data, err := temporalproto.CustomJSONMarshalOptions{Indent: "  "}.Marshal(history)
	require.NoError(tb, err)
	requireNothingMachineSpecific(tb, path, data)

	if _, err := os.Stat(path); err == nil {
		tb.Logf("keeping the existing %s: the corpus is add-only, so record a newer engine into a new dated directory (%s=<name>)", path, replayRecordDirEnv)

		return
	}

	require.NoError(tb, os.WriteFile(path, append(data, '\n'), 0o644))
	tb.Logf("recorded %s (%d events, %d bytes)", path, len(history.GetEvents()), len(data))
}

// requireNothingMachineSpecific fails a recording that carries something belonging
// to the machine that made it.
//
// A history is committed testdata, so anything in it is published. The recorder
// already pins the one field that would otherwise carry a hostname
// ([recorderIdentity]); this checks that no *other* path put one in, which is the
// kind of thing an SDK upgrade can change under us. Mechanical rather than a note
// in a README asking somebody to look, because the person recording the second
// history a year from now is the one it has to protect.
func requireNothingMachineSpecific(tb testing.TB, path string, data []byte) {
	tb.Helper()

	forbidden := map[string]string{}
	if hostname, err := os.Hostname(); err == nil && len(hostname) > 3 {
		forbidden["the recording machine's hostname"] = hostname
	}
	if home, err := os.UserHomeDir(); err == nil && home != "/" {
		forbidden["the recording user's home directory"] = home
	}
	for _, name := range []string{"USER", "LOGNAME"} {
		if user := os.Getenv(name); len(user) > 3 {
			forbidden["the recording user's name ($"+name+")"] = user
		}
	}

	for what, value := range forbidden {
		require.NotContainsf(tb, string(data), value,
			"refusing to write %s: it contains %s (%q). Recorded history is committed to this "+
				"repository; find what put it there rather than editing the file.", path, what, value)
	}
}

// replayScenario is one run to record.
type replayScenario struct {
	// name becomes both the workflow id and the file name, so it says what the
	// history covers rather than when it was taken — the directory says that.
	name string

	// state is what the run starts from, built directly rather than parsed from
	// a Flowfile: the corpus is about the engine's command sequence, and a
	// specification is the input to that whatever wrote it.
	state *v1.RunState

	// expectRunFailure marks a scenario whose run is meant to end failed.
	expectRunFailure bool

	// release does whatever has to happen from outside the run for it to
	// finish, after it has started and before it is waited on.
	//
	// A gate is the only shape that needs one: a `wait_for_signal:` that
	// nobody signals never returns, so a scenario recording one has to say who
	// answers it. See [signalWhenParked], which is the only implementation and
	// exists so a scenario can declare *when* the answer arrives — a signal
	// sent before the run reaches its gate is consumed without the run ever
	// parking, which is a different history and a different seam.
	release func(ctx context.Context, tb testing.TB, c client.Client, workflowID string)
}

// signalWhenParked answers a gate, once the run is actually held at it.
//
// The wait is the point. A signal sent the moment the run starts is buffered by
// the server and drained by `waitForSignal`'s early-arrival path, so history
// records a gate the run walked straight through: no timer, no selector, no
// park. Recording *that* would leave the seam this scenario exists for —
// scheduling a timeout timer, blocking on a selector, and cancelling the timer
// when the signal wins (the #770 path, which is guarded by
// [workflow.GetVersion] and so is exactly the kind of decision an old history
// fixes forever) — uncovered while looking covered.
//
// Parked-ness is read through [engine.ProgressQuery], which is what the server
// itself asks (`server.go`'s runProgress) and answers from live workflow state
// without writing a history event, so polling it cannot alter the history being
// recorded.
func signalWhenParked(stepID, name string, payload *v1.Node_Outputs) func(context.Context, testing.TB, client.Client, string) {
	return func(ctx context.Context, tb testing.TB, c client.Client, workflowID string) {
		tb.Helper()

		require.Eventually(tb, func() bool {
			encoded, err := c.QueryWorkflow(ctx, workflowID, "", engine.ProgressQuery)
			if err != nil {
				return false
			}

			var progress v1.RunProgress
			if err := encoded.Get(&progress); err != nil {
				return false
			}

			for _, wait := range progress.GetPendingWaits() {
				if wait.GetStepId() == stepID {
					return true
				}
			}

			return false
		}, 30*time.Second, 50*time.Millisecond,
			"run %s never parked on the gate %q", workflowID, stepID)

		// A [v1.SignalDelivery] rather than the bare payload, because that is
		// what `FlowstateServer.Signal` puts on this channel: a recorder
		// sending something else would record a history no deployment can
		// produce. The sender is the fixed fictional one below for the reason
		// [recorderIdentity] exists — everything in a recorded history is
		// published.
		require.NoError(tb, c.SignalWorkflow(ctx, workflowID, "", name, &v1.SignalDelivery{
			Payload: payload,
			Sender: &v1.SignalSender{
				Identity: &v1.WorkloadIdentity{
					Subject: "replay-corpus",
					Issuer:  "flowstate:test",
				},
			},
		}), "signalling %s with %q", workflowID, name)
	}
}

// replayScenarios are the runs the corpus covers.
//
// Chosen for the paths where a change to the interpreter would reorder or drop a
// command, and kept small on purpose — a history's value is the seam it crosses,
// not its length, and every event here is bytes this repository carries forever.
// Prefer depth of seams over breadth of duplicates when adding one.
func replayScenarios(tb testing.TB) []replayScenario {
	tb.Helper()

	baseURL := conformance.NewHTTPServer(tb)

	says := func(id, message string) *v1.Node {
		return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{
			Name:   "log",
			Inputs: map[string]*v1.Value{"message": v1.NewLiteral(message)},
		}}}
	}

	// An http step that posts a body to the loopback echo server and records what
	// came back under `said`, so a later step has something of an earlier step's
	// to name — which is what makes compaction's carryover decision observable.
	echoes := func(id, body string) *v1.Node {
		return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{
			Name: "http",
			Inputs: map[string]*v1.Value{
				"method":  v1.NewLiteral(http.MethodPost),
				"url":     v1.NewLiteral(baseURL + "/echo"),
				"body":    v1.NewExpr(body),
				"outputs": v1.NewExpr(`{"said": response.body}`),
			},
		}}}
	}

	return []replayScenario{
		{
			// The ordinary shape: several steps, one after another, one
			// activity each. Nothing exotic, which is the point — it is
			// the history that breaks if the interpreter's basic
			// sequencing changes, and every other entry is a variation on
			// it.
			name: "multi-step-tasks",
			state: &v1.RunState{Workflow: &v1.Workflow{
				Name:  "multi-step-tasks",
				Steps: []*v1.Node{says("first", "one"), says("second", "two"), says("third", "three")},
			}},
		},
		{
			// A tolerated failure. The step's activity fails and the run
			// walks on, so history holds an activity failure followed by
			// more scheduling — the sequence an engine that stopped
			// tolerating, or started tolerating differently, could not
			// reproduce. The failure comes from the task's own input
			// validation (`message` is required) so that recording it
			// needs nothing beyond the worker: no server to take down, no
			// timeout to sit through, and a non-retryable classification,
			// so the history is one attempt rather than five.
			name: "tolerated-failure",
			state: &v1.RunState{Workflow: &v1.Workflow{
				Name: "tolerated-failure",
				Steps: []*v1.Node{
					says("before", "before"),
					func() *v1.Node {
						node := &v1.Node{Id: "doomed", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}}}
						node.Policy = &v1.StepPolicy{ContinueOnError: true}

						return node
					}(),
					says("after", "after"),
				},
			}},
		},
		{
			// Concurrent iterations. A loop that runs its body several
			// times at once schedules several activities before any of
			// them completes, so the history pins an *ordering* rather
			// than a sequence — the thing a change to how iterations are
			// dispatched is most likely to disturb.
			name: "for-each-concurrent",
			state: &v1.RunState{Workflow: &v1.Workflow{
				Name: "for-each-concurrent",
				Steps: []*v1.Node{
					{Id: "loop", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
						Items:       v1.NewExpr(`["a", "b", "c"]`),
						Iterator:    "region",
						MaxParallel: 3,
						Body:        []*v1.Node{says("touch", "touching a region")},
					}}},
					says("done", "done"),
				},
			}},
		},
		{
			// The other concurrency shape, and a different one in
			// history: branches are independent by construction, so the
			// commands for both are emitted from one workflow task and
			// their relative order is decided entirely by the
			// interpreter.
			name: "parallel-branches",
			state: &v1.RunState{Workflow: &v1.Workflow{
				Name: "parallel-branches",
				Steps: []*v1.Node{
					{Id: "fan", Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{
						Branches: []*v1.Parallel_Branch{
							{Steps: []*v1.Node{says("left", "left")}},
							{Steps: []*v1.Node{says("right", "right")}},
						},
					}}},
					says("join", "joined"),
				},
			}},
		},
		{
			// A durable timer. `sleep:` is the one node kind that schedules
			// no activity at all, so its whole footprint in history is a
			// TimerStarted and a TimerFired between two ordinary steps —
			// and a change to how a wait computes its deadline (the
			// determinism input `versioning.go:18` and the package comment
			// in replay_test.go both name, and which nothing in the corpus
			// pinned) is a change to exactly those two events.
			//
			// A literal duration rather than a `${...}` one, because both
			// spellings resolve through [v1.EvalWaitDuration] and reach the
			// same `workflow.Sleep`: a second entry would record the same
			// timer twice. Short, because the recorder sits through it for
			// real — long enough that the run parks on it rather than
			// racing past.
			name: "wait-sleep",
			state: &v1.RunState{Workflow: &v1.Workflow{
				Name: "wait-sleep",
				Steps: []*v1.Node{
					says("before", "before the nap"),
					{Id: "settle", Kind: &v1.Node_Wait{Wait: &v1.Wait{
						Kind: &v1.Wait_Duration{Duration: durationpb.New(time.Second)},
					}}},
					says("after", "after the nap"),
				},
			}},
		},
		{
			// The approval gate, answered. The run reaches the gate,
			// schedules the timeout timer, blocks on a selector, and the
			// recorder signals it — so history holds the sequence a
			// bounded gate produces when the signal wins: TimerStarted,
			// WorkflowExecutionSignaled, TimerCanceled.
			//
			// That last event is why this entry is worth its bytes. It is
			// the #770 fix, and it is issued only when
			// [workflow.GetVersion] reports the marker this history
			// records — precisely the shape of decision a run in flight
			// has already fixed and a later engine must not reconsider. A
			// history without the gate's park cannot pin it.
			name: "wait-for-signal",
			state: &v1.RunState{Workflow: &v1.Workflow{
				Name: "wait-for-signal",
				Steps: []*v1.Node{
					says("announce", "asking for approval"),
					{Id: "approval", Kind: &v1.Node_Wait{Wait: &v1.Wait{
						Kind: &v1.Wait_Signal{Signal: &v1.Signal{
							Name: "deploy-approved",
							Outputs: map[string]*v1.Value{
								"approved": v1.NewExpr(`has(payload.approved) && payload.approved`),
								"lapsed":   v1.NewExpr(`timed_out`),
							},
							Prompt: v1.NewLiteral("approve the corpus deploy?"),
						}},
						// Long enough that the timer is certainly still
						// pending when the signal arrives: the point of
						// this entry is the cancelling branch, and a
						// bound that could lapse first would make which
						// history got recorded a matter of how loaded
						// the recording machine was.
						Timeout: durationpb.New(time.Hour),
					}}},
					says("deploy", "approved, deploying"),
				},
			}},
			release: signalWhenParked("approval", "deploy-approved", &v1.Node_Outputs{
				NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)},
			}),
		},
		{
			// The same gate, unanswered. Nobody signals, the bound lapses,
			// and the wait resolves through the timer branch instead — a
			// different command sequence from the one above, ending in a
			// TimerFired the run treats as a normal outcome rather than a
			// failure. Both halves of that race are recorded because an
			// interpreter can break one while replaying the other
			// perfectly.
			//
			// Its own bound is short for the plainest reason: the recorder
			// waits it out.
			name: "wait-for-signal-timeout",
			state: &v1.RunState{Workflow: &v1.Workflow{
				Name: "wait-for-signal-timeout",
				Steps: []*v1.Node{
					says("announce", "asking for an approval nobody gives"),
					{Id: "approval", Kind: &v1.Node_Wait{Wait: &v1.Wait{
						Kind: &v1.Wait_Signal{Signal: &v1.Signal{
							Name: "deploy-approved",
							Outputs: map[string]*v1.Value{
								"lapsed": v1.NewExpr(`timed_out`),
							},
						}},
						Timeout: durationpb.New(2 * time.Second),
					}}},
					says("give-up", "nobody answered"),
				},
			}},
		},
		{
			// The one no other suite can reach. A budget of one step
			// forces a Continue-As-New between every pair of steps, so
			// this scenario records a chain of executions rather than
			// one: each later run begins from the carryover compaction
			// chose, and each step names the step before it, so a
			// carryover that dropped an output would be visible as a run
			// that cannot resolve its own input.
			//
			// Examples CI cannot produce this history at all — the local
			// driver never suspends — which is precisely why it is here.
			name: "continue-as-new-carryover",
			state: &v1.RunState{
				Workflow: &v1.Workflow{
					Name: "continue-as-new-carryover",
					Steps: []*v1.Node{
						echoes("a", `"hi"`),
						echoes("b", `a.said + "!"`),
						echoes("c", `b.said + "?"`),
					},
				},
				StepsBudget: 1,
			},
		},
	}
}
