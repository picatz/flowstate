package engine_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
)

// The replay corpus, and what a failure of it means.
//
// Flowstate has one workflow type. [engine.Run] is an interpreter and every
// workload anybody has written is a value it executes, so a change to how
// `runNodes` sequences activities, how compaction decides carryover, or how a wait
// computes its deadline is not a change to one process — it is a change to the
// behaviour of every run currently in flight. Temporal replays a run's history
// through whatever code the worker is running *now*, so an interpreter that would
// today schedule a different activity, in a different order, or none at all, at a
// point where history records one is an interpreter that cannot resume that run.
// The run does not fail gracefully; it wedges on a non-determinism error the first
// time a worker picks it up after the deploy.
//
// Everything else in this package tests the current engine against itself: the
// durable examples suite, the versioning tests, the shared driver cases. All of
// them re-derive the history they then check. Nothing tested the current engine
// against history *a previous engine wrote*, which is the only shape the failure
// actually takes in production.
//
// So: `testdata/replay/<date>/*.json` holds histories recorded from real runs
// against a real dev server, by the engine as it stood on that date. This test
// replays every one of them through [engine.RegisterWorkflows] — the same
// registration a production worker gets — and fails if the current interpreter
// disagrees with any of them about what happened.
//
// # If this test fails
//
// It is not flaky and it is not a fixture that went stale. Replay reads bytes off
// disk, contacts nothing, and depends on no clock; the same corpus and the same
// code give the same answer every time. A failure means precisely one thing:
//
//	the change in your working tree would break every run started before it,
//	at the moment it deploys.
//
// The SDK names the divergence — the event id, what history recorded, and what the
// current code produced instead. Read that first, then choose:
//
//   - **Usually**: the change was meant to be compatible, and is not. Gate the new
//     behaviour so old histories keep the old path. `workflow.GetVersion` is the
//     SDK's tool; in this engine the more common answer is that the new behaviour
//     should key off something in [v1.RunState] that an older run does not set, and
//     therefore reads as absent. See the add-only rules in docs/ARCHITECTURE.md.
//   - **Rarely**: the incompatibility is intended, and is safe only because Worker
//     Versioning pins each run to the interpreter it started on (see versioning.go).
//     Then the corpus entry is retired, deliberately, in the same commit — and the
//     corpus diff *is* the compatibility claim, reviewable the way `buf breaking`
//     makes a schema claim reviewable. Deleting a history to make a red test green
//     is the one thing that must never happen quietly.
//
// # Adding to the corpus
//
// See testdata/replay/README.md and [TestRecordReplayCorpus]. In short: add a
// scenario to `replayScenarios`, run the recorder against a dev server, read the
// JSON it wrote, and commit it. Recording needs a server; this test does not, which
// is the whole point of the split — the gate runs on every PR, on a machine with
// nothing installed.

// replayCorpusDir is the root the corpus lives under, one directory per recording.
const replayCorpusDir = "testdata/replay"

// TestReplayCorpus replays every recorded history against the current interpreter.
//
// Deliberately not gated behind [testing.Short]: replay needs no Temporal server,
// no network and no fixtures beyond the committed JSON, so there is nothing to gate
// it on. It is the half of this feature that runs everywhere.
func TestReplayCorpus(t *testing.T) {
	t.Parallel()

	histories := replayCorpus(t)

	// A corpus that has silently emptied itself — a rename, a bad merge, a
	// .gitignore that swallowed testdata — leaves a test that passes by running
	// zero cases, which is the failure mode a gate can least afford.
	require.NotEmpty(t, histories, "replay corpus is empty: %s holds no histories, so this gate is checking nothing", replayCorpusDir)

	for _, path := range histories {
		// Named by the path below the corpus root, not by the base name: two
		// recordings of the same scenario, taken from different engines, are the
		// point of the corpus and must not collide into one subtest name.
		name := strings.TrimSuffix(strings.TrimPrefix(filepath.ToSlash(path), replayCorpusDir+"/"), ".json")

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			replayer := worker.NewWorkflowReplayer()
			engine.RegisterWorkflows(replayer)

			f, err := os.Open(path)
			require.NoError(t, err)
			defer f.Close()

			// Parsed here rather than through
			// ReplayWorkflowHistoryFromJSONFile so a corrupt file is
			// distinguishable from a determinism failure. They call for
			// completely different reactions and the SDK reports both
			// through the same return value.
			history, err := client.HistoryFromJSON(f, client.HistoryJSONOptions{})
			require.NoErrorf(t, err, "reading recorded history %s: the file is not a Temporal history", path)

			require.NoErrorf(t, replayer.ReplayWorkflowHistory(nil, history),
				"the current interpreter cannot replay %s.\n\n"+
					"This history was written by an earlier engine. A run in flight when this change "+
					"deploys resumes exactly this way, and would fail exactly here. See the package "+
					"comment in replay_test.go before changing anything in testdata/replay/.", path)
		})
	}
}

// TestReplayCorpusUnderTheFlowstateSerializer replays the same corpus through
// the converter a real worker is built with, rather than the SDK default
// [TestReplayCorpus] gets.
//
// The distinction only started mattering with #911. Every history in the corpus
// was recorded before it, so every payload in them is `json/protobuf`, while a
// worker built today serializes as `binary/protobuf`. The claim that made that
// flip a two-way door is that decode does not care — Temporal picks a converter
// per payload out of that payload's own `encoding` metadata — and the corpus is
// the only place in this repository holding bytes an *older* build actually
// wrote. Replaying them through the current converter is therefore the
// strongest available statement that a run in flight survives the deploy.
//
// A failure here alongside a green [TestReplayCorpus] means the converter
// rather than the interpreter: the reorder in payloadcodec became a replace,
// and ProtoJSON is no longer registered.
func TestReplayCorpusUnderTheFlowstateSerializer(t *testing.T) {
	t.Parallel()

	histories := replayCorpus(t)
	require.NotEmpty(t, histories, "replay corpus is empty: %s holds no histories, so this gate is checking nothing", replayCorpusDir)

	for _, path := range histories {
		name := strings.TrimSuffix(strings.TrimPrefix(filepath.ToSlash(path), replayCorpusDir+"/"), ".json")

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// The unconfigured deployment's converter, which is what
			// payloadcodec answers with when no codec is set — and, since
			// #911, binary proto first.
			replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{
				DataConverter: payloadcodec.Config{}.DataConverter(),
			})
			require.NoError(t, err)
			engine.RegisterWorkflows(replayer)

			f, err := os.Open(path)
			require.NoError(t, err)
			defer f.Close()

			history, err := client.HistoryFromJSON(f, client.HistoryJSONOptions{})
			require.NoErrorf(t, err, "reading recorded history %s: the file is not a Temporal history", path)

			require.NoErrorf(t, replayer.ReplayWorkflowHistory(nil, history),
				"a history written as ProtoJSON no longer replays through the converter a worker is built with today (%s).\n\n"+
					"Check payloadcodec: the write-side flip in #911 stays reversible only while both proto "+
					"converters remain registered, and dropping ProtoJSON strands every run started before it.", path)
		})
	}
}

// replayCorpus returns every history file in the corpus, in a stable order.
func replayCorpus(tb testing.TB) []string {
	tb.Helper()

	var paths []string
	err := filepath.WalkDir(replayCorpusDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		paths = append(paths, path)

		return nil
	})
	require.NoError(tb, err)

	return paths
}
