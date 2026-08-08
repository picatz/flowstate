# The replay corpus

Recorded Temporal histories, one file per workflow execution, produced by real runs
against a real dev server. `TestReplayCorpus` in `../../replay_test.go` replays
every one of them against the interpreter in the current working tree.

They are not fixtures of expected output. They are evidence of what an earlier
engine did, and the only thing in this repository that tests the current engine
against a *previous* one. Everything else — the durable examples suite, the
versioning tests, the shared driver cases — re-derives the history it then checks,
so all of it stays green through a change that would wedge every run already in
flight.

Read the package comment at the top of `replay_test.go` first. It says what a
failure means and what the two legitimate responses are. The short version: a
replay failure is "this change breaks a running deployment", never "a test went
stale", and deleting a history to turn the gate green is the one move that must
never happen quietly.

## What is here

One directory per recording, named for the date the engine was recorded on — the
directory is the version, the file name is the path covered.

| File | What it covers |
| --- | --- |
| `multi-step-tasks.json` | Three tasks in a row. The plain sequencing every other entry is a variation on. |
| `tolerated-failure.json` | `continue_on_error`: an activity fails and the run walks on, so the history holds a failure followed by more scheduling. |
| `for-each-concurrent.json` | A `for_each` with `max_parallel: 3`. Several activities scheduled before any completes, so the history pins an ordering rather than a sequence. |
| `parallel-branches.json` | Two `parallel:` branches. Both branches' commands come out of one workflow task, in an order the interpreter alone decides. |
| `continue-as-new-carryover-run{1,2,3}.json` | One Continue-As-New chain, one file per execution. Runs 2 and 3 start from the state compaction chose to carry, which no other suite in this repository can produce: examples CI runs the local driver, and the local driver never suspends. |

## Adding one

1. Add a scenario to `replayScenarios` in `../../replay_record_test.go`. Prefer a
   seam nothing else crosses over another arrangement of steps already covered —
   every event here is bytes this repository carries forever.
2. Record it:

   ```
   FLOWSTATE_REPLAY_RECORD=1 go test -run TestRecordReplayCorpus -timeout 600s ./pkg/flowstate/v1/engine/
   ```

   The recorder starts a dev server, runs each scenario for real, walks any
   Continue-As-New chain, and writes into `testdata/replay/<today's UTC date>/`.
   Set `FLOWSTATE_REPLAY_RECORD_DIR` to name the directory yourself.

3. Read the JSON it wrote before committing it. It is published the moment it
   lands. The recorder pins the SDK identity to `flowstate-replay-recorder` and
   refuses to write a file containing the recording machine's hostname, home
   directory or user name, but nothing mechanical can know what a *scenario* puts
   in a payload — so look at what the run actually sent. Nothing here may carry a
   credential, a real endpoint, or anything belonging to the machine that recorded
   it. The one loopback address in the corpus today is the `http` scenario's echo
   server, which replay never contacts.

4. Commit it. `TestReplayCorpus` picks it up by walking the directory; there is no
   list to update.

The corpus is add-only. The recorder will not overwrite a file that exists — it
says so and leaves it alone — because re-recording a history against the current
engine converts the gate back into the thing it replaced: the current engine tested
against itself. Recording the same scenario from a newer engine means a new dated
directory alongside the old one, which is an addition rather than an edit.

## Why replay is not gated on a dev server

Recording needs a server. Replay reads bytes off disk, contacts nothing, and
depends on no clock, so it runs in CI on every pull request under the ordinary
`go test ./...` — including under `-short`, where every other durable test here
skips. That split is the point of the whole arrangement.
