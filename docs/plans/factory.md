# Factory retro ledger

> [!NOTE]
> **Internal process, not product documentation.** This file is part of
> `docs/plans/`: how agent work is dispatched here, and what past waves
> measured. Nothing in it describes Flowstate to someone using it — the map of
> the documentation that does is [docs/README.md](../README.md).

One short append per wave: friction observed, slice landed, measurement.
Receiver-cost outcomes tracked per wave: clarification turns, owner edits
to our artifacts, review findings accepted vs noise. Entries stay terse;
the ledger is itself subject to the doctrine it measures.

## Wave 1 (2026-08-11/12): friction inventory

Source: [#482](https://github.com/picatz/flowstate/issues/482), the
charter; the inventory in full lives there.

Friction, measured: the full local gate duplicated a six-minute parallel
CI at 30-60+ minutes per agent under contention; four of five builders
parked mid-gate awaiting a watcher; a test reached outside its worktree
(a sibling's uncommitted DSL.md edits failed
TestTheMirrorMatchesTheRepository); the DSL.md generate step was
undocumented and cost two agents a gate round each; both Codex P2s were
instances of house lenses a pre-push pass would have caught; a dispatch
prompt asserted tree state that lived only on a dead branch.

Slices adopted (owner, 2026-08-12): tiered gates, weekly deep CI tier, Go
hooks, skills plus orchestration reference plus this ledger.

## Wave 1 addendum: communication sins

Source: [#482, Slice F comment](https://github.com/picatz/flowstate/issues/482#issuecomment-5260498706),
owner direction 2026-08-12.

Named sins: design passes and issue bodies restating carried context;
decision comments mirroring their pass; session updates narrating agent
mechanics and re-summarizing agent reports; double-summarization as
habit. Kept and named as good practice: fix-as-response, one-line
decisions, diff-explaining PR bodies, recommendation-first option chips.

Slice landed: the comms-* surface skills, pre-pr-review, the
orchestration reference, and this ledger (branch claude/factory-comms).

Measurement baseline for wave 2: count clarification turns, owner edits
to our artifacts, and review findings accepted vs noise, against this
wave's artifacts.

## Wave 2 (2026-09-01): the September slate, first dispatch

Source: `2026-09-roadmap.md` (the step-back review), issues #1376–#1388.

Dispatched: six builders and one reviewer at the deep tier, one mechanical
slice (a dependabot retidy) at the cheap tier, all in isolated worktrees on
one four-core machine. Landed the same day: two dependabot merges, PRs
#1387 (#1306 gate base channel), #1389 (#1336 token on a descriptor),
#1390 (#1332 egress grant, PR A), the #1281 review-and-fix, the #1372
retidy; the doc-truth sweep (#1382) and the appearance flip (#1319) on
the review branch itself.

Friction, measured: five concurrent `go run ./tools/gate` runs pushed the
load average to 50–80, turned ten-minute gates into forty-minute ones, and
failed unrelated deadline-bound tests in every builder's wide leg — each
slice lost roughly forty minutes to a result nobody could interpret. The
redirect that fixed it: targeted package tests, push, open the PR with an
honest verification section, CI as the authoritative gate; kill the
contended gate by PID. Recorded as #1388 (the gate's own half) and as a
rule below (the wave's half). Second friction: builders park inside a
long foreground command and cannot read a redirect until it returns —
bound the first gate attempt, not only the tests inside it.

Receiver-cost outcomes, this wave: zero clarification turns from the
owner on any brief; owner edits to our artifacts: none yet; review
findings accepted vs noise: the #1281 review returned four accepted
findings and one pre-existing non-finding; two reviews (#1389, #1390)
pending at the time of writing.

Third friction: the session scratchpad is shared across worktree agents,
so two builders overwrote each other's draft files and one gate log was
truncated mid-write. Rule: a dispatch brief names a per-agent scratch
directory (the worktree's own `.git`-ignored path or a subdirectory keyed
by slice), never the shared scratchpad.

Rule adopted (settles #1386, option 1): one entry per dispatched wave,
appended by the dispatching session before it ends, with the measurement
line above filled from what that session actually observed. Second rule:
at most two full gates run concurrently on one machine; the rest of a wave
verifies with targeted package tests and lets PR CI be the gate.

Evening addendum, same wave. Bot second passes: every push drew a fresh
Codex pass, and the second passes were not noise — #1394's found the
over-correction of a first-pass fix (a blanket cancelled-context guard that
withheld allows for requests that had already left) and a
permanent-versus-retryable inversion at the secret seam; #1390's found a
help-text over-claim at the egress boundary; #1392's found the
sensitive-output leak this session had already filed as #1396. Rule: a
finding on a fix is read as carefully as a finding on the original, and a
documented deferral gets an issue number in the reply, not a promise. CI
infrastructure: three failures this evening were not the PR's — a Go module
proxy stream error in the editors workflow, a `sum.golang.org` stream error
installing vhs in `appearance`, and a browser-launch test in `cmd/flow`
timing out under load on #1281 — each handled as one standing-down comment
plus one re-run, never a second re-run. Own error: re-running a superseded
editors run cancelled the newer head's run through the workflow's
concurrency group; rule: re-run only the run for the current head. Own
error: after a context reset the lead filed #1398 duplicating its own #1393
from an hour earlier; rule: dup-check the session's own filings (issues
created today by this account) before filing, not only the backlog.
Receiver-cost update: bot findings accepted this evening, seventeen;
disputed with evidence, one (a Copilot CEL parse claim); deferred with an
issue each, three.
