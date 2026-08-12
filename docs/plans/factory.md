# Factory retro ledger

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
