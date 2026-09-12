# Agent configuration

Flowstate supports Claude Code, Codex, and Amp without making any one host's
prompt format the repository's source of truth. The design is layered so agents
receive a small durable contract, load procedures only when relevant, and rely on
software rather than prose for checks that can be deterministic.

## The layers

| Artifact | Role | Loading behavior |
| --- | --- | --- |
| `AGENTS.md` | Shared repository-wide facts, invariants, and working contract | Always loaded by Amp and Codex; imported by Claude Code |
| `CLAUDE.md` | Thin Claude Code adapter | Always loaded by Claude Code |
| `.agents/skills/*/SKILL.md` | Portable task workflows for Amp and Codex | Name and description are advertised; the body loads when selected |
| `.claude/skills/*/SKILL.md` | Claude Code mirrors of the portable skills | The body loads when selected |
| `.claude/settings.json` | Claude-specific hooks, and the allow list that stops the repository's own verification commands from prompting | Enforced by Claude Code |
| `.claude/agents/*.md` | Fresh-context subagents carrying the review, verification, and review-hygiene rubrics (`flowstate-reviewer`, `flowstate-verifier`, `flowstate-pr-tidy`) | Claude Code delegates on the description; the body and preloaded skills load in the subagent's own context |
| `.claude/rules/*.md` | Path-scoped procedures, currently only the agent-configuration maintenance rule | Loaded when Claude Code reads or edits a file matching the rule's `paths` |
| `.amp/settings.json` | Amp workspace skill selection; no repository-specific permission prompts | Applied by Amp in this repository |
| `.agents/ship.md` | Versioned Amp Custom Ship procedure | Copied into the Amp project setting; it is not read automatically after edits |
| `.agents/setup` / `.agents/resume` | Amp Orb provisioning and wake behavior | Run by the Orb lifecycle |
| `AGENT_FIELD_NOTES.md` | Small index of historical guidance | Never imported; used to locate one relevant archive |
| `AGENT_FIELD_NOTES_LEGACY.md` and `.agent-history/` | Byte-preserved legacy guidance and skill bodies | Historical reference, loaded only for a concrete question |
| `tools/agentconfig` | Structural tests for this configuration | Run by the full Go test suite and selected by the diff-scoped gate for repository-owned agent configuration |
| `tools/citations` | Checks every `path.go:NNN` citation in the hand-written documentation against the tree: the file exists, the line is inside it, and the cited lines hold the symbol the sentence names | Run by the full Go test suite and selected by the diff-scoped gate for a change under `docs/`, `README.md` or `AGENTS.md` |

This follows each host's current discovery contract:

- Amp loads repository `AGENTS.md` files and project skills from
  [`.agents/skills/`](https://ampcode.com/docs/customize/skills). It recommends a
  general top-level `AGENTS.md` and more specific guidance closer to a subtree.
  See [Amp's AGENTS.md documentation](https://ampcode.com/docs/customize/agents-md).
- Codex loads `AGENTS.md` from the repository hierarchy and uses skills for
  reusable workflows. See [Codex AGENTS.md](https://developers.openai.com/codex/agent-configuration/agents-md)
  and [Build skills](https://developers.openai.com/codex/build-skills).
- Claude Code loads `CLAUDE.md` and project skills from `.claude/skills/`.
  Anthropic recommends keeping always-loaded instructions concise and moving
  procedures to skills. See [Claude memory](https://code.claude.com/docs/en/memory)
  and [Claude skills](https://code.claude.com/docs/en/skills).

## One contract, native controls

The successful Claude Code workflow supplies the design evidence: a 989-byte
adapter imports the shared contract, task procedures load as skills rather than
always-loaded prompt, the session hook selects the pinned toolchain once, edit
hooks return deterministic feedback, and the merge hook fails closed at the
last local action. The speed comes from moving repeatable judgments into small
tools and loading prose only when its task is active; the safety comes from
keeping merge evidence independent of the model's summary.

Amp and Codex share those semantics without pretending they share Claude's tool
events. `AGENTS.md` is the canonical durable policy; `.agents/ship.md` is the
canonical shipping procedure; `tools/gate` owns changed-area selection; and
`tools/shipcheck` owns remote exact-head evidence. Claude keeps its native hooks,
Amp keeps its Custom Ship adapter, and Codex consumes `AGENTS.md` plus portable
skills. `tools/agentconfig` validates the thin Claude adapter and byte-identical
skill mirrors, while `.amp/settings.json` prevents Amp from loading both mirror
trees. A host-specific control is added only when that host can test its tool
input and blocking behavior end to end.

`tools/agentconfig` bounds the always-loaded contract at 8 KB and the Claude
adapter at 2 KB, so a new always-loaded line has to displace an old one. The
portable skill corpus is about 27 KB per discovery tree, but only the selected
skill body enters a task, and Amp's disabled Claude fallback avoids loading
both. The 72.6 KB legacy manual is indexed rather than imported. The mirror
therefore costs repository bytes and one drift test, not duplicate model tokens
or an extra runtime read; replacing it with forwarding files would save storage
while adding latency and another host-specific resolution failure.

The shared shipping semantics are deliberately provider-neutral: one exact-head
code-and-security review is mandatory, and a fresh-context review carrying this
repository's rubrics is what satisfies it. Vendor review bots are not requested.
One that reviews on its own is input rather than a gate: its findings are read
and dispositioned once for the head they arrived on, and its silence, quota, or
absence is neither a defect nor a PASS. Only a material defect earns a new head;
an advisory finding is answered or deferred to a scoped issue, so a bot that
re-reviews each push cannot turn a finished change into an endless loop. That
bound is the difference between review as evidence and review as a treadmill.
`shipcheck` enforces what it can read: an attestation that any later comment,
review, or inline-comment update follows, an edited attestation, unresolved
threads, nonterminal or failing checks, enabled auto-merge, and more than one
owner comment asking `@codex` for a review, which is the only request shape it
recognizes, counted from when the head under review was committed: the control
is against re-rolling a vendor review on one head until it goes quiet, and a
request made before that head existed was aimed at a revision the attestation
does not cover. The bound is therefore per head rather than per pull request,
and a new head clears the count, which is why the rule that only a material
defect earns a new head is what keeps the reset from being free. The head's
date comes from the commit object, which the committer writes rather than
GitHub stamping it, and a later cutoff hides more requests, so the date is
clamped by the earliest instant GitHub stamped for that head: a check run
cannot start before the head exists. A head whose date will not parse, or
that no stamped check can bound, counts every request. A late artifact therefore forces explicit re-attestation without
requiring any vendor to answer. Two things it does not cover: a request to a
different vendor, which it never reads and which this repository's automatic
Copilot review would make ambiguous anyway, and, on the REST fallback, a review
body edited after the attestation, which REST cannot report; the tool says so
and asks the operator to read the reviews once more before merging.

## Rightsized for the Claude 5 generation

Anthropic removed most of Claude Code's own system prompt for the Claude 5
models and published the rules it applied in
[The new rules of context engineering for Claude 5 generation models](https://claude.com/blog/the-new-rules-of-context-engineering-for-claude-5-generation-models)
(July 2026), alongside the
[Claude Code best practices](https://code.claude.com/docs/en/best-practices).
This configuration applies them as follows; the audit that step 7 below asks
for after a model upgrade was this one.

- **Judgment over rules.** `AGENTS.md` keeps the invariants and the gotchas the
  tree cannot announce; guardrails that a current model applies unprompted
  (no generic praise, no invented priority, no file-by-file tours) were
  deleted from the skills, and conflicting instructions (a skill saying "draft
  by default" beside a contract that grants task-authorized actions) were
  collapsed to the one statement in `AGENTS.md`.
- **Single instructions.** Verification, shipping, and attribution each have
  one home: the `flowstate-verify` and `flowstate-ship` skills, `.agents/ship.md`,
  and one line of `AGENTS.md`. The always-loaded contract points at them and
  keeps the commands an agent needs in nearly every task.
- **Progressive disclosure.** Procedures are skills whose descriptions say when
  they apply; `.claude/rules/agent-config.md` loads the maintenance procedure
  only while agent-configuration files are open; the legacy manual is indexed,
  never imported.
- **Interfaces over examples.** Hook messages name the rule and the repair in
  the words the model acts on (`make fmt`, kill by PID), and the skills that
  carried example prose now state what an artifact must contain.
- **Rich references and verifier agents.** The review rubrics are preloaded
  into `flowstate-reviewer`, a subagent with no editing tool whose fresh
  context is the provider-neutral independent review the ship procedure
  requires;
  `flowstate-verifier` runs the bounded legs and returns evidence instead of
  logs, so a long session's context is not spent on test output.
- **Auto memory over guidance files.** A learning from one session goes to the
  host's auto memory; a repository rule needs a recurring failure or a
  mechanism, per the decision table below.

## Why skills are mirrored

Amp and Codex prefer `.agents/skills/`; Claude Code discovers project skills in
`.claude/skills/`. Every Flowstate skill therefore exists in both locations with
identical bytes. `tools/agentconfig/agentconfig_test.go` verifies:

- both directories contain the same skill names;
- every `name` matches its directory;
- every skill has a useful `description`;
- the corresponding `SKILL.md` files are byte-identical.

The mirror avoids host-specific forwarding instructions and an extra read when a
skill activates. Edit the portable copy first, copy it to the Claude location,
and run:

```sh
go test ./tools/agentconfig
```

The diff-scoped gate maps `.agents/`, `.claude/`, `.amp/`, `.agent-history/`,
the root adapters, and field-note archives to this package. Its gate tests keep
representative one-sided skill, settings, adapter, and archive edits from
silently skipping the structural checks.

The test is the mechanism that prevents the two copies from becoming competing
sources of truth.

## What belongs where

Use this decision rule when an agent or reviewer proposes another instruction:

| Kind of information or behavior | Put it here |
| --- | --- |
| Non-obvious fact needed in nearly every task | `AGENTS.md` |
| Package- or subtree-specific invariant | A nested `AGENTS.md`; add a Claude-scoped rule only when the same behavior cannot be expressed portably |
| Repeatable procedure, rubric, or artifact contract | A skill |
| Detailed architecture, examples, or historical incident | A referenced document |
| Exact parsing, discovery, validation, or transformation | A script or typed tool |
| Action that must be blocked or checked regardless of model judgment | Hook, permission rule, sandbox, or CI |
| An independent judgment, or a procedure whose output should not share the main context | A Claude subagent under `.claude/agents/` preloading the relevant skill; other hosts run the skill inline |
| A procedure that matters only while particular files are open, and only on Claude Code | `.claude/rules/*.md` with `paths:`; a portable equivalent is a nested `AGENTS.md` |
| A learning from one session that is not repository policy | The host's auto memory, not a repository file |
| One task's outcome, scope, and acceptance criteria | The current prompt |
| Personal tone, model, verbosity, or account preference | User-level host configuration, not this repository |

Do not add an instruction merely because one session failed. First ask whether
the failure is recurring, whether the current tree already expresses the answer,
and whether a deterministic mechanism can prevent it more reliably.

## Host-specific notes

### Amp

- Inspect active guidance with `agents-md list` from the command palette.
- Inspect skill precedence with `amp skills list`; `.agents/skills/` wins over the
  `.claude/skills/` compatibility location for repository skills. Workspace
  settings disable Claude-skill fallback so Amp uses the portable copy only.
- `.agents/setup` provisions the pinned toolchain and dependencies for a fresh
  Orb. `.agents/resume` stays fast because Flowstate has no persistent backing
  service or authentication state to repair on wake.
- Flowstate adds no repository-specific Amp approval prompts. Routine local work
  and task-authorized repository collaboration—branches, commits, pushes, pull
  requests, issues, review comments, and merges when the user asked to land the
  change—proceed under Amp's own authorization contract without a second
  Flowstate-specific confirmation layer. Repository guidance cannot widen the
  host's authority.
- The Amp project uses the supported
  [Custom Ship](https://ampcode.com/docs/orbs/shipping) behavior with the
  versioned prompt in `.agents/ship.md`. Amp stores a copy rather than reading
  that file at Ship time, so synchronize every accepted edit with:

  ```sh
  amp projects update picatz/flowstate --ship-behavior custom \
    --custom-ship-prompt-file .agents/ship.md
  ```

  The procedure opens a PR without auto-merge, requires a distinct independent
  exact-final-head AI code-and-security review, dispositions findings and
  reviews new heads after fixes, waits for all applicable checks, runs
  `go run ./tools/shipcheck --repo picatz/flowstate --pr NUMBER`, merges manually, and
  verifies `origin/main` afterward.
- Teams that later demonstrate a recurring need for stricter push, merge,
  release, or destructive-command controls should add an explicit user/team
  policy or an opt-in example. Flowstate does not ship a brittle command
  blacklist preemptively.
- Amp-specific controls do not make Claude hooks portable; add an Amp control
  only when a behavior genuinely needs enforcement on that host.

### Claude Code

- Verify the loaded memory files and skills with `/context`.
- Use `/doctor` when always-loaded guidance grows or becomes inconsistent.
- The repository's `.claude/settings.json` hooks guard generated files, process
  cleanup, merge review state, and formatting. SessionStart builds those four
  existing commands once in the checkout-local ignored `.claude/hooks/.bin`
  directory; per-tool hooks use a launcher to execute the ready binaries
  instead of recompiling through `go run`. The identity that decides whether a
  binary is current walks each package directory the compiler reaches, so a
  source Git ignores counts and a test file does not, and the build re-asks the
  compiler afterwards so an import added mid-build cannot be missed. A stale
  generation is rebuilt on the spot rather than deferred to a restart. A
  guard whose own package does not compile warns and lets the call through,
  since the guards match the very tools needed to repair it and a guard that
  could not be built has refused nothing. Each guard is compiled separately, so
  one mid-edit does not decide for the others, and a guard whose package is
  gone denies, because that is a control removed rather than unfinished. The
  merge guard is the one exception to failing open, and only for the calls it
  would have judged: the merge tool's entry passes `strict`, and on the shell
  the launcher refuses a payload that could be a merge while letting every
  other command through, since it is wired on all of Bash but ignores
  everything that is not a merge, and denying `go build` to guard a `go build`
  would take away the repair itself. That test is a coarse over-approximation,
  not a second recognizer, and it does not claim to be complete: the guard
  decides what a merge is by tokenizing the command, and a text test cannot
  equal a tokenizer. It covers the spellings a caller writes without trying to
  evade it, it is strictly narrower than the behaviour it replaced, and the
  gates that decide whether a change may land are `tools/shipcheck` and the
  exact-head review evidence, not this. Every other failure denies. Other hosts
  do not run these Claude-native tool events.
- Legacy `.claude/commands/ci-check.md` and `test-fast.md` remain only as short
  compatibility aliases for the `flowstate-verify` skill. New procedures belong
  in skills. A command never shares a skill's name: `both-drivers.md` was
  removed because the two shadowed each other.
- `permissions.allow` in `.claude/settings.json` pre-approves the build, test,
  format, gate, and regeneration commands the repository prescribes, so
  verification does not prompt. A test pins the list to exact
  program-and-subcommand entries, rejects a wildcard, push, merge, commit, or
  destructive one, and decides which entries may take arguments by whether the
  command needs them: a test run takes packages, while a command that only
  reads stays exact. That is why `git fetch origin` and `git fetch origin main`
  are pre-approved but `git fetch` with arguments is not (a refspec and
  `--update-head-ok` move the checked-out ref), and why `git diff`, `git log`,
  `git show`, and `git blame` are pre-approved only bare: each accepts
  `--output=<file>`, which truncates that file and writes the report into it,
  so `git diff --output=AGENTS.md HEAD` would overwrite tracked work with no
  prompt. Claude Code matches each subcommand of a compound command
  separately, so `go test ./... && git push` still prompts for the push.
- The allow list assumes the checkout is one the operator trusts. Its point is
  running the repository's own code: `go test` executes the checkout's tests,
  `-coverprofile` writes the path it is given, `-exec` and `-toolexec` name
  another program to run, and `go run ./tools/gate` runs a tool from the tree.
  The list creates no capability a contributor's own test run lacks, but it
  does remove the prompt, so on a head you do not trust (an external
  contributor's, say) review by reading and leave running its code to CI or a
  sandbox. `flowstate-reviewer` says the same.
- `.claude/agents/flowstate-reviewer.md` is the fresh-context reviewer: delegate
  a diff or PR head to it for the exact-head independent review, and to
  `flowstate-verifier` for a gate or full run whose output should stay out of
  the main context. The reviewer is given no editing tool and asks for a
  throwaway worktree (`isolation: worktree`); where the host honors that, a
  shell command it runs can change only the worktree, never the checkout under
  review, and where it does not the agent confirms as much with
  `git worktree list`, restricts itself to read-only commands, and says so.
  Its prompt forbids writing in either case. The verifier runs on the `sonnet` alias
  because running and summarizing tests needs less reasoning than reviewing
  them; the reviewer inherits the session model. `flowstate-pr-tidy`, also on
  `sonnet`, resolves
  review threads whose disposition is visible and hides decision-free bot
  comments so human reviewers see decisions rather than AI traffic; it never
  replies to an AI reviewer and runs before the attestation. All three are
  bounded and checked by `tools/agentconfig`.
- Auto memory is on by default and lives outside the repository. Session
  learnings go there; do not add them to `CLAUDE.md`. `/skill-doctor` reports
  which skills cost context and are never invoked; `/doctor` proposes cuts to
  a checked-in `CLAUDE.md`.

### Codex

- `AGENTS.md` is the repository instruction source; do not configure Codex to
  fall back to the large historical notes file.
- Invoke project skills explicitly when useful or let their descriptions route
  the task. Keep personal model effort and visible verbosity in user-level Codex
  configuration.
- The repository does not currently claim Claude hook parity in Codex. The
  existing generated-file, formatting, process, and merge guards were written and
  tested against Claude Code's tool events. Port a guard only after its Codex tool
  name, input shape, and blocking behavior are covered by an end-to-end test;
  pretending a hook is portable would be worse than naming the gap.
- Repository CI, not an agent's summary, is the final authority for mergeability.

## Maintaining the system

1. Reproduce the process failure and identify the smallest layer that owns it.
2. Before the next autonomous merge, add a regression check where feasible,
   update the owning guidance, or file a searched, scoped issue for work needing
   an admin/product decision. Link the durable artifact from the incident.
3. Propose the change before allowing an agent to persist a new rule.
4. Prefer deletion or movement over repeating the instruction in another file.
5. Update both skill mirrors and run `go test ./tools/agentconfig`.
6. Test the behavior on representative coding, review, and communication tasks.
7. After a major model or harness upgrade, audit old instructions for ritualized
   verification, mandatory narration, fixed reasoning sequences, and assumptions
   the new host no longer needs. The Claude 5 audit above is the most recent;
   repeat it against Anthropic's current guidance rather than this document's
   summary of it.

The migration preserves the former root `CLAUDE.md` as
`AGENT_FIELD_NOTES_LEGACY.md` and the replaced Claude skills and commands under
`.agent-history/`. They remain searchable evidence without occupying every
session or silently acting as current policy.

The success metric is not how much guidance the repository contains. It is
whether agents produce correct, reviewable work while consuming less context and
leaving less verification and interpretation work for the next person.
