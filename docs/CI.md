# CI: what runs, what decides, and what the owner has to switch on

This document exists because half of the design lives in repository *settings*
rather than in files, and a design half of which nobody can read is a design
that gets undone by the next person to look at the settings page.

## The problem, observed rather than theorised

On 2026-08-15, pull request #659 changed one markdown file — `CLAUDE.md` — and
ran `appearance`, `fuzz-smoke`, `proto`, `staticcheck` and `test`. None of those
can be affected by that file. Six minutes of wall clock and twenty-one billed
job-minutes, to learn nothing that was not already known.

The same evening, around twenty-five pull requests merged, each serialized
behind a full `main` CI cycle. That serialization was not superstition: #489
recorded four pull requests merged inside ninety seconds breaking `main`,
because a pull request's CI proves only *that branch merged with the main that
existed when the run started*. But the fix #489 earned was never applied. What
shipped instead was a person waiting between merges — a mechanism made of
attention, costing roughly six minutes each time, and paid twenty-five times.

And the rule those merges were being held to — "nothing merges red" — was not
enforced by anything. A curation pass on 2026-08-15 found a single ruleset on
this repository, `Copilot review for default branch`, with no
`required_status_checks` rule anywhere. Every green check was advice.

## The design

### One computation, three tiers

`tools/gate` already knew how to answer "what can this diff reach": changed
files against the merge-base with `origin/main`, mapped to Go packages, expanded
to every package whose build *or tests* can see a changed one, plus the data
dependency the import graph is blind to (packages whose tests read `examples/`
off disk — #589).

CI now asks that same code the same question, with `go run ./tools/gate -ci`,
and skips the jobs the answer excludes.

#### When there is no merge-base

The one input that computation needs is a merge-base, and the environments this
gate most needs to run in are the ones least likely to have one. A clone made
with `--single-branch` has no `origin/main` ref; one made with `--depth` has the
ref and none of the history it shares with the branch under review. The gate
used to exit with an error naming the fix — `git fetch origin main` — which is
correct advice and not something a CI job or an agent working in a supplied
checkout is in a position to act on. Seven pull requests in one wave reported
that refusal, and all seven were opened with nothing verified.

So `resolveBase` repairs what it can, cheapest first: use the ref, else fetch
the branch, else deepen a shallow clone. Each step is reached only because the
one before it produced no merge-base, so a normal checkout pays for one
`git merge-base` and nothing else.

When none of that works — no network, no remote — the gate runs anyway, with
every tracked file treated as changed. That answer goes through `buildPlan` like
any other, which sets `moduleWide` and `ciWide` and selects every conditional
leg, so the widest plan is reached by the one computation rather than by a
second code path meaning "everything". Wide and slow beats narrow and wrong:
overrunning the true scope costs time, while underrunning it passes commits the
checks reject. It is the same reasoning that makes the merge queue ignore the
plan entirely.

`ciForceReason` carries that as its fourth forcing, beside the event, the
harness and the module — because "must this ignore the diff" is one question and
docs/CI.md's whole argument is that it has one answer. It is there for the
*reason* rather than the scope: the whole tree would force a wide run through
`ciWide` regardless, and a run told it is wide because the workflows changed
sends a reader looking for a workflow diff that does not exist.

| tier | scope | what decides it |
|---|---|---|
| `go run ./tools/gate` | local, before a push | the diff |
| CI on `pull_request` | the `plan` job | the diff |
| CI on `merge_group` and `push` to main | everything | nothing; the full set always runs |
| `make check` | everything, locally | nothing; the full rehearsal |

The alternative — `paths:` filters in `ci.yml` — is the same value written down
twice, in the venue where the copy cannot be unit tested, cannot see the import
graph, and cannot see that a package with no import of `examples/` nonetheless
breaks when an example changes. `tools/gate/ci_test.go` pins the workflow's job
list, each job's `if:` expression, and the `verdict` job's `needs:` against the
plan, so the two halves cannot drift apart without a test failing.

### Why the conditional jobs are not the required checks

GitHub's semantics for a conditional required check fail in *both* directions,
and the more dangerous one is the default:

- A required job skipped by an `if:` reports the conclusion `skipped`, and a
  required status check counts `skipped` as satisfied. Make the six
  conditional jobs required, and a plan that wrongly skips `test` produces a
  green tick on a pull request nothing tested. **A gate that passed on
  something it never looked at.**
- A required check that never reports at all — what a workflow-level `paths:`
  filter produces, because the whole workflow is skipped and emits no check
  runs — stays pending forever and blocks the merge with nothing able to clear
  it.

So neither is available, and the required checks are two jobs that always run:

- **`plan`** computes the answer and publishes it, both as one boolean per job
  (what the `if:` expressions read) and as one JSON object (what `verdict`
  reads). Deliberately the same object, so a verdict cannot agree with a plan
  nobody ran.
- **`verdict`** runs `if: always()`, so it reports on every run including ones
  where something failed, and it re-reads the plan rather than trusting a tick.
  It fails unless:
  1. the `plan` job itself succeeded and published a non-empty set of
     obligations — an empty one would be vacuously satisfied;
  2. every job the plan **selected** has result `success` — a `skipped`,
     `failure`, `cancelled` or absent result is red;
  3. every job the plan **did not select** has result `skipped` — a job that
     ran anyway proved more than was asked, and still fails, because the only
     way it happens is that the plan and that job's `if:` disagree about which
     rule decides, and the next disagreement will not be in the safe direction;
  4. every job `verdict` can see was one the plan decided.

Point 4 has a blind spot `verdict` cannot close from inside the run: a job added
to `ci.yml` and to neither the plan nor `verdict`'s `needs:` is invisible to it.
`TestTheWorkflowAndThePlanDecideTheSameJobs` closes it before a push instead.

`tools/gate/verdict_test.go` extracts that script from `ci.yml` — read, not
copied — and executes it against each of those shapes, so "a skipped required
check cannot fail open" is a claim with a test behind it rather than a paragraph.

`appearance` carried `continue-on-error: true` for the 48-hour window every new
check gets, during which its `needs` result was `success` even when it failed and
`verdict` could not distinguish those — that is what advisory means. The window
lapsed on 2026-08-12 and the flag came off on 2026-08-31 (#1319), so its result now
reaches `verdict` like every other planned job's.

### The merge queue

`merge_group` is the mechanism #489's lesson deserved. GitHub builds the
*prospective* merge — this pull request on top of everything ahead of it — and
runs the required checks against that, which is exactly the artifact nothing was
testing. It batches, so N pull requests cost roughly N/batch-size runs instead
of N serialized cycles, and a failure ejects the offending pull request rather
than breaking `main`.

Every `merge_group` run is a **full** run (`ciForceReason` in
`tools/gate/ci.go`). The diff-scoping is a pull-request-lane optimisation; the
queue is the last gate before `main` and the one place where being wrong about
the plan is unrecoverable, so `main`'s protection never rests on the plan being
right.

`cancel-in-progress` excludes merge groups for a sharper reason than it excludes
`main`: cancelling a merge group does not save work, it strands the queue —
the required checks for that group never report and everything behind it waits
on an answer that is never coming.

### `govulncheck` stays legible

`vulncheck` skips when no Go package is affected, and that is safe only because
of where it still runs: **every** push to `main`, **every** merge group, and the
weekly `deep.yml` schedule, each fetching the advisory database at run time. A
new advisory therefore still arrives on a calendar rather than waiting for
somebody to touch a `.go` file. What the skip removes is `govulncheck` being
reported against the pull request that renamed a heading — which is the shape
that made GO-2026-6061 look like an unrelated author's problem.

Nothing caches the advisory database, and nothing should.

### Federation lives outside the plan entirely

The real-issuer OIDC check (`TestRealCIToken`, verified against
`token.actions.githubusercontent.com`) used to be a `federation` job here,
conditional on the auth package being affected like any other narrow job. It
moved to `federation.yml`, triggered by `push: [main]` only, because the gate
that mattered was never "does this diff touch auth" — it was "does this job's
runner check out code nobody has reviewed yet while holding `id-token: write`."
An `if:` cannot close that gap: the job still has to check out the pull
request's head to *evaluate* the condition, so the privileged permission and
the untrusted checkout were in the same job regardless of which files changed.
Running the live check only after code has reached the protected branch removes
the precondition instead of gating around it, and it is why `federation` is no
longer one of the plan's outputs or one of `verdict`'s `needs:` — a workflow
with no `pull_request` trigger never produces a check run for this repository's
required-status-checks list to see, so it participates in neither.

### Two more workflows outside the plan

`editors.yml` (**Editors**) runs on pushes to `main` and on pull requests, in two
jobs: *Neovim LSP smoke* drives a real, pinned Neovim through
`tools/editorsmoke/probe.lua` against `flow lsp` and asserts the fenced
configuration in `docs/EDITORS.md` is byte-identical to the file it loads; *VS
Code extension* builds and tests the extension. Neither is one of the plan's
outputs, because what they verify is an editor, not a Go package the plan can
reach from the import graph, and neither is a required check.

`release.yml` (**Distribution rehearsal**) is `workflow_dispatch` only. It
builds archives, SBOMs and checksums through `tools/release` and uploads them as
a same-run payload; its publication job is interlocked off (`if: false && …`,
with a sibling job that explains the interlock) until releases are switched on
deliberately — #1216 carries that decision. It produces no check run for a pull
request and is not in `verdict`'s `needs:`.

### Caching

`actions/setup-go` derives its cache key from a hash of `cache-dependency-path`,
defaulting to `go.sum`. Six jobs asked for one key, and `actions/cache` reserves
a key for the first writer and warns the rest — so exactly one job's build cache
was saved per run, and which one was a race between finishing times. The
measured effect on run 31909221065: `vulncheck` scanned in **5s** against a warm
`govulncheck` build, while `staticcheck` spent **85s** rebuilding staticcheck
from source and `proto` spent **52s** rebuilding `buf`, with nothing in the file
explaining the difference.

`proto` and `staticcheck` now name a file under `.github/cache-scope/` alongside
`go.sum`, which gives each a key of its own. The files' contents are arbitrary
and are deliberately *not* version pins: the tool versions live once, in
`ci.yml`'s `env:` block, and Go's build and module caches are content-addressed,
so a key that fails to change after a version bump costs one cold build and a
key that changes needlessly costs one cold build. Neither can produce a wrong
answer. That is the property that makes a cache safe to key loosely — and it is
the answer to "keyed so a stale cache cannot produce a false pass": the key is
not what makes it sound, the content addressing is.

## Measured

### One pull request, before

Run `31912978683` — PR #659, one markdown file — and run `31909221065`, a
twelve-file Go change, both on 2026-08-15, per-job durations from the Actions
API:

| job | #659 (1 markdown file) | 12-file Go change |
|---|---|---|
| `test` | 373s | 373s |
| `fuzz-smoke` | 280s | 270s |
| `appearance` | 155s | 161s |
| `staticcheck` | 104s | 102s |
| `proto` | 84s | 75s |
| `federation` | 30s | 38s |
| `vulncheck` | 23s | 21s |
| **total job-time** | **1049s** | **1040s** |
| **billed job-minutes** (rounded up per job) | **21** | **21** |
| **wall clock** | **6m15s** | **6m16s** |

The two are the same run, because nothing distinguished them.

### The same two, after

- **#659, markdown only.** The plan selects nothing. `plan` (~30s) and
  `verdict` (~10s) run: **2 billed job-minutes, well under a minute of wall
  clock**, against 21 and 6m15s. The 21 minutes bought nothing, which is the
  point.
- **The 12-file Go change.** The plan selects five of seven jobs — `proto` and
  `federation` skip — for about **17 billed job-minutes**, and the wall clock
  becomes `plan` + `test` + `verdict` ≈ **6m55s** against 6m16s. That is a
  **~40s wall-clock regression on Go pull requests**, which is the honest price
  of computing the answer before acting on it, and it is stated here rather than
  buried because it is paid by the majority of pull requests.

### Thirty real merges

Rather than pick two runs, the plan was evaluated against the actual diff of
each of the last thirty commits on `main`, priced with the measured per-job
durations above:

- **6 of 30 (20%) reach no CI job at all** — `CLAUDE.md` edits, a `.claude/`
  skill, `editors/vscode` changes, a `dependabot.yml` change. Those go from 21
  billed job-minutes to 2.
- **24 of 30 run five or six of seven.** `proto` fires on one, `federation` on
  two, `appearance` on roughly half.
- **Job-time: 31,350s → 23,692s (76%).** **Billed job-minutes: 630 → 496
  (79%).**

A 21% cut in compute and, on a fifth of pull requests, an answer in under a
minute instead of six.

### What the queue is worth

The serialization it replaces is not in those numbers, because it was never
billed to CI: it was twenty-five six-minute waits by a person, roughly two and a
half hours in one evening. The queue turns that into batched runs nobody has to
watch. The compute arithmetic per merged pull request changes shape too — today
it is a full PR run plus a full `main` push run; afterwards it is a diff-scoped
PR run, an amortized share of a merge-group run, and an amortized share of a
`main` push run — so the queue only pays for itself once it batches, which is a
thing to check rather than assume after the first busy evening.

`push: [main]` is deliberately kept, even though a merge group tests the exact
tree that lands: it covers direct pushes and is the record of whether a commit
was good when it landed. Dropping it, or reducing it to a nightly, is the
obvious next saving once the queue has proven itself, and should be a separate
change.

## What the owner has to apply, in settings

**None of the following is in a file.** Everything above ships as code; this
section is the half that has to be switched on, and the design does not work
without it.

### 1. The ruleset that makes "nothing merges red" a mechanism

There is no `required_status_checks` rule on this repository today. Create one.
The existing `Copilot review for default branch` ruleset can stay as it is; this
is a second ruleset, or these rules added to that one.

```bash
gh api -X POST repos/picatz/flowstate/rulesets --input - <<'JSON'
{
  "name": "main: nothing merges red",
  "target": "branch",
  "enforcement": "active",
  "conditions": { "ref_name": { "include": ["~DEFAULT_BRANCH"], "exclude": [] } },
  "rules": [
    { "type": "deletion" },
    { "type": "non_fast_forward" },
    {
      "type": "pull_request",
      "parameters": {
        "required_approving_review_count": 0,
        "dismiss_stale_reviews_on_push": false,
        "require_code_owner_review": false,
        "require_last_push_approval": false,
        "required_review_thread_resolution": false,
        "allowed_merge_methods": ["squash"]
      }
    },
    {
      "type": "required_status_checks",
      "parameters": {
        "strict_required_status_checks_policy": false,
        "do_not_enforce_on_create": false,
        "required_status_checks": [
          { "context": "plan" },
          { "context": "verdict" }
        ]
      }
    },
    {
      "type": "merge_queue",
      "parameters": {
        "merge_method": "SQUASH",
        "grouping_strategy": "ALLGREEN",
        "max_entries_to_build": 5,
        "max_entries_to_merge": 5,
        "min_entries_to_merge": 1,
        "min_entries_to_merge_wait_minutes": 1,
        "check_response_timeout_minutes": 20
      }
    }
  ]
}
JSON
```

Five parameters there are load-bearing and easy to get wrong:

- **`required_status_checks` names `plan` and `verdict` and nothing else.**
  Adding any of the six conditional jobs reintroduces exactly the fail-open
  case the design removes, because a skipped required check counts as
  satisfied. If you want more assurance, add it to `verdict`, not to this list.
- **Do not add the `Editors` workflow's jobs** (`neovim`, `vscode`). That
  workflow filters on `paths:`, so on a diff it does not match it produces no
  check runs at all, and a required check that never reports blocks the merge
  forever with no way to clear it. It is intentionally not required.
- **`strict_required_status_checks_policy: false`.** "Require branches to be up
  to date before merging" is the *old* answer to #489 and it fights the queue:
  it would force a rebase and a re-run per merge, which is the serialization
  written into settings. The queue is what handles staleness now.
- **`min_entries_to_merge: 1` with a one-minute wait.** A larger minimum makes
  a lone pull request sit idle waiting for company. Batching still emerges
  naturally, because entries arriving while a group is building join the next
  one, and `max_entries_to_build: 5` is what caps the group.
- **`check_response_timeout_minutes: 20`** must exceed the slowest job. `test`
  budgets 15 minutes and `appearance` 15; 20 leaves room without letting a
  wedged group hold the queue indefinitely.

Optionally pin the check provider by adding `"integration_id": <GitHub Actions'
app id>` to each entry, which stops any other app reporting a context by that
name. Read the id off an existing check run rather than typing one from memory.

### 2. Turn the merge queue on for the branch

The `merge_queue` rule above is what enables it. Confirm afterwards that the
repository's merge-queue settings show `Squash and merge`, matching
`allowed_merge_methods`, and that `Merge when ready` appears on a pull request.

### 3. Nothing else changes

`allow_auto_merge` is currently `false` and can stay so — the queue's "merge
when ready" replaces it. `delete_branch_on_merge` is unrelated.

### After applying

Open one throwaway pull request touching only a markdown file and check three
things: `plan` reports and its summary table says every job is skipped;
`verdict` reports green; and the pull request is mergeable via **Merge when
ready** rather than a direct merge. Then open one touching a `.go` file and
check that the same two checks are the only required ones while five to six
jobs run beneath them.

## What could not be verified locally

Stated plainly, because a CI change that claims more verification than it had is
the same category of mistake as a gate that passes without looking.

- **The `pull_request` path has run; the merge queue has not.** `act` is not
  available in the authoring environment, so nothing here was executed locally
  — the YAML parses, `tools/gate -ci` was run against real diffs, and the
  `verdict` script was executed by `tools/gate/verdict_test.go` under `bash`
  and `jq` with synthetic inputs. But this PR's own `pull_request` runs have
  since exercised the real thing: `plan`, all seven conditional jobs, and
  `verdict` have each executed successfully on GitHub's runners. What remains
  unverified is specifically the `merge_group` path — the full, unconditional
  set the force branch in `ciForceReason` selects — since nothing merges
  through a queue until this repository's branch protection is configured to
  require one.
- **The `after` numbers are computed, not observed.** They multiply *measured*
  per-job durations from runs `31912978683` and `31909221065` by the plan's
  decisions on *real* diffs. What is modelled is the `plan` job's own cost
  (~30s, from a locally measured `go list` plus typical checkout and setup-go
  timings on this repository's runs) and `verdict`'s (~10s, one bash step, no
  checkout).
- **The cache-key change is reasoned, not measured.** It rests on
  `actions/setup-go` deriving its key from `hashFiles(cache-dependency-path)`.
  If that reading is wrong, the keys stay shared and nothing is worse than
  today. Confirm by comparing the `staticcheck` job's "Static analysis" step
  against the 85s baseline after the second run on `main`.
- **Merge-queue behaviour is from the documented contract**, not from watching a
  queue run here. In particular, that required checks are evaluated against the
  `merge_group` ref, and that batching follows `max_entries_to_build`, should be
  confirmed on the first busy evening.

## Considered and excluded

- **Test sharding, and `-count` tuning.** `test` is the long pole at 6m13s.
  Sharding would cut wall clock and *increase* job-minutes, and every shard is
  another name someone will be tempted to add to the required list. The
  affected-set skip already removes the whole job on the diffs that cannot
  reach it, which is the larger win, and `make test` staying one command is
  what keeps CI and the local rehearsal from disagreeing about what "the tests"
  means.
- **Diff-scoping the `test` job's own package list.** The same objection, one
  level worse: `make test` is what the Makefile, `make check` and CI all run,
  and splitting it would put the "one value written down twice" defect inside
  the test command itself. The local gate already runs diff-scoped tests.
- **Folding `Editors` into the plan.** Its whole design is that it shares
  nothing with the Go jobs — not a cache, not a toolchain, not a workflow file
  — and a `plan` job there would be a third place computing the same thing. Its
  `paths:` filter over-runs rather than under-runs (it fires on all of
  `cmd/flow` and `pkg/flowstate/v1/flowfile`, which is coarse but never wrong in
  the dangerous direction), and it is not a required check. Left alone
  deliberately.
- **Reviewer timing.** Copilot already reviews once per pull request rather than
  once per push — the `Copilot review for default branch` ruleset sets
  `review_on_push: false` — so the noise on a docs typo is one review, not one
  per commit. Codex is not configured through a repository ruleset and is not
  reachable from this repository's files.
- **`appearance` running only when printed output can change.** Adopted, not
  excluded: it is one of the six conditional jobs, and its trigger is the
  package question rather than the path one, for the reason `needsDocs` gives —
  the goldens record what the `cmd/flow` *binary* prints, so its dependency
  closure is the real source set.
