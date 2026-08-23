# agentic-fix

The problem: the build is broken, an agent could probably fix it, and you would
like it to try — but not forever, not with your credentials in its context, not
against a moving branch, and not silently failing at 3am with nobody told.

This is that loop, as a workflow rather than as a script wrapped around a model.

## The durability property

**Every attempt survives the process that made it.**

An attempt is a prompt, a commit, and a test run at that commit. The commit is
in the repository, the run is in history, and the carried state — what failed
last time, and what the next patch is computed against — is part of the run
rather than part of a process's memory. A worker dying between the push and the
verification loses none of it: the run resumes and asks CI about a commit that
already exists.

The gate at the end has the same property from the other side. When the budget
is spent the run waits up to a day for a person, asleep, holding everything the
attempts produced.

## Why it is shaped this way

Four decisions, each of which is a claim `workflow.test.yaml` checks:

**The budget is a `max_iterations:`, and running out of it is a branch.** The
language treats a loop that spends its whole budget without its `until:` holding
as a distinct failure — not a quiet stop, because a quiet stop is how a runaway
hides. That would end the run FAILED with nobody asked, so the loop step carries
`continue_on_error:`, which turns exhaustion into the path where `handoff`
happens. The human is a step, not a bolt-on.

**The budget is one, and that is the tooling's limit rather than a taste.** A
second attempt cannot work today: `codex.exec` computes its `patch` by diffing
`working_context` against the state it observed before the turn, and it fails
closed when that workspace starts dirty. Attempt one's edits are still sitting in
the workspace — nothing reverts them, and `git.commit_push` operates on its own
in-memory clone rather than on that directory — so attempt two would receive an
empty patch and be refused for having nothing to commit. There is no input that
resets a working context, so the loop is kept (it is the shape that generalizes,
and with a budget of one it is still what turns "did not fix it" into a handoff)
and the number is the one thing that changes when that gap closes. See *What is
still missing*.

**The patch is a value.** `codex.exec` edits a workspace and emits a `patch` — a
unified diff of what it changed; `git.commit_push` applies that `patch`. Neither
plugin knows the other exists, and the type check that they agree happens before
either process runs. That is the whole inter-plugin contract, and it is why the
agent here is not given a shell and told to commit: what it produces is data the
workflow decides what to do with. The turn runs `SANDBOX_MODE_WORKSPACE_WRITE`
with a `working_context`, because that is the only configuration under which
`codex.exec` produces a `patch` at all — a read-only turn has nothing to diff, so
the commit step would have nothing to apply.

**A build that is already green is left untouched.** The initial verification
runs first; if it passes — a stale trigger, or a flaky failure that cleared — the
whole attempt loop is skipped, and the run reports "fixed, zero attempts" having
written nothing. Asking an agent to patch a branch that needed nothing is exactly
the kind of unprompted mutation this guard exists to prevent.

**Verification binds to content, never to a name.** `verify` is called with the
sha the push returned. Calling it with the branch name would verify whatever the
branch pointed at when CI got around to it — which, on attempt four, is not
necessarily attempt three's tree. The same reasoning drives `base_ref:`: each
attempt is compare-and-swapped against the commit it was computed on, so a
second writer to the same branch is refused rather than silently forced.

**The credential is never in the agent's context.** `${secret('env:OPENAI_API_KEY')}`
and `${secret('git:token')}` are references resolved inside the task that needs
the value. The prompt this file builds carries a test log and nothing else, and
nothing about either key reaches workflow history.

One more, smaller: the agentic turn is not retried. A retried turn is a *new*
turn — new cost, new nondeterminism — so a second attempt belongs to the loop,
where it gets a prompt carrying what actually happened. `examples/agentic-loop`
argues that decision at length and is worth reading beside this one.

## Running it

In CI, and on any machine, with no plugin, no model, no forge and no network:

```console
$ flow test examples/plugins/agentic-fix/
```

Five cases: the one attempt fixes the build; it does not and a person takes over;
it does not and nobody answers the gate either; an attempt that fails outright
(the gateway down) reaching the same person by a different road; and a build that
is already green, left untouched with nobody asked. The failure cases are why the
attempt count is read through a `has(steps.attempt)` guard: a loop that spends
its budget publishes an account of every attempt, a loop that fails part way
through one publishes none, and a loop that was *skipped* leaves no `steps.attempt`
at all — so the run reports zero attempts rather than failing while evaluating its
own report. Everything except the four effects runs for real — the loop, the
carried state, the budget, the `if:`, the gate and its 24-hour deadline, which
passes in microseconds on the virtual clock.

No case scripts a second attempt, deliberately: the installed plugin could not
produce one, and a stub that pretended otherwise would be asserting behavior the
real task cannot deliver.

For real, this file needs both plugins built and a worker told where they are,
the two secrets, a CI endpoint that answers `passed`, and two pieces of setup the
workflow itself cannot do:

- **The branch must already exist**, pointing at the commit whose build is
  broken. `git.commit_push` compare-and-swaps its destination against `base_ref`,
  so it cannot create an absent branch — create it once and never let the agent
  write to a shared trunk:

  ```console
  $ git push origin <broken-sha>:refs/heads/agent/fix
  ```

- **The workspace must be a checkout** of the repo at that commit, rooted under
  the operator's `FLOWSTATE_CODEX_WORKDIR_ROOT`, since that is where the agent
  edits and from which its `patch` is diffed.

Two of the worker's environment variables are not optional here, and leaving
either unset makes the turn fail rather than degrade — both are operator
decisions the workflow cannot make for itself:

- **`FLOWSTATE_CODEX_BASE_CONFIG`** must name a codex config that permits
  workspace writes. The plugin's ceiling is fail-closed: with this unset the
  operator policy is `SANDBOX_MODE_READ_ONLY`, and a task may only narrow
  *within* that ceiling — so `propose`, which asks for
  `SANDBOX_MODE_WORKSPACE_WRITE`, is refused outright as invalid input naming
  the field, never quietly downgraded. A minimal file:

  ```toml
  # /path/to/codex-base.toml
  sandbox_mode = "workspace-write"
  ```

- **`FLOWSTATE_CODEX_GIT_BIN`** must be an absolute path to a git binary. It is
  what the plugin uses to diff the workspace, and it never falls back to `$PATH`;
  unset, `patch` comes back empty and the commit step is refused for having
  nothing to commit.

```console
$ mkdir -p ./plugins
$ go -C plugins/codex build -o ../../plugins/flowstate-plugin-codex .
$ go -C plugins/git build -o ../../plugins/flowstate-plugin-git .
$ export FLOWSTATE_CODEX_BIN=/path/to/codex
$ export FLOWSTATE_CODEX_GIT_BIN=/usr/bin/git          # no $PATH fallback; without it, no patch
$ export FLOWSTATE_CODEX_BASE_CONFIG=/path/to/codex-base.toml  # else the ceiling stays read-only
$ export FLOWSTATE_CODEX_WORKDIR_ROOT=/path/to/checkouts
$ export FLOWSTATE_SECRET_OPENAI_API_KEY=sk-...
$ export GIT_SECRET_0__TOKEN=...
$ flow worker --plugin-dir ./plugins
$ flow run examples/plugins/agentic-fix/workflow.yaml \
    --input repo=https://github.com/your-org/your-repo.git \
    --input branch=agent/fix \
    --input workspace=repo
```

and answering the gate, when it is reached, is:

```console
$ flow signal <run-id> human-review --payload '{"taken_over": true}'
```

## What is still missing

**A way to reset a working context between turns**, and it is what caps the
budget at one rather than five.

`codex.exec` produces its `patch` by diffing `working_context` against a baseline
it reads before the turn, and it fails closed — no patch at all — when that
baseline is already dirty (`plugins/codex/diff.go`, `computePatch`:
`!baseline.observed || baseline.dirty`). Attempt one leaves its own edits in the
workspace. Nothing in this workflow can revert them: `git.commit_push` builds its
commit in an in-memory clone and never touches that directory, and `ExecInputs`
has no field that re-checks-out, cleans, or otherwise resets a working context.
So attempt two would start dirty, get an empty `patch`, and be refused by
`git.commit_push`, which requires files or a patch.

That is why this example ships one attempt and says so, rather than shipping five
and stubbing a patch the installed plugin could never emit. What would close it
is a way to hand a turn a clean tree at a named commit — an input on `codex.exec`,
or a separate task that materializes a workspace — at which point the only change
here is the number in `max_iterations:`. Tracked against
[#179](https://github.com/picatz/flowstate/issues/179)'s fourth showcase file,
whose "five tries" sketch needs it.

The related, smaller gap: even with a reset, each turn's tree would have to be the
commit that attempt is computed against (`base`), not the branch tip, so the
refresh has to take a sha.

## Why it sits here

`examples/plugins/` is where a file naming a task the built-in registry does not
have belongs — a `flow` that has not loaded the plugin is *supposed* to refuse
it, and the top-level [examples README](../../README.md) explains that placement.
This one is not under `plugins/codex/` or `plugins/git/` because it belongs to
neither: what it demonstrates is a value crossing between them.
