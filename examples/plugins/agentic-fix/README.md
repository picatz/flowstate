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

Five cases: fixed on the second attempt; five attempts and a person takes over;
five attempts and nobody answers the gate either; an attempt that fails outright
(the gateway down) reaching the same person by a different road; and a build that
is already green, left untouched with nobody asked. The failure cases are why the
attempt count is read through a `has(steps.attempt)` guard: a loop that spends
its budget publishes an account of every attempt, a loop that fails part way
through one publishes none, and a loop that was *skipped* leaves no `steps.attempt`
at all — so the run reports zero attempts rather than failing while evaluating its
own report. Everything except the four effects runs for real — the loop, the
carried state, the budget, the `if:`, the gate and its 24-hour deadline, which
passes in microseconds on the virtual clock.

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

```console
$ mkdir -p ./plugins
$ go -C plugins/codex build -o ../../plugins/flowstate-plugin-codex .
$ go -C plugins/git build -o ../../plugins/flowstate-plugin-git .
$ export FLOWSTATE_CODEX_BIN=/path/to/codex
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

One thing this file cannot yet express, named rather than faked: keeping the
`workspace` in step with the commit each attempt is computed against. The agent
edits whatever the operator's `working_context` holds, and `git.commit_push`
applies the resulting `patch` against `base` — so if the workspace does not track
`base` across attempts, a later patch may not apply. A production version would
re-checkout the workspace at `base` before each turn; there is no task that does
that today, so the example assumes a workspace at the tip being fixed and leaves
the per-attempt refresh as the gap.

## Why it sits here

`examples/plugins/` is where a file naming a task the built-in registry does not
have belongs — a `flow` that has not loaded the plugin is *supposed* to refuse
it, and the top-level [examples README](../../README.md) explains that placement.
This one is not under `plugins/codex/` or `plugins/git/` because it belongs to
neither: what it demonstrates is a value crossing between them.
