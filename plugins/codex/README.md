# flowstate-plugin-codex

One task, `codex.exec`: a single bounded run of OpenAI Codex, over
[github.com/picatz/openai/codex](https://github.com/picatz/openai) - a Go
wrapper around the real `codex` CLI, not a client of any codex network
protocol. This is the demonstration issue #162 asks for: flowstate
orchestrating an AI agent as a durable workload, with the same bounds,
secret handling, and failure classification every other task in this
repository has to earn.

An example that runs it lives at
[`examples/plugins/codex`](../../examples/plugins/codex); read that first if
you want to see it work rather than read about it.

## Sources of truth, pinned

Two upstream projects informed this plugin's design, and they disagree in
places - both are pinned in `doc.go` with the exact commit read, so a future
drift is diagnosable rather than mysterious:

- [github.com/picatz/openai/codex](https://github.com/picatz/openai) at
  `02ace0a229c75a724ede668ab405ae71405e406d` (2025-11-25) - the Go SDK this
  plugin's module graph depends on.
- [github.com/openai/codex](https://github.com/openai/codex) (the Codex
  CLI/app-server Rust sources) at
  `64bb8094ba3b2c77becea8281a4b070e05e6c758` (2026-08-04) - the actual
  protocol and config schema; source of truth over the Go SDK where the two
  disagree.

See `doc.go`, "Where the two disagree, and which one this plugin coded
against," for what was found: the Go SDK's `Args.ConfigFile` does not mean
what its name suggests (upstream's real `--config`/`-c` is a repeatable
`key=value` override, never a config-file path - this plugin does not use
that field), and the Go SDK's copy of the event schema is missing a couple
of newer item kinds and usage fields upstream has added, which `doc.go`
covers under "Defensive parsing."

## Building

```console
go build -o /path/to/plugins/flowstate-plugin-codex ./plugins/codex
```

## Configuration

Four environment variables, all read by the worker process this plugin runs
inside - never by a Flowfile, and none ever searched on `$PATH`:

- `FLOWSTATE_CODEX_BIN` (**required**) - the absolute path to a real `codex`
  binary. `codex.exec` refuses every call, and this plugin's own health
  check reports not-serving, without it. See `doc.go`, "Why this plugin
  execs a subprocess," for the fuller argument, and `binary.go` for the
  checks applied to it (absolute, exists, not a directory, executable).
- `FLOWSTATE_CODEX_WORKDIR_ROOT` (optional) - the directory `working_context`
  inputs are jailed inside. Unset means `working_context` is refused
  outright for every call - not defaulted to the worker's own working
  directory or filesystem root, which would make an unset root the
  *permissive* case instead of the restrictive one. See `binary.go`'s
  `resolveWorkingContext`.
- `FLOWSTATE_CODEX_BASE_CONFIG` (optional) - an operator-provided base
  `config.toml` (or a fragment naming just `sandbox_mode` and
  `sandbox_workspace_write.network_access`) that raises the ceiling a
  task's own `sandbox_mode` and `allow_network` inputs may narrow within.
  Unset means the ceiling is this plugin's own fail-closed baseline:
  `SANDBOX_MODE_READ_ONLY`, no network. See `policy.go` and doc.go's "Codex
  configuration" for the full three-layer design - a request over the
  ceiling is refused as `sdk.InvalidInput`, never silently downgraded.
- `FLOWSTATE_CODEX_GIT_BIN` (optional) - a real `git` binary, used only to
  render `patch` (a unified diff) after a `WORKSPACE_WRITE` or
  `DANGER_FULL_ACCESS` run that reported changed files. Unset means `patch`
  is always empty; `files_changed` (from codex's own event stream) is
  reported either way. See `diff.go`.

Every codex run gets its own ephemeral `CODEX_HOME` - a fresh, empty
directory this plugin creates per call, holding nothing but
`FLOWSTATE_CODEX_BASE_CONFIG`'s own bytes if that variable is set, and
destroyed when the run ends. codex never sees the worker user's own
`~/.codex/config.toml` or `auth.json` - see `ephemeral.go` and `process.go`.

## Examples, kept honest

The file below is pasted in whole, not summarized, and
`TestReadmeExamplesMatchTheFilesOnDisk` in this package holds it to the real
file byte for byte in both directions - a file added under
[`examples/plugins/codex`](../../examples/plugins/codex) with no matching
block here fails the build, same as a block that drifts from its file. The
convention: an HTML comment naming the file, on the line immediately before
the fence.

<!-- example: examples/plugins/codex/workflow.yaml -->
```yaml
edition: v2026.3
name: codex-exec
description: Runs one bounded OpenAI Codex agentic turn with the "codex" plugin - a read-only question, no filesystem or network access granted to the agent.

# codex.exec is the "codex" plugin's one task - a single bounded run of
# OpenAI Codex over the codex CLI. The dot in "codex.exec" is what marks
# this as a plugin task rather than a built-in: no built-in task has one.
# The engine has never compiled codex.v1.ExecInputs; it learns the shape
# from descriptors this plugin ships in its manifest at launch. See
# plugins/codex for the source and plugins/codex/README.md for what this
# plugin needs configured on the worker before this file can run for real.
#
# sandbox_mode is written explicitly here as SANDBOX_MODE_READ_ONLY, even
# though that is also this task's own default when the input is left unset
# - see codex.proto's own doc comment on the enum for why an author should
# still write it: a Flowfile that names its own sandbox is legible without
# reading this plugin's source to know what "unset" means today.
vars:
  prompt: In one sentence, what does the term "idempotent" mean in the context of a retried HTTP request?
steps:
  - id: ask
    codex.exec:
      api_key: ${secret('env:OPENAI_API_KEY')}
      prompt: ${vars.prompt}
      sandbox_mode: SANDBOX_MODE_READ_ONLY
  - id: announce
    log:
      message: '${"codex answered in %d output token(s): %s".format([steps.ask.output_tokens, steps.ask.final_message])}'
outputs:
  final_message:
    value: ${steps.ask.final_message}
    description: the agent's own answer to the prompt
  output_tokens:
    value: ${steps.ask.output_tokens}
    description: tokens the model spent producing that answer
```

## The library: what it actually is

Read before a line of this plugin was written (see `doc.go` for the file
list): `github.com/picatz/openai/codex` builds a `codex exec --json [flags]`
command line, starts it with `os/exec`, writes the prompt to its stdin, and
decodes one JSON event per line from its stdout. It is not a client of a
codex "app-server" or any network protocol of codex's own - every input
this plugin's schema exposes (`model`, `sandbox_mode`, `working_context`) is
a CLI flag, not a request body field. That single fact decided most of this
plugin's design:

- **A subprocess, on purpose, unlike `plugins/vcs`.** `plugins/vcs/doc.go`'s
  "No subprocesses, ever" is a rule about what a version-control task needs
  to do, backed by go-git giving that plugin a pure-Go alternative. There is
  no such alternative here - running an agent binary is not incidental to
  this task, it is the task. `doc.go` answers each of `plugins/vcs`'s three
  reasons for avoiding `exec` directly rather than treating them as
  unavoidable once a subprocess is involved.
- **`sandbox_mode` mirrors the CLI's own three levels** (`read-only`,
  `workspace-write`, `danger-full-access`) because those are the only three
  that exist. The schema's own default, and this task's own default when
  the input is left unset, is the most restricted one - fail closed, never
  "whatever the CLI happens to default to."
- **`working_context` is `--cd`.** There is no way to hand the library file
  content directly; a working directory is a path on disk the CLI is
  pointed at. See "Configuration" above for how that path is jailed.
- **`patch` is not something the library gives you.** The CLI's own
  `file_change` events report which files changed and how (add/delete/
  update), never a diff of the content - so this plugin renders one itself,
  with a real `git diff` against a checkout that was already there before
  the run started (see `diff.go`). This plugin creates no checkout of its
  own, for the same no-shared-workspace reason `plugins/vcs` has no
  `vcs.clone`. `patch` is deliberately the same unified-diff shape
  [`plugins/git`](../git)'s own `git.commit_push` task accepts as its
  `patch` input (`CommitPushInputs.patch`) - the #162 agentic-loop contract:
  `codex.exec.patch` flows straight into a `git.commit_push` step with
  nothing in between reading or writing a shared checkout.
- **Those `git` invocations run over a repository the task controls**, so
  they are hardened on both sides (`githarden.go`). The environment is built
  from an explicit allowlist rather than inherited, which is what keeps
  `GIT_DIR`, `GIT_EXTERNAL_DIFF`, `GIT_SSH_COMMAND`, `GIT_CONFIG_*` and a
  `~/.gitconfig` a `DANGER_FULL_ACCESS` run just wrote out of the picture;
  the repository's own config is judged by an allowlist, so a key that names
  a program and is *not* recognized costs the run its patch rather than
  running the program. The cost is stated where it is paid: an unusual but
  harmless config key, a linked worktree, a submodule checkout, or a
  `working_context` that is a subdirectory of a larger repository all get no
  patch, and `files_changed` still reports what the run touched.
- **The library's own subprocess launch (`Exec.Run`) is not what this plugin
  calls.** `process.go` builds the same argv shape but constructs the
  child's environment from an explicit allowlist rather than a copy of this
  plugin process's own `os.Environ()` - see `doc.go`, "Why this plugin does
  not use Exec.Run for the process launch." The library's `ThreadEvent`/
  `ThreadItem` decoding types are still used as-is.

## Secrets: the first task built entirely against secret_inputs

`api_key` is declared in `codex.exec`'s `secret_inputs` (see `main.go`), so
a Flowfile writes `api_key: ${secret('env:OPENAI_API_KEY')}` and this task's
`Fn` receives the resolved value directly - the host resolves the reference
under the caller's identity before this task ever runs (see
`pkg/flowstate/v1/plugin/task.go`'s `resolvePluginSecretInputs`), and this
plugin process never holds a `flowstate.v1.SecretRef` or a secret provider
of its own.

This is a real difference from `plugins/vcs` and `plugins/github`, both of
which predate `secret_inputs` and each stand up their own secret scheme
(`vcs:...`, `github:...`) resolved from the worker's own environment. This
plugin has no `Secrets` field in its `sdk.Plugin{}` at all - there is
nothing for it to resolve.

What `secret_inputs` does not do, and what this plugin does on top of it,
belt over suspenders: every task-level error and every output field this
plugin returns is passed through a `secrets.Scrubber` registered with the
resolved `api_key` before the value is ever used (`exec.go`), independent
of the scrubbing the host already applies to a plugin's response. See
`errors.go`'s own note on why the scrub happens *before* an error is
classified with an `sdk` constructor, not after - the two orders look
interchangeable and are not, because `secrets.Scrubber.ScrubError`'s result
answers `errors.Is` but never `errors.As`, and the sdk's own retry
classification is read with `errors.As`.

### SDK gap this plugin found

`secret_inputs` stops the *reference* from ever crossing into a plugin
process unresolved. It does not stop a Flowfile author from writing
`api_key: "sk-literal"` directly: by the time this task's `Fn` runs, a
value that started as a literal and a value the host just resolved from a
reference are the same shape, an already-resolved `flowstate.v1.Value`
holding a literal string, and nothing in the wire format lets this task
tell them apart. `plugins/vcs`'s own `tokenFromValue` can refuse a literal
because that plugin resolves its own scheme and sees the `SecretRef` before
resolving it; a task built against `secret_inputs` cannot do the equivalent
check itself. See `exec.go`'s `apiKeyFromValue` for where this is recorded
in code, not only here.

## Bounds

Every one of these exists because the resource it bounds is one an attacker
- a Flowfile author, a prompt, or the model's own output - controls (see
`bounds.go` for the full list and reasoning on each):

| Bound | What it limits |
| --- | --- |
| `maxPromptBytes`, `maxModelBytes`, `maxWorkingContextBytes` | Input string sizes, checked before anything reaches the subprocess |
| `max_output_bytes` (default 256 KiB, ceiling 4 MiB) | The combined size of `final_message`, `patch`, and every event summary |
| `max_events` (default 200, ceiling 2000) | How many `EventSummary` entries `codex.exec` returns - enforced at collection time, not only when the response is built |
| `maxFinalMessageBytes`, `maxEventSummaryBytes` | Individual text fields, so one enormous field cannot consume the whole output budget |
| `maxPatchBytes`, `maxDiffFiles` | The rendered diff and its file list, independently - a rename-heavy run can have many files and a small patch, or the reverse |
| `maxSubprocessBytes` (32 MiB) | The codex CLI's combined stdout, applied *below* the library's own JSON decoding via a wrapping `io.Reader` - the same "the bound belongs on the transport, not inside a library call whose error paths you don't control" lesson CLAUDE.md draws from connect-go's non-200 unmarshaler gap |
| `runTimeout` (10 minutes) | Backstops a hung subprocess, independent of and in addition to the step's own `timeout:` |

A request over any ceiling is refused rather than silently clamped - the
same reasoning `plugins/vcs/validate.go`'s `clampMaxCommits` gives for its
own ceiling: a silently reduced bound looks like a working request that
quietly returns less than it asked for.

## Errors

| Situation | Classification | Why |
| --- | --- | --- |
| The subprocess deadline expires on a read-only run, or one that never started acting on `working_context` | `sdk.Unavailable` | Nothing external changed; safe to retry |
| The subprocess deadline expires *after* a command or file change had already started, in a mutating run | `sdk.OutcomeUnknown` | May have already taken effect; a blind retry could repeat or compound it |
| codex's stderr mentions a rate limit | `sdk.UnavailableAfter(30s, ...)` | Retryable, with a fixed delay - the library gives no machine-readable `Retry-After` to forward instead |
| codex's stderr mentions an authentication failure | `sdk.PermissionDenied` | Permanent: the same credential fails the same way again |
| The subprocess exits abnormally, no side effect yet observed | `sdk.Failed` | Permanent, cause unclear |
| The subprocess exits abnormally, after a side effect | `sdk.OutcomeUnknown` | Same reasoning as the deadline case above |
| A `turn.failed` event | `sdk.Failed` | The CLI itself reported the turn did not succeed |

See `errors.go` for the full reasoning, including why the rate-limit and
auth classifications are a text-match heuristic against the library's own
unstructured stderr rather than a typed error - the library gives none, the
same gap `plugins/github/errors.go` documents falling back to a status code
for.

## What still needs live testing

Everything in this plugin is exercised against `testdata/fakecodex`, a
small Go program standing in for the real CLI - real `os/exec`, a real
pipe, real exit codes, but scripted events rather than a real model. Nothing
here has run against a live `codex` binary talking to the real OpenAI API.
Named plainly rather than papered over - see `doc.go`'s own "What still
needs live testing" for the specifics, in short:

- Whether `--skip-git-repo-check`, `--cd`, and the `-c
  sandbox_workspace_write.network_access=...` override actually compose the
  way this plugin's reading of codex's own flag/config precedence predicts.
- Whether an ephemeral `CODEX_HOME` with no `auth.json` and no `config.toml`
  (the default-policy case) behaves the way `CODEX_API_KEY` alone is
  expected to, rather than falling back to an interactive login prompt.
- The rate-limit and auth-failure text-match heuristics in
  `errors.go`'s `classifyRunError`, against real stderr wording.
- Real token usage numbers, given the Usage fields upstream added that this
  plugin does not yet surface.

## What proves this plugin's task is reachable

`TestAFlowfileCanNameTheCodexPluginsTasks`, in
[`plugins/codex/reachable`](reachable), builds this plugin as a real,
separately compiled binary, opens a
[`plugin.Host`](../../pkg/flowstate/v1/plugin) over it, and validates
[`examples/plugins/codex/workflow.yaml`](../../examples/plugins/codex/workflow.yaml)
from disk before and after registration - refused with a diagnostic naming
`codex.exec` beforehand, accepted afterward, its inputs checked against the
descriptors the plugin actually shipped, and its own spoofing check proving
the task's qualifier comes from binary discovery rather than from what the
plugin calls itself in its own manifest. It lives in its own package,
`plugins/codex/reachable`, rather than beside `main.go`, for the same
reason `plugins/vcs/reachable` and `plugins/github/reachable` do - see that
package's own doc comment for the registry-pollution failure mode this
avoids.

It does not run `codex.exec` for real: that reaches the real OpenAI API
through a real `codex` binary and costs real tokens, neither of which a
reachability test has any business doing. `exec_test.go`, `errors_test.go`,
`diff_test.go`, and `scrub_test.go` (in this module's root package)
exercise the task function itself against `testdata/fakecodex`, a small Go
program standing in for the real CLI - a real subprocess, a real pipe, real
exit codes, none of the network dependency.
