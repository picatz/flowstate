# A task a plugin provides: codex.exec

[`workflow.yaml`](workflow.yaml) asks OpenAI Codex a one-sentence question
with `codex.exec:` - the `codex` plugin's one task, a single bounded
agentic turn run over the real `codex` CLI, sandboxed to `SANDBOX_MODE_READ_ONLY`
(codex.exec's own default even when left unwritten - see
`plugins/codex/proto/codex/v1/codex.proto`'s doc comment on the enum).

Nothing about this step is special: it takes inputs, produces outputs a
later step reads, and its schema is checked before it runs. What is special
is that the schema belongs to the plugin - the engine has never compiled
`codex.v1.ExecInputs`, and learns the shape of `prompt`, `sandbox_mode`,
`final_message`, and so on from descriptors the plugin ships in its
manifest and hands over at launch. See
[`pkg/flowstate/v1/plugin/examples/flowstate-plugin-example`](../../../pkg/flowstate/v1/plugin/examples/flowstate-plugin-example)
for the smallest worked version of that mechanism, and
[`plugins/codex`](../../../plugins/codex) for this one's actual source.

## Why this one cannot run with no arguments

A few plugin examples in this repository ([`vcs`](../vcs),
[`github`](../github)'s read-only one) make an unauthenticated request and
run as committed. This one cannot: OpenAI's API requires a credential for
every request, codex.exec's `api_key` is not optional in practice the way
`vcs.log`'s `token` is for a public repository, and the task also needs a
real `codex` binary a worker has been told about explicitly (see
`plugins/codex/README.md`, "Configuration" - there is no `$PATH` fallback,
deliberately). This file is still safe to read as written: it names no
default owner, repository, or secret value, and posts nothing anywhere -
`codex.exec` in `SANDBOX_MODE_READ_ONLY` cannot write to disk or reach the
network from a shell it starts, only the model call itself does that.

## Running it

```console
$ mkdir -p ./plugins
$ go -C plugins/codex build -o ../../plugins/flowstate-plugin-codex .
$ export FLOWSTATE_SECRET_OPENAI_API_KEY=sk-...        # resolved via ${secret('env:OPENAI_API_KEY')}
$ flow plugins --plugin-dir ./plugins
$ flow worker --allow-unversioned-interpreter --plugin-dir ./plugins \
    --plugin-env codex=FLOWSTATE_CODEX_BIN=/path/to/codex \
    --secret-env OPENAI_API_KEY --auth-policy examples/plugins/greet/auth.yaml &
$ flow server --insecure-no-auth --plugin-dir ./plugins &
$ flow run examples/plugins/codex/workflow.yaml
```

The two variables travel differently. `FLOWSTATE_SECRET_OPENAI_API_KEY` is read
by the worker's own `env:` secret provider, which `--secret-env` turns on, and
`--auth-policy` has to authorize it: a worker holding a secret provider with no
policy refuses to start ([`examples/plugins/greet/auth.yaml`](../greet/auth.yaml)
is a rehearsal policy that allows every reference). `FLOWSTATE_CODEX_BIN` is
read by the plugin, whose environment starts empty, so it is named with
`--plugin-env`; exported in the shell, it would never reach the plugin.

`flow run` refuses this file today (#1548): it checks the file against its own
build's task registry, takes no `--plugin-dir`, and so reports `codex.exec` as a
task nothing registered before the server sees it. Until that is fixed,
`flow run local` with the same plugin, secret and policy flags runs it in one
process, and an agent host running `flow mcp --plugin-dir ./plugins` submits it
to this server with `flowstate_compile` then `flowstate_run`.

`--insecure-no-auth` is what makes this a rehearsal rather than a deployment:
the server authenticates every caller as anonymous, which is only ever right on
a machine nobody else can reach. A real one passes `--auth-policy` instead, plus
`--rpc-resource` when that policy trusts an issuer minting bearer tokens.

This makes a real call to OpenAI through the codex CLI, and costs real
tokens - it will fail without a configured `codex` binary, without
`FLOWSTATE_CODEX_BIN` pointing at one, and without a valid API key behind
`FLOWSTATE_SECRET_OPENAI_API_KEY`.

## Why this is not `examples/codex/workflow.yaml`

The same reason [`examples/plugins/vcs`](../vcs) and
[`examples/plugins/github`](../github) give, and
[`examples/README.md`](../../README.md) gives in full: the corpus
enumerated as `examples/*/workflow.yaml` is checked with the built-in task
registry alone, and a file naming a plugin's task is meant to be refused by
a process that has not loaded that plugin, with a diagnostic that says so
rather than a silent pass.

## What proves this file is reachable

`TestAFlowfileCanNameTheCodexPluginsTasks`, in
[`plugins/codex/reachable`](../../../plugins/codex/reachable), is this
plugin's equivalent of `TestAFlowfileCanNameAPluginTask` for
`examples/plugins/greet` in `pkg/flowstate/v1/plugin`: it builds this
plugin as a real, separately compiled binary, opens a
[`plugin.Host`](../../../pkg/flowstate/v1/plugin) over it, and validates
this exact file from disk before and after registration - refused with a
diagnostic naming `codex.exec` beforehand, accepted afterward, its inputs
checked against the descriptors the plugin actually shipped. It does not
run `codex.exec` for real - that reaches the real OpenAI API through a real
`codex` binary and costs real tokens, neither of which this test has any
business doing.
