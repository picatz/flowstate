# A task a plugin provides: vcs.log and vcs.diff

[`workflow.yaml`](workflow.yaml) reads a small public repository's recent
history and diffs its last commit, using `vcs.log:` and `vcs.diff:` - the
`vcs` plugin's two tasks, built on [go-git](https://github.com/go-git/go-git)
rather than a `git` binary the worker happens to have installed.

Nothing about either step is special: each takes inputs, produces outputs a
later step reads, and its schema is checked before it runs. What is special
is that the schema belongs to the plugin - the engine has never compiled
`vcs.v1.LogInputs`, and learns the shape of `url`, `max_commits`, `commits`,
and so on from descriptors the plugin ships in its manifest and hands over
at launch. See
[`pkg/flowstate/v1/plugin/examples/flowstate-plugin-example`](../../../pkg/flowstate/v1/plugin/examples/flowstate-plugin-example)
for the smallest worked version of that mechanism, and
[`plugins/vcs`](../../../plugins/vcs) for this one's actual source.

## Why "vcs" and not "git"

The tasks are named `vcs.log` and `vcs.diff` - not `git.log` - because
nothing about what they do is specific to git as opposed to some other
version-control backend. This build happens to speak git, because go-git is
what exists today, but a future plugin backed by `jj` could claim the same
two task names and this Flowfile would not need to change. See
`plugins/vcs/doc.go` for the fuller argument, including the one it is most
worth reading before extending this plugin: why there is no `vcs.clone`,
`vcs.commit`, or `vcs.push` in this version, and why that is a security
decision rather than a missing feature.

## Running it

A plugin is a separate executable a worker launches, so this example needs a
built binary and a worker told where to look - the same two things
[`examples/plugins/github`](../github) needs, and the same two things
[`examples/plugins/greet`](../greet) needs for the plugin that ships with the
engine itself.

```console
$ mkdir -p ./plugins
$ go -C plugins/vcs build -o ../../plugins/flowstate-plugin-vcs .
$ flow plugins --plugin-dir ./plugins
$ flow worker --allow-unversioned-interpreter --plugin-dir ./plugins &
$ flow server --insecure-no-auth &
$ flow run examples/plugins/vcs/workflow.yaml
```

`--insecure-no-auth` is what makes this a rehearsal rather than a deployment:
the server authenticates every caller as anonymous, which is only ever right on
a machine nobody else can reach. A real one passes `--auth-policy` instead, plus
`--rpc-resource` when that policy trusts an issuer minting bearer tokens.

This one makes a real, unauthenticated request to `github.com` to clone
`octocat/Hello-World` (chosen because it is small, public, and has existed
for years specifically as a test fixture) - it will fail without internet
access, the same as any of the network examples one level up.

## Why this is not `examples/vcs/workflow.yaml`

Every plugin example in this repository lives a directory deeper than the
rest, and [`examples/README.md`](../../README.md) says why in full: the
corpus enumerated as `examples/*/workflow.yaml` is checked with the built-in
task registry alone, by `flow validate examples/*/workflow.yaml` and by
several tests, and a file naming a plugin's task is meant to be refused by a
process that has not loaded that plugin:

```console
$ flow validate examples/plugins/vcs/workflow.yaml
examples/plugins/vcs/workflow.yaml:20:5: step "history": no plugin task "vcs.log" is
registered here; if the "vcs" plugin is installed on the worker this will run on, the
file is fine and this process simply has not loaded it - `flow plugins` shows what a
plugin directory provides
```

That is the correct answer from a process that has not been told about this
plugin, and it is worth keeping correct rather than growing an exception for
this directory.

Telling it is the other half, and it is a flag rather than an exception:
`flow validate --plugin-dir <dir> examples/plugins/vcs/workflow.yaml` launches
the plugins there and checks this file against the tasks and input schemas they
provide, and `flow tasks --plugin-dir <dir>` lists them with the plugin each one
came from (#724, #710). Build this plugin first — see
[`plugins/vcs`](../../../plugins/vcs), which is a module of its own.

## What proves this file is reachable

`TestAFlowfileCanNameTheVCSPluginsTasks`, in
[`plugins/vcs/reachable`](../../../plugins/vcs/reachable), is this file's
equivalent of `TestAFlowfileCanNameAPluginTask` for `examples/plugins/greet`
in `pkg/flowstate/v1/plugin`: it builds this plugin as a real, separately
compiled binary, opens a [`plugin.Host`](../../../pkg/flowstate/v1/plugin)
over it, and validates this exact file from disk before and after
registration - refused with a diagnostic naming `vcs.log` beforehand, accepted
afterward, its inputs checked against the descriptors the plugin actually
shipped. It lives in its own package under `plugins/vcs` rather than beside
`main.go`, and rather than in `pkg/flowstate/v1/plugin`: not in the root
module, because that module must never depend on go-git, and not beside
`main.go`, because that file imports this plugin's own generated types,
which would register its schema in the test binary's own global proto
registry before the test ever ran - see the package doc on
`plugins/vcs/reachable` for what that would have hidden. It does not run
`vcs.log` or `vcs.diff` for real, since both reach a real repository over
HTTPS and that is not what this test is for.
