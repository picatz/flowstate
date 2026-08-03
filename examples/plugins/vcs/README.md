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
$ flow server &
$ flow run examples/plugins/vcs/workflow.yaml
```

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

## What this does not exercise, and why

This file is not driven by an automated test that builds the plugin,
launches it, and runs the workflow against it the way
`TestAFlowfileCanNameAPluginTask` does for `examples/plugins/greet` in
`pkg/flowstate/v1/plugin`. Writing its equivalent for `plugins/vcs` - a
separate module, so it cannot be added to `pkg/flowstate/v1/plugin` without
editing code this engagement does not own - was not done, for time, and is
recorded here rather than left to be discovered. See the top-level report
for the same point stated once for both of this repository's new plugins.
