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
$ flow worker --allow-unversioned-interpreter --plugin-dir ./plugins \
    --auth-policy examples/plugins/greet/auth.yaml &
$ flow server --insecure-no-auth --plugin-dir ./plugins &
$ flow run examples/plugins/vcs/workflow.yaml
```

`--auth-policy` is needed although this file reads no secret: the plugin
registers the `vcs:` secret scheme, and a worker holding any secret provider
refuses to start without a policy that has a `secrets:` section.
[`examples/plugins/greet/auth.yaml`](../greet/auth.yaml) is a rehearsal policy
that allows every reference.

`flow run` asks the server it submits to which tasks it can run (`GetCatalog`), so a plugin task the
server loaded validates on the client without the client launching anything. Against a server whose
policy denies that call, or one you cannot reach, pass `--plugin-catalog` with the output of
`flow plugins --plugin-dir ./plugins --output json`.

`--insecure-no-auth` is what makes this a rehearsal rather than a deployment:
the server authenticates every caller as anonymous, which is only ever right on
a machine nobody else can reach. A real one passes `--auth-policy` instead, plus
`--rpc-resource` when that policy trusts an issuer minting bearer tokens.

This one makes a real, unauthenticated request to `github.com` to clone
`octocat/Hello-World` (chosen because it is small, public, and has existed
for years specifically as a test fixture) - it will fail without internet
access, the same as any of the network examples one level up.

## How this is checked

The file sits under `examples/plugins/` so the built-in-only corpus checks skip
it; [`examples/README.md`](../../README.md#plugin-examples) says why and how it is
validated against the plugin's real schema. `plugins/vcs/reachable` builds the
binary and proves the files are refused before the plugin is registered and
accepted after. It never runs `vcs.log` or `vcs.diff`, which reach a real
repository over HTTPS. Build the plugin first; it is a module of its own.
