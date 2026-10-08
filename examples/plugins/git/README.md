# Tasks a plugin provides: git.ls_remote, git.log, git.read_file, and git.commit_push

This directory's workflow files walk the auth shapes and the read/write
split this plugin's four tasks actually have - public read, private read,
read/audit, resumable read, an exhaustive paged walk, and write - rather
than only the one that happens to need no credential:

- [`workflow.yaml`](workflow.yaml) reads a real, public repository's branch
  refs with `git.ls_remote:` - no `token:`, and safe to run as written, with
  no arguments, the way every ordinary example in this repository is.
- [`ls-remote-private.yaml`](ls-remote-private.yaml) is the *same* call
  against a private repository - same task, same schema, one more field
  (`token:`) filled in. It cannot run by accident: there is no default
  private repository to read, and no default credential.
- [`log-and-read-file.yaml`](log-and-read-file.yaml) is this plugin's
  read/audit tier: `git.log` (bounded commit history, including messages)
  and `git.read_file` (one file's content at one ref), chained into the
  audit a security engineer actually runs - who last touched a file, and
  what does it contain now. No `token:`, safe to run as written, with no
  arguments.
- [`log-resume.yaml`](log-resume.yaml) runs `git.log` twice, chained
  through `next_cursor` -> `cursor`, to show a truncated listing's resume
  shape from issue #216 - a caller who received page one now has something
  to ask for page two with. No `token:`, safe to run as written, with no
  arguments. It shows exactly one resume so the chaining is legible; the
  loop to exhaustion is its own file, next.
- [`log-paginate.yaml`](log-paginate.yaml) is that loop: `loop:` carrying
  the cursor until a page reports `truncated: false`, bounded by
  `max_iterations:`, with a CI test file walking a stubbed three-page
  history and asserting every commit is seen exactly once. This is issue
  #216's acceptance shape, and the reason the file beside it stops at one
  resume.
- [`commit-push.yaml`](commit-push.yaml) pushes a real commit with
  `git.commit_push:` - `token:` here is never optional, because no forge
  accepts an anonymous push. It cannot run by accident either: there is no
  default url, branch, or base ref to write to.

All four are tasks the `git` plugin provides - see
[`plugins/git`](../../../plugins/git) for the source, and its `README.md`
for the security properties this plugin holds by construction, "Which git
server?" for what provider-agnosticism means concretely (including the one
provider, Bitbucket Cloud, that needs `username:` set explicitly), and what
else this plugin deliberately does not do yet.

A commit made this way against a local repository fixture is exactly what
`plugins/git`'s own tests exercise (see its README, "What was proven to
bite") - but a runnable *example* against the network cannot safely do the
same: there is no fixture repository this corpus can push to on every CI run
without either needing a credential checked into the repository (where
everyone who can read it could use it) or leaving commits scattered across a
real public repository each time CI runs. So, same as
[`examples/plugins/github`](../github) does for its own mutation
(`github.issue_comment`), the write half is a separate, parameterized file
that only runs when a human deliberately supplies real inputs.

## Running the read-only example

```console
$ mkdir -p ./plugins
$ go -C plugins/git build -o ../../plugins/flowstate-plugin-git .
$ flow plugins --plugin-dir ./plugins
$ flow worker --allow-unversioned-interpreter --plugin-dir ./plugins \
    --auth-policy examples/plugins/greet/auth.yaml &
$ flow server --insecure-no-auth --plugin-dir ./plugins &
$ flow run examples/plugins/git/workflow.yaml
```

`--auth-policy` is needed although this file reads no secret: the plugin
registers the `git:` secret scheme, and a worker holding any secret provider
refuses to start without a policy that has a `secrets:` section.
[`examples/plugins/greet/auth.yaml`](../greet/auth.yaml) is a rehearsal policy
that allows every reference.

`flow run` asks the server it submits to which tasks it can run
(`GetCatalog`), so a plugin task the server loaded validates on the client
without the client launching anything. Against a server whose policy denies
that call, or one you cannot reach, pass `--plugin-catalog` with the output of
`flow plugins --plugin-dir ./plugins --output json`.

`--insecure-no-auth` is what makes this a rehearsal rather than a deployment:
the server authenticates every caller as anonymous, which is only ever right on
a machine nobody else can reach. A real one passes `--auth-policy` instead, plus
`--rpc-resource` when that policy trusts an issuer minting bearer tokens.

This makes a real, unauthenticated request to the GitHub API/git smart-HTTP
endpoint - it will fail without internet access, the same as any of the
network examples one level up.

## Running the private-read example

Needs a real credential and a real private repository this token can read. Put
the token in a file only you can read (mode 0600), outside the checkout so it
is never staged, `~/.config/flowstate/plugin-env.yaml`:

```yaml
env:
  git:
    GIT_SECRET_0__TOKEN: ghp_...
```

```console
$ flow worker --allow-unversioned-interpreter --plugin-dir ./plugins \
    --auth-policy examples/plugins/greet/auth.yaml \
    --plugin-env-file ~/.config/flowstate/plugin-env.yaml &
$ flow run examples/plugins/git/ls-remote-private.yaml \
    --input url=https://github.com/your-org/your-private-repo.git
```

`${secret('git:token')}` is resolved by the plugin from its own environment,
which starts empty, so the variable is named to the worker in
`--plugin-env-file`; one exported in the shell that starts the worker never
reaches the plugin. `--plugin-env git=GIT_SECRET_0__TOKEN=...` works too, but
puts the token in the worker's argv, which any local user can read, and in
your shell history.

Either way the token then sits in the plugin's environment, which anything
running as the worker's user can read through `/proc/<pid>/environ` for as long
as the plugin runs: the file protects it at rest, not there
(`pkg/flowstate/v1/plugin/env_config.go`). On a host other users or processes
share, resolve it worker-side instead. `token` is a secret input the host
resolves for each call, so `token: ${secret('file:git-token')}`, with the worker
started with `--secret-dir` naming a directory only it can read, hands the
plugin the value for that call and keeps it out of the plugin's environment.

Compare this file to `workflow.yaml` line by line: the only difference is
`token: ${secret('git:token')}` on the `git.ls_remote:` step. Nothing about
the task, its other inputs, or its outputs changes between a public and a
private repository - see `plugins/git/README.md`, "Authentication."

## Running the read/audit-tier example

```console
$ mkdir -p ./plugins
$ go -C plugins/git build -o ../../plugins/flowstate-plugin-git .
$ flow plugins --plugin-dir ./plugins
$ flow worker --allow-unversioned-interpreter --plugin-dir ./plugins \
    --auth-policy examples/plugins/greet/auth.yaml &
$ flow server --insecure-no-auth --plugin-dir ./plugins &
$ flow run examples/plugins/git/log-and-read-file.yaml
```

Also a real, unauthenticated request - no `token:`, safe to run as written.
`git.log` walks a bounded, path-filtered slice of history (`max_commits: 5`,
`path: README`); `git.read_file` reads that same path's content at the sha
of the newest commit `git.log` found. Both clone only the shallow window each
call actually needs - see `plugins/git/README.md`, "Operational scale," for
why that matters against a repository whose full history is too large to
ever clone completely.

## Running the cursor-resume example

```console
$ mkdir -p ./plugins
$ go -C plugins/git build -o ../../plugins/flowstate-plugin-git .
$ flow plugins --plugin-dir ./plugins
$ flow worker --allow-unversioned-interpreter --plugin-dir ./plugins \
    --auth-policy examples/plugins/greet/auth.yaml &
$ flow server --insecure-no-auth --plugin-dir ./plugins &
$ flow run examples/plugins/git/log-resume.yaml
```

Also a real, unauthenticated request - no `token:`, safe to run as written.
The first `git.log` step asks for two commits; the second feeds the first's
`next_cursor` output back in as `cursor`, and gets the two commits
immediately after - never repeating the first page's last entry. See
`plugins/git/proto/git/v1/git.proto`, `LogInputs.cursor` and
`LogOutputs.next_cursor`, for the exact contract, and the file's own
top comment for why this shows one resume rather than a loop to
exhaustion.

## Running the write example

Do not run this against a repository you do not want a real commit pushed
to. It needs a real credential, in the same `~/.config/flowstate/plugin-env.yaml` as
above, and a real target:

```console
$ flow worker --allow-unversioned-interpreter --plugin-dir ./plugins \
    --auth-policy examples/plugins/greet/auth.yaml \
    --plugin-env-file ~/.config/flowstate/plugin-env.yaml &
$ flow run examples/plugins/git/commit-push.yaml \
    --input url=https://github.com/your-org/your-repo.git \
    --input branch=main \
    --input base_ref=$(git ls-remote https://github.com/your-org/your-repo.git refs/heads/main | cut -f1) \
    --input message="posted by a flowstate workflow" \
    --input content="hello from flowstate"
```

`base_ref` is never defaulted - read it first, the same way the step above
does with a plain `git ls-remote`, or with `git.ls_remote` itself (see
`workflow.yaml`), or with `vcs.log`. A retry of this exact command is safe;
a second, concurrent write to the same branch based on the same base_ref is
refused, not forced - see `plugins/git/README.md`, "Design decisions."

## How this is checked

The files sit under `examples/plugins/` so the built-in-only corpus checks skip them;
[`examples/README.md`](../../README.md#plugin-examples) says why and how they are
validated against the plugin's real schema. `plugins/git/reachable` builds the
binary and proves each file is refused before the plugin is registered and accepted
after. It never runs the four tasks, which reach the real network and, for a private
read or a push, need a credential and a target the test must not choose.
