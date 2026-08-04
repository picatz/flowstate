# Tasks a plugin provides: git.ls_remote and git.commit_push

This directory has three files, chosen to walk the three auth shapes this
plugin's two tasks actually have - public read, private read, and write -
rather than only the one that happens to need no credential:

- [`workflow.yaml`](workflow.yaml) reads a real, public repository's branch
  refs with `git.ls_remote:` - no `token:`, and safe to run as written, with
  no arguments, the way every ordinary example in this repository is.
- [`ls-remote-private.yaml`](ls-remote-private.yaml) is the *same* call
  against a private repository - same task, same schema, one more field
  (`token:`) filled in. It cannot run by accident: there is no default
  private repository to read, and no default credential.
- [`commit-push.yaml`](commit-push.yaml) pushes a real commit with
  `git.commit_push:` - `token:` here is never optional, because no forge
  accepts an anonymous push. It cannot run by accident either: there is no
  default url, branch, or base ref to write to.

All three are tasks the `git` plugin provides - see
[`plugins/git`](../../../plugins/git) for the source, and its `README.md`
for the security properties this plugin holds by construction, "Which git
server?" for what provider-agnosticism means concretely (and does not,
today, for Bitbucket Cloud - a reported gap, not a silent one), and what
else this plugin deliberately does not do yet.

A commit made this way against a local repository fixture is exactly what
`plugins/git`'s own tests exercise (see its README, "What was proven to
bite") - but a runnable *example* against the network cannot safely do the
same: there is no fixture repository this corpus can push to on every CI run
without either needing a credential checked into the repository (which
CLAUDE.md's own secrets story forbids) or leaving commits scattered across a
real public repository each time CI runs. So, same as
[`examples/plugins/github`](../github) does for its own mutation
(`github.issue_comment`), the write half is a separate, parameterized file
that only runs when a human deliberately supplies real inputs.

## Running the read-only example

```console
$ mkdir -p ./plugins
$ go -C plugins/git build -o ../../plugins/flowstate-plugin-git .
$ flow plugins --plugin-dir ./plugins
$ flow worker --allow-unversioned-interpreter --plugin-dir ./plugins &
$ flow server &
$ flow run examples/plugins/git/workflow.yaml
```

This makes a real, unauthenticated request to the GitHub API/git smart-HTTP
endpoint - it will fail without internet access, the same as any of the
network examples one level up.

## Running the private-read example

Needs a real credential and a real private repository this token can read:

```console
$ export GIT_SECRET_TOKEN=ghp_...
$ flow run examples/plugins/git/ls-remote-private.yaml \
    --input url=https://github.com/your-org/your-private-repo.git
```

Compare this file to `workflow.yaml` line by line: the only difference is
`token: ${secret('git:token')}` on the `git.ls_remote:` step. Nothing about
the task, its other inputs, or its outputs changes between a public and a
private repository - see `plugins/git/README.md`, "Authentication."

## Running the write example

Do not run this against a repository you do not want a real commit pushed
to. It needs a real credential and a real target:

```console
$ export GIT_SECRET_TOKEN=ghp_...
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

## Why this is not `examples/git/workflow.yaml`

The same reason [`examples/plugins/vcs`](../vcs) and
[`examples/plugins/github`](../github) give: `examples/*/workflow.yaml` is
checked with the built-in task registry alone, and a file naming a plugin's
task is meant to be refused by a process that has not loaded that plugin.
See [`examples/README.md`](../../README.md) for the fuller argument.

## What proves these files are reachable

`TestAFlowfileCanNameTheGitPluginsTasks`, in
[`plugins/git/reachable`](../../../plugins/git/reachable), builds this
plugin as a real, separately compiled binary, opens a
[`plugin.Host`](../../../pkg/flowstate/v1/plugin) over it, and validates all
three files here from disk before and after registration - each refused with
a diagnostic naming its task beforehand, accepted afterward, inputs checked
against the descriptors the plugin actually shipped. It does not run
`git.ls_remote` or `git.commit_push` for real - both reach the real network,
and reading a private repository or pushing a commit needs a credential and
a target this test has no business holding or choosing on a human's behalf.
