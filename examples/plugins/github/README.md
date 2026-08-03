# Tasks a plugin provides: github.pull_request_get and github.issue_comment

This directory has two files:

- [`workflow.yaml`](workflow.yaml) reads a real, public pull request's state
  with `github.pull_request_get:` - read-only, needs no credential, and safe
  to run as written, with no arguments, the way every ordinary example in
  this repository is.
- [`issue-comment.yaml`](issue-comment.yaml) posts a real comment with
  `github.issue_comment:` - a mutation, requires `inputs:` naming a real
  repository and issue/PR number, and a credential. It cannot run by
  accident: there is no default owner, repo, or number to post to.

Both are tasks the `github` plugin provides - see
[`plugins/github`](../../../plugins/github) for the source, and its
`README.md` for what authentication modes it supports and, just as
importantly, what this plugin deliberately does not do yet.

## Running the read-only example

```console
$ mkdir -p ./plugins
$ go -C plugins/github build -o ../../plugins/flowstate-plugin-github .
$ flow plugins --plugin-dir ./plugins
$ flow worker --allow-unversioned-interpreter --plugin-dir ./plugins &
$ flow server &
$ flow run examples/plugins/github/workflow.yaml
```

This makes a real, unauthenticated request to the GitHub API - it will fail
without internet access, and against a low, shared rate limit if run
often (GitHub's own unauthenticated limit is per source IP, not per
workflow).

## Running the comment example

Do not run this against a repository you do not want a bot comment posted
to. It needs a real credential - see `plugins/github/README.md`,
"Authentication," for how to configure one - and a real target:

```console
$ export GITHUB_TOKEN=ghp_...   # or configure a GitHub App - see plugins/github/README.md
$ flow run examples/plugins/github/issue-comment.yaml \
    --input owner=your-org --input repo=your-repo --input number=1 \
    --input body='posted by a flowstate workflow'
```

## Why github.* and not forge.*

Both tasks are named after GitHub specifically - `github.pull_request_get`,
not `forge.pull_request_get` - and that is a visible admission, not an
oversight: see `plugins/github/doc.go`, "Naming," for why a portable
`forge.*` vocabulary is the right eventual shape for `pull_request_get`
particularly, and why this plugin cannot expose it under two prefixes with
the schema as it exists today.

## Why this is not `examples/github/workflow.yaml`

The same reason [`examples/plugins/vcs`](../vcs) gives, and the same reason
[`examples/plugins/greet`](../greet) gives: `examples/*/workflow.yaml` is
checked with the built-in task registry alone, and a file naming a plugin's
task is meant to be refused by a process that has not loaded that plugin,
with a diagnostic that says so rather than a silent pass. See
[`examples/README.md`](../../README.md) for the fuller version of this
argument.

## What this does not exercise, and why

Neither file here is driven by an automated test that builds the plugin,
launches it, and runs the workflow against it the way
`TestAFlowfileCanNameAPluginTask` does for `examples/plugins/greet` in
`pkg/flowstate/v1/plugin`. That test lives in the core repository and
exercises the one plugin shipped there; writing its equivalent for this
plugin - inside `plugins/github`, since that is a separate module and
cannot be added to `pkg/flowstate/v1/plugin` without editing code this
engagement does not own - was not done, for time, and is recorded here
plainly rather than left to be discovered. See the top-level report for the
same point stated once for both plugins.

Posting a real, unattended comment as part of a CI run is also its own
decision an operator should make deliberately - which repository, which
credential, how often - rather than one this example should make for them
by existing in an automated corpus at all.
