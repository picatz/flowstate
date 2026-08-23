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
$ flow server --insecure-no-auth &
$ flow run examples/plugins/github/workflow.yaml
```

`--insecure-no-auth` is what makes this a rehearsal rather than a deployment:
the server authenticates every caller as anonymous, which is only ever right on
a machine nobody else can reach. A real one passes `--auth-policy` instead, plus
`--rpc-resource` when that policy trusts an issuer minting bearer tokens.

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

## What proves these files are reachable

`TestAFlowfileCanNameTheGitHubPluginsTasks`, in
[`plugins/github/reachable`](../../../plugins/github/reachable), is this
plugin's equivalent of `TestAFlowfileCanNameAPluginTask` for
`examples/plugins/greet` in `pkg/flowstate/v1/plugin`: it builds this plugin
as a real, separately compiled binary, opens a
[`plugin.Host`](../../../pkg/flowstate/v1/plugin) over it, and validates both
files here from disk before and after registration - each refused with a
diagnostic naming its task beforehand, accepted afterward, inputs checked
against the descriptors the plugin actually shipped. It lives in its own
package under `plugins/github` rather than beside `main.go`, and rather than
in `pkg/flowstate/v1/plugin`: not in the root module, because that module
must never depend on go-github, and not beside `main.go`, because that file
imports this plugin's own generated types, which would register its schema
in the test binary's own global proto registry before the test ever ran -
see the package doc on `plugins/github/reachable` for what that would have
hidden. It does not run `github.pull_request_get` or `github.issue_comment`
for real - both reach the real GitHub API, and posting a comment needs a
credential this test has no business holding.

Posting a real, unattended comment as part of a CI run is also its own
decision an operator should make deliberately - which repository, which
credential, how often - rather than one this example should make for them
by existing in an automated corpus at all.
