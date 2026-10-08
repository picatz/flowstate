# Tasks a plugin provides: github.pull_request_get and github.issue_comment

This directory has four files:

- [`workflow.yaml`](workflow.yaml) reads a real, public pull request's state
  with `github.pull_request_get:` - read-only, needs no credential, and safe
  to run as written, with no arguments, the way every ordinary example in
  this repository is.
- [`issue-comment.yaml`](issue-comment.yaml) posts a real comment with
  `github.issue_comment:` - a mutation, requires `inputs:` naming a real
  repository and issue/PR number, and a credential. It cannot run by
  accident: there is no default owner, repo, or number to post to.
- [`triage.yaml`](triage.yaml) runs the read/audit tier - open pull
  requests, the files one touches, open issues, and one issue's full
  record - against a public repository, with no arguments.
- [`list-resume.yaml`](list-resume.yaml) reads open issues in two bounded
  pages, chained through `github.issue_list`'s `next_cursor` -> `cursor`,
  with no arguments.

All four use tasks the `github` plugin provides - see
[`plugins/github`](../../../plugins/github) for the source, and its
`README.md` for what authentication modes it supports and, just as
importantly, what this plugin deliberately does not do yet.

## Running the read-only example

```console
$ mkdir -p ./plugins
$ go -C plugins/github build -o ../../plugins/flowstate-plugin-github .
$ flow plugins --plugin-dir ./plugins
$ flow worker --allow-unversioned-interpreter --plugin-dir ./plugins \
    --auth-policy examples/plugins/greet/auth.yaml &
$ flow server --insecure-no-auth --plugin-dir ./plugins &
$ flow run examples/plugins/github/workflow.yaml
```

`--auth-policy` is needed although this file reads no secret: the plugin
registers the `github:` secret scheme, and a worker holding any secret provider
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

This makes a real, unauthenticated request to the GitHub API - it will fail
without internet access, and against a low, shared rate limit if run
often (GitHub's own unauthenticated limit is per source IP, not per
workflow).

## Running the comment example

Do not run this against a repository you do not want a bot comment posted
to. It needs a real credential - see `plugins/github/README.md`,
"Authentication," for how to configure one - and a real target. Put the token
in a file only you can read (mode 0600), outside the checkout so it is never
staged, `~/.config/flowstate/plugin-env.yaml`:

```yaml
env:
  github:
    GITHUB_TOKEN: ghp_...
```

```console
$ flow worker --allow-unversioned-interpreter --plugin-dir ./plugins \
    --auth-policy examples/plugins/greet/auth.yaml \
    --plugin-env-file ~/.config/flowstate/plugin-env.yaml &
$ flow run examples/plugins/github/issue-comment.yaml \
    --input owner=your-org --input repo=your-repo --input number=1 \
    --input body='posted by a flowstate workflow'
```

`${secret('github:token')}` is resolved by the plugin from its own environment,
which starts empty: `GITHUB_TOKEN` (or a GitHub App's three variables) reaches
it only through `--plugin-env-file` or `--plugin-env`, never by being exported
in the shell that starts the worker. The file keeps the token out of the
worker's argv, which any local user can read, and out of your shell history,
but not out of the plugin's environment, which anything running as the worker's
user can read through `/proc/<pid>/environ`
(`pkg/flowstate/v1/plugin/env_config.go`). On a shared host, resolve `token`
worker-side instead: it is a secret input the host resolves for each call, so
`token: ${secret('file:github-token')}` with the worker's `--secret-dir` keeps
it out of the plugin's environment. The submission is refused by `flow run`
today, as above.

## Why github.* and not forge.*

Both tasks are named after GitHub specifically - `github.pull_request_get`,
not `forge.pull_request_get` - and that is a visible admission, not an
oversight: see `plugins/github/doc.go`, "Naming," for why a portable
`forge.*` vocabulary is the right eventual shape for `pull_request_get`
particularly, and why this plugin cannot expose it under two prefixes with
the schema as it exists today.

## How this is checked

The files sit under `examples/plugins/` so the built-in-only corpus checks skip them;
[`examples/README.md`](../../README.md#plugin-examples) says why and how they are
validated against the plugin's real schema. `plugins/github/reachable` builds the
binary and proves each file is refused before the plugin is registered and accepted
after. It never runs `github.pull_request_get` or `github.issue_comment`: both reach the
GitHub API, and posting a comment needs a credential the test must not hold.

Posting a real, unattended comment from a CI run is a decision an operator makes
deliberately (which repository, which credential, how often), so no example makes
it by existing in an automated corpus.
