# flowstate-plugin-github

GitHub forge tasks for Flowstate: `github.pull_request_get` (read) and
`github.issue_comment` (write), built on
[google/go-github](https://github.com/google/go-github) - the real GitHub
API client, deliberately, rather than a hand-rolled one. See "Why go-github"
below for why that is the right call for this one dependency even though
`plugins/vcs` (this repository's sibling plugin) takes the opposite position
on `git`.

An example that runs it lives at
[`examples/plugins/github`](../../examples/plugins/github); read that first
if you want to see it work rather than read about it.

## Building

```console
go build -o /path/to/plugins/flowstate-plugin-github ./plugins/github
```

## Examples, kept honest

Both files below are pasted in whole, not summarized, and
`TestReadmeExamplesMatchTheFilesOnDisk` in this package holds them to the real
files byte for byte in both directions - a file added under
[`examples/plugins/github`](../../examples/plugins/github) with no matching
block here fails the build, same as a block that drifts from its file. The
convention: an HTML comment naming the file, on the line immediately before
the fence.

<!-- example: examples/plugins/github/workflow.yaml -->
```yaml
edition: v2026.2
name: github-pull-request-get
description: Reads a public pull request's state using the "github" plugin - the one read task this example runs by default.

# github.pull_request_get is one of two tasks the "github" plugin provides;
# the other, github.issue_comment, posts a comment and needs a credential -
# see comment-inputs.json and the README before running that half.
#
# The dot in "github.pull_request_get" is what marks this as a plugin task
# rather than a built-in: no built-in task has one. The engine has never
# compiled github.v1.PullRequestGetInputs; it learns the shape from
# descriptors this plugin ships in its manifest at launch. See
# plugins/github for the source and plugins/github/README.md for the
# authentication modes it supports, in the order this plugin prefers them.

vars:
  owner: golang
  repo: go
  # A pull request old enough to be merged and stable, so this example's
  # output does not change out from under it.
  number: 1

steps:
  - id: pr
    github.pull_request_get:
      owner: ${vars.owner}
      repo: ${vars.repo}
      number: ${vars.number}
      # No token: a public repository's pull requests are readable
      # unauthenticated, at a much lower rate limit. Compare this to
      # comment-inputs.json, where posting a comment requires one.

  - id: announce
    log:
      message: "${'pull request #%d (%s) - %s'.format([vars.number, steps.pr.state, steps.pr.title])}"

outputs:
  title:
    value: ${steps.pr.title}
    description: the pull request's title, read from GitHub's API without this build knowing its schema

  state:
    value: ${steps.pr.state}
    description: "open or closed"

  head_sha:
    value: ${steps.pr.head_sha}
    description: the commit at the tip of the pull request's branch
```

<!-- example: examples/plugins/github/issue-comment.yaml -->
```yaml
edition: v2026.2
name: github-issue-comment
description: Posts a real comment on a real issue or pull request using the "github" plugin's one non-idempotent task. Requires inputs; never runs by accident.

# github.issue_comment is a mutation - unlike this directory's other file,
# workflow.yaml, running this posts a real, visible comment. It is written
# with required `inputs:` rather than a hard-coded owner/repo/number,
# specifically so it cannot run with no arguments the way every other
# example in this repository is designed to - see examples/README.md's own
# rule about that, and its one exception: "an example whose subject is a
# required input needs a file saying what it requires." This one's subject
# is a mutation, which is exactly the case for requiring input rather than
# assuming a default.
#
# See errors.go in plugins/github for why a failure here that leaves this
# plugin unable to tell whether the comment was actually created is never
# retried automatically - the same reasoning the core http task's
# retry_on_unknown_outcome exists for.

inputs:
  owner:
    type: string
    required: true
    description: repository owner, e.g. "octocat"
  repo:
    type: string
    required: true
    description: repository name, e.g. "hello-world"
  number:
    type: int
    required: true
    description: the issue or pull request number to comment on
  body:
    type: string
    required: true
    description: the comment's Markdown text

steps:
  - id: comment
    github.issue_comment:
      owner: ${inputs.owner}
      repo: ${inputs.repo}
      number: ${inputs.number}
      body: ${inputs.body}
      # A secret reference, resolved inside the task, never a literal - see
      # plugins/github/README.md, "Authentication," for how this plugin
      # answers it: a GitHub App installation token when one is configured,
      # a personal access token otherwise.
      token: ${secret('github:token')}

outputs:
  comment_url:
    value: ${steps.comment.html_url}
    description: where to see what this run just posted
```

## Why go-github, and not a hand-rolled client

`plugins/vcs` never execs `git` and never takes a git-client dependency,
because git's own protocol and object format are exactly the kind of thing
worth owning rather than trusting a subprocess with. GitHub's REST API is a
different shape of problem: it is a large, versioned, actively-evolving
surface (pagination cursors, rate-limit headers, dozens of resource types),
and go-github is the same team's own maintained client for it, tracking the
API as it changes. Reimplementing that surface by hand would be exactly the
"bespoke dependency" this engagement's constraints warn against taking on -
not fewer dependencies, but the same dependency rewritten worse, with none
of upstream's own test coverage against GitHub's actual behavior. The
constraint that matters is not "zero dependencies," it is "this plugin's own
module, so the operator decides whether to trust go-github, and it never
reaches the root module's dependency graph" - which is exactly what the
separate `go.mod` and `replace` directive in this plugin achieve.

## Tasks

| Task | Reads/Writes | Idempotent | Needs a credential |
| --- | --- | --- | --- |
| `github.pull_request_get` | reads | yes | only for a private repository |
| `github.issue_comment` | writes | **no** | always |

Two tasks, not four, on purpose: this plugin proves one forge operation -
reading a pull request and commenting on it - end to end, rather than a
wide, thin API surface. See "What was left undone" below for what a broader
version would need, and the naming question that has to be settled before it
is worth building.

## Authentication

Every task's `token` input is a secret reference -
`${secret('github:token')}` - never a literal. `pull_request_get` treats an
unset token as an unauthenticated request (works for public repositories, at
a much lower rate limit); `issue_comment` requires one, since GitHub does not
accept an anonymous comment.

This plugin supports two modes, checked in this order, and always prefers
the first when both happen to be configured:

1. **GitHub App** (`GITHUB_APP_ID`, `GITHUB_APP_PRIVATE_KEY` - PEM, PKCS#1 or
   PKCS#8 - and `GITHUB_APP_INSTALLATION_ID`). This plugin signs a JWT with
   the App's own key, exchanges it for an installation access token scoped
   to whatever repositories and permissions the installation grants, and
   caches the token for its lifetime (minus a safety margin), never logging
   either the key or the minted token. This is the credential this plugin
   is built to make the easy path, because an installation token is
   short-lived and scoped to a specific installation rather than a whole
   account.
2. **Personal access token** (`GITHUB_TOKEN`). Supported because it is what
   most people already have; it is a standing credential with no expiry this
   plugin can observe, and is the weaker option of the two.

All three App variables must be set together or not at all - a
half-configured App fails closed at startup (`checkHealth`) rather than
silently falling back to a PAT and running every request as the wrong
identity without saying so.

`GITHUB_API_BASE_URL` overrides the API base for GitHub Enterprise Server.
It stays governed by the same egress policy as github.com - see
`client.go` - so naming a GHES host does not open a hole the default policy
would otherwise close.

### Why this is not "workload identity federation" in this repository's own sense

This engagement was asked to prefer, in order: workload identity federation,
then a GitHub App, then a token. For GitHub specifically those first two
rungs collapse into one, and it is worth being precise about why rather than
forcing a fit: see `auth.go`'s `mintInstallationToken` doc comment for the
full argument. In short, `pkg/flowstate/v1/auth`'s federation broker
exchanges a Flowstate-issued OIDC assertion for a credential from an
external relying party (AWS STS, GCP Workload Identity Federation, an
RFC 8693 token-exchange endpoint) - and GitHub Apps have no equivalent
endpoint. The only proof of identity a GitHub App can present is a JWT
signed with that specific App's own key; there is nothing for Flowstate's
issuer to federate into. The GitHub App mode above is the best available
credential shape for GitHub today, and it does not route through
`auth.Broker` at all - which is also true for the independent reason given
in the SDK gaps below: the broker is not reachable from a plugin process
regardless of what a forge supports.

## Design decisions and the arguments for them

**Error classification distinguishes reads from writes.** `pull_request_get`
is idempotent, so its failures are classified as precisely as GitHub's
response allows: a 404 is permanent, a 5xx or a network failure is
retryable, a 403 with rate-limit or secondary-rate-limit signals carries the
wait time GitHub gave. `issue_comment` is not idempotent, and its
classifier (`classifyMutationError` in `errors.go`) asks a different
question first: could this failure mean the comment was already created?
Anything where that is genuinely unknown - a 5xx after the request was
sent, a context deadline mid-request, an unclassified network failure - is
reported as permanent and says so in the message an operator reads ("may
already have been applied, not retried automatically"), the same reasoning
the core `http` task's `retry_on_unknown_outcome` exists for. Only a clean,
fully-processed rejection (a 404, a 422, a rate limit that never reached the
server) is treated as unambiguous.

**Simplification, stated plainly:** distinguishing "the connection failed
before any bytes were sent" (safe to retry even for a mutation) from
"the connection failed after the request started sending" (not safe) would
need `net/http/httptrace` instrumentation this version does not have. Every
network-level failure for `issue_comment` is conservatively treated as
"unknown, do not retry" rather than attempting that distinction and getting
it wrong in the unsafe direction.

**Every request is bounded and egress-governed.** `client.go` installs one
`netpolicy.Policy` at startup and hands its `*http.Client` to both the App
JWT-minting request and every go-github call - so the response-byte cap and
the (deny-by-default) egress rules cover the GitHub Enterprise Server case
too, not only github.com.

## Naming: why github.* and not forge.*

This engagement's design review settled on a naming discipline: `vcs.*` for
portable version-control verbs, `forge.*` for portable forge verbs
(pull/merge request operations, issue comments, status reports), and
`github.*` (or `gitlab.*`, `gitea.*`) reserved for what genuinely has no
portable equivalent - GitHub's Checks API, for instance, which has no
GitLab counterpart. Under that scheme, `pull_request_get` and
`issue_comment` are exactly the two operations that *should* eventually be
`forge.pull_request_get` and `forge.issue_comment` - every major forge has
something close to both.

They are not, in this version, because the schema does not support it yet.
A `pluginv1.TaskManifest`'s name is qualified entirely by the plugin's own
advertised name (`qualified := p.name + "." + name"` in
`pkg/flowstate/v1/plugin/task.go`), and a plugin's name is one string,
validated at startup against a pattern with no dot in it. One plugin process
therefore gets exactly one qualifier for every task it provides - there is
no way, today, for this binary to expose `forge.pull_request_get` and
`github.check_run.create` (a task this version does not implement, but
would be the honest example of one that could never be portable) from the
same process. Making that possible needs a schema change - either relaxing
`TaskManifest.name`'s pattern to permit a qualifier segment the plugin
chooses per task, or adding a field letting a manifest entry declare its own
prefix independent of the plugin's name - and per this engagement's own
instructions, that change is reported here rather than made. If the
factoring research this engagement referenced settles on a different shape
for that field, this plugin's task names are the two names to revisit first.

If a portable vocabulary were not a design goal at all - if GitHub were
assumed to be the only forge this system would ever speak to - the honest
naming would have been the same as what is shipped: `github.pull_request_get`
and `github.issue_comment` read the same either way, which is itself worth
noting. The place a GitHub-only assumption would have changed something is
the *plugin's* name and its secret scheme: there would be no reason to keep
`vcs` and `github` as separate binaries with separate credential schemes at
all, and a single combined plugin could have shared one `token` input and
one `${secret(...)}` scheme across both. Keeping them separate here is the
bet that a second forge and a second vcs backend are both worth being able
to swap in independently later.

## SDK gaps found while building this

The two most significant gaps are identical to `plugins/vcs`'s and are not
repeated in full here - see `plugins/vcs/README.md`, "SDK gaps found while
building this":

1. A plugin task can only resolve a secret reference under its own scheme,
   never the engine's env/file/vault providers or another plugin's scheme -
   which is why this plugin cannot, say, borrow a token the `vcs` plugin
   already resolved, even for the exact same GitHub credential in a
   workflow that uses both plugins together.
2. A plugin task has no access to the caller's namespace or identity, so
   this plugin's in-process secret resolution is correct only for a
   single-tenant deployment.

Two more, specific to this plugin:

3. **The plugin SDK has no equivalent of
   [`flowstatev1.ErrorKindUpstreamUnknown`].** `sdk.Failed`, `sdk.
   Unavailable`, and the rest classify a failure as permanent or retryable,
   but there is no distinct "the outcome is unknown, and that is *why* it
   is permanent" classification a plugin can return - the engine's own
   `ErrorKindUpstreamUnknown` exists specifically to say that, for exactly
   the http task's own `retry_on_unknown_outcome` case, but nothing in
   `pkg/flowstate/v1/plugin/sdk/errors.go` exposes it to a plugin. The
   *behavior* this plugin needs - do not retry `issue_comment` when the
   outcome is unknown - is achieved by returning `sdk.Failed`, which the
   host maps to `ErrorKindInvalidInput` (see `taskError` in
   `pkg/flowstate/v1/plugin/task.go`'s own comment on why that is the
   least-wrong permanent kind available). The safety property holds; the
   diagnostic an operator reads is imprecise - it says "invalid input" for
   a failure that has nothing to do with the inputs. `classifyMutationError`
   works around this by writing the real reason into the error message
   itself, since the classification cannot carry it.

4. **A plugin has no way to carry a `Retry-After` duration on a retryable
   failure.** The engine's own `flowstatev1.TaskError.RetryAfter` exists so
   a 429 or 503 can tell the durable driver when to try again rather than
   guessing with ordinary backoff - CLAUDE.md's own "Both execution drivers
   must agree" section names this exact mechanism. `sdk.Unavailable` has no
   parameter for it. This plugin computes the correct wait from GitHub's
   rate-limit reset time and secondary-rate-limit `Retry-After` header (see
   `errors.go`'s handling of `*github.RateLimitError` and
   `*github.AbuseRateLimitError`) and puts it in the error *message*, which
   a human reads but which the retry scheduler cannot act on - a step
   retries on the engine's own backoff schedule rather than waiting exactly
   as long as GitHub asked. Fixing this needs `pluginv1.ExecuteResponse` (or
   the `sdk.classified` error type) to carry a duration alongside its
   existing `retryable` bool, and for `taskError` in
   `pkg/flowstate/v1/plugin/task.go` to read it - a schema and SDK change,
   not something this plugin can add on its own.

## What was left undone, and why

- **Broader API surface** (`github.pull_request_create`,
  `github.check_run.list`, `github.issue_create`, and the rest of what the
  original task list asked for). Deliberately not built: this engagement's
  design review explicitly asked for one or two forge operations proven end
  to end over a wide, thin surface, and for the naming question above to be
  settled first, since adding more tasks now would mean naming and
  potentially renaming several more of them once a portable `forge.*`
  vocabulary exists.
- **Workload identity federation.** See "Authentication" above - not a gap
  in this plugin so much as a mismatch between GitHub's own auth model and
  what the broker federates into; recorded rather than forced.
- **Precise pre-send/mid-send network failure classification** for
  `issue_comment`. See "Design decisions" above.
- ~~An integration test~~ that builds, launches, and runs this plugin
  against a real workflow file, the way `TestAFlowfileCanNameAPluginTask`
  does for `examples/plugins/greet` - now
  `TestAFlowfileCanNameTheGitHubPluginsTasks` in
  [`reachable/`](reachable), this module's own equivalent, kept in its own
  package for the same reason `plugins/vcs/reachable` is: `main.go` imports
  this plugin's own generated types, and a test beside it would register
  this plugin's schema in the test binary's own registry before the
  reconstruction under test ever happened. What exists beside it: unit
  tests for every pure function (validation, classification, JWT
  construction, containment).
