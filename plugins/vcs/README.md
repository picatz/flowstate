# flowstate-plugin-vcs

Version-control tasks for Flowstate: `vcs.log` and `vcs.diff`, built on
[go-git](https://github.com/go-git/go-git) - a pure-Go implementation of the
git protocol, chosen so this plugin never execs a `git` binary. See
[`doc.go`](doc.go) for the full argument for why, and for why this plugin's
task names ("vcs", not "git") are meant to outlive this particular backend.

An example that runs it lives at
[`examples/plugins/vcs`](../../examples/plugins/vcs); read that first if you
want to see it work rather than read about it.

## Building

```console
go build -o /path/to/plugins/flowstate-plugin-vcs ./plugins/vcs
```

## Tasks

| Task | Reads/Writes | Idempotent | Needs a credential |
| --- | --- | --- | --- |
| `vcs.log` | reads | yes | only for a private repository |
| `vcs.diff` | reads | yes | only for a private repository |

There is no `vcs.clone`, `vcs.commit`, `vcs.push`, or `vcs.fetch` in this
version. This is a deliberate scope decision, not an oversight - see
"Design decisions" below.

## Authentication

Both tasks accept a `token` input, always as a secret reference -
`${secret('vcs:some-name')}` - never a literal. Unset means an
unauthenticated request, which works for any public repository.

A reference's *name* is ignored; this plugin resolves the one credential its
own environment names:

```
VCS_SECRET_<NAME>=<https-password>
```

where `<NAME>` is the reference's own name, uppercased. `${secret('vcs:acme-org')}`
reads `VCS_SECRET_ACME_ORG`. The value is used as the password half of HTTP
Basic auth against the repository's remote - the same shape GitHub, GitLab,
and Gitea all accept for a token over HTTPS, which is why this plugin's
scheme is not named after any one forge.

## Design decisions and the arguments for them

**No shared workspace, ever.** Every task clones what it needs, in memory
(`storage/memory`, no working tree - see `clone.go`), for the duration of
one activity invocation, and returns *content*, never a filesystem path.
Two independent reasons converge on this:

- Security: GitHub Actions' cache/artifact poisoning class (the public
  TanStack incident) exists because a shared cache has no trust tier - an
  untrusted job writes an entry a privileged job later reads. A `vcs.clone`
  that handed a checkout to a later step would be the same shape. This
  system has never had shared storage between steps or runs, and this
  plugin does not introduce the first instance of it.
- Correctness under Temporal: an activity's steps are not guaranteed to run
  on the same worker or machine, and a retry can be scheduled anywhere. A
  path written by one activity execution is not reliably visible to
  another.

This is why there is no `vcs.clone` on its own (its only useful output would
be a path, which is exactly what is being refused) and no `vcs.commit` /
`vcs.push` (those need a persistent working tree across more than one
operation to be worth anything, or a much larger single-activity design for
"build a tree from given content and push it" - which is a real, coherent
feature and a substantially bigger one, deliberately left for a version that
can give it as much design care as this one gives log/diff).

**Bounds on attacker-chosen input.** A repository is attacker-chosen input
in the same sense a URL is to the core `http` task - see `validate.go` for
the full list (URL scheme allowlist, revision-string length, commit-count
ceiling, patch/file-count ceilings) and `clone.go` for the two that matter
most: clone depth (bounds the *commit graph* asked for) and a response-byte
cap installed on go-git's own HTTP transport (bounds every response, on
every status code, which is the layer this codebase's own CLAUDE.md names as
the only one a library's non-2xx error path cannot bypass). Depth does not
bound the size of any single blob within it - a shallow clone of a
repository whose latest commit adds one enormous file is still one enormous
file - and the response-byte cap, not a true "maximum repository size," is
the honest backstop for that gap. This is recorded rather than left for
someone to discover the hard way.

**Error classification.** Every operation here is read-only, so there is no
idempotency concern the way the http task's `retry_on_unknown_outcome`
exists for - see `errors.go`. What still matters is not retrying a failure
that will recur identically: a nonexistent repository, a bad credential, or
a revision outside the depth this task fetched are all classified as
permanent, and only a transport-level failure (DNS, connection refused, a
context deadline) is retried.

## SDK gaps found while building this

**A plugin task cannot use a secret from anywhere but its own plugin's
scheme.** The engine resolves a step's ordinary inputs before scheduling it,
but a [`flowstatev1.Value_SecretRef`] is deliberately left unresolved all the
way to the activity that needs it (invariant 7) - and for a plugin task,
that activity is generated code in `pkg/flowstate/v1/plugin/task.go` that
forwards inputs to the plugin process over RPC exactly as given, never
calling `ResolveSecret`. There is no RPC a plugin can call to ask the host
to resolve an arbitrary reference on its own behalf -
`pluginv1.SecretService` runs the other direction, letting the host ask a
plugin to resolve schemes *that plugin* advertises. So the only reference a
plugin task can act on is one whose scheme the same plugin binary resolves,
because then "ask the host" and "call the function answering
CAPABILITY_SECRETS requests" are the same code, called directly. This is why
`vcs`'s `token` input can only take a `vcs:` reference, never
`${secret('env:SOME_TOKEN')}` or a reference belonging to a different
plugin, even though nothing about the Flowfile grammar suggests that
restriction exists. See `secrets.go` for the full argument at the point it
matters.

**A plugin task has no access to the caller's namespace or identity.**
`sdk.Task.Fn`'s signature - `(ctx, inputs, scope)` - carries neither, even
though `pluginv1.ExecuteRequest` (the wire message the host actually sends)
has both. `SecretRequest.Namespace` exists precisely so a multi-tenant
secret backend can scope a lookup to the calling workload's tenant, and it
is available when the *host* calls a plugin's `SecretService.Resolve` - but
not when a task resolves its own scheme in-process, which is the only path
available per the gap above. The practical consequence: this plugin's
in-process resolution always uses the default (empty) namespace, which is
correct on a single-tenant deployment (this repository's own invariant 8,
"self-hosted first," treats that as the ordinary case) and silently wrong
on one serving several tenants from one worker pool - every tenant's
`${secret('vcs:...')}` would resolve the same variable. Fixing this needs
`sdk.TaskFunc`'s signature to carry the caller's namespace and identity,
which is a change to `pkg/flowstate/v1/plugin`, not to this plugin.

**Regenerating this plugin's own `.proto` needs the repository root's
workspace, temporarily.** `buf` refuses a workspace directory that reaches
outside the directory it was invoked against, and this plugin's schema
imports `flowstate/v1/flowstate.proto` from the repository root - the only
way to satisfy both is to run `buf generate` from the root with the root's
own `buf.work.yaml` temporarily listing this plugin's proto directory too.
See `buf.gen.yaml`'s own comment for the exact recipe. This works, but it is
friction a plugin author outside this repository would not have (they would
vendor or fetch the schema some other way); it is recorded here rather than
smoothed over.

## What was left undone, and why

- **`vcs.commit` and `vcs.push`.** See "Design decisions" above - these need
  a "build a tree from content, commit it, push it, all in one activity"
  design that is a real, larger feature, not a small addition to this one.
- **SSH remotes.** Only `https://` is accepted; go-git also speaks
  `ssh://`, which this plugin refuses outright by its scheme allowlist
  rather than half-support it without a credential story for SSH keys.
- ~~An integration test that builds, launches, and runs this plugin against
  a real workflow file~~ - now `TestAFlowfileCanNameTheVCSPluginsTasks` in
  [`reachable/`](reachable), this module's own equivalent of
  `TestAFlowfileCanNameAPluginTask` for `examples/plugins/greet` in
  `pkg/flowstate/v1/plugin`. It lives in its own package, separate from
  `main.go`, so that building this plugin's real binary and validating
  `examples/plugins/vcs/workflow.yaml` against it never has this test
  binary's own proto registry already holding `vcs.v1` before the
  reconstruction it exists to check. What exists beside it: unit tests for
  every pure function in this plugin (bounds, classification, containment).
