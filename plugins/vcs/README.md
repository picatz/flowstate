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

Both tasks accept a `token` input, always as a whole secret reference - for
example `${secret('env:GITHUB_TOKEN')}` or the compatibility provider
`${secret('vcs:some-name')}` - never a literal. Both declare `token` in
`secret_inputs` and `required_secret_inputs`, so the host resolves it under the
caller's namespace and scrubs the value from errors and outputs before the
plugin runs. Unset means an unauthenticated request, which works for any public
repository.

A credential is selected by two things, and only one of them is written in
the Flowfile. The *name* is what a reference carries: `${secret('vcs:token')}`
is scheme `vcs`, name `token`, and a reference has no third part — the form is
`scheme:name`. The *namespace* is the tenant the requesting workload belongs
to; a workflow cannot name it, choose it, or change it.

Both segments are lowercase ASCII letters, digits and hyphens — anything else
is refused rather than rewritten — and each is encoded into the variable name
by upcasing it and turning every hyphen into an underscore. The namespace's
*encoded* length is written in front, which is what preserves the boundary
between the two halves. The default namespace encodes to the empty string, so
its length is zero and the segment between the separators is empty:

```
VCS_SECRET_<NAMESPACE_LENGTH>_<NAMESPACE>_<NAME>=<https-password>
# ${secret('vcs:acme-org')}, run in the default namespace:
VCS_SECRET_0__ACME_ORG=<https-password>
# the same reference, run by a caller the server placed in tenant team-a:
VCS_SECRET_6_TEAM_A_ACME_ORG=<https-password>
```

The compatibility provider's value is used as the password half of HTTP Basic auth against the
repository's remote - the same shape GitHub, GitLab, and Gitea all accept for
a token over HTTPS. The task may equivalently receive a token from any other
host-configured secret provider; provider choice does not change transport
behavior.

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

**Packfile inflation.** The response-byte cap above bounds the *compressed*
bytes a remote sends; it does not bound what those bytes decompress into.
Git packs each object as its own zlib stream, so a remote can legally answer
a small, fast response with a pack that inflates to far more memory than it
ever sent on the wire - a delta or decompression bomb. `clone.go` clones
into a `packBoundedStorer` (`packbound.go`) that tracks the cumulative
decompressed size of every object go-git's packfile parser materializes and
refuses once it crosses `maxInflatedBytes`, naming the bound in its error.
This closes the *sum-across-objects* case - `packbound.go` documents, and
`packbound_test.go` proves, that it does not cap any single object's own
peak size while that one object is being decoded; go-git has no exported
hook earlier than "the object already exists in memory" (its own
per-object streaming hook, `lazyObjectWriter`, is unexported and cannot be
implemented from outside its package - checked directly against go-git's
source, not assumed). Until that residual is closed, this task should only
be pointed at remotes the deployment trusts.

**Error classification.** Every operation here is read-only, so there is no
idempotency concern the way the http task's `retry_on_unknown_outcome`
exists for - see `errors.go`. What still matters is not retrying a failure
that will recur identically: a nonexistent repository, a bad credential, or
a revision outside the depth this task fetched are all classified as
permanent, and only a transport-level failure (DNS, connection refused, a
context deadline) is retried.

## SDK boundary learned while building this

The original implementation resolved `vcs:` references inside each task,
which meant every call implicitly used the default namespace and bypassed the
host's scrubber. Tasks now declare `token` through `secret_inputs`; the host
resolves any configured provider under the established namespace and sends
only the value. The `vcs:` SecretService remains solely as a
migration-compatible provider behind that same host path.

**Regenerating this plugin's own `.proto` needs the repository root's
workspace, temporarily.** `buf` refuses a workspace directory that reaches
outside the directory it was invoked against, and this plugin's schema
imports `flowstate/v1/value.proto` from the repository root - the only
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
