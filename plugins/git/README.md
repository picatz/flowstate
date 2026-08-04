# flowstate-plugin-git

Git-specific version-control tasks for Flowstate: `git.ls_remote` (read) and
`git.commit_push` (write), built on [go-git](https://github.com/go-git/go-git)
and [go-gitdiff](https://github.com/bluekeyes/go-gitdiff) - two pure-Go
dependencies, chosen so this plugin never execs a `git` binary, a hook, or
any other subprocess. See [`doc.go`](doc.go) for the full argument, including
where this plugin's design departs from issue #149's own write-operations
comment, and why.

This is the rich, git-specific half of the factoring issue #149 settled;
[`plugins/vcs`](../vcs) (this repository's sibling) stays the small,
backend-agnostic core (`vcs.log`, `vcs.diff`). See `plugins/vcs/doc.go` for
the argument that named the split.

An example that runs the read task lives at
[`examples/plugins/git`](../../examples/plugins/git); read that first if you
want to see it work rather than read about it. Three files live there: a
public read that runs with no arguments (`workflow.yaml`), the identical
read against a private repository with one more field filled in
(`ls-remote-private.yaml`), and the write task (`commit-push.yaml`) - the
last two are parameterized and cannot run by accident.

## Building

```console
go build -o /path/to/plugins/flowstate-plugin-git ./plugins/git
```

## Tasks

| Task | Reads/Writes | Idempotent | Needs a credential |
| --- | --- | --- | --- |
| `git.ls_remote` | reads | yes | only for a private repository |
| `git.commit_push` | writes | **yes, by construction** | always |

`git.commit_push` is the centerpiece: one activity does materialize -> apply
-> commit -> push. See "Design decisions" below for why it is idempotent
despite being a write - that property is the whole design, not a footnote.

## Authentication

Both tasks accept a `token` input, always as a secret reference -
`${secret('git:some-name')}` - never a literal. `git.ls_remote` treats an
unset token as an unauthenticated request, which works for any public
repository - see `examples/plugins/git/workflow.yaml`. Reading a private one
is the exact same task with the exact same schema; the only difference is
this one field being set - see `ls-remote-private.yaml`.
`git.commit_push`, in contrast, requires a token unconditionally: no forge
accepts an anonymous push over HTTPS, so writing always needs a credential,
whichever repository it targets - see `commit-push.yaml`.

A reference's *name* is ignored; this plugin resolves the one credential its
own environment names:

```
GIT_SECRET_<NAME>=<https-password>
```

Used, unconditionally, as the *password* half of HTTP Basic auth
(`githttp.BasicAuth{Username: <resolved username>, Password: <resolved
token>}`, which go-git turns into a plain `http.Request.SetBasicAuth` call -
see `clone.go`, `commit_push.go`, and `refs.go`). The username defaults to
the literal `x-access-token` and is overridable via the optional `username`
input - see "Which git server?" below for exactly which providers the
default is verified to work against, and "Choosing the username" for when
and how to override it.

## Which git server?

This plugin speaks git's own smart-HTTP protocol through go-git, and go-git's
`BasicAuth` is nothing forge-specific - reading its source
(`plumbing/transport/http/common.go`), `SetAuth` is one call,
`r.SetBasicAuth(a.Username, a.Password)`, the same standard HTTP Basic
authentication (RFC 7617) any HTTPS server understands. Nothing in this
plugin or in go-git recognizes GitHub, GitLab, Gitea, or Bitbucket by name,
inspects a hostname, or branches on which forge it is talking to - that is
the actual design property worth naming plainly: **there is no provider
lock-in in `git.*`.** Any server that speaks the git-over-HTTPS protocol and
accepts a token as an HTTP Basic-auth password works with this plugin,
including one this plugin's author has never heard of. A provider's own
peculiarities - its REST API shape, its idea of a pull request, its specific
webhook payloads - are exactly what a *forge* plugin like `plugins/github`
exists for; `git.*` never grows a provider-specific input to accommodate one,
on purpose.

What differs between providers is not this plugin's protocol, but what a
provider's own server *validates* about the username half of that same
standard HTTP Basic-auth exchange - which is exactly what the optional
`username` input (default `x-access-token`; see "Choosing the username",
below) exists to let a workflow answer per provider, rather than this
plugin guessing on its behalf. Verified from each provider's own current
public documentation, not guessed:

- **GitHub.** A personal access token (classic or fine-grained) is the
  password; the username is not checked at all. GitHub's own docs on
  personal access tokens state it directly: "Although you are required to
  enter your username along with your personal access token, the username is
  not used to authenticate you. Instead, the personal access token is used to
  authenticate you" - an empty username is rejected, but any non-empty value
  works. The default, `x-access-token`, is fine; `username` never needs
  setting for GitHub.
- **GitLab.** Same shape, confirmed by GitLab's own personal access token
  documentation, which states the git username "Must not be an empty string"
  and that GitLab does not validate its value beyond that. `oauth2` is
  GitLab's own conventional choice for its docs' examples, but it is a
  convention, not a requirement - any non-empty username, including the
  default, is accepted.
- **Bitbucket Cloud.** Different, and the one provider where `username` must
  be set explicitly: Bitbucket validates it. Atlassian's own current
  documentation on API tokens (the mechanism that replaced app passwords,
  which stopped working entirely on June 9, 2026) gives two choices - the
  account's real, case-sensitive Bitbucket username, or the fixed literal
  string `x-bitbucket-api-token-auth` as a documented alternative. The
  default this plugin sends when `username` is left unset, `x-access-token`,
  is neither, so **reading or writing against Bitbucket Cloud needs
  `username` set to one of those two values** - see "Choosing the username,"
  below, and the commented example line in
  `examples/plugins/git/ls-remote-private.yaml`.
- **Gitea.** Not fully verified, and said so rather than guessed: Gitea's own
  API documentation shows a token as the *username* with a fixed password
  (`token:x-oauth-basic`), or a real username with a token as password, but
  does not state - the way GitHub's and GitLab's docs explicitly do - whether
  an arbitrary, non-account username paired with a token as the *password*
  (the shape this plugin sends) is accepted or validated. Untested against a
  real Gitea instance as part of this work; treat Gitea compatibility as
  unconfirmed until someone verifies it directly, not as "probably fine
  because GitHub and GitLab are."

## Choosing the username

Both tasks accept an optional `username` input, paired with `token` as the
HTTP Basic-auth credentials `clone.go`, `commit_push.go`, and `refs.go` all
send. Left unset, it resolves to `x-access-token` - the literal every
version of this plugin sent unconditionally before this input existed, so a
Flowfile written against an earlier version of this schema behaves
byte-identically today; adding this field never changed anyone's existing
behavior, only what a workflow can now ask for instead.

Most providers never look at this value at all (see "Which git server?",
above): GitHub and GitLab both explicitly document that the username is not
validated, so the default is correct there and there is normally nothing to
set. Bitbucket Cloud is the verified exception - its current API-token
scheme wants either the account's real, case-sensitive username or the
documented literal `x-bitbucket-api-token-auth`, neither of which is
`x-access-token` - so writing to or reading a private Bitbucket Cloud
repository needs `username` set explicitly to one of those two values. See
`examples/plugins/git/ls-remote-private.yaml` for a Bitbucket-shaped
`username:` line, commented out with this same explanation, next to the
default (unset) case the file actually runs with.

`username` is validated like every other input: non-empty when set (an
explicitly empty string is rejected rather than silently treated the same
as unset - see `resolveUsername`), bounded in length, and refused outright
if it contains a control character, a bare `CR`, or a bare `LF` - a
username reaches an HTTP `Authorization` header verbatim
(`net/http.Request.SetBasicAuth`), and a `CR`/`LF` there could inject a
second header or split the request into something else entirely.
Refused, never stripped, for the same reason every other attacker-adjacent
field in this plugin is (see `validateTreePath`'s own doc comment) -
proven to actually refuse, not merely declared to: see "What was proven to
bite," below.

## Security properties, and what holds by construction

**No shell-out, ever.** Nothing in this plugin calls `exec.Command`,
`os/exec`, or anything that spawns a process - go-git and go-gitdiff are
both pure-Go. This eliminates, by construction rather than by policy, three
classes of bug real git tooling has shipped: argument injection through a
crafted `--upload-pack=...` remote helper argument (the shape of CLI git's
own known RCEs), execution of a repository's own hooks (`post-checkout`,
`pre-commit`, and so on - this plugin never runs one, because there is no
`git` process here to run them), and config-driven execution
(`core.fsmonitor`, `credential.helper`, and similar directives that make a
checked-out `.gitconfig` or `.git/config` itself executable when a real `git`
binary reads it - there is no real `git` binary reading anything here).

**Ref and branch names are validated as refs.** `validateBranchName` builds
the full ref name and calls go-git's own
[`plumbing.ReferenceName.Validate`](https://pkg.go.dev/github.com/go-git/go-git/v5/plumbing#ReferenceName.Validate),
which refuses (among other things) a name beginning with `-`. Nothing here
ever builds an argv, so this is not closing an injection path into this
plugin - it is closing one into whatever reads this task's own `sha` and
`name` outputs later and might, someday, treat one as a flag.

**URL schemes are an allowlist, https-only in this version.** See doc.go,
"URL schemes," for the full argument, including the honest gap: this plugin
was asked to allowlist `https` and `ssh`, and only implements `https` today,
because it has no credential story for an SSH key yet and go-git's ssh
transport reaches for the operator's own agent/keys by default when given
none. A blocklist was never on the table either way - CLI git's own
`ext::`/`fd::` remote helper schemes are code execution *by design*, and a
blocklist that does not know about the next such scheme admits it by
omission.

**Path checks cover the traversal, not just one path.** Absolute paths, `..`
segments, and any `.git` path segment are refused outright (`validateTreePath`)
- and, independently, go-git's own `object.Tree.Encode` calls
`internal/pathutil.ValidTreePath` on every entry it writes, refusing the same
shapes again, one layer lower. That second layer was found while testing
this plugin, not assumed: `validate_test.go`'s escape-refusal check was
temporarily disabled to prove it bites, and `object.Tree.Encode` still
refused the write - genuine defense in depth, not redundant code, since this
plugin's own check runs first with a clearer diagnostic and is the *only*
layer that knows about the traversal go-git's per-name check cannot see:
writing *through* a path base_ref already has as a symlink or a submodule.
`tree.go`'s `rebuildTree` refuses that case directly - it is a property of
where a path sits in base_ref's tree, not of the path string alone, so no
per-name check (this plugin's or go-git's) can catch it; see
`TestCommitPushRefusesWritingThroughAnExistingSymlink` and
`TestCommitPushRefusesASubmoduleInBaseRef` for the traversal proven, not just
the single path.

**No submodules.** A gitlink (mode `160000`) anywhere along a touched path -
in base_ref already, or named by a patch's own mode line - is refused with a
positioned diagnostic. A submodule names a second repository's URL, and
accepting one here would mean re-running this plugin's own URL and secret
checks against a value that arrived nested inside a tree rather than as a
task input; out of scope for this version.

**No new symlinks, ever.** A patch whose new-file mode is a symlink (`120000`)
is refused outright, in every version - not just an existing one refused as
a traversal target. See `tree.go`'s own comment on why gitdiff's mode parsing
required getting this right: gitdiff parses a patch's raw octal mode number
directly into `os.FileMode` without translating git's bit layout into Go's,
so a naive `filemode.NewFromOSFileMode` call here silently produced the wrong
answer during development - caught by
`TestBuildChangeSetRefusesASymlinkFromPatch` going green for the wrong
reason before the fix, then red without it. This plugin now interprets the
raw octal number directly, as git's own mode encoding.

**A deletion is validated, not taken on faith.** A patch's deletion fragment
is applied against base_ref's actual current blob at that path with the same
`gitdiff.Apply` every other fragment goes through - a stale patch (whose
context no longer matches what is really there) or one naming a path
base_ref never had at all is refused, named in the diagnostic, rather than
silently deleting whatever currently occupies that path. Getting this wrong
is real, unannounced data loss, not a cosmetic bug - see "What was proven to
bite," below, for the tests that prove the refusal actually fires.

**An overwrite preserves the mode it finds, not a default.** Both write
paths - a `files:` entry replacing an existing path's content, and a
content-only patch (one with no mode header lines, the ordinary case) - keep
that path's existing regular/executable bit rather than silently forcing
`100644`. `files:` is documented as replacing *content*; a patch that says
nothing about the mode is a patch that does not change the mode. A brand
new path - one `files:` creates fresh, or one a patch's own "new file mode"
line names - still gets its own explicit mode (`Regular` unless the patch
says otherwise), since there is nothing to inherit.

**Credentials never enter a URL.** `validateRepositoryURL` refuses userinfo
outright; a token only ever travels as a resolved secret, in memory, used as
HTTP Basic auth. The one path a URL-embedded credential would otherwise
leak through - an error message that echoes the URL back - never arises,
since no URL this plugin accepts can carry one in the first place.

**Bounded egress, and the bound this version does not close.** The same
egress policy `plugins/vcs` installs bounds every response byte crossing
go-git's HTTP transport, on every status code, reused unchanged here for
both reads and the write's own clone-then-push. What it does not bound is
decompressed size - a small pack that inflates to an enormous object graph
("pack bomb") is a real class of attack neither go-git nor this plugin
closes today. Said plainly here rather than left for someone to discover,
per doc.go's own "Bounds this plugin cannot fully close."

**Every output carries a sha.** `git.ls_remote` returns each ref's current
hash alongside its name, and `git.commit_push` returns the commit it created
(or that a previous attempt already created). A workflow that binds to the
sha, not the movable name, cannot be quietly redirected by a later force-push
or branch reset - the same lesson a mutable release tag teaches in a forge
API.

**No signing in this slice.** `sign:`/`verify:` inputs for a signed commit
are explicitly deferred to a follow-up (issue #163) and not started here.

## Design decisions and the arguments for them

**One activity, one write.** `git.commit_push` materializes base_ref's tree,
applies `files` and/or `patch`, builds a commit, and pushes it to `branch` -
all inside the single activity invocation that runs the step. Nothing
crosses steps as a filesystem path; the only thing that crosses is the sha
this call returns. See `doc.go` for why that follows both #145's corrected
invocation-scoped-disk rule and the Temporal-retry argument `plugins/vcs`
makes for its own read tasks.

**Deterministic commits make a retried push safe.** Supply `timestamp`
(author/committer time as an *input*, never this activity's wall clock) and
the exact same inputs given twice produce the identical sha - pushing that
sha to a branch already at it is a no-op success. Leave it empty and the
same retry is still safe, just one field looser: the probe falls back to
comparing the branch's current tip against what this call would have
produced - same parent, same tree, same message - since the wall clock is
the only thing that would otherwise differ. Both paths are tested
(`TestCommitPushIdempotentRetryWithTimestamp`,
`TestCommitPushIdempotentRetryWithoutTimestamp`), and the second one was
deliberately broken and restored to prove it actually bites - see "What was
proven to bite," below.

**Content-level idempotency covers what that probe cannot see.** The probe
above compares against the remote branch's *current* tip, which assumes
base_ref keeps meaning the same starting point across a retry - an
assumption a movable base_ref (a branch name, the schema's own advertised
common case) breaks: after an unrecorded successful push, base_ref itself
resolves to that push's own commit on the next attempt, so the branch's tip
and the newly resolved base_ref end up identical and the probe never fires.
`doCommitPush` closes this with a second, independent check, run *before*
any commit is built: if applying `files`/`patch` to base_ref's own tree
reproduces that same tree, there is nothing to commit - report
`landed_previously: true, changed: false` and the resolved base_ref's own
sha, with no push attempted. A plain no-op call (content identical to what
base_ref already has, no retry involved) hits the exact same check, on
purpose - see doc.go, "Content-level idempotency," and
`gitv1.CommitPushOutputs.Changed`'s own doc comment.

This has no equivalent for `patch:` - a retried patch against a tree that
already carries its own change fails the patch's own context match before
this check is ever reached, surfacing as the ordinary `InvalidInput` a stale
patch always produces. `files:` converges; `patch:` refuses. Documented as a
real asymmetry in doc.go, "files and patch do not layer," not resolved into
one uniform behavior, since there is no sound way to tell "this patch
already landed" apart from "this patch is stale for an unrelated reason"
once its context stops matching either way.

**Compare-and-swap, never force.** Every push requires the remote branch to
be exactly base_ref (go-git's `PushOptions.RequireRemoteRefs`) and never sets
`Force`. A remote that has moved is refused with [`sdk.Conflict`] - a
distinct, non-retried classification a workflow's `dispatch:` can react to
deliberately (re-fetch, recompute, retry on purpose) - rather than an
ordinary failure or, worse, a forced overwrite. See `doc.go`'s "Concurrency"
section for exactly where this plugin's design departs from the write-ops
design comment's own wording: go-git's `Force`/`ForceWithLease` pairing is
not the CLI's single `--force-with-lease` flag, and this plugin uses neither
- `RequireRemoteRefs` is what actually gives a non-force compare-and-swap.

**`git.ls_remote` is `git.commit_push`'s own probe, exposed.** Resolving a
remote's current refs without a clone is cheap, and the write task's
idempotency and compare-and-swap logic needs exactly that lookup - so it is
one function (`listRemoteRefs`), used by both.

## What was proven to bite

CLAUDE.md's own rule: a bound or a refusal is worth exactly as much as the
evidence that it was tested to actually refuse, not merely declared. Every
item below was broken, run red, and restored, rather than left as an
assertion - not only the ones CLAUDE.md's own house gate demanded up front:

1. **The no-timestamp idempotency probe.** `commit_push.go`'s
   content-match fallback (`commitMatches`) was disabled with a literal
   `if false`. `TestCommitPushIdempotentRetryWithoutTimestamp` immediately
   failed - the retry was refused with `sdk.Conflict` instead of succeeding
   idempotently, because without the fallback the only thing left checking
   was the exact-sha shortcut, which a non-deterministic (no timestamp) retry
   never matches. Restoring the fallback made it pass again.
2. **The `..`-escape check in `validateTreePath`.** Disabled with
   `&& false`. The direct unit test
   (`TestValidateTreePathRefusesEscapesAndGitWrites`) went red immediately.
   The *integration* test
   (`TestCommitPushRefusesAPathEscapingTheTreeViaPatch`) stayed green even
   with this plugin's own check disabled - not a bug, but a real finding: see
   "Path checks cover the traversal, not just one path," above. go-git's own
   `object.Tree.Encode` independently refuses the same escape one layer
   lower, which is why the integration test alone would not have been
   enough to prove this plugin's own check does anything; the unit test is
   what shows it.
3. **The symlink-mode bug itself**, found by the test rather than assumed
   fixed: `filemode.NewFromOSFileMode(pf.NewMode)` looked correct and
   compiled, but silently treated every patch-created symlink as a regular
   file, because gitdiff's mode parsing does not carry Go's `os.FileMode` bit
   meanings - it parses git's raw octal mode number straight into the
   `os.FileMode` type without translation. `TestBuildChangeSetRefusesASymlinkFromPatch`
   caught this on first run, before any deliberate "break it" step was
   needed - the fix (`filemode.FileMode(pf.NewMode)`, a reinterpretation
   rather than a translation) is documented in `tree.go` at the point it
   matters.
4. **The movable-base_ref idempotency gap** (found in review, by Codex, and
   verified against this code before a fix was written). The new
   content-level check in `doCommitPush` was disabled with `&& false`; both
   `TestCommitPushBranchNameRetryAfterUnrecordedSuccessDoesNotStackACommit`
   and `TestCommitPushGenuineNoOpConverges` immediately failed on
   `Changed`/`LandedPreviously`, and the first of the two also asserts the
   *set* of commits on the remote is unchanged (`remoteCommitShas`,
   `assertSameCommitSet`) - not merely that the call reported success, since
   a duplicate no-op commit wedged in behind an otherwise-correct tip would
   still look right to a check that only read the branch head. Restoring the
   check made both pass again.
5. **Deletion validation** (also a review finding). The read-and-`gitdiff.Apply`
   step in `applyPatchFile`'s delete branch was disabled with an `if false`
   wrapping it. `TestBuildChangeSetRefusesAStaleContextDeletion` and
   `TestBuildChangeSetRefusesADeletionOfANonexistentPath` both went red -
   the stale-context deletion and the nonexistent-path deletion were both
   silently accepted. Restoring the check made both pass again;
   `TestBuildChangeSetValidDeletionLands` stayed green throughout, proving
   the fix does not also refuse an ordinary, correct deletion.
6. **Mode preservation**, on both write paths. Each of
   `TestBuildChangeSetPatchPreservesExecutableMode` and
   `TestBuildChangeSetFilesOverwritePreservesExecutableMode` was run against
   the previous unconditional-`Regular` behavior (the existing-mode check
   temporarily disabled) and failed with the executable bit silently
   stripped; restoring each check made its test pass again, and
   `TestBuildChangeSetFilesNewPathIsRegular` stayed green throughout,
   proving a brand-new path still gets the ordinary default rather than
   inheriting a mode from somewhere it should not.
7. **The username header-injection refusal.** `resolveUsername`'s control-
   character check was disabled (`if false && (...)`).
   `TestResolveUsernameRefusesHeaderInjection` immediately failed - a
   username carrying `"\r\nX-Injected: evil"` was accepted rather than
   refused. Restoring the check made it pass again. Separately,
   `TestListRemoteRefsSendsTheDefaultUsername`,
   `TestListRemoteRefsSendsAnOverriddenUsername`,
   `TestCloneBoundedSendsTheDefaultUsername`, and
   `TestCloneBoundedSendsAnOverriddenUsername` assert on the actual HTTP
   Basic-auth header a real local `net/http/httptest` server sees - not on
   what `githttp.BasicAuth` was merely constructed with - for both the
   default and an overridden username, on both the read path
   (`listRemoteRefs`) and the write path's own clone step (`cloneBounded`).

## SDK gaps found while building this

Both gaps `plugins/vcs/README.md` documents apply unchanged here - a plugin
task can only resolve its own secret scheme, and has no access to the
caller's namespace or tenant identity - see that README for the full
argument, not repeated here.

**Once PR #160 (`TaskManifest.secret_inputs`) merges**, this plugin should
declare its `token` input there instead of resolving its own `git:` scheme,
and drop `secretScheme` - referenced here by number because that PR has not
merged, and this plugin is not built against its unmerged branch.

**`sdk.Conflict`** did not exist before this plugin needed it - added to
`pkg/flowstate/v1/plugin/sdk` alongside `sdk.IsConflict` (a predicate other
plugins' tests, or a caller deciding how to log a failure, can use instead of
matching on message text). This is the first plugin to return it.

## Where is "git add"

There isn't one, and that is not a missing feature. `git.commit_push` has no
staging step because there is no working tree or index for anything to stage
into - materialize, apply, commit, and push all happen against git *objects*
(blobs and trees this plugin builds directly in memory; see `tree.go`),
never against files on disk. The closest thing to "git add" is simply naming
a path: in `files:`, or in a patch's own file header. Doing so is what adds
that path's content to the tree this call builds, in the same activity that
commits and pushes it - there is no separate instruction to stage what a
caller was just given, because a `files:` map that already is the tree diff
a caller wants (or a `patch:` a previous step already computed) already *is*
the staged state. See `doc.go`, "Where is git add," for the fuller version
of this.

## Operational scale: what this plugin does not have a ceiling for

go-git has no partial clone. `cloneBounded`'s depth bounds the *commit*
graph and the egress policy bounds compressed bytes per response, but there
is no way, with go-git as it exists today, to fetch only the subset of a
tree a change actually touches the way `git clone --filter=blob:none` or a
sparse checkout would. A monorepo whose full tree is enormous hits a real
ceiling here - not a bug this plugin has, a property of the library it is
built on - and this plugin's own bounds (`maxCloneDepth`, `maxResponseBytes`,
`maxFiles`, `maxFileBytes`, `maxTotalFileBytes`) will refuse a request that
exceeds them rather than attempt a clone or a tree rebuild that could exhaust
a worker's memory trying to serve one. Said plainly here, as an operating
constraint someone provisioning a worker for this plugin should know before
pointing it at their largest repository, not discovered from a worker that
fell over.

## What was left undone, and why

- **SSH remotes.** See "Security properties," above - a real, additive gap,
  not a one-line allowlist entry.
- **Submodules and binary files.** No submodule is accepted or produced
  anywhere; `files:` content is text only (a proto `string`, not `bytes`) -
  a binary file changed only through `patch:`'s own binary-fragment support
  is the only way to write non-text content today, and even that is
  untested beyond gitdiff's own coverage.
- **Signing (`sign:`/`verify:`).** Explicitly deferred to issue #163.
- **The decompression-bomb gap.** See "Bounded egress," above.
- **A multi-tenant secret namespace.** Same gap `plugins/vcs` has, for the
  same reason.
