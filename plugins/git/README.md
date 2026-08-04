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
want to see it work rather than read about it. The write task's example is a
separate, parameterized file in the same directory - it cannot run by
accident.

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
unset token as an unauthenticated request; `git.commit_push` requires one for
any repository that accepts pushes from the internet (which is to say,
almost always).

A reference's *name* is ignored; this plugin resolves the one credential its
own environment names:

```
GIT_SECRET_<NAME>=<https-password>
```

Used as the password half of HTTP Basic auth - the same shape GitHub,
GitLab, and Gitea all accept for a token over HTTPS.

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
