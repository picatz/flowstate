# flowstate-plugin-git

Git-specific version-control tasks for Flowstate: `git.ls_remote` (refs, no
clone), `git.log` and `git.read_file` (the read/audit tier - bounded commit
history and one file's content, each a fresh clone per call), and
`git.commit_push` (write), built on
[go-git](https://github.com/go-git/go-git) and
[go-gitdiff](https://github.com/bluekeyes/go-gitdiff) - two pure-Go
dependencies, chosen so this plugin never execs a `git` binary, a hook, or
any other subprocess. See [`doc.go`](doc.go) for the full argument, including
where this plugin's design departs from issue #149's own write-operations
comment, and why.

This is the rich, git-specific half of the factoring issue #149 settled;
[`plugins/vcs`](../vcs) (this repository's sibling) stays the small,
backend-agnostic core (`vcs.log`, `vcs.diff`). See `plugins/vcs/doc.go` for
the argument that named the split.

An example that runs the read tasks lives at
[`examples/plugins/git`](../../examples/plugins/git); read that first if you
want to see it work rather than read about it. Five files live there: a
public read that runs with no arguments (`workflow.yaml`), the identical
read against a private repository with one more field filled in
(`ls-remote-private.yaml`), the read/audit tier chained into a real audit
question - who last touched a file, and what does it contain now
(`log-and-read-file.yaml`, also runs with no arguments) - `git.log`'s own
cursor-resume shape, two calls chained through `next_cursor` -> `cursor`
(`log-resume.yaml`, also runs with no arguments) - and the write task
(`commit-push.yaml`) - the private-read and write files are parameterized
and cannot run by accident.

## Building

```console
go build -o /path/to/plugins/flowstate-plugin-git ./plugins/git
```

## Tasks

| Task | Reads/Writes | Idempotent | Needs a credential |
| --- | --- | --- | --- |
| `git.ls_remote` | reads | yes | only for a private repository |
| `git.log` | reads | yes | only for a private repository |
| `git.read_file` | reads | yes | only for a private repository |
| `git.commit_push` | writes | **yes, by construction** | always |

### Execution-mode posture

`git.commit_push` deliberately performs the same real push in a local rehearsal
as in durable execution. Rehearsal mode is a host-attested operational fact, not
authorization: task policy admits the task, secret policy releases the token,
and the plugin's network guard constrains the destination. Deployment-owned
egress composition remains #1321; execution mode does not fill that gap. A local
run with the authorities available today is not a dry run. This preserves the
local driver's purpose and the established write example; use a disposable
repository when rehearsing. The credential-free local repository mutations in
`commit_push_test.go` hold that behavior to the real go-git write path.

`git.commit_push` is the centerpiece: one activity does materialize -> apply
-> commit -> push. See "Design decisions" below for why it is idempotent
despite being a write - that property is the whole design, not a footnote.

`git.log` and `git.read_file` are this plugin's read/audit tier - the
exploratory operations a security engineer (or an agent doing the same job)
needs to answer questions about a repository without writing to it:
`git.log` is a bounded slice of commit history reachable from a ref,
including each commit's author, committer, full message, and parents, with
an optional `path` filter (`git log -- <path>`) and `since` cutoff;
`git.read_file` is one file's content, size, mode, and whether it looks
binary, at one ref. Both are genuinely stateless and clone only the
shallowest window each call actually needs for the *default* ref (the
remote's own HEAD) - `git.log` fetches `max_commits + 1` (see `log.go`'s
`fetchDepthForMaxCommits`), and `git.read_file` fetches depth 1, since
reading one file at the tip never needs history at all (see `validate.go`'s
`readFileCloneDepth`). Naming an explicit ref - anything go-git's own
revision parser accepts, including an older sha a previous `git.log` call
itself returned - deepens both to `maxCloneDepth` instead, so the schema's
own advertised contract (any branch, tag, or commit-ish) and what actually
resolves agree; see `doLog` and `doReadFileWithMax`'s own doc comments for
why that widening only ever fires for an explicit ref, never the common
default-ref call. Neither splits into smaller tasks the way `git.commit_push`
deliberately does not either - see "Where is `git add`" and doc.go for why a
write needs one activity with no workspace handed between steps; a read has
no such constraint, and `git.log`/`git.read_file` compose cleanly as two
ordinary, independent steps precisely because each is a single, complete,
stateless clone-and-read - `log-and-read-file.yaml` binds `git.read_file`'s
`ref` to the exact sha `git.log` found, so the two steps read the same
commit rather than "then" and "now."

## Authentication

All four tasks accept a `token` input, always as a whole secret reference -
for example `${secret('env:GITHUB_TOKEN')}` or the compatibility provider
`${secret('git:some-name')}` - never a literal. Every task declares `token` in
both `secret_inputs` and `required_secret_inputs`, so the host resolves the
reference under the caller's namespace, scrubs the value from plugin errors and
outputs, and only then invokes the plugin. `git.ls_remote`, `git.log`,
and `git.read_file` all treat an unset token as an unauthenticated request,
which works for any public repository - see
`examples/plugins/git/workflow.yaml` and `log-and-read-file.yaml`. Reading a
private one is the exact same task with the exact same schema; the only
difference is this one field being set - see `ls-remote-private.yaml`.
`git.commit_push`, in contrast, requires a token unconditionally: no forge
accepts an anonymous push over HTTPS, so writing always needs a credential,
whichever repository it targets - see `commit-push.yaml`.

A credential is selected by its provider scheme and name, plus a namespace the
host establishes from the authenticated caller. A workflow cannot name, choose,
or change that namespace. The `git:` provider remains for compatibility with
existing Flowfiles and reads the variables below; new Flowfiles may use any
host-configured provider instead.

So in a single-tenant deployment — no identity provider configured, or one
that assigns no namespaces — every reference arrives in the default namespace
and the only variables that resolve are the default namespace's. The
namespaced form below is what the *same* Flowfile resolves to when it runs on
a worker serving an authenticated tenant; it is not an alternative spelling
available to the author.

Both segments are lowercase ASCII letters, digits and hyphens — anything else
is refused rather than rewritten — and each is encoded into the variable name
by upcasing it and turning every hyphen into an underscore. The namespace's
*encoded* length is written in front, which is what preserves the boundary
between the two halves. The default namespace encodes to the empty string, so
its length is zero and the segment between the separators is empty:

```
GIT_SECRET_<NAMESPACE_LENGTH>_<NAMESPACE>_<NAME>=<https-password>
# ${secret('git:deploy-token')}, run in the default namespace:
GIT_SECRET_0__DEPLOY_TOKEN=<https-password>
# the same reference, run by a caller the server placed in tenant team-a:
GIT_SECRET_6_TEAM_A_DEPLOY_TOKEN=<https-password>
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

All four tasks accept an optional `username` input, paired with `token` as
the HTTP Basic-auth credentials `clone.go`, `commit_push.go`, `log.go`,
`read_file.go`, and `refs.go` all send. Left unset, it resolves to
`x-access-token` - the literal every
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
if it contains a `:`, a control character, a bare `CR`, or a bare `LF`. The
colon rule is not stylistic: `net/http.Request.SetBasicAuth` builds
`"username:password"` and Basic-auth parsing splits on the *first* colon,
so a colon in `username` would silently absorb part of `token` into what
the server reads as the password instead - a different, wrong credential
pair, not a syntax error. `net/http`'s own `SetBasicAuth` documentation
states the constraint directly. A `CR`/`LF` is refused for the separate
reason that a username reaches an HTTP `Authorization` header verbatim,
where either could inject a second header or split the request into
something else entirely. Refused, never stripped, for the same reason
every other attacker-adjacent
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

**Bounded egress, and the bound this version does not close.** The deployment's
own egress policy — granted to this process at launch, the same one `plugins/vcs`
takes — governs every destination, and this plugin's clone-sized response bound
is stated over it (`sdk.EgressPolicyWithBounds`), so every response byte crossing
go-git's HTTP transport is bounded on every status code, for both reads and the
write's own clone-then-push. What it does not bound is
decompressed size - a small pack that inflates to an enormous object graph
("pack bomb") is a real class of attack neither go-git nor this plugin
closes today. Said plainly here rather than left for someone to discover,
per doc.go's own "Bounds this plugin cannot fully close."

**Every output carries a sha.** `git.ls_remote` returns each ref's current
hash alongside its name, `git.log` returns `resolved_ref` (what a relative
ref like "main" actually meant) alongside every commit's own sha, and
`git.commit_push` returns the commit it created (or that a previous attempt
already created). A workflow that binds to the sha, not the movable name,
cannot be quietly redirected by a later force-push or branch reset - the
same lesson a mutable release tag teaches in a forge API.

**`git.log` and `git.read_file` are bounded on every resource an
attacker-chosen repository controls.** A repository this task reads is
untrusted input the same way `git.commit_push`'s `base_ref` tree is - see
CLAUDE.md's "Bound anything that consumes untrusted input." `git.log` bounds
commit *count* (`max_commits`, ceiling `maxMaxCommits`, refused rather than
silently clamped over it), per-commit *message size*
(`maxLogMessageBytes`), and, independently of both, the *sum* of every
message returned (`maxTotalLogMessageBytes`) - a history with many
merely-large messages, each under the per-entry cap, is refused the same way
one pathological message is, because count and per-entry size alone do not
bound their product's *reach* against the ratio an attacker actually
controls (a commit message has no natural size limit). `git.read_file`
bounds file content (`maxReadFileBytes`) by refusing outright, never
truncating, when a blob exceeds it - a truncated file that looks whole is a
worse failure than a clear refusal naming the actual size. Both reuse
`packBoundedStorer` (`packbound.go`) for the clone itself, the same
packfile-inflation bound `git.commit_push` and `git.ls_remote` already
depend on.

**`git.read_file` refuses a traversal path outright, the same check
`git.commit_push` writes through.** `path` is validated with the same
`validateTreePath` `git.commit_push`'s own `files`/`patch` paths go through
- no absolute path, no `..` segment, nothing under a `.git` path segment -
refused with a positioned diagnostic rather than sanitised, for the same
reason `validateTreePath`'s own doc comment gives: a path from a workflow or
a coding agent is attacker-adjacent input this plugin does not get to guess
about, whether it is about to be written or merely read.

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
8. **The username colon refusal**, found in review (Codex, PR #186) against
   this file's own *previous* doc comment, which reasoned that a colon in
   `username` was "the password's problem, not this field's" - wrong: since
   `net/http.Request.SetBasicAuth` builds `"username:password"` and
   Basic-auth parsing splits on the *first* colon it finds, a username of
   `"alice:admin"` paired with token `"secret"` arrives at a real server as
   username `"alice"`, password `"admin:secret"` - a silent, confusing
   authentication failure, not a rearrangement. The doc comment was rewritten
   to state the real rule rather than leave the wrong reasoning beside a
   correct-by-luck check, and `resolveUsername` now refuses a colon
   outright. `TestResolveUsernameRefusesAColon` was run against the check
   disabled and confirmed red before restoring;
   `TestUnvalidatedColonWouldSilentlySplitTheCredentialPair` demonstrates the
   split concretely, using the exact standard-library round trip
   (`SetBasicAuth` then `BasicAuth`) any real server's parsing would perform.
9. **The unconditional write-token requirement, actually enforced.** Also
   found in review (Codex, PR #186): this README claimed "writing always
   needs a credential" while `tokenFromValue(nil)` legitimately returning
   `""` for an unset input let both clone and push omit `Auth` entirely -
   fail-open code underneath fail-closed prose, and a clean instance of the
   house rule that a claim in prose must be enforced by code, not the other
   way round. `doCommitPush` now refuses an empty token, named as the input
   it is, before its first network call. Proven two ways:
   `TestDoCommitPushRefusesAMissingTokenBeforeAnyDial` points a real local
   server's own TCP listener at the call and asserts it accepted *zero*
   connections - not merely that an error came back, which a check placed
   anywhere before the point of failure would also produce - run against the
   check disabled and confirmed red (one connection was accepted) before
   restoring; and
   `TestGitLsRemoteSucceedsWithoutATokenAgainstThePublicPath` guards the
   other direction, so the same fix never spreads to `git.ls_remote`'s
   documented anonymous-read shape - proven to matter by temporarily adding
   the identical unconditional check to `listRemoteRefs`, confirming that
   test alone goes red, and removing it again.
10. **Four review findings against the read tier** (Codex, PR #202), all
    verified against the code before being fixed:
    - `collectLogCommits` set `truncated: true` only when `max_commits` or
      the message-byte budget stopped collection - never when the shallow
      clone's own fetch boundary did, which a sparse `path` filter reaches
      routinely. Worse than the false `truncated: false` the finding named:
      go-git's own commit walker actually errors out
      (`plumbing.ErrObjectNotFound`) the moment it steps past the boundary
      into a parent this clone never fetched, so `doLog` surfaced an opaque
      failure instead. `collectLogCommits` now recognizes that specific
      error, confirms the clone actually has a shallow boundary
      (`repoHasShallowBoundary`, via `repo.Storer`'s own shallow-commit
      bookkeeping) before treating it as one, and reports `truncated: true`
      rather than failing or under-answering.
      `TestGitLogReportsTruncatedWhenPathFilterReachesShallowBoundary` ran
      red (`doLog: object not found`) before the fix;
      `TestGitLogReportsNotTruncatedWhenPathFilteredHistoryGenuinelyEnds`
      is the opposite direction, so the fix is not merely "always true."
    - `path`'s `PathFilter` matched only an exact string, so `path: auth`
      matched nothing under `auth/` - `git log -- <path>`'s own documented
      semantics include descendants. `pathMatchesFilter` now matches the
      exact path or anything beneath it, with the separator checked
      explicitly so `path: auth` still does not also match a sibling like
      `authz/token.go` - the "obvious wrong implementation" a bare
      `strings.HasPrefix` would be.
      `TestPathMatchesFilterMatchesDescendantsNotSiblings` and
      `TestGitLogPathFilterMatchesDescendantsOfADirectory` cover both
      directions.
    - `ref` advertises any branch, tag, or commit-ish, but `git.log` and
      `git.read_file` both cloned at a fixed, small depth regardless of
      `ref`, so an explicitly named older sha - exactly what a previous
      `git.log` call itself returns - resolved as missing. Both now deepen
      to `maxCloneDepth` whenever a caller names a ref at all, never for the
      common empty-ref (HEAD) call, so the shallow default stays shallow.
      `log-and-read-file.yaml` now binds `git.read_file`'s `ref` to the sha
      `git.log` found, exercising the chain end to end rather than reading
      the default branch's tip twice under two names.
      `TestGitLogResolvesAShaOlderThanMaxCommitsWindow` and
      `TestGitReadFileResolvesAShaOlderThanTheDefaultDepthOneWindow` cover
      the two tasks separately.
    - `signatureOf` normalized every timestamp to UTC
      (`sig.When.UTC().Format(...)`), discarding the offset git actually
      recorded, though the schema promises RFC 3339 "in the recorded zone."
      go-git's own decoder already parses the raw offset into a
      `time.FixedZone` - the fix is simply not calling `.UTC()`.
      `TestGitLogPreservesTheAuthorsRecordedTimezoneOffset` asserts the
      returned string ends in the original `-07:00`, not merely that it
      parses as RFC 3339 (which `Z` also would).

## SDK boundary learned while building this

The original implementation resolved `git:` references inside each task. That
bypassed the host's namespace-aware `secret_inputs` path and output/error
scrubber. Tasks now receive only host-resolved values. The `git:` SecretService
remains solely as a migration-compatible provider behind that same host path;
it is not a second task-resolution mechanism.

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
`maxInflatedBytes`, `maxFiles`, `maxFileBytes`, `maxTotalFileBytes`,
`maxMaxCommits`, `maxTotalLogMessageBytes`, `maxReadFileBytes`) will refuse a
request that exceeds them rather than attempt a clone or a tree rebuild that
could exhaust a worker's memory trying to serve one. `git.log` and
`git.read_file` are deliberately the *cheapest* callers of `cloneBounded` in
this plugin against a large repository, for the default ref: `git.read_file`
clones at depth 1 (`readFileCloneDepth`) - the tip of a fetched branch never
needs history - and `git.log`'s own depth is `max_commits + 1`, never the
fixed `defaultCloneDepth` `git.commit_push` uses to resolve `base_ref`. Both
deepen to `maxCloneDepth` instead whenever a caller names an explicit ref, so
a historical sha - the audit chain `log-and-read-file.yaml` demonstrates -
actually resolves rather than being reported missing; see `doLog` and
`doReadFileWithMax`'s own doc comments. A `path` filter on `git.log` matches
the exact path or any path beneath it (`git log -- <path>`'s own semantics -
naming a directory includes everything under it, never a sibling that merely
shares a name prefix), narrowing *which* commits within the fetched window
are returned, never how deep the window itself reaches - a commit older than
that window that touched `path` is not found, and `truncated: true` is how
that is reported honestly, including when collection runs out of the
window itself rather than out of history (`collectLogCommits`), rather than
silently under-answering. Said plainly here, as an operating constraint
someone provisioning a worker for this plugin should know before pointing it
at their largest repository, not discovered from a worker that fell over.

**Packfile inflation.** `maxResponseBytes` bounds the *compressed* bytes a
remote sends over HTTP; it says nothing about what those bytes decompress
into. A git pack stores each object as its own zlib stream, so a small,
fast response can legally inflate to far more memory than it ever sent on
the wire - a delta or decompression bomb, the gap issue #171 tracked.
`cloneBounded` now clones into a `packBoundedStorer` (`packbound.go`) that
tracks the cumulative decompressed size of every object go-git's packfile
parser materializes and refuses once it crosses `maxInflatedBytes`, naming
the bound. This closes the *sum-across-objects* case, proven in
`packbound_test.go` by asserting peak allocation, not just the eventual
error. It does not close the *single-object* case: go-git exposes no
public hook earlier than "the object already exists in memory" (its own
per-object streaming hook is unexported and unreachable from outside its
package - checked against go-git's source, not assumed), so one
pathological object can still cost this process that object's own real
size before the bound catches it, a residual `packbound_test.go` proves
rather than hides. Until that residual is closed, this plugin should only
be pointed at remotes the deployment trusts.

## Resuming a truncated `git.log`

`truncated: true` used to be a dead end: a caller who received fewer commits
than existed had no way to ask for the rest (issue #216). `git.log` now
grows two fields that turn it into an actual resume position:

- **`LogOutputs.next_cursor`** - populated when `Truncated` is true, at
  least one commit was returned, and this task can both prove there is
  somewhere left to resume from and afford to encode it. Empty otherwise -
  see `LogOutputs.next_cursor`'s own doc comment (`git.proto`) for the two
  narrower cases (a shallow boundary with zero commits; the cursor-size
  ceiling) that are still `Truncated: true` with nothing resumable attached.
- **`LogInputs.cursor`** - fed a previous call's own `next_cursor` back,
  unmodified, and resumes the identical walk exactly where that call
  stopped: nothing dropped, nothing repeated.

**The cursor is not a single sha - it is a frontier plus an emitted set,
and that redesign exists because a single sha is provably wrong against a
merge.** The first version of this field carried one value - the last
commit returned - and resumed at that commit's own first parent. That is
correct for a linear chain, but the moment a page boundary lands on or
after a merge, "first parent only" silently drops every commit reachable
solely through the merge's second parent and beyond: a **miss**, not a
duplicate, and exactly the direction a purely linear test fixture cannot
expose (`TestGitLogCursorPagesReachEveryCommitExactlyOnce`'s own fixture
never caught it; `TestGitLogCursorReachesEveryCommitAcrossARealMerge`,
built specifically to construct a real two-parent merge, did - see "What
was proven to bite," below, for the review that found it).

The fix, in `plugins/git/cursor.go`: `LogInputs.cursor`/`LogOutputs.next_cursor`
now pack two full-sha lists, `"|"`-separated - **frontier** (every
not-yet-explored commit a resumed walk still owes an answer for - a merge
with N parents puts all N here, not just the first) and **emitted** (every
commit already returned across every earlier page of this same walk, so a
commit reachable a second time through a RECONVERGING history - two
branches merged together, sharing an ancestor further back - is recognized
and skipped rather than returned twice). Both lists, together, are what let
`multiRootCommitIter` (`cursor.go`) resume a walk correctly against any
commit graph shape, not merely a linear chain - see that file's own doc
comment for the full argument, including why frontier alone (without
emitted) still duplicates on reconvergence.

**The contract is still full-sha-only, deliberately narrower than `ref`,
just with more of them.** `ref` accepts anything go-git's own revision
parser does - a branch, a tag, `HEAD~3` - because a workflow author names
it by hand. Every element of a cursor's own two lists is required to be a
full, 40-character lowercase hex commit sha (`validateCursor`), because a
cursor is never something a caller composes: it is always the literal value
a previous `git.log` call itself emitted, decoded structurally
(`decodeCursor`) before a single byte of it is trusted for anything else.
The total number of shas across both lists is bounded
(`maxCursorEntries`, set equal to `maxCloneDepth`) - an incoming cursor
naming more than this task will ever track is refused outright, the same
as one shaped wrong in any other way. `ref` and `cursor` are still refused
together (`gitLog`), checked against the raw input before `validateCursor`
even runs, so the conflict is reported regardless of whether the cursor
half happens to be well-formed.

**A cursor is untrusted input, and resuming re-validates everything.**
`cursor` reaches this task the same way any other field does - a workflow
author's literal, or a value a coding agent composed - so it is fully
decoded and bounded by `validateCursor` before anything else touches it,
and a resumed call clones through the exact same `validateRepositoryURL`
and egress policy as a fresh one. A cursor names a position in history; it
is never a bypass around any check the original query performs.

**Filters compose across pages without gaps or duplicates.** `path` and
`since` apply to a resumed walk exactly as they would to a fresh one.
`since` is unchanged (`object.NewCommitLimitIterFromIter`, which consumes
its source one commit at a time with no lookahead). `path` is not: this
task no longer uses go-git's own `object.NewCommitPathIterFromIter`, which
diffs a commit against whatever its source iterator happens to return
*next* - correct only for a strictly linear, single-root walk, and silently
wrong (or simply data it has already thrown away - see "What was proven to
bite") once that source can be a multi-root frontier. `pathFilteringCommitIter`
(`cursor.go`) diffs a commit against its own actual parents instead, looked
up directly, with no lookahead into anything.
`TestGitLogCursorPagesReachEveryCommitExactlyOnce` is the acceptance test
this claim rests on (CLAUDE.md, "Test the traversal, not just the step"): a
23-commit fixture, half touching a filtered path and half not, walked to
exhaustion at `max_commits: 4` (5+ pages), asserting the union of every page
equals the full filtered set with every commit reached exactly once and the
final page reporting `truncated: false`.

**A resume can widen its own fetch as it goes deeper into history.** A
cursor-driven call no longer clones once at a fixed depth: `doLog` retries
at increasing depth (`resumeCloneDepthSteps`: `maxCloneDepth`,
`maxCloneDepth*2`, `maxResumeCloneDepth`) whenever the shallower attempt
cannot make progress on the cursor's own frontier, redoing the whole
clone-and-walk attempt at each step rather than only re-probing
reachability (a probe-only retry can still get stuck mid-walk - see
`TestGitLogCursorResumesLinearHistoryLongerThanTheFirstCloneDepth`'s own
history, "What was proven to bite," below). This is what lets a linear
history longer than one `maxCloneDepth` window still page all the way to
exhaustion, bounded so this can never become an unbounded fetch loop:
go-git has neither an arbitrary-sha "want" nor an incremental `--deepen`
(checked against its own source, not assumed - `resumeCloneDepthSteps`'s own
doc comment), so a fresh, deeper clone is the only way to reach a commit a
shallower one missed, and `maxResumeCloneDepth` is the hard ceiling on how
many times that fresh clone gets larger. Once even the largest step still
cannot resolve anything in the frontier, `git.log` returns a distinct,
actionable `InvalidInput` naming the ceiling reached and what to do next
(narrow with `since`/`path`, or accept the walk as complete) - an honest
refusal, never a broken or silently incomplete page.

**What this does not do.** `examples/plugins/git/log-resume.yaml`
demonstrates exactly one resume - page one, then page two - not a loop to
exhaustion: Flowstate's own workflow language has no loop primitive yet
(issue #157 is still design-only), so walking an entire history to
completion from a Flowfile is not yet expressible; only Go code (the tests
above) can do that today. This is issue #216's "layer 1": the task grows a
resume position a caller driving it from outside (an MCP agent, a script
calling `flow run` repeatedly) can already thread. "Layer 2" - the language
itself carrying a cursor from one iteration to the next - is a separate,
larger piece of work this change does not attempt.

**What was proven to bite (review findings on this feature specifically).**
Two P1s, both found by review against the very first version of this
field, which carried a single sha and resumed at that commit's own
`parents[0]`:

- **Merge parents dropped on resume.** The first-parent-only resume drops
  everything reachable only through a merge's second parent onward - a
  MISS. `TestGitLogCursorReachesEveryCommitAcrossARealMerge` constructs a
  real two-parent merge (`writeSyntheticCommit`, `merge_test.go`, since
  go-git's own `Repository.Merge` only supports fast-forward) with the page
  boundary forced onto the merge itself (`max_commits: 1`), walks to
  exhaustion, and asserts the union is the complete six-commit set exactly
  once. Confirmed to actually catch the bug it targets: reverting
  `multiRootCommitIter`'s parent-push to `ParentHashes[0]` only makes this
  test fail with "walked to exhaustion in 4 page(s), want 6+" (two commits,
  reachable only through the merge's second parent, silently never
  returned).
- **A resumed clone anchored at a fixed depth cannot reach deep history.**
  Before `resumeCloneDepthSteps` existed, every resumed call cloned at a
  single fixed depth regardless of how many pages had already gone by, so
  a linear history longer than that depth could never finish paging - not
  a slow walk, a permanently stuck one.
  `TestGitLogCursorResumesLinearHistoryLongerThanTheFirstCloneDepth` proves
  progressive deepening actually resolves this (a 13-commit history walked
  to exhaustion with a test-injected depth sequence too small to reach it
  in one step); `TestGitLogCursorResumeBeyondEveryDepthStepReportsAnHonestError`
  proves the honest ceiling fires once even the largest step is not
  enough. Confirmed to catch the original bug: pinning the retry loop to
  its first depth step only reproduces the original failure exactly - the
  distinct `InvalidInput` naming the depth ceiling, rather than silent
  incompleteness, which is itself the fix (an honest refusal beats a
  broken walk). A related, narrower version of the same class of bug lived
  one layer up: `collectLogCommits`'s own "peek one past `max_commits` to
  tell truncation from completion" trick calls `Next()` on the underlying
  iterator *before* deciding whether to keep the result, so a commit that
  iterator has already fully resolved (its own children pushed onto the
  frontier) can be discarded by the caller without ever being returned -
  silently lost, neither emitted nor pending.
  `TestGitLogCursorPagesReachEveryCommitExactlyOnce` caught this on a
  purely linear fixture (no merge involved at all) once the redesign's
  first draft moved `path` filtering to `pathFilteringCommitIter`: the walk
  under-counted by exactly the commits lost this way. `collectLogCommits`'s
  own `discarded` return value and `multiRootCommitIter.PushBack` are the
  fix - restoring precisely the hash a wrapping layer consumed but never
  used back onto the frontier before it is read.

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
