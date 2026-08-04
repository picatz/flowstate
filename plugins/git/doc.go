// Command flowstate-plugin-git provides git-specific version-control tasks:
// reading a remote's refs without cloning (git.ls_remote) and writing a
// commit to a branch in one activity (git.commit_push), all over the git
// smart-HTTP protocol via go-git - a pure-Go implementation, so nothing here
// ever execs a git binary, a hook, or any other subprocess.
//
// # Why "git" and not "vcs"
//
// plugins/vcs (this repository's sibling) is the small, deliberately
// backend-agnostic core: vcs.log and vcs.diff, named so a future jj-backed
// implementation could claim the same task names. This plugin is the other
// half of the factoring issue #149 settled: rich, unapologetically git
// vocabulary - a sha, a branch as refs/heads/<name>, a unified diff in git's
// own format - because what commit_push does (materialize a tree, build a
// commit object, force-with-lease it onto a ref) is git's object model and
// git's push semantics specifically, not a lowest-common-denominator
// abstraction over version control in general. See vcs's own doc.go for the
// argument that named this split before either plugin existed; this file
// only adds what is new here.
//
// # One activity, one write
//
// git.commit_push does materialize -> apply -> commit -> push inside a
// single activity invocation, exactly as issue #149's write-operations
// design (comment 5172963530) settled: content in (a `files:` map and/or a
// unified `patch:`), content out (a sha), nothing crossing steps as a
// filesystem path. Scratch, where any exists, is invocation-scoped and gone
// on return - see tree.go's own comment for why this version needs none at
// all.
//
// # The idempotency trick: deterministic commits make push retry-safe
//
// A push is a mutation, and a network failure after send is an unknown
// outcome - the class retry_on_unknown_outcome exists for in the core http
// task. This plugin's answer is the one the design settled on: construct the
// commit deterministically when the caller supplies timestamp (author and
// committer time as an input, never the activity's own wall clock), so a
// retried attempt building the same tree on the same parent with the same
// message produces the identical sha. Pushing that sha to a branch already
// at it is then a no-op success, and the retry probe in commit_push.go is
// exactly that comparison: does the remote branch already point at the sha
// this call would create? When timestamp is empty, the wall clock still
// varies between attempts, so the probe falls back to comparing content -
// parent, tree, and message - which content-addressing makes just as
// reliable a signal of "this already landed" as an exact sha match, only one
// field looser.
//
// # Concurrency: compare-and-swap, never force
//
// Every push in this plugin requires the remote branch to currently be
// exactly base_ref before it writes - go-git's own compare-and-swap
// primitive for this is PushOptions.RequireRemoteRefs, which checks the
// advertised ref against an exact hash and refuses otherwise. This is worth
// being precise about against the design comment's own words, because
// go-git's vocabulary is not the CLI's: go-git also has a PushOptions.Force
// and a PushOptions.ForceWithLease, but ForceWithLease in go-git only takes
// effect when Force is also set to true - it narrows a force push, it does
// not, on its own, provide a safe non-force compare-and-swap. RequireRemoteRefs
// is the field that does that without Force ever being true, and it is what
// this plugin uses; commit_push never sets Force. See errors.go for how a
// mismatch there - or the remote ref being absent when it was required
// present - is turned into [sdk.Conflict], a distinct, non-retried
// classification a workflow's `dispatch:` can react to deliberately, per the
// design comment's own instruction. This divergence from the design
// comment's literal phrase "force-with-lease" is reported here rather than
// left for a reader to notice go-git's Force/ForceWithLease pairing does not
// match the CLI's single --force-with-lease flag.
//
// # Why go-git and go-gitdiff, not "git apply"
//
// go-git builds, reads, and pushes git objects, but it does not apply a
// unified diff to a tree - there is no Patch.Apply the way there is a
// Patch.Encode. The design comment says "apply with go-git," and that is not
// quite what go-git can do on its own: this plugin uses
// github.com/bluekeyes/go-gitdiff (github.com/bluekeyes/go-gitdiff/gitdiff),
// a second small, pure-Go dependency, specifically for gitdiff.Parse and
// gitdiff.Apply. Both operate entirely on io.Reader/io.Writer over in-memory
// byte slices - a base file's content in, the patched content out - with no
// filesystem call anywhere in either function. That is worth being explicit
// about against another instruction this plugin was given: "if applying a
// patch honestly needs a filesystem, use a per-invocation temp dir." It
// turned out not to need one at all - gitdiff's API is exactly narrow enough
// to apply one file's patch against one file's bytes, which is precisely the
// shape tree.go's own tree-rebuilding needs anyway, so there is no scratch
// directory for a retry to leave anything in, and nothing to defer-remove
// because nothing was ever created. Same subprocess argument as go-git
// itself: "git apply" would be exec, argument-injection-adjacent (a
// crafted patch header is attacker-chosen input, same as everything else
// this plugin touches), and dependent on whatever git happens to be on the
// worker's PATH.
//
// # Attacker-adjacent input, and what is refused rather than sanitised
//
// A patch and a files map both name paths, and a workflow step's `patch:`
// input can come straight from a Flowfile or from a previous step's output -
// a coding agent's own patch, unreviewed by anything but this plugin. Every
// path is validated against what a git tree may actually contain (see
// validate.go): refused outright, never rewritten into something that might
// mean differently than what was checked, for the same reason doc.go in
// plugins/vcs gives for revision strings. Three checks matter specifically
// because they are the ones a naive "no leading /, no .." check misses:
//
//   - Nothing may be written under ".git/" - a path segment literally named
//     ".git" is refused regardless of position, not just at the root.
//   - Nothing may be written *through* a path that base_ref already has as a
//     non-directory entry - a symlink, a regular file, or (see the next
//     point) a submodule gitlink. tree.go's own recursive rebuild refuses to
//     descend into an existing leaf entry as though it were a directory,
//     which is what stops a change from landing at "vendor/lib/new-file"
//     when base_ref's own history already made "vendor/lib" a symlink to
//     somewhere outside the tree - the write never happens on git's side,
//     because there is no tree object to put it in, but a workflow that
//     later reads that path back through a *different* tool (a checkout, a
//     built artifact) would silently follow the link. Refusing here is
//     refusing before that tool ever runs.
//   - No submodule (gitlink, mode 160000) is accepted or produced anywhere
//     in this version - not as an existing base_ref entry this change's
//     paths would traverse through, and not as a mode a patch's own file
//     entries name. A submodule names a second repository's URL, and
//     accepting one here would mean re-running this plugin's own URL and
//     scheme checks against a value that arrived nested inside a tree rather
//     than as a task input - out of scope for this version, refused with a
//     positioned diagnostic rather than silently accepted and mishandled.
//
// # URL schemes: an allowlist, and the one still missing from it
//
// Only https:// is accepted (see validateRepositoryURL) - the allowlist
// direction plugins/vcs already takes, and the one CLAUDE.md's own reasoning
// about http://'s core task egress policy asks for: refuse by allowlist, so
// a scheme neither this build nor go-git's maintainers have thought hard
// about is refused by default rather than admitted by omission. This is
// reported as a real, deliberate gap rather than smoothed over: git itself
// also speaks ssh://, and a security review of this plugin specifically
// asked for an https/ssh allowlist. ssh:// is not in it. The reason is the
// same one plugins/vcs's own README gives for leaving it out: this plugin
// has no credential story for an SSH key (no secret scheme resolves one, no
// containment test protects one), and go-git's ssh transport, given no
// explicit auth, reaches for the local ssh-agent and the operator's own key
// files by default - an entirely different trust boundary than "a token this
// task resolved from a secret reference," and one this plugin does not open
// without having reasoned through it as carefully as the https path. Adding
// ssh:// support is real, additive future work, not a one-line allowlist
// entry.
//
// # Bounds this plugin cannot fully close, said plainly
//
// clone.go's egress policy bounds compressed bytes read from the transport,
// the same way plugins/vcs's does. What it does not bound is decompressed
// size - a small packfile that inflates to an enormous object graph
// ("pack bomb") is a real class of attack against any git implementation
// that reads a peer's pack data, and neither go-git nor this plugin puts a
// hard ceiling on inflation ratio today. This is the same shape of gap
// clone.go's vcs counterpart documents for shallow depth versus blob size -
// named here rather than left for someone to discover, not solved by this
// version.
package main
