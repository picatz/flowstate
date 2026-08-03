// Command flowstate-plugin-vcs provides version-control tasks: reading a
// repository's commit history and diffing two revisions of it, over the git
// smart-HTTP protocol via go-git (a pure-Go implementation - nothing here
// ever execs a git binary, jj, or any other subprocess; see "No subprocesses,
// ever" below).
//
// # Why "vcs" and not "git"
//
// The task names this plugin provides - vcs.log, vcs.diff - name what they do
// in words that mean the same thing for any version-control backend: git,
// jj, or something else entirely. This build happens to speak git, because
// go-git is what exists today, but nothing about the task names, their inputs,
// or their outputs says so. A future flowstate-plugin-vcs backed by jj (or by
// gitbutler, or libgit2) could claim the exact same two task names and a
// Flowfile using them would not need to change - only the binary a worker
// loads would.
//
// If git were the only backend this system would ever speak, the honest
// choice would have been to call this plugin "git" and its tasks "git.log"
// and "git.diff". Naming it "vcs" now, before a second backend exists, is a
// bet that the distinction matters later and costs nothing today - but it is
// a bet, and it is written down here rather than left for someone to
// reverse-engineer from a git.log task that turns out to also work with jj
// repositories.
//
// A forge plugin (this repository's sibling, "github") is the other half of
// what GitHub Actions and similar systems conflate into one thing: issues,
// pull requests, reviews, checks. Those concepts do not exist in git itself -
// a bare git remote has no notion of a "pull request" - and a plugin that
// mixed the two could never be paired with a different forge or a different
// vcs backend independently. Keeping them apart is what makes "clone with
// vcs, then open a pull request with github" a composition of two
// interchangeable parts rather than one plugin that only ever means GitHub.
//
// # Why there is no vcs.clone, vcs.commit, vcs.push, or vcs.fetch
//
// This is the load-bearing design decision in this plugin, and it is a
// security decision before it is an engineering one.
//
// GitHub Actions' cache and artifact poisoning class - an untrusted, less
// privileged job writes a cache entry or artifact that a later, more
// privileged job reads and acts on (the TanStack incident is the public
// example) - exists because GHA has a shared cache/artifact concept with no
// trust tier attached to it. Flowstate has never had that class of bug, for
// the dull but effective reason that it has never had shared storage between
// steps or between runs. A `vcs.clone` task that checked a repository out
// somewhere a later, unrelated step could then read - "clone here, subsequent
// steps use the checkout" - would introduce exactly the shared-mutable-state
// shape that class needs to exist. It does not matter how carefully the
// clone step validated its own inputs; the vulnerability would live in the
// handoff, not in either step.
//
// It is also unsound for a reason specific to Temporal, independent of the
// security argument: a workflow's steps are not guaranteed to run on the same
// worker, or even the same machine. A path written to local disk by one
// activity execution is not visible to another activity execution, an
// activity retry (which durable execution can schedule anywhere), or the
// same activity resumed after a worker restart. "Clone to a workspace, later
// steps use it" is not just risky, it is not reliably expressible in this
// execution model at all - the coordinator's own phrasing for the question
// this raised was "a task's result must be content, not a path into shared
// storage," and that is exactly the rule this plugin follows.
//
// So every task this plugin provides is self-contained: it clones what it
// needs, in memory (see clone.go), for the duration of one activity
// invocation, and returns *content* - commit metadata, a unified diff - never
// a filesystem path. There is no cross-run caching of a clone, either: if two
// steps in the same workflow both need this repository's history, both clone
// it. That is real, measurable waste against the alternative, and it is
// accepted deliberately: a cache keyed without a trust tier is the mechanism
// the poisoning class needs, and this plugin does not build one to save a
// second clone.
//
// This also means write operations - vcs.commit, vcs.push, and anything that
// mutates a remote - are out of scope for this version, not merely
// unimplemented. Once there is no persistent checkout, "commit" only means
// something as a single self-contained operation (assemble a tree from
// content given as task inputs, commit it, push it, all inside one
// activity), which is a coherent design but a substantially larger one -
// idempotency alone is a different problem for a push than for a read, since
// a push that partially succeeds and is retried can double a change history
// carries no way to undo. Log and diff are the read-only, idempotent half of
// version control, and they are what this version ships: small, complete,
// and reasoned through, rather than a wider surface with the hard half of it
// thin.
//
// # No subprocesses, ever
//
// Nothing in this plugin calls exec.Command, os/exec, or anything that
// spawns a process. go-git is a pure-Go implementation of the git protocol
// and object format chosen specifically so that never has to be true. Three
// reasons, all of which apply regardless of what gets exec'd:
//
//   - A worker must not depend on what happens to be installed on the
//     machine it runs on. A plugin that shells out to `git` works in
//     development and fails, or silently uses the wrong version, in
//     production - or works in production against a `git` an operator did
//     not intend to trust.
//   - An exec'd process is a much larger sandbox-escape and
//     argument-injection surface than a library call. GitHub Actions'
//     script-injection class exists because `${{ github.event... }}`
//     becomes shell text - Flowstate's own design avoids that structurally,
//     since CEL evaluates a typed expression tree and passes values to a
//     task, never splicing untrusted text into an interpreter. A plugin
//     that builds `exec.Command("sh", "-c", refName)` from a Flowfile's own
//     ref name would reintroduce that exact bug class inside the one
//     component holding forge credentials, undoing a guarantee the rest of
//     the system was built to provide.
//   - A subprocess's environment, file descriptors, and credentials are far
//     harder to keep inside this system's boundaries than a library call's
//     are - an exec'd git process inherits the plugin's environment (which
//     may hold a token) unless deliberately stripped, and a bug in argument
//     construction is a shell injection, not a Go compile error.
//
// Every ref name, branch name, and repository URL this plugin receives is
// attacker-chosen input by construction - it can come straight from a
// Flowfile or from a previous step's output. It is validated against what
// go-git's own revision parser and URL handling actually accept (see
// validate.go) and refused outright when it is not, never sanitised into
// something that might parse differently than what was checked. There is no
// path anywhere in this plugin where one of these strings is concatenated
// into a shell command, a constructed URL, or a filesystem path built by
// joining untrusted segments.
package main
