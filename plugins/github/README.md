# flowstate-plugin-github

GitHub forge tasks for Flowstate: `github.pull_request_get` and
`github.issue_comment` (the original one-forge-operation proof, read and
write respectively), plus a read/audit tier added alongside them -
`github.pull_request_list`, `github.pull_request_files`, `github.issue_get`,
`github.issue_list` - built on
[google/go-github](https://github.com/google/go-github) - the real GitHub
API client, deliberately, rather than a hand-rolled one. See "Why go-github"
below for why that is the right call for this one dependency even though
`plugins/vcs` (this repository's sibling plugin) takes the opposite position
on `git`.

Two examples live at
[`examples/plugins/github`](../../examples/plugins/github); read those first
if you want to see it work rather than read about it -
[`workflow.yaml`](../../examples/plugins/github/workflow.yaml) and
[`issue-comment.yaml`](../../examples/plugins/github/issue-comment.yaml)
exercise the original pair, and
[`triage.yaml`](../../examples/plugins/github/triage.yaml) exercises the
read/audit tier.

## Building

```console
go build -o /path/to/plugins/flowstate-plugin-github ./plugins/github
```

## Examples, kept honest

All three files below are pasted in whole, not summarized, and
`TestReadmeExamplesMatchTheFilesOnDisk` in this package holds them to the real
files byte for byte in both directions - a file added under
[`examples/plugins/github`](../../examples/plugins/github) with no matching
block here fails the build, same as a block that drifts from its file. The
convention: an HTML comment naming the file, on the line immediately before
the fence.

<!-- example: examples/plugins/github/workflow.yaml -->
```yaml
edition: v2026.3
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
  # A pull request old enough to be merged and stable, so this example's
  # output does not change out from under it.
  number: 1
  owner: golang
  repo: go
steps:
  - id: pr
    github.pull_request_get:
      number: ${vars.number}
      # No token: a public repository's pull requests are readable
      # unauthenticated, at a much lower rate limit. Compare this to
      # comment-inputs.json, where posting a comment requires one.
      owner: ${vars.owner}
      repo: ${vars.repo}
  - id: announce
    log:
      message: '${"pull request #%d (%s) - %s".format([vars.number, steps.pr.state, steps.pr.title])}'
outputs:
  title:
    value: ${steps.pr.title}
    description: the pull request's title, read from GitHub's API without this build knowing its schema
  state:
    value: ${steps.pr.state}
    description: open or closed
  head_sha:
    value: ${steps.pr.head_sha}
    description: the commit at the tip of the pull request's branch
```

<!-- example: examples/plugins/github/issue-comment.yaml -->
```yaml
edition: v2026.3
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
      body: ${inputs.body}
      number: ${inputs.number}
      owner: ${inputs.owner}
      repo: ${inputs.repo}
      # A secret reference, resolved by the host before the task, never a literal - see
      # plugins/github/README.md, "Authentication," for how this plugin
      # answers it: a GitHub App installation token when one is configured,
      # a personal access token otherwise.
      token: ${secret('github:token')}
outputs:
  comment_url:
    value: ${steps.comment.html_url}
    description: where to see what this run just posted
```

<!-- example: examples/plugins/github/triage.yaml -->
```yaml
edition: v2026.3
name: github-review-and-issue-triage
description: A review-triage pass over a public repository using the "github" plugin's read/audit tier - what is open, which files a candidate change touches, and the full record of whichever issue needs the next look. Runs with no arguments.

# github.pull_request_list, github.pull_request_files, github.issue_get, and
# github.issue_list are this plugin's read/audit tier, added alongside its
# original one-forge-operation proof (github.pull_request_get,
# github.issue_comment in workflow.yaml and issue-comment.yaml) - see
# plugins/github/README.md, "Read/audit tier," for which candidates were
# rejected and why. This file is the shape a security engineer (or an agent
# doing the same job) actually reaches for, deliberately not a CI shape:
# what pull requests are in flight, which files the most recent one touches
# before reading any diff content, and both a bounded listing of open
# issues and the full record (body included) of the oldest one still
# waiting for a look.
#
# Every task here accepts an unset token and works exactly the same against
# a public repository - see workflow.yaml's own comment on why, and this
# plugin's README, "Authentication," for the private-repository case.
vars:
  owner: golang
  repo: go
  # A path fragment worth flagging on a candidate change - stand-in for
  # whatever a real review policy would actually care about (an auth
  # module, a secrets file, a dependency manifest). golang/go's own
  # standard library ships a "crypto" tree, so this is a fragment its pull
  # requests plausibly touch without being tied to any one PR's identity.
  sensitive_path_fragment: crypto
steps:
  - id: open_prs
    github.pull_request_list:
      max_results: 5
      # No token: a public repository's pull requests are readable
      # unauthenticated, at a much lower rate limit - see workflow.yaml.
      owner: ${vars.owner}
      repo: ${vars.repo}
      state: open

  # The pull request this file goes on to name three times - here, in the log
  # line, and in an output. Bound once, so the "whatever is actually open right
  # now" choice is made in one place (docs/STYLE.md R5, `flow lint`).
  - id: newest_pr
    value: ${steps.open_prs.pull_requests[0]}
  - id: pr_files
    github.pull_request_files:
      max_results: 100
      # Bound to the most recently created open pull request
      # github.pull_request_list just found - github.pull_request_list's
      # own default sort, GitHub's "created", descending - rather than a
      # number this file hard-codes, so the audit chain is "whatever is
      # actually open right now," not one pull request frozen in time.
      number: ${steps.newest_pr.value.number}
      owner: ${vars.owner}
      repo: ${vars.repo}
  - id: open_issues
    github.issue_list:
      direction: asc
      max_results: 5
      owner: ${vars.owner}
      repo: ${vars.repo}
      # Ascending by creation time, explicitly - GitHub's own default for
      # this endpoint is "created" descending (newest first), which would
      # make element zero the newest match, not the oldest this file's own
      # outputs claim it to be. direction: asc is what actually earns the
      # name "oldest" below.
      sort: created
      state: open
  - id: issue_detail
    github.issue_get:
      # The audit chain's other half: github.issue_list found which issues
      # are open; github.issue_get reads the one at the front of that
      # queue in full - body included, which a listing deliberately leaves
      # out (see IssueSummary's own doc comment).
      #
      # .filter(i, !i.is_pull_request) exists because GitHub's own
      # repository-issues endpoint - what github.issue_list calls -
      # answers both issues and pull requests through this same response
      # (is_pull_request on each entry says which); without the filter,
      # element zero could be a pull request wearing an issue's number,
      # and this step (and open_issue_count below) would count and detail
      # the wrong thing. See IssueSummary.is_pull_request's own doc
      # comment (github.proto) for the endpoint quirk this filter exists
      # to correct for.
      number: ${steps.open_issues.issues.filter(i, !i.is_pull_request)[0].number}
      owner: ${vars.owner}
      repo: ${vars.repo}
  - id: announce
    log:
      message: '${"%s/%s - %d open pull request(s) seen, most recent is #%d touching %d file(s); %d open issue(s) seen, oldest of them is #%d (\"%s\")".format([vars.owner, vars.repo, steps.open_prs.pull_requests.size(), steps.newest_pr.value.number, steps.pr_files.files.size(), steps.open_issues.issues.filter(i, !i.is_pull_request).size(), steps.open_issues.issues.filter(i, !i.is_pull_request)[0].number, steps.issue_detail.title])}'
outputs:
  most_recent_open_pull_request:
    value: ${steps.newest_pr.value.number}
    description: the most recently created open pull request github.pull_request_list found (see PullRequestListOutputs.truncated for whether more than max_results are actually open)
  touches_a_sensitive_path:
    value: ${steps.pr_files.files.exists(f, f.filename.contains(vars.sensitive_path_fragment))}
    description: whether that pull request's own files (github.pull_request_files - filenames and line counts, no diff content read) include a path matching sensitive_path_fragment - the review-triage question this task exists to answer before any diff is ever read
  open_issue_count:
    value: ${steps.open_issues.issues.filter(i, !i.is_pull_request).size()}
    description: how many open issues (pull requests excluded - see issue_detail's own comment on why) github.issue_list found, capped at max_results (see IssueListOutputs.truncated for whether more exist)
  oldest_open_issue_title:
    value: ${steps.issue_detail.title}
    description: the full title of the oldest open issue among the listing above (github.issue_list's own sort/direction inputs, set to created/asc - see that step's comment), read in full via github.issue_get - the single-record detail a listing's own summary leaves out
  oldest_open_issue_is_actually_a_pull_request:
    value: ${steps.issue_detail.is_pull_request}
    description: GitHub answers issues and pull requests through the same endpoint - always false here, since issue_detail's own number is chosen by filtering pull requests out first (see that step's comment); kept as an explicit sanity check on that filter rather than an assumption
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
| `github.pull_request_list` | reads | yes | only for a private repository |
| `github.pull_request_files` | reads | yes | only for a private repository |
| `github.issue_get` | reads | yes | only for a private repository |
| `github.issue_list` | reads | yes | only for a private repository |
| `github.issue_comment` | writes | **no** | always |

### Execution-mode posture

`github.issue_comment` deliberately remains reachable in a local rehearsal. It
is a real comment, not a preview: the required target inputs keep the example
from running accidentally, while task policy, credential release, and egress
controls independently constrain an actual call. Deployment-owned egress
composition remains #1323; execution mode does not fill that gap. Changing this
established behavior to a production-only gate would be a compatibility change,
so it is not inferred from the newer Slack task's different notification
contract.
`TestIssueCommentWritesWithoutAProductionCaller` sends one comment to a local fake
API with an inert token and no production caller, proving the posture without a
real credential or GitHub mutation.

Six tasks, not two, not a wide surface either: the original pair proved one
forge operation end to end - reading a pull request and commenting on it -
and everything else here is the read/audit tier added once that had landed,
matching plugins/git's own evolution (`git.ls_remote` first, `git.log` and
`git.read_file` added as its read/audit tier in the same spirit). See "What
was left undone" below for what is still not built, and "Naming" below for
the naming question that has to be settled before any of this could honestly
be `forge.*` instead.

## Read/audit tier

`github.pull_request_list`, `github.pull_request_files`, `github.issue_get`,
and `github.issue_list` are the read/audit tier - the operations a security
engineer (or an agent doing the same job) actually reaches for when auditing
a repository rather than acting on it, chosen and rejected against a
standing preference for a small surface: if an existing task's output plus a
CEL filter already answers a question, it does not earn a new task (the same
call plugins/git's own `git.ls_remote` makes against `list_tags`/
`list_branches` - see that plugin's README).

**Chosen:**

- **`github.pull_request_list`** - "what is in flight," filtered by state
  and (optionally) branch. `github.pull_request_get` answers "what is pull
  request #N's state" for a number the caller already has; nothing existing
  answers "which pull requests exist at all," which is a genuinely different
  capability, not a filter over one this plugin already had.
- **`github.pull_request_files`** - "which files did this pull request
  touch, and how much" - the review-triage primitive: usually enough to
  decide whether a change needs a closer look, before reading any diff
  content at all. Deliberately returns no diff text - see
  `pull_request_files.go`'s own doc comment for why that would duplicate a
  different primitive (`git.read_file`'s "what is there now," or a future
  diff-shaped task) rather than extend this one.
- **`github.issue_get`** - the single-record read/audit-tier counterpart to
  `github.pull_request_get`, for a workflow that already has an issue number
  (from a webhook, a `wait_for_signal` payload, or a previous
  `github.issue_list` call) and needs the full record, body included.
- **`github.issue_list`** - "what needs attention," filtered by state,
  label, and an updated-since cutoff - the same "list exists, get does not"
  gap `pull_request_list` closes, on the issue side.

**Rejected:**

- **`github.workflow_run_list`** ("is main green"). Not because it is a bad
  primitive - it is a strong one - but because this repository's own example
  portfolio already skews heavily toward deploy/release/CI shapes, and this
  read/audit tier's own worked example (`triage.yaml`) was deliberately
  chosen to be something else: an audit/triage pass a non-CI reader
  recognizes. Revisit this if a future CI-observability need asks for it
  directly.
- **`github.release_get`/`github.release_list`**. A release's tag and commit
  are already answerable today, for a public or private repository, by
  `git.ls_remote` with `prefix: "refs/tags/"` (a name and a sha, with no
  GitHub-specific credential at all) - what is left that only GitHub's own
  Releases API adds is release notes, draft/prerelease flags, and asset
  metadata, which is real but is a second, smaller primitive rather than an
  obvious member of *this* tier's "what needs attention" shape. Left for a
  later, explicitly-scoped addition rather than folded in here to round the
  set out to eight.
- **A separate `github.issue_list` state/label filter combinator** (e.g.
  splitting "list my issues" or "list issues by milestone" into their own
  tasks). `github.issue_list`'s own `state`, `labels`, and `since` inputs,
  plus a CEL filter over what it returns, already answer every narrower
  question this would - exactly the "an existing task's output plus a filter
  already answers it" refusal above.

## Resuming a truncated listing

`github.pull_request_list`, `github.issue_list`, and
`github.pull_request_files` all report `truncated: true` when more matched
than the call returned - `max_results`, the request budget
(`maxListRequests`), or the byte budget (`maxResultBytes`), whichever binds
first (see "Bounds this tier enforces" below). Each also accepts `cursor`
back and returns `next_cursor` on a truncated result, the same "next_cursor
in, cursor out" shape issue #216 asked every bounded list task in this
repository to grow, and plugins/git's own `git.log` (PR #217) landed first.

GitHub's own listing endpoints are page-number pagination, not an opaque
server-side cursor the way `git.log`'s own commit-DAG walk is - a
repository's issues or pull requests can gain or lose entries between two
calls, which a commit history (immutable once written) never does. This
plugin's cursor packs a page number, a within-page skip count (for a resume
that lands mid-page, not only at a page boundary - see `paginate.go`'s own
doc comment), and a fingerprint of the filters the walk was running under,
including the API base the call actually reached - which for an
authenticated call is the operator-configured `GITHUB_API_BASE_URL`, not the
task's own `base_url` input (`effectiveAPIBase` in `client.go`, and
`cursor.go`) - opaque to a workflow, and refused
outright if replayed against different filters, or a different GitHub API
endpoint, than the call that produced it. That fingerprint check is also
what makes a cursor's own forward progress worth keeping even across a run
of pages GitHub answered with zero items apiece (a legitimate response
shape while a large result set is computed - see `cursorHasResumePosition`
in `cursor.go`): the position still advanced, and withholding a cursor just
because nothing was collected yet would be the exact dead end #216 exists
to close, one call later than the original bug.

That mechanism is honest about what it can and cannot guarantee against a
mutating list - and the guarantee is narrower than it may first look, in
both directions:

- **`github.issue_list` and `github.pull_request_list`** require `sort:
  created` and `direction: asc` alongside `cursor` - refusing one set
  without the other - because that ordering guarantees a genuinely NEW
  issue or pull request (whose created time is later than everything a
  walk has already reached) always appends past it rather than shifting it.
  That closes only the append-only case. Two gaps remain, both real and
  both demonstrated as passing tests rather than left to be discovered:
    - **removal can cause a miss**: an item closed (under a filter that now
      excludes it) or deleted between two calls shifts every later page
      backward by one, which this plugin's cursor - a position, not an
      identity - cannot detect.
      `TestIssueListCursorCanMissAnItemDeletedBetweenPages` shows it
      directly.
    - **an older item newly matching can cause a REPEAT**: "created"
      ascending says nothing about an item that already existed but did not
      match this call's own filters until between two calls - a reopened
      issue or pull request, one newly carrying a requested label, or one
      whose `updated_at` just crossed a `since` cutoff. Its created time
      never moved, so it re-enters the matching set at its original,
      possibly-earlier position, which can sit before the saved cursor and
      cause a resumed call to repeat an item it already returned.
      `TestIssueListCursorCanRepeatAReopenedIssueBetweenPages` shows it
      directly; `TestIssueListCursorToleratesAnInsertionBetweenPages` shows
      the append-only case the stable sort actually does close, for
      contrast.
  Neither gap makes the cursor useless - the common case, nothing in the
  matching set changing shape between two calls fetched moments apart, is
  the overwhelming majority - but this is a best-effort resume position for
  a triage/audit read, not a transactional snapshot, and both gaps are
  stated in `IssueListInputs.cursor`/`PullRequestListInputs.cursor`'s own
  doc comments (`proto/github/v1/github.proto`) rather than left implicit.
- **`github.pull_request_files`** has no ordering lever at all - GitHub's
  `ListFiles` endpoint takes no `sort` parameter this task could ask it to
  hold stable, so its own cursor's contract is honestly weaker still: it
  resumes *near* where a truncated call stopped, and a commit pushed to the
  pull request between two calls can cause a file to be missed or (less
  likely) returned twice. Acceptable for the triage read this task is - see
  `PullRequestFilesInputs.cursor`'s own doc comment for the full argument.

<!-- example: examples/plugins/github/list-resume.yaml -->
```yaml
edition: v2026.3
name: github-list-resume
description: Reads a repository's open issues in two bounded pages using github.issue_list's cursor input - the resume shape issue #216 asks every bounded list task to grow, closed for this plugin's read/audit-tier listings. Runs with no arguments.

# github.issue_list, github.pull_request_list, and github.pull_request_files
# all truncate rather than serializing an unbounded response, and all
# report that honestly as `truncated: true`. Before `next_cursor` existed on
# this plugin, that boolean was the same dead end plugins/git's own git.log
# had before PR #217: a caller who received page one of a longer listing
# had no way to ask for page two. This file demonstrates the fix -
# `next_cursor` in, `cursor` out - for github.issue_list, this plugin's own
# instance of the same shape:
#
#   page one:  github.issue_list(max_results: 3, sort: created, direction: asc)
#              -> truncated: true, next_cursor: <opaque>
#   page two:  github.issue_list(..., cursor: <opaque>)
#              -> continues exactly where page one stopped
#
# GitHub's own listing endpoints are page-number pagination, not an opaque
# server-side cursor the way plugins/git's own commit-DAG walk is - a
# repository's issues can gain or lose entries between two calls, which a
# commit history (immutable once written) never does. This plugin's cursor
# is honest about that difference rather than pretending git.log's
# exactly-once guarantee transfers unchanged - see
# plugins/github/proto/github/v1/github.proto, IssueListInputs.cursor, and
# plugins/github/cursor.go for the full contract:
#
#   - sort: created and direction: asc are REQUIRED alongside cursor (this
#     task refuses a cursor set without them, and never produces a
#     next_cursor without them either) - that ordering guarantees a
#     genuinely NEW issue (created later than everything a walk has already
#     reached) always appends past it rather than shifting it. That is the
#     append-only case, and only that case.
#   - what is NOT closed, in two directions - both real, both demonstrated
#     as passing tests, neither merely asserted:
#       - removal can cause a MISS: an issue closed (under a state filter
#         that now excludes it) or deleted between two calls shifts every
#         later page backward by one, which this task's cursor - a
#         position, not an identity - cannot detect
#         (TestIssueListCursorCanMissAnItemDeletedBetweenPages).
#       - an OLDER issue newly matching can cause a REPEAT: "created"
#         ascending says nothing about an issue that already existed but
#         did not match until between two calls - reopened, newly
#         carrying a requested label, or an updated_at that just crossed
#         since. Its created time never moved, so it re-enters the
#         matching set at its original, possibly-earlier position, ahead
#         of the saved cursor, and a resumed call repeats it
#         (TestIssueListCursorCanRepeatAReopenedIssueBetweenPages).
#     Neither gap makes this cursor useless - it is a best-effort resume
#     position for a triage/audit read, not a transactional snapshot - but
#     "created asc closes insertion" is true only for brand-new items, and
#     this file says so precisely rather than implying more.
#   - a cursor replayed against different filters (a different owner, repo,
#     state, label set, sort, direction, max_results, or base_url than the
#     call that produced it) is refused outright, not silently walked
#     against the wrong sequence.
#
# github.pull_request_list's own cursor works identically (sort: created,
# direction: asc required). github.pull_request_files's cursor is weaker
# still: GitHub's ListFiles endpoint has no sort parameter at all, so that
# task's cursor resumes NEAR where a truncated call stopped with no
# ordering guarantee to lean on - see PullRequestFilesInputs.cursor's own
# doc comment.
#
# What this file deliberately does NOT show: walking to exhaustion. Doing
# that for real means looping - fetching page after page until
# `truncated: false`. A `loop:` primitive landed on `main` as PR #220 after
# this file was first written; converting this example to use it (rather
# than the two fixed steps below) is left for a follow-up, kept separate
# from the cursor-integrity fixes this file otherwise documents, so the two
# changes stay reviewable independently. Two fixed steps, page one then
# page two, is what this file shows meanwhile. The exhaustive-walk tests
# that prove this plugin's cursor semantics are actually correct - every item
# reached, subject to the guarantee stated above, across as many pages as
# it takes, including a walk where the byte or item bound truncates
# mid-page - live in Go instead, at TestIssueListCursorWalksToExhaustion and
# its neighbors (plugins/github/issue_list_cursor_test.go,
# plugins/github/pull_request_list_cursor_test.go,
# plugins/github/pull_request_files_cursor_test.go, and
# plugins/github/cursor_test.go for the encoding itself), which are free to
# loop because they are not written in the DSL.
vars:
  owner: golang
  repo: go
steps:
  - id: page_one
    github.issue_list:
      direction: asc
      # No cursor: this is a fresh walk, oldest-first.
      max_results: 3
      owner: ${vars.owner}
      repo: ${vars.repo}
      sort: created
      state: open
  - id: page_two
    github.issue_list:
      cursor: ${steps.page_one.next_cursor}
      # Empty when page_one was not truncated (this task always populates
      # next_cursor empty in that case) - github.issue_list then treats an
      # empty cursor exactly like an ordinary fresh call, which the "not
      # truncated" case having nothing left to resume makes the right
      # fallback rather than an error.
      direction: asc
      max_results: 3
      owner: ${vars.owner}
      repo: ${vars.repo}
      sort: created
      state: open
  - id: announce
    log:
      message: '${"page one: %d issue(s) (truncated=%s); page two: %d issue(s) (truncated=%s), resumed at %s".format([steps.page_one.issues.size(), string(steps.page_one.truncated), steps.page_two.issues.size(), string(steps.page_two.truncated), steps.page_one.next_cursor])}'
outputs:
  page_one_numbers:
    value: ${steps.page_one.issues.map(i, i.number)}
    description: the first page's issue numbers, oldest first (sort created, direction asc)
  page_two_numbers:
    value: ${steps.page_two.issues.map(i, i.number)}
    description: the second page's issue numbers - continuing immediately after page_one's last entry (subject to the stated guarantee - see this file's own header comment for what a removal between calls can still do)
  resumed_from:
    value: ${steps.page_one.next_cursor}
    description: the opaque cursor page_two was resumed from - empty if page_one was not truncated
```

## Bounds this tier enforces, and the resource each one matches

Every list-shaped task here bounds two independent resources GitHub, not
this plugin, controls - the same "bound anything that consumes untrusted
input" reasoning as `pkg/flowstate/v1/server`'s own `List` RPC and its
`maxListScan`/`maxListRequests` pair:

- **Items collected** (`max_results`, default `defaultMaxResults` (30),
  ceiling `maxMaxResults` (200) - refused, not silently clamped, over the
  ceiling, the same discipline `plugins/git`'s `clampMaxCommits` documents).
  This is the resource a workflow author asks for.
- **Requests made** (`maxListRequests`, 20 page requests per call). This is
  the resource GitHub - or, in the adversarial case this bound actually
  exists for, any peer answering on GitHub's behalf - controls independently
  of the first: `go-github`'s own `*Response.NextPage` can legitimately stay
  non-zero on a page that carried zero items while a large result set is
  still being computed, so a loop bounded only by items collected does not
  terminate against a peer that always answers with an empty page and a
  next-page cursor. See `paginate.go`'s `paginateBounded` and
  `TestPaginateBoundedStopsAgainstAPeerThatPagesForever` for a peer built to
  do exactly that, and
  `TestPaginateBoundedCapsAtMaxItemsAndReportsTruncated` /
  `TestPaginateBoundedStopsAtTheBoundaryWithoutSpendingAnExtraRequest` for
  proof that each bound is actually *reached*, not merely respected -
  `scanned <= maxListScan` is also satisfied by a listing that gave up after
  one batch, and the tests here assert the ceiling was hit, per CLAUDE.md's
  own "Bounds must be proven reached."

Every list output also carries `truncated`: `false` only when GitHub itself
said there was nothing more (`NextPage == 0`), never merely because this call
stopped looking - the same honesty `git.log`'s own `Truncated` field
practices, and the same reasoning `git.log`'s own doc comment gives for why
a bounded read that says nothing about its own limit lets a caller believe a
partial answer is complete.

## Authentication

Every task's `token` input is a whole secret reference - for example
`${secret('env:GITHUB_TOKEN')}` or `${secret('github:token')}` - never a
literal. Every task declares `token` in `secret_inputs` and
`required_secret_inputs`; the host resolves it under the caller's namespace,
scrubs it from plugin errors and outputs, and invokes the plugin with only the
value. `pull_request_get` treats an
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

The `github:` provider is a compatibility path for the worker-wide App/PAT
configuration above. Because that configuration has no per-tenant selector,
it refuses named namespaces rather than sharing one credential across tenants.
Multi-tenant deployments must use a namespace-aware host secret provider for
`token`. The default namespace retains existing single-tenant behavior.

`GITHUB_API_BASE_URL` overrides the API base for GitHub Enterprise Server.
It is also the credential destination allowlist: an authenticated task uses
this operator-selected base (or `https://api.github.com` when it is unset),
and a task-provided `base_url` that names any other destination is refused.
Unauthenticated reads may still select another public API base, subject to the
egress policy in `client.go`.

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

**Every request is bounded and egress-governed.** `client.go` takes the
deployment's own egress policy - the bytes the worker granted this process at
launch, from its `--egress-policy` or, when the operator configured none, the
default its built-in HTTP task runs under - and hands its `*http.Client` to both
the App JWT-minting request and every go-github call. So a deny rule an operator
writes reaches a `github.*` task, and the response-byte cap and the egress rules
cover the GitHub Enterprise Server case too, not only github.com. The
response-byte and timeout bounds are this plugin's own, stated over the grant
(`sdk.EgressPolicyWithBounds`), because a paginated API response is not the shape
`max_response_bytes` is sized for.

**Not a clone-based plugin.** Issue #171 tracked a packfile-inflation bound
across "all three clone-based plugins (vcs, github, git)"; checked directly
against this module's own dependencies, this plugin has no go-git import at
all and never fetches or parses a git pack - every operation here is a
go-github REST call, already covered by the response-byte cap above (which,
for ordinary JSON bodies, bounds decompressed content directly, unlike a
git pack's independent per-object compression). There is no packfile
inflation surface here to close, so this plugin carries no
`packBoundedStorer`. It should still only be pointed at a GitHub host
(github.com or an Enterprise Server instance) the deployment trusts, the
same operational baseline as any credentialed integration.

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

Task credential resolution now uses the shared host `secret_inputs` path, so
the original provider and namespace gaps no longer apply.

The SDK now covers two gaps originally recorded here. Unknown mutation
outcomes use `sdk.OutcomeUnknown`, which the host maps to
`ErrorKindUpstreamUnknown` and never retries. GitHub rate-limit reset times and
secondary-limit `Retry-After` values use `sdk.UnavailableAfter`, so both drivers
receive the backend's preferred retry delay, saturated at the host's safety
bound instead of discarded into generic backoff.

## What was left undone, and why

- **Write and mutation coverage beyond `issue_comment`**
  (`github.pull_request_create`, `github.check_run.list`,
  `github.issue_create`, and the rest of what the original task list asked
  for). Deliberately not built: this engagement's design review asked for
  one forge write proven end to end, and for the naming question below to be
  settled first, since adding more mutating tasks now would mean naming and
  potentially renaming several more of them once a portable `forge.*`
  vocabulary exists. The read/audit tier above is a narrower, lower-risk
  extension in the meantime - nothing it added writes anything, so the
  naming and mutation-safety questions this bullet is about do not apply to
  it.
- **`github.release_get`/`github.release_list` and
  `github.workflow_run_list`.** See "Read/audit tier," "Rejected," above for
  why each was left out of this pass specifically, rather than folded in to
  round the read/audit tier's set out.
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
