# Review instructions

Flowstate is a durable, policy-governed workload engine: an author declares a
workload in a YAML+CEL Flowfile, it compiles to a typed Protobuf specification,
and it executes on Temporal. `AGENTS.md` holds the invariants and the working
contract; this file says how to review against them.

External review should confirm rather than discover the basic work. Finding
count is not a quality metric: no findings is a valid and common result, and a
reviewer asked to find gaps must not manufacture them.

## What Important means here

Reserve Important for a consequence a maintainer would hold the merge for.

- A violated architectural invariant, even when the immediate tests pass. The
  ones that bite: a trust boundary that no longer fails closed (authentication,
  authorization, egress, secret access, spec validation); a resolved secret
  value that can reach durable history, a step output, a log, or an error;
  nondeterministic, I/O-bound, or version-sensitive work moved into
  workflow-side code; unbounded work whose count, size, or depth another party
  controls.
- Observable divergence between the local driver and the Temporal driver.
- Incorrect logic on a reachable path, lost or corrupted durable state, or a
  compatibility break in a spec, a wire shape, or a persisted page token.
- A test that passes without exercising the mechanism it names, or that would
  still pass with the change reverted.

Style, naming, layout, and refactoring suggestions are Nit at most, as is a
missing test for a path the change does not alter.

## Cap the nits

Report at most five nits per review. If you found more, say "plus N similar
items" in the summary rather than posting them inline. When everything you found
is a nit, lead the summary with "no factual issues".

## Do not report

- Generated artifacts. `*.pb.go`, `docs/reference/`, and
  `cmd/flow/internal/reference/mirror/` are derived, and an edit hook already
  refuses them; review the schema, the generator, or the source it reads.
- Anything CI already enforces: `gofmt`, `go vet`, staticcheck, CodeQL,
  `vulncheck`, dependency review, commit-message shape, and the `path.go:NNN`
  citation checker.
- An established local idiom, unless the choice creates real correctness or
  maintenance risk. Report that as optional and say so.

## Always check

- A change to a trust boundary denies on missing state and on evaluation error.
- A limit is a work limit, not a reporting limit: bounding what a diagnostic
  prints does not bound the traversal that produced it.
- Behavior observable through both drivers is proved by a shared conformance
  case rather than by a test that only one driver runs.
- A secret crosses compilation and workflow boundaries only as a reference.
- A new authoring capability is reachable end to end: expressible in a Flowfile,
  understood by validation and tooling, executed by both drivers where
  applicable, and taught in the documentation.
- Documentation and comments the change makes false, including a `path.go:NNN`
  citation whose lines no longer hold the symbol the sentence names.

## Verification bar

Cite evidence in the source for a behavior claim rather than inferring it from a
name: a reader must be able to check the claim at the location you give. Label
uncertainty instead of reporting it as fact, deduplicate findings that share one
root cause, and prefer naming the mechanism that needs repair over prescribing a
patch.

## After the first review

A finding is owed one disposition on the head where it arrived. Repeating a
finding the author already dispositioned — fixed, refuted with evidence,
deferred to a scoped issue, or marked stale — is not a new finding, unless the
later head reintroduced the defect or the fix did not cure it. On a re-review,
post Important findings and suppress new nits.

## Summary shape

Open the review body with a one-line tally, such as `1 important, 3 nits`, and
lead with "no factual issues" when that is the case. Say what the change does
before saying what is wrong with it.
