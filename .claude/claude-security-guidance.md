# Security guidance for Flowstate

Flowstate compiles an author's YAML+CEL Flowfile into a Protobuf specification
and runs it on Temporal. `THREAT_MODEL.md` holds the boundaries and the honest
gaps; this is the short version a review of a diff needs.

## Fail closed at every trust boundary

Authentication, authorization, egress, secret access, and spec validation deny
on missing state and on evaluation error. A policy that cannot be evaluated is a
denial, and a CEL rule that errors denies. A new `nil`, empty, or error path that
reaches an allow is a vulnerability rather than a style question. The single
exception is a mechanism the architecture documents as availability-only, and it
says so at the call site.

Positive provenance is the tenancy rule: an execution whose recorded tenant is
absent or unreadable belongs to nobody and is refused, rather than resolved into
a default tenant. Caller-supplied provenance is forgeable and is not the same
evidence as provenance the server recorded. Cross-tenant reach through a list, a
scan, a schedule, a signal, or a page token is a tenancy break.

## Secrets never enter durable history

A secret crosses compilation and workflow boundaries only as a reference: a
scheme and a name. The resolved value lives inside the activity that uses it,
for that call — never a step output, never a log line, never an error, never a
workflow variable, and never anything Temporal persists in history or run state.
The value type redacts itself when marshaled or formatted and refuses to
deserialize, so flag a change that can print, return, wrap, or persist a
revealed value past that boundary, including through a `%v` on a struct that
holds one. Authorization happens before the secret store is consulted, on every
resolution rather than once per run.

## Workflow-side code stays deterministic

Nondeterministic, I/O-bound, or version-sensitive work belongs in an activity.
In workflow-side code the wall clock, random sources, environment reads, network
calls, and map iteration order are defects: a replay that diverges corrupts a run
that already committed. The engine threads a deterministic clock; `time.Now` is
the wrong one. A change to workflow-side logic that alters the sequence of
recorded commands needs a version gate rather than a silent edit.

## Bound work where it is spent

Files, collections, traversals, diagnostics, retries, payloads, and responses
another party controls need an explicit limit. A reporting limit is not a work
limit: truncating what a diagnostic prints does not bound the walk that produced
it. Recursive descent over an author-supplied or caller-supplied structure needs
a depth bound as well as a size bound.

## Untrusted input

An author's Flowfile, a caller's request, a webhook body, a plugin's response,
and an external HTTP response are each attacker-controlled in some deployment.
CEL expressions are author input: they are compiled, type-checked at load, and
cost-limited, and a new evaluation path that skips the shared construction loses
all three.

The approval card served to a browser inserts every value through `textContent`.
It assigns no `innerHTML` or `outerHTML` and calls no `insertAdjacentHTML`; an
edit that introduces one is an injection.

## What is not a finding here

Prefer evidence over pattern matching. Shelling out, reflection, and subprocess
use exist deliberately in the plugin host, the task set, and the CLI; judge them
by whether the input reaching them is bounded and authorized, not by the call's
presence. Formatting, naming, and test-organization preferences are not security
findings.
