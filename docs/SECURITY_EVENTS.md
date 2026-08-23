# Security events

Flowstate's security-event boundary accepts signed Security Event Tokens (SETs)
through a provider-neutral verifier and normalizes recognized CAEP, RISC, and
Shared Signals event profiles. Transport authentication is useful, but is never
the event's authority: the SET must independently pass issuer, audience,
signature, algorithm, event-type, `iat`, `exp`, `jti`, and subject-format checks.
A subject already known to Flowstate does not make an event valid.

The implementation is in `pkg/flowstate/v1/securityevent`. A deployment supplies
a `Verifier` whose configured issuer owns the verification keys and whose
configured audience is this Flowstate deployment. It also supplies a `Store`.
The included memory store is bounded and suitable only for one process; a
cluster must use a durable, linearizable store.

## Bounds and failure semantics

The receiver caps the HTTP body, individual token, decoded claims, claim-tree
depth and nodes, batch size, accepted skew and age, replay entries, security
state entries, deliveries per minute, inspection result size, and compaction
work. Unknown JSON fields, extra JSON values, unknown event URIs, ambiguous
batches, mismatched subject types, and duplicate `(issuer, jti)` pairs are
refused. Raw tokens and subject identifiers never enter observations or metric
labels.

An event that requires immediate enforcement is acknowledged only after
`Store.Apply` returns. `Apply` is required to be linearizable and durable. A
store error is not interpreted as “not revoked”; authentication, authorization,
MCP requests, credential refresh, signal delivery, and external-call checks
fail closed when their required strong read cannot complete. Eventual replicas
may serve explicitly weak administrative inspection, never an immediate use
boundary.

Entries expire at their verified event expiry. `Compact` removes at most the
operator-supplied bound on each call, so cleanup cannot monopolize the store.
The replay record remains independently bounded: expiry of effective state does
not turn a still-live duplicate delivery into a new event.

## Durable runs

Security state is consulted at use boundaries outside replayed workflow logic.
Every normalized event carries one explicit action:

| Change | Existing durable run |
|---|---|
| Group membership or other prospective claims change | Keep running; refuse or alter only future external calls after a fresh authorization decision. |
| Device posture loss | Pause at the next use boundary. |
| Credential or issuer-key compromise | Quarantine at the next use boundary; no credential refresh or external call proceeds. |
| Principal disabled, session revoked, application access withdrawn, delegation revoked, or tenant relationship removed | Cancel at the next use boundary. |

The transition is recorded as event data. Workflow replay does not reread a
mutable policy and does not silently reinterpret already-recorded decisions.
Run control code consumes the explicit action and records its cancel, pause, or
quarantine command as a new durable event.

## Operations and audit

Administrative surfaces should call `Inspect` for a bounded snapshot,
`EmergencyRevoke` for a linearizable operator-authored entry with an explicit
expiry and run action, and `Compact` for bounded expiration work. Emergency
revocation fails closed if the store cannot durably apply it.

The `Observer` reports the privacy-safe stages `receipt`, `application`,
`expiry`, `duplicate`, and `refusal`, plus bounded reason codes and normalized
event type. Audit sinks may correlate an identifier in protected audit storage,
but metrics must not label issuer, audience, `jti`, token, tenant, or subject.
