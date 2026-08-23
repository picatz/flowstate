# Authorization freshness

An effective protected-resource descriptor is the complete authorization contract
for one resource: public metadata, trusted authorization servers and their key
material, scope-to-operation mappings, accepted proof methods, delegation limits,
and tenant/policy boundaries. Every descriptor has two identities:

* `revision`, an operator-assigned monotonically increasing integer; and
* `digest`, SHA-256 over the canonical effective descriptor.

The revision orders intentional changes. The digest detects rollback, revision
reuse, and temporarily inconsistent fleet members. A lower revision is a rollback;
equal revisions with different digests are configuration split-brain. Both fail
closed. HTTP cache age is never evidence that an authorization decision is fresh.

## Where the identity travels

Both values are carried in MCP session records, OAuth client-cache entries,
authorization decisions, issued delegation capabilities, credential-cache keys,
audit events, trace attributes (`flowstate.auth.policy_revision` and
`flowstate.auth.resource_digest`), and administrative diagnostics. Secrets and
credentials are not inputs to the digest; their identifiers, requirements, and
policy boundaries are.

Every authorization decision compares its descriptor identity with the current
local descriptor. A cache hit is usable only after that comparison. A peer result
from the same revision with another digest is refused and reported as fleet
inconsistency rather than silently selecting either view.

## Change classes

The following changes require immediate session invalidation, credential eviction,
and reauthorization: issuer removal or disabling; signing-key compromise; stronger
proof (including DPoP becoming mandatory); scope removal or narrowing; delegation
narrowing; and any tenant-boundary, subject mapping, or policy-boundary change.
Already issued bearer credentials cannot be made safe by their original expiry in
these cases.

Additive scope publication, adding a trusted issuer, adding an optional proof
method, key rotation where the old key is not compromised, endpoint changes that
preserve the same trust boundary, and stricter cache bounds are compatible. Existing
sessions and credentials may continue only until their normal refresh, at which
point they must bind to the current descriptor. No change may extend an existing
credential's lifetime.

When classification is unavailable or ambiguous, the change is immediate. An
operator may choose immediate invalidation for a compatible change, never the
reverse.

## Public metadata caching

Protected-resource metadata uses a strong ETag over the served document and
`Cache-Control: public, max-age=300, must-revalidate`; clients should send
`If-None-Match`, which is matched per RFC 9110 section 13.1.2 — `*`, a
comma-separated list, and a weak `W/` prefix all compare as that section
requires. A `304` saves transfer only. Before authorization, the server still
compares the session, decision, and credential descriptor identities with
current authorization state.

The **descriptor digest is deliberately not served**. It covers the complete
effective descriptor, including the trust policy, its claim mappings, its
tenancy map and its secret boundaries — which is what makes it useful for
telling fleet members apart, and exactly why it may not be published on a
route that has no authentication. A hash of private policy on an anonymous
endpoint is an offline oracle: guess a mapping, hash the candidate, compare,
at no cost and unobservably. It is available to an operator who is already
inside, through `ProtectedResource.Digest`, and to telemetry
(`flowstate.auth.resource_digest`).

`Flowstate-Policy-Revision` is sent only when a deployment actually configures
a revision. Defaulting it and announcing it made a constant look like a
measurement.

Operational tests must exercise rolling upgrades and split-brain fleet members,
metadata rollback, a policy revision changing during an MCP session, issuer
removal, DPoP becoming mandatory, and an old client presenting a removed scope.
The safety assertion in each case is refusal or reauthorization, not merely that a
new metadata document was fetched.
