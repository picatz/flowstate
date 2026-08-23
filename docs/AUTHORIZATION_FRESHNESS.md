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

Protected-resource metadata uses a strong digest ETag and
`Cache-Control: public, max-age=300, must-revalidate`; clients should send
`If-None-Match`. A `304` saves transfer only. Before authorization, the server still
compares the session, decision, and credential descriptor identities with current
authorization state. Metadata responses expose the revision and digest for rolling
upgrade diagnostics without treating those headers as caller assertions.

Operational tests must exercise rolling upgrades and split-brain fleet members,
metadata rollback, a policy revision changing during an MCP session, issuer
removal, DPoP becoming mandatory, and an old client presenting a removed scope.
The safety assertion in each case is refusal or reauthorization, not merely that a
new metadata document was fetched.
