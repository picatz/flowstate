# Signing-key rotation

Flowstate signing deployments use one monotonically increasing `generation`
shared by every server and worker. A generation identifies the active signer
and the complete JWKS publication snapshot; it is not a timestamp and is never
reused. Private keys remain behind the `auth.Signer` boundary. A KMS or HSM
implementation sends claims to the service for signing and returns a compact
JWS; it must not export private material into configuration, logs, workflow
history, or the Flowstate process.

## Staged fleet rotation

1. Create the new non-exportable KMS/HSM key and record its public half. Choose
   generation `N+1` in the fleet's strongly consistent configuration store.
2. Publish a generation `N+1` public-key set containing **both** the generation
   `N` key and the new key. Keep the old key for at least the maximum assertion
   lifetime plus verifier-cache skew (normally `DefaultKeyRetention`).
3. Reload canary servers and workers. Readiness must stay false until the active
   signer's key ID and algorithm occur in the generation `N+1` set. Inspect the
   reload, publication, activation, and refusal events/counters; they contain
   generation and key ID only.
4. Roll generation `N+1` across the fleet. Mixed instances are safe: old
   instances mint with the retained old key and new instances publish and
   verify both keys. Never remove generation `N` while one of its assertions can
   still be valid.
5. After the retention deadline and after all instances report `N+1`, remove
   the old public key. Retirement is audited. Attempts to reload `N` (or reuse
   `N+1`) are refused, so stale configuration cannot roll the signer back.

## Emergency revocation

Compromise changes the availability trade-off: first activate a higher
generation whose signer is already present in its public set, then call
`SigningKeyring.Revoke` for the compromised key on every publisher. Revoking an
active key makes readiness fail immediately, preventing new unverifiable
assertions. Do not wait for normal retention. Purge downstream JWKS caches where
the relying party supports it, roll every process, and correlate refusal,
activation, retirement, and publication audit events by generation and key ID.
Previously issued assertions bearing the compromised key must be treated as
revoked even if their `exp` time has not arrived.
