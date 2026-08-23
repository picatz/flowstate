# Authorization policy bundles

Authorization policy is promoted as an immutable `PolicyBundle`, not edited in
place. The deterministic protobuf encoding (with `digest` cleared) is hashed;
that digest covers rules, vocabulary-facing names, claim projections,
relationships, and every dependency revision. Signatures and approvals name
that digest. Apply retrieves and hashes the artifact again and requires
`approval.bundle_digest == activation.approved_digest ==
activation.applied_digest == computed_digest`. Consequently a source-byte or
dependency-revision change after review is a new artifact and cannot inherit an
old approval.

## Review and rollout workflows

Review and rollout are Flowstate workloads. A review workload carries
`PolicyReviewWorkflowInput`, runs schema/CEL/vocabulary/projection validation,
curated simulation, redacted-history replay, and semantic diff activities, then
waits at durable approval gates. The resulting `PolicyApproval` is the input to
a rollout workload (`PolicyRolloutWorkflowInput`). The rollout uses activities
to apply tenant and deterministic-canary slices, observes fleet convergence and
safety signals, waits on durable timers for activation expiry, and records a
`PolicyRollback` when a configured signal crosses its threshold. External I/O
(artifact-store reads, signature verification against current configuration,
fleet mutation, and signal reads) belongs in activities; workflow-side code
only sequences recorded results. This preserves Flowstate/Temporal replay
determinism.

The serving path fails closed if the policy store is unavailable, an activation
has expired, artifact integrity fails, or the local node has not received the
active digest. Every decision identifies the bundle name, bundle revision,
digest, and each matching `rule@revision`, so an audit record never has to infer
which policy answered.

## Bootstrap boundary

The policy-author public keys, their separation-of-duty groups, and the
per-policy-class approval thresholds are **bootstrap root-of-trust
configuration** loaded by the deployment. They are not fields in a policy
bundle and no policy decision authorizes their replacement. Rotating that root
uses the deployment's out-of-band administrative mechanism. This boundary is
intentional: letting a governed bundle authorize a signer or threshold change
would let the policy authorize its own author.

Break-glass does not cross that boundary. It still needs a valid approval for
the exact digest, mandatory audit evidence, and a conspicuous expiry no more
than four hours away. It changes rollout urgency, not who is trusted.

## Proto records

`policy.proto` defines the wire-stable bundle, rule revision, signature,
provenance, validation, simulation, semantic diff, approval, activation,
rollback, authorization request/decision, review input, rollout input, and
safety-signal records. Historical replay inputs must be redacted before they
enter a review workflow; semantic-diff output retains only tenant/action/
resource request classes and never claims or subjects.
