# Authentication protocol profiles

Authentication interoperability is pinned by an immutable profile identifier. A
profile names a protocol family and an exact RFC, Internet-Draft, API revision,
or Flowstate contract. Configuration must name the profile; an unknown or
incompatible identifier is refused at load time and there is no revision
negotiation or fallback. `oauth-token-exchange-rfc8693` preserves the existing,
narrow RFC 8693 client behavior: JWT subject (or delegated actor) token, optional
audience/resource/scope, and an access-token response.

`flow auth capabilities` prints only public implementation metadata. It never
loads policy files, tokens, keys, client secrets, or environment credentials.
`policy_enabled` is consequently false in this local inventory; a deployment
may join it with its separately protected effective-policy report.

## Lifecycle procedure

1. **Introduce.** Add a new descriptor and identifier for every new draft
   revision, plus independent golden request/response fixtures and negative
   vectors. Drafts are experimental or preview and require both their identifier
   and the explicit experimental opt-in in configuration.
2. **Promote.** Never change an existing identifier's semantics or revision.
   Publish a new stable identifier, retain the old descriptor, and document the
   exact configuration edit. Promotion does not automatically migrate policy.
3. **Migrate.** Operators test conformance, change one target/issuer profile,
   and roll back by restoring the old identifier. Draft profiles are retained
   for at most two minor releases or 180 days after replacement, whichever is
   longer, and remain opt-in throughout.
4. **Deprecate/remove.** Mark the descriptor deprecated, announce the removal
   release and deadline, then remove implementation only after the bounded
   window. A removed identifier becomes unknown and startup fails.
5. **Emergency disable.** Ship a release that marks the affected implementation
   unavailable. Startup must fail for policies selecting it; it must never fall
   back to another revision. Operators must explicitly select a fixed profile
   or disable the affected target/issuer.

The descriptor schema is the source of truth for roles, required metadata,
grants and token types, sender constraint, claims, capability dependencies, and
downgrade refusal. Generated protobuf types keep inventories and administrative
surfaces on the same vocabulary.
