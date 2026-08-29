# Workload identity federation

Flowstate is a **workload assertion issuer**. It mints short-lived, audience-bound
signed JWTs that say which workload is running, in which namespace, on whose
behalf. It is not, by virtue of issuing a JWT, an OAuth authorization server or an
OpenID Provider: there is no authorization endpoint, no token endpoint, no client
registration, no end-user authentication.

That distinction is a wire contract, not terminology. Metadata must never make a
client attempt a protocol this server cannot finish. But the inverse is just as
real, and it is the reason this document exists in the shape it does: **a document
that omits what a consumer requires does not make the consumer more correct, it
makes federation stop working at the far end**, with no error on this side.

So Flowstate publishes two documents, and they are not alternatives.

## The two metadata documents

### `/.well-known/openid-configuration` — OpenID Provider Metadata

This is the path several major consumers hard-code, and the document they parse
before they will accept an issuer at all. Flowstate serves the OpenID Connect
Discovery fields those consumers require:

| Field | Why it is there |
| --- | --- |
| `issuer` | Must exactly equal the `iss` of every assertion and the origin this document is served from. Consumers check it; so does this package's own verifier. |
| `jwks_uri` | Where the public keys are published. |
| `response_types_supported`, `subject_types_supported` | Required by OpenID Connect Discovery. Flowstate is not interactive, so these are the minimum a strict consumer needs to accept the document. |
| `id_token_signing_alg_values_supported` | The algorithms of every published key. **AWS IAM OIDC providers, Google Cloud Workload Identity Federation configured by discovery, and Vault/OpenBao's `oidc_discovery_url` all require this field.** |
| `claims_supported` | Derived from the mint's own vocabulary, so an operator writing an attribute mapping can see what there is to map. |
| `scopes_supported` | Present because some consumers require the field. |

Flowstate does not claim to be a conforming OpenID Provider, and this document
advertises no endpoint it does not serve. It is the compatibility surface, and
the fields above are load-bearing: `TestTheOIDCDocumentKeepsEveryFieldAStrictConsumerReads`
reads them off the wire, because a field that stops being serialized is invisible
to a test that decodes into the struct that produced it, and because nothing in
this repository would otherwise notice it go — Flowstate-to-Flowstate federation
reads only `issuer` and `jwks_uri`.

### `/.well-known/workload-identity-configuration` — workload issuer metadata

Flowstate's own contract, at a path Flowstate chooses, saying what OpenID Connect
has no spelling for. Its profile is
`https://flowstate.dev/profiles/workload-assertion/v1`, advertised in
`assertion_profiles_supported` so a relying party keys its compatibility on a
versioned value rather than on the well-known path a document arrived at.

* `issuer` and `jwks_uri`, the same values as above — there is one issuer and one
  key set.
* `assertion_profiles_supported` — the profile identifier.
* `signing_alg_values_supported` — the algorithms of every published key.
  Deliberately *not* `id_token_signing_alg_values_supported`: an assertion is not
  an ID token, and that name means what OpenID Connect says it means, in the other
  document.
* `claims_supported` — the built-in mint vocabulary plus the deployment's
  `federation.declared_claims` allowlist.
* `key_types_supported` — the `kty` of every published key. OpenID Provider
  Metadata has no field for this, and it is the fact an operator most often needs
  before a consumer will accept the issuer: a federation target that takes RSA and
  not EC otherwise fails at verification time, at the far end, rather than at
  configuration time.

`signing_alg_values_supported` and `key_types_supported` are read from **one**
snapshot of the key set, under one lock and one reading of the clock. Answering
them separately lets a rotation, a revocation, or a retained key's retention
expiry land between the two, and publishes a document naming an algorithm whose
key type it does not name — cached by the relying party for five minutes, and
then refusing assertions signed by a key it was told half of.

## What Flowstate does not serve

**RFC 8414 authorization server metadata.** Flowstate advertises no authorization
endpoint, token endpoint, grant types, token endpoint authentication methods, or
registration endpoint. The separate RFC 9728 protected-resource document describes
how to authenticate *to* a Flowstate resource; it does not turn that resource into
an authorization server.

**OpenID Connect provider behaviour.** No authorization code, implicit or hybrid
response; no `openid` sign-in flow; no ID Token; no UserInfo endpoint; no end-user
authentication; no OIDC client.

`TestWorkloadMetadataNeverClaimsAnUnimplementedProtocol` pins the negative
direction on the workload document, which is under no compatibility obligation
and therefore has no excuse for advertising any of it.

## The assertions themselves

Compact signed JWTs with `typ: JWT`, audience-bound and short-lived. They are
bearer assertions, not ID tokens and not JWT access tokens — the `at+jwt` profile
does not apply. Every assertion carries `iss`, `sub`, `aud`, `exp`, `nbf`, `iat`,
`jti`, `namespace`, `deployment`, `workflow`, `step`, `on_behalf_of`,
`on_behalf_of_issuer` and `run_mode`; `run` is present when a durable run ID
exists. Declared carried claims may additionally be present.

An assertion used as `client_assertion` follows RFC 7523 at that exchange only:
the recipient dictates subject and audience, and the parameter uses the RFC's
JWT-bearer assertion type. Publishing a key does not claim that Flowstate exposes
an RFC 7523 token endpoint or accepts client assertions.

`GET <jwks_uri>` publishes public signing keys only — `kid`, `use: sig`, `alg`,
and the key-type parameters; no private parameter is ever served. The supported
pairs are `RS256`/`RSA` (2048 bits or larger), `ES256`/`EC` (P-256) and
`EdDSA`/`OKP` (Ed25519). The key set covers the active key and every key retained
from a rotation until its retention expires, so an operator must pick the
intersection their consumer accepts.

## Consumer compatibility

These statements describe the Flowstate side exactly. Cloud products change
independently; verify the provider's current algorithm, key count, claim, URL and
audience limits before rollout.

| Consumer | Compatibility |
| --- | --- |
| **AWS IAM / STS web identity** | Configure an IAM OIDC provider with the Flowstate issuer URL; IAM fetches `/.well-known/openid-configuration` and requires `id_token_signing_alg_values_supported`. Set the assertion audience to the provider's client ID and restrict `sub` and `aud` in the role trust policy. AWS consumes the assertion through `AssumeRoleWithWebIdentity`. Prefer RS256 for the conservative profile. |
| **Google Cloud Workload Identity Federation** | Configure an OIDC workload pool provider with the Flowstate issuer, or with the provider's uploaded-JWKS option where discovery is not wanted. Set the audience to an allowed audience and map top-level claims explicitly (`google.subject=assertion.sub`, plus attributes). Prefer RS256. |
| **Microsoft Entra / Azure** | A federated identity credential can accept a Flowstate assertion where the product resolves this issuer/JWKS shape and accepts the signing algorithm. Configure exact issuer, subject and audience; Entra's token endpoint is the downstream exchanger, not a Flowstate endpoint. Use RS256. Where the Azure surface requires complete OP metadata, place a conforming broker in between rather than falsifying Flowstate metadata. |
| **Kubernetes** | Kubernetes is normally the *upstream* issuer here: Flowstate verifies projected service-account tokens through the cluster's discovery and JWKS. Flowstate assertions are not service-account tokens and cannot be mounted as such. An API server configured for external JWT authentication may accept them under that authenticator's issuer, audience, algorithm, claim-mapping and discovery rules; that does not make Flowstate a service-account issuer. |
| **Vault / OpenBao** | The JWT auth method is compatible with `jwks_url` (the most exact mode) plus supported algorithms, bound audience, bound subject and bound claims. `oidc_discovery_url` is also compatible — it reads the OpenID Provider Metadata above, including `id_token_signing_alg_values_supported`. The *OIDC* auth method expects interactive OIDC and is not compatible. |
| **SPIFFE / SPIRE** | Not a JWT-SVID issuer. Flowstate subjects are workload paths, not SPIFFE IDs, and its discovery path and key bundle are not the SPIFFE JWT-SVID contract. A SPIFFE-aware broker can verify a Flowstate assertion and mint a JWT-SVID, or Flowstate can trust an upstream SPIFFE identity, but direct substitution is unsupported. |
| **Flowstate to Flowstate** | Native. Configure the downstream trust policy with the exact issuer and audience; it discovers `jwks_uri`, verifies `kid`, signature, time and audience, maps the namespace and applies subject and claim rules. RS256, ES256 and EdDSA all work when both ends run this implementation. No OAuth or OIDC role is involved — which is why it reads only `issuer` and `jwks_uri`, and why it cannot be the thing that notices the OIDC document losing a field. |

## Serving it

Both metadata documents and the key set come from one handler, and all three are
public and must stay public: a relying party fetches them before it holds any
credential, and they contain only public keys. Putting them behind the API's own
authentication is the usual reason a working federation setup suddenly stops
verifying.

```go
mux.Handle(auth.DiscoveryPath, issuer.Handler())
mux.Handle(auth.WorkloadIssuerMetadataPath, issuer.Handler())
mux.Handle(issuer.JWKSPath(), issuer.Handler())
```

`flow server` registers all three. The key set path is the one an operator can
move (`federation.jwks_path`), so `NewIssuer` refuses a key set path equal to
either metadata path: two identical `mux.Handle` patterns panic at start-up, and
a diagnosis beats a crash.

## Contract invariants

The tests are the contract. `TestWorkloadMetadataIsTheMintingContract` decodes a
real minted assertion and requires every minted claim to appear in the metadata
*and* every advertised claim to be mintable, then compares the minted algorithm
and the published key type against what the document says — so the advertised
vocabulary cannot become a parallel list that agreed with the mint on the day it
was written. `TestTheOIDCDocumentKeepsEveryFieldAStrictConsumerReads` and
`TestWorkloadMetadataNeverClaimsAnUnimplementedProtocol` pin the two documents in
opposite directions. `TestPublishedAlgorithmsAndKeyTypesDescribeOneKeySet` pins
the single snapshot. Routing tests keep all three endpoints reachable without a
credential, and keep them absent entirely on a deployment that issues no
assertions.
