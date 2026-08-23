# Workload assertion discovery and compatibility

Flowstate is a **workload assertion issuer**. It is not, by virtue of issuing a
JWT, an OAuth authorization server or an OpenID Provider. This distinction is a
wire-contract rule, not terminology: metadata must never make a client attempt a
protocol that the server cannot complete.

## The four surfaces

### 1. Workload assertion issuer metadata

`GET /.well-known/workload-identity-configuration` is Flowstate's authoritative
metadata document. Its profile is
`https://flowstate.dev/profiles/workload-assertion/v1`. It contains:

* `issuer`, exactly the value minted as `iss`;
* `jwks_uri`;
* `assertion_profiles_supported`;
* `signing_alg_values_supported` and `key_types_supported`, derived from the
  keys actually published; and
* `claims_supported`, derived from the built-in mint vocabulary plus the
  deployment's `federation.declared_claims` allowlist.

Assertions are compact signed JWTs with `typ: JWT`. They are audience-bound,
short-lived bearer assertions, not ID tokens and not JWT access tokens (the
`at+jwt` profile does not apply). Every assertion has `iss`, `sub`, `aud`,
`exp`, `nbf`, `iat`, `jti`, `namespace`, `deployment`, `workflow`, `step`,
`on_behalf_of`, `on_behalf_of_issuer`, and `run_mode`; `run` is present when a
durable run ID exists. Declared carried claims may additionally be present.

An assertion used as `client_assertion` follows RFC 7523 only at that exchange:
the recipient dictates the subject and audience and the parameter uses the
RFC's JWT-bearer assertion-type identifier. Publishing a key does not claim
that Flowstate exposes an RFC 7523 token endpoint or accepts client assertions.

### 2. JWKS publication

`GET <jwks_uri>` publishes only public signing keys. Each key has `kid`, `use:
sig`, `alg`, and its key-type parameters; no private parameter is served. The
supported pairs are `RS256`/`RSA` (RSA 2048 bits or larger), `ES256`/`EC`
(P-256), and `EdDSA`/`OKP` (Ed25519). Metadata reflects the active and retained
rotation keys, so operators must select the intersection accepted by their
consumer. Old public keys remain until the configured retention expires.

### 3. OAuth authorization-server metadata

Flowstate does not perform this role. It does not serve RFC 8414 authorization
server metadata and advertises no authorization endpoint, token endpoint,
grants, token endpoint authentication methods, registration endpoint, or client
capabilities. The separate RFC 9728 protected-resource document describes how
to authenticate *to* a Flowstate resource; it does not turn that resource into
an authorization server.

### 4. OpenID Provider metadata

Flowstate does not implement the required OpenID Connect semantics. There is no
authorization endpoint, Authorization Code/Implicit/Hybrid response, `openid`
scope, ID Token, UserInfo endpoint, end-user authentication, or OIDC client.

Some WIF products nevertheless hard-code
`/.well-known/openid-configuration`. Flowstate serves an **OIDC-shaped workload
issuer compatibility document** there, identified by
`workload_issuer_profile` =
`https://flowstate.dev/profiles/oidc-shaped-workload-issuer/v1`. It contains
only `issuer`, `jwks_uri`, `claims_supported`, signing algorithms, and that
profile marker. It deliberately omits `id_token_signing_alg_values_supported`,
`response_types_supported`, `subject_types_supported`, `scopes_supported`, and
all endpoints. A consumer that validates full OpenID Provider Metadata will
correctly reject it; configure that consumer with a direct/static JWKS when it
offers that mode rather than adding fictional capabilities here.

## Consumer compatibility matrix

These statements describe the exact Flowstate side of the integration. Cloud
products change independently; verify the provider's current algorithm, key
count, claim, URL, and audience limits before rollout.

| Consumer | Exact compatibility statement |
|---|---|
| **AWS IAM / STS web identity** | Compatible when IAM accepts the minimal OIDC-shaped discovery document and the configured signing key type/algorithm, or when the provider can be provisioned without discovery. Configure the Flowstate issuer URL, its HTTPS trust, and an audience equal to the IAM OIDC provider client ID; restrict `sub` and `aud` in the role trust policy. AWS consumes the assertion in `AssumeRoleWithWebIdentity`; Flowstate is not an OP. Prefer RS256 for the conservative interoperability profile. |
| **Google Cloud Workload Identity Federation** | Compatible through an OIDC workforce/workload pool provider when configured with the Flowstate issuer and an accepted algorithm, or with the provider's uploaded JWKS option. Set the assertion audience to an allowed audience and explicitly map desired top-level claims (`google.subject=assertion.sub`, plus attributes). The compatibility document is workload-issuer metadata, not proof of OIDC conformance. Prefer RS256. |
| **Microsoft Entra / Azure** | A federated identity credential can accept a Flowstate assertion only where that product can resolve this minimal issuer/JWKS shape and accepts its signing algorithm. Configure exact issuer, subject, and audience; Entra's token endpoint is the downstream exchanger, not a Flowstate endpoint. Use RS256. If the Azure surface requires complete OP metadata, it is not directly compatible; place a conforming broker in between rather than falsifying Flowstate metadata. |
| **Kubernetes** | Kubernetes is commonly the *upstream issuer* of projected service-account tokens, which Flowstate can verify through the cluster's discovery/JWKS. Flowstate assertions are not Kubernetes service-account tokens and cannot be mounted as such. An API server configured for external JWT authentication may accept them only under that authenticator's issuer, audience, algorithm, claim-mapping, and discovery/static-key rules; this does not make Flowstate a Kubernetes service-account issuer. |
| **Vault / OpenBao** | The JWT auth method is compatible when configured with `jwks_url` (the most exact mode), supported algorithms, bound audience, bound subject, and bound claims. The OIDC auth method expects interactive OIDC and is not compatible. `oidc_discovery_url` is compatible only if that deployment accepts the minimal workload compatibility document; prefer direct JWKS so no OP behavior is implied. |
| **SPIFFE / SPIRE** | Not a JWT-SVID issuer. Flowstate subjects are workload paths, not SPIFFE IDs; its discovery path and key bundle are not the SPIFFE JWT-SVID discovery/bundle contract. A SPIFFE-aware broker may verify a Flowstate assertion and mint a JWT-SVID, or Flowstate may trust an upstream SPIFFE identity, but direct substitution is unsupported. |
| **Flowstate-to-Flowstate** | Native and fully compatible. Configure the downstream trust policy with the exact issuer and audience; it discovers `jwks_uri`, verifies `kid`/signature/time/audience, maps namespace, and applies subject/claim rules. RS256, ES256, and EdDSA are supported when both deployments run this implementation. No OAuth or OIDC role is involved. |

## Contract invariants

Tests decode a real assertion and require every minted claim to occur in
metadata, require every advertised claim to be mintable by the complete
fixture, compare the minted algorithm and published key type to metadata, and
reject every OAuth/OIDC capability field listed above. Routing tests also keep
metadata and JWKS public: consumers fetch both before possessing a credential.
