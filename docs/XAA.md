# Experimental cross-application access (XAA)

Cross-application access (XAA) lets an agent or workload ask one application to
use a capability exposed by another without collapsing the applications, the
human, and the software acting for that human into one principal. This document
is Flowstate's profile and role boundary. It is a design and interoperability
contract, **not a declaration that XAA is generally available**.

Flowstate composes the standards-specific components named below. It does not
mint or accept a Flowstate-private "XAA token", and an XAA exchange never turns
the application, user, workload, and agent into aliases of one subject. Those
are separate participants in the authorization chain even when a particular
request has no human or no agent.

> [!WARNING]
> XAA is experimental and disabled by default. An operator-visible kill switch
> is an admission gate, not a hint: when it is off, or when the configured
> profile is not the pinned interoperable profile, every XAA request is refused.
> It must never retry as an ordinary bearer-token request, erase the actor chain,
> substitute ambient Flowstate authority, or fall back to broader OAuth scopes.

## The pinned profile

An implementation advertises one exact profile identifier. Changing any
required component produces a new identifier and requires the interoperability
gate again; version negotiation must not silently select a weaker profile.

| Concern | Pinned component |
| --- | --- |
| Authorization-server discovery | OAuth Authorization Server Metadata (RFC 8414) and, for an OpenID Provider, OpenID Connect Discovery |
| Resource-server discovery | OAuth Protected Resource Metadata (RFC 9728) |
| Resource targeting | OAuth resource indicators (RFC 8707), on every authorization and token request and in the resulting audience |
| Interactive grant | Authorization code with S256 PKCE; pushed authorization requests (PAR, RFC 9126) where the authorization server advertises them |
| Workload/client grant | OAuth client credentials or token exchange (RFC 8693), selected explicitly for the relationship rather than by fallback |
| Client authentication | An asymmetric method advertised by the authorization server (mTLS or `private_key_jwt`); public interactive clients use PKCE and no invented shared secret |
| Delegation | RFC 8693 `act` chains. `sub` remains the resource owner/subject and `act` identifies the current actor; an implementation that cannot preserve the complete chain refuses it |
| Proof of possession | DPoP (RFC 9449) or mutually authenticated TLS (RFC 8705), as selected by resource metadata and policy. Bearer is not an automatic fallback |
| Incremental assurance | OAuth authorization details (RFC 9396) carrying PARC operations; standard OAuth errors and challenges carry consent or stronger-authentication requirements |
| Token form | The resource server's advertised and locally configured form. Token introspection (RFC 7662) and JWT access-token validation are separate adapters, not assumptions made by orchestration |
| Revocation | RFC 7009 when exposed, plus introspection/cache invalidation and local policy re-evaluation. Short lifetime bounds the gap where a peer cannot provide immediate revocation |

The profile orchestrator passes typed results between these components:
discovered issuer and resource metadata, resolved tenant, client
authentication, authorization details, a participant chain, proof-key handle,
and a resource-specific credential. It never serializes those values into a
second all-purpose credential. In particular, an ID token proves an
authentication event to its client; it is not an API access token or a token to
exchange merely because it is a JWT.

The profile identifier, authorization-server issuer, protected-resource URI,
client identifier, tenant mapping identifier, PARC vocabulary version, grant
identifier, participant-chain digest, proof-key identifier, and final decision
identifier belong in the audit record. Tokens, authorization codes, assertions,
proof private keys, and consent-page contents do not.

## Identity is a chain, not a blended principal

The chain has four independently optional or required participant classes:

1. **Application** — the registered OAuth client. Registration and its key
   belong to one application deployment, not to whichever user invokes it.
2. **User** — the resource owner or human subject, when the operation is on a
   person's behalf. Tenant-local user mapping is explicit and may fail.
3. **Workload** — the durable Flowstate run and step whose identity is attested
   by Flowstate. A run is not the OAuth client registration.
4. **Agent** — the software actor selecting or invoking an operation, when one
   is involved. An agent is not silently treated as its user.

The protected resource authorizes the complete chain. `client_id`, `sub`, and
`act` answer different questions; a Flowstate workload assertion adds workload
provenance without replacing any of them. Missing participants are represented
as absent, never inferred from another participant. Reordering, truncating, or
turning a chain into one display string is a validation failure.

## Five independent roles

One deployment may be configured for more than one role, but each role has a
separate listener/configuration boundary, trust policy, keys, audit events, and
kill switch. Enabling one does not enable another.

### 1. Agent or workload OAuth client

This is Flowstate requesting cross-application access for a run, plugin, or
agent-facing operation.

| Property | Contract |
| --- | --- |
| Trust anchors | The configured protected-resource URI; TLS roots; signed/pinned authorization-server and resource metadata policy; the selected AS issuer and keys. Metadata learned from a challenge is input to policy, not trust by itself. |
| Discovery metadata | RFC 9728 from the resource, then RFC 8414/OIDC discovery from an allowed issuer. Exact issuer/resource comparison and SSRF-safe bounded retrieval are mandatory. |
| Client identity | A distinct registered `client_id` per Flowstate application/security boundary, authenticated with its configured asymmetric key where confidential. The workload and agent never become the `client_id`. |
| Tenant resolution | Map `(resource domain, AS issuer, external tenant)` through an operator-owned, versioned mapping to one Flowstate namespace. Ambiguous, absent, or cross-domain mappings refuse before authorization. |
| User and actor | Interactive access preserves the user as `sub`; agent/workload delegation is carried as actor/provenance. App-only access has no invented user. The originating application remains visible. |
| Decision owner | The external authorization server owns grant/consent; the target resource owns final enforcement. Flowstate additionally decides whether policy permits requesting and using the capability. No one decision substitutes for another. |
| Consent and step-up | Redirect only to the discovered, allow-listed AS and preserve transaction binding. Resume the durable operation after explicit success. Cancellation, timeout, denied consent, or unmet assurance fails the operation; background work never clicks through or broadens the request. |
| Audience/resource | The exact protected-resource URI is sent as `resource` and must be the returned token audience. One credential is cached only for that resource, tenant, client, participant chain, grant, and proof key. |
| Proof binding | Generate/select the DPoP or mTLS key before the grant, bind the token and every request to it, and keep private material out of workflow history and plugins that do not own the request. A nonce challenge may retry only the same operation. |
| Audit | Flowstate records request, redirect/pause, resumption, grant (including partial grant), use, refresh/exchange, refusal, revocation observation, and proof failure, correlated to run/step and the external decision ID. |
| Failure mode | Fail the step closed. Do not send an unbound token, retry with bearer, use a token for another resource/tenant, substitute the service's ambient credential, or reinterpret arbitrary returned scopes as permission. |

### 2. Resource server

Flowstate protects Connect RPC, HTTP MCP, API, and plugin-facing capabilities as
separate resource surfaces. A deployment may publish one metadata document with
separately addressable resources, or separate documents; it must not accept one
surface's audience at another.

| Property | Contract |
| --- | --- |
| Trust anchors | Operator-configured issuer entries, issuer verification keys, accepted algorithms, exact resource URI/audience, tenant mapping, proof method, and local PARC policy. An advertised issuer must be accepted by the same verifier. |
| Discovery metadata | RFC 9728 publishes the exact resource, allowed AS issuers, proof methods, and the pinned profile/PARC vocabulary extension. It does not advertise an unsupported scope or downgrade path. |
| Client identity | Read the authenticated OAuth client identity from a validated token/introspection result; never infer it from `User-Agent`, redirect URI, DPoP key, plugin name, or `sub`. Unknown or removed applications are denied. |
| Tenant resolution | Resolve issuer plus external tenant through the resource's mapping, then require it to agree with the requested namespace/resource. Host headers, user-controlled namespace strings, and `sub` suffixes cannot select a tenant. |
| User and actor | Validate the complete subject/actor chain and bind it to request context. A delegated token is not accepted as its bare subject; an app-only token is not assigned a synthetic human. |
| Decision owner | The AS decides what it granted. The Flowstate resource server owns the final, request-time PARC decision, including current application status, tenant boundary, proof, policy, and object state. |
| Consent and step-up | Return the standard challenge/authorization requirement necessary for the denied PARC operation. Never implement a consent UI at the resource and never treat a weaker token as sufficient while step-up is pending. Non-interactive callers receive a terminal actionable denial. |
| Audience/resource | Require the exact URI for this surface. Connect, MCP, general API, and plugin ingress do not share an audience merely because one process serves them. Reject token exchange targets that are not this resource. |
| Proof binding | Verify DPoP method/URI/nonce/replay and the token confirmation claim, or the mTLS certificate binding, before authorization. A valid token with absent, wrong, stale, or replayed proof is denied. |
| Audit | Record authentication outcome, resolved tenant and participant references, PARC action/resource, proof result, local policy version and decision, and challenge. The resource owns the definitive access/use event. |
| Failure mode | `401` for absent/invalid credentials or proof and `403` for an authenticated but unauthorized operation, with bounded standards-based challenge data. Discovery, introspection, mapping, policy, or audit-enforcement failure denies; it never exposes a reduced unauthenticated surface. |

### 3. Resource application

Here Flowstate is an application integrating with an independent resource
authorization server (for example, a SaaS application's own grant service), not
the protected API and not the issuer of the user's identity.

| Property | Contract |
| --- | --- |
| Trust anchors | The resource application's configured resource authorization server, its issuer/keys and TLS roots, the resource identifier, client registration, webhook/event verification keys, and tenant mapping. |
| Discovery metadata | Consume that server's RFC 8414/OIDC metadata and the resource's RFC 9728 metadata. Proprietary grant APIs live behind a named adapter with an explicit conformance contract; they do not change core XAA semantics. |
| Client identity | Flowstate uses the resource application's registered client identity. Installation/tenant instances are distinct from the global application registration and cannot overwrite it. |
| Tenant resolution | Installation callback/state binds external organization/tenant, Flowstate namespace, initiating principal, and transaction. Only an administrator-authorized mapping activates; domain text alone is not proof of tenant ownership. |
| User and actor | Preserve the external resource owner and actor chain returned by the grant service, correlated to the initiating Flowstate identities. Do not assert that two providers' users are the same without an explicit mapping. |
| Decision owner | The external resource authorization server owns enterprise grant and consent. The external resource enforces access. Flowstate decides whether its workload may request/use the resulting grant and translates only registered PARC operations. |
| Consent and step-up | Pause and resume through the external transaction. State/PKCE/issuer/redirect binding is mandatory. A partial grant is success only for the returned PARC subset; a missing operation remains denied. |
| Audience/resource | Request the external resource URI, never Flowstate's own audience. Tokens remain isolated by installation, tenant, participant chain, proof key, and resource. |
| Proof binding | Meet the external server's advertised DPoP/mTLS requirement and retain the binding through refresh or exchange. An adapter cannot strip proof requirements. |
| Audit | Flowstate records installation lifecycle, mapping changes, requested and returned PARC operations, grant identifier, uses and denials. The external AS/resource remains responsible for its issuance and access logs. |
| Failure mode | An invalid callback, removed application, revoked installation, mapping disagreement, unsupported partial grant, or unreachable decision service disables that integration. Other applications may continue; no cached broad token substitutes. |

### 4. Enterprise identity assertion issuer

Flowstate may issue an assertion only for an identity Flowstate legitimately
owns and can attest: its application deployment or a Flowstate workload/run/step.
It does not issue assertions saying it authenticated an enterprise user, an
external agent, or an external application merely because one invoked a run.

| Property | Contract |
| --- | --- |
| Trust anchors | Relying parties explicitly trust the Flowstate issuer URI and published signing keys. Flowstate trusts its run/step provenance, deployment configuration, and key custody; caller-supplied identity fields are not anchors. |
| Discovery metadata | Publish issuer metadata and JWKS at stable HTTPS locations, with overlap for rotation. Metadata describes assertion capability only and must not imply an OAuth authorization server. |
| Client identity | The assertion names the Flowstate application/workload identity it owns; any OAuth client using the assertion remains a distinct participant. Caller-selected `client_id` is never copied into issuer-owned identity. |
| Tenant resolution | Namespace/deployment comes from server-attested execution context and validated operator mapping. Local execution remains visibly local and cannot spell a production identity. |
| User and actor | External user/agent/application identities may be attached only as separately verified delegation-chain evidence; Flowstate never places them in an issuer-owned `sub`. Absence of verified evidence means absence, not service-user substitution. |
| Decision owner | Flowstate policy decides whether it may attest this owned identity and to which audience. The receiving AS/resource independently decides whether and what that assertion authorizes. |
| Consent and step-up | This issuer performs neither user consent nor user step-up. If the relying relationship requires either, the authorization server obtains it and binds it to its own grant. Flowstate refuses requests that ask the assertion to imply it. |
| Audience/resource | One exact configured relying party/audience per assertion, short lifetime, unique ID. It is not accepted back at Flowstate as an access token and is not a multi-resource credential. |
| Proof binding | Sign with the configured asymmetric issuer key and, where the consuming profile supports it, bind the exchange to the OAuth client/proof key. Signing-key possession does not erase downstream DPoP/mTLS. |
| Audit | Record issuance policy, owned subject, audience, lifetime, key ID and workload provenance without the assertion. The relying party owns exchange/use decisions. |
| Failure mode | Unknown provenance, caller-authored subject, disallowed audience, unavailable key, excessive lifetime, or request to assert an external identity refuses issuance. There is no unsigned, bearer, or generic-subject fallback. |

### 5. Authorization server

An authorization server is **not** an extension of Flowstate's existing identity
broker. The broker issues narrowly addressed workload assertions and exchanges
them with an external authority; that does not make it qualified to authenticate
users, register clients, collect enterprise consent, issue refresh tokens, or
operate revocation and grant-management endpoints.

Flowstate can take this role only as a separately packaged, separately
threat-modeled, opt-in component with its own protocol endpoints, storage,
keys, client-registration lifecycle, administrator model, consent UX, incident
response, audit retention, and kill switch. Until that component exists and
passes the independent interoperability gate, production deployments have no
Flowstate authorization-server role.

| Property | Contract |
| --- | --- |
| Trust anchors | Separately configured upstream identity providers, client registrations and keys, enterprise administrators, resource registrations, signing/encipherment keys, redirect URIs, and tenant directory. Existing broker trust is not imported implicitly. |
| Discovery metadata | Publish complete RFC 8414/OIDC metadata for only implemented endpoints, plus PAR, proof, authorization-details and revocation capabilities. Issuer and endpoint origins are stable and unambiguous. |
| Client identity | Authenticate registered clients according to their metadata; enforce redirect URI and key lifecycle exactly. Flowstate workloads and resource applications do not become registered clients by existing in the same database. |
| Tenant resolution | Resolve tenant before login/consent through a verified registration or administrator-bound mapping, carry it transactionally, and prevent issuer mix-up. No email-domain or host-header guessing. |
| User and actor | Authenticate users through configured upstream assurance, preserve application/workload/agent actors, and emit standards-shaped subject/actor claims without impersonating identities Flowstate did not authenticate. |
| Decision owner | This component owns grant issuance, consent records, assurance satisfaction and token minting. Each resource still owns final request-time authorization. Enterprise policy administration is distinct from resource enforcement. |
| Consent and step-up | Bind consent to client, tenant, subject, participant chain, exact PARC operations, resource, proof key and policy version. Step-up satisfies explicit assurance requirements; neither is inferred from a prior broad scope. |
| Audience/resource | Mint one resource-targeted token (or deliberately requested set permitted by profile), never a Flowstate-wide mega-token. Partial grants enumerate only approved PARC operations. |
| Proof binding | Bind tokens to the validated DPoP key or mTLS client certificate and preserve binding during refresh/exchange. Detect replay and rotate server keys with overlap and emergency retirement. |
| Audit | Own tamper-evident authentication, consent, grant, issuance, refresh, exchange, revocation, administration and key-lifecycle events, correlated without storing usable credentials. |
| Failure mode | Invalid client, redirect, tenant, user, actor chain, authorization detail, assurance, proof, policy, or storage/audit dependency fails the transaction using standard errors. It never delegates the request to the broker or emits a less constrained token. |

## PARC: stable authorization details, not downstream scope strings

XAA requests use Flowstate's **P**olicy **A**ction/**R**esource **C**atalog
(PARC). A PARC operation is the pair `(action, resource)` plus a vocabulary
version. Actions are stable verbs and resources are typed canonical identifiers;
neither is a provider's arbitrary scope string. They travel in an RFC 9396
authorization detail of type `flowstate_parc`:

```json
{
  "type": "flowstate_parc",
  "vocabulary": "flowstate-parc-v1",
  "actions": ["run.submit", "run.observe"],
  "resources": ["flowstate://acme/prod/workflows/reconcile"]
}
```

The initial catalog is deliberately small:

| Surface | PARC actions | Canonical resource |
| --- | --- | --- |
| Connect/API run submission and observation | `run.submit`, `run.observe` | `flowstate://{tenant}/{deployment}/workflows/{workflow}` or a specific `/runs/{run}` |
| Run mutation | `run.signal`, `run.cancel`, `run.terminate` | A specific canonical run URI |
| Schedules and triggers | `schedule.read`, `schedule.manage`, `trigger.invoke` | Canonical deployment-scoped schedule/trigger URI |
| MCP | The action for the underlying operation, not `mcp.call` | The same canonical object URI used by Connect/API |
| Plugin capability | `plugin.invoke` plus the registered task capability name | `flowstate://{tenant}/{deployment}/plugins/{plugin}/tasks/{task}` |

Aliases, display names, raw URLs supplied by a caller, CEL snippets, plugin
arguments, and downstream OAuth scopes are not PARC resources. The resource
server canonicalizes an object from trusted routing and tenant context, then
evaluates the pair. A provider adapter may map a registered PARC operation to a
fixed provider authorization detail or scope set. The mapping is reviewed data,
versioned with the adapter, injective where the provider permits it, and only
narrows: an unknown action/resource, unknown returned scope, or provider scope
that would grant more than the requested PARC set fails closed.

Partial grants are sets of PARC pairs, never strings to reinterpret. Execution
may proceed for an independently authorized pair and must deny every omitted
pair. Adding a new action to the catalog grants nothing until enterprise grant,
resource policy, adapter mapping, and interoperability fixtures all name it.

## Interoperability harness and release gate

The harness is role-split: a client-under-test suite drives a reference resource
and authorization service; a resource-under-test suite drives it with reference
clients and credentials. A passing client result says nothing about the resource
role, and vice versa. The reference peers are replaceable protocol adapters, not
in-process mocks of Flowstate internals.

Every candidate profile is tested in this matrix:

| Peer lane | Client-under-test | Resource-under-test |
| --- | --- | --- |
| Flowstate-to-Flowstate | Flowstate client orchestration | Flowstate Connect, MCP, API, and plugin resource adapters |
| Independent implementation A | Flowstate against an external conforming AS/resource | That implementation's client against Flowstate |
| Independent implementation B | Flowstate against a second, separately maintained AS/resource | That implementation's client against Flowstate |

"Independent" means a separately maintained implementation with no shared XAA
protocol library or fixture parser. Product names and pinned versions/digests
belong in the harness manifest; two configurations of one product do not count.
Recorded HTTP is useful for regression but does not count as an independent run.

Each lane executes the following scenarios for each supported role, grant type,
token form, and proof method:

1. **Baseline and cross-domain tenant mapping:** positive mapping, missing,
   ambiguous, mismatched issuer/resource domains, and a request attempting to
   select another tenant.
2. **Step-up:** insufficient assurance challenge, bound resumption and success;
   cancelled, timed-out, wrong-user and wrong-transaction resumption all deny.
3. **Consent:** exact PARC grant, denial, stale transaction and incremental
   request. Consent to one application/resource never transfers to another.
4. **Revocation:** grant, refresh token, application installation and active
   token revocation where supported; cache invalidation is measured and bounded.
5. **Proof binding:** correct DPoP/mTLS use plus wrong key/certificate, wrong
   method/URI, replay, stale proof, nonce retry, and attempted bearer downgrade.
6. **Policy change:** remove a PARC pair and tenant mapping while a token and a
   durable operation exist. The next resource decision denies; resumption does
   not retain a stale authorization snapshot.
7. **Partial grants:** return a strict subset of requested PARC pairs, run the
   subset, and prove omitted operations and surplus provider scopes are denied.
8. **Application removal:** unregister/disable the OAuth client or installation,
   then exercise access, refresh, token exchange and an in-flight resumption.
   Every path denies without substituting Flowstate's own client.
9. **Participant separation:** vary application, user, workload and agent one at
   a time; assert the audit chain and decision input change, and truncated,
   reordered, synthetic-user and client-as-actor forms are refused.
10. **Failure dependencies:** unavailable/malformed discovery, JWKS,
    introspection, tenant mapper, policy and required audit sink; oversized
    metadata/challenges and redirect/issuer mix-up. Each is bounded and closed.

The harness captures sanitized wire transcripts, metadata snapshots, clock and
peer versions, decisions, and audit assertions. It validates protocol messages
against the pinned standards independently of the implementation under test,
checks that no credential or proof private key appears in logs/history, and
emits a signed result manifest. Secrets and live tokens are redacted at capture,
not in a later publishing step.

The profile remains experimental until **both role suites pass Flowstate-to-
Flowstate and two independent external implementations**, all mandatory negative
cases have been observed (not skipped), and an operator can see the exact profile
and last passing manifest at startup and in diagnostics. A digest change to a
peer, profile component, PARC vocabulary, adapter, or mandatory scenario expires
the result.

## Kill switch and no-downgrade rule

There are two controls and both must allow a request:

- a build/deployment feature gate selects the exact pinned XAA profile; and
- an operator runtime kill switch is visible in startup logs, health/diagnostic
  output, and audit events, and can be changed without minting replacement
  credentials.

Disabled means discovery metadata does not advertise XAA, client orchestration
does not start or resume XAA transactions, assertion issuance for XAA refuses,
and resource endpoints reject XAA credentials/authorization details. Existing
credentials do not bypass the resource gate. An operation parked for consent or
step-up wakes to a denial, not to the policy state captured when it parked.

Neither switch enables the authorization-server role; that component has its
own explicit deployment and switch. No error path may retry with ordinary OAuth
scopes, a bearer token, a different audience, an ambient plugin/server
credential, a flattened subject, or the existing broker. Refusal without
downgrade is part of every interoperability lane and is the final requirement
for removing the experimental label.
