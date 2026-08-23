package interop

import "os"

// CoreCases is the provider-neutral suite. Each row names the exact section or
// immutable draft revision it probes; additions should be narrow rather than
// broad protocol labels.
var CoreCases = []Case{
	{ID: "oidc-discovery-issuer", Protocol: "OpenID Connect Discovery", Reference: "OpenID Connect Discovery 1.0 §4.3", Subset: "issuer equality and HTTPS jwks_uri", Deviation: BrokenMetadata},
	{ID: "jwk-kid-selection", Protocol: "JWK/JWS", Reference: "RFC 7517 §4.5; RFC 7515 §4.1.4", Subset: "kid omission and collision fail closed", Deviation: DuplicateKeyID},
	{ID: "jwt-issuer-audience-time", Protocol: "JWT", Reference: "RFC 7519 §§4.1.1, 4.1.3, 4.1.4–4.1.6", Subset: "iss, aud, exp, nbf, iat validation", Deviation: ClockSkew},
	{ID: "oauth-token-exchange", Protocol: "OAuth token exchange", Reference: "RFC 8693 §§2.1, 2.2.1, 4.1", Subset: "JWT subject/actor tokens and bearer access-token response", Deviation: ActorSubjectReversal},
	{ID: "oauth-resource-indicators", Protocol: "OAuth resource indicators", Reference: "RFC 8707 §2", Subset: "single resource target and audience distinction", Deviation: PartialGrant},
	{ID: "dpop-nonce-replay", Protocol: "DPoP", Reference: "RFC 9449 §§8, 9, 11", Subset: "resource nonce retry and jti replay rejection", Deviation: DPoPNonce},
	{ID: "oauth-mtls-rotation", Protocol: "OAuth mTLS", Reference: "RFC 8705 §§2, 3", Subset: "certificate-bound client authentication with SVID reload", Extension: "SPIFFE Workload API X.509-SVID", Deviation: MTLSRotation},
	{ID: "security-event-revocation", Protocol: "Security Event Token", Reference: "RFC 8417 §§2, 2.2", Subset: "SET transmission and session revocation", Deviation: RevocationDuringSession},
	{ID: "xaa-draft", Protocol: "XAA", Reference: "draft-ietf-oauth-transaction-tokens-04", Subset: "explicit draft negotiation only", Extension: "XAA/ID-JAG experimental profile", Deviation: XAAVersionMismatch},
}

// ExternalEnabled makes contacting an external provider an explicit per-provider
// action. The environment variable value must be exactly "1".
func ExternalEnabled(provider string) bool { return os.Getenv("FLOWSTATE_INTEROP_"+provider) == "1" }
