package interop

import (
	"net/http"
	"time"
)

// Deviation names protocol faults without coupling a suite to a provider.
type Deviation string

const (
	KeyRotation               Deviation = "key_rotation"
	StaleJWKSCache            Deviation = "stale_jwks_cache"
	DuplicateKeyID            Deviation = "duplicate_key_id"
	MissingKeyID              Deviation = "missing_key_id"
	IssuerMismatch            Deviation = "issuer_mismatch"
	AudienceMismatch          Deviation = "audience_mismatch"
	ClockSkew                 Deviation = "clock_skew"
	TokenTypeMismatch         Deviation = "token_type_mismatch"
	ExpiryMismatch            Deviation = "expiry_mismatch"
	RedirectAttempt           Deviation = "redirect_attempt"
	SSRFAttempt               Deviation = "ssrf_attempt"
	PartialGrant              Deviation = "partial_grant"
	OverbroadGrant            Deviation = "overbroad_grant"
	ActorSubjectReversal      Deviation = "actor_subject_reversal"
	BrokenMetadata            Deviation = "broken_metadata"
	DPoPNonce                 Deviation = "dpop_nonce"
	DPoPReplay                Deviation = "dpop_replay"
	MTLSRotation              Deviation = "mtls_rotation"
	XAAVersionMismatch        Deviation = "xaa_version_mismatch"
	RevocationDuringSession   Deviation = "revocation_during_session"
	PolicyChangeDuringSession Deviation = "policy_change_during_session"
)

// Scenario installs a common wire-level fault. Token claim faults are supplied
// by the adapter because its token format is part of the black-box contract.
func (e *Environment) Scenario(d Deviation) {
	switch d {
	case StaleJWKSCache:
		e.Set(JWKS, Script{Responses: []Response{JSONResponse(200, map[string]any{"keys": []any{}})}, RepeatLast: true})
	case DuplicateKeyID:
		e.Set(JWKS, Script{Responses: []Response{JSONResponse(200, map[string]any{"keys": []any{map[string]any{"kid": "same"}, map[string]any{"kid": "same"}}})}, RepeatLast: true})
	case MissingKeyID:
		e.Set(JWKS, Script{Responses: []Response{JSONResponse(200, map[string]any{"keys": []any{map[string]any{"kty": "EC"}}})}, RepeatLast: true})
	case BrokenMetadata:
		e.Set(Issuer, Script{Responses: []Response{JSONResponse(200, map[string]any{"issuer": 7})}, RepeatLast: true})
	case RedirectAttempt:
		e.Set(TokenExchange, Script{Responses: []Response{{Status: http.StatusFound, Header: http.Header{"Location": {e.URL(ResourceServer)}}}}, RepeatLast: true})
	case SSRFAttempt:
		e.Set(Issuer, Script{Responses: []Response{JSONResponse(200, map[string]any{"issuer": e.URL(Issuer), "jwks_uri": "http://169.254.169.254/latest/meta-data"})}, RepeatLast: true})
	case TokenTypeMismatch:
		e.Set(TokenExchange, Script{Responses: []Response{JSONResponse(200, map[string]any{"access_token": "opaque", "token_type": "MAC"})}, RepeatLast: true})
	case ExpiryMismatch:
		e.Set(TokenExchange, Script{Responses: []Response{JSONResponse(200, map[string]any{"access_token": "opaque", "token_type": "Bearer", "expires_in": -1})}, RepeatLast: true})
	case PartialGrant:
		e.Set(TokenExchange, Script{Responses: []Response{JSONResponse(200, map[string]any{"access_token": "opaque", "token_type": "Bearer", "scope": "read"})}, RepeatLast: true})
	case OverbroadGrant:
		e.Set(TokenExchange, Script{Responses: []Response{JSONResponse(200, map[string]any{"access_token": "opaque", "token_type": "Bearer", "scope": "read write admin"})}, RepeatLast: true})
	case DPoPNonce:
		e.Set(ResourceServer, Script{Responses: []Response{{Status: http.StatusUnauthorized, Header: http.Header{"DPoP-Nonce": {"nonce-1"}}}, {Status: http.StatusOK}}, RepeatLast: true})
	case DPoPReplay:
		e.Set(ResourceServer, Script{Responses: []Response{{Status: http.StatusOK}, JSONResponse(http.StatusUnauthorized, map[string]string{"error": "use_dpop_nonce"})}, RepeatLast: true})
	case XAAVersionMismatch:
		e.Set(XAA, Script{Responses: []Response{JSONResponse(http.StatusBadRequest, map[string]string{"error": "unsupported_draft", "supported": "draft-04"})}, RepeatLast: true})
	case RevocationDuringSession:
		e.Set(SecurityEvents, Script{Responses: []Response{JSONResponse(http.StatusAccepted, map[string]string{"status": "active"}), JSONResponse(http.StatusAccepted, map[string]string{"status": "revoked"})}, RepeatLast: true})
	case PolicyChangeDuringSession:
		e.Set(ResourceServer, Script{Responses: []Response{{Status: http.StatusOK}, JSONResponse(http.StatusForbidden, map[string]string{"error": "policy_changed"})}, RepeatLast: true})
	case ClockSkew:
		e.Advance(10 * time.Minute)
	case MTLSRotation:
		_ = e.RotateCertificate()
	}
}
