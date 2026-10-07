// Package deviceflow signs a person in from a terminal with the OAuth 2.0
// Device Authorization Grant, and keeps the result.
//
// # The hole this closes
//
// The server verifies bearer tokens from an OIDC trust policy, and
// [github.com/picatz/flowstate/pkg/flowstate/v1/credentialsource] acquires them
// for machines. Nothing acquired one for a person at a keyboard, so every
// interactive user pasted a token from an IdP console into a file. This package
// is the missing half: the user opens a URL in any browser, approves, and the
// CLI polls until the IdP hands over a token.
//
// # The flow
//
// [Client.Discover] reads device_authorization_endpoint, token_endpoint and,
// when advertised, revocation_endpoint from the issuer's
// /.well-known/openid-configuration ([OpenID Connect Discovery 1.0 §4]).
// [Client.Authorize] requests a device code ([RFC 8628 §3.1-3.2]); the caller
// shows the user the verification URI and user code ([RFC 8628 §3.3]);
// [Client.Poll] exchanges the device code for tokens ([RFC 8628 §3.4]) and
// answers authorization_pending, slow_down (add five seconds to the interval,
// [RFC 8628 §3.5]), access_denied and expired_token as the RFC says, honouring
// interval and expires_in. [Client.Refresh] renews an access token with the
// refresh token ([RFC 6749 §6]); [Client.Revoke] asks the IdP to invalidate a
// token ([RFC 7009 §2]). [Client.Login] runs discovery through polling
// together.
//
// # Bounds and trust
//
// Every URL this package dials must be https, or http only on a loopback host.
// Discovery's issuer must equal the issuer asked about ([OpenID Connect
// Discovery 1.0 §4.3]), which stops a document served for one issuer from
// naming another's endpoints, and every endpoint it names is held to the same
// scheme rule. Redirects are followed only within the origin of the request
// that was made, so a token request is never replayed somewhere else. Each
// response is read through a [MaxResponseBytes] limit and each request has a
// timeout; polling is bounded by the code's expires_in (itself capped) and
// never sleeps longer than [MaxPollInterval].
//
// Text an IdP supplies (error descriptions, the verification URI, the user
// code) is untrusted and is stripped of control characters before it reaches
// an error or a terminal.
//
// # Storage
//
// A [Store] keeps one file per issuer and client_id under
// os.UserConfigDir()/flowstate/login: mode 0600 in a 0700 directory, written
// atomically, and refused on read when its permissions are looser. Tokens are
// held in [Tokens], which renders as redacted under every fmt verb, in slog and
// in JSON; only the Store serializes them.
//
// [OpenID Connect Discovery 1.0 §4]: https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderConfig
// [OpenID Connect Discovery 1.0 §4.3]: https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderConfigurationValidation
// [RFC 8628 §3.1-3.2]: https://www.rfc-editor.org/rfc/rfc8628#section-3.1
// [RFC 8628 §3.3]: https://www.rfc-editor.org/rfc/rfc8628#section-3.3
// [RFC 8628 §3.4]: https://www.rfc-editor.org/rfc/rfc8628#section-3.4
// [RFC 8628 §3.5]: https://www.rfc-editor.org/rfc/rfc8628#section-3.5
// [RFC 6749 §6]: https://www.rfc-editor.org/rfc/rfc6749#section-6
// [RFC 7009 §2]: https://www.rfc-editor.org/rfc/rfc7009#section-2
package deviceflow
