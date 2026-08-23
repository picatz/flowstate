// Package oauthclient is Flowstate's shared outbound OAuth client.
//
// The package deliberately defines its request and credential types in Go rather
// than protobuf. They are a boundary that credentials must not cross: tokens,
// refresh tokens, private keys and proof material must never be serialised into a
// Flowstate specification or Temporal history. Callers receive a brokered
// [http.RoundTripper], not token bytes.
//
// Protocol-specific acquisition is supplied by an [Agent]. This keeps private
// material in an authorization agent, HSM/KMS, the SPIFFE Workload API, or a
// projected-token reader. The shared client owns policy validation, cache
// isolation, refresh single-flight, diagnostics, and applying proof-bound tokens.
package oauthclient
