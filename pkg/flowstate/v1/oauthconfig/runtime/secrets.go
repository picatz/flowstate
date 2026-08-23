// Package runtime contains OAuth values that are forbidden from protobufs and
// workflow history. Values are constructed only after resolving a
// flowstate.v1.SecretReference at the protocol boundary that consumes them.
package runtime

type PrivateKey []byte
type ClientSecret []byte
type AuthorizationCode []byte
type RefreshToken []byte
type DPoPKey []byte
type AccessToken []byte
type RawAssertion []byte

// Redacted prevents accidental diagnostic disclosure.
func (PrivateKey) String() string        { return "[REDACTED]" }
func (ClientSecret) String() string      { return "[REDACTED]" }
func (AuthorizationCode) String() string { return "[REDACTED]" }
func (RefreshToken) String() string      { return "[REDACTED]" }
func (DPoPKey) String() string           { return "[REDACTED]" }
func (AccessToken) String() string       { return "[REDACTED]" }
func (RawAssertion) String() string      { return "[REDACTED]" }
