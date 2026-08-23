// Package securityevent verifies, normalizes, and applies Security Event
// Tokens (SETs) without making the workload engine depend on an identity
// provider's vocabulary.
//
// Verification and normalization are separate trust boundaries. A Verifier
// establishes signature, issuer, and audience and returns claims only after all
// three succeed. An Adapter recognizes one negotiated CAEP/RISC or Shared
// Signals profile and normalizes its event. Ingestor then independently checks
// the required SET claims, event/subject pairing, time, replay, and bounds.
// Naming a subject that happens to exist is never evidence of authenticity.
//
// Index is consulted at meaningful use boundaries rather than only at token
// issuance. Its Store interface makes cross-node semantics explicit: an
// immediate event is acknowledged only after a linearizable Apply; a backend
// that cannot provide that guarantee must fail closed. Durable run behavior is
// data in SecurityEvent.enforcement: replay-time workflow behavior is never
// silently changed.
package securityevent
