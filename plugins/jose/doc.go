// Command flowstate-plugin-jose verifies JWTs against trust an operator
// configured.
//
// # Why this is not a generic JWT plugin
//
// #1344's audit refused one, and the reason was precise: a generic verifier
// "must choose trust roots, algorithms, claim policy, time/replay semantics,
// and key refresh", and a workflow that chose any of those would be choosing
// what counts as valid. Generic *signing* was refused too, because it puts
// private-key use and bearer outputs into a workflow's history.
//
// Both refusals hold here, and this plugin is what is left when they do: the
// trust roots are an operator's file, the algorithms and the claim policy are
// that file's, the clock and key refresh are the engine's own, and nothing here
// signs anything. A workflow names a token and, at most, which trusted entry it
// must have come from.
//
// # One verifier, not a second one
//
// The engine already verifies tokens - it is how every authenticated call
// reaches the server - with bounded discovery and key-set fetches, an RSA
// modulus floor, a clock-skew allowance, algorithm pinning per issuer and a key
// cache with a refresh floor. Writing a second implementation inside a plugin
// would be a second set of answers to keep correct, which is the thing
// AGENTS.md's second invariant forbids.
//
// So this plugin is [github.com/picatz/flowstate/pkg/flowstate/v1/auth]'s
// verifier with a task in front of it. The operator's file is the same
// [auth.Policy] shape a deployment already writes for `flow server
// --auth-policy`, which means a deployment that trusts an issuer for its own
// API trusts it here by pointing at the same document - and a reviewer reads
// one spelling rather than two.
//
// # What a verified token is, and is not
//
// It is a statement by an issuer the operator trusts, about a subject, valid
// for a window. It is not authorization: what a subject may do is a decision
// the workflow makes from the claims this task returns, with CEL, the way every
// other policy decision in this system is made.
//
// Replay is the same kind of question and has the same answer. A token verified
// twice is verified twice; this task says nothing about whether it was seen
// before, because "before" is a scope only the workflow knows - a run, a day, a
// tenant. A workflow that needs one-time use records the "jti" claim this task
// returns and refuses a repeat itself.
//
// # Where it is useful
//
// A workflow that receives a token from somewhere: a webhook whose body carries
// an identity token, a callback from a build system, a partner's request
// deposited by the http task. Verifying it turns bytes a workflow received into
// claims a workflow may act on, which is exactly the boundary the engine's own
// fail-closed rule applies to.
package main
