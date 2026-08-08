// Package authtest serves an OpenID Connect provider from inside a test, so
// that a trust policy can be proved without an identity provider, a network, or
// a credential.
//
// An [auth.Policy] is the part of a deployment that decides who may run code.
// It is also configuration, which means it is usually written once, deployed,
// and never exercised until the day it refuses somebody it should have admitted
// or admits somebody it should have refused. This package exists so that a
// policy can be run against a controlled issuer in an ordinary Go test:
//
//	issuer := authtest.NewIssuer()
//	defer issuer.Close()
//
//	verifier, err := auth.NewOIDCVerifier(auth.Policy{
//		Issuers: []auth.TrustedIssuer{{
//			Name:      "ci",
//			Issuer:    issuer.URL(),
//			Audiences: []string{"flowstate"},
//			Require:   []auth.ClaimRule{auth.RequireClaim("team", "platform")},
//			Namespace: "acme",
//		}},
//	})
//
//	token := issuer.MintToken(
//		map[string]any{"team": "platform"},
//		authtest.WithSubject("runner"),
//		authtest.WithAudience("flowstate"),
//	)
//
//	principal, err := verifier.Verify(ctx, token)
//
// What that proves is the whole configuration at once: the discovery document
// is fetched, the key set is fetched, the signature is checked against a
// published key, and the claim rules and namespace are applied to the claims
// the token actually carries. A policy that admits the wrong caller fails here,
// on a laptop, rather than in production.
//
// # Nothing reaches the network
//
// An [Issuer] listens on a loopback port in this process. Keys are generated in
// this process. Nothing is fetched, nothing is published, and no token minted
// here is valid anywhere else, because the issuer identifier is the loopback
// URL the listener was given.
//
// # Vendor neutral on purpose
//
// Nothing here knows the claim names any particular identity provider mints.
// Claims are a map the caller fills in, so a test describes the tokens its own
// provider issues, and this package does not have to be changed when a provider
// adds a claim or a deployment moves between providers. Where a test wants the
// claim set of a real platform, that claim set belongs in the test, next to the
// assertions that depend on it.
//
// # Failing closed
//
// A test double that quietly does the safe thing hides the hole it was written
// to find. The classic one is a token minted with no audience: it verifies
// against a policy that never checked, and the test reports a working
// configuration. So [Issuer.MintToken] refuses to mint a token without an
// audience unless the caller writes [WithoutAudience], which is a sentence
// somebody has to have meant.
//
// Misuse panics rather than returning an error. Every failure this package can
// have is a mistake in the test itself (an algorithm no key supports, an
// audience nobody named, a port that cannot be bound), and a panic stops the
// test at the line that caused it.
//
// # Misbehaving issuers
//
// An issuer that answers is only half of what a deployment meets. The other
// half is an issuer that is down, that serves a key set it cannot parse, that
// redirects its keys somewhere unprotected, or that claims to be someone else.
// [Issuer.SetKeySetResponse], [Issuer.SetDiscoveryHandler],
// [Issuer.SetDiscoveredIssuer] and [Issuer.RedirectKeySet] produce those, so a
// deployment can prove it refuses rather than degrades. [Issuer.Requests]
// reports how often each endpoint was reached, which is how a test shows that
// keys are cached and that a stream of unrecognized key ids does not turn the
// deployment into a load generator.
//
// # Beyond verification
//
// A verifier is the first thing a controlled issuer is good for, not the last.
// Trading a token for a credential, receiving a signed webhook, and carrying a
// transaction token through a chain of services all start with a token some
// issuer minted, and all of them are testable against this one. The API is
// deliberately about issuers, keys and tokens rather than about verification,
// so that those can be built on it without a second double.
//
// # Relation to the rest of the repo
//
// [flowtest] is the equivalent for workflow authors: it runs a Flowfile against
// a virtual clock with stubbed tasks. This package is the equivalent for
// operators and plugin authors, whose configuration is a trust policy rather
// than a workflow. The [Clock] here is not [flowtest]'s virtual clock, which
// exists to decide when a workflow's waiting ends; this one only answers what
// time a token was minted at and what time a verifier thinks it is.
//
// [auth.Issuer] is a different thing with the same word in its name: it is
// Flowstate acting as an issuer of its own assertions. [Issuer] here stands in
// for somebody else's identity provider.
//
// [auth.Policy]: https://pkg.go.dev/github.com/picatz/flowstate/pkg/flowstate/v1/auth#Policy
// [auth.Issuer]: https://pkg.go.dev/github.com/picatz/flowstate/pkg/flowstate/v1/auth#Issuer
// [flowtest]: https://pkg.go.dev/github.com/picatz/flowstate/pkg/flowstate/v1/flowtest
package authtest
