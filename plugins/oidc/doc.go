// Command flowstate-plugin-oidc mints short-lived access tokens for the
// systems an operator configured, and hands them to a workflow as secrets.
//
// # Why this is a secret provider and not a task
//
// A task returns outputs, and outputs are durable history. A task that returned
// an access token would write a bearer credential into every run's record,
// where it outlives its usefulness and reaches everyone who can read a run.
// That is the reason #1344's audit refused generic signing, and it applies to
// minting just as squarely.
//
// A secret provider is the other shape. `${secret('oidc:billing-api')}` is a
// *reference* in the Flowfile and in history; the host resolves it worker-side,
// at the moment the step runs, under the caller's own namespace, and hands the
// value only to the task that needs it. The credential exists in one process
// for one call. That is what "resolved at the point of use" means, and it is
// the whole reason this capability lives here.
//
// # What it does
//
// The OAuth 2.0 client credentials grant (RFC 6749 section 4.4), against token
// endpoints an operator names, with a client secret an operator put in a file.
// The exchange itself is
// [github.com/picatz/flowstate/pkg/flowstate/v1/auth]'s - the same exchanger
// the engine's own outbound federation uses - so there is one implementation of
// "ask an authorization server for a token" in this tree, bounded and tested
// where it already was.
//
// Every token comes back with the lifetime the authorization server reported,
// and that lifetime travels to the host as [sdk.SecretResponse.ExpiresIn], so
// the engine caches it no longer than the issuer considers it valid. Nothing
// here caches a credential of its own: a second cache would be a second answer
// about when a token stops being usable.
//
// # What it deliberately does not do
//
//   - No discovery. A provider names its token endpoint, because discovery is a
//     second fetch whose only purpose is to find a URL an operator already
//     knows, and an operator who can write one can write the other.
//   - No authorization code, device code, or refresh tokens. Those are flows
//     with a human at a browser, and a durable workload is not one.
//   - No token exchange (RFC 8693) yet. The engine already implements it for
//     workload identity federation, where the assertion is the *worker's* own
//     identity - reaching that from a plugin would mean handing a plugin the
//     issuer's signing key, which is server-side authority this plugin does not
//     want. Making federation reachable from a Flowfile is a real and separate
//     piece of work.
//   - No client secret in the configuration document. The provider names a file;
//     the value is read from it. A configuration file an operator diffs in
//     review should not be one they have to redact.
package main
