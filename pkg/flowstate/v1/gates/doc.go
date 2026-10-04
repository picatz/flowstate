// Package gates serves the browser surface for the humans a durable gate waits
// on: one server-rendered page per pending `wait_for_signal:` gate, with the
// question the gate asks and the two answers a person can give it.
//
// The page is a client of the Connect API and nothing else. It reads a run with
// Get and answers a gate with Signal, through the same handler, interceptors and
// authenticator every other caller goes through, so the tenancy check, the
// `signals:` policy decision, the sender attestation and the audit record are the
// ones `flow signal` produces. There is no second authorization mechanism to
// keep in step with the first: a person the policy refuses is refused by the
// server's own decision and the page only reports it.
//
// # Authentication
//
// The page holds no credential of its own. It forwards the request's
// Authorization header to the API it fronts, so an approver reaches it the way
// any caller reaches the API: through an identity-aware proxy that injects the
// token, or with a client that sets the header. A request without one is
// answered 401 by the API's own verdict. Credentials is the seam a browser login
// plugs into; this package neither stores sessions nor speaks to an issuer.
//
// # Answering
//
// A gate declares a name, a prompt and a timeout. It declares no input schema,
// so the page speaks the one payload shape the language's own examples read:
// `approved` (a boolean) and an optional `comment` (text). The buttons are
// Approve and Deny; both deliver the signal, and what the workflow does with a
// denial is the workflow's decision.
//
// # What the page defends against
//
// A browser attaches whatever ambient credential a proxy keeps, so a page on
// another origin could submit the form on an approver's behalf. An answer is
// therefore accepted only when the browser says the request is same-origin
// (Fetch Metadata, with an Origin comparison as the fallback for older
// browsers) and refused when it says nothing, so a client that cannot prove it
// came from this page uses the API. The check keeps no state, which is what lets
// a gate page rendered by one replica be answered at another.
//
// Every response is uncacheable, carries a Content-Security-Policy that
// forbids scripts, framing and third-party loads, and is built from
// html/template, so a prompt or comment a workflow computed from untrusted input
// is text on the page and never markup.
package gates
