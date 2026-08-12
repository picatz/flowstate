// Package server is the control plane: [FlowstateServer] implements the
// flowstate.v1 WorkflowService over Connect RPC, turning each request to
// submit, observe, signal, cancel, or schedule a run into Temporal operations
// on the workloads the engine executes.
//
// What each RPC is and carries, the schema already says; the generated
// descriptors and their comments are the reference. This comment covers what
// the schema cannot: the posture the handlers share.
//
// # Fail closed
//
// Everything this package decides denies by default and denies on error.
// Authentication itself is not one of those decisions: it happens in an
// interceptor in front of the handler (see the auth package), and this
// package reads the result with auth.PrincipalFromContext and derives the
// caller's identity in FlowstateServer.identityFor. Refusing an
// unauthenticated request is therefore the interceptor's decision, not this
// package's: a deployment that opts out of authentication by name (flow
// server --insecure-no-auth, or an embedder who mounts the handler with no
// interceptor) is accepted, and identityFor gives its callers an identity
// with no subject in the configured namespace. Validation follows the deny
// rule without that caveat: the handler validates its own inputs
// (protovalidate, then v1.CheckSpecSize at submit) rather than trusting
// whoever wired it up to have installed an interceptor.
//
// # Tenancy
//
// A run's tenant comes from the authenticated caller, never from the request
// or from the workload itself, because a request that names its own tenant
// can name someone else's. In a deployment that opted out of authentication,
// every caller therefore shares the one configured namespace; the boundary is
// only as real as the identities behind it. The tenant is recorded on the run
// when it starts and every later request about that run is authorized against
// it; a run belonging to another tenant answers as "no such run" rather than
// as a permission error, so existence does not leak across the boundary. When
// testing this boundary, write the negative direction: that A cannot reach
// B's run, not that A can reach A's.
//
// # Bounds
//
// Every input an outside party chooses is bounded, and the bound has to
// match the resource that party controls; see CLAUDE.md, "Bound anything
// that consumes untrusted input", for the discipline. The instructive case
// lives in list.go: a listing is bounded by executions read and by round
// trips made, separately, because Temporal decides how many executions come
// back per request, so a loop bounded only by what it reads never terminates
// against a peer that answers with empty pages. When adding a handler, ask
// which resource the caller (or Temporal) controls, bound that resource, and
// test that the bound is reached as well as not exceeded.
package server
