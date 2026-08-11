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
// Every decision here denies by default and denies on error. Authentication
// happens in an interceptor in front of the handler (see the auth package);
// this package reads the result with auth.PrincipalFromContext and derives
// the caller's identity in FlowstateServer.identityFor. A request the server
// cannot attribute or authorize is refused, and an errored check is a denial,
// never a pass. Validation follows the same rule: the handler validates its
// own inputs (protovalidate, then v1.CheckSpecSize at submit) rather than
// trusting whoever wired it up to have installed an interceptor.
//
// # Tenancy
//
// A run's tenant comes from the authenticated caller, never from the request
// or from the workload itself, because a request that names its own tenant
// can name someone else's. The tenant is recorded on the run when it starts
// and every later request about that run is authorized against it; a run
// belonging to another tenant answers as "no such run" rather than as a
// permission error, so existence does not leak across the boundary. When
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
