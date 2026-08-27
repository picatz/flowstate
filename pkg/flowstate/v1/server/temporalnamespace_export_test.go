package server

// TemporalNamespaceForTest exposes [FlowstateServer.temporalNamespaceFor] so a
// test in the external test package can ask it the question, without the answer
// becoming something a caller of this package can ask for.
//
// The accessor stays unexported because naming a Temporal namespace is this
// package's private arrangement with Temporal, and the reason is the same one
// [FlowstateServer.clientFor] is unexported: a caller that could name a namespace
// could name a *tenant's* namespace. What a test needs is narrower than an API —
// the ability to assert that a tenant this deployment cannot place is refused
// rather than handed somebody else's namespace — and the external test package is
// where the pool, the dev server and the namespace registration this needs already
// live (main_test.go, pool_test.go).
//
// This is [ScheduleIDForTest]'s shape and exists for its reason.
func (s *FlowstateServer) TemporalNamespaceForTest(namespace string) (string, error) {
	return s.temporalNamespaceFor(namespace)
}
