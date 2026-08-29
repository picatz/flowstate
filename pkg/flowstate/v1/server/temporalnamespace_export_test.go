package server

import "go.temporal.io/sdk/client"

// ClientAndTemporalNamespaceForTest exposes
// [FlowstateServer.clientAndTemporalNamespaceFor] so a test in the external test
// package can ask it the question, without the answer becoming something a caller
// of this package can ask for.
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
// It hands back both halves rather than only the namespace, because the pairing is
// the property under test: a shim that dropped the client would make the tests
// unable to see the defect the paired accessor exists to prevent.
//
// This is [ScheduleIDForTest]'s shape and exists for its reason.
func (s *FlowstateServer) ClientAndTemporalNamespaceForTest(namespace string) (client.Client, string, error) {
	return s.clientAndTemporalNamespaceFor(namespace)
}

// ClientForTest exposes [FlowstateServer.clientFor], so a test can check the
// paired accessor's client against the one this server would actually route a run
// through.
//
// That comparison is the point: the pairing property is not "both halves are
// non-nil" but "the namespace named belongs to the client routing would have
// used", and a test that could only see the paired accessor's own two return
// values has no independent answer to check it against.
func (s *FlowstateServer) ClientForTest(namespace string) (client.Client, error) {
	return s.clientFor(namespace)
}
