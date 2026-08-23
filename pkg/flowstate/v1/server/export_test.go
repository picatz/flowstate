package server

// ScheduleIDForTest exposes scheduleIDFor so a test in the external test package
// can assert against an id *derived from the same inputs the server derived it
// from*, rather than against the encoding hand-spelled a second time.
//
// The encoding stays unexported: it is this package's private arrangement with
// Temporal, and a caller that could spell an id could name a tenant. What a test
// needs is narrower than an API — the ability to say "the workflow id a firing
// produced begins with the id this tenant's schedule name maps to" without
// writing `flowstate-schedule-` down anywhere. A hand-spelled expectation
// breaks when the prefix changes, which is a failure about the test rather
// than about the property, and it cannot catch a change that alters the
// derivation and the literal together.
//
// TestDescribeScheduleFailsClosedWhenSpecIsUnavailable deliberately does *not*
// use this: that test finds its schedule by listing what Temporal holds,
// because the path it exercises is the one an ordinary caller takes.
func ScheduleIDForTest(namespace, name string) string {
	return scheduleIDFor(namespace, name)
}
