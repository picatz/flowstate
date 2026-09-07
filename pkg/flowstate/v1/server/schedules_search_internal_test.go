package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestScheduleListQueryAsksOnlyWhereItMay pins the three shapes of
// [FlowstateServer.scheduleListQuery]: a tenant's attribute plus the untagged
// entries where registration is confirmed, and no query at all — the walk —
// where it is not or where the namespace could not be spliced into a literal.
func TestScheduleListQueryAsksOnlyWhereItMay(t *testing.T) {
	t.Parallel()

	registered := &FlowstateServer{searchAttributesRegistered: true}
	assert.Equal(t, "FlowstateNamespace = 'team-a' OR FlowstateNamespace IS NULL", registered.scheduleListQuery("team-a"))
	assert.Equal(t, "FlowstateNamespace = '' OR FlowstateNamespace IS NULL", registered.scheduleListQuery(""),
		"the single-tenant default is a namespace too")

	assert.Empty(t, (&FlowstateServer{}).scheduleListQuery("team-a"),
		"a deployment that never confirmed registration must not name an attribute in a query")
	assert.Empty(t, registered.scheduleListQuery("team' OR 1=1 --"),
		"a namespace outside the grammar is not spliced into a query literal")
}
