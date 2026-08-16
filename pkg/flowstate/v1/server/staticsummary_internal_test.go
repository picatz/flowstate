package server

// runStaticSummary is the one function [FlowstateServer.prepareCreate] and
// [FlowstateServer.CreateSchedule] both build [client.StartWorkflowOptions.StaticSummary]
// /[client.ScheduleWorkflowAction.StaticSummary] from — see its doc comment
// for why (#753). This pins the rendering itself: both inputs appear,
// backtick-delimited, and neither is silently dropped or reordered.

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRunStaticSummaryRendersBothTenantAndWorkflowName(t *testing.T) {
	t.Parallel()

	got := runStaticSummary("team-a", "payments")

	assert.Equal(t, "`payments` · tenant `team-a`", got,
		"the rendering drifted from what prepareCreate/CreateSchedule callers and the issue's suggested format expect")
	assert.Contains(t, got, "payments", "the workflow name must appear")
	assert.Contains(t, got, "team-a", "the tenant must appear")
}

// TestRunStaticSummaryEmptyNamespaceStillRenders pins the default-tenant case:
// an empty namespace ([auth.ValidateNamespace] accepts "" as the default
// tenant) still produces a well-formed, backtick-delimited string rather than
// a malformed one — Temporal accepts an empty string inside a Markdown field
// exactly as it accepts one anywhere else.
func TestRunStaticSummaryEmptyNamespaceStillRenders(t *testing.T) {
	t.Parallel()

	got := runStaticSummary("", "payments")
	assert.Equal(t, "`payments` · tenant ``", got)
}
