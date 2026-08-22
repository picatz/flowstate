package server

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	common "go.temporal.io/api/common/v1"
	deployment "go.temporal.io/api/deployment/v1"
	"go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/mocks"

	v1types "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// Selecting a set of runs is the thing an operator does before doing anything
// else, and a selector is exactly the shape of thing that passes its tests while
// being wrong: a page of matches proves nothing about the walk, and a filter that
// finds a tenant's own runs proves nothing about whether it can reach anybody
// else's.
//
// So everything here walks to exhaustion and asserts the *set* — every run the
// selector should reach, reached exactly once, and nothing else touched — over a
// population salted with decoys chosen to be wrong in each available direction:
// the same label under another tenant, another label under this one, and runs
// carrying no labels at all.

// labelledRun is an execution carrying a tenant and a workflow's declared labels
// in its memo, exactly as [labelsMemoEntry] and the tenant entry beside it would
// have written them at submit.
//
// owner empty means the caller's own default tenant, matching how the rest of
// these tests build "mine". labels nil writes no labels entry at all, which is
// both a workflow that declared none and a run started before the memo key
// existed — the two cases the summary deliberately does not distinguish.
func labelledRun(t *testing.T, id, owner string, labels map[string]string) *workflow.WorkflowExecutionInfo {
	t.Helper()

	fields := map[string]*common.Payload{}

	if owner != "" {
		payload, err := converter.GetDefaultDataConverter().ToPayload(owner)
		require.NoError(t, err)
		fields[namespaceMemoKey] = payload
	}

	if labels != nil {
		payload, err := converter.GetDefaultDataConverter().ToPayload(labels)
		require.NoError(t, err)
		fields[labelsMemoKey] = payload
	}

	execution := &workflow.WorkflowExecutionInfo{
		Execution: &common.WorkflowExecution{WorkflowId: id},
	}
	if len(fields) > 0 {
		execution.Memo = &common.Memo{Fields: fields}
	}

	return execution
}

// pagingNamespace answers the way Temporal does: the token is an opaque
// position, and a request returns the executions after it.
//
// Shared by the walks below so that each one differs in its population and its
// filter rather than in how the peer pages, which is the half that has already
// been got wrong once.
func pagingNamespace(t *testing.T, all []*workflow.WorkflowExecutionInfo) *mocks.Client {
	t.Helper()

	temporal := &mocks.Client{}
	temporal.On("ListWorkflow", mock.Anything, mock.Anything).Return(
		func(_ context.Context, request *workflowservice.ListWorkflowExecutionsRequest) *workflowservice.ListWorkflowExecutionsResponse {
			offset := 0
			if token := request.GetNextPageToken(); len(token) > 0 {
				parsed, err := strconv.Atoi(string(token))
				require.NoError(t, err)
				offset = parsed
			}

			end := min(offset+int(request.GetPageSize()), len(all))

			resp := &workflowservice.ListWorkflowExecutionsResponse{Executions: all[offset:end]}
			if end < len(all) {
				resp.NextPageToken = []byte(strconv.Itoa(end))
			}

			return resp
		},
		nil,
	)

	return temporal
}

// walk lists every page a selector reaches and reports how many times each run
// came back, so a caller can assert the set rather than a page.
func walk(t *testing.T, server *FlowstateServer, ctx context.Context, filter string, pageSize int32) map[string]int {
	t.Helper()

	seen := map[string]int{}
	token := ""

	for pages := 0; ; pages++ {
		require.Less(t, pages, 200, "the listing never terminated")

		response, err := server.List(ctx, connect.NewRequest(&v1types.ListRequest{
			PageSize:  pageSize,
			PageToken: token,
			Filter:    filter,
		}))
		require.NoError(t, err)

		require.LessOrEqual(t, len(response.Msg.GetRuns()), int(pageSize),
			"a page came back larger than it was asked for")

		for _, run := range response.Msg.GetRuns() {
			seen[run.GetWorkflowId()]++
		}

		token = response.Msg.GetNextPageToken()
		if token == "" {
			break
		}
	}

	return seen
}

// TestALabelSelectionWalkedToExhaustionReachesEveryMatchAndNothingElse is the
// traversal claim: a label selector is a question about a whole namespace, and a
// page of it answers nothing.
func TestALabelSelectionWalkedToExhaustionReachesEveryMatchAndNothingElse(t *testing.T) {
	t.Parallel()

	// Interleaved rather than grouped, and a population that is not a multiple of
	// the page size, so batches straddle every boundary that exists: whose run
	// this is, which label it carries, and where a page happens to fill.
	const total, pageSize = 62, 4

	all := make([]*workflow.WorkflowExecutionInfo, 0, total)
	wanted := map[string]bool{}

	for i := range total {
		id := fmt.Sprintf("run-%02d", i)

		switch i % 4 {
		case 0:
			// Mine, and labelled the way the selector asks about.
			all = append(all, labelledRun(t, id, "", map[string]string{"team": "payments"}))
			wanted[id] = true
		case 1:
			// Mine, another team's workload: the decoy that catches a selector
			// which reads the memo but not the value.
			all = append(all, labelledRun(t, id, "", map[string]string{"team": "search"}))
		case 2:
			// Mine, carrying no labels at all: the decoy that catches a selector
			// erroring or matching on absence.
			all = append(all, labelledRun(t, id, "", nil))
		case 3:
			// Somebody else's, carrying the *same* label: the decoy that catches
			// a selector applied before the tenant check.
			all = append(all, labelledRun(t, id, "somebody-else", map[string]string{"team": "payments"}))
		}
	}

	server := mustNew(t, pagingNamespace(t, all))

	seen := walk(t, server, t.Context(),
		`"team" in labels && labels["team"] == "payments"`, pageSize)

	require.Len(t, seen, len(wanted),
		"walking every page did not reach every run the selector matches")
	for id := range wanted {
		require.Equal(t, 1, seen[id], "run %q was skipped or returned twice", id)
	}
	for id := range seen {
		require.True(t, wanted[id], "the selection returned %q, which it does not match", id)
	}

	// The negative question over the same population, because a selector that
	// only ever answers one shape of question is one whose complement has never
	// been walked: every unlabelled run of the caller's, and only those.
	unlabelled := walk(t, server, t.Context(), `!("team" in labels)`, pageSize)
	require.NotEmpty(t, unlabelled, "no run answered the negative question")
	for id := range unlabelled {
		index, err := strconv.Atoi(id[len("run-"):])
		require.NoError(t, err)
		require.Equal(t, 2, index%4,
			"the negative question returned %q, which carries a team label", id)
	}
}

// TestALabelSelectionCannotReachAnotherTenantsRuns writes the direction that
// matters.
//
// Asserting that team-a's selector finds team-a's payments runs is a
// functionality test wearing a security test's clothes. The population here is
// built so that both tenants have runs carrying the *identical* label, which is
// the only arrangement in which "the filter selected it" and "the caller is
// entitled to it" can be told apart.
func TestALabelSelectionCannotReachAnotherTenantsRuns(t *testing.T) {
	t.Parallel()

	const total, pageSize = 40, 3
	const filter = `"team" in labels && labels["team"] == "payments"`

	all := make([]*workflow.WorkflowExecutionInfo, 0, total)
	theirs := map[string]bool{}
	mine := map[string]bool{}

	for i := range total {
		id := fmt.Sprintf("run-%02d", i)
		labels := map[string]string{"team": "payments"}

		if i%2 == 0 {
			all = append(all, labelledRun(t, id, "team-a", labels))
			mine[id] = true
			continue
		}

		all = append(all, labelledRun(t, id, "team-b", labels))
		theirs[id] = true
	}

	server := mustNew(t, pagingNamespace(t, all))

	asTeamA := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:    "https://issuer.example",
		Subject:   "operator@team-a.example",
		Namespace: "team-a",
	})

	seen := walk(t, server, asTeamA, filter, pageSize)

	require.Len(t, seen, len(mine), "team-a's selector did not reach every run team-a owns")
	for id := range mine {
		require.Equal(t, 1, seen[id], "run %q was skipped or returned twice", id)
	}

	// The claim: not one of team-b's identically-labelled runs is reachable by a
	// selector team-a wrote, however exactly it describes them.
	for id := range theirs {
		require.Zero(t, seen[id],
			"team-a's label selector reached team-b's run %q — the filter selected across a tenant boundary", id)
	}

	// And the same selector run as team-b reaches team-b's runs and none of
	// team-a's, so the boundary is a boundary rather than one tenant being
	// invisible to everybody.
	asTeamB := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:    "https://issuer.example",
		Subject:   "operator@team-b.example",
		Namespace: "team-b",
	})

	seenByB := walk(t, server, asTeamB, filter, pageSize)

	require.Len(t, seenByB, len(theirs), "team-b's selector did not reach every run team-b owns")
	for id := range mine {
		require.Zero(t, seenByB[id], "team-b's label selector reached team-a's run %q", id)
	}
}

// TestAWorkerVersionSelectionReachesOnlyTheRunsOnThatBuild is the bad-deploy
// selection, walked the same way.
//
// The version is the one field in a summary that is not read from a memo: it
// comes off Temporal's own versioning info, because it is a fact about where the
// run is executing rather than about its submission. So the decoys here are the
// shapes that fact arrives in — another build, the deprecated string form, half a
// version, and no versioning at all.
func TestAWorkerVersionSelectionReachesOnlyTheRunsOnThatBuild(t *testing.T) {
	t.Parallel()

	const pageSize = 3

	pinned := func(id string, info *workflow.WorkflowExecutionVersioningInfo) *workflow.WorkflowExecutionInfo {
		execution := labelledRun(t, id, "", nil)
		execution.VersioningInfo = info
		return execution
	}

	structured := func(name, build string) *workflow.WorkflowExecutionVersioningInfo {
		return &workflow.WorkflowExecutionVersioningInfo{
			DeploymentVersion: &deployment.WorkerDeploymentVersion{
				DeploymentName: name,
				BuildId:        build,
			},
		}
	}

	all := []*workflow.WorkflowExecutionInfo{
		pinned("bad-1", structured("flowstate", "417")),
		pinned("good-1", structured("flowstate", "418")),

		// A server too old to send the structured form sends the same
		// `name.build-id` spelling as a bare string, and a run on the bad build
		// has to be found whichever way it is reported.
		pinned("bad-2", &workflow.WorkflowExecutionVersioningInfo{Version: "flowstate.417"}),

		// Half a version names nothing that can be selected on, so it reads as
		// unpinned rather than as `flowstate.` or `.417` — either of which would
		// be a version string no worker ever declared.
		pinned("half", structured("flowstate", "")),

		pinned("unversioned", nil),
	}

	server := mustNew(t, pagingNamespace(t, all))

	seen := walk(t, server, t.Context(), `worker_version == "flowstate.417"`, pageSize)
	require.Equal(t, map[string]int{"bad-1": 1, "bad-2": 1}, seen,
		"selecting one build reached the wrong set of runs")

	// The complement, which is what an operator asks next: everything *not* on
	// the bad build, including the two that are pinned to nothing.
	rest := walk(t, server, t.Context(), `worker_version != "flowstate.417"`, pageSize)
	require.Equal(t, map[string]int{"good-1": 1, "half": 1, "unversioned": 1}, rest,
		"the complement of a version selection reached the wrong set of runs")
}

// TestASummarysStarterIsWhatGetReports pins the two readers of one memo field to
// one answer.
//
// A listing and a Get answering differently about who started a run is a bug
// nobody can reproduce, so both go through [FlowstateServer.memoStarter] and both
// apply the rule that the qualified form of two empty strings — an
// unauthenticated submission, only possible in development — is reported as
// nothing rather than as a bare separator that compares equal to no real
// identity.
func TestASummarysStarterIsWhatGetReports(t *testing.T) {
	t.Parallel()

	starter := func(id, recorded string) *workflow.WorkflowExecutionInfo {
		execution := labelledRun(t, id, "", nil)

		payload, err := converter.GetDefaultDataConverter().ToPayload(recorded)
		require.NoError(t, err)

		if execution.Memo == nil {
			execution.Memo = &common.Memo{Fields: map[string]*common.Payload{}}
		}
		execution.Memo.Fields[starterMemoKey] = payload

		return execution
	}

	alice := v1types.QualifiedSubject("https://issuer.example", "alice")

	all := []*workflow.WorkflowExecutionInfo{
		starter("hers", alice),
		starter("unauthenticated", v1types.QualifiedSubject("", "")),

		// No starter memo at all: a run started before the key existed.
		labelledRun(t, "ancient", "", nil),
	}

	server := mustNew(t, pagingNamespace(t, all))

	seen := walk(t, server, t.Context(), `starter == "`+alice+`"`, 3)
	require.Equal(t, map[string]int{"hers": 1}, seen,
		"selecting by starter reached the wrong set of runs")

	// Both of the honest absences answer the same way, which is what lets an
	// operator ask for them without knowing which kind of absence they are.
	absent := walk(t, server, t.Context(), `starter == ""`, 3)
	require.Equal(t, map[string]int{"unauthenticated": 1, "ancient": 1}, absent,
		"a run with no nameable starter was not reported as having none")
}
