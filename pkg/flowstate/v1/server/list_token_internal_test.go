package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"connectrpc.com/connect"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	common "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/mocks"
	"google.golang.org/protobuf/proto"

	v1types "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// A page token is the one input to List that a caller can hand back changed,
// and before it was signed the server accepted any that parsed while refusing
// any that did not as "not a token this server issued" — a sentence it had no
// way to stand behind. These tests are the negative direction of that sentence:
// each way a token can be wrong, refused with the words for that way, and the
// one way it can be right, accepted.
//
// All against a mock rather than the dev server, because nothing here is about
// what a position means to Temporal. The mock's namespace is endless, which is
// the one property needed: every page comes back with a token to check.

// endlessNamespace is a Temporal that always has another page, holding runs
// the caller owns so a page fills and a token is issued.
func endlessNamespace() *mocks.Client {
	temporal := &mocks.Client{}
	temporal.On("ListWorkflow", mock.Anything, mock.Anything).Return(
		func(_ context.Context, request *workflowservice.ListWorkflowExecutionsRequest) *workflowservice.ListWorkflowExecutionsResponse {
			executions := make([]*workflow.WorkflowExecutionInfo, 0, request.GetPageSize())
			for range int(request.GetPageSize()) {
				executions = append(executions, &workflow.WorkflowExecutionInfo{
					Execution: &common.WorkflowExecution{WorkflowId: "mine"},
				})
			}

			return &workflowservice.ListWorkflowExecutionsResponse{
				Executions:    executions,
				NextPageToken: []byte("there is always more"),
			}
		},
		nil,
	)

	return temporal
}

// issuedToken asks the server for a first page and returns the token it hands
// back, which every test below then does something to.
func issuedToken(t *testing.T, ctx context.Context, server *FlowstateServer, request *v1types.ListRequest) string {
	t.Helper()

	response, err := server.List(ctx, connect.NewRequest(request))
	require.NoError(t, err)
	require.NotEmpty(t, response.Msg.GetNextPageToken(), "an endless namespace issued no token")

	return response.Msg.GetNextPageToken()
}

// splitToken takes an issued token apart into the cursor the server signed and
// the code it signed it with, so a test can change one and keep the other.
func splitToken(t *testing.T, token string) (cursor *v1types.ListCursor, code []byte) {
	t.Helper()

	sealed, err := base64.RawURLEncoding.DecodeString(token)
	require.NoError(t, err)
	require.Greater(t, len(sealed), listTokenKeySize)

	raw, code := sealed[:len(sealed)-listTokenKeySize], sealed[len(sealed)-listTokenKeySize:]

	cursor = &v1types.ListCursor{}
	require.NoError(t, proto.Unmarshal(raw, cursor))

	return cursor, code
}

// forgedToken re-encodes a cursor under a code that was not computed over it.
func forgedToken(t *testing.T, cursor *v1types.ListCursor, code []byte) string {
	t.Helper()

	raw, err := proto.Marshal(cursor)
	require.NoError(t, err)

	return base64.RawURLEncoding.EncodeToString(append(raw, code...))
}

// TestListRefusesAForgedPageToken is the transcript from #1772: an issued token
// with its position rewritten and re-encoded. Before the code was checked that
// was accepted and answered with an empty page; now it is refused with the
// sentence that was previously reserved for tokens that failed to parse.
func TestListRefusesAForgedPageToken(t *testing.T) {
	t.Parallel()

	server := mustNew(t, endlessNamespace())
	token := issuedToken(t, t.Context(), server, &v1types.ListRequest{PageSize: 10})

	// The control: the token as issued is accepted, so the refusals below are
	// about what was changed rather than about the token being unusable.
	_, err := server.List(t.Context(), connect.NewRequest(&v1types.ListRequest{PageSize: 10, PageToken: token}))
	require.NoError(t, err, "the token the server just issued was refused unchanged")

	cursor, code := splitToken(t, token)

	rewritten := proto.Clone(cursor).(*v1types.ListCursor)
	rewritten.Position = []byte("not-a-run")

	for name, forged := range map[string]string{
		"a rewritten position under the original code": forgedToken(t, rewritten, code),
		"the original cursor under another server's code": func() string {
			// Another process, holding its own key: what a replica issues, and
			// what a caller who reads the layout out of the schema can build.
			other := mustNew(t, endlessNamespace())
			_, otherCode := splitToken(t, issuedToken(t, t.Context(), other, &v1types.ListRequest{PageSize: 10}))
			return forgedToken(t, cursor, otherCode)
		}(),
		"the cursor with no code at all": func() string {
			raw, err := proto.Marshal(cursor)
			require.NoError(t, err)
			return base64.RawURLEncoding.EncodeToString(raw)
		}(),
		"garbage": "garbage",
		"the old JSON shape": base64.RawURLEncoding.EncodeToString(
			[]byte(`{"CloseTime":"2026-09-06T05:18:01Z","StartTime":"1970-01-01T00:00:00Z","RunID":"not-a-run"}`)),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := server.List(t.Context(), connect.NewRequest(&v1types.ListRequest{PageSize: 10, PageToken: forged}))
			require.Error(t, err, "a token the server did not issue was accepted")
			require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
			require.ErrorContains(t, err, "page token is not a token this server issued")
		})
	}
}

// TestListRefusesAPageTokenFromAnotherQuery pins that a cursor belongs to the
// question it was issued for.
//
// Under a different filter the position a cursor names is a page that need not
// exist, which used to come back as a silently empty page rather than an
// error. The refusal names the mismatch so a caller who changed the filter
// mid-listing is told what to put back.
func TestListRefusesAPageTokenFromAnotherQuery(t *testing.T) {
	t.Parallel()

	server := mustNew(t, endlessNamespace())

	issued := &v1types.ListRequest{PageSize: 10, Filter: `status == "RUNNING"`}
	token := issuedToken(t, t.Context(), server, issued)

	for name, request := range map[string]*v1types.ListRequest{
		"another filter":    {PageSize: 10, Filter: `status == "FAILED"`, PageToken: token},
		"no filter":         {PageSize: 10, PageToken: token},
		"another page size": {PageSize: 20, Filter: `status == "RUNNING"`, PageToken: token},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := server.List(t.Context(), connect.NewRequest(request))
			require.Error(t, err, "a token issued for one query was accepted for another")
			require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
			require.ErrorContains(t, err, "page token was issued for a different filter or page size")
		})
	}

	// The positive direction, which the refusals above cannot establish on
	// their own: the same query, spelled the same way, continues.
	_, err := server.List(t.Context(), connect.NewRequest(&v1types.ListRequest{
		PageSize: 10, Filter: `status == "RUNNING"`, PageToken: token,
	}))
	require.NoError(t, err, "a token was refused for the query it was issued for")
}

// TestAPageTokenIsBoundToTheEffectivePageSize pins that the digest is over the
// page size the server actually used, not the number the caller typed.
//
// Zero takes the default, so a caller who omitted the size on the first page
// and spelled out the default on the second is asking the same question and
// should not be told otherwise. The clamp above the ceiling is the same rule,
// but the schema refuses those sizes before the digest sees them (see
// TestPageSizeCeilingMatchesTheSchema), so zero is the one spelling that can
// be exercised.
func TestAPageTokenIsBoundToTheEffectivePageSize(t *testing.T) {
	t.Parallel()

	server := mustNew(t, endlessNamespace())
	token := issuedToken(t, t.Context(), server, &v1types.ListRequest{})

	_, err := server.List(t.Context(), connect.NewRequest(&v1types.ListRequest{
		PageSize: defaultListPageSize, PageToken: token,
	}))
	require.NoError(t, err, "a token issued under the default page size was refused when the default was spelled out")
}

// TestListRefusesAPageTokenFromAnotherNamespace pins that a cursor is a place
// in one tenant's listing and nothing in another's.
//
// One server, two authenticated callers, which is what a multi-tenant
// deployment is: the key is per process, so a token from tenant A carries a
// code tenant B's request would verify. The namespace inside the cursor is
// what refuses it.
func TestListRefusesAPageTokenFromAnotherNamespace(t *testing.T) {
	t.Parallel()

	server := mustNew(t, endlessNamespace())

	teamA := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer: "https://issuer.example.com", Subject: "a@example.com", Namespace: "team-a",
	})
	teamB := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer: "https://issuer.example.com", Subject: "b@example.com", Namespace: "team-b",
	})

	token := issuedToken(t, teamA, server, &v1types.ListRequest{PageSize: 10})

	_, err := server.List(teamB, connect.NewRequest(&v1types.ListRequest{PageSize: 10, PageToken: token}))
	require.Error(t, err, "a tenant continued another tenant's listing")
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	require.ErrorContains(t, err, "page token was issued to a different namespace")

	_, err = server.List(teamA, connect.NewRequest(&v1types.ListRequest{PageSize: 10, PageToken: token}))
	require.NoError(t, err, "a tenant was refused its own listing")
}

// TestListRefusesAnExpiredPageToken pins the lifetime, in both directions.
//
// The tokens are issued directly rather than through List, because List issues
// them at the wall clock and a day is longer than a test. Signed with the
// server's own key, so the only thing wrong with the expired one is its age.
func TestListRefusesAnExpiredPageToken(t *testing.T) {
	t.Parallel()

	server := mustNew(t, endlessNamespace())

	// What a first page with no filter and no page size is bound to: the
	// server's own default tenant and the default page size.
	query := listQueryDigest("", defaultListPageSize)

	t.Run("a day and a minute ago", func(t *testing.T) {
		token, err := server.issuePageToken([]byte("somewhere"), "", query, time.Now().Add(-listTokenLifetime-time.Minute))
		require.NoError(t, err)

		_, err = server.List(t.Context(), connect.NewRequest(&v1types.ListRequest{PageToken: token}))
		require.Error(t, err, "a token older than its lifetime was accepted")
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
		require.ErrorContains(t, err, "page token has expired")
	})

	t.Run("an hour ago", func(t *testing.T) {
		token, err := server.issuePageToken([]byte("somewhere"), "", query, time.Now().Add(-time.Hour))
		require.NoError(t, err)

		_, err = server.List(t.Context(), connect.NewRequest(&v1types.ListRequest{PageToken: token}))
		require.NoError(t, err, "a token well inside its lifetime was refused")
	})

	t.Run("with no issue time at all", func(t *testing.T) {
		// Nothing the server issues lacks one; this is the fail-closed reading
		// of a cursor that somehow does, which must not read as fresh.
		raw, err := proto.Marshal(&v1types.ListCursor{Position: []byte("somewhere"), QueryDigest: query})
		require.NoError(t, err)
		token := base64.RawURLEncoding.EncodeToString(server.sealListCursor(raw))

		_, err = server.List(t.Context(), connect.NewRequest(&v1types.ListRequest{PageToken: token}))
		require.Error(t, err)
		require.ErrorContains(t, err, "page token has expired")
	})
}

// TestARejectedPositionIsTheCallersToRestart pins what happens when a token
// this server issued names a position Temporal no longer accepts.
//
// The code proves who issued the token, not that the position inside it is
// still one the visibility store has — a store swapped under a running server
// within the token's lifetime leaves signed positions that name nothing. That
// is the caller's to start over from, said in this server's words: Temporal's
// own text can name namespaces this deployment does not disclose. A first
// page hands Temporal no position at all, so an error there is the server's.
func TestARejectedPositionIsTheCallersToRestart(t *testing.T) {
	t.Parallel()

	const temporalsOwnWords = "namespace some-other-tenant-namespace: invalid token"

	temporal := &mocks.Client{}
	temporal.On("ListWorkflow", mock.Anything, mock.MatchedBy(func(request *workflowservice.ListWorkflowExecutionsRequest) bool {
		return len(request.GetNextPageToken()) == 0
	})).Return(&workflowservice.ListWorkflowExecutionsResponse{
		Executions:    []*workflow.WorkflowExecutionInfo{mineExecution(t, "mine")},
		NextPageToken: []byte("a position the store then forgot"),
	}, nil)
	temporal.On("ListWorkflow", mock.Anything, mock.MatchedBy(func(request *workflowservice.ListWorkflowExecutionsRequest) bool {
		return len(request.GetNextPageToken()) > 0
	})).Return(nil, errors.New(temporalsOwnWords))

	server := mustNew(t, temporal)
	token := issuedToken(t, t.Context(), server, &v1types.ListRequest{PageSize: 1})

	_, err := server.List(t.Context(), connect.NewRequest(&v1types.ListRequest{PageSize: 1, PageToken: token}))
	require.Error(t, err, "a position the store rejected was answered with a page")
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err),
		"a rejected position was reported as something other than the caller's to restart: %v", err)
	require.ErrorContains(t, err, "the page token names a position this listing no longer has")
	require.NotContains(t, err.Error(), temporalsOwnWords, "Temporal's own text was relayed to the caller")

	// And the first page, which carried no position, is the one case that is
	// not the token's fault.
	firstPageFails := &mocks.Client{}
	firstPageFails.On("ListWorkflow", mock.Anything, mock.Anything).Return(nil, errors.New("visibility store unavailable"))

	_, err = mustNew(t, firstPageFails).List(t.Context(), connect.NewRequest(&v1types.ListRequest{PageSize: 1}))
	require.Error(t, err)
	require.Equal(t, connect.CodeInternal, connect.CodeOf(err),
		"a first-page failure, which no token could have caused, was blamed on the caller: %v", err)
}

// TestAWorstCaseTokenFitsTheSchema pins that every token this server can issue
// comes back through ListRequest.page_token's own bound.
//
// A token is issued by this server and validated by this server, and the two
// bounds are written in two places: the position bound here, the token bound
// in the schema. If the sum of the largest position, the longest namespace, a
// digest, a timestamp and the code, base64-encoded, ever exceeded the schema's
// limit, the failure would be a listing that works exactly once — the first
// page issued, the second refused by the validator as too long. So the limit
// is read from the descriptor rather than restated, and the token built here is
// the largest one the server can produce.
func TestAWorstCaseTokenFitsTheSchema(t *testing.T) {
	t.Parallel()

	field := (&v1types.ListRequest{}).ProtoReflect().Descriptor().Fields().ByName("page_token")
	require.NotNil(t, field, "page_token is gone from the schema")

	rules, _ := proto.GetExtension(field.Options(), validate.E_Field).(*validate.FieldRules)
	require.NotNil(t, rules, "page_token carries no validation rules")
	require.NotZero(t, rules.GetString().GetMaxLen(), "page_token carries no length bound, so nothing bounds what a caller may send")

	server := mustNew(t, endlessNamespace())

	// Every field at its largest: the position at its bound, a namespace at the
	// grammar's longest, a real digest, and a timestamp whose varints are as
	// wide as a timestamp's get.
	position := bytes.Repeat([]byte{0xff}, maxListPositionBytes)
	namespace := strings.Repeat("n", auth.MaxNamespaceLen)
	farFuture := time.Date(9999, time.December, 31, 23, 59, 59, 999_999_999, time.UTC)

	token, err := server.issuePageToken(position, namespace, listQueryDigest("", defaultListPageSize), farFuture)
	require.NoError(t, err, "a position at the bound was refused, so the bound is not the one that applies")
	require.LessOrEqual(t, uint64(len(token)), rules.GetString().GetMaxLen(),
		"the largest token this server can issue is longer than the schema lets a caller send back")

	require.NoError(t, v1types.Validate(&v1types.ListRequest{PageToken: token}),
		"the largest token this server can issue does not pass the request validator")
}

// TestAnOversizePositionIsRefusedAtIssue is the other side of the bound: a
// position the server cannot carry is refused where the listing can still say
// so, naming the size, rather than issued and refused on the page after.
func TestAnOversizePositionIsRefusedAtIssue(t *testing.T) {
	t.Parallel()

	temporal := &mocks.Client{}
	temporal.On("ListWorkflow", mock.Anything, mock.Anything).Return(
		&workflowservice.ListWorkflowExecutionsResponse{
			Executions:    []*workflow.WorkflowExecutionInfo{mineExecution(t, "mine")},
			NextPageToken: bytes.Repeat([]byte{0xff}, maxListPositionBytes+1),
		}, nil)

	_, err := mustNew(t, temporal).List(t.Context(), connect.NewRequest(&v1types.ListRequest{PageSize: 1}))
	require.Error(t, err, "a position over the bound was issued as a token")
	require.Equal(t, connect.CodeInternal, connect.CodeOf(err),
		"a visibility store's oversize position is not the caller's mistake: %v", err)
	require.ErrorContains(t, err, fmt.Sprintf("%d bytes", maxListPositionBytes+1))
	require.ErrorContains(t, err, fmt.Sprintf("more than the %d", maxListPositionBytes))
}

// TestTheEndOfAListingIsAnEmptyToken pins that exhaustion is still reported
// the one way a caller can read it, rather than as a signed cursor naming
// nothing.
func TestTheEndOfAListingIsAnEmptyToken(t *testing.T) {
	t.Parallel()

	temporal := &mocks.Client{}
	temporal.On("ListWorkflow", mock.Anything, mock.Anything).Return(
		&workflowservice.ListWorkflowExecutionsResponse{}, nil)

	response, err := mustNew(t, temporal).List(t.Context(), connect.NewRequest(&v1types.ListRequest{}))
	require.NoError(t, err)
	require.Empty(t, response.Msg.GetNextPageToken(), "an exhausted listing issued a token")
}
