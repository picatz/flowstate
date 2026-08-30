package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	mcpauth "github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// The verified caller reaching a tool handler is the whole point of
// [withMCPPrincipal], and the thing nothing else in this package would notice
// the loss of: every tool answers the same way with or without it, because no
// handler served today reads the principal (see auth.MCPTokenVerifier's note
// on why the carry is inert but not pointless). So the assertions live here,
// on the context the handler is called with, rather than on any tool's answer
// — a test written against an answer would stay green with the whole
// installation deleted.
//
// Both registration paths are covered, because they are two separate calls to
// the same wrapper and either could be dropped alone: [wrapToolHandler], which
// [AddLocalCapabilities] uses for every tool it registers, and [dispatch],
// which [AddCapabilities] registers directly with no wrapper around it.

// principalTestResource is the resource identifier the token below is minted
// for. It only has to be the same on both sides of the verifier.
const principalTestResource = "https://flowstate.example.com/mcp"

// verifiedTokenInfo mints a real token, verifies it through the real adapter,
// and returns the TokenInfo the SDK would hand a handler along with the token
// itself. Built rather than faked so that what travels is exactly what
// production puts in Extra — a hand-built TokenInfo would keep passing if the
// verifier stopped carrying the principal at all.
func verifiedTokenInfo(t *testing.T, claims map[string]any) (*mcp.CallToolRequest, string) {
	t.Helper()

	issuer := authtest.NewIssuer()
	t.Cleanup(func() { _ = issuer.Close() })

	verifier, err := auth.NewOIDCVerifier(auth.Policy{
		Issuers: []auth.TrustedIssuer{{
			Name:      "agent-idp",
			Issuer:    issuer.URL(),
			Audiences: []string{principalTestResource},
		}},
	}, auth.WithEgressPolicy(authtest.EgressPolicy()))
	require.NoError(t, err)

	token := issuer.MintToken(claims,
		authtest.WithSubject("agent"),
		authtest.WithAudience(principalTestResource),
	)

	info, err := auth.MCPTokenVerifier(verifier, principalTestResource)(t.Context(), token, nil)
	require.NoError(t, err)
	require.NotNil(t, info)

	return &mcp.CallToolRequest{
		Params: &mcp.CallToolParamsRaw{Arguments: json.RawMessage(`{}`)},
		Extra:  &mcp.RequestExtra{TokenInfo: info},
	}, token
}

// TestToolHandlersRunAsTheVerifiedCaller is the carry, asserted where a
// handler would read it.
func TestToolHandlersRunAsTheVerifiedCaller(t *testing.T) {
	t.Parallel()

	const secretClaim = "SUPERSECRET-PERSONAL-DATA"

	for name, build := range map[string]func(observe mcp.ToolHandler) mcp.ToolHandler{
		// The path AddLocalCapabilities registers every tool through,
		// including the extra ToolRegistrations `flow mcp serve` supplies.
		"wrapToolHandler": func(observe mcp.ToolHandler) mcp.ToolHandler {
			return wrapToolHandler(Deps{}, "flowstate_validate", observe)
		},

		// The same, with a Deps.WrapHandler in place: `flow mcp serve` always
		// has one (its registry guard), so a principal installed outside the
		// caller's wrapper must still arrive inside it.
		"wrapToolHandler behind Deps.WrapHandler": func(observe mcp.ToolHandler) mcp.ToolHandler {
			return wrapToolHandler(Deps{
				WrapHandler: func(_ string, next mcp.ToolHandler) mcp.ToolHandler {
					return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
						return next(ctx, req)
					}
				},
			}, "flowstate_validate", observe)
		},

		// AddCapabilities registers dispatch's handler with nothing around it,
		// so dispatch installs the principal itself. Observed from inside
		// ServiceMethod.Call, which is the context the RPC really runs on.
		"dispatch": func(observe mcp.ToolHandler) mcp.ToolHandler {
			return dispatch(ServiceMethod{
				Name:  "Validate",
				Input: (&v1.ValidateRequest{}).ProtoReflect().Descriptor(),
				Call: func(ctx context.Context, _ *server.FlowstateServer,
					_ func() flowstatev1connect.WorkflowServiceClient, _ proto.Message,
				) (proto.Message, error) {
					if _, err := observe(ctx, nil); err != nil {
						return nil, err
					}
					return &v1.ValidateResponse{}, nil
				},
			}, nil, nil, Deps{})
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			req, token := verifiedTokenInfo(t, map[string]any{"email": secretClaim})

			var (
				called    bool
				principal auth.Principal
				found     bool
				rendered  string
			)
			handler := build(func(ctx context.Context, _ *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				called = true
				principal, found = auth.PrincipalFromContext(ctx)
				rendered = fmt.Sprintf("%v %+v", ctx, ctx)
				return &mcp.CallToolResult{}, nil
			})

			_, err := handler(t.Context(), req)
			require.NoError(t, err)
			require.True(t, called, "the handler under test never ran, so it asserted nothing")

			require.True(t, found,
				"the verified caller did not reach the handler: auth.PrincipalFromContext found nothing, "+
					"so any authorization a handler performs would run unauthenticated")
			require.Equal(t, "agent", principal.Subject)
			require.Equal(t, secretClaim, principal.Claims["email"],
				"the principal that arrived is not the verified one")

			// And the half of the PR's title that is a refusal: what travels
			// is the caller, never the credential. A tool handler that logged
			// its own context must not be able to print a bearer token.
			require.NotContains(t, rendered, token,
				"the bearer token is reachable from the handler's context")
			require.NotContains(t, rendered, secretClaim,
				"the context prints the verified claims; Principal.String redacts them and something is "+
					"reaching past it")
		})
	}
}

// TestToolHandlersWithoutATokenRunUnauthenticated is the negative direction,
// and the one that keeps the test above from passing for the wrong reason.
//
// `flow mcp` over stdio has no bearer token at all, and `flow mcp serve` can
// be reached by a request the middleware admitted with no TokenInfo attached.
// Neither may leave a principal behind: a handler that reads one must see
// "unauthenticated" rather than whoever called last.
func TestToolHandlersWithoutATokenRunUnauthenticated(t *testing.T) {
	t.Parallel()

	for name, req := range map[string]*mcp.CallToolRequest{
		"no Extra at all": {Params: &mcp.CallToolParamsRaw{}},
		"Extra, no token": {Params: &mcp.CallToolParamsRaw{}, Extra: &mcp.RequestExtra{}},

		// A TokenInfo some other middleware produced: authenticated as far as
		// the SDK is concerned, but carrying nothing this repository verified.
		// Reading a principal out of it would be trusting an assertion nothing
		// here checked.
		"a TokenInfo with no principal": {
			Params: &mcp.CallToolParamsRaw{},
			Extra:  &mcp.RequestExtra{TokenInfo: &mcpauth.TokenInfo{UserID: "somebody"}},
		},
		"a TokenInfo whose Extra is somebody else's": {
			Params: &mcp.CallToolParamsRaw{},
			Extra: &mcp.RequestExtra{TokenInfo: &mcpauth.TokenInfo{
				UserID: "somebody",
				Extra: map[string]any{
					"flowstate.auth.principal": auth.Principal{Issuer: "https://forged.example.com", Subject: "mallory"},
				},
			}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var (
				called bool
				found  bool
			)
			handler := wrapToolHandler(Deps{}, "flowstate_validate",
				func(ctx context.Context, _ *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
					called = true
					_, found = auth.PrincipalFromContext(ctx)
					return &mcp.CallToolResult{}, nil
				})

			_, err := handler(t.Context(), req)
			require.NoError(t, err)
			require.True(t, called)
			require.False(t, found,
				"a request carrying no verified principal left one on the handler's context")
		})
	}
}

// TestMCPAuditIsWriteAheadAndExactlyOnceForEveryExecutionOutcome separates the
// authorization decision from what happens afterward. Success, a tool-level
// refusal, and cancellation all reached the same authoritative allow before
// the handler; none is a reason to rewrite that true decision or emit twice.
func TestMCPAuditIsWriteAheadAndExactlyOnceForEveryExecutionOutcome(t *testing.T) {
	const (
		secretClaim = "PRIVATE-CLAIM-CORPUS-TEXT"
		secretArg   = "prompt-with-secret-token"
		secretOut   = "unbounded-tool-output-secret"
	)

	for _, outcome := range []string{"allowed", "tool refusal", "cancelled"} {
		t.Run(outcome, func(t *testing.T) {
			req, token := verifiedTokenInfo(t, map[string]any{"private": secretClaim})
			req.Params.Arguments = json.RawMessage(`{"source":"` + secretArg + `"}`)

			mutated := false
			var sink toolAuditEmitter
			sink.onEmit = func(record *v1.AuditRecord) {
				require.False(t, mutated, "the tool ran before its authorization decision was written")
			}
			recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(&sink))
			require.NoError(t, err)

			handler := wrapToolHandler(Deps{Audit: recorder}, "flowstate_validate",
				func(ctx context.Context, _ *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
					mutated = true
					switch outcome {
					case "tool refusal":
						return ToolError(errors.New(secretOut)), nil
					case "cancelled":
						return ToolError(ctx.Err()), nil
					default:
						return &mcp.CallToolResult{}, nil
					}
				})

			ctx := t.Context()
			if outcome == "cancelled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			result, err := handler(ctx, req)
			require.NoError(t, err)
			require.True(t, mutated, "the positive control handler never ran")
			if outcome != "allowed" {
				require.True(t, result.IsError)
			}

			require.Len(t, sink.records, 1, "one tool invocation produced other than one authorization record")
			record := sink.records[0]
			require.Equal(t, "flowstate_validate", record.GetMcpTool())
			require.Empty(t, record.GetRpc())
			require.Equal(t, v1.AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_VALIDATE, record.GetAction())
			require.Equal(t, v1.AuditDecision_AUDIT_DECISION_ALLOW, record.GetDecision())
			require.Equal(t, "agent", record.GetIdentity().GetSubject())
			require.Equal(t, "agent-idp", record.GetIssuerName())
			require.Empty(t, record.GetIdentity().GetClaims(),
				"MCP audit copied the token's claims instead of the bounded audit identity")

			encoded, err := protojson.Marshal(record)
			require.NoError(t, err)
			for _, forbidden := range []string{token, secretClaim, secretArg, secretOut} {
				require.NotContains(t, string(encoded), forbidden,
					"credential, claim, argument, or tool output reached the audit record")
			}
		})
	}
}

// TestMCPAuditRefusesBeforeTheHandlerWhenItCannotNameTheDecision covers both
// fail-closed paths at the seam: a tool outside the authorization vocabulary
// and a required sink failure. Neither is allowed to reach tool mutation, and
// an unnameable operation cannot be emitted under a guessed action.
func TestMCPAuditRefusesBeforeTheHandlerWhenItCannotNameTheDecision(t *testing.T) {
	req, _ := verifiedTokenInfo(t, nil)

	t.Run("unbound tool", func(t *testing.T) {
		var sink toolAuditEmitter
		recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(&sink))
		require.NoError(t, err)

		called := false
		handler := wrapToolHandler(Deps{Audit: recorder}, "flowstate_not_registered",
			func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				called = true
				return &mcp.CallToolResult{}, nil
			})
		result, err := handler(t.Context(), req)
		require.NoError(t, err)
		require.True(t, result.IsError)
		require.False(t, called)
		require.Empty(t, sink.records)
	})

	t.Run("required sink failure", func(t *testing.T) {
		const privateSinkDetail = "collector.internal:4317 unavailable"
		recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(toolAuditEmitterFunc(
			func(context.Context, *v1.AuditRecord) error { return errors.New(privateSinkDetail) },
		)), audit.Required())
		require.NoError(t, err)

		called := false
		var reported error
		handler := wrapToolHandler(Deps{
			Audit: recorder,
			AuditFailure: func(err error) {
				reported = err
			},
		}, "flowstate_validate",
			func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				called = true
				return &mcp.CallToolResult{}, nil
			})
		result, err := handler(t.Context(), req)
		require.NoError(t, err)
		require.True(t, result.IsError)
		require.False(t, called)
		require.ErrorContains(t, reported, privateSinkDetail,
			"the operator-facing failure omitted the sink detail")
		require.Len(t, result.Content, 1)
		text, ok := result.Content[0].(*mcp.TextContent)
		require.True(t, ok)
		require.Equal(t, "the authorization decision could not be recorded; try again", text.Text)
		require.NotContains(t, text.Text, privateSinkDetail,
			"the remotely visible tool refusal exposed sink details")
	})
}

type toolAuditEmitter struct {
	records []*v1.AuditRecord
	onEmit  func(*v1.AuditRecord)
}

func (e *toolAuditEmitter) Emit(_ context.Context, record *v1.AuditRecord) error {
	if e.onEmit != nil {
		e.onEmit(record)
	}
	e.records = append(e.records, record)

	return nil
}

type toolAuditEmitterFunc func(context.Context, *v1.AuditRecord) error

func (f toolAuditEmitterFunc) Emit(ctx context.Context, record *v1.AuditRecord) error {
	return f(ctx, record)
}
