package server

import (
	"context"

	"connectrpc.com/connect"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// Whoami answers with the caller's own principal.
//
// It reads the same [auth.Principal] every other handler authorizes against and
// renders it through [v1.ProtoPrincipal], the converter a run's identity uses,
// so what a caller is told it is and what its runs record cannot differ. Its
// claims are only those the admitting policy entry carries; the credential
// itself is not part of a principal and so cannot appear in the answer.
//
// Every caller may ask: identity.read is held by every caller (see
// [v1.AuthorizationActionHeldByEveryCaller]), so the check below can refuse
// only an embedder's own decider. An unauthenticated caller is answered, not
// refused: `authenticated` is false and the principal is the anonymous one, or
// empty when the deployment has no authentication at all.
func (s *FlowstateServer) Whoami(
	ctx context.Context,
	req *connect.Request[v1.WhoamiRequest],
) (*connect.Response[v1.WhoamiResponse], error) {
	if err := v1.Validate(req.Msg); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	// The caller's own principal addresses no run, schedule or tenant resource.
	if err := s.auditAllow(ctx, "Whoami", v1.AuditResourceKind_AUDIT_RESOURCE_KIND_UNSPECIFIED, ""); err != nil {
		return nil, err
	}

	principal, ok := auth.PrincipalFromContext(ctx)
	response := &v1.WhoamiResponse{
		Principal:     s.identityFor(ctx).GetPrincipal(),
		Authenticated: ok && !principal.IsAnonymous() && !principal.IsZero(),
	}
	if response.Principal == nil {
		// Present even when empty, so "nobody" is stated rather than inferred
		// from absence.
		response.Principal = &v1.Principal{}
	}

	return connect.NewResponse(response), nil
}
