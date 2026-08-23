package relationships_test

import (
	"context"
	"errors"
	"testing"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/relationships"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type graphBackend struct {
	revision string
	edges    []*flowstatev1.RelationshipEdge
	err      error
	reads    int
}

func (b *graphBackend) Snapshot(context.Context, string) (string, error) {
	if b.err != nil {
		return "", b.err
	}
	return b.revision, nil
}

func (b *graphBackend) Edges(_ context.Context, subject *flowstatev1.EntityReference, revision string, limit uint32) (relationships.RelationshipRead, error) {
	b.reads++
	if b.err != nil {
		return relationships.RelationshipRead{}, b.err
	}
	var out []*flowstatev1.RelationshipEdge
	for _, edge := range b.edges {
		if proto.Equal(edge.GetSubject(), subject) {
			out = append(out, edge)
		}
	}
	return relationships.RelationshipRead{Revision: revision, Edges: out, BytesRead: uint64(len(out) * 32)}, nil
}

func entity(kind flowstatev1.EntityKind, id string) *flowstatev1.EntityReference {
	return &flowstatev1.EntityReference{Tenant: "acme", Kind: kind, Id: id}
}

func edge(subject *flowstatev1.EntityReference, relation string, resource *flowstatev1.EntityReference) *flowstatev1.RelationshipEdge {
	return &flowstatev1.RelationshipEdge{Subject: subject, Relation: relation, Resource: resource}
}

func request(subject *flowstatev1.EntityReference, relations ...string) *flowstatev1.RelationshipResolveRequest {
	return &flowstatev1.RelationshipResolveRequest{
		Subject: subject, Relations: relations,
		Limits: &flowstatev1.RelationshipLimits{MaxDepth: 8, MaxFanOut: 16, MaxEntitiesRead: 32, MaxBytesRead: 4096, MaxBackendRequests: 40, MaxEvaluationCost: 100},
	}
}

func TestRelationshipResolverGroupNestingResourceInheritanceAndRoles(t *testing.T) {
	alice, engineers, platform := entity(flowstatev1.EntityKind_ENTITY_KIND_PRINCIPAL, "alice"), entity(flowstatev1.EntityKind_ENTITY_KIND_GROUP, "engineers"), entity(flowstatev1.EntityKind_ENTITY_KIND_TEAM, "platform")
	service, plugin := entity(flowstatev1.EntityKind_ENTITY_KIND_SERVICE, "api"), entity(flowstatev1.EntityKind_ENTITY_KIND_PLUGIN, "deploy")
	backend := &graphBackend{revision: "42", edges: []*flowstatev1.RelationshipEdge{
		edge(alice, "member_of", engineers), edge(engineers, "member_of", platform),
		edge(platform, "owns", service), edge(service, "parent_of", plugin), edge(platform, "role:deployer", service),
	}}
	set, err := (relationships.BoundedRelationshipResolver{Backend: backend}).Resolve(t.Context(), request(alice, "member_of", "owns", "parent_of", "role:deployer"))
	require.NoError(t, err)
	require.Equal(t, "42", set.GetRevision())
	require.Len(t, set.GetEntities(), 5)
	require.True(t, relationships.Has(set, engineers, "member_of", platform))
	require.True(t, relationships.HasRole(set, platform, "deployer", service))
}

func TestRelationshipResolverSupportsAgentAndPrincipalSubjects(t *testing.T) {
	team := entity(flowstatev1.EntityKind_ENTITY_KIND_TEAM, "operators")
	for _, subject := range []*flowstatev1.EntityReference{entity(flowstatev1.EntityKind_ENTITY_KIND_PRINCIPAL, "user"), entity(flowstatev1.EntityKind_ENTITY_KIND_AGENT, "assistant")} {
		backend := &graphBackend{revision: "1", edges: []*flowstatev1.RelationshipEdge{edge(subject, "member_of", team)}}
		set, err := (relationships.BoundedRelationshipResolver{Backend: backend}).Resolve(t.Context(), request(subject, "member_of"))
		require.NoError(t, err)
		require.True(t, relationships.Has(set, subject, "member_of", team))
	}
}

func TestRelationshipResolverFailsClosed(t *testing.T) {
	root, group := entity(flowstatev1.EntityKind_ENTITY_KIND_PRINCIPAL, "alice"), entity(flowstatev1.EntityKind_ENTITY_KIND_GROUP, "g")
	t.Run("tenant isolation", func(t *testing.T) {
		other := proto.Clone(group).(*flowstatev1.EntityReference)
		other.Tenant = "other"
		_, err := (relationships.BoundedRelationshipResolver{Backend: &graphBackend{revision: "1", edges: []*flowstatev1.RelationshipEdge{edge(root, "member_of", other)}}}).Resolve(t.Context(), request(root, "member_of"))
		require.ErrorIs(t, err, relationships.ErrRelationshipDenied)
	})
	t.Run("cycle", func(t *testing.T) {
		backend := &graphBackend{revision: "1", edges: []*flowstatev1.RelationshipEdge{edge(root, "member_of", group), edge(group, "member_of", root)}}
		_, err := (relationships.BoundedRelationshipResolver{Backend: backend}).Resolve(t.Context(), request(root, "member_of"))
		require.ErrorIs(t, err, relationships.ErrRelationshipCycle)
	})
	t.Run("backend failure", func(t *testing.T) {
		_, err := (relationships.BoundedRelationshipResolver{Backend: &graphBackend{err: errors.New("offline")}}).Resolve(t.Context(), request(root, "member_of"))
		require.ErrorIs(t, err, relationships.ErrRelationshipBackend)
	})
	t.Run("stale required revision", func(t *testing.T) {
		req := request(root, "member_of")
		req.RequireFresh = true
		req.RequiredRevision = "new"
		_, err := (relationships.BoundedRelationshipResolver{Backend: &graphBackend{revision: "old"}}).Resolve(t.Context(), req)
		require.ErrorIs(t, err, relationships.ErrRelationshipStale)
	})
	t.Run("ambiguous duplicate", func(t *testing.T) {
		e := edge(root, "member_of", group)
		_, err := (relationships.BoundedRelationshipResolver{Backend: &graphBackend{revision: "1", edges: []*flowstatev1.RelationshipEdge{e, proto.Clone(e).(*flowstatev1.RelationshipEdge)}}}).Resolve(t.Context(), request(root, "member_of"))
		require.ErrorIs(t, err, relationships.ErrRelationshipAmbiguous)
	})
}

func TestRelationshipResolverExhaustsEveryIndependentBudget(t *testing.T) {
	root, a, b := entity(flowstatev1.EntityKind_ENTITY_KIND_PRINCIPAL, "alice"), entity(flowstatev1.EntityKind_ENTITY_KIND_GROUP, "a"), entity(flowstatev1.EntityKind_ENTITY_KIND_GROUP, "b")
	edges := []*flowstatev1.RelationshipEdge{edge(root, "member_of", a), edge(root, "member_of", b), edge(a, "member_of", b)}
	mutate := map[string]func(*flowstatev1.RelationshipLimits){
		"fan-out":          func(l *flowstatev1.RelationshipLimits) { l.MaxFanOut = 1 },
		"depth":            func(l *flowstatev1.RelationshipLimits) { l.MaxDepth = 1 },
		"entities":         func(l *flowstatev1.RelationshipLimits) { l.MaxEntitiesRead = 1 },
		"bytes":            func(l *flowstatev1.RelationshipLimits) { l.MaxBytesRead = 1 },
		"backend requests": func(l *flowstatev1.RelationshipLimits) { l.MaxBackendRequests = 1 },
		"evaluation cost":  func(l *flowstatev1.RelationshipLimits) { l.MaxEvaluationCost = 1 },
	}
	for name, change := range mutate {
		t.Run(name, func(t *testing.T) {
			req := request(root, "member_of")
			change(req.Limits)
			_, err := (relationships.BoundedRelationshipResolver{Backend: &graphBackend{revision: "1", edges: edges}}).Resolve(t.Context(), req)
			require.ErrorIs(t, err, relationships.ErrRelationshipBudget)
		})
	}
}

func TestUseBoundaryReauthorizesRemovedMembershipAndPolicyRevision(t *testing.T) {
	user, team := entity(flowstatev1.EntityKind_ENTITY_KIND_PRINCIPAL, "alice"), entity(flowstatev1.EntityKind_ENTITY_KIND_TEAM, "ops")
	backend := &graphBackend{revision: "1", edges: []*flowstatev1.RelationshipEdge{edge(user, "member_of", team)}}
	authorizer := relationships.ResolverAuthorizer{Resolver: relationships.BoundedRelationshipResolver{Backend: backend}}
	first, err := authorizer.AuthorizeUse(t.Context(), flowstatev1.AuthorizationUseBoundary_AUTHORIZATION_USE_BOUNDARY_MCP_TOOL_CALL, request(user, "member_of"))
	require.NoError(t, err)
	require.True(t, relationships.Has(first, user, "member_of", team))

	backend.revision = "2"
	backend.edges = nil // membership/policy changed during the session
	second, err := authorizer.AuthorizeUse(t.Context(), flowstatev1.AuthorizationUseBoundary_AUTHORIZATION_USE_BOUNDARY_DURABLE_STEP, request(user, "member_of"))
	require.NoError(t, err)
	require.Equal(t, "2", second.GetRevision())
	require.False(t, relationships.Has(second, user, "member_of", team))
	require.Greater(t, backend.reads, 1, "each meaningful use must consult the backend again")
}
