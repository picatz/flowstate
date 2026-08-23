package relationships

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"google.golang.org/protobuf/proto"
)

var (
	ErrRelationshipDenied         = errors.New("relationship resolution denied")
	ErrRelationshipBackend        = fmt.Errorf("%w: backend unavailable", ErrRelationshipDenied)
	ErrRelationshipBudget         = fmt.Errorf("%w: budget exhausted", ErrRelationshipDenied)
	ErrRelationshipCycle          = fmt.Errorf("%w: cycle detected", ErrRelationshipDenied)
	ErrRelationshipAmbiguous      = fmt.Errorf("%w: ambiguous graph", ErrRelationshipDenied)
	ErrRelationshipStale          = fmt.Errorf("%w: stale revision", ErrRelationshipDenied)
	ErrInvalidRelationshipRequest = fmt.Errorf("%w: invalid request", ErrRelationshipDenied)
)

// RelationshipRead is one bounded backend response. Revision must identify the
// same immutable snapshot returned by Snapshot. BytesRead is the number of
// backend bytes consumed, including framing not represented by Edges.
type RelationshipRead struct {
	Revision  string
	Edges     []*flowstatev1.RelationshipEdge
	BytesRead uint64
}

// RelationshipBackend is the I/O boundary a relationship resolver refuses to
// serialize. Edges returns only facts whose subject is exactly subject, at the
// requested immutable revision, and must honor limit before doing unbounded
// work. The resolver independently checks the returned result and all budgets.
type RelationshipBackend interface {
	Snapshot(context.Context, string) (string, error)
	Edges(context.Context, *flowstatev1.EntityReference, string, uint32) (RelationshipRead, error)
}

// RelationshipResolver produces a closed, typed policy input. Implementations
// must not expose a general graph query language to policy.
type RelationshipResolver interface {
	Resolve(context.Context, *flowstatev1.RelationshipResolveRequest) (*flowstatev1.RelationshipSet, error)
}

// UseBoundaryAuthorizer is called at a meaningful use boundary, not merely at
// MCP session creation or durable-workload submission. Implementations return
// the relationship snapshot used by that particular decision.
type UseBoundaryAuthorizer interface {
	AuthorizeUse(context.Context, flowstatev1.AuthorizationUseBoundary, *flowstatev1.RelationshipResolveRequest) (*flowstatev1.RelationshipSet, error)
}

// ResolverAuthorizer deliberately performs no caching. Removed membership and
// policy revisions therefore take effect at the next tool call, durable step,
// credential use, secret read, or signal delivery.
type ResolverAuthorizer struct{ Resolver RelationshipResolver }

func (a ResolverAuthorizer) AuthorizeUse(ctx context.Context, boundary flowstatev1.AuthorizationUseBoundary, req *flowstatev1.RelationshipResolveRequest) (*flowstatev1.RelationshipSet, error) {
	if boundary == flowstatev1.AuthorizationUseBoundary_AUTHORIZATION_USE_BOUNDARY_UNSPECIFIED || a.Resolver == nil {
		return nil, ErrInvalidRelationshipRequest
	}
	return a.Resolver.Resolve(ctx, req)
}

// BoundedRelationshipResolver resolves adjacency lists while independently
// bounding depth, breadth, unique entities, bytes, requests, and evaluation
// cost. Every uncertain condition is a denial.
type BoundedRelationshipResolver struct{ Backend RelationshipBackend }

var roleName = regexp.MustCompile(`^role:[a-z][a-z0-9_.-]*$`)

var stableRelations = map[string]struct{}{
	"member_of": {}, "owns": {}, "operates": {}, "parent_of": {}, "delegated_by": {},
}

func validRelation(relation string) bool {
	_, stable := stableRelations[relation]
	return stable || len(relation) <= 128 && roleName.MatchString(relation)
}

type traversalNode struct {
	entity *flowstatev1.EntityReference
	depth  uint32
	path   map[string]struct{}
}

func entityKey(entity *flowstatev1.EntityReference) string {
	return fmt.Sprintf("%s\x00%d\x00%s", entity.GetTenant(), entity.GetKind(), entity.GetId())
}

func validEntity(entity *flowstatev1.EntityReference) bool {
	return entity != nil && entity.GetTenant() != "" && len(entity.GetTenant()) <= 128 &&
		entity.GetKind() > flowstatev1.EntityKind_ENTITY_KIND_UNSPECIFIED &&
		entity.GetKind() <= flowstatev1.EntityKind_ENTITY_KIND_PROTECTED_RESOURCE &&
		entity.GetId() != "" && len(entity.GetId()) <= 512
}

func (r BoundedRelationshipResolver) Resolve(ctx context.Context, req *flowstatev1.RelationshipResolveRequest) (*flowstatev1.RelationshipSet, error) {
	if r.Backend == nil || req == nil || !validEntity(req.GetSubject()) || req.GetLimits() == nil {
		return nil, ErrInvalidRelationshipRequest
	}
	limits := req.GetLimits()
	if limits.GetMaxDepth() == 0 || limits.GetMaxFanOut() == 0 || limits.GetMaxEntitiesRead() == 0 || limits.GetMaxBytesRead() == 0 || limits.GetMaxBackendRequests() == 0 || limits.GetMaxEvaluationCost() == 0 || len(req.GetRelations()) == 0 || len(req.GetRelations()) > 32 {
		return nil, ErrInvalidRelationshipRequest
	}
	relations := make(map[string]struct{}, len(req.GetRelations()))
	for _, relation := range req.GetRelations() {
		if !validRelation(relation) {
			return nil, ErrInvalidRelationshipRequest
		}
		if _, exists := relations[relation]; exists {
			return nil, ErrRelationshipAmbiguous
		}
		relations[relation] = struct{}{}
	}
	if limits.GetMaxBackendRequests() < 1 {
		return nil, ErrRelationshipBudget
	}
	revision, err := r.Backend.Snapshot(ctx, req.GetSubject().GetTenant())
	if err != nil || revision == "" {
		return nil, fmt.Errorf("%w: snapshot: %v", ErrRelationshipBackend, err)
	}
	if req.GetRequireFresh() && (req.GetRequiredRevision() == "" || revision != req.GetRequiredRevision()) {
		return nil, ErrRelationshipStale
	}
	requests, cost := uint32(1), uint64(1)
	root := proto.Clone(req.GetSubject()).(*flowstatev1.EntityReference)
	rootKey := entityKey(root)
	queue := []traversalNode{{entity: root, path: map[string]struct{}{rootKey: {}}}}
	seen := map[string]*flowstatev1.EntityReference{rootKey: root}
	result := &flowstatev1.RelationshipSet{Revision: revision, Entities: []*flowstatev1.EntityReference{root}}
	edgeSeen := make(map[string]struct{})
	var bytesRead uint64

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		if requests == limits.GetMaxBackendRequests() {
			return nil, ErrRelationshipBudget
		}
		read, readErr := r.Backend.Edges(ctx, node.entity, revision, limits.GetMaxFanOut()+1)
		requests++
		if readErr != nil {
			return nil, fmt.Errorf("%w: edges: %v", ErrRelationshipBackend, readErr)
		}
		if read.Revision != revision {
			return nil, ErrRelationshipStale
		}
		if uint32(len(read.Edges)) > limits.GetMaxFanOut() {
			return nil, ErrRelationshipBudget
		}
		encodedBytes := uint64(0)
		for _, edge := range read.Edges {
			encodedBytes += uint64(proto.Size(edge))
		}
		if read.BytesRead > encodedBytes {
			encodedBytes = read.BytesRead
		}
		if encodedBytes > limits.GetMaxBytesRead()-min(bytesRead, limits.GetMaxBytesRead()) {
			return nil, ErrRelationshipBudget
		}
		bytesRead += encodedBytes

		for _, edge := range read.Edges {
			cost++
			if cost > limits.GetMaxEvaluationCost() {
				return nil, ErrRelationshipBudget
			}
			if edge == nil || !validEntity(edge.GetSubject()) || !validEntity(edge.GetResource()) || !proto.Equal(edge.GetSubject(), node.entity) || edge.GetSubject().GetTenant() != root.GetTenant() || edge.GetResource().GetTenant() != root.GetTenant() || !validRelation(edge.GetRelation()) {
				return nil, ErrRelationshipDenied
			}
			if _, wanted := relations[edge.GetRelation()]; !wanted {
				continue
			}
			edgeKey := entityKey(edge.GetSubject()) + "\x00" + edge.GetRelation() + "\x00" + entityKey(edge.GetResource())
			if _, duplicate := edgeSeen[edgeKey]; duplicate {
				return nil, ErrRelationshipAmbiguous
			}
			edgeSeen[edgeKey] = struct{}{}
			resourceKey := entityKey(edge.GetResource())
			if _, cycle := node.path[resourceKey]; cycle {
				return nil, ErrRelationshipCycle
			}
			if node.depth >= limits.GetMaxDepth() {
				return nil, ErrRelationshipBudget
			}
			result.Edges = append(result.Edges, proto.Clone(edge).(*flowstatev1.RelationshipEdge))
			if _, exists := seen[resourceKey]; exists {
				continue
			}
			if uint32(len(seen)) == limits.GetMaxEntitiesRead() {
				return nil, ErrRelationshipBudget
			}
			resource := proto.Clone(edge.GetResource()).(*flowstatev1.EntityReference)
			seen[resourceKey] = resource
			result.Entities = append(result.Entities, resource)
			path := make(map[string]struct{}, len(node.path)+1)
			for key := range node.path {
				path[key] = struct{}{}
			}
			path[resourceKey] = struct{}{}
			queue = append(queue, traversalNode{entity: resource, depth: node.depth + 1, path: path})
		}
	}
	result.EvaluationCost = cost
	return result, nil
}

// Has reports whether the resolved, bounded set contains an exact typed edge.
// It is intentionally a lookup rather than a traversal primitive suitable for
// exposing to CEL.
func Has(set *flowstatev1.RelationshipSet, subject *flowstatev1.EntityReference, relation string, resource *flowstatev1.EntityReference) bool {
	if set == nil || !validRelation(relation) {
		return false
	}
	for _, edge := range set.GetEdges() {
		if edge.GetRelation() == relation && proto.Equal(edge.GetSubject(), subject) && proto.Equal(edge.GetResource(), resource) {
			return true
		}
	}
	return false
}

// HasRole is the resource-specific-role form of Has.
func HasRole(set *flowstatev1.RelationshipSet, subject *flowstatev1.EntityReference, role string, resource *flowstatev1.EntityReference) bool {
	if strings.HasPrefix(role, "role:") {
		return false
	}
	return Has(set, subject, "role:"+role, resource)
}
