// Package policybundle implements the immutable authorization-policy supply
// chain.  The bootstrap RootTrust passed to Manager is deliberately ordinary
// process configuration: bundle rules are never consulted when authorities or
// approval requirements are changed.  This is the boundary that prevents the
// governed policy system from granting itself control of its own trust root.
package policybundle

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/cel-go/cel"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const maxBreakGlassDuration = 4 * time.Hour

var (
	ErrUnavailable       = errors.New("policy store unavailable")
	ErrUnapprovedDigest  = errors.New("applied digest differs from approved digest")
	ErrBootstrapBoundary = errors.New("policy bundles cannot modify the bootstrap root of trust")
)

// Authority is configured outside governed policy. Group supports separation
// of duty: a requirement can demand signers from distinct groups.
type Authority struct {
	PublicKey ed25519.PublicKey
	Group     string
}
type ApprovalRequirement struct{ Threshold, DistinctGroups int }
type RootTrust struct {
	Authorities  map[string]Authority
	Requirements map[string]ApprovalRequirement
}
type Vocabulary struct {
	Actions, Resources, Relations, Claims, Attributes map[string]bool
}

// Canonicalize returns a clone bearing the digest of deterministic protobuf
// bytes. The digest field itself is cleared, avoiding a self-referential hash.
func Canonicalize(in *flowstatev1.PolicyBundle) (*flowstatev1.PolicyBundle, error) {
	if in == nil {
		return nil, errors.New("nil policy bundle")
	}
	b := proto.Clone(in).(*flowstatev1.PolicyBundle)
	b.Digest = ""
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(b)
	if err != nil {
		return nil, fmt.Errorf("canonicalize policy bundle: %w", err)
	}
	b.Digest = flowstatev1.ContentDigest(raw)
	return b, nil
}

// Sign signs only the canonical digest, which already commits to every byte
// and dependency revision.
func Sign(b *flowstatev1.PolicyBundle, authority string, key ed25519.PrivateKey, now time.Time) (*flowstatev1.PolicySignature, error) {
	c, err := Canonicalize(b)
	if err != nil {
		return nil, err
	}
	return &flowstatev1.PolicySignature{BundleDigest: c.Digest, AuthorityId: authority,
		Signature: ed25519.Sign(key, []byte(c.Digest)), SignedAt: timestamppb.New(now)}, nil
}

// Validate performs protobuf schema, CEL type checking, vocabulary,
// relationship, and claim-projection checks. All errors are accumulated.
func Validate(b *flowstatev1.PolicyBundle, vocab Vocabulary) *flowstatev1.PolicyValidationResult {
	result := &flowstatev1.PolicyValidationResult{}
	c, err := Canonicalize(b)
	if err != nil {
		result.Diagnostics = append(result.Diagnostics, diag("bundle", err))
		return result
	}
	result.BundleDigest = c.Digest
	if err := flowstatev1.Validate(c); err != nil {
		result.Diagnostics = append(result.Diagnostics, diag("bundle", err))
	}
	base, err := flowstatev1.DefaultEvaluator().Env()
	if err != nil {
		result.Diagnostics = append(result.Diagnostics, diag("rules", err))
		return result
	}
	env, err := base.Extend(cel.Variable("request", cel.MapType(cel.StringType, cel.DynType)))
	if err != nil {
		result.Diagnostics = append(result.Diagnostics, diag("rules", err))
		return result
	}
	names := map[string]bool{}
	for i, r := range c.Rules {
		field := fmt.Sprintf("rules[%d]", i)
		if names[r.Name] {
			result.Diagnostics = append(result.Diagnostics, diag(field+".name", errors.New("duplicate rule name")))
		}
		names[r.Name] = true
		if !vocab.Actions[r.Action] {
			result.Diagnostics = append(result.Diagnostics, diag(field+".action", fmt.Errorf("unknown action %q", r.Action)))
		}
		if !vocab.Resources[r.Resource] {
			result.Diagnostics = append(result.Diagnostics, diag(field+".resource", fmt.Errorf("unknown resource %q", r.Resource)))
		}
		_, issues := env.Compile(r.Condition)
		if issues != nil && issues.Err() != nil {
			result.Diagnostics = append(result.Diagnostics, diag(field+".condition", issues.Err()))
		}
	}
	for i, rel := range c.Relationships {
		if rel.Subject == "" || rel.Resource == "" || !vocab.Relations[rel.Relation] {
			result.Diagnostics = append(result.Diagnostics, diag(fmt.Sprintf("relationships[%d]", i), errors.New("unknown relation or empty endpoint")))
		}
	}
	seenAttr := map[string]bool{}
	for i, p := range c.ClaimProjections {
		if !vocab.Claims[p.Claim] || !vocab.Attributes[p.Attribute] {
			result.Diagnostics = append(result.Diagnostics, diag(fmt.Sprintf("claim_projections[%d]", i), errors.New("unknown claim or attribute")))
		}
		if seenAttr[p.Attribute] {
			result.Diagnostics = append(result.Diagnostics, diag(fmt.Sprintf("claim_projections[%d].attribute", i), errors.New("attribute has multiple claim projections")))
		}
		seenAttr[p.Attribute] = true
	}
	result.Valid = len(result.Diagnostics) == 0
	return result
}

func diag(field string, err error) *flowstatev1.PolicyDiagnostic {
	return &flowstatev1.PolicyDiagnostic{Field: field, Message: err.Error()}
}

func Decision(ctx context.Context, b *flowstatev1.PolicyBundle, req *flowstatev1.AuthorizationRequest) *flowstatev1.PolicyDecision {
	d := &flowstatev1.PolicyDecision{BundleName: b.GetName(), BundleRevision: b.GetRevision(), BundleDigest: b.GetDigest(), Reason: "no matching allow rule"}
	if req == nil {
		d.Reason = "missing request"
		return d
	}
	activation := map[string]any{"request": map[string]any{"tenant": req.Tenant, "subject": req.Subject, "action": req.Action, "resource": req.Resource, "claims": req.Claims, "attributes": req.Attributes}}
	base, err := flowstatev1.DefaultEvaluator().Env()
	if err != nil {
		d.Reason = err.Error()
		return d
	}
	env, err := base.Extend(cel.Variable("request", cel.MapType(cel.StringType, cel.DynType)))
	if err != nil {
		d.Reason = err.Error()
		return d
	}
	matchedAllow := false
	for _, r := range b.Rules {
		if r.Action != req.Action || r.Resource != req.Resource {
			continue
		}
		ast, issues := env.Compile(r.Condition)
		if issues != nil && issues.Err() != nil {
			d.Reason = "active policy failed to compile"
			return d
		}
		out, err := flowstatev1.DefaultEvaluator().Eval(ctx, env, ast, activation)
		if err != nil {
			d.Reason = "active policy evaluation failed"
			return d
		}
		ok, good := out.Value().(bool)
		if !good || !ok {
			continue
		}
		d.RuleRevisions = append(d.RuleRevisions, r.Name+"@"+r.Revision)
		if !r.Allow {
			d.Reason = "explicit deny"
			return d
		}
		matchedAllow = true
	}
	if matchedAllow {
		d.Allowed = true
		d.Reason = "allowed"
	}
	return d
}

func Simulate(ctx context.Context, b *flowstatev1.PolicyBundle, scenarios []*flowstatev1.PolicyScenario) *flowstatev1.PolicySimulationResult {
	r := &flowstatev1.PolicySimulationResult{BundleDigest: b.GetDigest(), Passed: true}
	for _, s := range scenarios {
		d := Decision(ctx, b, s.Request)
		matched := d.Allowed == s.WantAllowed
		r.Passed = r.Passed && matched
		r.Cases = append(r.Cases, &flowstatev1.PolicySimulationCase{Name: s.Name, Decision: d, MatchedExpectation: matched})
	}
	return r
}

// SemanticDiff runs the old and new bundles over curated or redacted replay
// requests; request payloads are not retained in the result.
func SemanticDiff(ctx context.Context, from, to *flowstatev1.PolicyBundle, requests []*flowstatev1.AuthorizationRequest) *flowstatev1.PolicySemanticDiff {
	d := &flowstatev1.PolicySemanticDiff{FromDigest: from.GetDigest(), ToDigest: to.GetDigest()}
	seenA, seenD := map[string]bool{}, map[string]bool{}
	for _, req := range requests {
		a, z := Decision(ctx, from, req).Allowed, Decision(ctx, to, req).Allowed
		class := &flowstatev1.PolicyRequestClass{Action: req.Action, Resource: req.Resource, Tenant: req.Tenant}
		key := req.Action + "\x00" + req.Resource + "\x00" + req.Tenant
		if !a && z && !seenA[key] {
			d.NewlyAllowed = append(d.NewlyAllowed, class)
			seenA[key] = true
		}
		if a && !z && !seenD[key] {
			d.NewlyDenied = append(d.NewlyDenied, class)
			seenD[key] = true
		}
	}
	return d
}

type Store interface {
	Get(context.Context, string) (*flowstatev1.PolicyBundle, error)
}
type FleetNode struct{ ID, Tenant, AppliedDigest string }
type Manager struct {
	mu               sync.RWMutex
	root             RootTrust
	store            Store
	active, previous *flowstatev1.PolicyBundle
	activation       *flowstatev1.PolicyActivation
	nodes            map[string]*FleetNode
	signatures       map[string][]*flowstatev1.PolicySignature
}

func NewManager(root RootTrust, store Store, nodes []*FleetNode) *Manager {
	m := &Manager{root: root, store: store, nodes: map[string]*FleetNode{}, signatures: map[string][]*flowstatev1.PolicySignature{}}
	for _, n := range nodes {
		c := *n
		m.nodes[n.ID] = &c
	}
	return m
}

func (m *Manager) Approve(b *flowstatev1.PolicyBundle, sigs []*flowstatev1.PolicySignature, now time.Time) (*flowstatev1.PolicyApproval, error) {
	c, err := Canonicalize(b)
	if err != nil {
		return nil, err
	}
	req, ok := m.root.Requirements[c.PolicyClass]
	if !ok {
		return nil, fmt.Errorf("no bootstrap approval requirement for class %q", c.PolicyClass)
	}
	ids, groups := []string{}, map[string]bool{}
	seen := map[string]bool{}
	for _, s := range sigs {
		a, ok := m.root.Authorities[s.AuthorityId]
		if !ok || seen[s.AuthorityId] || s.BundleDigest != c.Digest || !ed25519.Verify(a.PublicKey, []byte(c.Digest), s.Signature) {
			continue
		}
		seen[s.AuthorityId] = true
		ids = append(ids, s.AuthorityId)
		groups[a.Group] = true
	}
	if len(ids) < req.Threshold || len(groups) < req.DistinctGroups {
		return nil, fmt.Errorf("approval threshold not met: got %d signers in %d groups", len(ids), len(groups))
	}
	slices.Sort(ids)
	cloned := make([]*flowstatev1.PolicySignature, 0, len(sigs))
	for _, s := range sigs {
		cloned = append(cloned, proto.Clone(s).(*flowstatev1.PolicySignature))
	}
	m.signatures[c.Digest] = cloned
	return &flowstatev1.PolicyApproval{BundleDigest: c.Digest, AuthorityIds: ids, ApprovedAt: timestamppb.New(now), ApprovalId: c.Digest + ":" + strings.Join(ids, ",")}, nil
}

func (m *Manager) Apply(ctx context.Context, approval *flowstatev1.PolicyApproval, activation *flowstatev1.PolicyActivation, now time.Time) error {
	if approval == nil || activation == nil {
		return errors.New("approval and activation are required")
	}
	if approval.BundleDigest != activation.ApprovedDigest || activation.AppliedDigest != approval.BundleDigest {
		return ErrUnapprovedDigest
	}
	b, err := m.store.Get(ctx, activation.AppliedDigest)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	c, err := Canonicalize(b)
	if err != nil {
		return err
	}
	if c.Digest != approval.BundleDigest {
		return ErrUnapprovedDigest
	}
	// Re-evaluate signatures against the current external root. Removing a signer
	// after review therefore invalidates the approval rather than being grandfathered.
	if _, err = m.Approve(c, m.signatures[c.Digest], now); err != nil {
		return fmt.Errorf("approval no longer valid: %w", err)
	}
	if activation.StartsAt != nil && now.Before(activation.StartsAt.AsTime()) {
		return errors.New("activation has not started")
	}
	if activation.ExpiresAt == nil || !activation.ExpiresAt.IsValid() || !now.Before(activation.ExpiresAt.AsTime()) {
		return errors.New("activation is expired")
	}
	if activation.BreakGlass {
		if strings.TrimSpace(activation.AuditEvidence) == "" {
			return errors.New("break-glass activation requires audit evidence")
		}
		if activation.ExpiresAt.AsTime().Sub(now) > maxBreakGlassDuration {
			return errors.New("break-glass expiry exceeds four hours")
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.previous = m.active
	m.active = c
	m.activation = proto.Clone(activation).(*flowstatev1.PolicyActivation)
	for _, n := range m.nodes {
		if selected(n, activation) {
			n.AppliedDigest = c.Digest
		}
	}
	return nil
}

func selected(n *FleetNode, a *flowstatev1.PolicyActivation) bool {
	if len(a.Tenants) > 0 && !slices.Contains(a.Tenants, n.Tenant) {
		return false
	}
	if a.CanaryPercent >= 100 {
		return true
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(n.ID))
	return h.Sum32()%100 < uint32(a.CanaryPercent)
}

// Decision fails closed on store errors, expiry, and nodes not yet carrying the
// active digest. This makes partial fleet rollout visible rather than allowing
// an old policy to answer as though rollout had completed.
func (m *Manager) Decision(ctx context.Context, nodeID string, req *flowstatev1.AuthorizationRequest, now time.Time) *flowstatev1.PolicyDecision {
	m.mu.RLock()
	a := m.activation
	active := m.active
	n := m.nodes[nodeID]
	m.mu.RUnlock()
	deny := func(reason string) *flowstatev1.PolicyDecision { return &flowstatev1.PolicyDecision{Reason: reason} }
	if a == nil || active == nil {
		return deny("no active policy")
	}
	if a.ExpiresAt == nil || !now.Before(a.ExpiresAt.AsTime()) {
		return deny("active policy expired")
	}
	if n == nil || n.AppliedDigest != active.Digest {
		return deny("policy rollout incomplete on node")
	}
	b, err := m.store.Get(ctx, active.Digest)
	if err != nil {
		return deny("policy store unavailable")
	}
	c, err := Canonicalize(b)
	if err != nil || c.Digest != active.Digest {
		return deny("active policy integrity failure")
	}
	return Decision(ctx, c, req)
}

func (m *Manager) ObserveSafetySignal(signal *flowstatev1.PolicySafetySignal, now time.Time) (*flowstatev1.PolicyRollback, error) {
	if signal == nil || signal.Value < signal.RollbackThreshold {
		return nil, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.activation == nil || m.previous == nil {
		return nil, errors.New("no rollback target")
	}
	r := &flowstatev1.PolicyRollback{ActivationId: m.activation.ActivationId, FromDigest: m.active.Digest, ToDigest: m.previous.Digest, SafetySignal: signal.Name, RolledBackAt: timestamppb.New(now)}
	m.active, m.previous = m.previous, m.active
	for _, n := range m.nodes {
		if n.AppliedDigest == r.FromDigest {
			n.AppliedDigest = r.ToDigest
		}
	}
	return r, nil
}

// UpdateRoot always refuses: only the deployment's bootstrap configuration may
// replace author keys or class requirements.
func (m *Manager) UpdateRoot(_ *flowstatev1.PolicyBundle) error { return ErrBootstrapBoundary }
