package flowstatev1

import (
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	MaxAuthorizationOperations   = 256
	MaxAuthorizationParameters   = 64
	MaxAuthorizationArtifacts    = 64
	MaxAuthorizationDestinations = 32
	MaxAuthorizationSecrets      = 32
	MaxAuthorizationDependencies = 128
	MaxDelegationDepth           = 16
	MaxCapabilityLifetime        = 15 * time.Minute
)

var (
	ErrAuthorizationDenied = errors.New("authorization denied")
	canonicalMarshal       = proto.MarshalOptions{Deterministic: true}
)

// DigestAuthorizationPlan validates and hashes the deterministic protobuf wire
// encoding. Map insertion order and JSON spelling therefore cannot change what
// was approved; operation order deliberately can.
func DigestAuthorizationPlan(plan *AuthorizationPlan) (string, error) {
	if err := ValidateAuthorizationPlan(plan); err != nil {
		return "", err
	}
	b, err := canonicalMarshal.Marshal(plan)
	if err != nil {
		return "", fmt.Errorf("marshal authorization plan: %w", err)
	}
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// ValidateAuthorizationPlan enforces the bounds and pinning rules that protect
// every consumer, including callers that construct protobufs without a DSL.
func ValidateAuthorizationPlan(plan *AuthorizationPlan) error {
	if plan == nil {
		return errors.New("authorization plan is required")
	}
	if plan.GetTenant() == "" || plan.GetWorkload() == "" || plan.GetAgent() == "" {
		return errors.New("tenant, workload, and agent are required")
	}
	if len(plan.GetNonce()) < 16 || len(plan.GetNonce()) > 64 {
		return errors.New("plan nonce must contain 16..64 bytes")
	}
	if len(plan.GetOperations()) == 0 || len(plan.GetOperations()) > MaxAuthorizationOperations {
		return fmt.Errorf("operations must contain 1..%d entries", MaxAuthorizationOperations)
	}
	if len(plan.GetDependencies()) > MaxAuthorizationDependencies || len(plan.GetDelegationChain()) > MaxDelegationDepth {
		return errors.New("authorization plan exceeds dependency or delegation bound")
	}
	ids := make(map[string]struct{}, len(plan.GetOperations()))
	for i, op := range plan.GetOperations() {
		if op.GetId() == "" || op.GetAction() == "" || op.GetResource() == "" || op.GetSequence() != uint32(i+1) {
			return fmt.Errorf("operation %d must have an id, action, resource, and sequence %d", i, i+1)
		}
		if _, exists := ids[op.GetId()]; exists {
			return fmt.Errorf("duplicate operation id %q", op.GetId())
		}
		ids[op.GetId()] = struct{}{}
		if len(op.GetParameters()) > MaxAuthorizationParameters || len(op.GetArtifacts()) > MaxAuthorizationArtifacts || len(op.GetNetworkDestinations()) > MaxAuthorizationDestinations || len(op.GetSecretReferences()) > MaxAuthorizationSecrets {
			return fmt.Errorf("operation %q exceeds a parameter or resource bound", op.GetId())
		}
		if op.GetHighRisk() && (hasWildcard(op.GetAction()) || hasWildcard(op.GetResource()) || len(op.GetParameters()) == 0) {
			return fmt.Errorf("high-risk operation %q is wildcard or underspecified", op.GetId())
		}
		for _, a := range op.GetArtifacts() {
			if a.GetIdentity() == "" || !validDigest(a.GetDigest()) {
				return fmt.Errorf("operation %q has an unpinned artifact", op.GetId())
			}
		}
		for _, d := range op.GetNetworkDestinations() {
			if d.GetScheme() == "" || d.GetHost() == "" || hasWildcard(d.GetHost()) || net.ParseIP(d.GetHost()) == nil && strings.ContainsAny(d.GetHost(), "/ ") {
				return fmt.Errorf("operation %q has an invalid network destination", op.GetId())
			}
		}
		for _, s := range op.GetSecretReferences() {
			if s.GetProvider() == "" || s.GetName() == "" || s.GetVersion() == "" {
				return fmt.Errorf("operation %q has an unversioned secret reference", op.GetId())
			}
		}
	}
	for _, d := range plan.GetDependencies() {
		if d.GetIdentity() == "" || d.GetRevision() == "" {
			return errors.New("every dependency must have an identity and revision")
		}
	}
	if plan.GetExpiresAt() == nil || !plan.GetExpiresAt().IsValid() || plan.GetMaximumDuration() == nil || plan.GetMaximumDuration().AsDuration() <= 0 {
		return errors.New("expiry and a positive maximum duration are required")
	}
	if plan.GetPolicyRevision() == "" {
		return errors.New("policy revision is required")
	}
	return nil
}

func hasWildcard(s string) bool { return s == "" || s == "*" || strings.ContainsAny(s, "?[") }
func validDigest(s string) bool {
	if !strings.HasPrefix(s, "sha256:") || len(s) != 71 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(s, "sha256:"))
	return err == nil
}

// SignPlanApproval creates an approval whose signature covers every field,
// including the approved digest, policy revision, approver, and expiry.
func SignPlanApproval(plan *AuthorizationPlan, approver string, expires time.Time, key ed25519.PrivateKey, now time.Time) (*PlanApproval, error) {
	digest, err := DigestAuthorizationPlan(plan)
	if err != nil {
		return nil, err
	}
	if approver == "" || !expires.After(now) || expires.After(plan.GetExpiresAt().AsTime()) {
		return nil, errors.New("approval identity or expiry is invalid")
	}
	a := &PlanApproval{PlanDigest: digest, Approver: approver, PolicyRevision: plan.GetPolicyRevision(), ApprovedAt: timestamppb.New(now), ExpiresAt: timestamppb.New(expires)}
	b, err := signingBytes(a, func(m proto.Message) { m.(*PlanApproval).Signature = nil })
	if err != nil {
		return nil, err
	}
	a.Signature = ed25519.Sign(key, b)
	return a, nil
}

func VerifyPlanApproval(plan *AuthorizationPlan, approval *PlanApproval, key ed25519.PublicKey, now time.Time) error {
	if approval == nil || approval.GetExpiresAt() == nil || !approval.GetExpiresAt().AsTime().After(now) {
		return fmt.Errorf("%w: approval expired", ErrAuthorizationDenied)
	}
	d, err := DigestAuthorizationPlan(plan)
	if err != nil {
		return err
	}
	if d != approval.GetPlanDigest() || approval.GetPolicyRevision() != plan.GetPolicyRevision() {
		return fmt.Errorf("%w: approval does not bind this plan and policy", ErrAuthorizationDenied)
	}
	b, err := signingBytes(approval, func(m proto.Message) { m.(*PlanApproval).Signature = nil })
	if err != nil {
		return err
	}
	if !ed25519.Verify(key, b, approval.GetSignature()) {
		return fmt.Errorf("%w: invalid approval signature", ErrAuthorizationDenied)
	}
	return nil
}

// IssueExecutionCapability mints an issuer-signed capability constrained to a
// holder proof key. The capability never embeds the plan or any secret value.
func IssueExecutionCapability(plan *AuthorizationPlan, approval *PlanApproval, approvalKey ed25519.PublicKey, issuerKey ed25519.PrivateKey, audience string, proofKey ed25519.PublicKey, nonce []byte, now, expires time.Time) (*ExecutionCapability, error) {
	if err := VerifyPlanApproval(plan, approval, approvalKey, now); err != nil {
		return nil, err
	}
	if audience == "" || len(proofKey) != ed25519.PublicKeySize || len(nonce) < 16 || len(nonce) > 64 || !expires.After(now) || expires.Sub(now) > MaxCapabilityLifetime || expires.After(approval.GetExpiresAt().AsTime()) {
		return nil, errors.New("capability constraints or lifetime are invalid")
	}
	ab, err := signingBytes(approval, func(m proto.Message) { m.(*PlanApproval).Signature = nil })
	if err != nil {
		return nil, err
	}
	ah := sha256.Sum256(ab)
	c := &ExecutionCapability{PlanDigest: approval.GetPlanDigest(), ApprovalDigest: "sha256:" + hex.EncodeToString(ah[:]), Agent: plan.GetAgent(), Workload: plan.GetWorkload(), Tenant: plan.GetTenant(), Audience: audience, ProofKey: append([]byte(nil), proofKey...), IssuedAt: timestamppb.New(now), ExpiresAt: timestamppb.New(expires), Nonce: append([]byte(nil), nonce...)}
	b, err := signingBytes(c, func(m proto.Message) { m.(*ExecutionCapability).IssuerSignature = nil })
	if err != nil {
		return nil, err
	}
	c.IssuerSignature = ed25519.Sign(issuerKey, b)
	return c, nil
}

// SignOperationAttempt proves possession of the capability's sender key.
func SignOperationAttempt(attempt *OperationAttempt, capability *ExecutionCapability, key ed25519.PrivateKey) error {
	b, err := attemptProofBytes(attempt, capability)
	if err != nil {
		return err
	}
	attempt.HolderProof = ed25519.Sign(key, b)
	return nil
}

func signingBytes(message proto.Message, clear func(proto.Message)) ([]byte, error) {
	clone := proto.Clone(message)
	clear(clone)
	return canonicalMarshal.Marshal(clone)
}
func attemptProofBytes(a *OperationAttempt, c *ExecutionCapability) ([]byte, error) {
	b, err := signingBytes(a, func(m proto.Message) { m.(*OperationAttempt).HolderProof = nil })
	if err != nil {
		return nil, err
	}
	b = append(b, c.GetNonce()...)
	return b, nil
}

type AuthorizationAuditSink interface {
	RecordAuthorization(*AuthorizationAuditRecord)
}

// BoundaryAuthorizer independently verifies a capability and exact operation at
// each effect boundary. Its replay set must be shared by all instances serving
// the same audience in production.
type BoundaryAuthorizer struct {
	IssuerKey ed25519.PublicKey
	Audience  string
	Now       func() time.Time
	Audit     AuthorizationAuditSink
	mu        sync.Mutex
	used      map[string]struct{}
	next      map[string]uint32
}

func (a *BoundaryAuthorizer) Authorize(plan *AuthorizationPlan, capability *ExecutionCapability, attempt *OperationAttempt, correlationID string) error {
	now := time.Now()
	if a.Now != nil {
		now = a.Now()
	}
	a.record(correlationID, attempt, AuthorizationEvent_AUTHORIZATION_EVENT_ATTEMPTED, "", now)
	deny := func(reason string) error {
		a.record(correlationID, attempt, AuthorizationEvent_AUTHORIZATION_EVENT_DENIED, reason, now)
		return fmt.Errorf("%w: %s", ErrAuthorizationDenied, reason)
	}
	if plan == nil || capability == nil || attempt == nil {
		return deny("plan, capability, and attempt are required")
	}
	digest, err := DigestAuthorizationPlan(plan)
	if err != nil {
		return deny(err.Error())
	}
	cb, err := signingBytes(capability, func(m proto.Message) { m.(*ExecutionCapability).IssuerSignature = nil })
	if err != nil || !ed25519.Verify(a.IssuerKey, cb, capability.GetIssuerSignature()) {
		return deny("invalid capability signature")
	}
	if capability.GetExpiresAt() == nil || !capability.GetExpiresAt().AsTime().After(now) || capability.GetAudience() != a.Audience {
		return deny("capability expired or has the wrong audience")
	}
	if digest != capability.GetPlanDigest() || digest != attempt.GetPlanDigest() || capability.GetAgent() != plan.GetAgent() || capability.GetWorkload() != plan.GetWorkload() || capability.GetTenant() != plan.GetTenant() {
		return deny("capability identity or plan mismatch")
	}
	pb, err := attemptProofBytes(attempt, capability)
	if err != nil || !ed25519.Verify(ed25519.PublicKey(capability.GetProofKey()), pb, attempt.GetHolderProof()) {
		return deny("invalid sender proof")
	}
	if attempt.GetAttemptId() == "" {
		return deny("attempt id is required")
	}
	a.mu.Lock()
	if a.used == nil {
		a.used = make(map[string]struct{})
	}
	replayKey := hex.EncodeToString(capability.GetNonce()) + ":" + attempt.GetAttemptId()
	_, reused := a.used[replayKey]
	if !reused {
		a.used[replayKey] = struct{}{}
	}
	a.mu.Unlock()
	if reused {
		return deny("capability attempt was already used")
	}
	op := operationByID(plan, attempt.GetOperationId())
	if op == nil {
		return deny("operation is not in the approved plan")
	}
	if op.GetSequence() != attempt.GetSequence() || op.GetAction() != attempt.GetAction() || op.GetResource() != attempt.GetResource() {
		return deny("operation identity, order, action, or resource changed")
	}
	if !parametersMatch(op.GetParameters(), attempt.GetParameters(), plan.GetTolerances().GetMutableParameterNames()) || !protoSliceEqual(op.GetArtifacts(), attempt.GetArtifacts()) || !protoSliceEqual(op.GetNetworkDestinations(), attempt.GetNetworkDestinations()) || !protoSliceEqual(op.GetSecretReferences(), attempt.GetSecretReferences()) {
		return deny("security-relevant operation parameters changed")
	}
	if attempt.GetPolicyRevision() != plan.GetPolicyRevision() || !protoSliceEqual(plan.GetDependencies(), attempt.GetDependencies()) || !protoSliceEqual(plan.GetDelegationChain(), attempt.GetDelegationChain()) {
		return deny("policy, dependency graph, or delegation changed")
	}
	capabilityKey := hex.EncodeToString(capability.GetNonce())
	a.mu.Lock()
	if a.next == nil {
		a.next = make(map[string]uint32)
	}
	next := a.next[capabilityKey]
	if next == 0 {
		next = 1
	}
	if op.GetCompensationFor() == "" && op.GetSequence() > next {
		a.mu.Unlock()
		return deny("operation was attempted out of approved order")
	}
	// A retry with a new attempt id is accepted without advancing again.
	if op.GetCompensationFor() == "" && op.GetSequence() == next {
		a.next[capabilityKey] = next + 1
	}
	a.mu.Unlock()
	a.record(correlationID, attempt, AuthorizationEvent_AUTHORIZATION_EVENT_ALLOWED, "", now)
	return nil
}

func operationByID(p *AuthorizationPlan, id string) *PlannedOperation {
	for _, op := range p.GetOperations() {
		if op.GetId() == id {
			return op
		}
	}
	return nil
}
func parametersMatch(want, got map[string]string, mutable []string) bool {
	allowed := make(map[string]bool, len(mutable))
	for _, k := range mutable {
		allowed[k] = true
	}
	if len(want) != len(got) {
		return false
	}
	for k, v := range want {
		if !allowed[k] && subtle.ConstantTimeCompare([]byte(v), []byte(got[k])) != 1 {
			return false
		}
		if _, ok := got[k]; !ok {
			return false
		}
	}
	return true
}
func (a *BoundaryAuthorizer) record(c string, at *OperationAttempt, event AuthorizationEvent, reason string, now time.Time) {
	if a.Audit == nil {
		return
	}
	r := &AuthorizationAuditRecord{CorrelationId: c, Event: event, OccurredAt: timestamppb.New(now), Reason: reason}
	if at != nil {
		r.PlanDigest, r.OperationId, r.AttemptId = at.GetPlanDigest(), at.GetOperationId(), at.GetAttemptId()
	}
	a.Audit.RecordAuthorization(r)
}

func protoSliceEqual[T proto.Message](a, b []T) bool {
	return slices.EqualFunc(a, b, func(x, y T) bool { return proto.Equal(x, y) })
}
