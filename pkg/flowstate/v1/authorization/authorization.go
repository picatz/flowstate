// Package authorization evaluates the proto-first authorization model.
package authorization

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	MaxRules             = 256
	MaxValues            = 64
	MaxRelationships     = 512
	MaxRelationshipDepth = 16
	MaxDelegations       = 16
	MaxFieldBytes        = 1024
)

var ErrInvalid = errors.New("invalid authorization input")

// ParsePolicy parses a bounded proto JSON policy. Unknown fields are rejected.
func ParsePolicy(data []byte) (*v1.AuthorizationPolicy, error) {
	if len(data) > 1<<20 {
		return nil, fmt.Errorf("%w: policy exceeds byte limit", ErrInvalid)
	}
	p := new(v1.AuthorizationPolicy)
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(data, p); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if err := validate(nil, p); err != nil {
		return nil, err
	}
	return p, nil
}

func validate(r *v1.AuthorizationRequest, p *v1.AuthorizationPolicy) error {
	if p == nil {
		return fmt.Errorf("%w: missing policy", ErrInvalid)
	}
	n := 0
	for _, l := range append(append([]*v1.AuthorizationPolicyLayer{}, p.GetGrants()...), p.GetBoundaries()...) {
		if l == nil {
			return fmt.Errorf("%w: nil layer", ErrInvalid)
		}
		n += len(l.GetRules())
		for _, x := range l.GetRules() {
			if x == nil || x.GetEffect() == v1.AuthorizationEffect_AUTHORIZATION_EFFECT_UNSPECIFIED {
				return fmt.Errorf("%w: malformed rule", ErrInvalid)
			}
			if ruleValues(x) > MaxValues {
				return fmt.Errorf("%w: rule value limit", ErrInvalid)
			}
		}
	}
	if n > MaxRules || len(p.GetRelationships()) > MaxRelationships {
		return fmt.Errorf("%w: policy limit", ErrInvalid)
	}
	if r != nil {
		if r.GetPrincipal() == nil || r.GetAction() == "" || r.GetResource() == "" || len(r.GetDelegation()) > MaxDelegations {
			return fmt.Errorf("%w: malformed request", ErrInvalid)
		}
		for _, s := range []string{r.GetAction(), r.GetResource(), r.GetPrincipal().GetSubject(), r.GetPrincipal().GetIssuer(), r.GetPrincipal().GetTenant(), r.GetPrincipal().GetAudience(), r.GetPrincipal().GetProofKey()} {
			if len(s) > MaxFieldBytes {
				return fmt.Errorf("%w: field limit", ErrInvalid)
			}
		}
	}
	return nil
}
func ruleValues(r *v1.AuthorizationRule) int {
	return len(r.GetSubjects()) + len(r.GetIssuers()) + len(r.GetTenants()) + len(r.GetAudiences()) + len(r.GetActions()) + len(r.GetResources()) + len(r.GetProofKeys()) + len(r.GetActorChains()) + len(r.GetContext()) + len(r.GetAttestedFacts())
}

// ReferenceEvaluate is the deliberately direct specification evaluator. It scans
// every rule and edge and shares no compiled representation with Evaluator.
func ReferenceEvaluate(req *v1.AuthorizationRequest, p *v1.AuthorizationPolicy) (*v1.AuthorizationDecision, error) {
	if err := validate(req, p); err != nil {
		return deny("invalid input", false), err
	}
	if !referenceDelegation(req) {
		return deny("delegation does not authorize request", false), nil
	}
	facts, ok := referenceFacts(req)
	if !ok {
		return deny("malformed attested facts", false), nil
	}
	grant, explicit := referenceLayers(req, p.GetGrants(), p.GetRelationships(), facts)
	if explicit {
		return deny("explicit deny", true), nil
	}
	if !grant {
		return deny("no grant", false), nil
	}
	for _, b := range p.GetBoundaries() {
		allow, d := referenceLayer(req, b, p.GetRelationships(), facts)
		if d {
			return deny("explicit deny", true), nil
		}
		if !allow {
			return deny("maximum-permission boundary", false), nil
		}
	}
	return &v1.AuthorizationDecision{Allowed: true, Reason: "grant"}, nil
}
func referenceLayers(q *v1.AuthorizationRequest, ls []*v1.AuthorizationPolicyLayer, edges []*v1.Relationship, f map[string]string) (bool, bool) {
	a := false
	for _, l := range ls {
		x, d := referenceLayer(q, l, edges, f)
		a = a || x
		if d {
			return false, true
		}
	}
	return a, false
}
func referenceLayer(q *v1.AuthorizationRequest, l *v1.AuthorizationPolicyLayer, e []*v1.Relationship, f map[string]string) (bool, bool) {
	a := false
	for _, r := range l.GetRules() {
		if referenceMatch(q, r, e, f) {
			if r.GetEffect() == v1.AuthorizationEffect_AUTHORIZATION_EFFECT_DENY {
				return false, true
			}
			if r.GetEffect() == v1.AuthorizationEffect_AUTHORIZATION_EFFECT_ALLOW {
				a = true
			}
		}
	}
	return a, false
}
func referenceMatch(q *v1.AuthorizationRequest, r *v1.AuthorizationRule, e []*v1.Relationship, f map[string]string) bool {
	p := q.GetPrincipal()
	if !one(r.GetSubjects(), p.GetSubject()) || !one(r.GetIssuers(), p.GetIssuer()) || !one(r.GetTenants(), p.GetTenant()) || !one(r.GetAudiences(), p.GetAudience()) || !one(r.GetActions(), q.GetAction()) || !one(r.GetResources(), q.GetResource()) || !one(r.GetProofKeys(), p.GetProofKey()) || !one(r.GetActorChains(), strings.Join(p.GetActorChain(), ">")) {
		return false
	}
	for k, v := range r.GetContext() {
		if q.GetContext()[k] != v {
			return false
		}
	}
	for k, v := range r.GetAttestedFacts() {
		if f[k] != v {
			return false
		}
	}
	return r.GetRelation() == "" || referenceReach(p.GetSubject(), r.GetRelation(), q.GetResource(), e)
}
func referenceReach(s, rel, target string, e []*v1.Relationship) bool {
	seen := map[string]bool{s: true}
	q := []string{s}
	for depth := 0; depth < MaxRelationshipDepth && len(q) > 0; depth++ {
		next := []string{}
		for _, at := range q {
			for _, x := range e {
				if x.GetSubject() == at && x.GetRelation() == rel {
					if x.GetResource() == target {
						return true
					}
					if !seen[x.GetResource()] {
						seen[x.GetResource()] = true
						next = append(next, x.GetResource())
					}
				}
			}
		}
		q = next
	}
	return false
}
func referenceFacts(q *v1.AuthorizationRequest) (map[string]string, bool) {
	m := map[string]string{}
	for _, f := range q.GetFacts() {
		if f == nil || !f.GetAttested() || f.GetName() == "" {
			return nil, false
		}
		if _, ok := m[f.GetName()]; ok {
			return nil, false
		}
		m[f.GetName()] = f.GetValue()
	}
	return m, true
}
func referenceDelegation(q *v1.AuthorizationRequest) bool {
	ds := q.GetDelegation()
	if len(ds) == 0 {
		return true
	}
	chain := append([]string{}, q.GetPrincipal().GetActorChain()...)
	chain = append(chain, q.GetPrincipal().GetSubject())
	if len(chain) != len(ds)+1 {
		return false
	}
	for i, d := range ds {
		if d == nil || d.GetDelegator() != chain[i] || d.GetDelegate() != chain[i+1] || !one(d.GetActions(), q.GetAction()) || !one(d.GetResources(), q.GetResource()) {
			return false
		}
	}
	return true
}
func one(xs []string, v string) bool {
	if len(xs) == 0 {
		return true
	}
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}
func deny(reason string, explicit bool) *v1.AuthorizationDecision {
	return &v1.AuthorizationDecision{Reason: reason, ExplicitDeny: explicit}
}

// Evaluator is the production evaluator. Construction validates and indexes
// relationships; Evaluate remains deterministic and safe for concurrent use.
type Evaluator struct {
	policy    *v1.AuthorizationPolicy
	adjacency map[string]map[string][]string
}

func NewEvaluator(p *v1.AuthorizationPolicy) (*Evaluator, error) {
	if err := validate(nil, p); err != nil {
		return nil, err
	}
	x := &Evaluator{policy: p, adjacency: map[string]map[string][]string{}}
	for _, e := range p.GetRelationships() {
		if x.adjacency[e.GetRelation()] == nil {
			x.adjacency[e.GetRelation()] = map[string][]string{}
		}
		x.adjacency[e.GetRelation()][e.GetSubject()] = append(x.adjacency[e.GetRelation()][e.GetSubject()], e.GetResource())
	}
	return x, nil
}
func (e *Evaluator) Evaluate(q *v1.AuthorizationRequest) (*v1.AuthorizationDecision, error) {
	if err := validate(q, e.policy); err != nil {
		return deny("invalid input", false), err
	}
	if !productionDelegation(q) {
		return deny("delegation does not authorize request", false), nil
	}
	facts, ok := productionFacts(q)
	if !ok {
		return deny("malformed attested facts", false), nil
	}
	allowed, denied := e.combine(q, e.policy.GetGrants(), facts)
	if denied {
		return deny("explicit deny", true), nil
	}
	if !allowed {
		return deny("no grant", false), nil
	}
	for _, boundary := range e.policy.GetBoundaries() {
		a, d := e.combine(q, []*v1.AuthorizationPolicyLayer{boundary}, facts)
		if d {
			return deny("explicit deny", true), nil
		}
		if !a {
			return deny("maximum-permission boundary", false), nil
		}
	}
	return &v1.AuthorizationDecision{Allowed: true, Reason: "grant"}, nil
}
func (e *Evaluator) combine(q *v1.AuthorizationRequest, layers []*v1.AuthorizationPolicyLayer, facts map[string]string) (bool, bool) {
	allow := false
	for _, layer := range layers {
		for _, rule := range layer.GetRules() {
			if !e.matches(q, rule, facts) {
				continue
			}
			switch rule.GetEffect() {
			case v1.AuthorizationEffect_AUTHORIZATION_EFFECT_DENY:
				return false, true
			case v1.AuthorizationEffect_AUTHORIZATION_EFFECT_ALLOW:
				allow = true
			}
		}
	}
	return allow, false
}
func (e *Evaluator) matches(q *v1.AuthorizationRequest, r *v1.AuthorizationRule, facts map[string]string) bool {
	p := q.GetPrincipal()
	selectors := []struct {
		set   []string
		value string
	}{{r.GetSubjects(), p.GetSubject()}, {r.GetIssuers(), p.GetIssuer()}, {r.GetTenants(), p.GetTenant()}, {r.GetAudiences(), p.GetAudience()}, {r.GetActions(), q.GetAction()}, {r.GetResources(), q.GetResource()}, {r.GetProofKeys(), p.GetProofKey()}, {r.GetActorChains(), strings.Join(p.GetActorChain(), ">")}}
	for _, s := range selectors {
		if len(s.set) > 0 && !slices.Contains(s.set, s.value) {
			return false
		}
	}
	for k, v := range r.GetContext() {
		if q.GetContext()[k] != v {
			return false
		}
	}
	for k, v := range r.GetAttestedFacts() {
		if facts[k] != v {
			return false
		}
	}
	if r.GetRelation() == "" {
		return true
	}
	seen := map[string]struct{}{p.GetSubject(): {}}
	frontier := []string{p.GetSubject()}
	graph := e.adjacency[r.GetRelation()]
	for depth := 0; depth < MaxRelationshipDepth && len(frontier) > 0; depth++ {
		next := []string{}
		for _, from := range frontier {
			for _, to := range graph[from] {
				if to == q.GetResource() {
					return true
				}
				if _, ok := seen[to]; !ok {
					seen[to] = struct{}{}
					next = append(next, to)
				}
			}
		}
		frontier = next
	}
	return false
}
func productionFacts(q *v1.AuthorizationRequest) (map[string]string, bool) {
	m := make(map[string]string, len(q.GetFacts()))
	for _, f := range q.GetFacts() {
		if f == nil || !f.GetAttested() || f.GetName() == "" {
			return nil, false
		}
		if _, exists := m[f.GetName()]; exists {
			return nil, false
		}
		m[f.GetName()] = f.GetValue()
	}
	return m, true
}
func productionDelegation(q *v1.AuthorizationRequest) bool {
	ds := q.GetDelegation()
	if len(ds) == 0 {
		return true
	}
	actors := q.GetPrincipal().GetActorChain()
	if len(actors)+1 != len(ds)+1 {
		return false
	}
	from := append(append([]string{}, actors...), q.GetPrincipal().GetSubject())
	for i, d := range ds {
		if d == nil || d.GetDelegator() != from[i] || d.GetDelegate() != from[i+1] || !selectorAllows(d.GetActions(), q.GetAction()) || !selectorAllows(d.GetResources(), q.GetResource()) {
			return false
		}
	}
	return true
}
func selectorAllows(set []string, value string) bool {
	return len(set) == 0 || slices.Contains(set, value)
}

// Surface names each adapter whose canonical request must have identical semantics.
type Surface string

const (
	Connect  Surface = "connect"
	MCP      Surface = "mcp"
	API      Surface = "api"
	Plugin   Surface = "plugin"
	Internal Surface = "internal"
)

func (e *Evaluator) EvaluateSurface(_ Surface, q *v1.AuthorizationRequest) (*v1.AuthorizationDecision, error) {
	return e.Evaluate(q)
}

// PublicError conceals existence without changing or re-evaluating the decision.
func PublicError(d *v1.AuthorizationDecision, resourceExists bool) error {
	if d.GetAllowed() {
		return nil
	}
	if !resourceExists {
		return errors.New("not found")
	}
	return errors.New("permission denied")
}

// RenderAudit returns stable, bounded JSON and never includes credentials.
func RenderAudit(q *v1.AuthorizationRequest, d *v1.AuthorizationDecision) ([]byte, error) {
	if q == nil || d == nil {
		return nil, ErrInvalid
	}
	m := map[string]any{"subject": q.GetPrincipal().GetSubject(), "action": q.GetAction(), "resource": q.GetResource(), "allowed": d.GetAllowed(), "explicit_deny": d.GetExplicitDeny(), "reason": d.GetReason()}
	keys := make([]string, 0, len(q.GetContext()))
	for k := range q.GetContext() {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	m["context_keys"] = keys
	b, err := json.Marshal(m)
	if len(b) > 16<<10 {
		return nil, ErrInvalid
	}
	return b, err
}
