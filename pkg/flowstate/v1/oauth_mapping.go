package flowstatev1

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// ErrUnrepresentable is returned instead of broadening an OAuth request.
var ErrUnrepresentable = errors.New("OAuth grant cannot faithfully represent the PARC request")

// PARCPolicy is the deployment's CEL-backed decision. Mapping authorization
// and policy authorization are deliberately separate conjuncts.
type PARCPolicy func(context.Context, *PARCRequest) (bool, error)

// MappingBoundary holds facts established by the authenticated entry point,
// never facts supplied by the request body.
type MappingBoundary struct {
	Audience, Deployment, Tenant, Resource string
	Delegation                             []string
	Context                                map[string]string
	AssuranceLevel                         uint32
}

// MeaningDigest returns a stable digest of a revision's authorization
// meaning. Operators persist it with the revision number; changing meaning
// without changing that number is rejected by CheckMappingProfile.
func MeaningDigest(r *MappingRevision) (string, error) {
	if r == nil {
		return "", errors.New("nil mapping revision")
	}
	c := proto.Clone(r).(*MappingRevision)
	c.MeaningDigest = ""
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	if err != nil {
		return "", err
	}
	d := sha256.Sum256(b)
	return hex.EncodeToString(d[:]), nil
}

// CheckMappingProfile lints references, directionality, external exposure and
// revision integrity. An action absent from mappings remains internal.
func CheckMappingProfile(p *MappingProfile) error {
	if p == nil || len(p.GetRevisions()) == 0 {
		return errors.New("mapping profile has no revisions")
	}
	var previous uint64
	for _, r := range p.GetRevisions() {
		if r.GetRevision() <= previous {
			return fmt.Errorf("mapping revisions are not strictly increasing at %d", r.GetRevision())
		}
		previous = r.GetRevision()
		digest, err := MeaningDigest(r)
		if err != nil || !strings.EqualFold(digest, r.GetMeaningDigest()) {
			return fmt.Errorf("mapping revision %d meaning_digest does not match its contents", r.GetRevision())
		}
		actions, resources, scopes, details := map[string]bool{}, map[string]bool{}, map[string]bool{}, map[string]bool{}
		for _, x := range r.GetActions() {
			if !addUnique(actions, x.GetName()) {
				return fmt.Errorf("duplicate action %q", x.GetName())
			}
		}
		for _, x := range r.GetResourceTypes() {
			if !addUnique(resources, x.GetName()) {
				return fmt.Errorf("duplicate resource type %q", x.GetName())
			}
		}
		for _, x := range r.GetScopes() {
			if !addUnique(scopes, x.GetName()) {
				return fmt.Errorf("duplicate scope %q", x.GetName())
			}
		}
		for _, x := range r.GetAuthorizationDetailTypes() {
			if !addUnique(details, x.GetType()) {
				return fmt.Errorf("duplicate authorization-detail type %q", x.GetType())
			}
		}
		seenMapping := map[string]bool{}
		for _, m := range r.GetMappings() {
			key := fmt.Sprintf("%d\x00%s\x00%s", m.GetDirectionality(), m.GetAction(), strings.Join(m.GetResourceTypes(), "\x00"))
			if !addUnique(seenMapping, key) {
				return fmt.Errorf("ambiguous duplicate mapping for action %q", m.GetAction())
			}
			if !actions[m.GetAction()] {
				return fmt.Errorf("mapping exposes undeclared internal action %q", m.GetAction())
			}
			if len(m.GetRequiredScopes()) == 0 && len(m.GetAuthorizationDetailTypes()) == 0 {
				return fmt.Errorf("mapping for %q has no explicit external grant", m.GetAction())
			}
			for _, x := range m.GetResourceTypes() {
				if !resources[x] {
					return fmt.Errorf("mapping for %q references unknown resource type %q", m.GetAction(), x)
				}
			}
			for _, x := range m.GetRequiredScopes() {
				if !scopes[x] {
					return fmt.Errorf("mapping for %q references unknown scope %q", m.GetAction(), x)
				}
			}
			for _, x := range m.GetAuthorizationDetailTypes() {
				if !details[x] {
					return fmt.Errorf("mapping for %q references unknown authorization-detail type %q", m.GetAction(), x)
				}
			}
		}
	}
	return nil
}

func addUnique(set map[string]bool, s string) bool {
	if s == "" || set[s] {
		return false
	}
	set[s] = true
	return true
}
func directionAllows(got Directionality, outbound bool) bool {
	return got == Directionality_DIRECTIONALITY_BIDIRECTIONAL || (!outbound && got == Directionality_DIRECTIONALITY_INBOUND) || (outbound && got == Directionality_DIRECTIONALITY_OUTBOUND)
}

// ScopesSupported generates RFC 9728 scopes_supported from the selected
// revision rather than maintaining a second vocabulary.
func ScopesSupported(r *MappingRevision) []string {
	out := make([]string, 0, len(r.GetScopes()))
	for _, s := range r.GetScopes() {
		out = append(out, s.GetName())
	}
	slices.Sort(out)
	return out
}

// ConsentDescriptions generates the human-facing scope descriptions.
func ConsentDescriptions(r *MappingRevision) map[string]*ConsentDescription {
	out := make(map[string]*ConsentDescription, len(r.GetScopes()))
	for _, s := range r.GetScopes() {
		if s.GetConsent() != nil {
			out[s.GetName()] = proto.Clone(s.GetConsent()).(*ConsentDescription)
		}
	}
	return out
}

// AuthorizeInbound verifies the audience, normalizes an OAuth grant into a
// canonical request, applies trusted boundaries, then requires both mapping
// and policy permission.
func AuthorizeInbound(ctx context.Context, r *MappingRevision, expectedAudience, tokenAudience string, grant *OAuthGrant, intended *PARCRequest, b MappingBoundary, policy PARCPolicy) (*PARCRequest, error) {
	if expectedAudience == "" || tokenAudience != expectedAudience || b.Audience != expectedAudience {
		return nil, errors.New("token audience does not name this resource")
	}
	if r == nil || grant == nil || intended == nil {
		return nil, errors.New("inbound authorization is incomplete")
	}
	scopeSet, detailSet := map[string]bool{}, map[string]bool{}
	for _, s := range grant.GetScopes() {
		if !hasScope(r, s) {
			return nil, fmt.Errorf("unknown OAuth scope %q", s)
		}
		scopeSet[s] = true
	}
	constraints := map[string]string{}
	for _, d := range grant.GetAuthorizationDetails() {
		definition := findDetail(r, d.GetType())
		if definition == nil {
			return nil, fmt.Errorf("unknown authorization-detail type %q", d.GetType())
		}
		for field := range d.GetConstraints() {
			if !slices.Contains(definition.GetConstraintFields(), field) {
				return nil, fmt.Errorf("authorization-detail type %q cannot represent constraint %q", d.GetType(), field)
			}
		}
		detailSet[d.GetType()] = true
		for k, v := range d.GetConstraints() {
			constraints[k] = v
		}
	}
	for _, action := range intended.GetActions() {
		for _, resource := range intended.GetResourceTypes() {
			m := findMapping(r, action, resource, false)
			if m == nil || (!allPresent(scopeSet, m.GetRequiredScopes()) && !anyPresent(detailSet, m.GetAuthorizationDetailTypes())) {
				return nil, fmt.Errorf("mapped grant does not permit %s on %s", action, resource)
			}
			if err := checkRequiredContext(m.GetRequiredContext(), b); err != nil {
				return nil, err
			}
		}
	}
	request := proto.Clone(intended).(*PARCRequest)
	request.Deployment, request.Tenant, request.Resource = b.Deployment, b.Tenant, b.Resource
	request.DelegationChain, request.Context, request.AssuranceLevel, request.Constraints = slices.Clone(b.Delegation), cloneMap(b.Context), b.AssuranceLevel, constraints
	if policy == nil {
		return nil, errors.New("no CEL policy configured")
	}
	ok, err := policy(ctx, request)
	if err != nil || !ok {
		if err != nil {
			return nil, fmt.Errorf("CEL policy failed: %w", err)
		}
		return nil, errors.New("CEL policy denied request")
	}
	return request, nil
}

// OutboundGrant computes the exact mapped grant. Required attenuation which
// cannot be carried by an authorization detail is refused.
func OutboundGrant(r *MappingRevision, request *PARCRequest) (*OAuthGrant, error) {
	if r == nil || request == nil {
		return nil, ErrUnrepresentable
	}
	scopes := map[string]bool{}
	for _, action := range request.GetActions() {
		for _, resource := range request.GetResourceTypes() {
			m := findMapping(r, action, resource, true)
			if m == nil {
				return nil, fmt.Errorf("%w: %s on %s has no outbound mapping", ErrUnrepresentable, action, resource)
			}
			for _, a := range m.GetAttenuation() {
				if a.GetRequired() {
					return nil, fmt.Errorf("%w: required constraint %q needs an authorization detail", ErrUnrepresentable, a.GetName())
				}
			}
			if len(m.GetRequiredScopes()) == 0 {
				return nil, fmt.Errorf("%w: mapping has no scope representation", ErrUnrepresentable)
			}
			for _, s := range m.GetRequiredScopes() {
				scopes[s] = true
			}
		}
	}
	out := &OAuthGrant{}
	for s := range scopes {
		out.Scopes = append(out.Scopes, s)
	}
	slices.Sort(out.Scopes)
	return out, nil
}

func findMapping(r *MappingRevision, action, resource string, outbound bool) *ActionMapping {
	for _, m := range r.GetMappings() {
		if m.GetAction() == action && slices.Contains(m.GetResourceTypes(), resource) && directionAllows(m.GetDirectionality(), outbound) {
			return m
		}
	}
	return nil
}
func hasScope(r *MappingRevision, name string) bool {
	for _, s := range r.GetScopes() {
		if s.GetName() == name {
			return true
		}
	}
	return false
}
func findDetail(r *MappingRevision, name string) *AuthorizationDetailType {
	for _, d := range r.GetAuthorizationDetailTypes() {
		if d.GetType() == name {
			return d
		}
	}
	return nil
}
func allPresent(set map[string]bool, values []string) bool {
	for _, x := range values {
		if !set[x] {
			return false
		}
	}
	return len(values) > 0
}
func anyPresent(set map[string]bool, values []string) bool {
	for _, x := range values {
		if set[x] {
			return true
		}
	}
	return false
}
func cloneMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
func checkRequiredContext(r *RequiredContext, b MappingBoundary) error {
	if r == nil {
		return nil
	}
	if b.AssuranceLevel < r.GetMinimumAssuranceLevel() {
		return errors.New("grant does not meet required assurance")
	}
	for _, c := range r.GetClaims() {
		if _, ok := b.Context[c]; !ok {
			return fmt.Errorf("required context %q is absent", c)
		}
	}
	return nil
}

// MappingDocumentation emits a stable Markdown reference from the mapping.
func MappingDocumentation(r *MappingRevision) (string, error) {
	if r == nil {
		return "", errors.New("nil mapping revision")
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# OAuth mapping revision %d\n\n| Scope | Consent | Actions | Resources |\n|---|---|---|---|\n", r.GetRevision())
	for _, s := range r.GetScopes() {
		fmt.Fprintf(&b, "| `%s` | %s | %s | %s |\n", s.GetName(), s.GetConsent().GetSummary(), strings.Join(s.GetActions(), ", "), strings.Join(s.GetResourceTypes(), ", "))
	}
	return b.String(), nil
}

// OAuthClientRequest is the JSON request fragment generated for an OAuth client.
func OAuthClientRequest(r *MappingRevision, request *PARCRequest) ([]byte, error) {
	g, err := OutboundGrant(r, request)
	if err != nil {
		return nil, err
	}
	return protojson.MarshalOptions{UseProtoNames: true}.Marshal(g)
}
