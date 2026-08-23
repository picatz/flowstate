package flowstatev1

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
)

const (
	AuthorizationSchemaVersion = 1
	FlowstateAPIType           = "flowstate_api_v1"
	MaxAuthorizationDetails    = 64 << 10
	MaxAuthorizationIntents    = 64
)

var ErrInvalidAuthorizationDetails = errors.New("auth: invalid authorization_details")

// ScopeActions gives scopes a closed meaning when scopes and RAR are combined.
// Every RAR action must be authorized by at least one requested scope; the two
// representations are an intersection, never a union. An empty map means scopes
// and authorization_details may not be combined.
type ScopeActions map[string][]string

type oauthAuthorizationDetail struct {
	Type         string   `json:"type"`
	Locations    []string `json:"locations"`
	Actions      []string `json:"actions"`
	Identifier   string   `json:"identifier,omitempty"`
	Methods      []string `json:"methods,omitempty"`
	PathPrefixes []string `json:"path_prefixes,omitempty"`
	Critical     bool     `json:"critical,omitempty"`
}

// AuthorizationDetailsToRequest converts the supported RAR type to canonical
// PARC intents. Unknown non-critical types are ignored; an unknown critical type
// is refused. Duplicate semantic intents are refused rather than merged.
func AuthorizationDetailsToRequest(raw []byte, scopes []string, meanings ScopeActions) (*AuthorizationRequest, error) {
	if len(raw) == 0 || len(raw) > MaxAuthorizationDetails {
		return nil, fmt.Errorf("%w: JSON must contain 1..%d bytes", ErrInvalidAuthorizationDetails, MaxAuthorizationDetails)
	}
	var entries []json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(&entries); err != nil || len(entries) == 0 || len(entries) > MaxAuthorizationIntents {
		return nil, fmt.Errorf("%w: expected 1..%d entries", ErrInvalidAuthorizationDetails, MaxAuthorizationIntents)
	}
	if dec.Decode(new(any)) == nil {
		return nil, fmt.Errorf("%w: trailing JSON", ErrInvalidAuthorizationDetails)
	}

	allowed, err := actionsForScopes(scopes, meanings)
	if err != nil {
		return nil, err
	}
	request := &AuthorizationRequest{SchemaVersion: AuthorizationSchemaVersion}
	seen := map[string]struct{}{}
	for i, encoded := range entries {
		var header struct {
			Type     string `json:"type"`
			Critical bool   `json:"critical"`
		}
		if err := json.Unmarshal(encoded, &header); err != nil || header.Type == "" {
			return nil, fmt.Errorf("%w: entry %d has no valid type", ErrInvalidAuthorizationDetails, i)
		}
		if header.Type != FlowstateAPIType {
			if header.Critical {
				return nil, fmt.Errorf("%w: entry %d has unknown critical type %q", ErrInvalidAuthorizationDetails, i, header.Type)
			}
			continue
		}
		var detail oauthAuthorizationDetail
		decoder := json.NewDecoder(bytes.NewReader(encoded))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&detail); err != nil {
			return nil, fmt.Errorf("%w: entry %d: %v", ErrInvalidAuthorizationDetails, i, err)
		}
		if err := validateDetail(&detail); err != nil {
			return nil, fmt.Errorf("%w: entry %d: %v", ErrInvalidAuthorizationDetails, i, err)
		}
		for _, location := range detail.Locations {
			for _, action := range detail.Actions {
				if allowed != nil {
					if _, ok := allowed[action]; !ok {
						return nil, fmt.Errorf("%w: action %q is not permitted by the requested scopes", ErrInvalidAuthorizationDetails, action)
					}
				}
				intent := &AuthorizationIntent{Action: action, Resource: &AuthorizationResource{Uri: location, Identifier: detail.Identifier}}
				if len(detail.Methods)+len(detail.PathPrefixes) > 0 {
					intent.Constraints = &AuthorizationConstraints{Methods: slices.Clone(detail.Methods), PathPrefixes: slices.Clone(detail.PathPrefixes)}
				}
				canonicalizeIntent(intent)
				key := intentKey(intent)
				if _, ok := seen[key]; ok {
					return nil, fmt.Errorf("%w: duplicate semantic entry", ErrInvalidAuthorizationDetails)
				}
				seen[key] = struct{}{}
				request.Intents = append(request.Intents, intent)
				if len(request.Intents) > MaxAuthorizationIntents {
					return nil, fmt.Errorf("%w: expansion exceeds %d intents", ErrInvalidAuthorizationDetails, MaxAuthorizationIntents)
				}
			}
		}
	}
	if len(request.Intents) == 0 {
		return nil, fmt.Errorf("%w: no supported entries", ErrInvalidAuthorizationDetails)
	}
	slices.SortFunc(request.Intents, func(a, b *AuthorizationIntent) int { return strings.Compare(intentKey(a), intentKey(b)) })
	return request, nil
}

func actionsForScopes(scopes []string, meanings ScopeActions) (map[string]struct{}, error) {
	if len(scopes) == 0 {
		return nil, nil
	}
	if len(meanings) == 0 {
		return nil, fmt.Errorf("%w: scopes and authorization_details cannot be combined without an explicit scope mapping", ErrInvalidAuthorizationDetails)
	}
	allowed := map[string]struct{}{}
	for _, scope := range scopes {
		actions, ok := meanings[scope]
		if !ok {
			return nil, fmt.Errorf("%w: scope %q has no action mapping", ErrInvalidAuthorizationDetails, scope)
		}
		for _, action := range actions {
			allowed[action] = struct{}{}
		}
	}
	return allowed, nil
}

func validateDetail(d *oauthAuthorizationDetail) error {
	if len(d.Locations) == 0 || len(d.Locations) > 16 || len(d.Actions) == 0 || len(d.Actions) > 16 || len(d.Identifier) > 256 || len(d.Methods) > 16 || len(d.PathPrefixes) > 32 {
		return errors.New("locations, actions, identifier, or constraints exceed their bounds")
	}
	for i, location := range d.Locations {
		u, err := url.Parse(location)
		if err != nil || !u.IsAbs() || u.Host == "" || u.Fragment != "" || u.String() != location {
			return fmt.Errorf("location %d is not a canonical absolute resource URI", i)
		}
	}
	for _, action := range d.Actions {
		if !validToken(action, 128) {
			return fmt.Errorf("invalid action %q", action)
		}
	}
	for _, method := range d.Methods {
		if !validToken(method, 32) || method != strings.ToUpper(method) {
			return fmt.Errorf("invalid canonical method %q", method)
		}
	}
	for _, prefix := range d.PathPrefixes {
		if len(prefix) == 0 || len(prefix) > 512 || prefix[0] != '/' || strings.Contains(prefix, "..") {
			return fmt.Errorf("invalid path prefix %q", prefix)
		}
	}
	if hasDuplicate(d.Locations) || hasDuplicate(d.Actions) || hasDuplicate(d.Methods) || hasDuplicate(d.PathPrefixes) {
		return errors.New("duplicate value")
	}
	return nil
}

func validToken(s string, limit int) bool {
	if len(s) == 0 || len(s) > limit {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("._:-", r)) {
			return false
		}
	}
	return true
}
func hasDuplicate(v []string) bool {
	seen := map[string]bool{}
	for _, x := range v {
		if seen[x] {
			return true
		}
		seen[x] = true
	}
	return false
}

func canonicalizeIntent(i *AuthorizationIntent) {
	if c := i.GetConstraints(); c != nil {
		slices.Sort(c.Methods)
		slices.Sort(c.PathPrefixes)
	}
}
func intentKey(i *AuthorizationIntent) string {
	b, _ := json.Marshal([]any{i.GetResource().GetUri(), i.GetResource().GetIdentifier(), i.GetAction(), i.GetConstraints().GetMethods(), i.GetConstraints().GetPathPrefixes()})
	return string(b)
}

// AuthorizationRequestToDetails emits only the closed Flowstate RAR subset.
// Its grouping and ordering are deterministic, so equivalent intent has one
// wire representation suitable for request signing and cache binding.
func AuthorizationRequestToDetails(request *AuthorizationRequest) ([]byte, error) {
	if request == nil || request.GetSchemaVersion() != AuthorizationSchemaVersion || len(request.GetIntents()) == 0 || len(request.GetIntents()) > MaxAuthorizationIntents {
		return nil, fmt.Errorf("%w: unsupported or empty PARC request", ErrInvalidAuthorizationDetails)
	}
	type group struct {
		detail  oauthAuthorizationDetail
		actions map[string]bool
	}
	groups := map[string]*group{}
	for _, intent := range request.GetIntents() {
		if intent.GetResource() == nil {
			return nil, fmt.Errorf("%w: missing resource", ErrInvalidAuthorizationDetails)
		}
		d := oauthAuthorizationDetail{Type: FlowstateAPIType, Locations: []string{intent.GetResource().GetUri()}, Identifier: intent.GetResource().GetIdentifier(), Methods: slices.Clone(intent.GetConstraints().GetMethods()), PathPrefixes: slices.Clone(intent.GetConstraints().GetPathPrefixes())}
		if !validToken(intent.GetAction(), 128) {
			return nil, fmt.Errorf("%w: invalid action", ErrInvalidAuthorizationDetails)
		}
		if err := validateDetail(&oauthAuthorizationDetail{Type: d.Type, Locations: d.Locations, Actions: []string{intent.GetAction()}, Identifier: d.Identifier, Methods: d.Methods, PathPrefixes: d.PathPrefixes}); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidAuthorizationDetails, err)
		}
		keyBytes, _ := json.Marshal([]any{d.Locations[0], d.Identifier, d.Methods, d.PathPrefixes})
		key := string(keyBytes)
		g := groups[key]
		if g == nil {
			g = &group{detail: d, actions: map[string]bool{}}
			groups[key] = g
		}
		if g.actions[intent.GetAction()] {
			return nil, fmt.Errorf("%w: duplicate intent", ErrInvalidAuthorizationDetails)
		}
		g.actions[intent.GetAction()] = true
	}
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	out := make([]oauthAuthorizationDetail, 0, len(keys))
	for _, k := range keys {
		g := groups[k]
		for a := range g.actions {
			g.detail.Actions = append(g.detail.Actions, a)
		}
		slices.Sort(g.detail.Actions)
		out = append(out, g.detail)
	}
	return json.Marshal(out)
}

// ValidateAuthorizationGrant rejects substitution and any partial response item
// that is not an exact member of the request. Partial grants are allowed only
// when explicitly requested; an empty grant is always refused.
func ValidateAuthorizationGrant(request, grant *AuthorizationRequest, allowPartial bool) error {
	if request == nil || grant == nil || grant.GetSchemaVersion() != request.GetSchemaVersion() || len(grant.GetIntents()) == 0 {
		return fmt.Errorf("%w: empty or version-mismatched grant", ErrInvalidAuthorizationDetails)
	}
	want := map[string]bool{}
	for _, i := range request.GetIntents() {
		want[intentKey(i)] = true
	}
	seen := map[string]bool{}
	for _, i := range grant.GetIntents() {
		k := intentKey(i)
		if !want[k] {
			return fmt.Errorf("%w: response grant is broader than or substituted for the request", ErrInvalidAuthorizationDetails)
		}
		if seen[k] {
			return fmt.Errorf("%w: duplicate grant", ErrInvalidAuthorizationDetails)
		}
		seen[k] = true
	}
	if !allowPartial && len(seen) != len(want) {
		return fmt.Errorf("%w: partial grant", ErrInvalidAuthorizationDetails)
	}
	return nil
}

// AuthorizationBinding is the non-secret canonical digest that must accompany
// every cache key, PoP key/state, delegation boundary, audit record, and
// downstream enforcement decision derived from a grant.
func AuthorizationBinding(grant *AuthorizationRequest) (string, error) {
	b, err := AuthorizationRequestToDetails(grant)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// BoundAuthorization keeps every security-sensitive derivative attached to the
// exact grant that created it. Callers must use CacheKey for credentials,
// ProofState for proof-of-possession state, Delegate before crossing a
// delegation boundary, AuditFields for authorization audit events, and Enforce
// immediately before a downstream request. Keeping these operations on one
// value prevents a call site from accidentally binding one and enforcing
// another.
type BoundAuthorization struct {
	grant  *AuthorizationRequest
	digest string
}

func BindAuthorization(request, grant *AuthorizationRequest, allowPartial bool) (*BoundAuthorization, error) {
	if err := ValidateAuthorizationGrant(request, grant, allowPartial); err != nil {
		return nil, err
	}
	digest, err := AuthorizationBinding(grant)
	if err != nil {
		return nil, err
	}
	return &BoundAuthorization{grant: grant, digest: digest}, nil
}

func (b *BoundAuthorization) CacheKey(base string) string { return base + "|authorization=" + b.digest }
func (b *BoundAuthorization) ProofState(nonce string) string {
	sum := sha256.Sum256([]byte(b.digest + "\x00" + nonce))
	return hex.EncodeToString(sum[:])
}
func (b *BoundAuthorization) AuditFields() map[string]string {
	return map[string]string{"authorization_schema": fmt.Sprint(AuthorizationSchemaVersion), "authorization_binding": b.digest}
}
func (b *BoundAuthorization) Delegate(grant *AuthorizationRequest, allowPartial bool) (*BoundAuthorization, error) {
	return BindAuthorization(b.grant, grant, allowPartial)
}
func (b *BoundAuthorization) Enforce(action, resource, identifier, method, path string) error {
	return EnforceAuthorization(b.grant, action, resource, identifier, method, path)
}

// EnforceAuthorization checks a concrete downstream request against the bound
// grant. The resource origin, action, identifier and constraints must all match.
func EnforceAuthorization(grant *AuthorizationRequest, action, resource, identifier, method, path string) error {
	for _, i := range grant.GetIntents() {
		if i.GetAction() != action || i.GetResource().GetUri() != resource || i.GetResource().GetIdentifier() != identifier {
			continue
		}
		c := i.GetConstraints()
		if c != nil && len(c.Methods) > 0 && !slices.Contains(c.Methods, method) {
			continue
		}
		if c != nil && len(c.PathPrefixes) > 0 {
			ok := false
			for _, p := range c.PathPrefixes {
				if strings.HasPrefix(path, p) {
					ok = true
				}
			}
			if !ok {
				continue
			}
		}
		return nil
	}
	return fmt.Errorf("%w: downstream request is outside the granted authorization", ErrInvalidAuthorizationDetails)
}
