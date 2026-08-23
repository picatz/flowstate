package auth

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

const (
	MaxProjectedString    = 1024
	MaxProjectedItems     = 256
	MaxRawProjectionBytes = 4096
)

// ClaimProjection is a versioned provider-to-Flowstate semantic contract.
// Revision changes whenever provider syntax changes; Version identifies the
// independently versioned normalized schema understood by policies.
type ClaimProjection struct {
	Version  string                        `json:"version" yaml:"version"`
	Revision string                        `json:"revision" yaml:"revision"`
	Fields   map[string]ProjectionField    `json:"fields" yaml:"fields"`
	Raw      map[string]RawClaimProjection `json:"raw,omitempty" yaml:"raw,omitempty"`
}

// ProjectionField maps one exact JSON claim type. Missing must be "reject",
// "omit", or "default"; default requires Default of the declared type.
type ProjectionField struct {
	Claim     string `json:"claim" yaml:"claim"`
	Type      string `json:"type" yaml:"type"`
	Missing   string `json:"missing" yaml:"missing"`
	Default   any    `json:"default,omitempty" yaml:"default,omitempty"`
	MaxLength int    `json:"max_length,omitempty" yaml:"max_length,omitempty"`
	MaxItems  int    `json:"max_items,omitempty" yaml:"max_items,omitempty"`
}

// RawClaimProjection is the sole escape hatch for provider-specific claims.
// Its name is local and narrow rather than the provider's arbitrary claim map.
type RawClaimProjection struct {
	Claim   string `json:"claim" yaml:"claim"`
	Type    string `json:"type" yaml:"type"`
	MaxSize int    `json:"max_size" yaml:"max_size"`
	Issuer  string `json:"issuer" yaml:"issuer"`
	Purpose string `json:"purpose" yaml:"purpose"`
}

var normalizedFields = []string{"subject", "kind", "tenant", "groups", "roles", "assurance_level", "authentication_methods", "workload_repository", "workload_ref", "workload_environment", "service_account", "device_posture_ref", "organization_ids"}
var projectionTypes = []string{"string", "string_list", "bool"}

func (p ClaimProjection) validate(issuer string) error {
	if p.Version == "" || p.Revision == "" {
		return fmt.Errorf("version and revision are required")
	}
	if len(p.Fields) == 0 {
		return fmt.Errorf("fields is required")
	}
	for name, f := range p.Fields {
		if !slices.Contains(normalizedFields, name) {
			return fmt.Errorf("field %q is not in normalized schema version %q", name, p.Version)
		}
		if err := validateProjectionField(name, f); err != nil {
			return err
		}
	}
	if _, ok := p.Fields["subject"]; !ok {
		return fmt.Errorf("stable subject projection is required")
	}
	for name, r := range p.Raw {
		if name == "" || r.Claim == "" || !slices.Contains(projectionTypes, r.Type) || r.MaxSize <= 0 || r.MaxSize > MaxRawProjectionBytes || r.Issuer != issuer || strings.TrimSpace(r.Purpose) == "" {
			return fmt.Errorf("raw projection %q must declare claim, exact type, max_size (1..%d), matching issuer, and policy purpose", name, MaxRawProjectionBytes)
		}
	}
	return nil
}

func validateProjectionField(name string, f ProjectionField) error {
	if f.Claim == "" || !slices.Contains(projectionTypes, f.Type) {
		return fmt.Errorf("field %q requires claim and exact type", name)
	}
	if !slices.Contains([]string{"reject", "omit", "default"}, f.Missing) {
		return fmt.Errorf("field %q missing must be reject, omit, or default", name)
	}
	if f.Missing == "default" {
		if _, err := projectValue(f.Default, f); err != nil {
			return fmt.Errorf("field %q default: %w", name, err)
		}
	} else if f.Default != nil {
		return fmt.Errorf("field %q default requires missing: default", name)
	}
	if f.Type == "string" && (f.MaxLength <= 0 || f.MaxLength > MaxProjectedString) {
		return fmt.Errorf("field %q max_length must be 1..%d", name, MaxProjectedString)
	}
	if f.Type == "string_list" && (f.MaxItems <= 0 || f.MaxItems > MaxProjectedItems || f.MaxLength <= 0 || f.MaxLength > MaxProjectedString) {
		return fmt.Errorf("field %q list bounds must include max_items 1..%d and max_length 1..%d", name, MaxProjectedItems, MaxProjectedString)
	}
	return nil
}

func projectValue(v any, f ProjectionField) (any, error) {
	switch f.Type {
	case "string":
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("is %T, want string", v)
		}
		if s == "" || len(s) > f.MaxLength {
			return nil, fmt.Errorf("string is empty or exceeds %d bytes", f.MaxLength)
		}
		return s, nil
	case "bool":
		b, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("is %T, want bool", v)
		}
		return b, nil
	case "string_list":
		values, ok := v.([]any)
		if !ok {
			if ss, yes := v.([]string); yes {
				values = make([]any, len(ss))
				for i := range ss {
					values[i] = ss[i]
				}
			} else {
				return nil, fmt.Errorf("is %T, want array of strings", v)
			}
		}
		if len(values) > f.MaxItems {
			return nil, fmt.Errorf("has %d items, maximum is %d", len(values), f.MaxItems)
		}
		out := make([]string, 0, len(values))
		seen := map[string]bool{}
		for _, item := range values {
			s, ok := item.(string)
			if !ok || s == "" || len(s) > f.MaxLength {
				return nil, fmt.Errorf("contains non-string, empty, or oversized item")
			}
			if !seen[s] {
				seen[s] = true
				out = append(out, s)
			}
		}
		return out, nil
	}
	return nil, fmt.Errorf("unsupported type %q", f.Type)
}

func (p ClaimProjection) apply(claims map[string]any, issuer, namespace string) (NormalizedPrincipal, map[string]any, error) {
	_ = issuer // issuer equality for raw projections was pinned at startup.
	values := map[string]any{}
	for name, f := range p.Fields {
		v, ok := claims[f.Claim]
		if !ok {
			switch f.Missing {
			case "omit":
				continue
			case "default":
				v = f.Default
			default:
				return NormalizedPrincipal{}, nil, fmt.Errorf("projection %s/%s: required claim %q is missing", p.Version, p.Revision, f.Claim)
			}
		}
		projected, err := projectValue(v, f)
		if err != nil {
			return NormalizedPrincipal{}, nil, fmt.Errorf("projection %s/%s field %q: %w", p.Version, p.Revision, name, err)
		}
		values[name] = projected
	}
	n := normalizedFrom(values, p.Version, p.Revision)
	if n.Tenant != "" && namespace != "" && n.Tenant != namespace {
		return NormalizedPrincipal{}, nil, fmt.Errorf("projection tenant %q does not match admitted namespace %q", n.Tenant, namespace)
	}
	raw := map[string]any{}
	for name, r := range p.Raw {
		v, ok := claims[r.Claim]
		if !ok {
			continue
		}
		f := ProjectionField{Type: r.Type, MaxLength: r.MaxSize, MaxItems: MaxProjectedItems}
		pv, err := projectValue(v, f)
		if err != nil {
			return NormalizedPrincipal{}, nil, fmt.Errorf("raw projection %q: %w", name, err)
		}
		encoded, err := json.Marshal(pv)
		if err != nil || len(encoded) > r.MaxSize {
			return NormalizedPrincipal{}, nil, fmt.Errorf("raw projection %q exceeds its %d byte maximum", name, r.MaxSize)
		}
		raw[name] = pv
	}
	if n.Kind != "" && !slices.Contains([]string{"human", "workload", "agent"}, n.Kind) {
		return NormalizedPrincipal{}, nil, fmt.Errorf("projection kind %q is not human, workload, or agent", n.Kind)
	}
	return n, raw, nil
}

func normalizedFrom(v map[string]any, version, revision string) NormalizedPrincipal {
	n := NormalizedPrincipal{SchemaVersion: version, ProjectionRevision: revision}
	str := func(k string) string { s, _ := v[k].(string); return s }
	list := func(k string) []string { s, _ := v[k].([]string); return s }
	n.Subject = str("subject")
	n.Kind = str("kind")
	n.Tenant = str("tenant")
	n.Groups = list("groups")
	n.Roles = list("roles")
	n.AssuranceLevel = str("assurance_level")
	n.AuthenticationMethods = list("authentication_methods")
	n.WorkloadRepository = str("workload_repository")
	n.WorkloadRef = str("workload_ref")
	n.WorkloadEnvironment = str("workload_environment")
	n.ServiceAccount = str("service_account")
	n.DevicePostureRef = str("device_posture_ref")
	n.OrganizationIDs = list("organization_ids")
	return n
}
