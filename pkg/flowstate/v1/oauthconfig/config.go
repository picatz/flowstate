// Package oauthconfig loads and composes the serializable, non-secret OAuth and
// workload-identity configuration. Credential values live in package runtime.
package oauthconfig

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"

	"github.com/goccy/go-yaml"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const MaxConfigurationBytes = 1 << 20

// Parse decodes YAML sugar directly into the protobuf model. Duplicate YAML
// keys, unknown protobuf fields, oversized documents, descriptor constraints,
// and cross-message inconsistencies are all rejected at startup.
func Parse(data []byte) (*v1.OAuthConfiguration, error) {
	if len(data) > MaxConfigurationBytes {
		return nil, fmt.Errorf("oauth configuration exceeds %d bytes", MaxConfigurationBytes)
	}
	var document any
	if err := yaml.UnmarshalWithOptions(data, &document, yaml.Strict()); err != nil {
		return nil, fmt.Errorf("decode oauth configuration: %w", err)
	}
	raw, err := json.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("normalize oauth configuration: %w", err)
	}
	config := new(v1.OAuthConfiguration)
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(raw, config); err != nil {
		return nil, fmt.Errorf("decode oauth configuration: %w", err)
	}
	if err := Validate(config); err != nil {
		return nil, err
	}
	return config, nil
}

// Render returns the deterministic, descriptor-named YAML representation.
func Render(config *v1.OAuthConfiguration) ([]byte, error) {
	if err := Validate(config); err != nil {
		return nil, err
	}
	raw, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("marshal oauth configuration: %w", err)
	}
	var document any
	if err := json.Unmarshal(raw, &document); err != nil {
		return nil, err
	}
	return yaml.Marshal(document)
}

// Validate runs protovalidate and the relational checks descriptors cannot state.
func Validate(config *v1.OAuthConfiguration) error {
	if err := v1.Validate(config); err != nil {
		return fmt.Errorf("invalid oauth configuration: %w", err)
	}
	servers := make(map[string]struct{}, len(config.GetAuthorizationServers()))
	for i, server := range config.GetAuthorizationServers() {
		if _, found := servers[server.GetName()]; found {
			return fmt.Errorf("authorization_servers[%d]: duplicate name %q", i, server.GetName())
		}
		servers[server.GetName()] = struct{}{}
		for field, value := range map[string]string{"issuer": server.GetIssuer(), "authorization_server": server.GetAuthorizationServer()} {
			if err := secureURL(value); err != nil {
				return fmt.Errorf("authorization_servers[%d].%s: %w", i, field, err)
			}
		}
		if server.GetEndpointTrust().GetRequireHttps() {
			if location := server.GetMetadata().GetDiscoveryUrl(); location != "" {
				if err := secureURL(location); err != nil {
					return fmt.Errorf("authorization_servers[%d].metadata.discovery_url: %w", i, err)
				}
			}
		}
	}
	profiles := map[string]struct{}{}
	for i, profile := range config.GetGrantProfiles() {
		if _, found := profiles[profile.GetName()]; found {
			return fmt.Errorf("grant_profiles[%d]: duplicate name %q", i, profile.GetName())
		}
		profiles[profile.GetName()] = struct{}{}
		if profile.GetRequirePar() && !slices.Contains(profile.GetRevisions(), v1.ProtocolRevision_PROTOCOL_REVISION_RFC_9126) {
			return fmt.Errorf("grant_profiles[%d]: require_par needs RFC 9126 in revisions", i)
		}
		if profile.GetRequireJar() && !slices.Contains(profile.GetRevisions(), v1.ProtocolRevision_PROTOCOL_REVISION_RFC_9101) {
			return fmt.Errorf("grant_profiles[%d]: require_jar needs RFC 9101 in revisions", i)
		}
		if profile.GetProof() == v1.ProofRequirement_PROOF_REQUIREMENT_DPOP && !slices.Contains(profile.GetRevisions(), v1.ProtocolRevision_PROTOCOL_REVISION_RFC_9449) {
			return fmt.Errorf("grant_profiles[%d]: DPoP needs RFC 9449 in revisions", i)
		}
	}
	for i, resource := range config.GetProtectedResources() {
		for _, server := range resource.GetAuthorizationServers() {
			if _, found := servers[server]; !found {
				return fmt.Errorf("protected_resources[%d]: unknown authorization server %q", i, server)
			}
		}
	}
	return nil
}

func secureURL(value string) error {
	u, err := url.Parse(value)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Fragment != "" {
		return errors.New("must be an absolute HTTPS URL without userinfo or fragment")
	}
	return nil
}

// Layers names the precedence order explicitly. Each lower layer restricts the
// result; it cannot add a named server, resource, profile, or policy.
type Layers struct {
	Deployment *v1.OAuthConfiguration
	Tenant     *v1.OAuthConfiguration
	Surface    *v1.OAuthConfiguration
	Target     *v1.OAuthConfiguration
}

// Intersect applies deployment defaults, tenant restrictions, a surface
// profile, and target restrictions in that order using fail-closed intersection.
func Intersect(layers Layers) (*v1.OAuthConfiguration, error) {
	if layers.Deployment == nil {
		return nil, errors.New("deployment oauth boundary is required")
	}
	if err := Validate(layers.Deployment); err != nil {
		return nil, fmt.Errorf("deployment: %w", err)
	}
	result := proto.Clone(layers.Deployment).(*v1.OAuthConfiguration)
	for _, layer := range []struct {
		name   string
		config *v1.OAuthConfiguration
	}{{"tenant", layers.Tenant}, {"surface", layers.Surface}, {"target", layers.Target}} {
		if layer.config == nil {
			continue
		}
		if err := Validate(layer.config); err != nil {
			return nil, fmt.Errorf("%s: %w", layer.name, err)
		}
		var err error
		result.AuthorizationServers, err = restrictByName(result.AuthorizationServers, layer.config.AuthorizationServers, func(v *v1.AuthorizationServer) string { return v.GetName() })
		if err != nil {
			return nil, fmt.Errorf("%s authorization_servers: %w", layer.name, err)
		}
		result.ProtectedResources, err = restrictByName(result.ProtectedResources, layer.config.ProtectedResources, func(v *v1.ProtectedResource) string { return v.GetName() })
		if err != nil {
			return nil, fmt.Errorf("%s protected_resources: %w", layer.name, err)
		}
		result.GrantProfiles, err = restrictByName(result.GrantProfiles, layer.config.GrantProfiles, func(v *v1.GrantProfile) string { return v.GetName() })
		if err != nil {
			return nil, fmt.Errorf("%s grant_profiles: %w", layer.name, err)
		}
		if layer.config.TelemetryPrivacy < result.TelemetryPrivacy && !slices.Contains(result.DelegableFields, v1.DelegableField_DELEGABLE_FIELD_TELEMETRY_PRIVACY) {
			return nil, fmt.Errorf("%s telemetry_privacy weakens the deployment boundary", layer.name)
		}
		if layer.config.TelemetryPrivacy > result.TelemetryPrivacy {
			result.TelemetryPrivacy = layer.config.TelemetryPrivacy
		}
	}
	if err := Validate(result); err != nil {
		return nil, fmt.Errorf("effective oauth configuration: %w", err)
	}
	return result, nil
}

func restrictByName[T any](upper, lower []T, name func(T) string) ([]T, error) {
	if len(lower) == 0 {
		return upper, nil
	}
	allowed := make(map[string]T, len(upper))
	for _, item := range upper {
		allowed[name(item)] = item
	}
	result := make([]T, 0, len(lower))
	for _, item := range lower {
		boundary, ok := allowed[name(item)]
		if !ok {
			return nil, fmt.Errorf("%q is outside the deployment boundary", name(item))
		}
		// Membership is the intersection. The deployment's descriptor-shaped
		// value survives, so a lower layer cannot quietly replace endpoint trust,
		// proof, assurance, PAR/JAR, cache, audience, or scope constraints.
		result = append(result, boundary)
	}
	return result, nil
}

// CompactJSON is useful to descriptor-driven documentation/schema generators.
func CompactJSON(config *v1.OAuthConfiguration) ([]byte, error) {
	raw, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(config)
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	if err := json.Compact(&out, raw); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
