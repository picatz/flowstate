package oauthconfig_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/oauthconfig"
	oauthruntime "github.com/picatz/flowstate/pkg/flowstate/v1/oauthconfig/runtime"
)

func TestDescriptorGeneratedArtifacts(t *testing.T) {
	schema, err := oauthconfig.JSONSchema()
	require.NoError(t, err)
	var document map[string]any
	require.NoError(t, json.Unmarshal(schema, &document))
	require.Contains(t, document["properties"], "authorization_servers")
	require.Contains(t, oauthconfig.Reference(), "`authorization_servers`")
	env, err := cel.NewEnv(oauthconfig.CELTypes(), cel.Variable("config", cel.ObjectType("flowstate.v1.OAuthConfiguration")))
	require.NoError(t, err)
	_, issues := env.Compile("config.authorization_servers.size()")
	require.NoError(t, issues.Err())
}

const valid = `
authorization_servers:
  - name: corp
    issuer: https://id.example.com
    authorization_server: https://id.example.com
    metadata: {discovery_url: https://id.example.com/.well-known/openid-configuration}
    revisions: [PROTOCOL_REVISION_OAUTH_2_1, PROTOCOL_REVISION_RFC_9126]
    endpoint_trust: {require_https: true, require_same_origin: true}
    cache: {maximum_age: 300s, stale_if_error: 30s, clock_skew: 10s}
protected_resources:
  - name: api
    resource: https://api.example.com
    metadata: {discovery_url: https://api.example.com/.well-known/oauth-protected-resource}
    authorization_servers: [corp]
    scopes: [read]
    proof: PROOF_REQUIREMENT_DPOP
    telemetry_privacy: TELEMETRY_PRIVACY_CLASS_INTERNAL
grant_profiles:
  - name: interactive
    grant: GRANT_TYPE_AUTHORIZATION_CODE
    revisions: [PROTOCOL_REVISION_OAUTH_2_1, PROTOCOL_REVISION_RFC_9126]
    resources: [https://api.example.com]
    scopes: [read]
    client_id: flow-cli
    client_authentication_method_ref: public-pkce
    require_par: true
telemetry_privacy: TELEMETRY_PRIVACY_CLASS_INTERNAL
`

func TestYAMLProtoCanonicalRoundTrip(t *testing.T) {
	first, err := oauthconfig.Parse([]byte(valid))
	require.NoError(t, err)
	rendered, err := oauthconfig.Render(first)
	require.NoError(t, err)
	second, err := oauthconfig.Parse(rendered)
	require.NoError(t, err)
	require.Equal(t, first, second)
}

func TestSecretsHaveNoProtobufField(t *testing.T) {
	forbidden := []string{"private_key", "client_secret", "authorization_code", "refresh_token", "dpop_key", "access_token", "raw_assertion"}
	var visit func(protoreflect.MessageDescriptor)
	visit = func(message protoreflect.MessageDescriptor) {
		for i := range message.Fields().Len() {
			field := message.Fields().Get(i)
			for _, word := range forbidden {
				require.NotContains(t, string(field.Name()), word)
			}
			if field.Message() != nil && !field.IsMap() {
				visit(field.Message())
			}
		}
	}
	visit((&v1.OAuthConfiguration{}).ProtoReflect().Descriptor())
	encoded, err := protojson.Marshal(&v1.SecretReference{Provider: "vault", Name: "oauth/client"})
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "secret-value")
}

func TestRuntimeSecretsRedactDiagnostics(t *testing.T) {
	secret := "super-secret-value"
	values := []fmt.Stringer{
		oauthruntime.PrivateKey(secret), oauthruntime.ClientSecret(secret),
		oauthruntime.AuthorizationCode(secret), oauthruntime.RefreshToken(secret),
		oauthruntime.DPoPKey(secret), oauthruntime.AccessToken(secret), oauthruntime.RawAssertion(secret),
	}
	for _, value := range values {
		diagnostic := fmt.Sprintf("credential: %s", value)
		require.NotContains(t, diagnostic, secret)
		require.Contains(t, diagnostic, "[REDACTED]")
	}
}

func TestSemanticValidationAndLayerIntersection(t *testing.T) {
	deployment, err := oauthconfig.Parse([]byte(valid))
	require.NoError(t, err)
	tenant := &v1.OAuthConfiguration{
		AuthorizationServers: deployment.AuthorizationServers,
		ProtectedResources:   deployment.ProtectedResources,
		GrantProfiles:        deployment.GrantProfiles,
		TelemetryPrivacy:     v1.TelemetryPrivacyClass_TELEMETRY_PRIVACY_CLASS_SENSITIVE_METADATA,
	}
	effective, err := oauthconfig.Intersect(oauthconfig.Layers{Deployment: deployment, Tenant: tenant})
	require.NoError(t, err)
	require.Equal(t, v1.TelemetryPrivacyClass_TELEMETRY_PRIVACY_CLASS_SENSITIVE_METADATA, effective.TelemetryPrivacy)

	tenant.GrantProfiles[0].RequireJar = true
	err = oauthconfig.Validate(tenant)
	require.ErrorContains(t, err, "RFC 9101")
}

func TestUnknownAndDuplicateYAMLFieldsFailClosed(t *testing.T) {
	_, err := oauthconfig.Parse([]byte(valid + "mystery: true\n"))
	require.Error(t, err)
	duplicate := strings.Replace(valid, "telemetry_privacy: TELEMETRY_PRIVACY_CLASS_INTERNAL\n", "telemetry_privacy: TELEMETRY_PRIVACY_CLASS_INTERNAL\ntelemetry_privacy: TELEMETRY_PRIVACY_CLASS_PUBLIC\n", 1)
	_, err = oauthconfig.Parse([]byte(duplicate))
	require.Error(t, err)
}
