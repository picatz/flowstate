package auth

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func field(claim, typ string) ProjectionField {
	f := ProjectionField{Claim: claim, Type: typ, Missing: "reject", MaxLength: 128}
	if typ == "string_list" {
		f.MaxItems = 16
	}
	return f
}

func TestProviderProjectionFixtures(t *testing.T) {
	fixtures := []struct {
		name   string
		fields map[string]ProjectionField
		claims map[string]any
	}{
		{"github-actions", map[string]ProjectionField{"subject": field("sub", "string"), "kind": field("actor_kind", "string"), "workload_repository": field("repository", "string"), "workload_ref": field("ref", "string"), "workload_environment": field("environment", "string")}, map[string]any{"sub": "repo:acme/api:ref:refs/heads/main", "actor_kind": "workload", "repository": "acme/api", "ref": "refs/heads/main", "environment": "prod"}},
		{"gitlab", map[string]ProjectionField{"subject": field("sub", "string"), "kind": field("kind", "string"), "workload_repository": field("project_path", "string"), "workload_ref": field("ref_path", "string")}, map[string]any{"sub": "project_path:acme/api:ref_type:branch:ref:main", "kind": "workload", "project_path": "acme/api", "ref_path": "refs/heads/main"}},
		{"kubernetes", map[string]ProjectionField{"subject": field("sub", "string"), "kind": field("kind", "string"), "service_account": field("sa", "string")}, map[string]any{"sub": "system:serviceaccount:prod:runner", "kind": "workload", "sa": "prod/runner"}},
		{"spiffe-spire", map[string]ProjectionField{"subject": field("sub", "string"), "kind": field("kind", "string")}, map[string]any{"sub": "spiffe://example.test/ns/prod/sa/runner", "kind": "workload"}},
		{"workforce-idp", map[string]ProjectionField{"subject": field("sub", "string"), "kind": field("kind", "string"), "groups": field("groups", "string_list"), "authentication_methods": field("amr", "string_list")}, map[string]any{"sub": "00u-stable-id", "kind": "human", "groups": []any{"ops", "ops", "release"}, "amr": []any{"pwd", "mfa"}}},
		{"xaa", map[string]ProjectionField{"subject": field("sub", "string"), "kind": field("kind", "string"), "assurance_level": field("acr", "string")}, map[string]any{"sub": "assertion-actor", "kind": "agent", "acr": "high"}},
		{"flowstate", map[string]ProjectionField{"subject": field("sub", "string"), "kind": field("kind", "string"), "tenant": field("namespace", "string"), "workload_repository": field("workflow", "string")}, map[string]any{"sub": "flowstate:acme:deploy", "kind": "workload", "namespace": "acme", "workflow": "deploy"}},
	}
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			p := ClaimProjection{Version: "principal.v1", Revision: fixture.name + ".v1", Fields: fixture.fields}
			require.NoError(t, p.validate("https://issuer.example"))
			n, raw, err := p.apply(fixture.claims, "https://issuer.example", "")
			require.NoError(t, err)
			require.NotEmpty(t, n.Subject)
			require.Empty(t, raw)
			if fixture.name == "workforce-idp" {
				require.Equal(t, []string{"ops", "release"}, n.Groups)
			}
		})
	}
}

func TestProjectionRejectsTypeConfusionBoundsAndTenantCrossing(t *testing.T) {
	p := ClaimProjection{Version: "principal.v1", Revision: "idp.v1", Fields: map[string]ProjectionField{"subject": field("sub", "string"), "tenant": field("tenant", "string"), "groups": field("groups", "string_list")}}
	for name, claims := range map[string]map[string]any{
		"array became scalar":  {"sub": "stable", "tenant": "acme", "groups": "ops"},
		"subject became array": {"sub": []any{"stable"}, "tenant": "acme", "groups": []any{}},
		"oversized":            {"sub": "stable", "tenant": "acme", "groups": []any{"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := p.apply(claims, "https://issuer.example", "acme")
			require.Error(t, err)
		})
	}
	_, _, err := p.apply(map[string]any{"sub": "stable", "tenant": "other", "groups": []any{}}, "https://issuer.example", "acme")
	require.ErrorContains(t, err, "does not match")
}

func TestProjectionRevisionAndNarrowRawClaims(t *testing.T) {
	base := ClaimProjection{Version: "principal.v1", Revision: "github.v1", Fields: map[string]ProjectionField{"subject": field("sub", "string")}, Raw: map[string]RawClaimProjection{"workflow_visibility": {Claim: "visibility", Type: "string", MaxSize: 32, Issuer: "https://issuer.example", Purpose: "select the workflow visibility authorization rule"}}}
	revised := base
	revised.Revision = "github.v2"
	revised.Fields = map[string]ProjectionField{"subject": field("stable_sub", "string")}
	n1, raw, err := base.apply(map[string]any{"sub": "stable", "visibility": "private", "unlisted": "secret"}, "https://issuer.example", "")
	require.NoError(t, err)
	require.Equal(t, map[string]any{"workflow_visibility": "private"}, raw)
	n2, _, err := revised.apply(map[string]any{"stable_sub": "stable"}, "https://issuer.example", "")
	require.NoError(t, err)
	require.Equal(t, n1.SchemaVersion, n2.SchemaVersion)
	require.NotEqual(t, n1.ProjectionRevision, n2.ProjectionRevision)
}
