package flowstatev1

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func testMapping(t *testing.T) *MappingRevision {
	t.Helper()
	r := &MappingRevision{
		Revision:      1,
		Actions:       []*Action{{Name: "run.signal"}, {Name: "admin.internal"}},
		ResourceTypes: []*ResourceType{{Name: "run"}},
		Scopes: []*OAuthScope{
			{Name: "runs.write", Actions: []string{"run.signal"}, ResourceTypes: []string{"run"}, Consent: &ConsentDescription{Summary: "Signal runs"}},
			{Name: "tenant.select", Actions: []string{"run.signal"}, ResourceTypes: []string{"run"}, Consent: &ConsentDescription{Summary: "Select tenant"}},
		},
		Mappings: []*ActionMapping{{
			Action:          "run.signal",
			ResourceTypes:   []string{"run"},
			RequiredScopes:  []string{"runs.write", "tenant.select"},
			Directionality:  Directionality_DIRECTIONALITY_BIDIRECTIONAL,
			RequiredContext: &RequiredContext{Claims: []string{"mfa"}, MinimumAssuranceLevel: 2},
		}},
	}
	digest, err := MeaningDigest(r)
	require.NoError(t, err)
	r.MeaningDigest = digest
	return r
}

func TestOAuthMappingIsExplicitCompoundAndConjunctive(t *testing.T) {
	r := testMapping(t)
	require.NoError(t, CheckMappingProfile(&MappingProfile{Name: "default", Revisions: []*MappingRevision{r}}))

	intended := &PARCRequest{Actions: []string{"run.signal"}, ResourceTypes: []string{"run"}}
	boundary := MappingBoundary{Audience: "https://flow.example/mcp", Deployment: "prod", Tenant: "acme", Resource: "run/42", Context: map[string]string{"mfa": "true"}, AssuranceLevel: 2}
	_, err := AuthorizeInbound(t.Context(), r, boundary.Audience, boundary.Audience, &OAuthGrant{Scopes: []string{"runs.write"}}, intended, boundary, func(_ context.Context, _ *PARCRequest) (bool, error) { return true, nil })
	require.ErrorContains(t, err, "mapped grant does not permit", "one scope must not satisfy a compound grant")

	got, err := AuthorizeInbound(t.Context(), r, boundary.Audience, boundary.Audience, &OAuthGrant{Scopes: []string{"tenant.select", "runs.write"}}, intended, boundary, func(_ context.Context, request *PARCRequest) (bool, error) {
		return request.GetTenant() == "acme" && request.GetDeployment() == "prod", nil
	})
	require.NoError(t, err)
	require.Equal(t, "run/42", got.GetResource())

	_, err = AuthorizeInbound(t.Context(), r, boundary.Audience, boundary.Audience, &OAuthGrant{Scopes: []string{"tenant.select", "runs.write"}}, intended, boundary, func(context.Context, *PARCRequest) (bool, error) { return false, nil })
	require.ErrorContains(t, err, "CEL policy denied")
}

func TestOAuthMappingRefusesImplicitExposureAndBroadOutboundGrant(t *testing.T) {
	r := testMapping(t)
	_, err := OutboundGrant(r, &PARCRequest{Actions: []string{"admin.internal"}, ResourceTypes: []string{"run"}})
	require.ErrorIs(t, err, ErrUnrepresentable, "a declared internal action is not external until mapped")

	grant, err := OutboundGrant(r, &PARCRequest{Actions: []string{"run.signal"}, ResourceTypes: []string{"run"}})
	require.NoError(t, err)
	require.Equal(t, []string{"runs.write", "tenant.select"}, grant.GetScopes())

	r.Mappings[0].Attenuation = []*AttenuationRule{{Name: "only run/42", Required: true}}
	_, err = OutboundGrant(r, &PARCRequest{Actions: []string{"run.signal"}, ResourceTypes: []string{"run"}})
	require.True(t, errors.Is(err, ErrUnrepresentable), "a required constraint must be refused, never widened")
}

func TestMeaningDigestPinsRevisionContents(t *testing.T) {
	r := testMapping(t)
	r.Scopes[0].Actions = append(r.Scopes[0].Actions, "admin.internal")
	err := CheckMappingProfile(&MappingProfile{Name: "default", Revisions: []*MappingRevision{r}})
	require.ErrorContains(t, err, "meaning_digest", "changing a scope meaning under the same revision must be visible")
}
