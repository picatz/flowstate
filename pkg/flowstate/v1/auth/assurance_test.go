package auth

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIssuerSpecificAssuranceProjection(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	life := lifetime{now: now}
	first := TrustedIssuer{Assurance: &AssuranceProjection{ACR: map[string]AssuranceLevel{"gold": AssuranceHardwareBacked}, AMR: map[string]AuthenticationMethod{"key": MethodHardwareKey}}}
	second := TrustedIssuer{Assurance: &AssuranceProjection{ACR: map[string]AssuranceLevel{"gold": AssuranceBaseline}}}
	claims := map[string]any{"acr": "gold", "amr": []any{"key"}, "auth_time": float64(now.Unix())}

	got, err := first.projectAssurance(claims, life, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, AssuranceHardwareBacked, got.Level)
	assert.Equal(t, []AuthenticationMethod{MethodHardwareKey}, got.Methods)
	assert.Equal(t, now, got.AuthenticatedAt)
	other, err := second.projectAssurance(claims, life, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, AssuranceBaseline, other.Level, "acr meaning must be issuer-specific")
}

func TestProjectionRejectsMalformedAndFutureClaims(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	issuer := TrustedIssuer{Assurance: &AssuranceProjection{}}
	for name, claims := range map[string]map[string]any{
		"acr":       {"acr": []any{"strong"}},
		"amr":       {"amr": "key"},
		"auth_time": {"auth_time": "now"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := issuer.projectAssurance(claims, lifetime{now: now}, 0)
			require.ErrorIs(t, err, ErrMalformedToken)
		})
	}
	_, err := issuer.projectAssurance(map[string]any{"auth_time": float64(now.Add(31 * time.Second).Unix())}, lifetime{now: now}, 30*time.Second)
	require.ErrorIs(t, err, ErrTokenNotYetValid)
	_, err = issuer.projectAssurance(map[string]any{"auth_time": float64(now.Add(30 * time.Second).Unix())}, lifetime{now: now}, 30*time.Second)
	require.NoError(t, err, "configured skew is inclusive")
}

func TestAuthorizeAssuranceAndInformationHiding(t *testing.T) {
	now := time.Now()
	requirement := AssuranceRequirement{AcceptableACR: []string{"phr", "hardware"}, MinimumLevel: AssurancePhishingResistant, MaximumAge: 5 * time.Minute}
	request := AssuranceRequest{CallerKind: CallerHuman, UserInteractionPossible: true, Human: AuthenticationContext{ACR: "phr", Level: AssurancePhishingResistant, AuthenticatedAt: now.Add(-4 * time.Minute)}}
	decision, challenge := Authorize(true, request, requirement, now)
	assert.Equal(t, DecisionAllowed, decision)
	assert.Nil(t, challenge)

	request.Human.AuthenticatedAt = now.Add(-6 * time.Minute)
	decision, challenge = Authorize(true, request, requirement, now)
	assert.Equal(t, DecisionChallengeRequired, decision)
	require.NotNil(t, challenge)
	assert.Contains(t, challenge.Reasons, "authentication_too_old")
	decision2, challenge2 := Authorize(true, request, requirement, now)
	assert.Equal(t, decision, decision2, "repeated insufficiency remains a challenge")
	assert.Equal(t, challenge, challenge2)

	decision, challenge = Authorize(false, request, requirement, now)
	assert.Equal(t, DecisionDenied, decision)
	assert.Nil(t, challenge, "base denial must not reveal assurance policy")
}

func TestAutonomousAndAgentCallers(t *testing.T) {
	now := time.Now()
	senderConstrained := AssuranceRequirement{RequiredMethods: []AuthenticationMethod{MethodSenderConstrainedCredential}}
	workload := AssuranceRequest{CallerKind: CallerWorkload, UserInteractionPossible: false, Agent: AuthenticationContext{Methods: []AuthenticationMethod{MethodPassword}}}
	decision, challenge := Authorize(true, workload, senderConstrained, now)
	assert.Equal(t, DecisionDenied, decision)
	assert.Nil(t, challenge, "a non-interactive workload cannot step up")

	agentRequirement := AssuranceRequirement{Agent: &AssuranceRequirement{RequiredMethods: []AuthenticationMethod{MethodSenderConstrainedCredential}}, Human: &AssuranceRequirement{MinimumLevel: AssuranceHardwareBacked}}
	agent := AssuranceRequest{CallerKind: CallerAgentForHuman, UserInteractionPossible: true, Agent: AuthenticationContext{Methods: []AuthenticationMethod{MethodSenderConstrainedCredential}}, Human: AuthenticationContext{Level: AssuranceBaseline}}
	decision, _ = Authorize(true, agent, agentRequirement, now)
	assert.Equal(t, DecisionChallengeRequired, decision, "both agent and human boundaries are required")
}

func TestRFC9470Adapter(t *testing.T) {
	recorder := httptest.NewRecorder()
	WriteInsufficientUserAuthentication(recorder, &Challenge{RequiredACR: []string{"phr", "hardware"}})
	assert.Equal(t, 401, recorder.Code)
	assert.Equal(t, `Bearer error="insufficient_user_authentication", acr_values="phr hardware"`, recorder.Header().Get("WWW-Authenticate"))
}
