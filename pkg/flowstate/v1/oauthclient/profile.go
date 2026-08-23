// Package oauthclient implements the policy boundary for interactive OAuth
// clients.  Its transaction values deliberately are not protobuf messages:
// PKCE verifiers and proof-key handles must never be serialized into a
// Flowstate specification or durable workflow history.
package oauthclient

import (
	"fmt"
	"slices"
	"time"
)

// ProfileName identifies a closed, versioned set of OAuth requirements.
type ProfileName string

const (
	BaselinePublicClient        ProfileName = "baseline-public-client"
	BaselineConfidentialClient  ProfileName = "baseline-confidential-client"
	SenderConstrainedAgent      ProfileName = "sender-constrained-agent"
	EnterpriseInteractive       ProfileName = "enterprise-interactive"
	HighAssuranceAdministration ProfileName = "high-assurance-administration"
	WorkloadFederation          ProfileName = "workload-federation"
	ExperimentalXAA             ProfileName = "experimental-xaa"
)

type Binding string

const (
	BindingNone Binding = "none"
	BindingDPoP Binding = "dpop"
	BindingMTLS Binding = "mtls"
)

// Requirements is returned by value. Its slices are copied, so callers cannot
// weaken the named profile used by another transaction.
type Requirements struct {
	Name                        ProfileName
	PKCEMethod                  string
	ExactRedirectURI            bool
	PARRequired                 bool
	SignedRequestObject         bool
	IssuerIdentification        bool
	Binding                     Binding
	ClientAuthenticationMethods []string
	ResponseModes               []string
	ResourceIndicatorRequired   bool
	IssueRefreshTokens          bool
	RotateRefreshTokens         bool
	StepUpSupported             bool
	MaximumMetadataAge          time.Duration
	RequestObjectAlgorithms     []string
	IDTokenAlgorithms           []string
	DPoPAlgorithms              []string
	ProhibitDowngrade           bool
}

var profiles = map[ProfileName]Requirements{
	BaselinePublicClient:        requirement(BaselinePublicClient, false, false, BindingNone, []string{"none"}, false, true),
	BaselineConfidentialClient:  requirement(BaselineConfidentialClient, false, false, BindingNone, []string{"private_key_jwt", "tls_client_auth"}, false, true),
	SenderConstrainedAgent:      requirement(SenderConstrainedAgent, true, true, BindingDPoP, []string{"private_key_jwt"}, true, true),
	EnterpriseInteractive:       requirement(EnterpriseInteractive, true, true, BindingDPoP, []string{"private_key_jwt", "tls_client_auth"}, true, true),
	HighAssuranceAdministration: requirement(HighAssuranceAdministration, true, true, BindingMTLS, []string{"tls_client_auth", "self_signed_tls_client_auth"}, true, false),
	WorkloadFederation:          requirement(WorkloadFederation, true, true, BindingMTLS, []string{"private_key_jwt", "tls_client_auth"}, true, false),
	ExperimentalXAA:             requirement(ExperimentalXAA, true, true, BindingDPoP, []string{"private_key_jwt"}, true, false),
}

func requirement(name ProfileName, par, jar bool, binding Binding, auth []string, stepUp, refresh bool) Requirements {
	return Requirements{Name: name, PKCEMethod: "S256", ExactRedirectURI: true,
		PARRequired: par, SignedRequestObject: jar, IssuerIdentification: true,
		Binding: binding, ClientAuthenticationMethods: auth,
		ResponseModes: []string{"query"}, ResourceIndicatorRequired: true,
		IssueRefreshTokens: refresh, RotateRefreshTokens: refresh,
		StepUpSupported: stepUp, MaximumMetadataAge: 15 * time.Minute,
		RequestObjectAlgorithms: []string{"PS256", "ES256"}, IDTokenAlgorithms: []string{"PS256", "ES256"},
		DPoPAlgorithms: []string{"ES256"}, ProhibitDowngrade: true}
}

// Profile returns the immutable requirements for name.
func Profile(name ProfileName) (Requirements, error) {
	p, ok := profiles[name]
	if !ok {
		return Requirements{}, fmt.Errorf("oauth profile %q is not defined", name)
	}
	p.ClientAuthenticationMethods = slices.Clone(p.ClientAuthenticationMethods)
	p.ResponseModes = slices.Clone(p.ResponseModes)
	p.RequestObjectAlgorithms = slices.Clone(p.RequestObjectAlgorithms)
	p.IDTokenAlgorithms = slices.Clone(p.IDTokenAlgorithms)
	p.DPoPAlgorithms = slices.Clone(p.DPoPAlgorithms)
	return p, nil
}
