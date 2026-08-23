package auth

import (
	"fmt"
	"slices"

	authv1 "github.com/picatz/flowstate/pkg/flowstate/auth/v1"
	"google.golang.org/protobuf/proto"
)

const (
	ProfileOIDCCore10            = "oidc-core-1.0"
	ProfileMTLSRFC8705           = "oauth-mtls-rfc8705"
	ProfileTokenExchange8693     = "oauth-token-exchange-rfc8693"
	ProfileClientCredentials6749 = "oauth-client-credentials-rfc6749"
	ProfileAWSWebIdentity        = "aws-sts-web-identity-2011-06-15"
	ProfileGCPWorkloadIdentity   = "gcp-workload-identity-v1"
	ProfileFlowstateAssertionV1  = "flowstate-assertion-v1"
)

var authProfiles = []*authv1.AuthProfileDescriptor{
	{Id: ProfileOIDCCore10, ProtocolFamily: "OpenID Connect", Revision: "OpenID Connect Core 1.0 incorporating errata set 2", Maturity: authv1.AuthProfileMaturity_AUTH_PROFILE_MATURITY_STABLE, Roles: []authv1.AuthProfileRole{authv1.AuthProfileRole_AUTH_PROFILE_ROLE_RESOURCE_SERVER}, RequiredMetadata: []string{"issuer", "jwks_uri"}, TokenTypes: []string{"JWT ID token"}, SenderConstraint: authv1.AuthSenderConstraint_AUTH_SENDER_CONSTRAINT_NONE, RequiredClaims: []string{"iss", "sub", "aud", "exp"}, CapabilityDependencies: []string{"oidc-discovery", "jwt-signature-verification"}, DowngradeBehavior: authv1.AuthDowngradeBehavior_AUTH_DOWNGRADE_BEHAVIOR_REFUSE},
	{Id: ProfileMTLSRFC8705, ProtocolFamily: "OAuth 2.0 Mutual-TLS", Revision: "RFC 8705", Maturity: authv1.AuthProfileMaturity_AUTH_PROFILE_MATURITY_STABLE, Roles: []authv1.AuthProfileRole{authv1.AuthProfileRole_AUTH_PROFILE_ROLE_RESOURCE_SERVER}, RequiredMetadata: []string{"client_ca_file", "subject_from"}, SenderConstraint: authv1.AuthSenderConstraint_AUTH_SENDER_CONSTRAINT_MTLS, CapabilityDependencies: []string{"tls-client-certificate-verification"}, DowngradeBehavior: authv1.AuthDowngradeBehavior_AUTH_DOWNGRADE_BEHAVIOR_REFUSE},
	{Id: ProfileTokenExchange8693, ProtocolFamily: "OAuth 2.0 Token Exchange", Revision: "RFC 8693", Maturity: authv1.AuthProfileMaturity_AUTH_PROFILE_MATURITY_STABLE, Roles: []authv1.AuthProfileRole{authv1.AuthProfileRole_AUTH_PROFILE_ROLE_CLIENT}, RequiredMetadata: []string{"token_url", "audience"}, GrantTypes: []string{grantTypeTokenExchange}, TokenTypes: []string{tokenTypeJWT, tokenTypeAccessToken}, SenderConstraint: authv1.AuthSenderConstraint_AUTH_SENDER_CONSTRAINT_NONE, RequiredClaims: []string{"iss", "sub", "aud", "exp", "jti"}, CapabilityDependencies: []string{"oauth-token-endpoint", "jwt-assertion-minting"}, DowngradeBehavior: authv1.AuthDowngradeBehavior_AUTH_DOWNGRADE_BEHAVIOR_REFUSE},
	{Id: ProfileClientCredentials6749, ProtocolFamily: "OAuth 2.0", Revision: "RFC 6749 section 4.4 with RFC 7523 client assertion", Maturity: authv1.AuthProfileMaturity_AUTH_PROFILE_MATURITY_STABLE, Roles: []authv1.AuthProfileRole{authv1.AuthProfileRole_AUTH_PROFILE_ROLE_CLIENT}, RequiredMetadata: []string{"token_url", "client_id"}, GrantTypes: []string{grantTypeClientCredentials}, TokenTypes: []string{clientAssertionTypeJWT}, SenderConstraint: authv1.AuthSenderConstraint_AUTH_SENDER_CONSTRAINT_NONE, RequiredClaims: []string{"iss", "sub", "aud", "exp", "jti"}, CapabilityDependencies: []string{"oauth-token-endpoint", "jwt-assertion-minting"}, DowngradeBehavior: authv1.AuthDowngradeBehavior_AUTH_DOWNGRADE_BEHAVIOR_REFUSE},
	{Id: ProfileAWSWebIdentity, ProtocolFamily: "AWS STS", Revision: "API 2011-06-15 AssumeRoleWithWebIdentity", Maturity: authv1.AuthProfileMaturity_AUTH_PROFILE_MATURITY_STABLE, Roles: []authv1.AuthProfileRole{authv1.AuthProfileRole_AUTH_PROFILE_ROLE_CLIENT}, RequiredMetadata: []string{"role_arn"}, TokenTypes: []string{"JWT"}, SenderConstraint: authv1.AuthSenderConstraint_AUTH_SENDER_CONSTRAINT_NONE, RequiredClaims: []string{"iss", "sub", "aud", "exp"}, CapabilityDependencies: []string{"jwt-assertion-minting"}, DowngradeBehavior: authv1.AuthDowngradeBehavior_AUTH_DOWNGRADE_BEHAVIOR_REFUSE},
	{Id: ProfileGCPWorkloadIdentity, ProtocolFamily: "Google Workload Identity Federation", Revision: "STS v1", Maturity: authv1.AuthProfileMaturity_AUTH_PROFILE_MATURITY_STABLE, Roles: []authv1.AuthProfileRole{authv1.AuthProfileRole_AUTH_PROFILE_ROLE_CLIENT}, RequiredMetadata: []string{"audience"}, GrantTypes: []string{grantTypeTokenExchange}, TokenTypes: []string{tokenTypeJWT, tokenTypeAccessToken}, SenderConstraint: authv1.AuthSenderConstraint_AUTH_SENDER_CONSTRAINT_NONE, RequiredClaims: []string{"iss", "sub", "aud", "exp"}, CapabilityDependencies: []string{"gcp-sts-v1", "jwt-assertion-minting"}, DowngradeBehavior: authv1.AuthDowngradeBehavior_AUTH_DOWNGRADE_BEHAVIOR_REFUSE},
	{Id: ProfileFlowstateAssertionV1, ProtocolFamily: "Flowstate assertion", Revision: "v1", Maturity: authv1.AuthProfileMaturity_AUTH_PROFILE_MATURITY_STABLE, Roles: []authv1.AuthProfileRole{authv1.AuthProfileRole_AUTH_PROFILE_ROLE_ISSUER}, RequiredMetadata: []string{"audience"}, TokenTypes: []string{"JWT"}, SenderConstraint: authv1.AuthSenderConstraint_AUTH_SENDER_CONSTRAINT_NONE, RequiredClaims: []string{"iss", "sub", "aud", "exp", "jti"}, CapabilityDependencies: []string{"jwt-assertion-minting"}, DowngradeBehavior: authv1.AuthDowngradeBehavior_AUTH_DOWNGRADE_BEHAVIOR_REFUSE},
}

// AuthProfiles returns defensive copies of the immutable implemented contracts.
func AuthProfiles() []*authv1.AuthProfileDescriptor {
	out := make([]*authv1.AuthProfileDescriptor, len(authProfiles))
	for i, p := range authProfiles {
		out[i] = proto.Clone(p).(*authv1.AuthProfileDescriptor)
	}
	return out
}

func requireProfile(id, expected string, experimentalOptIn bool) error {
	if id == "" {
		return fmt.Errorf("%w: profile is required; select %q explicitly", ErrInvalidPolicy, expected)
	}
	i := slices.IndexFunc(authProfiles, func(p *authv1.AuthProfileDescriptor) bool { return p.GetId() == id })
	if i < 0 {
		return fmt.Errorf("%w: unknown auth profile revision %q", ErrInvalidPolicy, id)
	}
	p := authProfiles[i]
	if id != expected {
		return fmt.Errorf("%w: profile %q is incompatible with this configuration; expected %q", ErrInvalidPolicy, id, expected)
	}
	if p.GetMaturity() != authv1.AuthProfileMaturity_AUTH_PROFILE_MATURITY_STABLE && !experimentalOptIn {
		return fmt.Errorf("%w: profile %q requires explicit experimental opt-in", ErrInvalidPolicy, id)
	}
	return nil
}
