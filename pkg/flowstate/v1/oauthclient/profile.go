package oauthclient

import (
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
)

// Flow is an OAuth grant enabled by a security profile.
type Flow string

const (
	AuthorizationCodePKCE Flow = "authorization_code_pkce"
	DeviceAuthorization   Flow = "device_authorization"
	ClientCredentials     Flow = "client_credentials"
	TokenExchange         Flow = "token_exchange"
	XAAIDJAG              Flow = "xaa_id_jag"
)

// Profile is operator-owned policy, not a requester's wish list. Experimental
// protocols must be enabled explicitly and insecure combinations fail closed.
type Profile struct {
	Name                 string
	Issuer               string
	ClientID             string
	Flows                []Flow
	PrivateKeyJWT        bool // RFC 7523 client authentication
	DPoP                 bool
	MTLS                 bool
	PAR                  bool
	JAR                  bool
	ResourceIndicators   bool // RFC 8707
	AuthorizationDetails bool // RFC 9396
	RefreshRotation      bool
	ExperimentalXAA      bool
}

// Validate rejects ambiguous or weakened profiles before network activity.
func (p Profile) Validate() error {
	if p.Name == "" || p.Issuer == "" || p.ClientID == "" || len(p.Flows) == 0 {
		return errors.New("oauthclient: profile name, issuer, client ID, and at least one flow are required")
	}
	u, err := url.Parse(p.Issuer)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Fragment != "" {
		return fmt.Errorf("oauthclient: profile %q issuer must be an absolute HTTPS URL", p.Name)
	}
	seen := map[Flow]bool{}
	for _, flow := range p.Flows {
		if seen[flow] {
			return fmt.Errorf("oauthclient: profile %q repeats flow %q", p.Name, flow)
		}
		seen[flow] = true
		switch flow {
		case AuthorizationCodePKCE, DeviceAuthorization, ClientCredentials, TokenExchange:
		case XAAIDJAG:
			if !p.ExperimentalXAA {
				return fmt.Errorf("oauthclient: profile %q must explicitly enable experimental XAA/ID-JAG", p.Name)
			}
		default:
			return fmt.Errorf("oauthclient: profile %q has unknown flow %q", p.Name, flow)
		}
	}
	if p.RefreshRotation && !slices.Contains(p.Flows, AuthorizationCodePKCE) && !slices.Contains(p.Flows, DeviceAuthorization) {
		return fmt.Errorf("oauthclient: profile %q enables refresh rotation without an interactive flow", p.Name)
	}
	return nil
}

func (p Profile) permits(flow Flow) bool { return slices.Contains(p.Flows, flow) }

func canonicalStrings(in []string) []string {
	out := append([]string(nil), in...)
	for i := range out {
		out[i] = strings.TrimSpace(out[i])
	}
	slices.Sort(out)
	return slices.Compact(out)
}
