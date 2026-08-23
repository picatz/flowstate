package auth

import (
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"
)

// These values deliberately match flowstate.v1's canonical protobuf enums.
type AssuranceLevel int32

const (
	AssuranceUnspecified AssuranceLevel = iota
	AssuranceBaseline
	AssuranceMultiFactor
	AssurancePhishingResistant
	AssuranceHardwareBacked
)

type AuthenticationMethod int32

const (
	MethodUnspecified AuthenticationMethod = iota
	MethodPassword
	MethodOneTimePassword
	MethodBiometric
	MethodSecurityKey
	MethodHardwareKey
	MethodMutualTLS
	MethodSenderConstrainedCredential
)

type CallerKind int32

const (
	CallerUnspecified CallerKind = iota
	CallerHuman
	CallerWorkload
	CallerAgentForHuman
)

type AuthenticationContext struct {
	ACR             string                 `json:"acr,omitempty"`
	Level           AssuranceLevel         `json:"level,omitempty"`
	Methods         []AuthenticationMethod `json:"methods,omitempty"`
	AuthenticatedAt time.Time              `json:"authenticated_at,omitempty"`
}

// AssuranceProjection is issuer-specific: equal acr strings from different
// issuers need not mean equal assurance.
type AssuranceProjection struct {
	ACRClaim      string                          `json:"acr_claim,omitempty" yaml:"acr_claim,omitempty"`
	AMRClaim      string                          `json:"amr_claim,omitempty" yaml:"amr_claim,omitempty"`
	AuthTimeClaim string                          `json:"auth_time_claim,omitempty" yaml:"auth_time_claim,omitempty"`
	ACR           map[string]AssuranceLevel       `json:"acr,omitempty" yaml:"acr,omitempty"`
	AMR           map[string]AuthenticationMethod `json:"amr,omitempty" yaml:"amr,omitempty"`
}

func (p AssuranceProjection) validate() error {
	for acr, level := range p.ACR {
		if acr == "" || level < AssuranceBaseline || level > AssuranceHardwareBacked {
			return fmt.Errorf("invalid acr mapping %q", acr)
		}
	}
	for amr, method := range p.AMR {
		if amr == "" || method < MethodPassword || method > MethodSenderConstrainedCredential {
			return fmt.Errorf("invalid amr mapping %q", amr)
		}
	}
	return nil
}

func (t TrustedIssuer) projectAssurance(claims map[string]any, lifetime lifetime, skew time.Duration) (AuthenticationContext, error) {
	if t.Assurance == nil {
		return AuthenticationContext{}, nil
	}
	p := t.Assurance
	acrName, amrName, timeName := p.ACRClaim, p.AMRClaim, p.AuthTimeClaim
	if acrName == "" {
		acrName = "acr"
	}
	if amrName == "" {
		amrName = "amr"
	}
	if timeName == "" {
		timeName = "auth_time"
	}
	var out AuthenticationContext
	if raw, ok := claims[acrName]; ok {
		acr, ok := raw.(string)
		if !ok || acr == "" {
			return out, fmt.Errorf("%w: %s must be a non-empty string", ErrMalformedToken, acrName)
		}
		out.ACR = acr
		out.Level = p.ACR[acr]
	}
	if raw, ok := claims[amrName]; ok {
		values, ok := raw.([]any)
		if !ok {
			return out, fmt.Errorf("%w: %s must be an array", ErrMalformedToken, amrName)
		}
		for _, rawMethod := range values {
			method, ok := rawMethod.(string)
			if !ok {
				return out, fmt.Errorf("%w: %s entries must be strings", ErrMalformedToken, amrName)
			}
			if normalized := p.AMR[method]; normalized != 0 && !slices.Contains(out.Methods, normalized) {
				out.Methods = append(out.Methods, normalized)
			}
		}
	}
	if raw, ok := claims[timeName]; ok {
		seconds, ok := raw.(float64)
		if !ok || seconds != float64(int64(seconds)) {
			return out, fmt.Errorf("%w: %s must be integer NumericDate", ErrMalformedToken, timeName)
		}
		out.AuthenticatedAt = time.Unix(int64(seconds), 0)
		if out.AuthenticatedAt.After(lifetime.now.Add(skew)) {
			return out, fmt.Errorf("%w: %s is in the future", ErrTokenNotYetValid, timeName)
		}
	}
	return out, nil
}

type AssuranceRequirement struct {
	AcceptableACR        []string
	MinimumLevel         AssuranceLevel
	RequiredMethods      []AuthenticationMethod
	MaximumAge           time.Duration
	Agent                *AssuranceRequirement
	Human                *AssuranceRequirement
	TransactionReference string
}
type AssuranceRequest struct {
	CallerKind              CallerKind
	UserInteractionPossible bool
	Agent                   AuthenticationContext
	Human                   AuthenticationContext
}
type Decision int

const (
	DecisionDenied Decision = iota
	DecisionAllowed
	DecisionChallengeRequired
)

type Challenge struct {
	Reasons              []string
	RequiredACR          []string
	MaximumAge           time.Duration
	TransactionReference string
}

// Authorize evaluates base authorization first. Assurance is deliberately not
// inspected on denial, preventing a challenge from disclosing a protected
// resource or policy requirement.
func Authorize(baseAuthorized bool, request AssuranceRequest, requirement AssuranceRequirement, now time.Time) (Decision, *Challenge) {
	if !baseAuthorized {
		return DecisionDenied, nil
	}
	context, required := request.Human, requirement
	if request.CallerKind == CallerWorkload {
		context = request.Agent
	}
	if request.CallerKind == CallerAgentForHuman {
		if requirement.Agent != nil {
			if reasons := insufficient(request.Agent, *requirement.Agent, now); len(reasons) > 0 {
				return challenge(request, reasons, requirement.Agent)
			}
		}
		if requirement.Human != nil {
			if reasons := insufficient(request.Human, *requirement.Human, now); len(reasons) > 0 {
				return challenge(request, reasons, requirement.Human)
			}
		}
	}
	reasons := insufficient(context, required, now)
	if len(reasons) > 0 {
		return challenge(request, reasons, &required)
	}
	return DecisionAllowed, nil
}
func insufficient(c AuthenticationContext, r AssuranceRequirement, now time.Time) []string {
	var out []string
	if len(r.AcceptableACR) > 0 && !slices.Contains(r.AcceptableACR, c.ACR) {
		out = append(out, "acr")
	}
	if c.Level < r.MinimumLevel {
		out = append(out, "assurance_level")
	}
	for _, method := range r.RequiredMethods {
		if !slices.Contains(c.Methods, method) {
			out = append(out, "authentication_method")
			break
		}
	}
	if r.MaximumAge > 0 && (c.AuthenticatedAt.IsZero() || now.Sub(c.AuthenticatedAt) > r.MaximumAge) {
		out = append(out, "authentication_too_old")
	}
	return out
}
func challenge(request AssuranceRequest, reasons []string, r *AssuranceRequirement) (Decision, *Challenge) {
	if request.CallerKind == CallerWorkload || !request.UserInteractionPossible {
		return DecisionDenied, nil
	}
	return DecisionChallengeRequired, &Challenge{Reasons: reasons, RequiredACR: slices.Clone(r.AcceptableACR), MaximumAge: r.MaximumAge, TransactionReference: r.TransactionReference}
}

var ErrChallengeRequired = errors.New("auth: additional authentication required")

// WriteInsufficientUserAuthentication adapts an internal challenge to RFC
// 9470. Ordinary authorization denial must never be passed to this function.
func WriteInsufficientUserAuthentication(w http.ResponseWriter, challenge *Challenge) {
	value := `Bearer error="insufficient_user_authentication"`
	if challenge != nil && len(challenge.RequiredACR) > 0 {
		value += `, acr_values="` + strings.Join(challenge.RequiredACR, " ") + `"`
	}
	w.Header().Set("WWW-Authenticate", value)
	w.WriteHeader(http.StatusUnauthorized)
}
