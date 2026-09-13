package main

import (
	"context"
	"errors"
	"slices"
	"time"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	josev1 "github.com/picatz/flowstate/plugins/jose/gen/jose/v1"
)

const (
	// maxTokenBytes bounds what a workflow may hand this task. The verifier
	// bounds it again; this refuses the obviously-not-a-token before a
	// signature check is attempted on it.
	maxTokenBytes = 64 << 10

	// maxAudienceBytes bounds the extra audience an input may require.
	maxAudienceBytes = 512

	// maxTrustNameBytes bounds the policy entry name an input may select.
	maxTrustNameBytes = 128

	// maxClaims bounds how many claims travel into a step's outputs. A token
	// is another party's document, and its claim set is a collection they
	// control.
	maxClaims = 128
)

func joseVerify(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	if verifier == nil {
		return nil, sdk.Failed("%v", trustRefusal)
	}

	var in josev1.VerifyInputs
	if err := sdk.DecodeInputs(inputs, &in); err != nil {
		return nil, err
	}

	token, err := tokenFrom(in.GetToken())
	if err != nil {
		return nil, err
	}
	if len(in.GetTrust()) > maxTrustNameBytes {
		return nil, sdk.InvalidInput("trust is %d bytes, over the %d-byte limit", len(in.GetTrust()), maxTrustNameBytes)
	}
	if len(in.GetAudience()) > maxAudienceBytes {
		return nil, sdk.InvalidInput("audience is %d bytes, over the %d-byte limit", len(in.GetAudience()), maxAudienceBytes)
	}

	principal, err := verifier.Verify(ctx, token)
	if err != nil {
		return nil, classifyVerification(err)
	}

	// The narrowing checks, after the signature: a token this policy would
	// refuse is refused whatever an input says, and these can only make the
	// answer stricter.
	if name := in.GetTrust(); name != "" && principal.IssuerName != name {
		return nil, sdk.PermissionDenied(
			"the token was verified, and it was admitted by the trust entry %q rather than the %q this step requires",
			truncate(principal.IssuerName, maxTrustNameBytes), truncate(name, maxTrustNameBytes))
	}
	if audience := in.GetAudience(); audience != "" && !slices.Contains(principal.Audience, audience) {
		// The token's own audiences are not echoed: they are a fact about
		// somebody else's token, and a refusal is not the place to publish one.
		return nil, sdk.PermissionDenied(
			"the token was verified and does not carry the audience this step requires")
	}

	return sdk.EncodeOutputs(&josev1.VerifyOutputs{
		Issuer:    principal.Issuer,
		Subject:   principal.Subject,
		TrustName: principal.IssuerName,
		Audience:  principal.Audience,
		ExpiresAt: formatTime(principal.ExpiresAt),
		IssuedAt:  formatTime(principal.IssuedAt),
		Claims:    boundedClaims(principal.Claims),
	})
}

// tokenFrom reads the token, whether it arrived as a resolved secret or as a
// literal.
//
// Both are accepted, which is the one place this plugin's input rules are looser
// than the credential-carrying tasks in this tree. A token to be *verified* is
// usually something the run received rather than something the deployment holds:
// refusing a literal would make the webhook case - the case this task exists for
// - impossible to write. The README says what a literal costs.
func tokenFrom(value *flowstatev1.Value) (string, error) {
	if value == nil {
		return "", sdk.InvalidInput("token is required")
	}

	switch kind := value.GetKind().(type) {
	case *flowstatev1.Value_Literal:
		text, ok := kind.Literal.GetKind().(*expr.Value_StringValue)
		if !ok || text.StringValue == "" {
			return "", sdk.InvalidInput("token must resolve to a non-empty string")
		}
		if len(text.StringValue) > maxTokenBytes {
			return "", sdk.InvalidInput("token is %d bytes, over the %d-byte limit", len(text.StringValue), maxTokenBytes)
		}
		return text.StringValue, nil
	case *flowstatev1.Value_SecretRef:
		return "", sdk.Failed(
			"token reached this plugin as an unresolved secret reference; the host resolves secret inputs before a plugin runs")
	default:
		return "", sdk.InvalidInput("token must resolve to a string")
	}
}

// classifyVerification turns the verifier's refusal into a classification.
//
// Every refusal is permanent: the same token verified again is the same token,
// and an expired one does not become valid by waiting. The exception is reaching
// the issuer's key set at all, which is a backend that could not be reached and
// is the one retryable case here.
func classifyVerification(err error) error {
	if errors.Is(err, auth.ErrIssuerUnavailable) {
		return sdk.Unavailable("the issuer's metadata or keys could not be fetched, so this token could not be verified: %v", err)
	}

	// A malformed token is the caller's mistake rather than a refusal of a
	// well-formed one, and a workflow dispatching on the two differently is
	// dispatching on something real: one is fixed by sending a token, the other
	// by being trusted.
	if errors.Is(err, auth.ErrMalformedToken) || errors.Is(err, auth.ErrNoToken) {
		return sdk.InvalidInput("the token could not be parsed: %v", err)
	}

	// The message is the verifier's own, which is written for an operator
	// reading a log and carries no token material.
	return sdk.PermissionDenied("the token was not verified: %v", err)
}

// boundedClaims renders the verified claim set for a step's outputs.
//
// Sorted and bounded: a claim set is another party's collection, and what
// reaches durable history should not depend on map iteration or on how many
// groups somebody's directory put in a token.
func boundedClaims(claims map[string]any) *expr.Value {
	if len(claims) == 0 {
		return sdk.Literal(map[string]any{})
	}

	keys := make([]string, 0, len(claims))
	for key := range claims {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	bounded := make(map[string]any, min(len(keys), maxClaims))
	for _, key := range keys {
		if len(bounded) == maxClaims {
			break
		}
		bounded[truncate(key, 128)] = claims[key]
	}
	return sdk.Literal(bounded)
}

// formatTime renders a verified time claim, empty when the token carried none.
func formatTime(at time.Time) string {
	if at.IsZero() {
		return ""
	}
	return at.UTC().Format(time.RFC3339)
}
