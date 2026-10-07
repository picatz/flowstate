package auth

import (
	"testing"

	"google.golang.org/protobuf/types/known/structpb"
)

// overBoundWire is a claim set holding one claim nested past
// [MaxCarriedClaimDepth], which [WorkloadIdentity.WithWireClaims] refuses.
func overBoundWire() map[string]*structpb.Value {
	deep := structpb.NewStringValue("contractor")
	for range MaxCarriedClaimDepth + 1 {
		deep = structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{deep}})
	}

	return map[string]*structpb.Value{"contractors": deep, "team": structpb.NewStringValue("platform")}
}

// TestRefusedClaimsFailClosedOnTheAuthSurfaces is #2426 on the assumption and
// secret surfaces: an identity whose claims were refused must make a rule that
// only tests a claim's absence error, and so deny, instead of reading the
// refused claim as missing and permitting.
func TestRefusedClaimsFailClosedOnTheAuthSurfaces(t *testing.T) {
	t.Parallel()

	identity, ref := callerFixture()
	minted, err := identity.SubjectFor(ref)
	if err != nil {
		t.Fatalf("SubjectFor: %v", err)
	}

	refused := identity.WithWireClaims(overBoundWire())
	if refused.unreadable == nil {
		t.Fatal("the fixture claim must be over the bounds")
	}
	normal := identity.WithWireClaims(map[string]*structpb.Value{"team": structpb.NewStringValue("platform")})

	for _, rule := range []string{
		`!("contractors" in identity.claims)`,
		`!("contractors" in identity.claims) && identity.namespace != ""`,
		`identity.claims.all(k, k != "contractors")`,
		`size(identity.claims) == 1`,
	} {
		assume, err := compileAssumeRules([]string{rule}, nil, DefaultAssumeRuleCostLimit)
		if err != nil {
			t.Fatalf("compiling %q: %v", rule, err)
		}
		secrets, err := compileSecretRules([]string{rule}, nil, DefaultAssumeRuleCostLimit)
		if err != nil {
			t.Fatalf("compiling %q against the secret surface: %v", rule, err)
		}

		for caller, wantErr := range map[string]bool{"refused": true, "normal": false} {
			who := map[string]WorkloadIdentity{"refused": refused, "normal": normal}[caller]
			assumeAttrs := assumeVars("aws-prod", minted, "https://as.example.com", who, ref)
			secretAttrs := map[string]any{
				attrIdentity: assumeAttrs[attrIdentity],
				attrWorkload: assumeAttrs[attrWorkload],
				attrSecret:   secret{Scheme: "env", Name: "API_KEY"},
			}

			_, assumeErr := assume.Allow[0].Match(t.Context(), assumeAttrs)
			_, secretErr := secrets.Allow[0].Match(t.Context(), secretAttrs)
			if (assumeErr != nil) != wantErr || (secretErr != nil) != wantErr {
				t.Errorf("%s caller, %q: assume error = %v, secret error = %v, want error %v",
					caller, rule, assumeErr, secretErr, wantErr)
			}
		}
	}
}

// TestRefusedClaimsCallerKeepsEverythingElse pins that only the claims are
// withheld: the caller still names who it is, so a rule on another field is
// unchanged and an operator's audit line stays attributable.
func TestRefusedClaimsCallerKeepsEverythingElse(t *testing.T) {
	t.Parallel()

	identity, _ := callerFixture()
	caller := identity.WithWireClaims(overBoundWire()).Caller()

	if caller.Claims.Refused() == nil {
		t.Fatal("the caller must carry the refusal")
	}
	if caller.Subject != identity.Subject || caller.Namespace != identity.Namespace {
		t.Errorf("caller = %+v, want the identity's own subject and namespace", caller)
	}
	if got := identity.WithWireClaims(nil).Caller().Claims.Refused(); got != nil {
		t.Errorf("a caller with no claims must not be refused, got %v", got)
	}
}
