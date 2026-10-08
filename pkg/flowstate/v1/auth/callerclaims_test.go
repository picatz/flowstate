package auth

import "testing"

// TestAssumeAndSecretRulesReadKindGroupsAndActions proves the two auth surfaces
// bind the one principal.Caller: `identity.kind`, a list claim, a nested claim
// and the `in` guard all work, and a claim the caller lacks denies rather than
// permits.
func TestAssumeAndSecretRulesReadKindGroupsAndActions(t *testing.T) {
	t.Parallel()

	identity, ref := callerFixture()
	identity.Kind = "workload"
	identity.Actions = []string{"run.start"}
	identity.Claims = map[string]any{
		"groups": []any{"eng", "oncall"},
		"slack":  map[string]any{"user": "U1"},
	}

	minted, err := identity.SubjectFor(ref)
	if err != nil {
		t.Fatalf("SubjectFor: %v", err)
	}

	for _, test := range []struct {
		rule    string
		want    bool
		wantErr bool
	}{
		{rule: `identity.kind == "workload"`, want: true},
		{rule: `identity.kind == "human"`, want: false},
		{rule: `"oncall" in identity.claims.groups`, want: true},
		{rule: `"sales" in identity.claims.groups`, want: false},
		{rule: `identity.claims.slack.user == "U1"`, want: true},
		{rule: `"run.start" in identity.actions`, want: true},
		{rule: `"groups" in identity.claims && "eng" in identity.claims.groups`, want: true},
		{rule: `"nope" in identity.claims && "x" in identity.claims.nope`, want: false},
		// Reading what the caller lacks is an error, never a permit.
		{rule: `"x" in identity.claims.nope`, wantErr: true},
	} {
		t.Run(test.rule, func(t *testing.T) {
			t.Parallel()

			assume, err := compileAssumeRules([]string{test.rule}, nil, DefaultAssumeRuleCostLimit)
			if err != nil {
				t.Fatalf("compiling %q: %v", test.rule, err)
			}
			secrets, err := compileSecretRules([]string{test.rule}, nil, DefaultAssumeRuleCostLimit)
			if err != nil {
				t.Fatalf("compiling %q against the secret surface: %v", test.rule, err)
			}

			assumeAttrs := assumeVars("aws-prod", minted, "https://as.example.com", identity, ref)
			secretAttrs := map[string]any{
				attrIdentity: assumeAttrs[attrIdentity],
				attrWorkload: assumeAttrs[attrWorkload],
				attrSecret:   secret{Scheme: "env", Name: "API_KEY"},
			}

			for name, run := range map[string]func() (bool, error){
				"assume": func() (bool, error) { return assume.Allow[0].Match(t.Context(), assumeAttrs) },
				"secret": func() (bool, error) { return secrets.Allow[0].Match(t.Context(), secretAttrs) },
			} {
				matched, err := run()
				if (err != nil) != test.wantErr {
					t.Errorf("%s: %q error = %v, want error %v", name, test.rule, err, test.wantErr)
				}
				if err == nil && matched != test.want {
					t.Errorf("%s: %q = %v, want %v", name, test.rule, matched, test.want)
				}
			}
		})
	}
}
