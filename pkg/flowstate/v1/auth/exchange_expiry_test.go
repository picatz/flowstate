package auth_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/stretchr/testify/require"
)

// A relying party's own expiry timestamp is compared against this host's clock,
// and the two clocks are not the same clock. Everything in this file is about
// that gap: an upper bound with no slack in it refuses a correct answer whenever
// the worker is behind the provider, and slack too generous stops being a bound.
//
// The OAuth expires_in path deliberately has no equivalent, because expires_in is
// a duration the provider measures on its own clock — TestTokenResponseCredential
// LifetimeBoundaries is where that path is pinned.

// awsSessionXML is an AssumeRoleWithWebIdentity response with the given
// expiration, or with the element absent entirely when expiration is zero.
func awsSessionXML(expiration time.Time) string {
	element := ""
	if !expiration.IsZero() {
		element = "<Expiration>" + expiration.UTC().Format(time.RFC3339) + "</Expiration>"
	}

	return `<AssumeRoleWithWebIdentityResponse>
  <AssumeRoleWithWebIdentityResult>
    <Credentials>
      <AccessKeyId>ASIAEXAMPLE</AccessKeyId>
      <SecretAccessKey>secret-key</SecretAccessKey>
      <SessionToken>session-token</SessionToken>
      ` + element + `
    </Credentials>
  </AssumeRoleWithWebIdentityResult>
</AssumeRoleWithWebIdentityResponse>`
}

// TestAWSExchangerBoundsSessionExpiration walks the expirations STS could report
// for a one-hour session against a worker whose clock is not STS's.
//
// The case that matters most is the second one. STS computes Expiration against
// its own clock, so a worker running even a few seconds behind sees a perfectly
// honoured hour land after now+1h. A bound with no slack refuses that, and it
// refuses it every single time until somebody fixes NTP — a whole target down for
// a reason that reads like the provider is broken.
//
// The rest of the table is the other direction, which is the reason the slack is
// [auth.DefaultClockSkew] and not something roomier: past the allowance the answer
// is refused again, so the tolerance stays a tolerance rather than becoming a way
// for a relying party to hand Flowstate a session far longer than it asked for.
func TestAWSExchangerBoundsSessionExpiration(t *testing.T) {
	const duration = time.Hour

	tests := []struct {
		name       string
		expiration time.Time
		wantErr    string
	}{
		{
			name:       "exactly the session that was requested",
			expiration: referenceTime.Add(duration),
		},
		{
			name:       "a worker clock lagging STS by ten seconds",
			expiration: referenceTime.Add(duration + 10*time.Second),
		},
		{
			name:       "the whole skew allowance",
			expiration: referenceTime.Add(duration + auth.DefaultClockSkew),
		},
		{
			name:       "one second past the skew allowance",
			expiration: referenceTime.Add(duration + auth.DefaultClockSkew + time.Second),
			wantErr:    "outside the 1h0m0s session that was requested",
		},
		{
			name:       "a session twice as long as the one requested",
			expiration: referenceTime.Add(2 * duration),
			wantErr:    "outside the 1h0m0s session that was requested",
		},
		{
			name:       "a session that has already expired",
			expiration: referenceTime.Add(-time.Second),
			wantErr:    "outside the 1h0m0s session that was requested",
		},
		{
			name:    "no expiration at all",
			wantErr: "no expiration",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clock := authtest.NewClock(referenceTime)
			issuer, _ := newIssuer(t, clock)

			party := newRelyingParty(t, func(w http.ResponseWriter, r *http.Request, body recordedRequest) {
				w.Header().Set("Content-Type", "text/xml")
				_, _ = w.Write([]byte(awsSessionXML(test.expiration)))
			})

			exchanger, err := auth.NewAWSExchanger(auth.AWSConfig{
				RoleARN:  "arn:aws:iam::123456789012:role/flowstate",
				Endpoint: party.url + "/",
				Duration: duration,
				Clock:    clock.Now,
			})
			require.NoError(t, err)

			credential, err := exchanger.Exchange(t.Context(),
				mintAssertion(t, issuer, exchanger.Requirement().Audience))

			if test.wantErr != "" {
				require.ErrorIs(t, err, auth.ErrExchangeFailed)
				require.ErrorContains(t, err, test.wantErr)
				require.True(t, credential.IsZero(), "a refused exchange must return no credential")
				return
			}

			require.NoError(t, err)
			require.Equal(t, test.expiration.UTC(), credential.ExpiresAt.UTC(),
				"the credential expires when STS said it does, never when Flowstate guessed")
		})
	}
}

// TestGCPExchangerBoundsServiceAccountExpiry is the same walk over Google's IAM
// Credentials API, which reports an RFC 3339 expireTime rather than a duration.
func TestGCPExchangerBoundsServiceAccountExpiry(t *testing.T) {
	const (
		pool     = "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/flowstate"
		lifetime = 30 * time.Minute
	)

	tests := []struct {
		name       string
		expireTime time.Time
		wantErr    bool
	}{
		{name: "exactly the lifetime that was requested", expireTime: referenceTime.Add(lifetime)},
		{name: "a worker clock lagging Google by ten seconds", expireTime: referenceTime.Add(lifetime + 10*time.Second)},
		{name: "the whole skew allowance", expireTime: referenceTime.Add(lifetime + auth.DefaultClockSkew)},
		{name: "one second past the skew allowance", expireTime: referenceTime.Add(lifetime + auth.DefaultClockSkew + time.Second), wantErr: true},
		{name: "an hour when half an hour was requested", expireTime: referenceTime.Add(time.Hour), wantErr: true},
		{name: "already expired", expireTime: referenceTime.Add(-time.Second), wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clock := authtest.NewClock(referenceTime)
			issuer, _ := newIssuer(t, clock)

			party := newRelyingParty(t, func(w http.ResponseWriter, r *http.Request, body recordedRequest) {
				if r.URL.Path == "/v1/token" {
					writeJSON(t, w, http.StatusOK, map[string]any{
						"access_token": "federated-token",
						"token_type":   "Bearer",
						"expires_in":   3600,
					})
					return
				}
				writeJSON(t, w, http.StatusOK, map[string]any{
					"accessToken": "service-account-token",
					"expireTime":  test.expireTime.UTC().Format(time.RFC3339),
				})
			})

			exchanger, err := auth.NewGCPExchanger(auth.GCPConfig{
				Audience:            pool,
				Endpoint:            party.url + "/v1/token",
				IAMEndpoint:         party.url + "/iam/v1",
				ServiceAccountEmail: "flowstate@project.iam.gserviceaccount.com",
				Lifetime:            lifetime,
				Clock:               clock.Now,
			})
			require.NoError(t, err)

			credential, err := exchanger.Exchange(t.Context(), mintAssertion(t, issuer, pool))

			if test.wantErr {
				require.ErrorIs(t, err, auth.ErrExchangeFailed)
				require.ErrorContains(t, err, "outside the 30m0s lifetime that was requested")
				require.True(t, credential.IsZero(), "a refused exchange must return no credential")
				return
			}

			require.NoError(t, err)
			require.Equal(t, test.expireTime.UTC(), credential.ExpiresAt.UTC())
		})
	}
}

// TestGCPExchangerLifetimePolicy covers what a GCP target may configure, and the
// two ways the check can be wrong in opposite directions.
//
// Refusing too much: Lifetime is documented as ignored without a service account,
// and iam_endpoint four lines above it in the same constructor is already gated
// that way — so validating it unconditionally turns a field that does nothing into
// a field that can stop a deployment from starting. Twelve hours is legal too, for
// a project that has set the credential lifetime extension constraint; Google
// enforces that constraint, and refusing at an hour would be Flowstate guessing at
// somebody else's policy.
//
// Refusing too little: a sub-second lifetime is sent as int(seconds) + "s", so
// 500ms becomes "0s" — a request Google reads as naming no lifetime, answers with
// an hour-long token, and which impersonate then refuses as out of policy on every
// exchange. That target can never succeed, so it is refused where an operator can
// still see why.
func TestGCPExchangerLifetimePolicy(t *testing.T) {
	const pool = "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/flowstate"

	tests := []struct {
		name           string
		serviceAccount string
		lifetime       time.Duration
		wantErr        bool
	}{
		{name: "no lifetime, no impersonation"},
		{name: "an ignored lifetime is not validated", lifetime: 2 * time.Hour},
		{name: "an ignored negative lifetime is not validated either", lifetime: -time.Hour},
		{
			name:           "no lifetime under impersonation takes Google's default",
			serviceAccount: "flowstate@project.iam.gserviceaccount.com",
		},
		{
			name:           "one second is the shortest that survives truncation",
			serviceAccount: "flowstate@project.iam.gserviceaccount.com",
			lifetime:       time.Second,
		},
		{
			name:           "two hours, which the extension constraint allows",
			serviceAccount: "flowstate@project.iam.gserviceaccount.com",
			lifetime:       2 * time.Hour,
		},
		{
			name:           "twelve hours, the longest Google issues",
			serviceAccount: "flowstate@project.iam.gserviceaccount.com",
			lifetime:       12 * time.Hour,
		},
		{
			name:           "a sub-second lifetime truncates to no lifetime at all",
			serviceAccount: "flowstate@project.iam.gserviceaccount.com",
			lifetime:       500 * time.Millisecond,
			wantErr:        true,
		},
		{
			name:           "a negative lifetime",
			serviceAccount: "flowstate@project.iam.gserviceaccount.com",
			lifetime:       -time.Hour,
			wantErr:        true,
		},
		{
			name:           "longer than Google will ever issue",
			serviceAccount: "flowstate@project.iam.gserviceaccount.com",
			lifetime:       13 * time.Hour,
			wantErr:        true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := auth.NewGCPExchanger(auth.GCPConfig{
				Audience:            pool,
				ServiceAccountEmail: test.serviceAccount,
				Lifetime:            test.lifetime,
			})

			if test.wantErr {
				require.ErrorIs(t, err, auth.ErrInvalidPolicy)
				require.ErrorContains(t, err, "service account lifetime")
				return
			}

			require.NoError(t, err)
		})
	}
}

// TestFederationPolicyCredentialLifetimeCeiling is the reachability half: the
// ceiling is only worth anything if it can be written in the file an operator
// actually edits, and if the hard bound cannot be configured away there.
//
// Both directions are here on purpose. A test that only proves a five-minute
// ceiling refuses an hour-long token proves the knob is wired up; it says nothing
// about whether an operator can simply write a week and be handed a week.
func TestFederationPolicyCredentialLifetimeCeiling(t *testing.T) {
	policyFor := func(t *testing.T, tokenURL, ceiling string) (auth.FederationPolicy, error) {
		t.Helper()

		return auth.ParseFederationPolicy([]byte(`
issuer: https://flowstate.example.com
declared_claims: [repository]
allow:
  - 'target == "partner"'
targets:
  - name: partner
    token_exchange:
      token_url: ` + tokenURL + `
      audience: https://as.partner.example.com
      max_credential_lifetime: ` + ceiling + `
`))
	}

	// exchange builds a broker from the policy and asks it for the target's
	// credential, against a relying party that reports the given expires_in.
	exchange := func(t *testing.T, ceiling string, expiresIn int) (auth.Credential, error) {
		t.Helper()

		clock := authtest.NewClock(referenceTime)

		party := newRelyingParty(t, func(w http.ResponseWriter, r *http.Request, body recordedRequest) {
			writeJSON(t, w, http.StatusOK, map[string]any{
				"access_token":      "partner-token",
				"issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
				"token_type":        "Bearer",
				"expires_in":        expiresIn,
			})
		})

		policy, err := policyFor(t, party.url+"/oauth2/token", ceiling)
		require.NoError(t, err)

		key, err := auth.GenerateSigningKey("k", jwa.ES256)
		require.NoError(t, err)

		broker, err := policy.Broker(key, auth.WithFederationClock(clock.Now))
		require.NoError(t, err)

		return broker.Credential(t.Context(), testIdentity(), testStepRef(), "partner")
	}

	t.Run("a token inside the ceiling the file names", func(t *testing.T) {
		credential, err := exchange(t, "5m", 300)
		require.NoError(t, err)
		require.Equal(t, referenceTime.Add(5*time.Minute), credential.ExpiresAt.UTC(),
			"the credential expires when the provider said it does")
	})

	t.Run("a token longer than the ceiling the file names", func(t *testing.T) {
		credential, err := exchange(t, "5m", 3600)
		require.ErrorIs(t, err, auth.ErrExchangeFailed)
		require.ErrorContains(t, err, "longer than the 5m0s this target allows")
		require.True(t, credential.IsZero())
	})

	t.Run("the default applies to a target that names no ceiling", func(t *testing.T) {
		clock := authtest.NewClock(referenceTime)

		party := newRelyingParty(t, func(w http.ResponseWriter, r *http.Request, body recordedRequest) {
			writeJSON(t, w, http.StatusOK, map[string]any{
				"access_token":      "partner-token",
				"issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
				"token_type":        "Bearer",
				"expires_in":        int(auth.DefaultMaxCredentialLifetime.Seconds()) + 1,
			})
		})

		policy, err := auth.ParseFederationPolicy([]byte(`
issuer: https://flowstate.example.com
declared_claims: [repository]
allow:
  - 'target == "partner"'
targets:
  - name: partner
    token_exchange:
      token_url: ` + party.url + `/oauth2/token
      audience: https://as.partner.example.com
`))
		require.NoError(t, err)

		key, err := auth.GenerateSigningKey("k", jwa.ES256)
		require.NoError(t, err)

		broker, err := policy.Broker(key, auth.WithFederationClock(clock.Now))
		require.NoError(t, err)

		_, err = broker.Credential(t.Context(), testIdentity(), testStepRef(), "partner")
		require.ErrorIs(t, err, auth.ErrExchangeFailed,
			"a target that names no ceiling is bounded anyway, not unbounded")
	})

	t.Run("a ceiling past the hard bound cannot be written", func(t *testing.T) {
		_, err := policyFor(t, "https://as.partner.example.com/oauth2/token", "48h")
		require.ErrorIs(t, err, auth.ErrInvalidPolicy)
		require.ErrorContains(t, err, "maximum credential lifetime 48h0m0s is outside")
	})

	t.Run("a sub-second ceiling cannot be written", func(t *testing.T) {
		_, err := policyFor(t, "https://as.partner.example.com/oauth2/token", "500ms")
		require.ErrorIs(t, err, auth.ErrInvalidPolicy)
	})

	t.Run("a negative ceiling cannot be written", func(t *testing.T) {
		_, err := policyFor(t, "https://as.partner.example.com/oauth2/token", "-1h")
		require.ErrorIs(t, err, auth.ErrInvalidPolicy)
	})
}
