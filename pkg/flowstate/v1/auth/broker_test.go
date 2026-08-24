package auth_test

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingExchanger is a relying party that never fails, and records every
// assertion it was presented, so a test can assert what the broker minted without
// standing up an HTTP server.
type recordingExchanger struct {
	name           string
	audience       string
	subject        string
	lifetime       time.Duration
	clock          func() time.Time
	credentialType auth.CredentialType

	exchanges atomic.Int64

	mu   sync.Mutex
	seen []auth.Assertion
	err  error
}

// newRecordingExchanger returns an exchanger requiring the given audience.
func newRecordingExchanger(name, audience string, clock func() time.Time) *recordingExchanger {
	return &recordingExchanger{
		name:           name,
		audience:       audience,
		lifetime:       time.Hour,
		clock:          clock,
		credentialType: auth.CredentialBearer,
	}
}

// Name implements [auth.Exchanger].
func (e *recordingExchanger) Name() string { return e.name }

// Requirement implements [auth.Exchanger].
func (e *recordingExchanger) Requirement() auth.Requirement {
	return auth.Requirement{Audience: e.audience, Subject: e.subject}
}

// Exchange implements [auth.Exchanger].
func (e *recordingExchanger) Exchange(ctx context.Context, assertion auth.Assertion) (auth.Credential, error) {
	e.exchanges.Add(1)

	e.mu.Lock()
	e.seen = append(e.seen, assertion)
	failure := e.err
	e.mu.Unlock()

	if failure != nil {
		return auth.Credential{}, failure
	}

	credential := auth.Credential{
		Type:      e.credentialType,
		Provider:  e.name,
		ExpiresAt: e.clock().Add(e.lifetime),
	}

	return credential, nil
}

// assertions returns every assertion this exchanger was presented.
func (e *recordingExchanger) assertions() []auth.Assertion {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]auth.Assertion(nil), e.seen...)
}

// fail makes subsequent exchanges return err.
func (e *recordingExchanger) fail(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.err = err
}

// TestBrokerAssumePolicy covers who may assume what. Deny must win, an errored rule
// must refuse, and a target nobody configured must never be reachable.
func TestBrokerAssumePolicy(t *testing.T) {
	clock := authtest.NewClock(referenceTime)

	tests := []struct {
		name       string
		allow      []string
		deny       []string
		identity   auth.WorkloadIdentity
		ref        auth.StepRef
		target     string
		wantErr    error
		wantReason auth.AssumeReason
	}{
		{
			name:     "no rules, so any workload may use a configured target",
			identity: testIdentity(),
			ref:      testStepRef(),
			target:   "aws-prod",
		},
		{
			name:     "an allow rule that matches the workload",
			allow:    []string{`target == "aws-prod" && workload.namespace == "acme"`},
			identity: testIdentity(),
			ref:      testStepRef(),
			target:   "aws-prod",
		},
		{
			name:     "an allow rule matching who the workload acts for",
			allow:    []string{`workload.on_behalf_of.startsWith("repo:picatz/flowstate:")`},
			identity: testIdentity(),
			ref:      testStepRef(),
			target:   "aws-prod",
		},
		{
			name:     "an allow rule matching a carried claim",
			allow:    []string{`workload.claims["repository"] == "picatz/flowstate"`},
			identity: testIdentity(),
			ref:      testStepRef(),
			target:   "aws-prod",
		},
		{
			name:     "an allow rule matching the derived subject",
			allow:    []string{`workload.subject.startsWith("flowstate:acme/prod/")`},
			identity: testIdentity(),
			ref:      testStepRef(),
			target:   "aws-prod",
		},
		{
			name:       "no allow rule matches",
			allow:      []string{`workload.namespace == "someone-else"`},
			identity:   testIdentity(),
			ref:        testStepRef(),
			target:     "aws-prod",
			wantErr:    auth.ErrAssumeDenied,
			wantReason: auth.ReasonAssumeNoAllowRule,
		},
		{
			name:       "a deny rule matches",
			deny:       []string{`workload.step == "push-image"`},
			identity:   testIdentity(),
			ref:        testStepRef(),
			target:     "aws-prod",
			wantErr:    auth.ErrAssumeDenied,
			wantReason: auth.ReasonAssumeDenyRule,
		},
		{
			name:       "deny beats allow",
			allow:      []string{`workload.namespace == "acme"`},
			deny:       []string{`workload.workflow == "deploy-service"`},
			identity:   testIdentity(),
			ref:        testStepRef(),
			target:     "aws-prod",
			wantErr:    auth.ErrAssumeDenied,
			wantReason: auth.ReasonAssumeDenyRule,
		},
		{
			name:       "a rule that cannot be evaluated refuses",
			allow:      []string{`workload.claims["missing"] == "x"`},
			identity:   auth.WorkloadIdentity{Subject: "s", Issuer: "https://idp.example.com"},
			ref:        testStepRef(),
			target:     "aws-prod",
			wantErr:    auth.ErrAssumeDenied,
			wantReason: auth.ReasonAssumeRuleError,
		},
		{
			name:     "a target that is not configured",
			identity: testIdentity(),
			ref:      testStepRef(),
			target:   "aws-staging",
			wantErr:  auth.ErrUnknownTarget,
		},
		{
			name:     "a workload with no identity",
			identity: auth.WorkloadIdentity{},
			ref:      testStepRef(),
			target:   "aws-prod",
			wantErr:  auth.ErrInvalidIdentity,
		},
		{
			name:     "a request that does not name the work",
			identity: testIdentity(),
			ref:      auth.StepRef{},
			target:   "aws-prod",
			wantErr:  auth.ErrInvalidIdentity,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			issuer, _ := newIssuer(t, clock)
			exchanger := newRecordingExchanger("aws-sts", "sts.amazonaws.com", clock.Now)

			broker, err := auth.NewBroker(issuer,
				auth.WithTarget("aws-prod", exchanger),
				auth.WithAssumeAllowRules(test.allow...),
				auth.WithAssumeDenyRules(test.deny...),
				auth.WithBrokerClock(clock.Now),
			)
			require.NoError(t, err)

			credential, err := broker.Credential(t.Context(), test.identity, test.ref, test.target)

			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
				require.True(t, credential.IsZero())

				// Nothing is minted for a request that is refused, so no assertion
				// signed by Flowstate exists for a workload that was not allowed
				// to ask.
				require.Zero(t, exchanger.exchanges.Load(), "a refused request must not reach the relying party")
				require.Empty(t, exchanger.assertions())

				if test.wantReason != "" {
					var denied *auth.AssumeDeniedError
					require.ErrorAs(t, err, &denied)
					require.Equal(t, test.wantReason, denied.Reason)
					require.Equal(t, test.target, denied.Target)
					require.NotEmpty(t, denied.Detail)
				}
				return
			}

			require.NoError(t, err)
			require.Equal(t, "aws-prod", credential.Target, "audit records name the operator's target")
			require.Equal(t, "aws-sts", credential.Provider)
			require.Equal(t, int64(1), exchanger.exchanges.Load())
		})
	}
}

// TestBrokerMintsScopedAssertions checks that the assertion presented to a relying
// party is the one that party requires, and no more.
func TestBrokerMintsScopedAssertions(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, server := newIssuer(t, clock)

	var (
		aws     = newRecordingExchanger("aws-sts", "sts.amazonaws.com", clock.Now)
		partner = newRecordingExchanger("token-exchange", "https://partner.example.com", clock.Now)
	)

	broker, err := auth.NewBroker(issuer,
		auth.WithTarget("aws-prod", aws),
		auth.WithTarget("partner", partner),
		auth.WithBrokerClock(clock.Now),
	)
	require.NoError(t, err)

	require.Equal(t, []string{"aws-prod", "partner"}, broker.Targets())
	require.Equal(t, issuer, broker.Issuer())

	_, err = broker.Credential(t.Context(), testIdentity(), testStepRef(), "aws-prod")
	require.NoError(t, err)

	_, err = broker.Credential(t.Context(), testIdentity(), testStepRef(), "partner")
	require.NoError(t, err)

	// Each relying party got an assertion addressed only to it, which is what
	// stops one from replaying it at the other.
	awsAssertions := aws.assertions()
	require.Len(t, awsAssertions, 1)
	require.Equal(t, "sts.amazonaws.com", awsAssertions[0].Audience)
	require.Equal(t, "flowstate:acme/prod/deploy-service/push-image", awsAssertions[0].Subject)
	require.Equal(t, server.URL, awsAssertions[0].Issuer)

	partnerAssertions := partner.assertions()
	require.Len(t, partnerAssertions, 1)
	require.Equal(t, "https://partner.example.com", partnerAssertions[0].Audience)

	require.NotEqual(t, awsAssertions[0].ID, partnerAssertions[0].ID, "each assertion is distinct")
}

// TestBrokerSubjectOverride checks that a protocol dictating its own subject gets
// it, while policy still evaluates the workload's real subject. Otherwise an
// override would be a way to escape the assumption rules.
func TestBrokerSubjectOverride(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	exchanger := newRecordingExchanger("client-credentials", "https://as.example.com", clock.Now)
	exchanger.subject = "flowstate-prod" // as RFC 7523 client authentication requires

	broker, err := auth.NewBroker(issuer,
		auth.WithTarget("partner", exchanger),
		// The rule names the workload's real subject, not the override.
		auth.WithAssumeAllowRules(`workload.subject == "flowstate:acme/prod/deploy-service/push-image"`),
		auth.WithBrokerClock(clock.Now),
	)
	require.NoError(t, err)

	_, err = broker.Credential(t.Context(), testIdentity(), testStepRef(), "partner")
	require.NoError(t, err, "policy must evaluate the workload subject, not the protocol's override")

	assertions := exchanger.assertions()
	require.Len(t, assertions, 1)
	require.Equal(t, "flowstate-prod", assertions[0].Subject, "the protocol's subject is what gets minted")

	// A rule written against the override does not match, because that is not the
	// workload's identity.
	denying, err := auth.NewBroker(issuer,
		auth.WithTarget("partner", exchanger),
		auth.WithAssumeAllowRules(`workload.subject == "flowstate-prod"`),
		auth.WithBrokerClock(clock.Now),
	)
	require.NoError(t, err)

	_, err = denying.Credential(t.Context(), testIdentity(), testStepRef(), "partner")
	require.ErrorIs(t, err, auth.ErrAssumeDenied)
}

// TestBrokerCaching checks that a short-lived credential is reused until shortly
// before it expires, and then replaced.
func TestBrokerCaching(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	exchanger := newRecordingExchanger("aws-sts", "sts.amazonaws.com", clock.Now)
	exchanger.lifetime = 10 * time.Minute

	broker, err := auth.NewBroker(issuer,
		auth.WithTarget("aws-prod", exchanger),
		auth.WithBrokerClock(clock.Now),
		auth.WithRefreshMargin(time.Minute),
	)
	require.NoError(t, err)

	first, err := broker.Credential(t.Context(), testIdentity(), testStepRef(), "aws-prod")
	require.NoError(t, err)

	second, err := broker.Credential(t.Context(), testIdentity(), testStepRef(), "aws-prod")
	require.NoError(t, err)

	require.Equal(t, int64(1), exchanger.exchanges.Load(), "a valid credential should be reused")
	require.Equal(t, first.ExpiresAt, second.ExpiresAt)

	// Still comfortably valid.
	clock.Advance(5 * time.Minute)
	_, err = broker.Credential(t.Context(), testIdentity(), testStepRef(), "aws-prod")
	require.NoError(t, err)
	require.Equal(t, int64(1), exchanger.exchanges.Load())

	// Inside the refresh margin: replaced before it stops working, rather than
	// handed out with seconds left.
	clock.Advance(4*time.Minute + 30*time.Second)
	third, err := broker.Credential(t.Context(), testIdentity(), testStepRef(), "aws-prod")
	require.NoError(t, err)
	require.Equal(t, int64(2), exchanger.exchanges.Load(), "a credential near expiry must be exchanged again")
	require.True(t, third.ExpiresAt.After(first.ExpiresAt))

	// A credential is never held indefinitely: past its expiry a new one is
	// obtained even if nothing else changed.
	clock.Advance(time.Hour)
	_, err = broker.Credential(t.Context(), testIdentity(), testStepRef(), "aws-prod")
	require.NoError(t, err)
	require.Equal(t, int64(3), exchanger.exchanges.Load())
}

// TestBrokerCacheIsolation is the cross-tenant test. Two runs of the same step can
// act for different callers, and a relying party may well authorize on that, so a
// credential obtained for one caller must never be handed to the other.
func TestBrokerCacheIsolation(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	exchanger := newRecordingExchanger("aws-sts", "sts.amazonaws.com", clock.Now)

	broker, err := auth.NewBroker(issuer,
		auth.WithTarget("aws-prod", exchanger),
		auth.WithBrokerClock(clock.Now),
	)
	require.NoError(t, err)

	base := testIdentity()

	// Each of these differs from the base identity in exactly one way that a
	// relying party could authorize on, so each must get its own credential.
	variants := map[string]auth.WorkloadIdentity{
		"same identity": base,
		"another caller": func() auth.WorkloadIdentity {
			other := base
			other.Subject = "repo:attacker/fork:ref:refs/heads/main"
			return other
		}(),
		"another issuer": func() auth.WorkloadIdentity {
			other := base
			other.Issuer = "https://gitlab.example.com"
			return other
		}(),
		"another namespace": func() auth.WorkloadIdentity {
			other := base
			other.Namespace = "other-tenant"
			return other
		}(),
		"another carried claim": func() auth.WorkloadIdentity {
			other := base
			other.Claims = map[string]string{"repository": "attacker/fork"}
			return other
		}(),
	}

	for name, identity := range variants {
		_, err := broker.Credential(t.Context(), identity, testStepRef(), name)
		require.ErrorIs(t, err, auth.ErrUnknownTarget, "guard: only aws-prod is configured")

		_, err = broker.Credential(t.Context(), identity, testStepRef(), "aws-prod")
		require.NoError(t, err)
	}

	// The first identity is cached; every other variant caused its own exchange.
	require.Equal(t, int64(len(variants)), exchanger.exchanges.Load(),
		"a credential must not be shared between workloads that differ in anything a relying party can see")

	// Asking again for the identity that was already seen reuses its credential.
	_, err = broker.Credential(t.Context(), base, testStepRef(), "aws-prod")
	require.NoError(t, err)
	require.Equal(t, int64(len(variants)), exchanger.exchanges.Load())

	// A different step of the same workload is a different subject, so a
	// different credential.
	_, err = broker.Credential(t.Context(), base, auth.StepRef{Workflow: "deploy-service", Step: "other-step"}, "aws-prod")
	require.NoError(t, err)
	require.Equal(t, int64(len(variants)+1), exchanger.exchanges.Load())
}

// TestBrokerExchangeFailure checks that a failed exchange is reported and not
// cached, so a transient failure does not become a lasting one.
func TestBrokerExchangeFailure(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	exchanger := newRecordingExchanger("aws-sts", "sts.amazonaws.com", clock.Now)
	exchanger.fail(auth.ErrExchangeFailed)

	broker, err := auth.NewBroker(issuer,
		auth.WithTarget("aws-prod", exchanger),
		auth.WithBrokerClock(clock.Now),
	)
	require.NoError(t, err)

	_, err = broker.Credential(t.Context(), testIdentity(), testStepRef(), "aws-prod")
	require.ErrorIs(t, err, auth.ErrExchangeFailed)

	exchanger.fail(nil)

	credential, err := broker.Credential(t.Context(), testIdentity(), testStepRef(), "aws-prod")
	require.NoError(t, err, "a failure must not be cached")
	require.False(t, credential.IsZero())
}

// TestBrokerRejectsUnusableCredentials checks that an exchanger returning something
// unusable is an error rather than a credential nobody can rely on.
func TestBrokerRejectsUnusableCredentials(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	t.Run("a credential with no expiry", func(t *testing.T) {
		exchanger := newRecordingExchanger("forever", "https://as.example.com", clock.Now)
		exchanger.lifetime = 0
		exchanger.clock = func() time.Time { return time.Time{} }

		broker, err := auth.NewBroker(issuer,
			auth.WithTarget("forever", exchanger),
			auth.WithBrokerClock(clock.Now),
		)
		require.NoError(t, err)

		_, err = broker.Credential(t.Context(), testIdentity(), testStepRef(), "forever")
		require.ErrorIs(t, err, auth.ErrExchangeFailed)
	})

	t.Run("a credential with no type", func(t *testing.T) {
		exchanger := newRecordingExchanger("typeless", "https://as.example.com", clock.Now)
		exchanger.credentialType = ""

		broker, err := auth.NewBroker(issuer,
			auth.WithTarget("typeless", exchanger),
			auth.WithBrokerClock(clock.Now),
		)
		require.NoError(t, err)

		_, err = broker.Credential(t.Context(), testIdentity(), testStepRef(), "typeless")
		require.ErrorIs(t, err, auth.ErrExchangeFailed)
	})
}

// TestNewBrokerRejectsBadConfiguration checks that unusable configuration fails at
// startup, where an operator will see it.
func TestNewBrokerRejectsBadConfiguration(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	exchanger := newRecordingExchanger("aws-sts", "sts.amazonaws.com", clock.Now)

	tests := []struct {
		name string
		opts []auth.BrokerOption
	}{
		{
			name: "a rule that does not compile",
			opts: []auth.BrokerOption{auth.WithAssumeAllowRules(`workload.namespace ==`)},
		},
		{
			name: "a rule referencing an attribute that does not exist",
			opts: []auth.BrokerOption{auth.WithAssumeAllowRules(`workload.tenant == "acme"`)},
		},
		{
			name: "a rule that does not produce a boolean",
			opts: []auth.BrokerOption{auth.WithAssumeAllowRules(`workload.namespace`)},
		},
		{
			name: "an empty rule",
			opts: []auth.BrokerOption{auth.WithAssumeDenyRules("")},
		},
		{
			name: "the same target twice",
			opts: []auth.BrokerOption{
				auth.WithTarget("aws-prod", exchanger),
				auth.WithTarget("aws-prod", exchanger),
			},
		},
		{
			name: "a target with no name",
			opts: []auth.BrokerOption{auth.WithTarget("", exchanger)},
		},
		{
			name: "a target with no exchanger",
			opts: []auth.BrokerOption{auth.WithTarget("aws-prod", nil)},
		},
		{
			name: "an exchanger that requires no audience",
			opts: []auth.BrokerOption{auth.WithTarget("anywhere", newRecordingExchanger("anywhere", "", clock.Now))},
		},
		{
			name: "a negative refresh margin",
			opts: []auth.BrokerOption{auth.WithRefreshMargin(-time.Minute)},
		},
		{
			name: "a cache that holds nothing",
			opts: []auth.BrokerOption{auth.WithMaxCachedCredentials(0)},
		},
		{
			name: "no rule cost limit",
			opts: []auth.BrokerOption{
				auth.WithAssumeAllowRules(`workload.namespace == "acme"`),
				auth.WithAssumeRuleCostLimit(0),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			broker, err := auth.NewBroker(issuer, test.opts...)
			require.ErrorIs(t, err, auth.ErrInvalidPolicy)
			require.Nil(t, broker)
		})
	}

	t.Run("no issuer", func(t *testing.T) {
		broker, err := auth.NewBroker(nil, auth.WithTarget("aws-prod", exchanger))
		require.ErrorIs(t, err, auth.ErrInvalidPolicy)
		require.Nil(t, broker)
	})

	t.Run("no targets at all", func(t *testing.T) {
		broker, err := auth.NewBroker(issuer)
		require.NoError(t, err, "a broker with no targets is valid")

		_, err = broker.Credential(t.Context(), testIdentity(), testStepRef(), "anything")
		require.ErrorIs(t, err, auth.ErrUnknownTarget, "and refuses everything")
	})
}

// TestBrokerAuthorize checks the path a task actually uses: the credential goes
// straight onto the request, so the task never holds the secret.
func TestBrokerAuthorize(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	party := newRelyingParty(t, func(w http.ResponseWriter, r *http.Request, body recordedRequest) {
		writeJSON(t, w, http.StatusOK, map[string]any{
			"access_token":      "downstream-token",
			"issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
			"token_type":        "Bearer",
			"expires_in":        3600,
		})
	})

	exchanger, err := auth.NewTokenExchanger(auth.TokenExchangeConfig{
		TokenURL: party.url + "/token",
		Audience: "https://as.example.com",
		Clock:    clock.Now,
	})
	require.NoError(t, err)

	broker, err := auth.NewBroker(issuer,
		auth.WithTarget("partner", exchanger),
		auth.WithAssumeAllowRules(`target == "partner" && workload.namespace == "acme"`),
		auth.WithBrokerClock(clock.Now),
	)
	require.NoError(t, err)

	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://api.partner.example.com/things", nil)
	require.NoError(t, err)

	require.NoError(t, broker.Authorize(t.Context(), request, testIdentity(), testStepRef(), "partner"))
	require.Equal(t, "Bearer downstream-token", request.Header.Get("Authorization"))

	t.Run("a refused request leaves the header alone", func(t *testing.T) {
		denied, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://api.partner.example.com/things", nil)
		require.NoError(t, err)

		other := testIdentity()
		other.Namespace = "someone-else"

		err = broker.Authorize(t.Context(), denied, other, testStepRef(), "partner")
		require.ErrorIs(t, err, auth.ErrAssumeDenied)
		require.Empty(t, denied.Header.Get("Authorization"))
	})
}

// TestBrokerConcurrent checks that simultaneous requests for the same credential
// produce one exchange rather than one each, that different targets do not wait for
// each other, and that the broker holds up under the race detector.
func TestBrokerConcurrent(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	var (
		aws     = newRecordingExchanger("aws-sts", "sts.amazonaws.com", clock.Now)
		partner = newRecordingExchanger("token-exchange", "https://partner.example.com", clock.Now)
	)

	broker, err := auth.NewBroker(issuer,
		auth.WithTarget("aws-prod", aws),
		auth.WithTarget("partner", partner),
		auth.WithBrokerClock(clock.Now),
	)
	require.NoError(t, err)

	const callers = 24

	var wait sync.WaitGroup
	for i := range callers {
		target := "aws-prod"
		if i%2 == 1 {
			target = "partner"
		}

		wait.Go(func() {
			credential, err := broker.Credential(t.Context(), testIdentity(), testStepRef(), target)
			assert.NoError(t, err)
			assert.Equal(t, target, credential.Target)
		})
	}
	wait.Wait()

	require.Equal(t, int64(1), aws.exchanges.Load(), "concurrent requests should share one exchange")
	require.Equal(t, int64(1), partner.exchanges.Load())
}

// TestBrokerCacheIsBounded checks that a long-running worker cannot accumulate
// credentials without limit. Every step of every run is its own identity, so an
// unbounded cache would grow forever.
func TestBrokerCacheIsBounded(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	exchanger := newRecordingExchanger("aws-sts", "sts.amazonaws.com", clock.Now)
	exchanger.lifetime = time.Minute

	broker, err := auth.NewBroker(issuer,
		auth.WithTarget("aws-prod", exchanger),
		auth.WithBrokerClock(clock.Now),
		auth.WithMaxCachedCredentials(8),
		auth.WithRefreshMargin(time.Second),
	)
	require.NoError(t, err)

	// Far more distinct workloads than the cache can hold.
	for i := range 200 {
		ref := auth.StepRef{Workflow: "deploy-service", Step: fmt.Sprintf("step-%d", i)}

		_, err := broker.Credential(t.Context(), testIdentity(), ref, "aws-prod")
		require.NoError(t, err)

		// Expire everything periodically, which is what a real worker's
		// short-lived credentials do.
		if i%20 == 0 {
			clock.Advance(2 * time.Minute)
		}
	}

	// Every request was served, which is the observable behavior; the cache having
	// stayed bounded is what the limit is for.
	require.Equal(t, int64(200), exchanger.exchanges.Load())
}
