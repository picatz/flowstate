package auth_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/stretchr/testify/require"
)

// relyingParty is an in-process stand-in for a system that accepts a Flowstate
// assertion. It records what was sent, so a test can assert the protocol on the
// wire rather than only the credential that comes back.
type relyingParty struct {
	url string

	mu       sync.Mutex
	requests []recordedRequest
	handler  func(w http.ResponseWriter, r *http.Request, body recordedRequest)
}

// recordedRequest is one request a relying party received.
type recordedRequest struct {
	path        string
	form        url.Values
	json        map[string]any
	authHeader  string
	contentType string
}

// newRelyingParty starts a relying party that answers with the given handler.
func newRelyingParty(t *testing.T, handler func(w http.ResponseWriter, r *http.Request, body recordedRequest)) *relyingParty {
	t.Helper()

	party := &relyingParty{handler: handler}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorded := recordedRequest{
			path:        r.URL.Path,
			authHeader:  r.Header.Get("Authorization"),
			contentType: r.Header.Get("Content-Type"),
		}

		switch {
		case recorded.contentType == "application/x-www-form-urlencoded":
			require.NoError(t, r.ParseForm())
			recorded.form = r.PostForm
		case recorded.contentType == "application/json":
			require.NoError(t, json.NewDecoder(r.Body).Decode(&recorded.json))
		}

		party.mu.Lock()
		party.requests = append(party.requests, recorded)
		party.mu.Unlock()

		party.handler(w, r, recorded)
	}))
	t.Cleanup(server.Close)

	party.url = server.URL

	return party
}

// received returns the requests the relying party has served.
func (p *relyingParty) received() []recordedRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]recordedRequest(nil), p.requests...)
}

// last returns the most recent request.
func (p *relyingParty) last(t *testing.T) recordedRequest {
	t.Helper()

	received := p.received()
	require.NotEmpty(t, received, "the relying party received no requests")

	return received[len(received)-1]
}

// writeJSON answers with a JSON body and status.
func writeJSON(t *testing.T, w http.ResponseWriter, status int, body any) {
	t.Helper()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	require.NoError(t, json.NewEncoder(w).Encode(body))
}

// mintAssertion mints an assertion for the given audience.
func mintAssertion(t *testing.T, issuer *auth.Issuer, audience string) auth.Assertion {
	t.Helper()

	assertion, err := issuer.Mint(t.Context(), testIdentity(), testStepRef(), audience)
	require.NoError(t, err)

	return assertion
}

// serialized returns the assertion as it arrives after a round trip through a
// serializer: metadata intact, token gone.
func serialized(t *testing.T, assertion auth.Assertion) auth.Assertion {
	t.Helper()

	encoded, err := json.Marshal(assertion)
	require.NoError(t, err)

	var restored auth.Assertion
	require.NoError(t, json.Unmarshal(encoded, &restored))
	require.Empty(t, restored.Token())

	return restored
}

// TestTokenExchanger covers RFC 8693 token exchange, the standards-based path.
func TestTokenExchanger(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	party := newRelyingParty(t, func(w http.ResponseWriter, r *http.Request, body recordedRequest) {
		writeJSON(t, w, http.StatusOK, map[string]any{
			"access_token":      "downstream-token",
			"issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
			"token_type":        "Bearer",
			"expires_in":        3600,
			"scope":             "read write",
		})
	})

	exchanger, err := auth.NewTokenExchanger(auth.TokenExchangeConfig{
		TokenURL:       party.url + "/token",
		Audience:       "https://as.example.com",
		TargetAudience: "https://api.partner.example.com",
		Scopes:         []string{"read", "write"},
		Clock:          clock.Now,
	})
	require.NoError(t, err)

	require.Equal(t, "https://as.example.com", exchanger.Requirement().Audience)
	require.Empty(t, exchanger.Requirement().Subject, "token exchange carries the workload's own subject")

	assertion := mintAssertion(t, issuer, exchanger.Requirement().Audience)

	credential, err := exchanger.Exchange(t.Context(), assertion)
	require.NoError(t, err)

	// What went on the wire is the protocol, and getting it wrong is how an
	// exchange fails against a real authorization server.
	sent := party.last(t)
	require.Equal(t, "urn:ietf:params:oauth:grant-type:token-exchange", sent.form.Get("grant_type"))
	require.Equal(t, assertion.Token(), sent.form.Get("subject_token"))
	require.Equal(t, "urn:ietf:params:oauth:token-type:jwt", sent.form.Get("subject_token_type"))
	require.Equal(t, "urn:ietf:params:oauth:token-type:access_token", sent.form.Get("requested_token_type"))
	require.Equal(t, "https://api.partner.example.com", sent.form.Get("audience"))
	require.Equal(t, "read write", sent.form.Get("scope"))

	require.Equal(t, auth.CredentialBearer, credential.Type)
	require.Equal(t, referenceTime.Add(time.Hour), credential.ExpiresAt)
	require.Equal(t, []string{"read", "write"}, credential.Scopes)
	require.Equal(t, assertion.ID, credential.AssertionID)

	bearer, ok := credential.Bearer()
	require.True(t, ok)
	require.Equal(t, "downstream-token", bearer)

	// Applying it is how a task uses it, without ever holding the secret itself.
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://api.partner.example.com/", nil)
	require.NoError(t, err)
	require.NoError(t, credential.Apply(request))
	require.Equal(t, "Bearer downstream-token", request.Header.Get("Authorization"))
}

// TestTokenExchangerRejects covers the answers a relying party can give that must
// not become a credential.
func TestTokenExchangerRejects(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	tests := []struct {
		name    string
		respond func(t *testing.T, w http.ResponseWriter)
		// wantDetail is text from the relying party's own explanation that must
		// reach the operator.
		wantDetail string
	}{
		{
			name: "the trust relationship is refused",
			respond: func(t *testing.T, w http.ResponseWriter) {
				writeJSON(t, w, http.StatusBadRequest, map[string]any{
					"error":             "invalid_grant",
					"error_description": "subject token issuer is not trusted",
				})
			},
			wantDetail: "subject token issuer is not trusted",
		},
		{
			name: "no token in a successful response",
			respond: func(t *testing.T, w http.ResponseWriter) {
				writeJSON(t, w, http.StatusOK, map[string]any{"token_type": "Bearer"})
			},
		},
		{
			name: "a token that is not a bearer token",
			respond: func(t *testing.T, w http.ResponseWriter) {
				writeJSON(t, w, http.StatusOK, map[string]any{
					"access_token": "not-a-bearer",
					"token_type":   "mac",
				})
			},
			wantDetail: "mac",
		},
		{
			name: "an answer that is not JSON",
			respond: func(t *testing.T, w http.ResponseWriter) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte("<html>not json</html>"))
			},
		},
		{
			name: "a server error",
			respond: func(t *testing.T, w http.ResponseWriter) {
				w.WriteHeader(http.StatusInternalServerError)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			party := newRelyingParty(t, func(w http.ResponseWriter, r *http.Request, body recordedRequest) {
				test.respond(t, w)
			})

			exchanger, err := auth.NewTokenExchanger(auth.TokenExchangeConfig{
				TokenURL: party.url + "/token",
				Audience: "https://as.example.com",
				Clock:    clock.Now,
			})
			require.NoError(t, err)

			assertion := mintAssertion(t, issuer, "https://as.example.com")

			credential, err := exchanger.Exchange(t.Context(), assertion)
			require.ErrorIs(t, err, auth.ErrExchangeFailed)
			require.True(t, credential.IsZero())

			// An error must never carry the assertion that was presented.
			require.NotContains(t, err.Error(), assertion.Token())

			if test.wantDetail != "" {
				require.Contains(t, err.Error(), test.wantDetail,
					"the relying party's own explanation must reach the operator")
			}
		})
	}

	t.Run("an assertion that has been serialized", func(t *testing.T) {
		party := newRelyingParty(t, func(w http.ResponseWriter, r *http.Request, body recordedRequest) {
			t.Error("a serialized assertion must not reach the relying party")
		})

		exchanger, err := auth.NewTokenExchanger(auth.TokenExchangeConfig{
			TokenURL: party.url + "/token",
			Audience: "https://as.example.com",
			Clock:    clock.Now,
		})
		require.NoError(t, err)

		_, err = exchanger.Exchange(t.Context(), serialized(t, mintAssertion(t, issuer, "https://as.example.com")))
		require.ErrorIs(t, err, auth.ErrCredentialUnresolved)
	})
}

// TestClientCredentialsExchanger covers the client credentials grant, both
// secretless and with a secret.
func TestClientCredentialsExchanger(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	party := newRelyingParty(t, func(w http.ResponseWriter, r *http.Request, body recordedRequest) {
		writeJSON(t, w, http.StatusOK, map[string]any{
			"access_token": "service-token",
			"token_type":   "Bearer",
			"expires_in":   1800,
		})
	})

	t.Run("authenticated by the assertion", func(t *testing.T) {
		exchanger, err := auth.NewClientCredentialsExchanger(auth.ClientCredentialsConfig{
			TokenURL: party.url + "/token",
			ClientID: "flowstate-prod",
			Scopes:   []string{"api.read"},
			Clock:    clock.Now,
		})
		require.NoError(t, err)

		requirement := exchanger.Requirement()
		require.Equal(t, party.url+"/token", requirement.Audience,
			"RFC 7523 requires the assertion to be addressed to the token endpoint")
		require.Equal(t, "flowstate-prod", requirement.Subject,
			"RFC 7523 requires the subject to be the client id")

		assertion, err := issuer.Mint(t.Context(), testIdentity(), testStepRef(), requirement.Audience)
		require.NoError(t, err)

		credential, err := exchanger.Exchange(t.Context(), assertion)
		require.NoError(t, err)

		sent := party.last(t)
		require.Equal(t, "client_credentials", sent.form.Get("grant_type"))
		require.Equal(t, "flowstate-prod", sent.form.Get("client_id"))
		require.Equal(t, "urn:ietf:params:oauth:client-assertion-type:jwt-bearer", sent.form.Get("client_assertion_type"))
		require.Equal(t, assertion.Token(), sent.form.Get("client_assertion"))
		require.Empty(t, sent.form.Get("client_secret"), "the secretless path must send no secret")

		require.Equal(t, referenceTime.Add(30*time.Minute), credential.ExpiresAt)
	})

	t.Run("authenticated by a secret", func(t *testing.T) {
		exchanger, err := auth.NewClientCredentialsExchanger(auth.ClientCredentialsConfig{
			TokenURL:     party.url + "/token",
			ClientID:     "flowstate-prod",
			ClientSecret: "hunter2",
			Clock:        clock.Now,
		})
		require.NoError(t, err)

		require.Empty(t, exchanger.Requirement().Subject,
			"with a secret the assertion is not client authentication, so the workload keeps its own subject")

		_, err = exchanger.Exchange(t.Context(), mintAssertion(t, issuer, exchanger.Requirement().Audience))
		require.NoError(t, err)

		sent := party.last(t)
		require.Equal(t, "hunter2", sent.form.Get("client_secret"))
		require.Empty(t, sent.form.Get("client_assertion"))
	})
}

// TestAWSExchanger covers STS AssumeRoleWithWebIdentity.
func TestAWSExchanger(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	expiry := referenceTime.Add(time.Hour).UTC()

	party := newRelyingParty(t, func(w http.ResponseWriter, r *http.Request, body recordedRequest) {
		w.Header().Set("Content-Type", "text/xml")
		_, _ = w.Write([]byte(`<AssumeRoleWithWebIdentityResponse>
  <AssumeRoleWithWebIdentityResult>
    <Credentials>
      <AccessKeyId>ASIAEXAMPLE</AccessKeyId>
      <SecretAccessKey>secret-key</SecretAccessKey>
      <SessionToken>session-token</SessionToken>
      <Expiration>` + expiry.Format(time.RFC3339) + `</Expiration>
    </Credentials>
    <AssumedRoleUser>
      <Arn>arn:aws:sts::123456789012:assumed-role/flowstate/session</Arn>
    </AssumedRoleUser>
  </AssumeRoleWithWebIdentityResult>
</AssumeRoleWithWebIdentityResponse>`))
	})

	exchanger, err := auth.NewAWSExchanger(auth.AWSConfig{
		RoleARN:  "arn:aws:iam::123456789012:role/flowstate",
		Endpoint: party.url + "/",
		Duration: time.Hour,
		Clock:    clock.Now,
	})
	require.NoError(t, err)

	require.Equal(t, "sts.amazonaws.com", exchanger.Requirement().Audience)

	assertion := mintAssertion(t, issuer, exchanger.Requirement().Audience)

	credential, err := exchanger.Exchange(t.Context(), assertion)
	require.NoError(t, err)

	sent := party.last(t)
	require.Equal(t, "AssumeRoleWithWebIdentity", sent.form.Get("Action"))
	require.Equal(t, "arn:aws:iam::123456789012:role/flowstate", sent.form.Get("RoleArn"))
	require.Equal(t, assertion.Token(), sent.form.Get("WebIdentityToken"))
	require.Equal(t, "3600", sent.form.Get("DurationSeconds"))

	// The session name is what appears in CloudTrail, so it has to be derived from
	// the workload and legal for AWS at the same time.
	sessionName := sent.form.Get("RoleSessionName")
	require.NotEmpty(t, sessionName)
	require.LessOrEqual(t, len(sessionName), 64)
	require.NotContains(t, sessionName, ":")
	require.NotContains(t, sessionName, "/")
	require.Contains(t, sessionName, "push-image", "the session name should identify the step")

	require.Equal(t, auth.CredentialAWSSession, credential.Type)
	require.Equal(t, expiry, credential.ExpiresAt.UTC())

	for name, want := range map[string]string{
		auth.CredentialAccessKeyID:     "ASIAEXAMPLE",
		auth.CredentialSecretAccessKey: "secret-key",
		auth.CredentialSessionToken:    "session-token",
	} {
		got, ok := credential.Value(name)
		require.True(t, ok, "AWS session credentials must carry %q", name)
		require.Equal(t, want, got)
	}

	// AWS authenticates a signature, not a header, so attaching this to a request
	// has to fail rather than half work.
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://s3.amazonaws.com/", nil)
	require.NoError(t, err)
	require.Error(t, credential.Apply(request))
	require.Empty(t, request.Header.Get("Authorization"))
}

// TestAWSExchangerRejects covers AWS refusing the assertion, and configurations
// that must not be built.
func TestAWSExchangerRejects(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	t.Run("AWS refuses the role", func(t *testing.T) {
		party := newRelyingParty(t, func(w http.ResponseWriter, r *http.Request, body recordedRequest) {
			w.Header().Set("Content-Type", "text/xml")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`<ErrorResponse><Error><Type>Sender</Type>` +
				`<Code>AccessDenied</Code><Message>Not authorized to perform sts:AssumeRoleWithWebIdentity</Message>` +
				`</Error></ErrorResponse>`))
		})

		exchanger, err := auth.NewAWSExchanger(auth.AWSConfig{
			RoleARN:  "arn:aws:iam::123456789012:role/flowstate",
			Endpoint: party.url + "/",
			Clock:    clock.Now,
		})
		require.NoError(t, err)

		_, err = exchanger.Exchange(t.Context(), mintAssertion(t, issuer, "sts.amazonaws.com"))
		require.ErrorIs(t, err, auth.ErrExchangeFailed)
		require.Contains(t, err.Error(), "AccessDenied")
		require.Contains(t, err.Error(), "Not authorized")
	})

	t.Run("AWS returns incomplete credentials", func(t *testing.T) {
		party := newRelyingParty(t, func(w http.ResponseWriter, r *http.Request, body recordedRequest) {
			_, _ = w.Write([]byte(`<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult>` +
				`<Credentials><AccessKeyId>ASIAEXAMPLE</AccessKeyId></Credentials>` +
				`</AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>`))
		})

		exchanger, err := auth.NewAWSExchanger(auth.AWSConfig{
			RoleARN:  "arn:aws:iam::123456789012:role/flowstate",
			Endpoint: party.url + "/",
			Clock:    clock.Now,
		})
		require.NoError(t, err)

		_, err = exchanger.Exchange(t.Context(), mintAssertion(t, issuer, "sts.amazonaws.com"))
		require.ErrorIs(t, err, auth.ErrExchangeFailed)
	})

	tests := []struct {
		name   string
		config auth.AWSConfig
	}{
		{name: "no role", config: auth.AWSConfig{}},
		{name: "a role that is not an ARN", config: auth.AWSConfig{RoleARN: "flowstate"}},
		{
			name:   "a session shorter than AWS allows",
			config: auth.AWSConfig{RoleARN: "arn:aws:iam::1:role/r", Duration: time.Minute},
		},
		{
			name:   "a session longer than AWS allows",
			config: auth.AWSConfig{RoleARN: "arn:aws:iam::1:role/r", Duration: 24 * time.Hour},
		},
		{
			name:   "a region that is a URL",
			config: auth.AWSConfig{RoleARN: "arn:aws:iam::1:role/r", Region: "evil.example.com/x"},
		},
		{
			name:   "an endpoint on an unprotected host",
			config: auth.AWSConfig{RoleARN: "arn:aws:iam::1:role/r", Endpoint: "http://sts.example.com/"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exchanger, err := auth.NewAWSExchanger(test.config)
			require.ErrorIs(t, err, auth.ErrInvalidPolicy)
			require.Nil(t, exchanger)
		})
	}

	t.Run("a region selects the regional endpoint", func(t *testing.T) {
		exchanger, err := auth.NewAWSExchanger(auth.AWSConfig{
			RoleARN: "arn:aws:iam::123456789012:role/flowstate",
			Region:  "us-east-1",
		})
		require.NoError(t, err)
		require.NotNil(t, exchanger)
	})
}

// TestGCPExchanger covers Google Cloud Workload Identity Federation, including
// service account impersonation.
func TestGCPExchanger(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	const pool = "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/flowstate"

	t.Run("federated token", func(t *testing.T) {
		party := newRelyingParty(t, func(w http.ResponseWriter, r *http.Request, body recordedRequest) {
			writeJSON(t, w, http.StatusOK, map[string]any{
				"access_token":      "federated-token",
				"issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
				"token_type":        "Bearer",
				"expires_in":        3600,
			})
		})

		exchanger, err := auth.NewGCPExchanger(auth.GCPConfig{
			Audience: pool,
			Endpoint: party.url + "/v1/token",
			Clock:    clock.Now,
		})
		require.NoError(t, err)

		require.Equal(t, pool, exchanger.Requirement().Audience)

		assertion := mintAssertion(t, issuer, pool)

		credential, err := exchanger.Exchange(t.Context(), assertion)
		require.NoError(t, err)

		sent := party.last(t)
		require.Equal(t, pool, sent.json["audience"])
		require.Equal(t, "urn:ietf:params:oauth:grant-type:token-exchange", sent.json["grantType"])
		require.Equal(t, "urn:ietf:params:oauth:token-type:jwt", sent.json["subjectTokenType"])
		require.Equal(t, assertion.Token(), sent.json["subjectToken"])
		require.Equal(t, "https://www.googleapis.com/auth/cloud-platform", sent.json["scope"])

		bearer, ok := credential.Bearer()
		require.True(t, ok)
		require.Equal(t, "federated-token", bearer)
	})

	t.Run("impersonating a service account", func(t *testing.T) {
		party := newRelyingParty(t, func(w http.ResponseWriter, r *http.Request, body recordedRequest) {
			switch {
			case r.URL.Path == "/v1/token":
				writeJSON(t, w, http.StatusOK, map[string]any{
					"access_token": "federated-token",
					"token_type":   "Bearer",
					"expires_in":   3600,
				})
			default:
				// The IAM Credentials API authenticates with the federated token,
				// which is the whole point of the second leg.
				require.Equal(t, "Bearer federated-token", body.authHeader)
				writeJSON(t, w, http.StatusOK, map[string]any{
					"accessToken": "service-account-token",
					"expireTime":  referenceTime.Add(30 * time.Minute).UTC().Format(time.RFC3339),
				})
			}
		})

		exchanger, err := auth.NewGCPExchanger(auth.GCPConfig{
			Audience:            pool,
			Endpoint:            party.url + "/v1/token",
			IAMEndpoint:         party.url + "/iam/v1",
			ServiceAccountEmail: "flowstate@project.iam.gserviceaccount.com",
			Lifetime:            30 * time.Minute,
			Clock:               clock.Now,
		})
		require.NoError(t, err)

		credential, err := exchanger.Exchange(t.Context(), mintAssertion(t, issuer, pool))
		require.NoError(t, err)

		bearer, ok := credential.Bearer()
		require.True(t, ok)
		require.Equal(t, "service-account-token", bearer, "the service account token is the one to use")
		require.Equal(t, referenceTime.Add(30*time.Minute).UTC(), credential.ExpiresAt.UTC())

		received := party.received()
		require.Len(t, received, 2, "impersonation is two calls: federate, then impersonate")
		require.Contains(t, received[1].path, "generateAccessToken")
		require.Equal(t, "1800s", received[1].json["lifetime"], "Google expects a lifetime in seconds")
	})

	tests := []struct {
		name   string
		config auth.GCPConfig
	}{
		{name: "no audience", config: auth.GCPConfig{}},
		{
			name:   "an endpoint on an unprotected host",
			config: auth.GCPConfig{Audience: pool, Endpoint: "http://sts.example.com/v1/token"},
		},
		{
			name: "a service account that is not an email address",
			config: auth.GCPConfig{
				Audience:            pool,
				ServiceAccountEmail: "flowstate",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exchanger, err := auth.NewGCPExchanger(test.config)
			require.ErrorIs(t, err, auth.ErrInvalidPolicy)
			require.Nil(t, exchanger)
		})
	}
}

// TestExchangerRejectsUnprotectedEndpoints checks that no exchanger can be
// configured to send an assertion over a connection anyone can read.
func TestExchangerRejectsUnprotectedEndpoints(t *testing.T) {
	const insecure = "http://as.example.com/token"

	_, err := auth.NewTokenExchanger(auth.TokenExchangeConfig{TokenURL: insecure, Audience: "a"})
	require.ErrorIs(t, err, auth.ErrInvalidPolicy)

	_, err = auth.NewClientCredentialsExchanger(auth.ClientCredentialsConfig{TokenURL: insecure, ClientID: "c"})
	require.ErrorIs(t, err, auth.ErrInvalidPolicy)

	// A missing endpoint is refused too, rather than defaulting to something.
	_, err = auth.NewTokenExchanger(auth.TokenExchangeConfig{Audience: "a"})
	require.ErrorIs(t, err, auth.ErrInvalidPolicy)

	// And an exchanger with no audience cannot be built, because the broker would
	// have to mint an assertion that any relying party would accept.
	_, err = auth.NewTokenExchanger(auth.TokenExchangeConfig{TokenURL: "https://as.example.com/token"})
	require.ErrorIs(t, err, auth.ErrInvalidPolicy)
}

// TestCredentialNeverRevealsItself checks the property that makes the
// workflow-history rule enforceable: a credential's secret cannot be printed,
// logged, or serialized, and one that has been serialized fails closed.
func TestCredentialNeverRevealsItself(t *testing.T) {
	clock := authtest.NewClock(referenceTime)
	issuer, _ := newIssuer(t, clock)

	party := newRelyingParty(t, func(w http.ResponseWriter, r *http.Request, body recordedRequest) {
		writeJSON(t, w, http.StatusOK, map[string]any{
			"access_token": "super-secret-token",
			"token_type":   "Bearer",
			"expires_in":   3600,
		})
	})

	exchanger, err := auth.NewTokenExchanger(auth.TokenExchangeConfig{
		TokenURL: party.url + "/token",
		Audience: "https://as.example.com",
		Clock:    clock.Now,
	})
	require.NoError(t, err)

	credential, err := exchanger.Exchange(t.Context(), mintAssertion(t, issuer, "https://as.example.com"))
	require.NoError(t, err)

	for _, rendered := range []string{
		credential.String(),
		fmt.Sprint(credential),
		fmt.Sprintf("%v", credential),
		fmt.Sprintf("%+v", credential),
	} {
		require.NotContains(t, rendered, "super-secret-token")
	}

	encoded, err := json.Marshal(credential)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "super-secret-token",
		"serializing a credential must not carry the secret into durable history")

	// The metadata survives, which is what makes an audit record possible.
	require.Contains(t, string(encoded), "bearer")

	var restored auth.Credential
	require.NoError(t, json.Unmarshal(encoded, &restored))

	_, ok := restored.Bearer()
	require.False(t, ok)

	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://api.example.com/", nil)
	require.NoError(t, err)

	err = restored.Apply(request)
	require.ErrorIs(t, err, auth.ErrCredentialUnresolved,
		"a credential that lost its secret must fail closed, not send an empty header")
	require.Empty(t, request.Header.Get("Authorization"))
}
