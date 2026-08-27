package conformance

import (
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// Shared cases for #963 half one: the http task refuses a request that
// carries a bearer secret or a JIT federation credential to a non-loopback
// http:// destination, mirroring secrets/vault/vault.go's guard against
// sending its own client token in cleartext (vault.go:751-766). Run against
// both execution drivers — eval_test.go's runAuthorityCase locally,
// engine/authority_test.go's identically-named runAuthorityCase durably —
// through the same [AuthorityCase] shape [AuthorityDenialCases] and
// [AuthorityContainmentCases] already use, which is what lets these cases
// reuse both drivers' existing installation code rather than adding a third.
//
// # Why every negative case installs a working authority rather than none
//
// A case with [Authority.NoRuntime] set would also see a failure, but it
// would prove nothing about *ordering*: with no secret backend and no broker
// configured at all, resolution would fail on its own even if this refusal
// never ran. Every negative case here instead installs an authority that
// *would* succeed — a real fixture provider, an allow-everything policy, a
// real fixture broker — and counts whether it was ever consulted
// ([Authority.ProviderCalls], [Federation.ExchangeCalls]). A refusal that
// regressed to run after resolution would still produce a failure (the
// fixture values are literal strings, not valid destinations) but the call
// count would move off zero, which is what the assertion in each driver's
// test file checks alongside the error text.
//
// # Half two, and the plugin path
//
// This covers half one only: the http task's own refusal. Half two — a
// `credentials` input to netpolicy's request rules, letting a deployment
// scope which destinations may ever carry one — is a separate, gated change
// and is not implemented here. The plugin credential path
// (plugin/task.go) resolves its own secret inputs and egresses on its own
// policy; this refusal has no visibility into it and does not cover it.
const cleartextCredentialTarget = "http://cleartext-credential-refusal.invalid/never-dialed"

// cleartextCredentialErrorText is the refusal text both drivers must produce
// identically, mirroring secrets/vault/vault.go:751-766's wording for the
// same situation.
const cleartextCredentialErrorText = cleartextCredentialTarget +
	` would send a credential in cleartext; use https, or http only for a loopback address such as a sidecar terminating TLS`

// TLSCredentialServer is a loopback TLS server for the positive direction:
// proof that this refusal reaches only http://, not every request a
// credential travels with.
type TLSCredentialServer struct {
	URL  string
	cert *x509.Certificate
}

// NewTLSCredentialServer starts the server. Call
// [TLSCredentialServer.InstallTrustingHTTPTask] before running a case that
// points at it — a policy with no trusted root refuses an unknown
// certificate, which is correct and not what this set is testing.
func NewTLSCredentialServer(tb testing.TB) *TLSCredentialServer {
	tb.Helper()

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("reached"))
	}))
	tb.Cleanup(srv.Close)

	return &TLSCredentialServer{URL: srv.URL, cert: srv.Certificate()}
}

// InstallTrustingHTTPTask registers an http task that trusts s's certificate
// and permits loopback, for the duration of the test, restoring whatever was
// registered before.
//
// Nested the way [InstallEgressIdentityPolicy] nests over [allowLoopback]:
// this installs a *different* policy (one with a trusted root added), so it
// cannot share that exemption's count — see that function's own doc for why
// nesting rather than counting is correct here, and the same restriction
// applies: do not call this from a parallel test.
func (s *TLSCredentialServer) InstallTrustingHTTPTask(tb testing.TB) {
	tb.Helper()

	pool := x509.NewCertPool()
	pool.AddCert(s.cert)

	policy, err := netpolicy.New(netpolicy.WithAllowLoopback(), netpolicy.WithRootCAs(pool))
	if err != nil {
		tb.Fatalf("building the TLS-trusting egress policy: %v", err)
	}

	registry := v1.DefaultRegistry()
	original, existed := registry.Lookup("http")
	if err := registry.Register(v1.HTTPTaskDef(policy)); err != nil {
		tb.Fatalf("registering the TLS-trusting http task: %v", err)
	}
	tb.Cleanup(func() {
		if existed {
			_ = registry.Register(original)
		}
	})
}

// CleartextCredentialCases returns the shared cases both drivers must agree
// on.
//
// tlsServerURL must come from a [TLSCredentialServer] that already had
// [TLSCredentialServer.InstallTrustingHTTPTask] called.
func CleartextCredentialCases(tlsServerURL string) []AuthorityCase {
	identity := auth.WorkloadIdentity{
		Subject: "svc-reader", Issuer: "https://issuer.example", Namespace: "acme-tenant",
	}

	return []AuthorityCase{
		{
			// The negative direction for `bearer:`. The workflow never reaches
			// the network — refused before the request is dialed, same as
			// [AuthorityDenialCases]' unreachable URLs — so the URL named is
			// deliberately unresolvable.
			Case: Case{
				Name: "a bearer secret to a cleartext, non-loopback destination is refused before it is resolved",
				Workflow: &v1.Workflow{
					Name:  "cleartext-credential-bearer",
					Steps: []*v1.Node{bearerSecretStep("call", cleartextCredentialTarget, "fixture-secret", "API_TOKEN")},
				},
				ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
					"call": v1.FailedStepOutputs(`task "http" failed (PolicyDenied): ` + cleartextCredentialErrorText),
				}},
			},
			Authority: Authority{
				// An authority that would succeed if ever consulted — see the
				// package doc above for why that is the point.
				Scheme: "fixture-secret", FixtureValue: "must-not-resolve",
				Allow: []string{"true"}, Identity: identity,
				ProviderCalls: new(atomic.Int32),
			},
		},
		{
			// Its pair for `credential:` — the JIT federation path, which
			// reaches [v1.AuthorizeCredential] rather than [v1.ResolveSecret],
			// and so needs its own ordering proof
			// ([Federation.ExchangeCalls]) rather than reusing ProviderCalls.
			Case: Case{
				Name: "a JIT federation credential to a cleartext, non-loopback destination is refused before authorization",
				Workflow: &v1.Workflow{
					Name:  "cleartext-credential-jit",
					Steps: []*v1.Node{credentialStep("call", cleartextCredentialTarget, "partner-api")},
				},
				ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
					"call": v1.FailedStepOutputs(`task "http" failed (PolicyDenied): ` + cleartextCredentialErrorText),
				}},
			},
			Authority: Authority{
				Identity: identity,
				Federation: &Federation{
					Target: "partner-api", Token: "must-not-exchange",
					ExchangeCalls: new(atomic.Int32),
				},
			},
		},
		{
			// The positive direction, without which the two cases above could
			// be a task that refuses every request rather than one that
			// refuses cleartext specifically: the identical bearer secret,
			// the identical fixture authority, over https instead — and it
			// must reach the peer and come back.
			Case: Case{
				Name: "the same bearer secret succeeds over https",
				Workflow: &v1.Workflow{
					Name: "cleartext-credential-bearer-https",
					Steps: []*v1.Node{{
						Id: "call",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name: "http",
							Inputs: map[string]*v1.Value{
								"url":     v1.NewLiteral(tlsServerURL),
								"bearer":  {Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "fixture-secret", Name: "API_TOKEN"}}},
								"outputs": v1.NewExpr(`{"said": response.body}`),
							},
						}},
					}},
				},
				ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
					"call": said("reached"),
				}},
			},
			Authority: Authority{
				Scheme: "fixture-secret", FixtureValue: "https-secret-value",
				Allow: []string{"true"}, Identity: identity,
			},
		},
	}
}
