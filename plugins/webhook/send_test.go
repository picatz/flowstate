package main

import (
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"

	webhookv1 "github.com/picatz/flowstate/plugins/webhook/gen/webhook/v1"
)

const testKey = "whsec_not_a_real_key_0123456789"

// TestMain gives this test binary the grant a worker gives a launched plugin,
// and installs it exactly as main does. The grant is captured once per process.
func TestMain(m *testing.M) {
	if err := os.Setenv(sdk.EgressPolicyEnv,
		base64.StdEncoding.EncodeToString([]byte("deployment_default: true\negress: {}\n"))); err != nil {
		panic(err)
	}

	installEgressPolicy()

	os.Exit(m.Run())
}

func key(value string) secrets.Secret {
	return secrets.NewSecret(secrets.NewRef("webhook", "signing_key"), value)
}

// loopbackClient is a governed client that may reach the test server, which the
// default policy would refuse.
func loopbackClient(t *testing.T, opts ...netpolicy.Option) *http.Client {
	t.Helper()

	policy, err := netpolicy.New(append([]netpolicy.Option{
		netpolicy.WithAllowLoopback(),
		netpolicy.WithMaxResponseBytes(1 << 20),
		netpolicy.WithTimeout(2 * time.Second),
	}, opts...)...)
	if err != nil {
		t.Fatalf("building test egress policy: %v", err)
	}
	return policy.Client()
}

type received struct {
	header http.Header
	body   []byte
}

// receiver records what arrives and answers with respond.
func receiver(t *testing.T, got *atomic.Pointer[received], respond func(http.ResponseWriter, *http.Request)) *httptest.Server {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got.Store(&received{header: r.Header.Clone(), body: body})
		if respond != nil {
			respond(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

// verifyAs runs a delivery through the engine's inbound verifier under scheme.
func verifyAs(scheme string, k secrets.Secret, r *received, body []byte, now time.Time) error {
	headers := map[string]string{}
	for name := range r.header {
		headers[strings.ToLower(name)] = r.header.Get(name)
	}
	trigger := &flowstatev1.WebhookTrigger{
		Name:           "receiver",
		IdempotencyKey: flowstatev1.NewExpr(`event.body.id`),
		Verify: map[string]*flowstatev1.Value{scheme: {Kind: &flowstatev1.Value_SecretRef{
			SecretRef: &flowstatev1.SecretRef{Scheme: "env", Name: "WEBHOOK_SECRET"},
		}}},
	}
	return flowstatev1.VerifyWebhookDelivery(trigger, map[string]secrets.Secret{scheme: k}, headers, body, now)
}

// TestADeliveryRoundTripsThroughTheEnginesVerifier is the proof the two halves
// agree, for every scheme the engine can verify: what this task sends, the
// inbound verifier accepts, and a tampered body, timestamp or key is refused.
func TestADeliveryRoundTripsThroughTheEnginesVerifier(t *testing.T) {
	const body = `{"id":"evt_1","amount":42}`

	for _, scheme := range flowstatev1.WebhookVerificationSchemes() {
		t.Run(scheme, func(t *testing.T) {
			var got atomic.Pointer[received]
			server := receiver(t, &got, nil)
			now := time.Now()

			out, err := deliver(t.Context(), loopbackClient(t), &webhookv1.SendInputs{
				Url: server.URL, Body: body, Scheme: scheme, IdempotencyKey: "delivery-0001",
				Headers: map[string]string{"X-Event-Type": "order.paid"},
			}, key(testKey), now)
			if err != nil {
				t.Fatalf("deliver: %v", err)
			}
			if out.GetStatus() != 200 {
				t.Fatalf("status = %d", out.GetStatus())
			}

			r := got.Load()
			if string(r.body) != body {
				t.Fatalf("the wire body %q is not exactly what was signed, %q", r.body, body)
			}
			if r.header.Get("Idempotency-Key") != "delivery-0001" || r.header.Get("X-Event-Type") != "order.paid" ||
				r.header.Get("Content-Type") != "application/json" {
				t.Fatalf("headers = %v", r.header)
			}

			if err := verifyAs(scheme, key(testKey), r, []byte(body), now); err != nil {
				t.Fatalf("the engine's verifier refused what the task sent: %v", err)
			}
			if err := verifyAs(scheme, key(testKey), r, []byte(body+" "), now); err == nil {
				t.Error("a tampered body verified")
			}
			if err := verifyAs(scheme, key(testKey+"x"), r, []byte(body), now); err == nil {
				t.Error("a different key verified")
			}
		})
	}
}

func TestAStripeDeliverySignsItsTimestamp(t *testing.T) {
	var got atomic.Pointer[received]
	server := receiver(t, &got, nil)
	now := time.Now()

	if _, err := deliver(t.Context(), loopbackClient(t), &webhookv1.SendInputs{
		Url: server.URL, Body: `{"id":"evt_1"}`, Scheme: flowstatev1.WebhookSchemeStripe,
	}, key(testKey), now); err != nil {
		t.Fatalf("deliver: %v", err)
	}

	r := got.Load()
	body := []byte(`{"id":"evt_1"}`)
	if err := verifyAs(flowstatev1.WebhookSchemeStripe, key(testKey), r, body, now.Add(flowstatev1.WebhookReplayWindow+time.Minute)); err == nil {
		t.Error("a stale timestamp verified")
	}

	// Retiming the signature without re-signing is a forgery.
	forged := &received{header: r.header.Clone(), body: r.body}
	forged.header.Set(flowstatev1.StripeSignatureHeader,
		strings.Replace(r.header.Get(flowstatev1.StripeSignatureHeader), "t=", "t=1", 1))
	if err := verifyAs(flowstatev1.WebhookSchemeStripe, key(testKey), forged, body, now); err == nil {
		t.Error("a retimed signature verified")
	}
}

func TestTheDefaultSchemeIsTheGenericOne(t *testing.T) {
	var got atomic.Pointer[received]
	server := receiver(t, &got, nil)
	now := time.Now()

	if _, err := deliver(t.Context(), loopbackClient(t), &webhookv1.SendInputs{Url: server.URL, Body: `{}`}, key(testKey), now); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if got.Load().header.Get(flowstatev1.WebhookSignatureHeader) == "" {
		t.Fatal("no generic signature header was sent")
	}
	if got.Load().header.Get("Idempotency-Key") != "" {
		t.Error("an idempotency key appeared that nobody asked for")
	}
}

// TestADeniedDestinationIsNotDialed is the fail-closed direction: the default
// policy denies loopback, so the receiver sees nothing and the refusal is
// permanent and names no destination.
func TestADeniedDestinationIsNotDialed(t *testing.T) {
	var got atomic.Pointer[received]
	server := receiver(t, &got, nil)

	policy, err := netpolicy.New()
	if err != nil {
		t.Fatal(err)
	}
	_, err = deliver(t.Context(), policy.Client(), &webhookv1.SendInputs{Url: server.URL, Body: `{}`}, key(testKey), time.Now())
	if err == nil || !sdk.IsPermissionDenied(err) {
		t.Fatalf("error = %v, want a permission denial", err)
	}
	if got.Load() != nil {
		t.Fatal("the denied receiver was dialed")
	}
	if strings.Contains(err.Error(), server.URL) || strings.Contains(err.Error(), testKey) {
		t.Fatalf("the denial echoed a value: %v", err)
	}
}

func TestWithoutAUsableGrantTheTaskRefusesBeforeDecoding(t *testing.T) {
	old, oldRefusal := egressPolicy, egressRefusal
	egressPolicy = nil
	t.Cleanup(func() { egressPolicy, egressRefusal = old, oldRefusal })

	_, err := webhookSend(t.Context(), nil, nil)
	if err == nil || !sdk.IsPermissionDenied(err) {
		t.Fatalf("error = %v, want a permission denial", err)
	}
}

func TestAnEgressRuleNamingCredentialsSeesTheDelivery(t *testing.T) {
	var got atomic.Pointer[received]
	server := receiver(t, &got, nil)

	// A rule that keeps credentials away from every host: the signature is a
	// credential no header name shows, so only the task's own mark can trip it.
	client := loopbackClient(t, netpolicy.WithDenyRules("credentials"))
	_, err := deliver(t.Context(), client, &webhookv1.SendInputs{Url: server.URL, Body: `{}`}, key(testKey), time.Now())
	if err == nil || !sdk.IsPermissionDenied(err) {
		t.Fatalf("error = %v, want the credentials rule to deny the delivery", err)
	}
	if got.Load() != nil {
		t.Fatal("a delivery the operator's credentials rule denies was sent")
	}
}

// TestTheKeyAndSignatureNeverComeBack covers every path a receiver controls: an
// echoed response, and the error text of each failing status.
func TestTheKeyAndSignatureNeverComeBack(t *testing.T) {
	for _, scheme := range flowstatev1.WebhookVerificationSchemes() {
		t.Run(scheme, func(t *testing.T) {
			var got atomic.Pointer[received]
			server := receiver(t, &got, func(w http.ResponseWriter, r *http.Request) {
				// A receiver that echoes every header, and the key itself, back.
				var b strings.Builder
				for name := range r.Header {
					b.WriteString(name + "=" + r.Header.Get(name) + "\n")
				}
				b.WriteString("key=" + testKey + "\n")
				_, _ = w.Write([]byte(b.String()))
			})

			out, err := deliver(t.Context(), loopbackClient(t), &webhookv1.SendInputs{
				Url: server.URL, Body: `{"id":"1"}`, Scheme: scheme,
			}, key(testKey), time.Now())
			if err != nil {
				t.Fatalf("deliver: %v", err)
			}

			sent := got.Load().header.Get(map[string]string{
				flowstatev1.WebhookSchemeHMACSHA256: flowstatev1.WebhookSignatureHeader,
				flowstatev1.WebhookSchemeStripe:     flowstatev1.StripeSignatureHeader,
			}[scheme])
			digest := sent[strings.LastIndex(sent, "=")+1:]
			for _, secret := range []string{testKey, sent, digest} {
				if strings.Contains(out.GetResponse(), secret) {
					t.Fatalf("the response output carried %q", secret)
				}
			}
			if !strings.Contains(out.GetResponse(), redacted) {
				t.Fatal("nothing was redacted, so the test did not exercise the scrubber")
			}
		})
	}
}

func TestStatusesAreClassifiedAndTheirErrorsEchoNothing(t *testing.T) {
	for _, test := range []struct {
		status int
		check  func(error) bool
		name   string
	}{
		{429, sdk.IsUnavailable, "rate limited is retryable"},
		{500, sdk.IsOutcomeUnknown, "a server error may have applied"},
		{503, sdk.IsOutcomeUnknown, "unavailable after the write is unknown"},
		{400, sdk.IsFailed, "a refusal is permanent"},
		{302, sdk.IsFailed, "a redirect is refused, not followed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var got atomic.Pointer[received]
			var other atomic.Pointer[received]
			elsewhere := receiver(t, &other, nil)
			server := receiver(t, &got, func(w http.ResponseWriter, r *http.Request) {
				if test.status == 302 {
					w.Header().Set("Location", elsewhere.URL)
				}
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte("echo " + testKey + " " + r.Header.Get(flowstatev1.WebhookSignatureHeader)))
			})

			_, err := deliver(t.Context(), loopbackClient(t), &webhookv1.SendInputs{Url: server.URL, Body: `{}`}, key(testKey), time.Now())
			if err == nil || !test.check(err) {
				t.Fatalf("error = %v", err)
			}
			if strings.Contains(err.Error(), testKey) || strings.Contains(err.Error(), "echo") ||
				strings.Contains(err.Error(), got.Load().header.Get(flowstatev1.WebhookSignatureHeader)) {
				t.Fatalf("the error echoed the receiver or a secret: %v", err)
			}
			if other.Load() != nil {
				t.Fatal("a signed delivery was followed to another host")
			}
		})
	}
}

func TestAResponseIsBoundedAndASecretOnTheCutIsStillRedacted(t *testing.T) {
	var got atomic.Pointer[received]
	// The key begins 10 bytes before the cap: a cut-then-scrub would leave its
	// first 10 bytes in the output.
	server := receiver(t, &got, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("a", maxResponseBytes-10) + testKey + strings.Repeat("b", 4*maxResponseBytes)))
	})

	out, err := deliver(t.Context(), loopbackClient(t), &webhookv1.SendInputs{Url: server.URL, Body: `{}`}, key(testKey), time.Now())
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if len(out.GetResponse()) > maxResponseBytes {
		t.Fatalf("response is %d bytes, over the %d cap", len(out.GetResponse()), maxResponseBytes)
	}
	if !out.GetResponseTruncated() {
		t.Error("an over-cap response was not marked truncated")
	}
	if strings.Contains(out.GetResponse(), testKey[:10]) {
		t.Fatal("the cut left a prefix of the key in the response")
	}
}

// A receiver that echoes the signature upper-cased has still returned a
// signature that verifies for this body, so the scrub does not care about case.
func TestAnUpperCasedEchoOfTheSignatureIsRedacted(t *testing.T) {
	var got atomic.Pointer[received]
	server := receiver(t, &got, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("echo: " + strings.ToUpper(r.Header.Get(flowstatev1.WebhookSignatureHeader))))
	})

	out, err := deliver(t.Context(), loopbackClient(t), &webhookv1.SendInputs{Url: server.URL, Body: `{}`}, key(testKey), time.Now())
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}

	sig := got.Load().header.Get(flowstatev1.WebhookSignatureHeader)
	if sig == "" {
		t.Fatal("no signature header was sent")
	}
	if strings.Contains(strings.ToLower(out.GetResponse()), strings.ToLower(sig)) {
		t.Fatalf("an upper-cased echo of the signature came back: %q", out.GetResponse())
	}
}

func TestAnOperatorResponseCapStillReturnsTheAcceptedDelivery(t *testing.T) {
	var got atomic.Pointer[received]
	server := receiver(t, &got, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", 4096)))
	})

	out, err := deliver(t.Context(), loopbackClient(t, netpolicy.WithMaxResponseBytes(100)),
		&webhookv1.SendInputs{Url: server.URL, Body: `{}`}, key(testKey), time.Now())
	if err != nil {
		t.Fatalf("a delivery the receiver accepted failed over its response size: %v", err)
	}
	if !out.GetResponseTruncated() || out.GetStatus() != 200 {
		t.Fatalf("outputs = %v", out)
	}
}

func TestAHangingReceiverIsAnUnknownOutcome(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	t.Cleanup(func() { close(release); server.Close() })

	policy, err := netpolicy.New(netpolicy.WithAllowLoopback(), netpolicy.WithTimeout(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	_, err = deliver(t.Context(), policy.Client(), &webhookv1.SendInputs{Url: server.URL, Body: `{}`}, key(testKey), time.Now())
	if err == nil || !sdk.IsOutcomeUnknown(err) {
		t.Fatalf("error = %v, want an unknown outcome", err)
	}
}

func TestInputsAreBounded(t *testing.T) {
	valid := func() *webhookv1.SendInputs {
		return &webhookv1.SendInputs{Url: "https://receiver.example/hook", Body: `{}`}
	}
	if err := validateSend(valid()); err != nil {
		t.Fatalf("a valid delivery was refused: %v", err)
	}

	for name, mutate := range map[string]func(*webhookv1.SendInputs){
		"relative url":       func(in *webhookv1.SendInputs) { in.Url = "/hook" },
		"other scheme":       func(in *webhookv1.SendInputs) { in.Url = "ftp://receiver.example/hook" },
		"url userinfo":       func(in *webhookv1.SendInputs) { in.Url = "https://user:pw@receiver.example/hook" },
		"url too long":       func(in *webhookv1.SendInputs) { in.Url = "https://r.example/" + strings.Repeat("a", maxURLBytes) },
		"body too large":     func(in *webhookv1.SendInputs) { in.Body = strings.Repeat("a", maxBodyBytes+1) },
		"unknown scheme":     func(in *webhookv1.SendInputs) { in.Scheme = "sha1" },
		"idempotency space":  func(in *webhookv1.SendInputs) { in.IdempotencyKey = "a b" },
		"idempotency long":   func(in *webhookv1.SendInputs) { in.IdempotencyKey = strings.Repeat("a", maxIdempotencyKeyBytes+1) },
		"authorization":      func(in *webhookv1.SendInputs) { in.Headers = map[string]string{"authorization": "Bearer x"} },
		"cookie":             func(in *webhookv1.SendInputs) { in.Headers = map[string]string{"Cookie": "s=1"} },
		"signature override": func(in *webhookv1.SendInputs) { in.Headers = map[string]string{"x-flowstate-signature": "00"} },
		"stripe override":    func(in *webhookv1.SendInputs) { in.Headers = map[string]string{"Stripe-Signature": "00"} },
		"idempotency header": func(in *webhookv1.SendInputs) { in.Headers = map[string]string{"Idempotency-Key": "x"} },
		"host header":        func(in *webhookv1.SendInputs) { in.Headers = map[string]string{"Host": "evil.example"} },
		"header injection":   func(in *webhookv1.SendInputs) { in.Headers = map[string]string{"X-A": "a\r\nX-B: b"} },
		"bad header name":    func(in *webhookv1.SendInputs) { in.Headers = map[string]string{"X A": "a"} },
		"too many headers":   func(in *webhookv1.SendInputs) { in.Headers = manyHeaders(maxHeaders + 1) },
		"header value long": func(in *webhookv1.SendInputs) {
			in.Headers = map[string]string{"X-A": strings.Repeat("a", maxHeaderValueBytes+1)}
		},
	} {
		t.Run(name, func(t *testing.T) {
			in := valid()
			mutate(in)
			err := validateSend(in)
			if err == nil || !sdk.IsInvalidInput(err) {
				t.Fatalf("error = %v, want an invalid-input refusal", err)
			}
			if strings.Contains(err.Error(), "pw@") {
				t.Fatalf("the refusal echoed a credential: %v", err)
			}
		})
	}
}

func manyHeaders(n int) map[string]string {
	headers := make(map[string]string, n)
	for i := range n {
		headers["X-H"+strings.Repeat("a", i+1)] = "v"
	}
	return headers
}

func TestTheKeyMustArriveResolved(t *testing.T) {
	if _, err := keyFromValue(nil); err == nil {
		t.Error("a missing key was accepted")
	}
	ref := &flowstatev1.Value{Kind: &flowstatev1.Value_SecretRef{SecretRef: &flowstatev1.SecretRef{Scheme: "env", Name: "K"}}}
	if _, err := keyFromValue(ref); err == nil {
		t.Error("an unresolved reference was accepted")
	}
	if _, err := keyFromValue(flowstatev1.NewValue("")); err == nil {
		t.Error("an empty key was accepted")
	}
	if _, err := keyFromValue(flowstatev1.NewValue(strings.Repeat("k", maxKeyBytes+1))); err == nil {
		t.Error("an oversized key was accepted")
	}
	if k, err := keyFromValue(flowstatev1.NewValue(testKey)); err != nil || k.Reveal() != testKey {
		t.Errorf("a resolved key was refused: %v", err)
	}
}
