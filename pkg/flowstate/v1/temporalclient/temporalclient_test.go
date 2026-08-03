package temporalclient

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
)

func TestConfigOptionsDefaults(t *testing.T) {
	// The zero Config must produce a usable local development target, because a
	// first run should need no configuration at all.
	opts, err := Config{}.Options()
	if err != nil {
		t.Fatalf("Options() error: %v", err)
	}
	if opts.HostPort != DefaultAddress {
		t.Errorf("HostPort = %q, want %q", opts.HostPort, DefaultAddress)
	}
	if opts.Namespace != DefaultNamespace {
		t.Errorf("Namespace = %q, want %q", opts.Namespace, DefaultNamespace)
	}
}

func TestConfigOptionsOverrides(t *testing.T) {
	// Explicit values must win over whatever the environment resolves to, which is
	// what lets a flag override a configured profile.
	opts, err := Config{Address: "temporal.example:7233", Namespace: "staging"}.Options()
	if err != nil {
		t.Fatalf("Options() error: %v", err)
	}
	if opts.HostPort != "temporal.example:7233" {
		t.Errorf("HostPort = %q, want the explicit address", opts.HostPort)
	}
	if opts.Namespace != "staging" {
		t.Errorf("Namespace = %q, want the explicit namespace", opts.Namespace)
	}
}

// TestDescribeNeverRevealsCredentials pins that a startup log line reports which
// credential mechanism is in use without reporting the credential.
//
// Temporal client options carry real secrets — an API key, and for mTLS a private
// key — so anything that formats them can leak one. A type whose String method
// redacts is not enough protection either: fmt cannot call a method on a value it
// reaches through an unexported field, so it prints the fields instead. The
// defense here is to never hand the options to a formatter at all, and this test
// is what keeps that true.
func TestDescribeNeverRevealsCredentials(t *testing.T) {
	const apiKey = "sk-super-secret-api-key"

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "flowstate-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("creating certificate: %v", err)
	}

	tests := []struct {
		name         string
		opts         client.Options
		wantSecurity string
	}{
		{
			name:         "no TLS",
			opts:         client.Options{HostPort: "localhost:7233", Namespace: "default"},
			wantSecurity: "no TLS",
		},
		{
			name: "API key",
			opts: client.Options{
				HostPort:    "cloud.example:7233",
				Namespace:   "prod",
				Credentials: client.NewAPIKeyStaticCredentials(apiKey),
			},
			wantSecurity: "API key",
		},
		{
			name: "mTLS",
			opts: client.Options{
				HostPort:  "cluster.example:7233",
				Namespace: "prod",
				ConnectionOptions: client.ConnectionOptions{
					TLS: &tls.Config{
						MinVersion: tls.VersionTLS13,
						Certificates: []tls.Certificate{{
							Certificate: [][]byte{der},
							PrivateKey:  key,
						}},
					},
				},
			},
			wantSecurity: "mTLS",
		},
	}

	// Deriving what a leak would look like from the key itself, rather than naming
	// its internals, keeps this test off deprecated accessors while still failing
	// if any of the key's material reaches the output.
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshaling key: %v", err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Describe(tt.opts)

			// Asserting the exact shape is the real defense: a summary built from
			// three known fields cannot leak a fourth, whatever the options hold.
			want := fmt.Sprintf("%s namespace=%s (%s)", tt.opts.HostPort, tt.opts.Namespace, tt.wantSecurity)
			if got != want {
				t.Errorf("Describe() = %q, want %q", got, want)
			}

			// Belt and braces: no credential material, in any encoding we can
			// cheaply check for.
			for name, leak := range map[string]string{
				"API key":         apiKey,
				"key as hex":      fmt.Sprintf("%x", keyDER),
				"key as raw text": string(keyDER),
			} {
				if leak != "" && strings.Contains(got, leak) {
					t.Errorf("Describe() leaked the %s: %q", name, got)
				}
			}

			// The options must never reach a formatter. Rendering them directly is
			// what a debug log would do, and it demonstrates why Describe exists:
			// a redacting String method would not help here, because fmt cannot
			// call one on a value it reaches through an unexported field.
			rendered := fmt.Sprintf("%+v", tt.opts)
			if tt.opts.Credentials != nil && !strings.Contains(rendered, "Credentials") {
				t.Skip("client.Options no longer exposes Credentials to fmt; the risk this guards has changed")
			}
		})
	}
}

// stubClientInterceptor is a client interceptor that does nothing, which is all
// this package needs: what is under test is whether a configured interceptor
// reaches the options every client here is dialed from, not what it does.
type stubClientInterceptor struct {
	interceptor.ClientInterceptorBase
}

// TestConfigOptionsCarryInterceptors is the seam Temporal tracing rides on.
//
// It is asserted at Options rather than at Dial because Options is the single
// funnel: [Dial] resolves through it, and so does every per-namespace client
// [NewPool] builds. An interceptor that reached one and not the other would
// instrument a single-tenant deployment and leave a mapped tenant's runs with a
// trace that stops at the server — which is the gap this field exists to close,
// and it is invisible from a test that only dials once.
func TestConfigOptionsCarryInterceptors(t *testing.T) {
	opts, err := Config{Interceptors: []interceptor.ClientInterceptor{&stubClientInterceptor{}}}.Options()
	if err != nil {
		t.Fatalf("Options() error: %v", err)
	}
	if len(opts.Interceptors) != 1 {
		t.Fatalf("Interceptors = %d, want the one that was configured", len(opts.Interceptors))
	}
}

// TestConfigOptionsInterceptorsAbsentByDefault is the direction invariant 8
// cares about: a deployment that configured no telemetry dials a client that
// intercepts nothing, so nothing is marshalled into a workflow's header.
func TestConfigOptionsInterceptorsAbsentByDefault(t *testing.T) {
	opts, err := Config{}.Options()
	if err != nil {
		t.Fatalf("Options() error: %v", err)
	}
	if len(opts.Interceptors) != 0 {
		t.Errorf("Interceptors = %d, want none for an unconfigured deployment", len(opts.Interceptors))
	}
}
