package codecserver_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/encoding/protojson"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/codecserver"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/envelope"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider/local"
)

const (
	markerA = "synthetic-tenant-a-3f9e"
	markerB = "synthetic-tenant-b-81c2"
)

// fixture is two tenants, each mapped to a Temporal namespace of its own, with
// a keyring holding both namespaces' keys, and the server in front of it.
type fixture struct {
	codecs  payloadcodec.Config
	server  *httptest.Server
	handler http.Handler
	trail   *bytes.Buffer
}

// principals stand in for the trust policy's verifier: a bearer token names a
// principal. The middleware attaches it exactly as auth.Authenticator does.
var principals = map[string]auth.Principal{
	"a-decoder": {Issuer: "https://issuer.example", Subject: "alice", Namespace: "team-a", Actions: []string{"payload.decode", "payload.encode"}},
	"a-reader":  {Issuer: "https://issuer.example", Subject: "ann", Namespace: "team-a", Actions: []string{"workload.read"}},
	"a-legacy":  {Issuer: "https://issuer.example", Subject: "abe", Namespace: "team-a"},
	"b-decoder": {Issuer: "https://issuer.example", Subject: "bob", Namespace: "team-b", Actions: []string{"payload.decode"}},
}

func newFixture(t *testing.T, mutate func(*codecserver.Options)) *fixture {
	t.Helper()

	env := map[string]string{"A": string(local.Generate()), "B": string(local.Generate())}
	cfg, err := envelope.ParseConfig([]byte(`
namespaces:
  ns-a: {current: a-1, keys: [{id: a-1, env: A}]}
  ns-b: {current: b-1, keys: [{id: b-1, env: B}]}
`))
	require.NoError(t, err)
	kr, err := envelope.Open(t.Context(), cfg, envelope.OpenOptions{Getenv: func(n string) string { return env[n] }})
	require.NoError(t, err)

	trail := &bytes.Buffer{}
	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithWriter(trail))
	require.NoError(t, err)

	opts := codecserver.Options{
		Codecs:         kr.PayloadCodecConfig(),
		Tenancy:        &auth.Tenancy{Temporal: map[string]string{"team-a": "ns-a", "team-b": "ns-b"}},
		AllowedOrigins: []string{"https://temporal.example.com"},
		Auditor:        recorder,
	}
	if mutate != nil {
		mutate(&opts)
	}
	h, err := codecserver.New(opts)
	require.NoError(t, err)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if p, ok := principals[token]; ok {
			r = r.WithContext(auth.ContextWithPrincipal(r.Context(), p))
		}
		h.ServeHTTP(w, r)
	})
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	return &fixture{codecs: opts.Codecs, server: srv, handler: handler, trail: trail}
}

// seal is what a worker in namespace ns writes to history.
func (f *fixture) seal(t *testing.T, ns, text string) *commonpb.Payload {
	t.Helper()
	one, err := f.codecs.ForNamespace(ns)
	require.NoError(t, err)
	p, err := one.DataConverter().ToPayload(text)
	require.NoError(t, err)
	return p
}

func (f *fixture) post(t *testing.T, endpoint, token, ns string, payloads ...*commonpb.Payload) (*http.Response, string) {
	t.Helper()
	body, err := protojson.Marshal(&commonpb.Payloads{Payloads: payloads})
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, f.server.URL+"/codec"+endpoint, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if ns != "" {
		req.Header.Set(codecserver.NamespaceHeader, ns)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp, string(out)
}

// TestTemporalsOwnRemoteCodecClientDecodesThroughTheServer is the protocol
// check: the SDK's remote codec, which is what the Temporal CLI and UI use,
// round-trips through this server.
func TestTemporalsOwnRemoteCodecClientDecodesThroughTheServer(t *testing.T) {
	t.Parallel()

	f := newFixture(t, nil)
	remote := converter.NewRemotePayloadCodec(converter.RemotePayloadCodecOptions{
		Endpoint: f.server.URL + "/codec",
		ModifyRequest: func(r *http.Request) error {
			r.Header.Set("Authorization", "Bearer a-decoder")
			r.Header.Set(codecserver.NamespaceHeader, "ns-a")
			return nil
		},
	})

	sealed := f.seal(t, "ns-a", markerA)
	decoded, err := remote.Decode([]*commonpb.Payload{sealed})
	require.NoError(t, err)
	var got string
	require.NoError(t, payloadcodec.Serializer().FromPayload(decoded[0], &got))
	require.Equal(t, markerA, got)

	// Encode through the server, and the namespace's own worker reads it.
	plain, err := payloadcodec.Serializer().ToPayload(markerA)
	require.NoError(t, err)
	encoded, err := remote.Encode([]*commonpb.Payload{plain})
	require.NoError(t, err)
	require.Equal(t, envelope.Encoding, string(encoded[0].GetMetadata()["encoding"]))
	one, _ := f.codecs.ForNamespace("ns-a")
	require.NoError(t, one.DataConverter().FromPayload(encoded[0], &got))
	require.Equal(t, markerA, got)
}

// TestTwoTenantsCannotReadEachOther: identical requests, different keys; a
// forged namespace header, a spliced ciphertext, and a caller without the
// explicit action are all refused, and nothing refused carries plaintext.
func TestTwoTenantsCannotReadEachOther(t *testing.T) {
	t.Parallel()

	f := newFixture(t, nil)
	a := f.seal(t, "ns-a", markerA)
	b := f.seal(t, "ns-b", markerB)

	resp, body := f.post(t, codecserver.DecodeEndpoint, "a-decoder", "ns-a", a)
	require.Equal(t, http.StatusOK, resp.StatusCode, body)
	require.Contains(t, body, jsonBase64(markerA), "the authorized decode did not return the plaintext")
	require.Equal(t, "no-store", resp.Header.Get("Cache-Control"))

	cases := []struct {
		name, token, ns string
		payload         *commonpb.Payload
		status          int
	}{
		{"forged namespace header", "a-decoder", "ns-b", b, http.StatusForbidden},
		{"other tenant's ciphertext under own header", "a-decoder", "ns-a", b, http.StatusBadRequest},
		{"read action is not decode", "a-reader", "ns-a", a, http.StatusForbidden},
		{"no action list is not decode", "a-legacy", "ns-a", a, http.StatusForbidden},
		{"encode-less caller cannot encode", "b-decoder", "ns-b", b, http.StatusForbidden},
		{"unauthenticated", "", "ns-a", a, http.StatusUnauthorized},
		{"unknown token", "nobody", "ns-a", a, http.StatusUnauthorized},
		{"missing namespace header", "a-decoder", "", a, http.StatusBadRequest},
	}
	for _, c := range cases {
		endpoint := codecserver.DecodeEndpoint
		if c.name == "encode-less caller cannot encode" {
			endpoint = codecserver.EncodeEndpoint
		}
		resp, body := f.post(t, endpoint, c.token, c.ns, c.payload)
		require.Equal(t, c.status, resp.StatusCode, "%s: %s", c.name, body)
		require.NotContains(t, body, markerA, c.name)
		require.NotContains(t, body, markerB, c.name)
		require.NotContains(t, body, jsonBase64(markerA), c.name)
		require.NotContains(t, body, jsonBase64(markerB), c.name)
		require.Equal(t, "no-store", resp.Header.Get("Cache-Control"), c.name)

		// A scope challenge only where a new token would help: the action is
		// missing. A caller holding it who named another tenant's namespace
		// is not told to go and get a scope it already has.
		challenge := resp.Header.Get("WWW-Authenticate")
		switch c.name {
		case "read action is not decode", "no action list is not decode", "encode-less caller cannot encode":
			require.Contains(t, challenge, `error="insufficient_scope"`, c.name)
		case "forged namespace header":
			require.Empty(t, challenge, c.name)
		}
	}

	// Every decision is on the trail, by endpoint, and the trail holds no
	// payload.
	trail := f.trail.String()
	require.NotContains(t, trail, markerA)
	require.NotContains(t, trail, markerB)
	var allows, denies int
	forgedRecorded := false
	for line := range strings.SplitSeq(strings.TrimSpace(trail), "\n") {
		var rec v1.AuditRecord
		require.NoError(t, protojson.Unmarshal([]byte(line), &rec), line)
		require.NoError(t, v1.Validate(&rec))
		require.Contains(t, []string{codecserver.DecodeEndpoint, codecserver.EncodeEndpoint}, rec.GetHttpEndpoint())
		// The resource is the Temporal namespace addressed, not the
		// caller's tenant, so a forged header names what it reached for.
		require.Contains(t, []string{"ns-a", "ns-b"}, rec.GetResourceKey(), line)
		if rec.GetIdentity().GetSubject() == "alice" && rec.GetResourceKey() == "ns-b" &&
			rec.GetDecision() == v1.AuditDecision_AUDIT_DECISION_DENY {
			forgedRecorded = true
		}
		if id := rec.GetIdentity(); id.GetSubject() != "" {
			// The coordinates an RPC record carries for the same caller, so
			// the two correlate: issuer and subject apart, never joined.
			require.Equal(t, "https://issuer.example", id.GetIssuer(), line)
			require.Contains(t, []string{"alice", "ann", "abe", "bob"}, id.GetSubject(), line)
		}
		switch rec.GetDecision() {
		case v1.AuditDecision_AUDIT_DECISION_ALLOW:
			allows++
		case v1.AuditDecision_AUDIT_DECISION_DENY:
			denies++
		}
	}
	require.Equal(t, 2, allows, "the successful decode and the spliced one that authorization admitted")
	// The missing-header request is refused as a malformed request before any
	// authorization decision, so it is not on the trail; the other six are.
	require.Equal(t, 6, denies)
	require.True(t, forgedRecorded, "the forged header's denial did not name the namespace it addressed")
}

func TestSharedNamespacesAreRefusedUnlessAllowed(t *testing.T) {
	t.Parallel()

	shared := func(o *codecserver.Options) {
		o.Tenancy = &auth.Tenancy{Temporal: map[string]string{"team-a": "ns-a", "team-b": "ns-a"}}
	}
	f := newFixture(t, shared)
	a := f.seal(t, "ns-a", markerA)

	resp, body := f.post(t, codecserver.DecodeEndpoint, "a-decoder", "ns-a", a)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
	require.Contains(t, body, "shared")

	// A default namespace is shared with every unmapped tenant, too.
	f = newFixture(t, func(o *codecserver.Options) {
		o.Tenancy = &auth.Tenancy{Temporal: map[string]string{"team-a": "ns-a"}, Default: "ns-a"}
	})
	resp, _ = f.post(t, codecserver.DecodeEndpoint, "a-decoder", "ns-a", f.seal(t, "ns-a", markerA))
	require.Equal(t, http.StatusForbidden, resp.StatusCode)

	f = newFixture(t, func(o *codecserver.Options) {
		shared(o)
		o.AllowSharedNamespaces = true
	})
	resp, _ = f.post(t, codecserver.DecodeEndpoint, "a-decoder", "ns-a", f.seal(t, "ns-a", markerA))
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestBrowserOriginsAreExact(t *testing.T) {
	t.Parallel()

	f := newFixture(t, nil)
	do := func(method, origin string) *http.Response {
		req, err := http.NewRequest(method, f.server.URL+"/decode", strings.NewReader(`{"payloads":[]}`))
		require.NoError(t, err)
		req.Header.Set("Origin", origin)
		req.Header.Set("Authorization", "Bearer a-decoder")
		req.Header.Set(codecserver.NamespaceHeader, "ns-a")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		return resp
	}

	ok := do(http.MethodOptions, "https://temporal.example.com")
	require.Equal(t, http.StatusNoContent, ok.StatusCode)
	require.Equal(t, "https://temporal.example.com", ok.Header.Get("Access-Control-Allow-Origin"))
	require.Contains(t, ok.Header.Get("Access-Control-Allow-Headers"), codecserver.NamespaceHeader)

	for _, bad := range []string{"https://evil.example.com", "https://temporal.example.com.evil", "null"} {
		resp := do(http.MethodPost, bad)
		require.Equal(t, http.StatusForbidden, resp.StatusCode, bad)
		require.Empty(t, resp.Header.Get("Access-Control-Allow-Origin"), bad)
	}

	for _, origin := range []string{
		"*", "https://x.example/", "", "null", "https://x.example/path", "https://user@x.example",
		"ftp://x.example", "https://x.example?q=1", "https://x.example#f", "https://X.example", "x.example",
	} {
		_, err := codecserver.New(codecserver.Options{Codecs: f.codecs, AllowedOrigins: []string{origin}})
		require.Error(t, err, "origin %q was accepted", origin)
	}
	for _, origin := range []string{"https://x.example", "http://localhost:8233", "https://[::1]:8080"} {
		_, err := codecserver.New(codecserver.Options{Codecs: f.codecs, AllowedOrigins: []string{origin}})
		require.NoError(t, err, "origin %q was refused", origin)
	}
}

func TestRequestsAreBoundedBeforeAnyKeyIsTouched(t *testing.T) {
	t.Parallel()

	f := newFixture(t, func(o *codecserver.Options) {
		o.MaxBodyBytes = 1 << 10
		o.MaxPayloads = 2
		o.RequestsPerMinute = 3
	})
	a := f.seal(t, "ns-a", markerA)

	big := &commonpb.Payload{Data: make([]byte, 2<<10)}
	resp, _ := f.post(t, codecserver.DecodeEndpoint, "a-decoder", "ns-a", big)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	resp, _ = f.post(t, codecserver.DecodeEndpoint, "a-decoder", "ns-a", a, a, a)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	resp, _ = f.post(t, codecserver.DecodeEndpoint, "a-decoder", "ns-a", a)
	require.Equal(t, http.StatusOK, resp.StatusCode, "the third request in a window was refused")

	resp, _ = f.post(t, codecserver.DecodeEndpoint, "a-decoder", "ns-a", a)
	require.Equal(t, http.StatusTooManyRequests, resp.StatusCode, "the fourth request in a window was served")
	require.Equal(t, "60", resp.Header.Get("Retry-After"))

	// Another caller has a window of its own.
	resp, _ = f.post(t, codecserver.DecodeEndpoint, "b-decoder", "ns-b", f.seal(t, "ns-b", markerB))
	require.Equal(t, http.StatusOK, resp.StatusCode)

	for _, method := range []string{http.MethodGet, http.MethodPut} {
		req, err := http.NewRequest(method, f.server.URL+"/decode", nil)
		require.NoError(t, err)
		r, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		r.Body.Close()
		require.Equal(t, http.StatusMethodNotAllowed, r.StatusCode)
	}
}

// TestTheEndpointsAreTheBoundOnes holds the handler's two routes to the
// authorization bindings, in both directions.
func TestTheEndpointsAreTheBoundOnes(t *testing.T) {
	t.Parallel()

	var bound []string
	for _, b := range v1.AuthorizationActionBindings() {
		bound = append(bound, b.GetHttpEndpoints()...)
	}
	slices.Sort(bound)
	require.Equal(t, []string{codecserver.DecodeEndpoint, codecserver.EncodeEndpoint}, bound)

	decode, err := v1.AuthorizationActionForHTTPEndpoint(codecserver.DecodeEndpoint)
	require.NoError(t, err)
	require.Equal(t, v1.AuthorizationAction_AUTHORIZATION_ACTION_PAYLOAD_DECODE, decode)
	require.Equal(t, "payload.decode", v1.AuthorizationActionScope(decode))
}

func TestInsecureModeServesAnyCaller(t *testing.T) {
	t.Parallel()

	f := newFixture(t, func(o *codecserver.Options) { o.Insecure = true })
	resp, body := f.post(t, codecserver.DecodeEndpoint, "", "ns-b", f.seal(t, "ns-b", markerB))
	require.Equal(t, http.StatusOK, resp.StatusCode, body)
}

// jsonBase64 is how protojson spells a payload's data holding the JSON string
// text, which is what a decoded payload's bytes are.
func jsonBase64(text string) string {
	raw, _ := json.Marshal(text)
	b, _ := json.Marshal(raw)
	return strings.Trim(string(b), `"`)
}

// failingAuditor is a required recorder whose sink is down.
type failingAuditor struct{}

func (failingAuditor) Allow(context.Context, audit.Subject) error { return errors.New("sink down") }

func (failingAuditor) Deny(context.Context, audit.Subject, v1.AuditDenyCode) error {
	return errors.New("sink down")
}

// TestARequiredTrailThatCannotRecordIsAnOutage: with a recorder that cannot
// record, no decision is acted on, allowed or refused, and nothing is
// released. A refusal answered normally would leave a gap in a trail the
// operator required to be complete.
func TestARequiredTrailThatCannotRecordIsAnOutage(t *testing.T) {
	t.Parallel()

	f := newFixture(t, func(o *codecserver.Options) { o.Auditor = failingAuditor{} })
	a := f.seal(t, "ns-a", markerA)
	b := f.seal(t, "ns-b", markerB)

	for name, c := range map[string]struct {
		token, ns string
		payload   *commonpb.Payload
	}{
		"allowed":          {"a-decoder", "ns-a", a},
		"refused":          {"a-reader", "ns-a", a},
		"forged namespace": {"a-decoder", "ns-b", b},
		"undecodable":      {"a-decoder", "ns-a", b},
	} {
		resp, body := f.post(t, codecserver.DecodeEndpoint, c.token, c.ns, c.payload)
		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode, name)
		require.NotContains(t, body, jsonBase64(markerA), name)
	}
}

// slowCodec takes a second to process each payload, as a provider unwrapping
// data keys it has not seen would.
type slowCodec struct {
	payloadcodec.Codec
	calls int
}

func (c *slowCodec) Decode(p []*commonpb.Payload) ([]*commonpb.Payload, error) {
	c.calls++
	<-time.After(time.Second) // fake time: only ever called inside synctest.Test
	return p, nil
}

// TestOneRequestStartsBoundedWork: a decode needing more provider time than
// its budget stops starting payloads once the budget has passed and answers
// 503, rather than running on after its response deadline cut it off.
func TestOneRequestStartsBoundedWork(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		env := map[string]string{"A": string(local.Generate())}
		cfg, err := envelope.ParseConfig([]byte(`
namespaces:
  ns-a: {current: a-1, keys: [{id: a-1, env: A}]}
`))
		require.NoError(t, err)
		kr, err := envelope.Open(t.Context(), cfg, envelope.OpenOptions{Getenv: func(n string) string { return env[n] }})
		require.NoError(t, err)
		sealer, _ := kr.Codec("ns-a")
		slow := &slowCodec{Codec: sealer}

		h, err := codecserver.New(codecserver.Options{
			Codecs:      payloadcodec.Config{Codec: kr.Reader(), Namespaces: map[string]payloadcodec.Codec{"ns-a": slow}},
			Insecure:    true,
			WorkTimeout: 3 * time.Second,
		})
		require.NoError(t, err)

		payloads := make([]*commonpb.Payload, 10)
		for i := range payloads {
			payloads[i] = &commonpb.Payload{Data: []byte("x")}
		}
		body, err := protojson.Marshal(&commonpb.Payloads{Payloads: payloads})
		require.NoError(t, err)
		req := httptest.NewRequest(http.MethodPost, "/codec"+codecserver.DecodeEndpoint, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(codecserver.NamespaceHeader, "ns-a")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
		require.Equal(t, 3, slow.calls, "work was started past the request's budget")
	})
}

// TestABodyThatCannotBeReadIsNotASizeRefusal: only the byte limit is a size
// decision the trail records; a read that failed otherwise is a bad request.
func TestABodyThatCannotBeReadIsNotASizeRefusal(t *testing.T) {
	t.Parallel()

	f := newFixture(t, nil)
	h, err := codecserver.New(codecserver.Options{Codecs: f.codecs, Insecure: true})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/codec"+codecserver.DecodeEndpoint, iotest.ErrReader(errors.New("connection reset")))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(codecserver.NamespaceHeader, "ns-a")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.NotContains(t, rec.Body.String(), "limit", "a failed read was reported as a size refusal")
}

// TestRequestsInFlightAreBoundedAcrossCallers: the per-caller rate bounds how
// many requests a caller sends in a minute, not how many bodies the server
// holds at once. Past MaxConcurrent, a request is refused before its body is
// read, whoever sends it, and the slot is free again once a request is done.
func TestRequestsInFlightAreBoundedAcrossCallers(t *testing.T) {
	t.Parallel()

	f := newFixture(t, func(o *codecserver.Options) { o.MaxConcurrent = 1 })
	a := f.seal(t, "ns-a", markerA)

	// Served in process, so the body is read by the handler itself: its
	// first read says the request holds the one slot, and it holds it until
	// released.
	body := &heldBody{reading: make(chan struct{}), release: make(chan struct{})}
	req := httptest.NewRequest(http.MethodPost, "/codec"+codecserver.DecodeEndpoint, body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer a-decoder")
	req.Header.Set(codecserver.NamespaceHeader, "ns-a")
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.handler.ServeHTTP(httptest.NewRecorder(), req)
	}()
	<-body.reading

	resp, _ := f.post(t, codecserver.DecodeEndpoint, "b-decoder", "ns-b", f.seal(t, "ns-b", markerB))
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode, "another caller was served while the one slot was held")
	require.Equal(t, "1", resp.Header.Get("Retry-After"))

	close(body.release)
	<-done
	resp, _ = f.post(t, codecserver.DecodeEndpoint, "a-decoder", "ns-a", a)
	require.Equal(t, http.StatusOK, resp.StatusCode, "the slot was not given back")
}

// heldBody is a request body whose first read reports it, then waits.
type heldBody struct {
	reading, release chan struct{}
	once             sync.Once
}

func (b *heldBody) Read([]byte) (int, error) {
	b.once.Do(func() { close(b.reading) })
	<-b.release
	return 0, io.EOF
}
