package interop

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
)

// Endpoint identifies one independently scriptable protocol role.
type Endpoint string

const (
	Issuer              Endpoint = "issuer"
	AuthorizationServer Endpoint = "authorization_server"
	ResourceServer      Endpoint = "resource_server"
	JWKS                Endpoint = "jwks"
	TokenExchange       Endpoint = "token_exchange"
	XAA                 Endpoint = "xaa"
	IDJAG               Endpoint = "id_jag"
	SecurityEvents      Endpoint = "security_events"
	WorkloadAPI         Endpoint = "spiffe_workload_api"
)

// Request is the bounded, secret-redacted observation retained by Environment.
type Request struct {
	Endpoint Endpoint
	Method   string
	Path     string
	Header   http.Header
	Form     url.Values
}

// Response is one step in an endpoint script. After is invoked after the
// response is written and is useful for atomic key, policy, or certificate
// rotation between requests.
type Response struct {
	Status int
	Header http.Header
	Body   []byte
	Delay  time.Duration
	After  func(*Environment)
}

// Script is consumed in order. When RepeatLast is true its final response is
// reused; otherwise an exhausted script fails closed with HTTP 503.
type Script struct {
	Responses  []Response
	RepeatLast bool
}

type route struct {
	endpoint Endpoint
	script   Script
	next     int
}

// Environment is a self-contained set of protocol peers. No listener is bound
// beyond loopback and no default handler makes an outbound request.
type Environment struct {
	server      *httptest.Server
	dir         string
	workload    net.Listener
	mu          sync.Mutex
	routes      map[string]*route
	requests    []Request
	now         time.Time
	certificate *tlsMaterial
}

type tlsMaterial struct {
	certPEM, keyPEM []byte
	serial          string
}

// New starts every mock role and a SPIFFE Workload API-shaped Unix endpoint.
func New(now time.Time) (*Environment, error) {
	e := &Environment{routes: make(map[string]*route), now: now}
	e.server = httptest.NewServer(http.HandlerFunc(e.serveHTTP))
	dir, err := os.MkdirTemp("", "flowstate-interop-")
	if err != nil {
		e.server.Close()
		return nil, err
	}
	e.dir = dir
	l, err := net.Listen("unix", filepath.Join(dir, "workload.sock"))
	if err != nil {
		e.Close()
		return nil, err
	}
	e.workload = l
	if err := e.RotateCertificate(); err != nil {
		e.Close()
		return nil, err
	}
	e.Set(WorkloadAPI, Script{Responses: []Response{{Status: http.StatusOK}}, RepeatLast: true})
	go func() { _ = http.Serve(l, http.HandlerFunc(e.serveWorkload)) }()
	return e, nil
}

// Close releases all listeners and temporary credentials.
func (e *Environment) Close() error {
	if e.server != nil {
		e.server.Close()
	}
	if e.workload != nil {
		_ = e.workload.Close()
	}
	if e.dir != "" {
		return os.RemoveAll(e.dir)
	}
	return nil
}

// URL returns the URL of a mock role.
func (e *Environment) URL(endpoint Endpoint) string { return e.server.URL + "/" + string(endpoint) }

// WorkloadAPIAddr returns a unix:// address suitable for client configuration.
func (e *Environment) WorkloadAPIAddr() string {
	return "unix://" + filepath.Join(e.dir, "workload.sock")
}

// Now and Advance provide deterministic skew, expiry, and mid-session changes.
func (e *Environment) Now() time.Time { e.mu.Lock(); defer e.mu.Unlock(); return e.now }
func (e *Environment) Advance(d time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.now = e.now.Add(d)
}

// Set atomically replaces an endpoint's script.
func (e *Environment) Set(endpoint Endpoint, script Script) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.routes["/"+string(endpoint)] = &route{endpoint: endpoint, script: cloneScript(script)}
}

// Requests returns a snapshot of observations. Authorization, cookies, and
// token-bearing form values are omitted so credentials never enter test logs.
func (e *Environment) Requests() []Request {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.requests)
}

func cloneScript(s Script) Script { s.Responses = slices.Clone(s.Responses); return s }

func (e *Environment) serveHTTP(w http.ResponseWriter, r *http.Request) {
	e.mu.Lock()
	route := e.routes[r.URL.Path]
	if route == nil {
		e.mu.Unlock()
		http.NotFound(w, r)
		return
	}
	_ = r.ParseForm()
	form := r.PostForm.Clone()
	for _, k := range []string{"subject_token", "actor_token", "client_secret", "assertion"} {
		form.Del(k)
	}
	header := r.Header.Clone()
	for _, k := range []string{"Authorization", "Cookie", "DPoP"} {
		header.Del(k)
	}
	e.requests = append(e.requests, Request{route.endpoint, r.Method, r.URL.Path, header, form})
	if len(route.script.Responses) == 0 || (route.next >= len(route.script.Responses) && !route.script.RepeatLast) {
		e.mu.Unlock()
		http.Error(w, "script exhausted", http.StatusServiceUnavailable)
		return
	}
	idx := min(route.next, len(route.script.Responses)-1)
	response := route.script.Responses[idx]
	if route.next < len(route.script.Responses) {
		route.next++
	}
	e.mu.Unlock()
	if response.Delay > 0 {
		time.Sleep(response.Delay)
	}
	for k, values := range response.Header {
		for _, value := range values {
			w.Header().Add(k, value)
		}
	}
	status := response.Status
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	_, _ = w.Write(response.Body)
	if response.After != nil {
		response.After(e)
	}
}

func (e *Environment) serveWorkload(w http.ResponseWriter, r *http.Request) {
	e.mu.Lock()
	material := *e.certificate
	e.requests = append(e.requests, Request{Endpoint: WorkloadAPI, Method: r.Method, Path: r.URL.Path, Header: make(http.Header)})
	e.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"spiffe_id": "spiffe://flowstate.test/workload", "certificate": string(material.certPEM), "private_key": string(material.keyPEM), "serial": material.serial})
}

// RotateCertificate replaces the SVID returned to subsequent Workload API
// calls, allowing a long-lived client to prove it reloads mTLS material.
func (e *Environment) RotateCertificate() error {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}
	e.mu.Lock()
	now := e.now
	e.mu.Unlock()
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		return err
	}
	template := x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "spiffe://flowstate.test/workload"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	m := &tlsMaterial{certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), keyPEM: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), serial: serial.String()}
	e.mu.Lock()
	e.certificate = m
	e.mu.Unlock()
	return nil
}

// JSONResponse constructs a bounded JSON response and panics only for a test
// author error (a value json.Marshal cannot encode).
func JSONResponse(status int, value any) Response {
	body, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("interop: JSON response: %v", err))
	}
	return Response{Status: status, Header: http.Header{"Content-Type": {"application/json"}}, Body: body}
}

// Do issues a request using the hermetic server's client.
func (e *Environment) Do(ctx context.Context, endpoint Endpoint, method string, body *url.Values) (*http.Response, error) {
	var reader *strings.Reader
	if body == nil {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body.Encode())
	}
	req, err := http.NewRequestWithContext(ctx, method, e.URL(endpoint), reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	return e.server.Client().Do(req)
}
