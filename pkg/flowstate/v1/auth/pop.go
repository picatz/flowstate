package auth

// Proof-of-possession access-token support.  The types in this file are Go
// boundary types deliberately: proof keys and replay decisions must never be
// serialized into a workflow specification or Temporal history.

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type TokenMode string

const (
	TokenModeBearer           TokenMode = "bearer"
	TokenModeDPoP             TokenMode = "dpop"
	TokenModeCertificateBound TokenMode = "mtls"
)

// ReplayStore is intentionally an interface: clustered servers must use a
// shared, atomic implementation (Redis SET NX with TTL, a database unique key,
// etc.). CheckAndStore returns false when key was already live.
type ReplayStore interface {
	CheckAndStore(context.Context, string, time.Time) (bool, error)
}

// MemoryReplayStore is bounded and expiring. It is suitable for one process;
// pass the same instance to multiple authenticators in tests or nodes sharing
// an address space. Production clusters should provide a distributed store.
type MemoryReplayStore struct {
	mu      sync.Mutex
	entries map[string]time.Time
	max     int
}

func NewMemoryReplayStore(max int) *MemoryReplayStore {
	if max <= 0 {
		max = 10000
	}
	return &MemoryReplayStore{entries: make(map[string]time.Time), max: max}
}

func (s *MemoryReplayStore) CheckAndStore(_ context.Context, key string, expires time.Time) (bool, error) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, until := range s.entries {
		if !now.Before(until) {
			delete(s.entries, k)
		}
	}
	if until, ok := s.entries[key]; ok && now.Before(until) {
		return false, nil
	}
	if len(s.entries) >= s.max {
		return false, errors.New("auth: DPoP replay store is full")
	}
	s.entries[key] = expires
	return true, nil
}

// DPoPConfig configures one serving surface. CanonicalURL, when set, is the
// trusted externally visible URL resolver used behind a TLS terminator. It must
// ignore untrusted forwarding headers. Nonce returns the currently required AS
// or RS nonce; returning an empty string disables nonce enforcement.
type DPoPConfig struct {
	Replay       ReplayStore
	MaxAge       time.Duration
	ClockSkew    time.Duration
	CanonicalURL func(*http.Request) (*url.URL, error)
	Nonce        func(context.Context, *http.Request) (string, error)
}

// TrustedProxyConfig describes exactly which proxy addresses may supply the
// external scheme/host and verified client certificate. Never enable it for an
// untrusted direct peer.
type TrustedProxyConfig struct {
	Trusted           []*net.IPNet
	CertificateHeader string
}

func (p TrustedProxyConfig) trusted(remote string) bool {
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		host = remote
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	for _, n := range p.Trusted {
		if n != nil && n.Contains(ip) {
			return true
		}
	}
	return false
}

// CanonicalURL returns an external URL. Forwarded values are honored only from
// a configured trusted proxy and must be single, syntactically valid values.
func (p TrustedProxyConfig) CanonicalURL(r *http.Request) (*url.URL, error) {
	u := *r.URL
	if !p.trusted(r.RemoteAddr) {
		if r.TLS != nil {
			u.Scheme = "https"
		} else {
			u.Scheme = "http"
		}
		u.Host = r.Host
		return &u, nil
	}
	proto, host := r.Header.Get("X-Forwarded-Proto"), r.Header.Get("X-Forwarded-Host")
	if strings.ContainsAny(proto+host, ",\r\n") || (proto != "https" && proto != "http") || host == "" {
		return nil, errors.New("auth: invalid trusted proxy URL headers")
	}
	u.Scheme, u.Host = proto, host
	return &u, nil
}

var b64 = base64.RawURLEncoding

func decodePart(s string, dst any) error {
	b, err := b64.DecodeString(s)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, dst)
}

func jwkKey(j map[string]any) (*ecdsa.PublicKey, error) {
	if j["kty"] != "EC" || j["crv"] != "P-256" {
		return nil, errors.New("DPoP jwk must be an EC P-256 public key")
	}
	if _, private := j["d"]; private {
		return nil, errors.New("DPoP jwk must not contain private key material")
	}
	xs, xok := j["x"].(string)
	ys, yok := j["y"].(string)
	if !xok || !yok {
		return nil, errors.New("DPoP jwk coordinates are required")
	}
	xb, e1 := b64.DecodeString(xs)
	yb, e2 := b64.DecodeString(ys)
	if e1 != nil || e2 != nil {
		return nil, errors.New("invalid DPoP jwk coordinates")
	}
	k := &ecdsa.PublicKey{Curve: elliptic.P256(), X: new(big.Int).SetBytes(xb), Y: new(big.Int).SetBytes(yb)}
	if !k.Curve.IsOnCurve(k.X, k.Y) {
		return nil, errors.New("invalid DPoP public key")
	}
	return k, nil
}

func jwkThumbprint(j map[string]any) (string, error) {
	k, err := jwkKey(j)
	if err != nil {
		return "", err
	}
	canon, _ := json.Marshal(map[string]string{"crv": "P-256", "kty": "EC", "x": b64.EncodeToString(k.X.FillBytes(make([]byte, 32))), "y": b64.EncodeToString(k.Y.FillBytes(make([]byte, 32)))})
	h := sha256.Sum256(canon)
	return b64.EncodeToString(h[:]), nil
}

func canonicalHTU(u *url.URL) string {
	v := *u
	v.Fragment = ""
	v.RawQuery = ""
	host := strings.ToLower(v.Hostname())
	port := v.Port()
	if port != "" && !((v.Scheme == "https" && port == "443") || (v.Scheme == "http" && port == "80")) {
		host = net.JoinHostPort(host, port)
	}
	v.Scheme = strings.ToLower(v.Scheme)
	v.Host = host
	if v.Path == "" {
		v.Path = "/"
	}
	return v.String()
}

func verifyDPoP(ctx context.Context, req *http.Request, token string, principal Principal, cfg DPoPConfig) error {
	proof := req.Header.Get("DPoP")
	parts := strings.Split(proof, ".")
	if len(parts) != 3 {
		return errors.New("auth: missing or malformed DPoP proof")
	}
	var hdr struct {
		Typ, Alg string
		JWK      map[string]any `json:"jwk"`
	}
	if err := decodePart(parts[0], &hdr); err != nil {
		return errors.New("auth: invalid DPoP JOSE header")
	}
	if !strings.EqualFold(hdr.Typ, "dpop+jwt") || hdr.Alg != "ES256" {
		return errors.New("auth: DPoP typ must be dpop+jwt and alg ES256")
	}
	key, err := jwkKey(hdr.JWK)
	if err != nil {
		return err
	}
	sig, err := b64.DecodeString(parts[2])
	if err != nil || len(sig) != 64 {
		return errors.New("auth: invalid DPoP signature")
	}
	hash := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if !ecdsa.Verify(key, hash[:], new(big.Int).SetBytes(sig[:32]), new(big.Int).SetBytes(sig[32:])) {
		return errors.New("auth: invalid DPoP signature")
	}
	var c struct {
		HTM, HTU, JTI, ATH, Nonce string
		IAT                       int64
	}
	if err := decodePart(parts[1], &c); err != nil {
		return errors.New("auth: invalid DPoP claims")
	}
	if c.HTM != req.Method {
		return errors.New("auth: DPoP method mismatch")
	}
	u := req.URL
	if cfg.CanonicalURL != nil {
		u, err = cfg.CanonicalURL(req)
		if err != nil {
			return err
		}
	} else {
		copy := *req.URL
		copy.Host = req.Host
		if req.TLS != nil {
			copy.Scheme = "https"
		} else {
			copy.Scheme = "http"
		}
		u = &copy
	}
	if c.HTU != canonicalHTU(u) {
		return errors.New("auth: DPoP URI mismatch")
	}
	now := time.Now()
	maxAge := cfg.MaxAge
	if maxAge <= 0 {
		maxAge = 5 * time.Minute
	}
	skew := cfg.ClockSkew
	if skew <= 0 {
		skew = 30 * time.Second
	}
	issued := time.Unix(c.IAT, 0)
	if c.IAT == 0 || issued.Before(now.Add(-maxAge-skew)) || issued.After(now.Add(skew)) {
		return errors.New("auth: stale DPoP proof")
	}
	if c.JTI == "" {
		return errors.New("auth: DPoP jti is required")
	}
	ath := sha256.Sum256([]byte(token))
	if subtle.ConstantTimeCompare([]byte(c.ATH), []byte(b64.EncodeToString(ath[:]))) != 1 {
		return errors.New("auth: DPoP access-token hash mismatch")
	}
	jkt, err := jwkThumbprint(hdr.JWK)
	if err != nil {
		return err
	}
	cnf, _ := principal.Claims["cnf"].(map[string]any)
	bound, _ := cnf["jkt"].(string)
	if bound == "" || subtle.ConstantTimeCompare([]byte(jkt), []byte(bound)) != 1 {
		return errors.New("auth: DPoP token key mismatch")
	}
	if cfg.Nonce != nil {
		wanted, e := cfg.Nonce(ctx, req)
		if e != nil {
			return e
		}
		if wanted != "" && subtle.ConstantTimeCompare([]byte(wanted), []byte(c.Nonce)) != 1 {
			return errors.New("auth: DPoP nonce required")
		}
	}
	if cfg.Replay == nil {
		return errors.New("auth: DPoP replay store is not configured")
	}
	fresh, e := cfg.Replay.CheckAndStore(ctx, jkt+":"+c.JTI, issued.Add(maxAge+skew))
	if e != nil {
		return e
	}
	if !fresh {
		return errors.New("auth: DPoP proof replayed")
	}
	return nil
}

func verifyCertificateBinding(req *http.Request, p Principal) error {
	if req.TLS == nil || len(req.TLS.VerifiedChains) == 0 || len(req.TLS.VerifiedChains[0]) == 0 {
		return errors.New("auth: certificate-bound token requires a verified client certificate")
	}
	leaf := req.TLS.VerifiedChains[0][0]
	sum := sha256.Sum256(leaf.Raw)
	got := b64.EncodeToString(sum[:])
	cnf, _ := p.Claims["cnf"].(map[string]any)
	want, _ := cnf["x5t#S256"].(string)
	if want == "" || subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
		return errors.New("auth: certificate-bound token mismatch")
	}
	return nil
}

// CertificateThumbprint computes RFC 8705's base64url SHA-256 leaf thumbprint.
func CertificateThumbprint(cert *x509.Certificate) (string, error) {
	if cert == nil {
		return "", errors.New("auth: nil certificate")
	}
	h := sha256.Sum256(cert.Raw)
	return b64.EncodeToString(h[:]), nil
}

// NewDPoPCredential generates a P-256 proof key in the calling activity's
// memory and binds it to token. The returned Credential cannot serialize the
// key or token; callers must construct and apply it wholly inside an activity.
func NewDPoPCredential(expiresAt time.Time, token string) (Credential, error) {
	if token == "" {
		return Credential{}, errors.New("auth: DPoP credential needs an access token")
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return Credential{}, err
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return Credential{}, err
	}
	return NewCredential(CredentialDPoP, expiresAt, map[string]string{CredentialAccessToken: token, CredentialProofKey: b64.EncodeToString(der)})
}

func makeProof(key *ecdsa.PrivateKey, token string, req *http.Request, now time.Time) (string, error) {
	x := b64.EncodeToString(key.X.FillBytes(make([]byte, 32)))
	y := b64.EncodeToString(key.Y.FillBytes(make([]byte, 32)))
	hdr, _ := json.Marshal(map[string]any{"typ": "dpop+jwt", "alg": "ES256", "jwk": map[string]string{"kty": "EC", "crv": "P-256", "x": x, "y": y}})
	ath := sha256.Sum256([]byte(token))
	jti := make([]byte, 18)
	if _, e := rand.Read(jti); e != nil {
		return "", e
	}
	claims, _ := json.Marshal(map[string]any{"jti": b64.EncodeToString(jti), "htm": req.Method, "htu": canonicalHTU(req.URL), "iat": now.Unix(), "ath": b64.EncodeToString(ath[:])})
	a := b64.EncodeToString(hdr) + "." + b64.EncodeToString(claims)
	h := sha256.Sum256([]byte(a))
	r, s, e := ecdsa.Sign(rand.Reader, key, h[:])
	if e != nil {
		return "", e
	}
	sig := append(r.FillBytes(make([]byte, 32)), s.FillBytes(make([]byte, 32))...)
	return a + "." + b64.EncodeToString(sig), nil
}
