package authtest

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/asn1"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TransitToken is the Vault token a [Transit] accepts until
// [Transit.RevokeToken] is called.
const TransitToken = "authtest-transit-token"

// TransitErrorMarker is contained in every error body a [Transit] writes. It
// stands for whatever a Vault says in one, which a client must never echo: a
// test asserts the marker is absent from every error it gets back.
const TransitErrorMarker = "authtest-vault-said-something-private"

// Key types a [Transit] can hold, spelled as Vault spells them.
const (
	// TransitECDSAP256 is an ECDSA P-256 key, which signs ES256.
	TransitECDSAP256 = "ecdsa-p256"

	// TransitEd25519 is an Ed25519 key, which signs EdDSA.
	TransitEd25519 = "ed25519"

	// TransitECDSAP384 is an ECDSA P-384 key. A signer for it has to be
	// refused: the issuer publishes no P-384 key.
	TransitECDSAP384 = "ecdsa-p384"

	// TransitRSA2048 is an RSA-2048 key. A Transit signer refuses it; it is
	// here so a test can show that refusal.
	TransitRSA2048 = "rsa-2048"
)

// transitVersion is one version of a key. The private half lives only here,
// in the test process, standing in for the backend that holds it.
type transitVersion struct {
	private crypto.Signer
	created time.Time
}

type transitKey struct {
	typ        string
	versions   []transitVersion // versions[i] is version i+1
	minDecrypt int
	minAvail   int // lowest version still held; versions below it are trimmed

	// publicOverride replaces the public half served for a version, which is a
	// backend that answers with a key that is not the one it signs with.
	publicOverride map[int]crypto.PublicKey
}

// TransitRequest is one request a [Transit] served, without its body or token.
type TransitRequest struct {
	// Method and Path are the HTTP method and URL path.
	Method, Path string

	// KeyVersion is the "key_version" a sign request asked for, zero when none.
	KeyVersion int

	// Marshaling is the "marshaling_algorithm" a sign request asked for.
	Marshaling string

	// HashAlgorithm is the "hash_algorithm" a sign request asked for.
	HashAlgorithm string

	// Prehashed is whether a sign request said its input was already a digest.
	Prehashed bool

	// HadToken is whether the request carried the expected token.
	HadToken bool
}

// Transit is an in-memory stand-in for the part of a Vault or OpenBao Transit
// secrets engine an asymmetric signer uses: reading a key (GET
// /v1/transit/keys/NAME) and signing with it (POST /v1/transit/sign/NAME). It
// does real ECDSA and Ed25519 signatures, so what a test verifies is what a
// backend would have produced, and it honours "key_version" and
// "marshaling_algorithm" the way Vault does.
//
// The private keys exist only inside the Transit, in the test process, standing
// in for the backend: nothing it serves returns one.
//
// Its failure knobs model a backend that is wrong rather than down: an
// answer with the wrong public key, a signature in the wrong format, an
// oversized body, a hang, a refusal.
//
// A Transit is safe for concurrent use. Close it when the test is done.
type Transit struct {
	server *httptest.Server

	mu       sync.Mutex
	keys     map[string]*transitKey
	tokenOK  bool
	token    string // the one token the Transit accepts
	requests []TransitRequest

	status      int           // answers every request with this status, when set
	hang        bool          // holds every request until the client gives up
	oversize    int           // pads every successful body to this many bytes
	ignoreJWS   bool          // answers with ASN.1 DER whatever format was asked for
	skewVersion bool          // signs with the latest version whatever one was asked for
	signDelay   time.Duration // holds each sign request this long

	inFlight, peak int
}

// NewTransit starts a Transit on a loopback listener. Reach it with
// [Transit.URL] and [TransitToken], through [EgressPolicy] since it is on
// loopback. It panics if the listener cannot be started.
func NewTransit() *Transit {
	transit := &Transit{keys: map[string]*transitKey{}, tokenOK: true, token: TransitToken}
	transit.server = httptest.NewServer(http.HandlerFunc(transit.serve))
	return transit
}

// URL returns the base address, such as http://127.0.0.1:41234.
func (t *Transit) URL() string { return t.server.URL }

// Close stops the listener. It always returns nil, so a test may defer it.
func (t *Transit) Close() error {
	t.mu.Lock()
	t.hang = false
	t.mu.Unlock()

	t.server.Close()
	return nil
}

// CreateKey creates version 1 of a key of the given type, which must be one of
// the Transit* constants.
func (t *Transit) CreateKey(name, typ string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	key := &transitKey{typ: typ, minAvail: 1}
	key.versions = append(key.versions, newTransitVersion(typ))
	t.keys[name] = key
}

// RotateKey adds a new version, which becomes the one Transit signs with when a
// request names none.
func (t *Transit) RotateKey(name string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	key := t.mustKey(name)
	key.versions = append(key.versions, newTransitVersion(key.typ))
}

// SetMinDecryptionVersion raises the oldest version the key still serves, which
// is how an operator retires a version.
func (t *Transit) SetMinDecryptionVersion(name string, version int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.mustKey(name).minDecrypt = version
}

// TrimKey drops every version below the given one from what the key holds, as
// Vault's trim endpoint does.
func (t *Transit) TrimKey(name string, below int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.mustKey(name).minAvail = below
}

// PublicKey returns the public half of a version of a key, which is what a
// relying party would be told. A test uses it to verify what the Transit signed.
func (t *Transit) PublicKey(name string, version int) crypto.PublicKey {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.mustKey(name).versions[version-1].private.Public()
}

// ServePublicKeyOf makes a version of a key answer with the public half of
// another key's version, so its published key no longer matches what it signs.
// It models a misconfigured or compromised backend, which a signer must catch
// before it mints a single assertion.
func (t *Transit) ServePublicKeyOf(name string, version int, otherName string, otherVersion int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	key := t.mustKey(name)
	if key.publicOverride == nil {
		key.publicOverride = map[int]crypto.PublicKey{}
	}
	key.publicOverride[version] = t.mustKey(otherName).versions[otherVersion-1].private.Public()
}

// RevokeToken makes the Transit refuse [TransitToken] with 403, as a Vault does
// for a token whose lease ended or whose policy was removed.
func (t *Transit) RevokeToken() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tokenOK = false
}

// AcceptToken makes the Transit accept the given token and refuse every other,
// including [TransitToken], with 403: a Vault Agent having rotated the token a
// worker holds.
func (t *Transit) AcceptToken(token string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.token = token
}

// SetStatus answers every request with the given status and an error body, or
// serves normally again when given zero.
func (t *Transit) SetStatus(status int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status = status
}

// Hang holds every request until the client gives up, or serves normally again
// when given false.
func (t *Transit) Hang(hang bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.hang = hang
}

// Oversize pads every successful answer to at least the given number of bytes,
// with a well-formed JSON field a client has no use for. Zero turns it off.
func (t *Transit) Oversize(bytes int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.oversize = bytes
}

// IgnoreMarshaling makes sign answer with ASN.1 DER whatever
// "marshaling_algorithm" was asked for, as a backend that does not know the
// parameter would. A client that takes the answer for r||s signs garbage.
func (t *Transit) IgnoreMarshaling() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.ignoreJWS = true
}

// IgnoreKeyVersion makes sign use the latest version whatever "key_version"
// was asked for, as a backend that does not honour the parameter would: the
// signature then does not match the version it was told it was made with.
func (t *Transit) IgnoreKeyVersion() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.skewVersion = true
}

// DelaySign holds each sign request for the given time before answering, so a
// test can put several in flight at once.
func (t *Transit) DelaySign(delay time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.signDelay = delay
}

// Requests returns what the Transit has served, oldest first.
func (t *Transit) Requests() []TransitRequest {
	t.mu.Lock()
	defer t.mu.Unlock()
	return slices.Clone(t.requests)
}

// PeakInFlight returns the most sign requests that were being served at once.
func (t *Transit) PeakInFlight() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.peak
}

func (t *Transit) mustKey(name string) *transitKey {
	key, ok := t.keys[name]
	if !ok {
		panic(fmt.Sprintf("authtest: no Transit key %q", name))
	}
	return key
}

func newTransitVersion(typ string) transitVersion {
	var (
		private crypto.Signer
		err     error
	)

	switch typ {
	case TransitECDSAP256:
		private, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	case TransitECDSAP384:
		private, err = ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	case TransitEd25519:
		_, private, err = ed25519.GenerateKey(rand.Reader)
	case TransitRSA2048:
		private, err = rsa.GenerateKey(rand.Reader, rsaKeyBits)
	default:
		panic(fmt.Sprintf("authtest: unknown Transit key type %q", typ))
	}
	if err != nil {
		panic(fmt.Sprintf("authtest: generating a %s key: %v", typ, err))
	}

	return transitVersion{private: private, created: time.Now().UTC()}
}

// serve answers the two endpoints.
func (t *Transit) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))

	t.mu.Lock()
	hadToken := r.Header.Get("X-Vault-Token") == t.token && t.tokenOK
	record := TransitRequest{Method: r.Method, Path: r.URL.Path, HadToken: hadToken}
	status, hang, oversize := t.status, t.hang, t.oversize
	t.mu.Unlock()

	if hang {
		<-r.Context().Done()
		return
	}

	rest, ok := strings.CutPrefix(r.URL.Path, "/v1/transit/")
	operation, name, _ := strings.Cut(rest, "/")

	var request struct {
		Input               string `json:"input"`
		KeyVersion          int    `json:"key_version"`
		HashAlgorithm       string `json:"hash_algorithm"`
		MarshalingAlgorithm string `json:"marshaling_algorithm"`
		Prehashed           bool   `json:"prehashed"`
	}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &request)
	}
	record.KeyVersion = request.KeyVersion
	record.Marshaling = request.MarshalingAlgorithm
	record.HashAlgorithm = request.HashAlgorithm
	record.Prehashed = request.Prehashed

	t.mu.Lock()
	t.requests = append(t.requests, record)
	t.mu.Unlock()

	switch {
	case status != 0:
		t.fail(w, status)
		return
	case !hadToken:
		t.fail(w, http.StatusForbidden)
		return
	case !ok:
		t.fail(w, http.StatusNotFound)
		return
	}

	var (
		data any
		code = http.StatusOK
	)

	switch {
	case operation == "keys" && r.Method == http.MethodGet:
		data, code = t.readKey(name)
	case operation == "sign" && r.Method == http.MethodPost:
		data, code = t.sign(name, request.Input, request.KeyVersion, request.MarshalingAlgorithm, request.HashAlgorithm, request.Prehashed)
	default:
		code = http.StatusNotFound
	}

	if code != http.StatusOK {
		t.fail(w, code)
		return
	}

	envelope := map[string]any{"request_id": "authtest", "data": data}
	if oversize > 0 {
		envelope["padding"] = strings.Repeat("x", oversize)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(envelope)
}

func (t *Transit) fail(w http.ResponseWriter, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `{"errors":[%q]}`, TransitErrorMarker)
}

func (t *Transit) readKey(name string) (any, int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	key, ok := t.keys[name]
	if !ok {
		return nil, http.StatusNotFound
	}

	versions := map[string]any{}
	for i := key.minAvail; i <= len(key.versions); i++ {
		if i < 1 {
			continue
		}
		public := key.versions[i-1].private.Public()
		if override, ok := key.publicOverride[i]; ok {
			public = override
		}

		entry := map[string]any{"creation_time": key.versions[i-1].created.Format(time.RFC3339)}
		switch typed := public.(type) {
		case ed25519.PublicKey:
			entry["name"] = "ed25519"
			entry["public_key"] = base64.StdEncoding.EncodeToString(typed)
		default:
			der, err := x509.MarshalPKIXPublicKey(public)
			if err != nil {
				panic(err)
			}
			entry["name"] = map[string]string{TransitECDSAP256: "P-256", TransitECDSAP384: "P-384", TransitRSA2048: "rsa-2048"}[key.typ]
			entry["public_key"] = string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
		}
		versions[strconv.Itoa(i)] = entry
	}

	return map[string]any{
		"type":                   key.typ,
		"latest_version":         len(key.versions),
		"min_decryption_version": max(key.minDecrypt, 1),
		"min_available_version":  key.minAvail - 1,
		"supports_signing":       true,
		"keys":                   versions,
	}, http.StatusOK
}

func (t *Transit) sign(name, input string, version int, marshaling, hash string, prehashed bool) (any, int) {
	t.mu.Lock()
	key, ok := t.keys[name]
	delay, ignoreJWS, skew := t.signDelay, t.ignoreJWS, t.skewVersion
	t.inFlight++
	t.peak = max(t.peak, t.inFlight)
	t.mu.Unlock()

	defer func() {
		t.mu.Lock()
		t.inFlight--
		t.mu.Unlock()
	}()

	if delay > 0 {
		time.Sleep(delay)
	}

	if !ok {
		return nil, http.StatusNotFound
	}

	message, err := base64.StdEncoding.DecodeString(input)
	if err != nil || len(message) == 0 {
		return nil, http.StatusBadRequest
	}

	if version == 0 {
		version = len(key.versions)
	}
	if version < 1 || version > len(key.versions) || version < key.minAvail || version < key.minDecrypt {
		return nil, http.StatusBadRequest
	}
	signWith := version
	if skew {
		signWith = len(key.versions)
	}

	var signature []byte

	switch private := key.versions[signWith-1].private.(type) {
	case *ecdsa.PrivateKey:
		if hash != "" && hash != "sha2-256" {
			return nil, http.StatusBadRequest
		}

		digest := message
		if !prehashed {
			sum := sha256.Sum256(message)
			digest = sum[:]
		}

		r, s, err := ecdsa.Sign(rand.Reader, private, digest)
		if err != nil {
			return nil, http.StatusInternalServerError
		}

		if marshaling == "jws" && !ignoreJWS {
			size := (private.Curve.Params().BitSize + 7) / 8
			signature = r.FillBytes(make([]byte, size))
			signature = append(signature, s.FillBytes(make([]byte, size))...)
			return map[string]any{
				"signature":   fmt.Sprintf("vault:v%d:%s", signWith, base64.RawURLEncoding.EncodeToString(signature)),
				"key_version": signWith,
			}, http.StatusOK
		}

		signature = transitDER(r, s)
	case ed25519.PrivateKey:
		signature = ed25519.Sign(private, message)
	default:
		// Vault's RSA default is PSS, which a JWS does not use; the fake does
		// not sign for key types a signer must refuse before it asks.
		return nil, http.StatusBadRequest
	}

	return map[string]any{
		"signature":   fmt.Sprintf("vault:v%d:%s", signWith, base64.StdEncoding.EncodeToString(signature)),
		"key_version": signWith,
	}, http.StatusOK
}

// transitDER encodes an ECDSA signature as ASN.1 DER, which is what Vault
// answers with unless asked for the JWS form.
func transitDER(r, s *big.Int) []byte {
	der, err := asn1.Marshal(struct{ R, S *big.Int }{r, s})
	if err != nil {
		panic(err)
	}
	return der
}
