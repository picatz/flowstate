// Package vaulttest serves an in-memory stand-in for the part of the Vault
// Transit secrets engine that Flowstate's payload key provider uses, so tests
// in any package can wrap and unwrap data keys without a Vault, a network, or
// a credential.
//
// The server implements reading a key (GET /v1/transit/keys/NAME), encrypt
// and decrypt (POST /v1/transit/encrypt/NAME and /v1/transit/decrypt/NAME),
// and the Kubernetes login (POST /v1/auth/kubernetes/login). It does real
// AES-256-GCM with the request's associated_data, so associated data and key
// versions are genuinely bound rather than simulated, and it honors key
// rotation and the minimum decryption version.
//
//	srv := vaulttest.NewServer(t)
//	srv.Create("payloads", "aes256-gcm96")
//	transit, err := vault.NewTransit(srv.URL(), "", vault.WithToken(vaulttest.Token))
//
// Every error body the server writes contains [ErrorMarker], so a test can
// assert that a client never echoes what Vault said.
package vaulttest

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
)

const (
	// Token is the static Vault token the server accepts until
	// [Server.RevokeTokens] is called.
	Token = "static-token"

	// Role is the Kubernetes auth role the login endpoint accepts, with any
	// non-empty JWT.
	Role = "flowstate-worker"

	// ErrorMarker is contained in every error body the server writes. It
	// stands for whatever Vault says, which a client must never echo.
	ErrorMarker = "vault-said-something-private"
)

type key struct {
	typ        string
	derived    bool
	noDecrypt  bool
	versions   [][]byte // versions[i] is version i+1
	minDecrypt int
}

// Server is an in-memory Transit engine served over HTTP. Start one with
// [NewServer]. A Server is safe for concurrent use.
type Server struct {
	server *httptest.Server

	mu       sync.Mutex
	keys     map[string]*key
	accepted map[string]bool

	issued int
	logins int

	// status, when set, answers every transit request with it.
	status int

	// hang holds every transit request until the client gives up.
	hang bool

	// oversize answers an encrypt with a well-formed but huge ciphertext.
	oversize bool

	// bodies records every transit request body, to check what crossed the
	// wire.
	bodies []string
}

// NewServer starts a Server that accepts [Token] and Kubernetes logins for
// [Role], and closes it when the test ends.
func NewServer(t testing.TB) *Server {
	t.Helper()

	f := &Server{
		keys:     make(map[string]*key),
		accepted: map[string]bool{Token: true},
	}
	f.server = httptest.NewServer(f)
	t.Cleanup(f.server.Close)

	return f
}

// URL is the server's base address: the Vault address a client is given.
func (f *Server) URL() string { return f.server.URL }

// Create adds, or replaces, a key named name of Transit type typ (such as
// "aes256-gcm96") with one version and a minimum decryption version of 1.
// Encryption is real AES-256-GCM whatever typ says; typ is only reported.
func (f *Server) Create(name, typ string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.keys[name] = &key{typ: typ, minDecrypt: 1, versions: [][]byte{randomKey()}}
}

func randomKey() []byte {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return b
}

// Rotate adds a version to the named key; later encryptions use it.
func (f *Server) Rotate(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	k := f.keys[name]
	k.versions = append(k.versions, randomKey())
}

// SetMinDecrypt sets the named key's minimum decryption version; ciphertext
// of an older version is refused.
func (f *Server) SetMinDecrypt(name string, v int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.keys[name].minDecrypt = v
}

// SetDerived sets whether the named key reports itself as derived.
func (f *Server) SetDerived(name string, derived bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.keys[name].derived = derived
}

// SetNoDecrypt sets whether the named key reports that it does not support
// decryption.
func (f *Server) SetNoDecrypt(name string, noDecrypt bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.keys[name].noDecrypt = noDecrypt
}

// SetStatus makes every Transit request fail with status and an error body
// containing [ErrorMarker]; zero restores normal service. Logins are
// unaffected.
func (f *Server) SetStatus(status int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.status = status
}

// SetHang makes every Transit request block until the client gives up.
func (f *Server) SetHang(hang bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.hang = hang
}

// SetOversize makes every encrypt answer with a well-formed but oversized
// ciphertext.
func (f *Server) SetOversize(oversize bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.oversize = oversize
}

// Bodies returns a copy of every Transit request body received since the
// server started or [Server.ResetBodies] was last called. Login bodies are not
// recorded.
func (f *Server) Bodies() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.bodies)
}

// ResetBodies forgets the recorded request bodies.
func (f *Server) ResetBodies() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.bodies = nil
}

// Logins is the number of Kubernetes login attempts received, successful or
// not.
func (f *Server) Logins() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.logins
}

// RevokeTokens revokes every token, [Token] and those issued by login alike,
// so the next Transit request is refused as forbidden.
func (f *Server) RevokeTokens() {
	f.mu.Lock()
	defer f.mu.Unlock()

	clear(f.accepted)
}

// ServeHTTP serves the part of the Vault API the server fakes.
func (f *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/")

	var raw json.RawMessage
	if r.Method == http.MethodPost {
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			writeErrors(w, http.StatusBadRequest)
			return
		}
	}

	if path == "auth/kubernetes/login" {
		f.serveLogin(w, raw)
		return
	}

	f.mu.Lock()
	f.bodies = append(f.bodies, string(raw))
	status, hang, oversize := f.status, f.hang, f.oversize
	valid := f.accepted[r.Header.Get("X-Vault-Token")]
	f.mu.Unlock()

	if hang {
		<-r.Context().Done()
		return
	}
	if status != 0 {
		writeErrors(w, status)
		return
	}
	if !valid {
		writeErrors(w, http.StatusForbidden)
		return
	}

	mount, rest, _ := strings.Cut(path, "/")
	operation, name, _ := strings.Cut(rest, "/")
	if mount != "transit" {
		writeErrors(w, http.StatusNotFound)
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	key := f.keys[name]

	switch {
	case operation == "keys" && r.Method == http.MethodGet:
		if key == nil {
			writeErrors(w, http.StatusNotFound)
			return
		}
		versions := map[string]int64{}
		for i := range key.versions {
			versions[strconv.Itoa(i+1)] = 1700000000
		}
		writeData(w, map[string]any{
			"type":                   key.typ,
			"derived":                key.derived,
			"keys":                   versions,
			"latest_version":         len(key.versions),
			"min_decryption_version": key.minDecrypt,
			"min_encryption_version": 0,
			"name":                   name,
			"supports_encryption":    true,
			"supports_decryption":    !key.noDecrypt,
		})

	case operation == "encrypt" && r.Method == http.MethodPost:
		if key == nil {
			writeErrors(w, http.StatusBadRequest, "encryption key not found")
			return
		}
		var req struct {
			Plaintext      string `json:"plaintext"`
			AssociatedData string `json:"associated_data"`
		}
		_ = json.Unmarshal(raw, &req)
		plaintext, err1 := base64.StdEncoding.DecodeString(req.Plaintext)
		aad, err2 := base64.StdEncoding.DecodeString(req.AssociatedData)
		if err1 != nil || err2 != nil {
			writeErrors(w, http.StatusBadRequest)
			return
		}
		if oversize {
			writeData(w, map[string]any{"ciphertext": "vault:v1:" + base64.StdEncoding.EncodeToString(make([]byte, 600))})
			return
		}
		version := len(key.versions)
		gcm := newGCM(key.versions[version-1])
		nonce := make([]byte, gcm.NonceSize())
		_, _ = rand.Read(nonce)
		sealed := gcm.Seal(nonce, nonce, plaintext, aad)
		writeData(w, map[string]any{
			"ciphertext":  fmt.Sprintf("vault:v%d:%s", version, base64.StdEncoding.EncodeToString(sealed)),
			"key_version": version,
		})

	case operation == "decrypt" && r.Method == http.MethodPost:
		if key == nil {
			writeErrors(w, http.StatusBadRequest, "encryption key not found")
			return
		}
		var req struct {
			Ciphertext     string `json:"ciphertext"`
			AssociatedData string `json:"associated_data"`
		}
		_ = json.Unmarshal(raw, &req)
		aad, _ := base64.StdEncoding.DecodeString(req.AssociatedData)

		rest, ok := strings.CutPrefix(req.Ciphertext, "vault:v")
		digits, encoded, ok2 := strings.Cut(rest, ":")
		version, err := strconv.Atoi(digits)
		sealed, err2 := base64.StdEncoding.DecodeString(encoded)
		switch {
		case !ok || !ok2 || err != nil || err2 != nil:
			writeErrors(w, http.StatusBadRequest, "invalid ciphertext")
			return
		case version < key.minDecrypt || version > len(key.versions):
			writeErrors(w, http.StatusBadRequest, "ciphertext or signature version is disallowed by policy (too old)")
			return
		}
		gcm := newGCM(key.versions[version-1])
		if len(sealed) < gcm.NonceSize() {
			writeErrors(w, http.StatusBadRequest, "invalid ciphertext: unable to decrypt")
			return
		}
		plaintext, err := gcm.Open(nil, sealed[:gcm.NonceSize()], sealed[gcm.NonceSize():], aad)
		if err != nil {
			writeErrors(w, http.StatusBadRequest, "cipher: message authentication failed")
			return
		}
		writeData(w, map[string]any{"plaintext": base64.StdEncoding.EncodeToString(plaintext)})

	default:
		writeErrors(w, http.StatusMethodNotAllowed)
	}
}

func (f *Server) serveLogin(w http.ResponseWriter, raw json.RawMessage) {
	var body struct {
		Role string `json:"role"`
		JWT  string `json:"jwt"`
	}
	_ = json.Unmarshal(raw, &body)

	f.mu.Lock()
	defer f.mu.Unlock()

	f.logins++
	if body.Role != Role || body.JWT == "" {
		writeErrors(w, http.StatusBadRequest)
		return
	}

	f.issued++
	token := fmt.Sprintf("issued-token-%d", f.issued)
	f.accepted[token] = true

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"auth":{"client_token":%q,"lease_duration":3600,"renewable":true}}`, token)
}

func newGCM(key []byte) cipher.AEAD {
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		panic(err)
	}
	return gcm
}

func writeData(w http.ResponseWriter, data map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
}

// writeErrors answers the way Vault does, with a JSON errors array, always
// including [ErrorMarker].
func writeErrors(w http.ResponseWriter, status int, messages ...string) {
	body, _ := json.Marshal(map[string]any{"errors": append(messages, ErrorMarker)})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
