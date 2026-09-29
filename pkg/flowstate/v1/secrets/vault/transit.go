package vault

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// DefaultTransitMount is where the Transit secrets engine is mounted in a stock
// installation.
const DefaultTransitMount = "transit"

// maxTransitKeyName bounds a Transit key name.
const maxTransitKeyName = 128

// ciphertextPrefix begins every ciphertext Transit produces, before the key
// version. OpenBao kept Vault's prefix for compatibility.
const ciphertextPrefix = "vault:v"

// ErrInvalidCiphertext reports a ciphertext Transit refused to decrypt: not
// one of its own, altered, bound to other associated data, or under a key
// version below the key's minimum decryption version. It is permanent.
var ErrInvalidCiphertext = errors.New("secrets/vault: transit refused the ciphertext")

// Transit encrypts and decrypts with named keys of a Vault or OpenBao Transit
// secrets engine. The keys never leave Vault; only a key's name, the data, and
// its ciphertext cross the wire.
//
// It shares everything below the API path with [Provider]: the address rules,
// TLS, the refusal to follow redirects, authentication and its token cache, the
// X-Vault-Namespace header, timeouts, response bounds, and the classification of
// failures as [secrets.ErrUnavailable], [secrets.ErrPermission] and
// [secrets.ErrNotFound], plus [ErrInvalidCiphertext] for a refused decryption.
//
// No error it returns carries plaintext, ciphertext, associated data, a token,
// or a response body.
//
// Vault's encrypt endpoint creates a key that does not exist when the caller's
// policy grants "create" on it. Grant only "update" on
// <mount>/encrypt/<key> and <mount>/decrypt/<key>, and "read" on
// <mount>/keys/<key>, so a deleted key is an error rather than silently
// replaced with a new one.
//
// A Transit is safe for concurrent use.
type Transit struct {
	p     *Provider
	mount string
}

// NewTransit returns a Transit client for the engine mounted at mount (an
// empty mount means [DefaultTransitMount]) on the Vault or OpenBao at addr.
//
// It takes the same options as [NewProvider] and validates them the same way,
// except the KV-only ones — [WithMount], [WithPathPrefix] and [WithScheme] —
// which it refuses rather than ignores.
func NewTransit(addr, mount string, opts ...Option) (*Transit, error) {
	p, err := NewProvider(addr, opts...)
	if err != nil {
		return nil, err
	}

	if p.mount != DefaultMount || p.prefix != "" || p.scheme != DefaultScheme {
		return nil, fmt.Errorf(
			"secrets/vault: WithMount, WithPathPrefix and WithScheme configure KV reads; " +
				"pass the Transit mount to NewTransit instead",
		)
	}

	cleaned := DefaultTransitMount
	if mount != "" {
		cleaned, err = cleanMount(mount, "NewTransit")
		if err != nil {
			return nil, err
		}
	}

	return &Transit{p: p, mount: cleaned}, nil
}

// Address returns the vault's address as configured. It is safe to log.
func (t *Transit) Address() string { return t.p.addr }

// Mount returns the Transit mount. It is safe to log.
func (t *Transit) Mount() string { return t.mount }

// TransitKey is what [Transit.ReadKey] reports about a key.
type TransitKey struct {
	// Type is the key type, such as "aes256-gcm96" or "chacha20-poly1305".
	Type string

	// LatestVersion is the version new encryptions use.
	LatestVersion uint32

	// MinDecryptionVersion is the oldest version Vault will decrypt under.
	MinDecryptionVersion uint32

	// SupportsEncryption and SupportsDecryption say whether the key type
	// encrypts and decrypts at all.
	SupportsEncryption, SupportsDecryption bool

	// Derived is a key that derives a per-request key from a context, which
	// every request must then supply.
	Derived bool
}

// Encrypt encrypts plaintext under the named key, authenticating
// associatedData with it (AEAD key types only), and returns Transit's
// ciphertext with the key version that produced it.
func (t *Transit) Encrypt(ctx context.Context, key string, plaintext, associatedData []byte) (string, uint32, error) {
	apiPath, err := t.path("encrypt", key)
	if err != nil {
		return "", 0, err
	}

	// Written out rather than marshaled: the plaintext is a data key, and
	// json.Marshal would take it as a base64 string, which nothing can clear.
	// Base64 needs no JSON escaping, so appending it between quotes is the
	// encoding.
	body := make([]byte, 0, 64+base64.StdEncoding.EncodedLen(len(plaintext))+base64.StdEncoding.EncodedLen(len(associatedData)))
	body = append(body, `{"plaintext":"`...)
	body = base64.StdEncoding.AppendEncode(body, plaintext)
	body = append(body, '"')
	if len(associatedData) > 0 {
		body = append(body, `,"associated_data":"`...)
		body = base64.StdEncoding.AppendEncode(body, associatedData)
		body = append(body, '"')
	}
	body = append(body, '}')
	defer clear(body)

	response, err := t.call(ctx, http.MethodPost, apiPath, body, false)
	if err != nil {
		return "", 0, err
	}

	var payload struct {
		Data struct {
			Ciphertext string `json:"ciphertext"`
		} `json:"data"`
	}
	if err := decodeJSON(response, &payload); err != nil {
		return "", 0, fmt.Errorf("%s answered %q with %w", t.p.addr, apiPath, err)
	}

	version, err := CiphertextVersion(payload.Data.Ciphertext)
	if err != nil {
		return "", 0, fmt.Errorf("%s answered %q with a malformed ciphertext: %w", t.p.addr, apiPath, err)
	}

	return payload.Data.Ciphertext, version, nil
}

// Decrypt decrypts a ciphertext [Transit.Encrypt] produced under the named key
// and the same associatedData. A ciphertext that is malformed is refused before
// any request; one Vault refuses is [ErrInvalidCiphertext].
func (t *Transit) Decrypt(ctx context.Context, key, ciphertext string, associatedData []byte) ([]byte, error) {
	apiPath, err := t.path("decrypt", key)
	if err != nil {
		return nil, err
	}

	if _, err := CiphertextVersion(ciphertext); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCiphertext, err)
	}

	body, err := json.Marshal(struct {
		Ciphertext     string `json:"ciphertext"`
		AssociatedData string `json:"associated_data,omitempty"`
	}{
		Ciphertext:     ciphertext,
		AssociatedData: base64.StdEncoding.EncodeToString(associatedData),
	})
	if err != nil {
		return nil, fmt.Errorf("building the decrypt request: %w", err)
	}

	response, err := t.call(ctx, http.MethodPost, apiPath, body, true)
	if err != nil {
		return nil, err
	}
	// The response holds the plaintext.
	defer clear(response)

	// The plaintext is a data key, so it is taken as raw bytes rather than a
	// string, which nothing could clear, and decoded by json.Unmarshal in
	// place rather than through a json.Decoder, whose buffer would be a copy
	// of the response nothing clears either.
	var payload struct {
		Data struct {
			Plaintext json.RawMessage `json:"plaintext"`
		} `json:"data"`
	}
	err = json.Unmarshal(response, &payload)
	defer clear(payload.Data.Plaintext)
	if err != nil {
		return nil, fmt.Errorf("%s answered %q with %w", t.p.addr, apiPath, describeJSONError(err, response))
	}

	plaintext, err := decodeBase64String(payload.Data.Plaintext)
	if err != nil {
		// The decoder's error quotes the offending byte; report only the length.
		return nil, fmt.Errorf(
			"%s answered %q with a plaintext that is not a base64 string (%d bytes)",
			t.p.addr, apiPath, len(payload.Data.Plaintext),
		)
	}

	return plaintext, nil
}

// decodeBase64String decodes a JSON string of standard base64 into a buffer
// the caller owns and clears, clearing it itself on failure. The one escape
// base64 can meet in JSON, `\/`, is undone in place; any other is not base64.
func decodeBase64String(raw json.RawMessage) ([]byte, error) {
	if len(raw) < 2 || raw[0] != '"' || raw[len(raw)-1] != '"' {
		return nil, errors.New("not a JSON string")
	}
	text := raw[1 : len(raw)-1]
	n := 0
	for i := 0; i < len(text); i++ {
		c := text[i]
		if c == '\\' {
			if i+1 == len(text) || text[i+1] != '/' {
				return nil, errors.New("an escape base64 does not use")
			}
			c = '/'
			i++
		}
		text[n] = c
		n++
	}
	text = text[:n]

	plaintext := make([]byte, base64.StdEncoding.DecodedLen(len(text)))
	written, err := base64.StdEncoding.Decode(plaintext, text)
	if err != nil {
		clear(plaintext)
		return nil, err
	}
	return plaintext[:written], nil
}

// ReadKey reports the named key's type, versions, and capabilities. It reads
// the key's metadata, never its material.
func (t *Transit) ReadKey(ctx context.Context, key string) (TransitKey, error) {
	apiPath, err := t.path("keys", key)
	if err != nil {
		return TransitKey{}, err
	}

	response, err := t.call(ctx, http.MethodGet, apiPath, nil, false)
	if err != nil {
		return TransitKey{}, err
	}

	var payload struct {
		Data struct {
			Type                 string                     `json:"type"`
			LatestVersion        uint32                     `json:"latest_version"`
			MinDecryptionVersion uint32                     `json:"min_decryption_version"`
			SupportsEncryption   bool                       `json:"supports_encryption"`
			SupportsDecryption   bool                       `json:"supports_decryption"`
			Derived              bool                       `json:"derived"`
			Keys                 map[string]json.RawMessage `json:"keys"`
		} `json:"data"`
	}
	if err := decodeJSON(response, &payload); err != nil {
		return TransitKey{}, fmt.Errorf("%s answered %q with %w", t.p.addr, apiPath, err)
	}

	data := payload.Data

	// Older servers omit latest_version; the highest version in keys is the
	// same number.
	latest := data.LatestVersion
	if latest == 0 {
		for version := range data.Keys {
			n, err := strconv.ParseUint(version, 10, 32)
			if err != nil {
				return TransitKey{}, fmt.Errorf("%s answered %q with a key version that is not a number", t.p.addr, apiPath)
			}
			latest = max(latest, uint32(n))
		}
	}

	switch {
	case data.Type == "":
		return TransitKey{}, fmt.Errorf("%s answered %q with no key type", t.p.addr, apiPath)
	case latest == 0:
		return TransitKey{}, fmt.Errorf("%s answered %q with no key version", t.p.addr, apiPath)
	case data.MinDecryptionVersion > latest:
		return TransitKey{}, fmt.Errorf(
			"%s answered %q with a minimum decryption version %d above the latest %d",
			t.p.addr, apiPath, data.MinDecryptionVersion, latest,
		)
	}

	return TransitKey{
		Type:                 data.Type,
		LatestVersion:        latest,
		MinDecryptionVersion: data.MinDecryptionVersion,
		SupportsEncryption:   data.SupportsEncryption,
		SupportsDecryption:   data.SupportsDecryption,
		Derived:              data.Derived,
	}, nil
}

// CiphertextVersion validates a Transit ciphertext, "vault:v<N>:<base64>", and
// returns N. A refusal never quotes the ciphertext.
func CiphertextVersion(ciphertext string) (uint32, error) {
	rest, ok := strings.CutPrefix(ciphertext, ciphertextPrefix)
	if !ok {
		return 0, fmt.Errorf("a Transit ciphertext begins %q", ciphertextPrefix)
	}

	digits, encoded, ok := strings.Cut(rest, ":")
	switch {
	case !ok:
		return 0, fmt.Errorf("a Transit ciphertext has a version and a body")
	case digits == "" || len(digits) > 10 || digits[0] == '0' || strings.IndexFunc(digits, notDigit) >= 0:
		return 0, fmt.Errorf("a Transit ciphertext's version is a positive decimal number")
	case encoded == "":
		return 0, fmt.Errorf("a Transit ciphertext's body is empty")
	}

	version, err := strconv.ParseUint(digits, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("a Transit ciphertext's version is out of range")
	}

	if _, err := base64.StdEncoding.Strict().DecodeString(encoded); err != nil {
		return 0, fmt.Errorf("a Transit ciphertext's body is not standard base64 (%d characters)", len(encoded))
	}

	return uint32(version), nil
}

func notDigit(r rune) bool { return r < '0' || r > '9' }

// path builds <mount>/<operation>/<key> for a validated key name.
func (t *Transit) path(operation, key string) (string, error) {
	if err := validateTransitKey(key); err != nil {
		return "", err
	}

	return t.mount + "/" + operation + "/" + key, nil
}

// validateTransitKey admits [A-Za-z0-9._-]{1,128}, except "." and "..", which
// a URL path would resolve rather than send.
func validateTransitKey(key string) error {
	switch {
	case key == "" || len(key) > maxTransitKeyName:
		return fmt.Errorf("secrets/vault: a Transit key name is 1 to %d characters", maxTransitKeyName)
	case key == "." || key == "..":
		return fmt.Errorf("secrets/vault: %q is not a Transit key name", key)
	}

	for _, r := range key {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '.', r == '_', r == '-':
		default:
			return fmt.Errorf(
				"secrets/vault: a Transit key name may only contain letters, digits, dots, underscores, and dashes",
			)
		}
	}

	return nil
}

// call sends one authenticated request and classifies its status. Vault's error
// body is matched against, never quoted.
func (t *Transit) call(ctx context.Context, method, apiPath string, body []byte, decrypting bool) ([]byte, error) {
	status, response, err := t.p.send(ctx, method, apiPath, body)
	if err != nil {
		return nil, err
	}

	addr := t.p.addr

	switch {
	case status == http.StatusOK:
		return response, nil

	case status == http.StatusForbidden:
		return nil, fmt.Errorf("%w: %s refused %q", secrets.ErrPermission, addr, apiPath)

	case status == http.StatusNotFound,
		status == http.StatusBadRequest && bytes.Contains(response, []byte("key not found")):
		// Vault answers a read of a missing key with 404, and an encrypt or
		// decrypt under one with 400 "encryption key not found".
		return nil, fmt.Errorf("%w: %s has no Transit key at %q", secrets.ErrNotFound, addr, apiPath)

	case status == http.StatusBadRequest && decrypting:
		return nil, fmt.Errorf("%w: %s answered %d to %q", ErrInvalidCiphertext, addr, status, apiPath)

	case unavailable(status):
		return nil, unavailableStatus(addr, status, apiPath)

	default:
		return nil, fmt.Errorf("%s answered %d to %q", addr, status, apiPath)
	}
}
