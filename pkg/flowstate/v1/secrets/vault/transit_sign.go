package vault

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// MaxTransitKeyVersions bounds how many key versions [Transit.ReadSigningKey]
// accepts in one answer. A Transit key keeps every version it has not trimmed,
// so the count is the backend's to grow; the limit is what keeps a key rotated
// for years, or a backend answering with a million entries, from becoming the
// reader's allocation.
const MaxTransitKeyVersions = 256

// maxTransitSignInput bounds the data one [Transit.Sign] call sends. It is the
// request-side twin of the response bound every call has: the caller's data is
// small by construction (a JWS signing input), and a limit here keeps a bug
// from turning one signature into an unbounded upload.
const maxTransitSignInput = 128 << 10

// ErrTransitSignature reports a sign answer that is not a signature in the format
// the request asked for: a malformed envelope, versions that disagree, or a
// body that is not the encoding requested. The backend answered, and what it
// said is wrong, so it is neither a retry nor a permission problem.
var ErrTransitSignature = errors.New("secrets/vault: transit answered a malformed signature")

// TransitKeyVersion is one version of an asymmetric Transit key as the backend
// reports it: the public half and nothing else.
type TransitKeyVersion struct {
	// Name is how Transit names the key's curve or algorithm for this version,
	// such as "P-256" or "ed25519".
	Name string

	// PublicKey is the public half exactly as Transit renders it: a PKIX PEM
	// for ECDSA and RSA keys, standard base64 of the raw 32 bytes for Ed25519.
	// Parsing it is the caller's job, because only the caller knows which key
	// types it admits.
	PublicKey string
}

// TransitSigningKey is what [Transit.ReadSigningKey] reports about an
// asymmetric key: its type, the version new signatures use, and the public half
// of every version the backend still holds.
type TransitSigningKey struct {
	// Type is the key type, such as "ecdsa-p256" or "ed25519".
	Type string

	// LatestVersion is the version Transit signs with when a request names none.
	LatestVersion uint32

	// MinDecryptionVersion is the oldest version Transit will still use; zero
	// means every version it holds. Raising it is how an operator retires a
	// version, so a caller that publishes previous versions should stop at it.
	MinDecryptionVersion uint32

	// Versions holds the public half of each version the backend returned,
	// keyed by version number.
	Versions map[uint32]TransitKeyVersion
}

// ReadSigningKey reads the named asymmetric key's type, versions, and the
// public half of each version. It reads metadata and public keys, never private
// material, and needs only "read" on <mount>/keys/<key>.
//
// A key that holds no public half, a symmetric key, is not an error here: the
// caller decides what it admits from Type. An answer with more than
// [MaxTransitKeyVersions] versions is refused.
func (t *Transit) ReadSigningKey(ctx context.Context, key string) (TransitSigningKey, error) {
	apiPath, err := t.path("keys", key)
	if err != nil {
		return TransitSigningKey{}, err
	}

	response, err := t.call(ctx, http.MethodGet, apiPath, nil, false)
	if err != nil {
		return TransitSigningKey{}, err
	}

	var payload struct {
		Data struct {
			Type                 string                     `json:"type"`
			LatestVersion        uint32                     `json:"latest_version"`
			MinDecryptionVersion uint32                     `json:"min_decryption_version"`
			Keys                 map[string]json.RawMessage `json:"keys"`
		} `json:"data"`
	}
	if err := decodeJSON(response, &payload); err != nil {
		return TransitSigningKey{}, fmt.Errorf("%s answered %q with %w", t.p.addr, apiPath, err)
	}

	data := payload.Data

	switch {
	case data.Type == "":
		return TransitSigningKey{}, fmt.Errorf("%s answered %q with no key type", t.p.addr, apiPath)
	case len(data.Keys) > MaxTransitKeyVersions:
		return TransitSigningKey{}, fmt.Errorf("%s answered %q with %d key versions, over the limit of %d",
			t.p.addr, apiPath, len(data.Keys), MaxTransitKeyVersions)
	}

	versions := make(map[uint32]TransitKeyVersion, len(data.Keys))
	var highest uint32
	for number, raw := range data.Keys {
		n, err := strconv.ParseUint(number, 10, 32)
		if err != nil || n == 0 {
			return TransitSigningKey{}, fmt.Errorf("%s answered %q with a key version that is not a positive number", t.p.addr, apiPath)
		}

		// A symmetric key lists versions as bare numbers or opaque strings;
		// the caller refuses its type, so a version that is not an object is
		// left empty here rather than raised as an error that would hide the
		// better one.
		var entry struct {
			Name      string `json:"name"`
			PublicKey string `json:"public_key"`
		}
		_ = json.Unmarshal(raw, &entry)

		versions[uint32(n)] = TransitKeyVersion{Name: entry.Name, PublicKey: entry.PublicKey}
		highest = max(highest, uint32(n))
	}

	// Older servers omit latest_version; the highest listed version is the
	// same number.
	latest := data.LatestVersion
	if latest == 0 {
		latest = highest
	}
	if latest == 0 {
		return TransitSigningKey{}, fmt.Errorf("%s answered %q with no key version", t.p.addr, apiPath)
	}

	return TransitSigningKey{
		Type:                 data.Type,
		LatestVersion:        latest,
		MinDecryptionVersion: data.MinDecryptionVersion,
		Versions:             versions,
	}, nil
}

// TransitSignRequest is one signature [Transit.Sign] asks for.
type TransitSignRequest struct {
	// Input is the message to sign. Transit hashes it with HashAlgorithm
	// before signing (ECDSA), or signs it as it stands (Ed25519); it is never
	// sent as a prehashed digest, so what is signed is what was passed here.
	Input []byte

	// KeyVersion is the version to sign with. Zero lets Transit choose its
	// latest, which a caller that has told anyone which version it signs with
	// must not do: a rotation between the two would sign with a key nobody was
	// told about.
	KeyVersion uint32

	// HashAlgorithm names the digest for key types that hash, such as
	// "sha2-256". Empty sends none and takes Transit's default.
	HashAlgorithm string

	// JWS asks for an ECDSA signature as the raw r||s concatenation, base64url
	// without padding, which is what a JWS carries, rather than the ASN.1 DER
	// Transit answers with by default. Key types with one signature format
	// ignore it, so it is meaningful only for ECDSA.
	JWS bool
}

// TransitSignature is Transit's answer to [Transit.Sign].
type TransitSignature struct {
	// Signature is the decoded signature bytes, in the format the request asked
	// for.
	Signature []byte

	// KeyVersion is the version Transit says it signed with.
	KeyVersion uint32
}

// Sign signs the request's input with the named key. The key never leaves
// Transit; only the input and the signature cross the wire. It needs "update"
// on <mount>/sign/<key>.
//
// A refusal is classified as for every Transit call: 403 is
// [secrets.ErrPermission], an unreachable or sealed vault is
// [secrets.ErrUnavailable], and a missing key is [secrets.ErrNotFound]. No
// error it returns carries the input, the signature, a token, or a response
// body.
func (t *Transit) Sign(ctx context.Context, key string, request TransitSignRequest) (TransitSignature, error) {
	apiPath, err := t.path("sign", key)
	if err != nil {
		return TransitSignature{}, err
	}

	if len(request.Input) == 0 || len(request.Input) > maxTransitSignInput {
		return TransitSignature{}, fmt.Errorf("secrets/vault: a Transit signing input is 1 to %d bytes", maxTransitSignInput)
	}

	type signBody struct {
		Input               string `json:"input"`
		KeyVersion          uint32 `json:"key_version,omitempty"`
		HashAlgorithm       string `json:"hash_algorithm,omitempty"`
		MarshalingAlgorithm string `json:"marshaling_algorithm,omitempty"`
	}
	built := signBody{
		Input:         base64.StdEncoding.EncodeToString(request.Input),
		KeyVersion:    request.KeyVersion,
		HashAlgorithm: request.HashAlgorithm,
	}
	if request.JWS {
		built.MarshalingAlgorithm = "jws"
	}

	body, err := json.Marshal(built)
	if err != nil {
		return TransitSignature{}, fmt.Errorf("building the sign request: %w", err)
	}

	response, err := t.call(ctx, http.MethodPost, apiPath, body, false)
	if err != nil {
		return TransitSignature{}, err
	}

	var payload struct {
		Data struct {
			Signature  string `json:"signature"`
			KeyVersion uint32 `json:"key_version"`
		} `json:"data"`
	}
	if err := decodeJSON(response, &payload); err != nil {
		return TransitSignature{}, fmt.Errorf("%s answered %q with %w", t.p.addr, apiPath, err)
	}

	version, encoded, ok := splitSignature(payload.Data.Signature)
	if !ok {
		return TransitSignature{}, fmt.Errorf("%w: %s answered %q with a malformed envelope", ErrTransitSignature, t.p.addr, apiPath)
	}

	// The envelope names which version signed; the separate field is advisory
	// and must agree with it.
	if payload.Data.KeyVersion != 0 && payload.Data.KeyVersion != version {
		return TransitSignature{}, fmt.Errorf("%w: %s answered %q with disagreeing key versions", ErrTransitSignature, t.p.addr, apiPath)
	}

	encoding := base64.StdEncoding.Strict()
	if request.JWS {
		encoding = base64.RawURLEncoding.Strict()
	}

	raw, err := encoding.DecodeString(encoded)
	if err != nil {
		return TransitSignature{}, fmt.Errorf("%w: %s answered %q with a signature that is not in the format asked for (%d characters)",
			ErrTransitSignature, t.p.addr, apiPath, len(encoded))
	}

	return TransitSignature{Signature: raw, KeyVersion: version}, nil
}

// splitSignature takes Transit's "vault:v<N>:<body>" envelope apart. It is not
// [CiphertextVersion], whose body check is standard padded base64: a JWS
// signature is unpadded base64url, and each caller decodes its own body.
func splitSignature(signature string) (version uint32, body string, ok bool) {
	rest, found := strings.CutPrefix(signature, ciphertextPrefix)
	if !found {
		return 0, "", false
	}

	digits, body, found := strings.Cut(rest, ":")
	if !found || body == "" || digits == "" || len(digits) > 10 || digits[0] == '0' || strings.IndexFunc(digits, notDigit) >= 0 {
		return 0, "", false
	}

	n, err := strconv.ParseUint(digits, 10, 32)
	if err != nil {
		return 0, "", false
	}

	return uint32(n), body, true
}
