package vault

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// signServer answers every request with one status and body, and reports the
// last request it saw.
func signServer(t *testing.T, status int, response string) (*Transit, func() (path, body string)) {
	t.Helper()

	var path, body string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		path, body = strings.TrimPrefix(r.URL.Path, "/v1/"), string(raw)

		w.WriteHeader(status)
		_, _ = io.WriteString(w, response)
	}))
	t.Cleanup(server.Close)

	transit, err := NewTransit(server.URL, "transit", WithToken("static-token"))
	require.NoError(t, err)

	return transit, func() (string, string) { return path, body }
}

func Test_Transit_Sign(t *testing.T) {
	t.Run("a JWS signature, with the version pinned", func(t *testing.T) {
		// 64 bytes of 0xfb encode with both - and _ in base64url, and with + and /
		// in the standard alphabet, so the wrong decoder cannot pass.
		transit, seen := signServer(t, http.StatusOK,
			`{"data":{"signature":"vault:v3:`+strings.Repeat("-_", 42)+`-w","key_version":3}}`)

		got, err := transit.Sign(t.Context(), "k", TransitSignRequest{
			Input: []byte("a.b"), KeyVersion: 3, HashAlgorithm: "sha2-256", JWS: true,
		})
		require.NoError(t, err)
		require.Equal(t, uint32(3), got.KeyVersion)
		require.Len(t, got.Signature, 64)

		path, body := seen()
		require.Equal(t, "transit/sign/k", path)
		require.JSONEq(t, `{"input":"YS5i","key_version":3,"hash_algorithm":"sha2-256","marshaling_algorithm":"jws"}`, body)
	})

	t.Run("no format asked for sends none", func(t *testing.T) {
		transit, seen := signServer(t, http.StatusOK, `{"data":{"signature":"vault:v1:AAAA"}}`)

		_, err := transit.Sign(t.Context(), "k", TransitSignRequest{Input: []byte("m")})
		require.NoError(t, err)

		_, body := seen()
		require.JSONEq(t, `{"input":"bQ=="}`, body)
	})

	for name, response := range map[string]string{
		"no signature":           `{"data":{}}`,
		"no envelope":            `{"data":{"signature":"AAAA"}}`,
		"a zero version":         `{"data":{"signature":"vault:v0:AAAA"}}`,
		"disagreeing versions":   `{"data":{"signature":"vault:v1:AAAA","key_version":2}}`,
		"standard base64 as JWS": `{"data":{"signature":"vault:v1:+/+/"}}`,
		"not JSON":               `nope`,
	} {
		t.Run("refuses "+name, func(t *testing.T) {
			transit, _ := signServer(t, http.StatusOK, response)

			_, err := transit.Sign(t.Context(), "k", TransitSignRequest{Input: []byte("m"), JWS: true})
			require.Error(t, err)
			require.NotContains(t, err.Error(), "+/+/")
		})
	}

	t.Run("classifies a refusal", func(t *testing.T) {
		transit, _ := signServer(t, http.StatusForbidden, `{"errors":["permission denied"]}`)

		_, err := transit.Sign(t.Context(), "k", TransitSignRequest{Input: []byte("m")})
		require.ErrorIs(t, err, secrets.ErrPermission)
	})

	t.Run("bounds the input it sends", func(t *testing.T) {
		transit, seen := signServer(t, http.StatusOK, `{}`)

		_, err := transit.Sign(t.Context(), "k", TransitSignRequest{})
		require.Error(t, err)
		_, err = transit.Sign(t.Context(), "k", TransitSignRequest{Input: make([]byte, maxTransitSignInput+1)})
		require.Error(t, err)
		_, err = transit.Sign(t.Context(), "../k", TransitSignRequest{Input: []byte("m")})
		require.Error(t, err)

		path, _ := seen()
		require.Empty(t, path, "nothing was sent")
	})
}

func Test_Transit_ReadSigningKey(t *testing.T) {
	t.Run("reads the public half of each version", func(t *testing.T) {
		transit, seen := signServer(t, http.StatusOK, `{"data":{"type":"ecdsa-p256","latest_version":2,
			"min_decryption_version":1,"keys":{
				"1":{"name":"P-256","public_key":"PEM-ONE"},
				"2":{"name":"P-256","public_key":"PEM-TWO"}}}}`)

		key, err := transit.ReadSigningKey(t.Context(), "k")
		require.NoError(t, err)
		require.Equal(t, "ecdsa-p256", key.Type)
		require.Equal(t, uint32(2), key.LatestVersion)
		require.Equal(t, TransitKeyVersion{Name: "P-256", PublicKey: "PEM-TWO"}, key.Versions[2])

		path, _ := seen()
		require.Equal(t, "transit/keys/k", path)
	})

	t.Run("takes the latest from the listing when the server omits it", func(t *testing.T) {
		transit, _ := signServer(t, http.StatusOK, `{"data":{"type":"ed25519","keys":{"1":{},"4":{}}}}`)

		key, err := transit.ReadSigningKey(t.Context(), "k")
		require.NoError(t, err)
		require.Equal(t, uint32(4), key.LatestVersion)
	})

	for name, response := range map[string]string{
		"no type":          `{"data":{"latest_version":1}}`,
		"no version":       `{"data":{"type":"ed25519"}}`,
		"a bad version":    `{"data":{"type":"ed25519","keys":{"one":{}}}}`,
		"a zero version":   `{"data":{"type":"ed25519","keys":{"0":{}}}}`,
		"not JSON":         `nope`,
		"too many version": tooManyVersions(),
	} {
		t.Run("refuses "+name, func(t *testing.T) {
			transit, _ := signServer(t, http.StatusOK, response)

			_, err := transit.ReadSigningKey(t.Context(), "k")
			require.Error(t, err)
		})
	}
}

func tooManyVersions() string {
	var b strings.Builder
	b.WriteString(`{"data":{"type":"ed25519","keys":{`)
	for i := 1; i <= MaxTransitKeyVersions+1; i++ {
		if i > 1 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `"%d":{}`, i)
	}
	b.WriteString(`}}}`)
	return b.String()
}

func Test_WithTokenFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte("file-token\n"), 0o600))

	_, err := NewProvider("https://vault.example.com", WithTokenFile(path), WithToken("x"))
	require.Error(t, err)
	_, err = NewProvider("https://vault.example.com", WithTokenFile(path), WithKubernetesAuth("r"))
	require.Error(t, err)

	require.NoError(t, os.WriteFile(path, []byte("\n"), 0o600))
	_, err = NewProvider("https://vault.example.com", WithTokenFile(path))
	require.ErrorIs(t, err, secrets.ErrUnavailable)
	require.NotContains(t, err.Error(), "file-token")

	_, err = NewProvider("https://vault.example.com", WithTokenFile(path+".missing"))
	require.Error(t, err)
}
