package vault

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	"github.com/stretchr/testify/require"
)

func Test_NewTransit(t *testing.T) {
	t.Run("defaults to the transit mount", func(t *testing.T) {
		transit, err := NewTransit("https://vault.example.com:8200", "", WithToken("t"))
		require.NoError(t, err)
		require.Equal(t, DefaultTransitMount, transit.Mount())
		require.Equal(t, "https://vault.example.com:8200", transit.Address())
	})

	t.Run("a nested mount", func(t *testing.T) {
		transit, err := NewTransit("https://vault.example.com:8200", "/platform/transit/", WithToken("t"))
		require.NoError(t, err)
		require.Equal(t, "platform/transit", transit.Mount())
	})

	for name, tc := range map[string]struct {
		addr, mount string
		opts        []Option
		wantErr     string
	}{
		"cleartext to a remote host": {addr: "http://vault.example.com", opts: []Option{WithToken("t")}, wantErr: "cleartext"},
		"no authentication":          {addr: "https://vault.example.com", wantErr: "no way to authenticate"},
		"a traversing mount":         {addr: "https://vault.example.com", mount: "transit/../sys", opts: []Option{WithToken("t")}, wantErr: "outside"},
		"a KV mount option":          {addr: "https://vault.example.com", opts: []Option{WithToken("t"), WithMount("kv")}, wantErr: "configure KV reads"},
		"a KV prefix option":         {addr: "https://vault.example.com", opts: []Option{WithToken("t"), WithPathPrefix("p")}, wantErr: "configure KV reads"},
		"a scheme option":            {addr: "https://vault.example.com", opts: []Option{WithToken("t"), WithScheme("bao")}, wantErr: "configure KV reads"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewTransit(tc.addr, tc.mount, tc.opts...)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func Test_CiphertextVersion(t *testing.T) {
	valid := map[string]uint32{
		"vault:v1:AAAA":          1,
		"vault:v42:AAAA":         42,
		"vault:v4294967295:AAAA": 4294967295,
	}
	for text, want := range valid {
		got, err := CiphertextVersion(text)
		require.NoError(t, err, text)
		require.Equal(t, want, got, text)
	}

	for _, text := range []string{
		"",
		"vault:",
		"vault:v",
		"vault:v1",
		"vault:v1:",
		"vault:v0:AAAA",
		"vault:v01:AAAA",
		"vault:v-1:AAAA",
		"vault:v+1:AAAA",
		"vault:v4294967296:AAAA",
		"vault:vx:AAAA",
		"vault:v1:AAA",
		"vault:v1:AA AA",
		"vault:v1:AAAA:BBBB",
		"bao:v1:AAAA",
		"VAULT:v1:AAAA",
	} {
		_, err := CiphertextVersion(text)
		require.Error(t, err, "%q", text)
		if len(text) > len("vault:v1:") {
			require.NotContains(t, err.Error(), text[len("vault:v1:"):], "a refusal must not quote the ciphertext")
		}
	}
}

// transitRecord is one request the stand-in served.
type transitRecord struct {
	method, path, token, namespace string
	body                           map[string]string
}

func transitServer(t *testing.T, status int, response string) (*Transit, func() []transitRecord) {
	t.Helper()

	var (
		mu      sync.Mutex
		records []transitRecord
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		body := map[string]string{}
		if len(raw) > 0 {
			require.NoError(t, json.Unmarshal(raw, &body))
		}

		mu.Lock()
		records = append(records, transitRecord{
			method:    r.Method,
			path:      strings.TrimPrefix(r.URL.Path, "/v1/"),
			token:     r.Header.Get("X-Vault-Token"),
			namespace: r.Header.Get("X-Vault-Namespace"),
			body:      body,
		})
		mu.Unlock()

		jsonHandler(status, response)(w, r)
	}))
	t.Cleanup(server.Close)

	transit, err := NewTransit(server.URL, "transit", WithToken("static-token"), WithVaultNamespace("ops"))
	require.NoError(t, err)

	return transit, func() []transitRecord {
		mu.Lock()
		defer mu.Unlock()
		return append([]transitRecord(nil), records...)
	}
}

func Test_Transit_requestShape(t *testing.T) {
	t.Run("encrypt", func(t *testing.T) {
		transit, served := transitServer(t, http.StatusOK, `{"data":{"ciphertext":"vault:v3:AAAA","key_version":3}}`)

		ciphertext, version, err := transit.Encrypt(t.Context(), "payloads", []byte("plain"), []byte("aad"))
		require.NoError(t, err)
		require.Equal(t, "vault:v3:AAAA", ciphertext)
		require.Equal(t, uint32(3), version)

		records := served()
		require.Len(t, records, 1)
		require.Equal(t, transitRecord{
			method: http.MethodPost, path: "transit/encrypt/payloads", token: "static-token", namespace: "ops",
			body: map[string]string{
				"plaintext":       base64.StdEncoding.EncodeToString([]byte("plain")),
				"associated_data": base64.StdEncoding.EncodeToString([]byte("aad")),
			},
		}, records[0])
	})

	t.Run("encrypt without associated data omits it", func(t *testing.T) {
		transit, served := transitServer(t, http.StatusOK, `{"data":{"ciphertext":"vault:v1:AAAA"}}`)

		_, _, err := transit.Encrypt(t.Context(), "payloads", []byte("plain"), nil)
		require.NoError(t, err)
		require.NotContains(t, served()[0].body, "associated_data")
	})

	t.Run("decrypt", func(t *testing.T) {
		plaintext := base64.StdEncoding.EncodeToString([]byte("plain"))
		transit, served := transitServer(t, http.StatusOK, `{"data":{"plaintext":"`+plaintext+`"}}`)

		got, err := transit.Decrypt(t.Context(), "payloads", "vault:v1:AAAA", []byte("aad"))
		require.NoError(t, err)
		require.Equal(t, []byte("plain"), got)

		require.Equal(t, transitRecord{
			method: http.MethodPost, path: "transit/decrypt/payloads", token: "static-token", namespace: "ops",
			body: map[string]string{
				"ciphertext":      "vault:v1:AAAA",
				"associated_data": base64.StdEncoding.EncodeToString([]byte("aad")),
			},
		}, served()[0])
	})

	t.Run("read key", func(t *testing.T) {
		transit, served := transitServer(t, http.StatusOK, `{"data":{
			"type":"aes256-gcm96","derived":false,"keys":{"1":1,"2":2},"latest_version":2,
			"min_decryption_version":1,"supports_encryption":true,"supports_decryption":true}}`)

		key, err := transit.ReadKey(t.Context(), "payloads")
		require.NoError(t, err)
		require.Equal(t, TransitKey{
			Type: "aes256-gcm96", LatestVersion: 2, MinDecryptionVersion: 1,
			SupportsEncryption: true, SupportsDecryption: true,
		}, key)
		require.Equal(t, http.MethodGet, served()[0].method)
		require.Equal(t, "transit/keys/payloads", served()[0].path)
	})
}

func Test_Transit_responses(t *testing.T) {
	t.Run("latest version from keys when the server omits it", func(t *testing.T) {
		transit, _ := transitServer(t, http.StatusOK, `{"data":{"type":"chacha20-poly1305","derived":true,
			"keys":{"1":1,"7":7,"3":3},"min_decryption_version":3}}`)

		key, err := transit.ReadKey(t.Context(), "payloads")
		require.NoError(t, err)
		require.Equal(t, uint32(7), key.LatestVersion)
		require.True(t, key.Derived)
	})

	for name, body := range map[string]string{
		"no type":            `{"data":{"latest_version":1}}`,
		"no version":         `{"data":{"type":"aes256-gcm96"}}`,
		"a non-numeric key":  `{"data":{"type":"aes256-gcm96","keys":{"one":1}}}`,
		"min above latest":   `{"data":{"type":"aes256-gcm96","latest_version":1,"min_decryption_version":2}}`,
		"a negative version": `{"data":{"type":"aes256-gcm96","latest_version":-1}}`,
		"not JSON":           `nope`,
	} {
		t.Run("read key refuses "+name, func(t *testing.T) {
			transit, _ := transitServer(t, http.StatusOK, body)

			_, err := transit.ReadKey(t.Context(), "payloads")
			require.Error(t, err)
		})
	}

	t.Run("encrypt refuses a malformed ciphertext", func(t *testing.T) {
		transit, _ := transitServer(t, http.StatusOK, `{"data":{"ciphertext":"vault:v1:not base64!"}}`)

		_, _, err := transit.Encrypt(t.Context(), "payloads", []byte("plain"), nil)
		require.ErrorContains(t, err, "malformed ciphertext")
		require.NotContains(t, err.Error(), "not base64!")
	})

	t.Run("decrypt refuses a plaintext that is not base64, without quoting it", func(t *testing.T) {
		transit, _ := transitServer(t, http.StatusOK, `{"data":{"plaintext":"s3cr3t!!"}}`)

		_, err := transit.Decrypt(t.Context(), "payloads", "vault:v1:AAAA", nil)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "s3cr3t")
	})

	for name, tc := range map[string]struct {
		status  int
		body    string
		decrypt bool
		want    error
	}{
		"decrypt 400":             {status: 400, body: `{"errors":["cipher: message authentication failed"]}`, decrypt: true, want: ErrInvalidCiphertext},
		"decrypt 400 missing key": {status: 400, body: `{"errors":["encryption key not found"]}`, decrypt: true, want: secrets.ErrNotFound},
		"encrypt 400 missing key": {status: 400, body: `{"errors":["encryption key not found"]}`, want: secrets.ErrNotFound},
		"encrypt 403":             {status: 403, body: `{"errors":["permission denied"]}`, want: secrets.ErrPermission},
		"encrypt 404":             {status: 404, body: `{"errors":[]}`, want: secrets.ErrNotFound},
		"decrypt 503":             {status: 503, body: `{"errors":["Vault is sealed"]}`, decrypt: true, want: secrets.ErrUnavailable},
		"encrypt 429":             {status: 429, body: `{"errors":[]}`, want: secrets.ErrUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			transit, _ := transitServer(t, tc.status, tc.body)

			var err error
			if tc.decrypt {
				_, err = transit.Decrypt(t.Context(), "payloads", "vault:v1:AAAA", []byte("aad"))
			} else {
				_, _, err = transit.Encrypt(t.Context(), "payloads", []byte("plain"), []byte("aad"))
			}
			require.ErrorIs(t, err, tc.want)
			require.NotContains(t, err.Error(), "static-token")
			require.NotContains(t, err.Error(), "errors")
		})
	}

	t.Run("an unclassified 400 on encrypt is not a ciphertext refusal", func(t *testing.T) {
		transit, _ := transitServer(t, http.StatusBadRequest, `{"errors":["something else"]}`)

		_, _, err := transit.Encrypt(t.Context(), "payloads", []byte("plain"), nil)
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrInvalidCiphertext)
		require.False(t, secrets.Retryable(err))
	})
}

func Test_Transit_refusesBeforeRequesting(t *testing.T) {
	transit, served := transitServer(t, http.StatusOK, `{}`)

	for _, key := range []string{"", ".", "..", "a/b", "a%2Fb", "a b", "a\nb", "ключ", strings.Repeat("k", 129)} {
		_, err := transit.ReadKey(t.Context(), key)
		require.Error(t, err, "%q", key)
		_, _, err = transit.Encrypt(t.Context(), key, []byte("plain"), nil)
		require.Error(t, err, "%q", key)
	}

	_, err := transit.Decrypt(t.Context(), "payloads", "not-a-ciphertext", nil)
	require.ErrorIs(t, err, ErrInvalidCiphertext)

	require.Empty(t, served())

	// The longest permitted name, with every permitted character class, is sent.
	longest := "Az09._-" + strings.Repeat("k", 121)
	_, _ = transit.ReadKey(t.Context(), longest)
	require.Len(t, served(), 1)
	require.Equal(t, "transit/keys/"+longest, served()[0].path)
}
