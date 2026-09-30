package vault

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider/keyprovidertest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec/keyprovider/vault/vaulttest"
	secretsvault "github.com/picatz/flowstate/pkg/flowstate/v1/secrets/vault"
)

// TestConformance holds a Transit key to the suite every provider passes.
func TestConformance(t *testing.T) {
	for _, typ := range []string{"aes256-gcm96", "chacha20-poly1305"} {
		t.Run(typ, func(t *testing.T) {
			f := vaulttest.NewServer(t)
			f.Create("flowstate", typ)
			keyprovidertest.Run(t, newTestKey(t, f, "flowstate"))
		})
	}
}

var testContext = keyprovider.Context{Namespace: "team-a", KeyID: "primary", Suite: 1}

func newTestKey(t *testing.T, f *vaulttest.Server, name string, opts ...secretsvault.Option) *Key {
	t.Helper()

	transit, err := secretsvault.NewTransit(
		f.URL(), "",
		append([]secretsvault.Option{secretsvault.WithToken(vaulttest.Token)}, opts...)...,
	)
	require.NoError(t, err)

	return New(transit, name)
}

func dataKey() []byte { return bytes.Repeat([]byte{0x5a}, keyprovider.DataKeyBytes) }

func Test_Key_roundTrip(t *testing.T) {
	for _, typ := range []string{"aes256-gcm96", "chacha20-poly1305"} {
		t.Run(typ, func(t *testing.T) {
			f := vaulttest.NewServer(t)
			f.Create("payloads", typ)
			key := newTestKey(t, f, "payloads")

			info, err := key.Describe(t.Context())
			require.NoError(t, err)
			require.Equal(t, keyprovider.KeyInfo{
				Kind: "vault", MaxWrappedBytes: 512, CanWrap: true, CanUnwrap: true, Authenticates: true, Version: 1,
			}, info)

			wrapped, err := key.Wrap(t.Context(), dataKey(), testContext)
			require.NoError(t, err)
			require.Equal(t, uint32(1), wrapped.Version)
			require.True(t, bytes.HasPrefix(wrapped.Bytes, []byte("vault:v1:")))
			require.LessOrEqual(t, len(wrapped.Bytes), info.MaxWrappedBytes)

			got, err := key.Unwrap(t.Context(), wrapped, testContext)
			require.NoError(t, err)
			require.Equal(t, dataKey(), got)
		})
	}
}

func Test_Key_versions(t *testing.T) {
	f := vaulttest.NewServer(t)
	f.Create("payloads", "aes256-gcm96")
	key := newTestKey(t, f, "payloads")

	old, err := key.Wrap(t.Context(), dataKey(), testContext)
	require.NoError(t, err)
	require.Equal(t, uint32(1), old.Version)

	f.Rotate("payloads")

	info, err := key.Describe(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint32(2), info.Version)

	current, err := key.Wrap(t.Context(), dataKey(), testContext)
	require.NoError(t, err)
	require.Equal(t, uint32(2), current.Version)
	require.True(t, bytes.HasPrefix(current.Bytes, []byte("vault:v2:")))

	t.Run("an older version still unwraps", func(t *testing.T) {
		got, err := key.Unwrap(t.Context(), old, testContext)
		require.NoError(t, err)
		require.Equal(t, dataKey(), got)
	})

	t.Run("a recorded version that disagrees with the wrap is refused", func(t *testing.T) {
		_, err := key.Unwrap(t.Context(), keyprovider.Wrapped{Bytes: old.Bytes, Version: 2}, testContext)
		require.ErrorIs(t, err, keyprovider.ErrInvalidWrapped)
	})

	t.Run("a version below the minimum decryption version is refused", func(t *testing.T) {
		f.SetMinDecrypt("payloads", 2)

		_, err := key.Unwrap(t.Context(), old, testContext)
		require.ErrorIs(t, err, keyprovider.ErrInvalidWrapped)

		got, err := key.Unwrap(t.Context(), current, testContext)
		require.NoError(t, err)
		require.Equal(t, dataKey(), got)
	})
}

func Test_Key_Unwrap_refusals(t *testing.T) {
	f := vaulttest.NewServer(t)
	f.Create("payloads", "aes256-gcm96")
	key := newTestKey(t, f, "payloads")

	wrapped, err := key.Wrap(t.Context(), dataKey(), testContext)
	require.NoError(t, err)

	t.Run("another context", func(t *testing.T) {
		for _, other := range []keyprovider.Context{
			{Namespace: "team-b", KeyID: "primary", Suite: 1},
			{Namespace: "team-a", KeyID: "secondary", Suite: 1},
			{Namespace: "team-a", KeyID: "primary", Suite: 2},
		} {
			_, err := key.Unwrap(t.Context(), wrapped, other)
			require.ErrorIs(t, err, keyprovider.ErrInvalidWrapped, "%+v", other)
		}
	})

	t.Run("tampered ciphertext", func(t *testing.T) {
		text := string(wrapped.Bytes)
		prefix, encoded := text[:len("vault:v1:")], text[len("vault:v1:"):]
		raw, err := base64.StdEncoding.DecodeString(encoded)
		require.NoError(t, err)
		raw[len(raw)-1] ^= 1

		tampered := keyprovider.Wrapped{
			Bytes:   []byte(prefix + base64.StdEncoding.EncodeToString(raw)),
			Version: 1,
		}
		_, err = key.Unwrap(t.Context(), tampered, testContext)
		require.ErrorIs(t, err, keyprovider.ErrInvalidWrapped)
	})

	t.Run("malformed wraps are refused before any request", func(t *testing.T) {
		f.ResetBodies()

		for _, bad := range []string{
			"",
			"not-vault",
			"vault:v:AAAA",
			"vault:v0:AAAA",
			"vault:v01:AAAA",
			"vault:v1:",
			"vault:v1:not base64!",
			"vault:v99999999999:AAAA",
			"vault:v1:" + strings.Repeat("A", 600),
		} {
			_, err := key.Unwrap(t.Context(), keyprovider.Wrapped{Bytes: []byte(bad)}, testContext)
			require.ErrorIs(t, err, keyprovider.ErrInvalidWrapped, "%q", bad)
		}

		require.Empty(t, f.Bodies())
	})

	t.Run("a plaintext that is not a data key", func(t *testing.T) {
		ciphertext, version, err := key.transit.Encrypt(t.Context(), "payloads", []byte("short"), testContext.Bytes())
		require.NoError(t, err)

		_, err = key.Unwrap(t.Context(), keyprovider.Wrapped{Bytes: []byte(ciphertext), Version: version}, testContext)
		require.ErrorIs(t, err, keyprovider.ErrInvalidWrapped)
	})

	t.Run("a data key of the wrong length is not wrapped", func(t *testing.T) {
		_, err := key.Wrap(t.Context(), make([]byte, 16), testContext)
		require.Error(t, err)
	})
}

func Test_Key_Describe_refusals(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(f *vaulttest.Server)
		typ     string
		wantErr string
	}{
		{name: "not an AEAD key", typ: "rsa-2048", wantErr: `"rsa-2048" key`},
		{name: "a weaker AEAD key", typ: "aes128-gcm96", wantErr: `"aes128-gcm96" key`},
		{name: "a derived key", typ: "aes256-gcm96", setup: func(f *vaulttest.Server) { f.SetDerived("payloads", true) }, wantErr: "derived"},
		{name: "a key that cannot decrypt", typ: "aes256-gcm96", setup: func(f *vaulttest.Server) { f.SetNoDecrypt("payloads", true) }, wantErr: "both encryption and decryption"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := vaulttest.NewServer(t)
			f.Create("payloads", tt.typ)
			if tt.setup != nil {
				tt.setup(f)
			}

			_, err := newTestKey(t, f, "payloads").Describe(t.Context())
			require.ErrorIs(t, err, keyprovider.ErrDenied)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func Test_Key_errorClassification(t *testing.T) {
	type op func(t *testing.T, key *Key, wrapped keyprovider.Wrapped) error

	ops := map[string]op{
		"Describe": func(t *testing.T, key *Key, _ keyprovider.Wrapped) error {
			_, err := key.Describe(t.Context())
			return err
		},
		"Wrap": func(t *testing.T, key *Key, _ keyprovider.Wrapped) error {
			_, err := key.Wrap(t.Context(), dataKey(), testContext)
			return err
		},
		"Unwrap": func(t *testing.T, key *Key, wrapped keyprovider.Wrapped) error {
			_, err := key.Unwrap(t.Context(), wrapped, testContext)
			return err
		},
	}

	tests := []struct {
		name   string
		status int
		want   error
	}{
		{name: "forbidden", status: http.StatusForbidden, want: keyprovider.ErrDenied},
		{name: "sealed", status: http.StatusServiceUnavailable, want: keyprovider.ErrUnavailable},
		{name: "rate limited", status: http.StatusTooManyRequests, want: keyprovider.ErrUnavailable},
		{name: "internal error", status: http.StatusInternalServerError, want: keyprovider.ErrUnavailable},
		{name: "an unexpected status fails closed", status: http.StatusTeapot, want: keyprovider.ErrDenied},
	}

	for _, tt := range tests {
		for name, run := range ops {
			t.Run(tt.name+"/"+name, func(t *testing.T) {
				f := vaulttest.NewServer(t)
				f.Create("payloads", "aes256-gcm96")
				key := newTestKey(t, f, "payloads")

				wrapped, err := key.Wrap(t.Context(), dataKey(), testContext)
				require.NoError(t, err)

				f.SetStatus(tt.status)

				err = run(t, key, wrapped)
				require.ErrorIs(t, err, tt.want)
				require.NotContains(t, err.Error(), vaulttest.ErrorMarker)
			})
		}
	}

	t.Run("an unknown key", func(t *testing.T) {
		f := vaulttest.NewServer(t)
		f.Create("payloads", "aes256-gcm96")
		wrapped, err := newTestKey(t, f, "payloads").Wrap(t.Context(), dataKey(), testContext)
		require.NoError(t, err)

		missing := newTestKey(t, f, "missing")

		for name, run := range ops {
			err := run(t, missing, wrapped)
			require.ErrorIs(t, err, keyprovider.ErrUnknownKey, name)
			require.NotContains(t, err.Error(), vaulttest.ErrorMarker)
		}
	})

	t.Run("an unknown mount", func(t *testing.T) {
		f := vaulttest.NewServer(t)
		transit, err := secretsvault.NewTransit(f.URL(), "elsewhere", secretsvault.WithToken(vaulttest.Token))
		require.NoError(t, err)

		_, err = New(transit, "payloads").Describe(t.Context())
		require.ErrorIs(t, err, keyprovider.ErrUnknownKey)
	})

	t.Run("an invalid key name makes no request", func(t *testing.T) {
		f := vaulttest.NewServer(t)
		for _, name := range []string{"", ".", "..", "a/b", "a b", "%2e%2e", strings.Repeat("k", 129)} {
			_, err := newTestKey(t, f, name).Describe(t.Context())
			require.Error(t, err, "%q", name)
		}

		require.Empty(t, f.Bodies())
	})

	t.Run("the provider's timeout is unavailable", func(t *testing.T) {
		f := vaulttest.NewServer(t)
		f.Create("payloads", "aes256-gcm96")
		f.SetHang(true)
		key := newTestKey(t, f, "payloads", secretsvault.WithTimeout(50*time.Millisecond))

		_, err := key.Wrap(context.Background(), dataKey(), testContext)
		require.ErrorIs(t, err, keyprovider.ErrUnavailable)
	})

	t.Run("the caller's deadline is unavailable too", func(t *testing.T) {
		f := vaulttest.NewServer(t)
		f.Create("payloads", "aes256-gcm96")
		f.SetHang(true)
		key := newTestKey(t, f, "payloads", secretsvault.WithTimeout(time.Minute))

		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()

		_, err := key.Describe(ctx)
		require.ErrorIs(t, err, keyprovider.ErrUnavailable)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("an oversized wrap is refused", func(t *testing.T) {
		f := vaulttest.NewServer(t)
		f.Create("payloads", "aes256-gcm96")
		f.SetOversize(true)

		_, err := newTestKey(t, f, "payloads").Wrap(t.Context(), dataKey(), testContext)
		require.ErrorIs(t, err, keyprovider.ErrDenied)
	})
}

func Test_Key_neverDisclosesSecrets(t *testing.T) {
	f := vaulttest.NewServer(t)
	f.Create("payloads", "aes256-gcm96")
	key := newTestKey(t, f, "payloads")

	wrapped, err := key.Wrap(t.Context(), dataKey(), testContext)
	require.NoError(t, err)

	secretsToHide := []string{
		vaulttest.Token,
		vaulttest.ErrorMarker,
		string(wrapped.Bytes),
		string(wrapped.Bytes[len("vault:v1:"):]),
		base64.StdEncoding.EncodeToString(dataKey()),
		base64.StdEncoding.EncodeToString(testContext.Bytes()),
	}

	var errs []error
	collect := func(err error) {
		require.Error(t, err)
		errs = append(errs, err)
	}

	_, err = key.Unwrap(t.Context(), wrapped, keyprovider.Context{Namespace: "team-b"})
	collect(err)

	_, err = key.Unwrap(t.Context(), keyprovider.Wrapped{Bytes: append([]byte(nil), wrapped.Bytes[:len(wrapped.Bytes)-2]...)}, testContext)
	collect(err)

	for _, status := range []int{http.StatusForbidden, http.StatusBadRequest, http.StatusServiceUnavailable, http.StatusTeapot} {
		f.SetStatus(status)

		_, err = key.Describe(t.Context())
		collect(err)
		_, err = key.Wrap(t.Context(), dataKey(), testContext)
		collect(err)
		_, err = key.Unwrap(t.Context(), wrapped, testContext)
		collect(err)
	}

	for _, err := range errs {
		for _, secret := range secretsToHide {
			require.False(t, strings.Contains(err.Error(), secret), "error %q discloses %q", err, secret)
		}
	}

	t.Run("the data key and the context crossed the wire only base64-encoded", func(t *testing.T) {
		for _, body := range f.Bodies() {
			require.NotContains(t, body, string(dataKey()))
		}
	})
}

func Test_Key_formatting(t *testing.T) {
	f := vaulttest.NewServer(t)
	key := newTestKey(t, f, "payloads")

	const want = "vault.Key(payloads)"
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x"} {
		require.Equal(t, want, fmt.Sprintf(verb, key), verb)
	}

	var logged bytes.Buffer
	slog.New(slog.NewTextHandler(&logged, nil)).Info("key", "key", key)
	require.Contains(t, logged.String(), want)
	require.NotContains(t, logged.String(), vaulttest.Token)
}

func Test_Key_kubernetesAuth(t *testing.T) {
	f := vaulttest.NewServer(t)
	f.Create("payloads", "aes256-gcm96")

	jwtPath := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(jwtPath, []byte("projected-jwt"), 0o600))

	transit, err := secretsvault.NewTransit(f.URL(), "transit",
		secretsvault.WithKubernetesAuth(vaulttest.Role),
		secretsvault.WithKubernetesJWTPath(jwtPath),
	)
	require.NoError(t, err)
	key := New(transit, "payloads")

	_, err = key.Describe(t.Context())
	require.NoError(t, err)

	wrapped, err := key.Wrap(t.Context(), dataKey(), testContext)
	require.NoError(t, err)

	t.Run("a revoked token is replaced by one login", func(t *testing.T) {
		f.RevokeTokens()

		got, err := key.Unwrap(t.Context(), wrapped, testContext)
		require.NoError(t, err)
		require.Equal(t, dataKey(), got)

		require.Equal(t, 2, f.Logins())
	})
}

// TestAServerThatIgnoresTheContextIsRefused: a Vault that drops
// associated_data, as one before 1.13 does, would make a wrap unwrap under any
// context without saying so. Describe asks, and refuses it at startup.
func TestAServerThatIgnoresTheContextIsRefused(t *testing.T) {
	f := vaulttest.NewServer(t)
	f.Create("flowstate", "aes256-gcm96")
	key := newTestKey(t, f, "flowstate")

	_, err := key.Describe(t.Context())
	require.NoError(t, err)

	f.SetIgnoreAssociatedData(true)
	_, err = key.Describe(t.Context())
	require.ErrorIs(t, err, keyprovider.ErrDenied)
	require.ErrorContains(t, err, "does not bind the encryption context")
}
