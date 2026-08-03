package vault

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
	"github.com/stretchr/testify/require"
)

func Test_NewProvider(t *testing.T) {
	jwt := func(t *testing.T) string {
		t.Helper()

		path := filepath.Join(t.TempDir(), "token")
		require.NoError(t, os.WriteFile(path, []byte("projected-jwt"), 0o600))

		return path
	}

	tests := []struct {
		name string
		addr string
		opts func(t *testing.T) []Option
		// wantErr is a substring of the construction failure, empty when the
		// configuration should be accepted.
		wantErr string
		check   func(t *testing.T, provider *Provider)
	}{
		// Negative cases first: a provider that cannot work must say so at
		// startup, not on the first workflow that needs a secret.
		{
			name:    "no address",
			addr:    "",
			opts:    func(*testing.T) []Option { return []Option{WithToken("t")} },
			wantErr: "an address is required",
		},
		{
			name:    "unparseable address",
			addr:    "://vault",
			opts:    func(*testing.T) []Option { return []Option{WithToken("t")} },
			wantErr: "could not be parsed",
		},
		{
			name:    "address with no host",
			addr:    "vault.example.com:8200",
			opts:    func(*testing.T) []Option { return []Option{WithToken("t")} },
			wantErr: "has no host",
		},
		{
			name:    "cleartext http to a remote host",
			addr:    "http://vault.example.com:8200",
			opts:    func(*testing.T) []Option { return []Option{WithToken("t")} },
			wantErr: "cleartext",
		},
		{
			name:    "an unsupported scheme",
			addr:    "ftp://vault.example.com:21",
			opts:    func(*testing.T) []Option { return []Option{WithToken("t")} },
			wantErr: "must use https",
		},
		{
			name:    "a unix socket, which Vault's API does not serve secrets on",
			addr:    "unix:///var/run/vault.sock",
			opts:    func(*testing.T) []Option { return []Option{WithToken("t")} },
			wantErr: "has no host",
		},
		{
			name:    "credentials in the address",
			addr:    "https://user:pass@vault.example.com:8200",
			opts:    func(*testing.T) []Option { return []Option{WithToken("t")} },
			wantErr: "embeds credentials",
		},
		{
			name:    "a query in the address",
			addr:    "https://vault.example.com:8200/?ns=team",
			opts:    func(*testing.T) []Option { return []Option{WithToken("t")} },
			wantErr: "must be a base URL",
		},
		{
			name:    "no authentication method",
			addr:    "https://vault.example.com:8200",
			opts:    func(*testing.T) []Option { return nil },
			wantErr: "no way to authenticate",
		},
		{
			name: "two authentication methods",
			addr: "https://vault.example.com:8200",
			opts: func(t *testing.T) []Option {
				return []Option{WithToken("t"), WithKubernetesAuth("role"), WithKubernetesJWTPath(jwt(t))}
			},
			wantErr: "one authentication method",
		},
		{
			name:    "an empty token",
			addr:    "https://vault.example.com:8200",
			opts:    func(*testing.T) []Option { return []Option{WithToken("  ")} },
			wantErr: "empty token",
		},
		{
			name:    "an empty role",
			addr:    "https://vault.example.com:8200",
			opts:    func(*testing.T) []Option { return []Option{WithKubernetesAuth("")} },
			wantErr: "needs the name of a Vault role",
		},
		{
			name: "a missing service account token",
			addr: "https://vault.example.com:8200",
			opts: func(t *testing.T) []Option {
				return []Option{
					WithKubernetesAuth("role"),
					WithKubernetesJWTPath(filepath.Join(t.TempDir(), "absent")),
				}
			},
			wantErr: "reading the Kubernetes service account token",
		},
		{
			name: "an empty service account token",
			addr: "https://vault.example.com:8200",
			opts: func(t *testing.T) []Option {
				path := filepath.Join(t.TempDir(), "token")
				require.NoError(t, os.WriteFile(path, []byte("\n"), 0o600))

				return []Option{WithKubernetesAuth("role"), WithKubernetesJWTPath(path)}
			},
			wantErr: "is empty",
		},
		{
			name:    "an empty service account token path",
			addr:    "https://vault.example.com:8200",
			opts:    func(*testing.T) []Option { return []Option{WithKubernetesAuth("r"), WithKubernetesJWTPath("")} },
			wantErr: "WithKubernetesJWTPath was given an empty path",
		},
		{
			name:    "an empty auth mount",
			addr:    "https://vault.example.com:8200",
			opts:    func(*testing.T) []Option { return []Option{WithToken("t"), WithKubernetesAuthMount("")} },
			wantErr: "WithKubernetesAuthMount was given an empty path",
		},
		{
			name:    "an empty vault namespace",
			addr:    "https://vault.example.com:8200",
			opts:    func(*testing.T) []Option { return []Option{WithToken("t"), WithVaultNamespace("")} },
			wantErr: "WithVaultNamespace was given an empty path",
		},
		{
			name:    "an empty scheme",
			addr:    "https://vault.example.com:8200",
			opts:    func(*testing.T) []Option { return []Option{WithToken("t"), WithScheme("")} },
			wantErr: "was given an empty scheme",
		},
		{
			name: "a scheme longer than a reference may carry",
			addr: "https://vault.example.com:8200",
			opts: func(*testing.T) []Option {
				return []Option{WithToken("t"), WithScheme(strings.Repeat("v", secrets.MaxSchemeLen+1))}
			},
			wantErr: "is longer than",
		},
		{
			name:    "a mount that escapes",
			addr:    "https://vault.example.com:8200",
			opts:    func(*testing.T) []Option { return []Option{WithToken("t"), WithMount("../sys")} },
			wantErr: "points outside its namespace",
		},
		{
			name:    "an empty mount",
			addr:    "https://vault.example.com:8200",
			opts:    func(*testing.T) []Option { return []Option{WithToken("t"), WithMount("/")} },
			wantErr: "WithMount was given an empty path",
		},
		{
			name:    "a prefix with a query in it",
			addr:    "https://vault.example.com:8200",
			opts:    func(*testing.T) []Option { return []Option{WithToken("t"), WithPathPrefix("flow?state")} },
			wantErr: "WithPathPrefix",
		},
		{
			name:    "an uppercase scheme",
			addr:    "https://vault.example.com:8200",
			opts:    func(*testing.T) []Option { return []Option{WithToken("t"), WithScheme("Vault")} },
			wantErr: "lowercase letters",
		},
		{
			name:    "an unbounded response",
			addr:    "https://vault.example.com:8200",
			opts:    func(*testing.T) []Option { return []Option{WithToken("t"), WithMaxResponseBytes(0)} },
			wantErr: "positive limit",
		},
		{
			name:    "a negative renewal margin",
			addr:    "https://vault.example.com:8200",
			opts:    func(*testing.T) []Option { return []Option{WithToken("t"), WithRenewBefore(-time.Second)} },
			wantErr: "non-negative",
		},
		{
			name:    "no CA pool",
			addr:    "https://vault.example.com:8200",
			opts:    func(*testing.T) []Option { return []Option{WithToken("t"), WithRootCAs(nil)} },
			wantErr: "was given no pool",
		},
		{
			name: "a missing CA bundle",
			addr: "https://vault.example.com:8200",
			opts: func(t *testing.T) []Option {
				return []Option{WithToken("t"), WithRootCAsFile(filepath.Join(t.TempDir(), "absent.pem"))}
			},
			wantErr: "reading CA bundle",
		},
		{
			name: "a CA bundle holding no certificate",
			addr: "https://vault.example.com:8200",
			opts: func(t *testing.T) []Option {
				path := filepath.Join(t.TempDir(), "ca.pem")
				require.NoError(t, os.WriteFile(path, []byte("not a certificate"), 0o600))

				return []Option{WithToken("t"), WithRootCAsFile(path)}
			},
			wantErr: "holds no PEM certificate",
		},
		{
			name: "a custom client and custom roots",
			addr: "https://vault.example.com:8200",
			opts: func(*testing.T) []Option {
				return []Option{
					WithToken("t"),
					WithHTTPClient(&http.Client{}),
					WithRootCAs(x509.NewCertPool()),
				}
			},
			wantErr: "cannot be combined",
		},
		{
			name:    "no client",
			addr:    "https://vault.example.com:8200",
			opts:    func(*testing.T) []Option { return []Option{WithToken("t"), WithHTTPClient(nil)} },
			wantErr: "was given no client",
		},

		// Then the configurations that must be accepted.
		{
			name: "a static token and the defaults",
			addr: "https://vault.example.com:8200",
			opts: func(*testing.T) []Option { return []Option{WithToken("t")} },
			check: func(t *testing.T, provider *Provider) {
				require.Equal(t, DefaultScheme, provider.Scheme())
				require.Equal(t, "https://vault.example.com:8200", provider.Address())
				require.Equal(t, DefaultMount, provider.Mount())
			},
		},
		{
			name: "kubernetes auth",
			addr: "https://vault.example.com:8200",
			opts: func(t *testing.T) []Option {
				return []Option{WithKubernetesAuth("flowstate-worker"), WithKubernetesJWTPath(jwt(t))}
			},
			check: func(t *testing.T, provider *Provider) {
				require.Equal(t, "auth/kubernetes/login", provider.loginPath())
			},
		},
		{
			name: "http to a loopback address, for a vault agent sidecar",
			addr: "http://127.0.0.1:8200",
			opts: func(*testing.T) []Option { return []Option{WithToken("t")} },
			check: func(t *testing.T, provider *Provider) {
				require.Equal(t, "http://127.0.0.1:8200", provider.Address())
			},
		},
		{
			name: "http to localhost",
			addr: "http://localhost:8200",
			opts: func(*testing.T) []Option { return []Option{WithToken("t")} },
		},
		{
			name: "a nested mount, a prefix, and a second scheme",
			addr: "https://vault.example.com:8200",
			opts: func(*testing.T) []Option {
				return []Option{
					WithToken("t"),
					WithMount("/kv/platform/"),
					WithPathPrefix("flowstate"),
					WithScheme("vault-eu"),
					WithKubernetesAuthMount("kubernetes/prod"),
				}
			},
			check: func(t *testing.T, provider *Provider) {
				require.Equal(t, "vault-eu", provider.Scheme())
				require.Equal(t, "kv/platform", provider.Mount())
				require.Equal(t, "auth/kubernetes/prod/login", provider.loginPath())

				path, err := provider.SecretPath("team-a", "apps/api#token")
				require.NoError(t, err)
				require.Equal(t, "kv/platform/data/flowstate/team-a/apps/api", path)
			},
		},
		{
			name: "a nil option is ignored, so a conditional option needs no branch",
			addr: "https://vault.example.com:8200",
			opts: func(*testing.T) []Option { return []Option{WithToken("t"), nil} },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var opts []Option
			if test.opts != nil {
				opts = test.opts(t)
			}

			provider, err := NewProvider(test.addr, opts...)

			if test.wantErr != "" {
				require.Nil(t, provider)
				require.ErrorContains(t, err, test.wantErr)
				require.Contains(t, err.Error(), "secrets/vault:", "a construction error should name the package")

				return
			}

			require.NoError(t, err)
			require.NotNil(t, provider)

			if test.check != nil {
				test.check(t, provider)
			}
		})
	}
}

// Test_Provider_Resolve_referenceValidation covers the names that are refused
// before anything is asked of Vault, which is where a traversal has to be stopped:
// a reference is workflow-authored, and the namespace segment it sits under is the
// tenant boundary.
func Test_Provider_Resolve_referenceValidation(t *testing.T) {
	tests := []struct {
		name    string
		ref     string
		wantErr string
	}{
		{name: "empty", ref: "", wantErr: "must not be empty"},
		{name: "absolute", ref: "/apps/api", wantErr: "must be relative to the mount"},
		{name: "parent traversal", ref: "../team-b/apps/api", wantErr: "points outside its namespace"},
		{name: "deep traversal", ref: "apps/../../../team-b/api", wantErr: "points outside its namespace"},
		{name: "bare parent", ref: "..", wantErr: "points outside its namespace"},
		{name: "current directory", ref: ".", wantErr: "points outside its namespace"},
		{name: "trailing slash", ref: "apps/", wantErr: "must name a secret"},
		{name: "empty segment", ref: "apps//api", wantErr: "empty path segment"},
		{name: "backslash", ref: `apps\api`, wantErr: "forward slashes"},
		{name: "encoded traversal", ref: "apps%2f..%2fapi", wantErr: "may only contain"},
		{name: "a query", ref: "apps/api?version=1", wantErr: "may only contain"},
		{name: "a space", ref: "apps/the api", wantErr: "may only contain"},
		{name: "a newline", ref: "apps/api\nlogged", wantErr: "may only contain"},
		{name: "a field separator with no field", ref: "apps/api#", wantErr: "names no field"},
		{name: "two field separators", ref: "apps/api#a#b", wantErr: "may hold one"},
		{name: "a control character in the field", ref: "apps/api#tok\ren", wantErr: "control character"},
		{name: "an empty path with a field", ref: "#token", wantErr: "must be a path within the mount"},
		{
			name:    "a field name longer than an error should carry",
			ref:     "apps/api#" + strings.Repeat("f", maxFieldLen+1),
			wantErr: "longer than",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var served atomic.Int64

			provider, _ := newTestProvider(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				served.Add(1)
				fmt.Fprint(w, kvBody(1, map[string]any{"token": "should-never-be-read"}))
			}))

			ref := secrets.NewRef(provider.Scheme(), test.ref)

			secret, err := provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: ref})
			require.True(t, secret.IsZero())
			require.ErrorIs(t, err, secrets.ErrInvalidRef)
			require.ErrorContains(t, err, test.wantErr)
			require.False(t, secrets.Retryable(err), "a malformed reference is permanent")

			require.Zero(t, served.Load(), "a malformed reference must be refused before Vault is asked")
		})
	}
}

// Test_Provider_Resolve_backendFailures covers what Vault can answer with, and the
// classification each answer must produce: the engine decides whether to retry a
// step from exactly this.
func Test_Provider_Resolve_backendFailures(t *testing.T) {
	const value = "s3cr3t-value-do-not-leak"

	tests := []struct {
		name      string
		ref       string
		opts      []Option
		handler   http.HandlerFunc
		wantIs    error
		wantErr   string
		retryable bool
	}{
		{
			name:    "a missing secret",
			ref:     "apps/api#token",
			handler: jsonHandler(http.StatusNotFound, `{"errors":[]}`),
			wantIs:  secrets.ErrNotFound,
			wantErr: "holds no secret at",
		},
		{
			name:    "a missing mount",
			ref:     "apps/api#token",
			handler: jsonHandler(http.StatusNotFound, `{"errors":["no handler for route \"secret/data/x\""]}`),
			wantIs:  secrets.ErrNotFound,
			wantErr: "no KV v2 mount at",
		},
		{
			name:    "permission denied",
			ref:     "apps/api#token",
			handler: jsonHandler(http.StatusForbidden, `{"errors":["1 error occurred: permission denied"]}`),
			wantIs:  secrets.ErrPermission,
			wantErr: "refused to read",
		},
		{
			name:      "an internal error",
			ref:       "apps/api#token",
			handler:   jsonHandler(http.StatusInternalServerError, `{"errors":["internal error"]}`),
			wantIs:    secrets.ErrUnavailable,
			wantErr:   "answered 500",
			retryable: true,
		},
		{
			name:      "a sealed vault",
			ref:       "apps/api#token",
			handler:   jsonHandler(http.StatusServiceUnavailable, `{"errors":["Vault is sealed"]}`),
			wantIs:    secrets.ErrUnavailable,
			wantErr:   "sealed",
			retryable: true,
		},
		{
			name:      "an uninitialized vault",
			ref:       "apps/api#token",
			handler:   jsonHandler(http.StatusNotImplemented, `{"errors":["not initialized"]}`),
			wantIs:    secrets.ErrUnavailable,
			wantErr:   "not initialized",
			retryable: true,
		},
		{
			name:      "a rate limit quota",
			ref:       "apps/api#token",
			handler:   jsonHandler(http.StatusTooManyRequests, `{"errors":["rate limit quota exceeded"]}`),
			wantIs:    secrets.ErrUnavailable,
			wantErr:   "rate limit",
			retryable: true,
		},
		{
			name:      "a standby that is behind",
			ref:       "apps/api#token",
			handler:   jsonHandler(http.StatusPreconditionFailed, `{"errors":["stale read"]}`),
			wantIs:    secrets.ErrUnavailable,
			wantErr:   "standby",
			retryable: true,
		},
		{
			name:    "an unrecognized status",
			ref:     "apps/api#token",
			handler: jsonHandler(http.StatusBadRequest, `{"errors":["bad request"]}`),
			wantErr: "answered 400",
		},
		{
			name:    "a body that is not JSON",
			ref:     "apps/api#token",
			handler: jsonHandler(http.StatusOK, value),
			wantErr: "not JSON, at byte",
		},
		{
			name:    "a body whose data is the wrong shape",
			ref:     "apps/api#token",
			handler: jsonHandler(http.StatusOK, `{"data":{"data":"`+value+`"}}`),
			wantErr: "at byte",
		},
		{
			name:    "a secret with no fields",
			ref:     "apps/api#token",
			handler: jsonHandler(http.StatusOK, kvBody(1, map[string]any{})),
			wantIs:  secrets.ErrEmpty,
			wantErr: "holds no fields",
		},
		{
			name:    "a deleted current version",
			ref:     "apps/api#token",
			handler: jsonHandler(http.StatusOK, `{"data":{"data":null,"metadata":{"version":4,"destroyed":false}}}`),
			wantIs:  secrets.ErrNotFound,
			wantErr: "was deleted",
		},
		{
			name:    "a destroyed version",
			ref:     "apps/api#token",
			handler: jsonHandler(http.StatusOK, `{"data":{"data":null,"metadata":{"version":4,"destroyed":true}}}`),
			wantIs:  secrets.ErrNotFound,
			wantErr: "version 4",
		},
		{
			name:    "a missing field",
			ref:     "apps/api#token",
			handler: jsonHandler(http.StatusOK, kvBody(1, map[string]any{"username": "svc", "password": value})),
			wantIs:  secrets.ErrNotFound,
			wantErr: `no field "token"`,
		},
		{
			name:    "an empty field",
			ref:     "apps/api#token",
			handler: jsonHandler(http.StatusOK, kvBody(1, map[string]any{"token": ""})),
			wantIs:  secrets.ErrEmpty,
			wantErr: `field "token"`,
		},
		{
			name:    "a null field",
			ref:     "apps/api#token",
			handler: jsonHandler(http.StatusOK, kvBody(1, map[string]any{"token": nil})),
			wantIs:  secrets.ErrEmpty,
			wantErr: "is null",
		},
		{
			name: "a field holding an object",
			ref:  "apps/api#token",
			handler: jsonHandler(http.StatusOK, kvBody(1, map[string]any{
				"token": map[string]any{"nested": value},
			})),
			wantErr: "JSON object rather than a value",
		},
		{
			name: "a field holding an array",
			ref:  "apps/api#token",
			handler: jsonHandler(http.StatusOK, kvBody(1, map[string]any{
				"token": []any{value},
			})),
			wantErr: "JSON array rather than a value",
		},
		{
			name: "several fields and no field named",
			ref:  "apps/api",
			handler: jsonHandler(http.StatusOK, kvBody(1, map[string]any{
				"token": value, "username": "svc", "password": value,
			})),
			wantIs:  secrets.ErrInvalidRef,
			wantErr: "holds 3 fields",
		},
		{
			name: "a response larger than the limit",
			ref:  "apps/api#token",
			opts: []Option{WithMaxResponseBytes(512)},
			handler: func(w http.ResponseWriter, _ *http.Request) {
				fmt.Fprint(w, `{"data":{"data":{"token":"`+strings.Repeat("A", 4096)+`"}}}`)
			},
			wantIs:  secrets.ErrTooLarge,
			wantErr: "more than 512 bytes",
		},
		{
			name: "an enormous error page from a vault that is down",
			ref:  "apps/api#token",
			opts: []Option{WithMaxResponseBytes(512)},
			handler: func(w http.ResponseWriter, _ *http.Request) {
				// The status is the accurate classification here: a broken gateway
				// answering with a wall of HTML is a vault to retry, not an
				// oversized secret to give up on.
				w.WriteHeader(http.StatusBadGateway)
				fmt.Fprint(w, "<html>"+strings.Repeat("x", 4096)+"</html>")
			},
			wantIs:    secrets.ErrUnavailable,
			wantErr:   "answered 502",
			retryable: true,
		},
		{
			name:    "an empty body",
			ref:     "apps/api#token",
			handler: jsonHandler(http.StatusOK, ""),
			wantErr: "an empty body",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider, _ := newTestProvider(t, test.handler, test.opts...)

			ref := secrets.NewRef(provider.Scheme(), test.ref)

			secret, err := provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: ref})
			require.True(t, secret.IsZero())
			require.Error(t, err)

			if test.wantIs != nil {
				require.ErrorIs(t, err, test.wantIs)
			}

			require.ErrorContains(t, err, test.wantErr)
			require.Equal(t, test.retryable, secrets.Retryable(err), "classification decides whether the step is retried")

			// Whatever went wrong, the error names the reference and not the value.
			require.Contains(t, err.Error(), "vault:"+test.ref)
			require.NotContains(t, err.Error(), value)
		})
	}
}

// Test_Provider_Resolve_values covers what a KV v2 field may hold, and the field
// selection rules.
func Test_Provider_Resolve_values(t *testing.T) {
	tests := []struct {
		name   string
		ref    string
		fields map[string]any
		want   string
	}{
		{
			name:   "a named field",
			ref:    "apps/api#token",
			fields: map[string]any{"token": "abc123", "username": "svc"},
			want:   "abc123",
		},
		{
			name:   "the only field of a single-field secret",
			ref:    "apps/api",
			fields: map[string]any{"value": "abc123"},
			want:   "abc123",
		},
		{
			name:   "a nested path",
			ref:    "team/db/primary#password",
			fields: map[string]any{"password": "hunter2"},
			want:   "hunter2",
		},
		{
			name:   "a number keeps the digits it was written with",
			ref:    "apps/api#serial",
			fields: map[string]any{"serial": 123456789012345678},
			want:   "123456789012345678",
		},
		{
			name:   "a boolean",
			ref:    "apps/api#enabled",
			fields: map[string]any{"enabled": true},
			want:   "true",
		},
		{
			name:   "a field name outside the path character set",
			ref:    "apps/api#api token",
			fields: map[string]any{"api token": "abc123"},
			want:   "abc123",
		},
		{
			name:   "a multiline value is returned verbatim",
			ref:    "apps/api#key",
			fields: map[string]any{"key": "-----BEGIN KEY-----\nline\n-----END KEY-----\n"},
			want:   "-----BEGIN KEY-----\nline\n-----END KEY-----\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			vault := newFakeVault(t)
			provider := vault.staticProvider(t)

			path, err := provider.SecretPath("team-a", test.ref)
			require.NoError(t, err)

			vault.put(path, test.fields)

			ref := secrets.NewRef(provider.Scheme(), test.ref)

			secret, err := provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: ref})
			require.NoError(t, err)
			require.True(t, secret.EqualString(test.want))
			require.Equal(t, secrets.RefString(ref), secrets.RefString(secret.Ref()))

			// The secret redacts itself everywhere, which is what keeps a value out
			// of a log that formats one by accident.
			require.Equal(t, secrets.Redacted, fmt.Sprintf("%v", secret))
		})
	}
}

func Test_Provider_SecretPath(t *testing.T) {
	tests := []struct {
		name      string
		opts      []Option
		namespace string
		ref       string
		want      string
		wantErr   string
	}{
		{
			name:      "a reserved namespace cannot be spelled",
			namespace: EmptyNamespaceSegment,
			ref:       "apps/api",
			wantErr:   "may only contain lowercase letters",
		},
		{
			name:      "a namespace with a slash is refused",
			namespace: "team-a/team-b",
			ref:       "apps/api",
			wantErr:   "may only contain lowercase letters",
		},
		{
			name:      "the KV v2 data segment is inserted",
			namespace: "team-a",
			ref:       "apps/api#token",
			want:      "secret/data/team-a/apps/api",
		},
		{
			name:      "the empty namespace gets a segment of its own",
			namespace: "",
			ref:       "apps/api#token",
			want:      "secret/data/" + EmptyNamespaceSegment + "/apps/api",
		},
		{
			name:      "a prefix sits above the namespace",
			opts:      []Option{WithPathPrefix("flowstate")},
			namespace: "team-a",
			ref:       "apps/api",
			want:      "secret/data/flowstate/team-a/apps/api",
		},
		{
			name:      "a nested mount keeps its segments",
			opts:      []Option{WithMount("kv/platform")},
			namespace: "team-a",
			ref:       "db/primary#password",
			want:      "kv/platform/data/team-a/db/primary",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider, err := NewProvider(
				"https://vault.example.com:8200",
				append([]Option{WithToken("t")}, test.opts...)...,
			)
			require.NoError(t, err)

			path, err := provider.SecretPath(test.namespace, test.ref)

			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				require.ErrorIs(t, err, secrets.ErrNamespace)

				return
			}

			require.NoError(t, err)
			require.Equal(t, test.want, path)
		})
	}
}

// Test_Provider_Resolve_namespaceScoping is the tenancy test: the same reference
// in two namespaces must be two secrets, and neither tenant may reach the other's
// path by any spelling of a reference.
func Test_Provider_Resolve_namespaceScoping(t *testing.T) {
	vault := newFakeVault(t)
	provider := vault.staticProvider(t)

	vault.put("secret/data/team-a/apps/api", map[string]any{"token": "team-a-token"})
	vault.put("secret/data/team-b/apps/api", map[string]any{"token": "team-b-token"})
	vault.put("secret/data/"+EmptyNamespaceSegment+"/apps/api", map[string]any{"token": "single-tenant-token"})

	ref := secrets.NewRef(provider.Scheme(), "apps/api#token")

	resolve := func(t *testing.T, namespace string) secrets.Secret {
		t.Helper()

		secret, err := provider.Resolve(t.Context(), secrets.Request{Namespace: namespace, Ref: ref})
		require.NoError(t, err)

		return secret
	}

	t.Run("one reference resolves differently per namespace", func(t *testing.T) {
		teamA := resolve(t, "team-a")
		teamB := resolve(t, "team-b")
		none := resolve(t, "")

		require.True(t, teamA.EqualString("team-a-token"))
		require.True(t, teamB.EqualString("team-b-token"))
		require.True(t, none.EqualString("single-tenant-token"))

		require.False(t, teamA.Equal(teamB), "two tenants must not share a secret")
		require.False(t, teamA.Equal(none))
	})

	t.Run("the empty namespace cannot spell its way into a tenant's subtree", func(t *testing.T) {
		// Without a segment of its own, namespace "" with this reference would read
		// exactly what namespace "team-a" reads with "apps/api".
		crafted := secrets.NewRef(provider.Scheme(), "team-a/apps/api#token")

		secret, err := provider.Resolve(t.Context(), secrets.Request{Ref: crafted})
		require.True(t, secret.IsZero())
		require.ErrorIs(t, err, secrets.ErrNotFound)
	})

	t.Run("a tenant cannot traverse into another", func(t *testing.T) {
		for _, name := range []string{
			"../team-b/apps/api#token",
			"apps/../../team-b/apps/api#token",
			"/secret/data/team-b/apps/api#token",
		} {
			crafted := secrets.NewRef(provider.Scheme(), name)

			secret, err := provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: crafted})
			require.True(t, secret.IsZero())
			require.ErrorIs(t, err, secrets.ErrInvalidRef)
			require.NotContains(t, err.Error(), "team-b-token")
		}
	})

	t.Run("every read stayed inside the namespace it asked for", func(t *testing.T) {
		for _, path := range vault.readPaths() {
			require.True(t,
				strings.HasPrefix(path, "secret/data/team-a/") ||
					strings.HasPrefix(path, "secret/data/team-b/") ||
					strings.HasPrefix(path, "secret/data/"+EmptyNamespaceSegment+"/"),
				"unexpected path reached the vault: %q", path,
			)
		}
	})
}

// Test_Provider_neverDisclosesTheValue holds the line the contract draws: a value
// belongs in the [secrets.Secret] that was asked for and nowhere else — not in an
// error, and not in a log.
func Test_Provider_neverDisclosesTheValue(t *testing.T) {
	const value = "hunter2-do-not-leak"

	// Nothing in this package logs, so anything that appears here is a leak. The
	// default logger is captured rather than trusted, because a leak through it
	// would be silent.
	var logged strings.Builder

	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	// A vault that puts the value in everything it says, including its own error
	// text, which is why that text is never echoed.
	handlers := map[string]http.HandlerFunc{
		"in an error body": jsonHandler(
			http.StatusForbidden,
			`{"errors":["permission denied reading `+value+`"]}`,
		),
		"in a 500":            jsonHandler(http.StatusInternalServerError, `{"errors":["`+value+`"]}`),
		"in a malformed body": jsonHandler(http.StatusOK, value),
		"in an unwanted field": jsonHandler(
			http.StatusOK,
			kvBody(1, map[string]any{"other": value}),
		),
		"in a field of the wrong type": jsonHandler(
			http.StatusOK,
			kvBody(1, map[string]any{"token": map[string]any{"inner": value}}),
		),
		"in fields the reference did not ask for": jsonHandler(
			http.StatusOK,
			kvBody(1, map[string]any{"first": value, "second": value}),
		),
		"in a header": func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("X-Vault-Leak", value)
			w.WriteHeader(http.StatusBadRequest)
		},
	}

	for name, handler := range handlers {
		t.Run(name, func(t *testing.T) {
			provider, _ := newTestProvider(t, handler)

			ref := secrets.NewRef(provider.Scheme(), "apps/api#token")

			_, err := provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: ref})
			require.Error(t, err)
			require.NotContains(t, err.Error(), value)
			require.NotContains(t, fmt.Sprintf("%+v", err), value)
		})
	}

	t.Run("the ambiguous-secret error does not name the fields either", func(t *testing.T) {
		// A tenant's field names describe what their credential is made of, and a
		// resolution error is recorded in workflow history.
		provider, _ := newTestProvider(t, handlers["in fields the reference did not ask for"])

		ref := secrets.NewRef(provider.Scheme(), "apps/api")

		_, err := provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: ref})
		require.ErrorIs(t, err, secrets.ErrInvalidRef)
		require.NotContains(t, err.Error(), "second")
	})

	t.Run("nothing was logged", func(t *testing.T) {
		require.Empty(t, logged.String())
	})
}

func Test_Provider_Resolve_transportFailures(t *testing.T) {
	t.Run("a vault that is not listening is unavailable", func(t *testing.T) {
		// A listener that is opened and closed leaves an address nothing answers
		// on, which is a refused connection rather than a timeout.
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)

		addr := "http://" + listener.Addr().String()
		require.NoError(t, listener.Close())

		provider, err := NewProvider(addr, WithToken("t"), WithTimeout(2*time.Second))
		require.NoError(t, err)

		ref := secrets.NewRef(provider.Scheme(), "apps/api#token")

		_, err = provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: ref})
		require.ErrorIs(t, err, secrets.ErrUnavailable)
		require.True(t, secrets.Retryable(err), "an unreachable vault is worth another attempt")
	})

	t.Run("the provider's own timeout is transient", func(t *testing.T) {
		provider, _ := newTestProvider(t, hangingHandler(), WithTimeout(50*time.Millisecond))

		ref := secrets.NewRef(provider.Scheme(), "apps/api#token")

		_, err := provider.Resolve(context.Background(), secrets.Request{Namespace: "team-a", Ref: ref})
		require.ErrorIs(t, err, secrets.ErrUnavailable)
		require.True(t, secrets.Retryable(err))
	})

	t.Run("a caller's deadline wins, and is not reported as transient", func(t *testing.T) {
		// The caller's deadline is nearer than the provider's timeout, and its
		// error comes back as its own: a step that ran out of time is not a step to
		// retry on this provider's advice.
		provider, _ := newTestProvider(t, hangingHandler(), WithTimeout(time.Minute))

		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()

		ref := secrets.NewRef(provider.Scheme(), "apps/api#token")

		_, err := provider.Resolve(ctx, secrets.Request{Namespace: "team-a", Ref: ref})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.False(t, secrets.Retryable(err))
	})

	t.Run("a cancelled context is not a request", func(t *testing.T) {
		var served atomic.Int64

		provider, _ := newTestProvider(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			served.Add(1)
		}))

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		ref := secrets.NewRef(provider.Scheme(), "apps/api#token")

		_, err := provider.Resolve(ctx, secrets.Request{Namespace: "team-a", Ref: ref})
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, served.Load(), "a cancelled activity should not spend a round trip")
	})

	t.Run("a redirect is not followed, so the token goes nowhere else", func(t *testing.T) {
		var elsewhere atomic.Int64

		other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			elsewhere.Add(1)
			fmt.Fprint(w, kvBody(1, map[string]any{"token": "from-the-other-server"}))
		}))
		t.Cleanup(other.Close)

		provider, _ := newTestProvider(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, other.URL+r.URL.Path, http.StatusTemporaryRedirect)
		}))

		ref := secrets.NewRef(provider.Scheme(), "apps/api#token")

		_, err := provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: ref})
		require.ErrorContains(t, err, "answered 307")
		require.Zero(t, elsewhere.Load(), "a client token must not be sent to a redirect target")
	})

	t.Run("a caller-supplied client does not follow redirects either", func(t *testing.T) {
		// Go's default policy follows a redirect and strips only the headers it
		// knows to be credentials. X-Vault-Token is not one of them, so a client
		// that followed one would hand this worker's token to whatever host the
		// redirect named — and would then accept that host's answer as the secret.
		var elsewhere atomic.Int64

		other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			elsewhere.Add(1)
			fmt.Fprint(w, kvBody(1, map[string]any{"token": "forged-by-the-redirect-target"}))
		}))
		t.Cleanup(other.Close)

		provider, _ := newTestProvider(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, other.URL+r.URL.Path, http.StatusFound)
		}), WithHTTPClient(&http.Client{}))

		ref := secrets.NewRef(provider.Scheme(), "apps/api#token")

		secret, err := provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: ref})
		require.True(t, secret.IsZero(), "a redirect target must not be able to forge a secret")
		require.ErrorContains(t, err, "answered 302")
		require.Zero(t, elsewhere.Load(), "a client token must not be sent to a redirect target")
	})

	t.Run("a deadline that expires mid-body is the caller's, not a classification", func(t *testing.T) {
		// The status alone would say "sealed, worth retrying". What actually
		// happened is that the caller ran out of time, and a step that ran out of
		// time is not one to retry on this provider's advice.
		provider, _ := newTestProvider(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, `{"errors":["Vault is sea`)
			w.(http.Flusher).Flush()

			<-r.Context().Done()
		}), WithTimeout(time.Minute))

		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()

		ref := secrets.NewRef(provider.Scheme(), "apps/api#token")

		_, err := provider.Resolve(ctx, secrets.Request{Namespace: "team-a", Ref: ref})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.False(t, secrets.Retryable(err))
	})

	t.Run("no request is made without a context", func(t *testing.T) {
		provider, _ := newTestProvider(t, jsonHandler(http.StatusOK, kvBody(1, map[string]any{"token": "v"})))

		ref := secrets.NewRef(provider.Scheme(), "apps/api#token")

		//lint:ignore SA1012 passing nil is the mistake under test; the refusal is the assertion
		_, err := provider.Resolve(nil, secrets.Request{Namespace: "team-a", Ref: ref})
		require.ErrorContains(t, err, "requires a context")
	})

	t.Run("a missing reference is refused", func(t *testing.T) {
		provider, _ := newTestProvider(t, jsonHandler(http.StatusOK, kvBody(1, map[string]any{"token": "v"})))

		_, err := provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a"})
		require.ErrorIs(t, err, secrets.ErrInvalidRef)
	})
}

// hangingHandler never answers, holding the request until its context ends, which
// is what an unresponsive vault looks like from the outside.
func hangingHandler() http.HandlerFunc {
	return func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}
}

func Test_Provider_TLS(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, kvBody(1, map[string]any{"token": "abc123"}))
	}))
	t.Cleanup(server.Close)

	ref := secrets.NewRef(DefaultScheme, "apps/api#token")

	t.Run("an untrusted certificate is refused", func(t *testing.T) {
		// Verification is on by default and cannot be turned off, so a vault
		// presenting a certificate the worker does not trust is unreachable rather
		// than trusted anyway.
		provider, err := NewProvider(server.URL, WithToken("t"))
		require.NoError(t, err)

		_, err = provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: ref})
		require.ErrorIs(t, err, secrets.ErrUnavailable)
		require.ErrorContains(t, err, "certificate")
	})

	t.Run("a private CA is trusted when it is configured", func(t *testing.T) {
		pool := x509.NewCertPool()
		pool.AddCert(server.Certificate())

		provider, err := NewProvider(server.URL, WithToken("t"), WithRootCAs(pool))
		require.NoError(t, err)

		secret, err := provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: ref})
		require.NoError(t, err)
		require.True(t, secret.EqualString("abc123"))
	})
}

// Test_Provider_requestShape checks what the vault actually receives, since the
// audit log an operator reads is made of this and the token must travel in a
// header rather than anywhere it could be recorded.
func Test_Provider_requestShape(t *testing.T) {
	vault := newFakeVault(t)
	provider := vault.staticProvider(t, WithVaultNamespace("platform/prod"))

	vault.put("secret/data/team-a/apps/api", map[string]any{"token": "abc123"})

	ref := secrets.NewRef(provider.Scheme(), "apps/api#token")

	_, err := provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: ref})
	require.NoError(t, err)

	reads := vault.readsServed()
	require.Len(t, reads, 1)

	require.Equal(t, "secret/data/team-a/apps/api", reads[0].path)
	require.Equal(t, "static-token", reads[0].token)
	require.Equal(t, "platform/prod", reads[0].vaultNS, "the enterprise namespace is the operator's, not the tenant's")
	require.Equal(t, userAgent, reads[0].userAgent)
	require.Equal(t, "application/json", reads[0].accept)
	require.Equal(t, "true", reads[0].request)
}

// Test_Provider_asAStoreProvider wires the provider up the way a worker does, so
// that the checks the store makes of a provider — a non-empty secret carrying the
// reference that was asked for — are exercised too.
func Test_Provider_asAStoreProvider(t *testing.T) {
	vault := newFakeVault(t)
	provider := vault.staticProvider(t)

	vault.put("secret/data/team-a/apps/api", map[string]any{"token": "abc123"})

	store, err := secrets.NewStore(secrets.NewCache(provider))
	require.NoError(t, err)
	require.Equal(t, []string{DefaultScheme}, store.Schemes())

	resolver, err := store.For(secrets.Namespace("team-a"))
	require.NoError(t, err)

	ref, err := secrets.ParseRef("vault:apps/api#token")
	require.NoError(t, err)

	secret, err := resolver.Resolve(t.Context(), ref)
	require.NoError(t, err)
	require.True(t, secret.EqualString("abc123"))

	// The cache is what keeps a network round trip off the second resolution; the
	// provider itself holds no value between calls.
	_, err = resolver.Resolve(t.Context(), ref)
	require.NoError(t, err)
	require.Len(t, vault.readsServed(), 1)

	t.Run("another tenant does not see it", func(t *testing.T) {
		other, err := store.For(secrets.Namespace("team-b"))
		require.NoError(t, err)

		_, err = other.Resolve(t.Context(), ref)
		require.ErrorIs(t, err, secrets.ErrNotFound)
	})
}

// Test_Provider_concurrentResolve is the test the race detector cares about: one
// provider serves every task execution on a worker.
func Test_Provider_concurrentResolve(t *testing.T) {
	vault := newFakeVault(t)
	provider, _ := vault.kubernetesProvider(t)

	for _, namespace := range []string{"team-a", "team-b"} {
		vault.put("secret/data/"+namespace+"/apps/api", map[string]any{"token": namespace + "-token"})
	}

	const workers = 24

	errs := make(chan error, workers)

	for i := range workers {
		namespace := "team-a"
		if i%2 == 0 {
			namespace = "team-b"
		}

		go func() {
			ref := secrets.NewRef(provider.Scheme(), "apps/api#token")

			secret, err := provider.Resolve(context.Background(), secrets.Request{Namespace: namespace, Ref: ref})
			if err != nil {
				errs <- err
				return
			}

			if !secret.EqualString(namespace + "-token") {
				errs <- fmt.Errorf("namespace %q resolved another tenant's secret", namespace)
				return
			}

			errs <- nil
		}()
	}

	for range workers {
		require.NoError(t, <-errs)
	}

	// One login between them, not one each: a burst of task executions at worker
	// startup must not be a burst of writes to Vault's token store.
	require.Equal(t, 1, vault.loginCount())
	require.Len(t, vault.readsServed(), workers)
}

// Test_errorsWrapResolveError checks the shape the contract asks for, since the
// store repairs a provider that gets it wrong and a test would not otherwise
// notice.
func Test_errorsWrapResolveError(t *testing.T) {
	provider, _ := newTestProvider(t, jsonHandler(http.StatusNotFound, `{"errors":[]}`))

	ref := secrets.NewRef(provider.Scheme(), "apps/api#token")

	_, err := provider.Resolve(t.Context(), secrets.Request{Namespace: "team-a", Ref: ref})

	var resolveErr *secrets.ResolveError
	require.True(t, errors.As(err, &resolveErr), "every failure must carry the reference")
	require.Equal(t, secrets.RefString(ref), secrets.RefString(resolveErr.Ref))
	require.ErrorIs(t, resolveErr, secrets.ErrNotFound)
}
