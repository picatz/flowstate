package secrets

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_KeychainProvider_Resolve(t *testing.T) {
	tests := []struct {
		name string
		// namespaced opts the provider into tenancy. Off by default, matching the
		// provider, because a worker configured for one tenant must not become
		// multi-tenant just because a request carried a namespace.
		namespaced bool
		namespace  string
		ref        string
		out        string
		runErr     error
		check      func(t *testing.T, secret Secret, err error, runner *fakeRunner)
	}{
		// Negative cases first.
		{
			name:   "the tool reports the item is missing",
			ref:    "absent",
			runErr: errors.New("wrapped: " + ErrNotFound.Error()),
			check: func(t *testing.T, _ Secret, err error, _ *fakeRunner) {
				require.Error(t, err)
				require.ErrorContains(t, err, "keychain:absent")
			},
		},
		{
			name:   "the tool is unavailable",
			ref:    "token",
			runErr: ErrUnavailable,
			check: func(t *testing.T, _ Secret, err error, _ *fakeRunner) {
				require.ErrorIs(t, err, ErrUnavailable)
				require.True(t, Retryable(err), "an unreachable tool is worth another attempt")
			},
		},
		{
			name: "an empty item is a configuration mistake",
			ref:  "blank",
			out:  "\n",
			check: func(t *testing.T, _ Secret, err error, _ *fakeRunner) {
				require.ErrorIs(t, err, ErrEmpty)
				require.False(t, Retryable(err))
			},
		},
		{
			name: "an account starting with a dash is refused",
			ref:  "-w",
			check: func(t *testing.T, _ Secret, err error, runner *fakeRunner) {
				// Not because a shell would misread it — there is no shell — but
				// because the tool itself would take it as an option.
				require.ErrorIs(t, err, ErrInvalidRef)
				require.Empty(t, runner.calls, "an invalid name must not reach the tool")
			},
		},
		{
			name: "a control character is refused",
			ref:  "tok\nen",
			check: func(t *testing.T, _ Secret, err error, runner *fakeRunner) {
				require.ErrorIs(t, err, ErrInvalidRef)
				require.Empty(t, runner.calls)
			},
		},

		{
			name: "a stored item resolves",
			ref:  "github-token",
			out:  "ghp_example_value\n",
			check: func(t *testing.T, secret Secret, err error, runner *fakeRunner) {
				require.NoError(t, err)
				require.Equal(t, "ghp_example_value", secret.Reveal())

				require.Equal(t, []string{
					"security", "find-generic-password",
					"-s", "flowstate",
					"-a", "github-token",
					"-w",
				}, runner.argv(t))
			},
		},
		{
			name:       "a namespace scopes the service",
			namespaced: true,
			namespace:  "team-a",
			ref:        "github-token",
			out:        "team-a-value\n",
			check: func(t *testing.T, secret Secret, err error, runner *fakeRunner) {
				require.NoError(t, err)
				require.Equal(t, "team-a-value", secret.Reveal())
				require.Contains(t, runner.argv(t), "flowstate/team-a",
					"two tenants on one machine must not share keychain entries")
			},
		},
		{
			name: "a name that would need quoting is passed as one argument",
			ref:  "a name; rm -rf /",
			out:  "value\n",
			check: func(t *testing.T, secret Secret, err error, runner *fakeRunner) {
				require.NoError(t, err)
				require.Equal(t, "value", secret.Reveal())
				require.Contains(t, runner.argv(t), "a name; rm -rf /",
					"the name reaches the tool intact, as one argument, with no shell to reinterpret it")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := &fakeRunner{out: []byte(test.out), err: test.runErr}

			options := []KeychainOption{withKeychainRunner(runner)}
			if test.namespaced {
				options = append(options, WithKeychainNamespaced())
			}

			provider, err := NewKeychainProvider(options...)
			require.NoError(t, err)
			require.Equal(t, "keychain", provider.Scheme())

			secret, err := provider.Resolve(t.Context(), Request{
				Namespace: test.namespace,
				Ref:       NewRef("keychain", test.ref),
			})
			test.check(t, secret, err, runner)
		})
	}
}

func Test_NewKeychainProvider(t *testing.T) {
	t.Run("an empty service is refused", func(t *testing.T) {
		_, err := NewKeychainProvider(withKeychainRunner(&fakeRunner{}), WithKeychainService(""))
		require.ErrorContains(t, err, "must not be empty")
	})

	t.Run("the service is reportable", func(t *testing.T) {
		provider, err := NewKeychainProvider(withKeychainRunner(&fakeRunner{}), WithKeychainService("custom"))
		require.NoError(t, err)
		require.Equal(t, "custom", provider.Service())
	})
}

func Test_OnePasswordProvider_Resolve(t *testing.T) {
	tests := []struct {
		name string
		// namespaced opts the provider into tenancy. Off by default, matching the
		// provider, because a worker configured for one tenant must not become
		// multi-tenant just because a request carried a namespace.
		namespaced bool
		namespace  string
		ref        string
		out        string
		runErr     error
		check      func(t *testing.T, secret Secret, err error, runner *fakeRunner)
	}{
		// Negative cases first.
		{
			name:   "the CLI is not installed",
			ref:    "github",
			runErr: ErrUnavailable,
			check: func(t *testing.T, _ Secret, err error, _ *fakeRunner) {
				require.ErrorIs(t, err, ErrUnavailable)
			},
		},
		{
			name: "an empty field is a configuration mistake",
			ref:  "github#token",
			out:  "",
			check: func(t *testing.T, _ Secret, err error, _ *fakeRunner) {
				require.ErrorIs(t, err, ErrEmpty)
			},
		},
		{
			name: "an item containing a slash is refused",
			ref:  "vault/item#field",
			check: func(t *testing.T, _ Secret, err error, runner *fakeRunner) {
				// A slash would let the name address a different vault, since the
				// reference is assembled into an op:// URI.
				require.ErrorIs(t, err, ErrInvalidRef)
				require.Empty(t, runner.calls)
			},
		},
		{
			name: "an empty item is refused",
			ref:  "#field",
			check: func(t *testing.T, _ Secret, err error, runner *fakeRunner) {
				require.ErrorIs(t, err, ErrInvalidRef)
				require.Empty(t, runner.calls)
			},
		},
		{
			name: "an empty field is refused",
			ref:  "item#",
			check: func(t *testing.T, _ Secret, err error, runner *fakeRunner) {
				require.ErrorIs(t, err, ErrInvalidRef)
				require.Empty(t, runner.calls)
			},
		},
		{
			name: "a control character is refused",
			ref:  "item\x00#field",
			check: func(t *testing.T, _ Secret, err error, runner *fakeRunner) {
				require.ErrorIs(t, err, ErrInvalidRef)
				require.Empty(t, runner.calls)
			},
		},

		{
			name: "an item resolves through the default field",
			ref:  "github",
			out:  "default-field-value",
			check: func(t *testing.T, secret Secret, err error, runner *fakeRunner) {
				require.NoError(t, err)
				require.Equal(t, "default-field-value", secret.Reveal())
				require.Contains(t, runner.argv(t), "op://flowstate/github/password")
			},
		},
		{
			name: "a named field resolves",
			ref:  "github#token",
			out:  "token-value",
			check: func(t *testing.T, secret Secret, err error, runner *fakeRunner) {
				require.NoError(t, err)
				require.Equal(t, "token-value", secret.Reveal())
				require.Contains(t, runner.argv(t), "op://flowstate/github/token")
			},
		},
		{
			name:       "a namespace selects the vault",
			namespaced: true,
			namespace:  "team-a",
			ref:        "github#token",
			out:        "team-a-token",
			check: func(t *testing.T, secret Secret, err error, runner *fakeRunner) {
				require.NoError(t, err)
				require.Contains(t, runner.argv(t), "op://team-a/github/token",
					"each tenant reads its own vault")
			},
		},
		{
			name: "the reference is passed after a -- separator",
			ref:  "github",
			out:  "v",
			check: func(t *testing.T, _ Secret, err error, runner *fakeRunner) {
				require.NoError(t, err)
				require.Contains(t, runner.argv(t), "--",
					"the separator keeps a reference from being read as an option")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := &fakeRunner{out: []byte(test.out), err: test.runErr}

			options := []OnePasswordOption{withOnePasswordRunner(runner)}
			if test.namespaced {
				options = append(options, WithOnePasswordNamespaced())
			}

			provider, err := NewOnePasswordProvider(options...)
			require.NoError(t, err)
			require.Equal(t, "op", provider.Scheme())

			secret, err := provider.Resolve(t.Context(), Request{
				Namespace: test.namespace,
				Ref:       NewRef("op", test.ref),
			})
			test.check(t, secret, err, runner)
		})
	}
}

func Test_parseOnePasswordName(t *testing.T) {
	tests := []struct {
		name      string
		in        string
		wantItem  string
		wantField string
		wantErr   bool
	}{
		{name: "item only", in: "github", wantItem: "github", wantField: "password"},
		{name: "item and field", in: "github#token", wantItem: "github", wantField: "token"},
		{name: "empty", in: "", wantErr: true},
		{name: "empty item", in: "#f", wantErr: true},
		{name: "empty field", in: "i#", wantErr: true},
		{name: "slash in item", in: "a/b", wantErr: true},
		{name: "slash in field", in: "i#a/b", wantErr: true},
		{name: "two hashes", in: "i#f#g", wantErr: true},
		{name: "dash-led item", in: "-i", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			item, field, err := parseOnePasswordName(test.in)

			if test.wantErr {
				require.ErrorIs(t, err, ErrInvalidRef)
				return
			}

			require.NoError(t, err)
			require.Equal(t, test.wantItem, item)
			require.Equal(t, test.wantField, field)
		})
	}
}

// Test_localProviders_neverLeakValues checks that nothing a local provider reports
// carries the secret it read, since these errors reach workflow history.
func Test_localProviders_neverLeakValues(t *testing.T) {
	const value = "local-provider-secret-4f2a"

	t.Run("a keychain value stays out of errors", func(t *testing.T) {
		runner := &fakeRunner{out: []byte(value + "\n")}

		provider, err := NewKeychainProvider(withKeychainRunner(runner))
		require.NoError(t, err)

		secret, err := provider.Resolve(t.Context(), Request{Ref: NewRef("keychain", "item")})
		require.NoError(t, err)
		require.Equal(t, value, secret.Reveal())

		// The value is reachable only through Reveal; everything else redacts.
		require.NotContains(t, secret.String(), value)
		require.NotContains(t, runnerArgs(runner), value,
			"a secret must never be passed to a tool as an argument, where a process listing would show it")
	})

	t.Run("a 1Password value stays out of errors", func(t *testing.T) {
		runner := &fakeRunner{out: []byte(value)}

		provider, err := NewOnePasswordProvider(withOnePasswordRunner(runner))
		require.NoError(t, err)

		secret, err := provider.Resolve(t.Context(), Request{Ref: NewRef("op", "item#field")})
		require.NoError(t, err)
		require.Equal(t, value, secret.Reveal())
		require.NotContains(t, runnerArgs(runner), value)
	})
}

// runnerArgs flattens every recorded argv, for asserting what a tool was told.
func runnerArgs(r *fakeRunner) string {
	var all string
	for _, call := range r.calls {
		for _, arg := range call {
			all += arg + " "
		}
	}

	return all
}
