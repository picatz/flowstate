package secrets

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// mustEnvProvider builds an env provider, failing the test if the options are
// rejected.
func mustEnvProvider(t *testing.T, opts ...EnvOption) *EnvProvider {
	t.Helper()

	provider, err := NewEnvProvider(opts...)
	require.NoError(t, err)

	return provider
}

func Test_NewEnvProvider(t *testing.T) {
	tests := []struct {
		name    string
		opts    []EnvOption
		wantErr string
	}{
		{
			// An empty prefix would expose every variable in the worker's
			// environment, so it must not be reachable by a typo in config.
			name:    "an empty prefix with no allowlist is refused",
			opts:    []EnvOption{WithEnvPrefix("")},
			wantErr: "exposes every variable",
		},
		{
			name:    "an invalid allowed name is refused",
			opts:    []EnvOption{WithEnvAllow("not-a-valid-name")},
			wantErr: "not a valid environment variable name",
		},
		{
			name: "an empty prefix with an allowlist is permitted",
			opts: []EnvOption{WithEnvPrefix(""), WithEnvAllow("HOME")},
		},
		{
			name: "the default is safe",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider, err := NewEnvProvider(test.opts...)

			if test.wantErr != "" {
				require.Nil(t, provider)
				require.ErrorContains(t, err, test.wantErr)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, provider)
		})
	}
}

func Test_EnvProvider_Resolve(t *testing.T) {
	tests := []struct {
		name  string
		env   map[string]string
		opts  []EnvOption
		ref   Ref
		check func(t *testing.T, secret Secret, err error)
	}{
		// Negative cases first.
		{
			name: "unset variable",
			ref:  NewRef("env", "MISSING"),
			check: func(t *testing.T, _ Secret, err error) {
				require.ErrorIs(t, err, ErrNotFound)
				require.ErrorContains(t, err, "$FLOWSTATE_SECRET_MISSING is not configured")
			},
		},
		{
			name: "set but empty",
			env:  map[string]string{"FLOWSTATE_SECRET_BLANK": ""},
			ref:  NewRef("env", "BLANK"),
			check: func(t *testing.T, _ Secret, err error) {
				require.ErrorIs(t, err, ErrEmpty)
				require.ErrorContains(t, err, "set but empty")
			},
		},
		{
			// The prefix is what stops a workflow reading the worker's own
			// credentials. Without it, a Flowfile could name any variable.
			name: "a variable outside the prefix is invisible",
			env:  map[string]string{"AWS_SECRET_ACCESS_KEY": "the-worker-credentials"},
			ref:  NewRef("env", "AWS_SECRET_ACCESS_KEY"),
			check: func(t *testing.T, _ Secret, err error) {
				require.ErrorIs(t, err, ErrNotFound)
			},
		},
		{
			name: "a name cannot escape the prefix",
			env:  map[string]string{"PATH_INJECTED": "nope"},
			ref:  NewRef("env", "../PATH_INJECTED"),
			check: func(t *testing.T, _ Secret, err error) {
				require.ErrorIs(t, err, ErrInvalidRef)
				require.ErrorContains(t, err, "not a valid environment variable name")
			},
		},
		{
			name: "a name with an equals sign is refused",
			ref:  NewRef("env", "A=B"),
			check: func(t *testing.T, _ Secret, err error) {
				require.ErrorIs(t, err, ErrInvalidRef)
			},
		},
		{
			name: "a name starting with a digit is refused",
			ref:  NewRef("env", "1KEY"),
			check: func(t *testing.T, _ Secret, err error) {
				require.ErrorIs(t, err, ErrInvalidRef)
			},
		},
		{
			name: "a name outside the allowlist is refused even when it exists",
			env:  map[string]string{"FLOWSTATE_SECRET_OTHER": "value"},
			opts: []EnvOption{WithEnvAllow("API_KEY")},
			ref:  NewRef("env", "OTHER"),
			check: func(t *testing.T, _ Secret, err error) {
				// Deliberately the same error as an unset variable, so a workflow
				// cannot enumerate the allowlist by comparing failures.
				require.ErrorIs(t, err, ErrNotFound)
				require.ErrorContains(t, err, "is not configured on this worker")
			},
		},

		{
			name: "a set variable resolves",
			env:  map[string]string{"FLOWSTATE_SECRET_API_KEY": "abc123"},
			ref:  NewRef("env", "API_KEY"),
			check: func(t *testing.T, secret Secret, err error) {
				require.NoError(t, err)
				require.Equal(t, "abc123", secret.Reveal())
				require.Equal(t, "env:API_KEY", RefString(secret.Ref()))
			},
		},
		{
			name: "a name in the allowlist resolves",
			env:  map[string]string{"FLOWSTATE_SECRET_API_KEY": "abc123"},
			opts: []EnvOption{WithEnvAllow("API_KEY", "OTHER")},
			ref:  NewRef("env", "API_KEY"),
			check: func(t *testing.T, secret Secret, err error) {
				require.NoError(t, err)
				require.Equal(t, "abc123", secret.Reveal())
			},
		},
		{
			name: "a custom prefix is used",
			env:  map[string]string{"MYAPP_": "wrong", "MYAPP_TOKEN": "right"},
			opts: []EnvOption{WithEnvPrefix("MYAPP_")},
			ref:  NewRef("env", "TOKEN"),
			check: func(t *testing.T, secret Secret, err error) {
				require.NoError(t, err)
				require.Equal(t, "right", secret.Reveal())
			},
		},
		{
			name: "whitespace in a value is preserved",
			env:  map[string]string{"FLOWSTATE_SECRET_PADDED": "  spaced  "},
			ref:  NewRef("env", "PADDED"),
			check: func(t *testing.T, secret Secret, err error) {
				require.NoError(t, err)
				require.Equal(t, "  spaced  ", secret.Reveal(),
					"an environment value is taken exactly as set")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for name, value := range test.env {
				t.Setenv(name, value)
			}

			provider, err := NewEnvProvider(test.opts...)
			require.NoError(t, err)
			require.Equal(t, "env", provider.Scheme())

			secret, err := provider.Resolve(t.Context(), Request{Ref: test.ref})
			test.check(t, secret, err)
		})
	}
}

func Test_EnvProvider_Names(t *testing.T) {
	t.Setenv("FLOWSTATE_SECRET_BETA", "b")
	t.Setenv("FLOWSTATE_SECRET_ALPHA", "a")
	t.Setenv("UNRELATED_VARIABLE", "x")

	t.Run("names under the prefix, sorted", func(t *testing.T) {
		names := mustEnvProvider(t).Names()

		require.Contains(t, names, "ALPHA")
		require.Contains(t, names, "BETA")
		require.NotContains(t, names, "UNRELATED_VARIABLE")
		require.Equal(t, []string{"ALPHA", "BETA"}, names)
	})

	t.Run("the allowlist narrows the report", func(t *testing.T) {
		names := mustEnvProvider(t, WithEnvAllow("ALPHA")).Names()
		require.Equal(t, []string{"ALPHA"}, names)
	})

	t.Run("names never include values", func(t *testing.T) {
		// The values are distinctive strings so the assertion means something; the
		// earlier version compared against single letters that no name could contain.
		t.Setenv("FLOWSTATE_SECRET_GAMMA", "gamma-secret-value")

		for _, name := range mustEnvProvider(t).Names() {
			require.NotContains(t, name, "gamma-secret-value")
			require.NotContains(t, name, "-secret-value")
		}
	})
}

func Test_validEnvName(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want bool
	}{
		{name: "empty", in: "", want: false},
		{name: "leading digit", in: "1A", want: false},
		{name: "equals", in: "A=B", want: false},
		{name: "dash", in: "A-B", want: false},
		{name: "dot", in: "A.B", want: false},
		{name: "slash", in: "A/B", want: false},
		{name: "space", in: "A B", want: false},
		{name: "null byte", in: "A\x00B", want: false},
		{name: "newline", in: "A\nB", want: false},
		{name: "uppercase", in: "API_KEY", want: true},
		{name: "lowercase", in: "api_key", want: true},
		{name: "leading underscore", in: "_KEY", want: true},
		{name: "trailing digits", in: "KEY2", want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, validEnvName(test.in))
		})
	}
}
