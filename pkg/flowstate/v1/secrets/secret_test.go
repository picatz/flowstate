package secrets

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// theValue is the secret used throughout these tests. It is a distinctive string
// so that a leak anywhere is unambiguous.
const theValue = "s3cret-value-do-not-log-3f9a1c"

// testSecret returns a resolved secret holding [theValue].
func testSecret(t *testing.T) Secret {
	t.Helper()

	return NewSecret(NewRef("env", "API_KEY"), theValue)
}

// requireNoLeak fails if text contains the secret value.
func requireNoLeak(t *testing.T, what, text string) {
	t.Helper()

	require.NotContains(t, text, theValue, "%s leaked the secret value", what)
}

// Test_Secret_neverLeaksThroughFormatting covers every fmt verb. A Secret
// implements fmt.Formatter, so the redaction must hold for verbs nobody
// anticipated, not just %v and %s.
func Test_Secret_neverLeaksThroughFormatting(t *testing.T) {
	secret := testSecret(t)

	verbs := []string{
		"%v", "%+v", "%#v", "%s", "%q", "%d", "%x", "%X", "%t",
		"%f", "%e", "%g", "%c", "%U", "%b", "%o", "%8.3v", "%-20s",
	}

	for _, verb := range verbs {
		t.Run(verb, func(t *testing.T) {
			out := fmt.Sprintf(verb, secret)
			requireNoLeak(t, "fmt "+verb, out)
			require.Contains(t, out, Redacted)
		})
	}

	// fmt intercepts %p and %T before it consults a Formatter, so neither can be
	// redacted. Neither exposes anything: %T is a type name, and %p reports the
	// reference and the address of the closure holding the value.
	for _, verb := range []string{"%p", "%T"} {
		t.Run(verb+" cannot be redacted but reveals nothing", func(t *testing.T) {
			requireNoLeak(t, "fmt "+verb, fmt.Sprintf(verb, secret))
		})
	}

	t.Run("as a formatting argument list", func(t *testing.T) {
		requireNoLeak(t, "Sprint", fmt.Sprint(secret))
		requireNoLeak(t, "Sprintln", fmt.Sprintln(secret))
		requireNoLeak(t, "Errorf %v", fmt.Errorf("failed: %v", secret).Error())
		requireNoLeak(t, "Errorf %w-adjacent", fmt.Errorf("failed: %s", secret).Error())
	})

	t.Run("as a pointer", func(t *testing.T) {
		requireNoLeak(t, "pointer %v", fmt.Sprintf("%v", &secret))
		requireNoLeak(t, "pointer %+v", fmt.Sprintf("%+v", &secret))
		requireNoLeak(t, "pointer %#v", fmt.Sprintf("%#v", &secret))
	})
}

// Test_Secret_neverLeaksThroughContainingStructs covers the case the method set
// alone does not: a Secret held by another struct.
//
// When the holding field is unexported, fmt may not call the Secret's methods and
// reflects over its fields instead. That is why the value lives behind a pointer;
// without it, %v on the holder would print the value in full.
func Test_Secret_neverLeaksThroughContainingStructs(t *testing.T) {
	secret := testSecret(t)

	type unexportedField struct{ token Secret }
	type exportedField struct{ Token Secret }
	type nested struct{ inner unexportedField }

	subjects := map[string]any{
		"unexported field":         unexportedField{token: secret},
		"exported field":           exportedField{Token: secret},
		"pointer to unexported":    &unexportedField{token: secret},
		"nested unexported":        nested{inner: unexportedField{token: secret}},
		"slice of holders":         []unexportedField{{token: secret}},
		"map of holders":           map[string]unexportedField{"a": {token: secret}},
		"slice of secrets":         []Secret{secret},
		"map with secret values":   map[string]Secret{"a": secret},
		"any holding a secret":     any(secret),
		"pointer slice of holders": []*unexportedField{{token: secret}},
	}

	for name, subject := range subjects {
		t.Run(name, func(t *testing.T) {
			for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
				requireNoLeak(t, "fmt "+verb+" of "+name, fmt.Sprintf(verb, subject))
			}
		})
	}
}

// Test_Secret_neverLeaksThroughLogging covers the logging paths, since an
// activity's logger is exactly where a value would end up by accident.
func Test_Secret_neverLeaksThroughLogging(t *testing.T) {
	secret := testSecret(t)

	type holder struct{ token Secret }

	tests := []struct {
		name string
		log  func(t *testing.T, buf *bytes.Buffer)
	}{
		{
			name: "slog text handler, secret as attribute",
			log: func(t *testing.T, buf *bytes.Buffer) {
				slog.New(slog.NewTextHandler(buf, nil)).Info("using secret", "secret", secret)
			},
		},
		{
			name: "slog JSON handler, secret as attribute",
			log: func(t *testing.T, buf *bytes.Buffer) {
				slog.New(slog.NewJSONHandler(buf, nil)).Info("using secret", "secret", secret)
			},
		},
		{
			name: "slog with a struct holding a secret",
			log: func(t *testing.T, buf *bytes.Buffer) {
				slog.New(slog.NewJSONHandler(buf, nil)).Info("using secret", "holder", holder{token: secret})
			},
		},
		{
			name: "slog group",
			log: func(t *testing.T, buf *bytes.Buffer) {
				logger := slog.New(slog.NewTextHandler(buf, nil))
				logger.WithGroup("auth").With("secret", secret).Error("boom")
			},
		},
		{
			name: "standard library log",
			log: func(t *testing.T, buf *bytes.Buffer) {
				log.New(buf, "", 0).Printf("secret is %v", secret)
			},
		},
		{
			name: "standard library log with Println",
			log: func(t *testing.T, buf *bytes.Buffer) {
				log.New(buf, "", 0).Println(secret)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer
			test.log(t, &buf)

			require.NotEmpty(t, buf.String(), "the test logged nothing, so it proves nothing")
			requireNoLeak(t, test.name, buf.String())
		})
	}
}

// Test_Secret_neverLeaksThroughSerialization covers marshaling, and the refusal to
// unmarshal.
func Test_Secret_neverLeaksThroughSerialization(t *testing.T) {
	secret := testSecret(t)

	t.Run("json.Marshal of the secret", func(t *testing.T) {
		out, err := json.Marshal(secret)
		require.NoError(t, err)
		requireNoLeak(t, "json.Marshal", string(out))
		require.JSONEq(t, `"`+Redacted+`"`, string(out))
	})

	t.Run("json.Marshal of a struct with an exported secret", func(t *testing.T) {
		out, err := json.Marshal(struct{ Token Secret }{secret})
		require.NoError(t, err)
		requireNoLeak(t, "json.Marshal struct", string(out))
	})

	t.Run("json.Marshal of a struct with an unexported secret", func(t *testing.T) {
		// encoding/json ignores unexported fields, so nothing is emitted at all.
		out, err := json.Marshal(struct{ token Secret }{secret})
		require.NoError(t, err)
		requireNoLeak(t, "json.Marshal unexported", string(out))
	})

	t.Run("json.MarshalIndent of a map", func(t *testing.T) {
		out, err := json.MarshalIndent(map[string]Secret{"api": secret}, "", "  ")
		require.NoError(t, err)
		requireNoLeak(t, "json.MarshalIndent", string(out))
	})

	t.Run("GoString", func(t *testing.T) {
		// %#v reaches Format rather than GoString, so the method is exercised
		// directly: it is exported surface a debugger or printer may call.
		require.Equal(t, Redacted, secret.GoString())
	})

	t.Run("MarshalText", func(t *testing.T) {
		out, err := secret.MarshalText()
		require.NoError(t, err)
		require.Equal(t, Redacted, string(out))
	})

	t.Run("unmarshaling a secret is refused", func(t *testing.T) {
		var target Secret
		err := json.Unmarshal([]byte(`"`+theValue+`"`), &target)
		require.ErrorIs(t, err, ErrNotDeserializable)
		require.True(t, target.IsZero(), "a refused unmarshal must leave nothing behind")
	})

	t.Run("unmarshaling a struct containing a secret is refused", func(t *testing.T) {
		var target struct{ Token Secret }
		err := json.Unmarshal([]byte(`{"Token":"`+theValue+`"}`), &target)
		require.ErrorIs(t, err, ErrNotDeserializable)
		require.True(t, target.Token.IsZero())
	})

	t.Run("UnmarshalText is refused", func(t *testing.T) {
		var target Secret
		require.ErrorIs(t, target.UnmarshalText([]byte(theValue)), ErrNotDeserializable)
	})
}

func Test_Secret_Reveal(t *testing.T) {
	t.Run("the zero secret reveals nothing", func(t *testing.T) {
		var zero Secret

		require.True(t, zero.IsZero())
		require.Empty(t, zero.Reveal())
		require.Zero(t, zero.Len())
		require.Equal(t, Redacted, zero.String())
		require.Nil(t, zero.Ref())
	})

	t.Run("an empty value is not a resolved secret", func(t *testing.T) {
		secret := NewSecret(NewRef("env", "X"), "")

		require.True(t, secret.IsZero(), "an empty value must not look resolved")
		require.Empty(t, secret.Reveal())
	})

	t.Run("a resolved secret reveals its value", func(t *testing.T) {
		secret := testSecret(t)

		require.False(t, secret.IsZero())
		require.Equal(t, theValue, secret.Reveal())
		require.Equal(t, len(theValue), secret.Len())
		require.Equal(t, "env:API_KEY", RefString(secret.Ref()))
	})
}

func Test_Secret_EqualString(t *testing.T) {
	secret := testSecret(t)

	tests := []struct {
		name  string
		other string
		want  bool
	}{
		{name: "equal", other: theValue, want: true},
		{name: "different", other: "something else", want: false},
		{name: "empty", other: "", want: false},
		{name: "prefix of the value", other: theValue[:8], want: false},
		{name: "value with a trailing newline", other: theValue + "\n", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, secret.EqualString(test.other))
		})
	}

	t.Run("an unresolved secret equals nothing at all", func(t *testing.T) {
		// Not even the empty string. A caller that ignored a resolution error must
		// not end up authenticating anyone who presents an empty credential.
		var zero Secret

		require.False(t, zero.EqualString(""))
		require.False(t, zero.EqualString(theValue))
		require.False(t, NewSecret(NewRef("env", "X"), "").EqualString(""))
	})
}

// Test_ResolveError_neverCarriesAValue checks the error type has nowhere for a
// value to hide, and that its message names the reference instead.
func Test_ResolveError_neverCarriesAValue(t *testing.T) {
	err := &ResolveError{
		Ref: NewRef("env", "API_KEY"),
		Err: fmt.Errorf("%w: $FLOWSTATE_SECRET_API_KEY is not set", ErrNotFound),
	}

	require.ErrorIs(t, err, ErrNotFound)
	require.Equal(t,
		`secrets: resolving "env:API_KEY": secret not found: $FLOWSTATE_SECRET_API_KEY is not set`,
		err.Error(),
	)

	// The reference is what makes a failure diagnosable, and it is safe to record.
	require.Contains(t, err.Error(), "env:API_KEY")

	for _, verb := range []string{"%v", "%+v", "%s", "%#v"} {
		requireNoLeak(t, "ResolveError "+verb, fmt.Sprintf(verb, err))
	}
}

func Test_Secret_Equal(t *testing.T) {
	ref := NewRef("env", "API_KEY")

	tests := []struct {
		name string
		a, b Secret
		want bool
	}{
		{name: "same value", a: NewSecret(ref, "v"), b: NewSecret(ref, "v"), want: true},
		{name: "different value", a: NewSecret(ref, "v"), b: NewSecret(ref, "w"), want: false},
		{name: "both unresolved", a: Secret{}, b: Secret{}, want: true},
		{name: "one unresolved", a: NewSecret(ref, "v"), b: Secret{}, want: false},
		{name: "other unresolved", a: Secret{}, b: NewSecret(ref, "v"), want: false},
		{
			name: "the reference is not part of equality",
			a:    NewSecret(ref, "v"),
			b:    NewSecret(NewRef("file", "other"), "v"),
			want: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, test.a.Equal(test.b))
			require.Equal(t, test.want, test.b.Equal(test.a), "equality is symmetric")
		})
	}
}

func Test_Secret_isNotComparable(t *testing.T) {
	// A Secret must not be comparable with ==, so that comparing one goes through
	// the constant-time path. This is enforced at compile time by an incomparable
	// field; the check here is that the type stays that way.
	//
	// The equivalent of `secret == secret` does not compile:
	//
	//	invalid operation: secret == secret (struct containing [0]func() cannot be compared)
	//
	// Note that reflect.DeepEqual does NOT work: a Secret holds its value in a func
	// field, and functions are DeepEqual only when both are nil. Compare with Equal
	// or EqualString instead, which is what the doc comment tells callers.
	a := testSecret(t)
	b := testSecret(t)

	require.False(t, reflect.DeepEqual(a, b), "DeepEqual cannot compare secrets")
	require.True(t, a.Equal(b))
	require.True(t, a.EqualString(b.Reveal()))
	require.NotContains(t, fmt.Sprintf("%v", []Secret{a, b}), theValue)
}

func Test_Redacted(t *testing.T) {
	// The placeholder must not be empty or whitespace: an empty redaction in a log
	// line is indistinguishable from a missing field, which hides that a secret
	// was there at all.
	require.NotEmpty(t, strings.TrimSpace(Redacted))
	require.False(t, strings.Contains(Redacted, theValue))
}

// Test_errorsSurfaceSentinels checks that every sentinel is reachable through the
// wrapping this package does, since callers classify failures with errors.Is.
func Test_errorsSurfaceSentinels(t *testing.T) {
	sentinels := []error{
		ErrNotFound,
		ErrEmpty,
		ErrUnknownScheme,
		ErrInvalidRef,
		ErrNotDeserializable,
		ErrTooLarge,
	}

	for _, sentinel := range sentinels {
		t.Run(sentinel.Error(), func(t *testing.T) {
			wrapped := &ResolveError{
				Ref: NewRef("env", "X"),
				Err: fmt.Errorf("context: %w", sentinel),
			}

			require.ErrorIs(t, wrapped, sentinel)
			require.NotErrorIs(t, wrapped, errors.New("unrelated"))
		})
	}
}
