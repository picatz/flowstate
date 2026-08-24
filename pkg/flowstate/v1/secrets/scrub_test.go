package secrets

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Scrubber_Scrub(t *testing.T) {
	const value = "tok-live-9f8e7d6c"

	secret := NewSecret(NewRef("env", "TOKEN"), value)

	tests := []struct {
		name  string
		text  string
		check func(t *testing.T, got string)
	}{
		{
			name: "the literal value",
			text: "authorization failed for " + value,
			check: func(t *testing.T, got string) {
				require.Equal(t, "authorization failed for "+Redacted, got)
			},
		},
		{
			name: "several occurrences",
			text: value + " and " + value,
			check: func(t *testing.T, got string) {
				require.Equal(t, Redacted+" and "+Redacted, got)
			},
		},
		{
			name: "embedded in a URL, as a client error would report it",
			text: `Get "https://api.example.com/v1?token=` + value + `": connection refused`,
			check: func(t *testing.T, got string) {
				require.NotContains(t, got, value)
				require.Contains(t, got, "connection refused")
			},
		},
		{
			name: "percent-encoded, as a query parameter would carry it",
			text: "query=" + url.QueryEscape(value+"/+="),
			check: func(t *testing.T, got string) {
				require.NotContains(t, got, url.QueryEscape(value))
			},
		},
		{
			name: "standard base64, as a token store might hold it",
			text: "payload " + base64.StdEncoding.EncodeToString([]byte(value)),
			check: func(t *testing.T, got string) {
				require.NotContains(t, got, base64.StdEncoding.EncodeToString([]byte(value)))
			},
		},
		{
			name: "raw URL base64, as a JWT segment would encode it",
			text: "segment " + base64.RawURLEncoding.EncodeToString([]byte(value)),
			check: func(t *testing.T, got string) {
				require.NotContains(t, got, base64.RawURLEncoding.EncodeToString([]byte(value)))
			},
		},
		{
			name: "text with nothing to redact is unchanged",
			text: "connection refused",
			check: func(t *testing.T, got string) {
				require.Equal(t, "connection refused", got)
			},
		},
		{
			name: "empty text",
			text: "",
			check: func(t *testing.T, got string) {
				require.Empty(t, got)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scrubber := NewScrubber(secret)

			got := scrubber.Scrub(test.text)
			require.NotContains(t, got, value, "the raw value survived scrubbing")
			test.check(t, got)
		})
	}
}

func Test_Scrubber_Add(t *testing.T) {
	t.Run("the zero secret registers nothing", func(t *testing.T) {
		scrubber := NewScrubber(Secret{})
		require.Zero(t, scrubber.Len())
		require.Equal(t, "anything", scrubber.Scrub("anything"))
	})

	t.Run("an empty value registers nothing", func(t *testing.T) {
		// An empty needle appears in every string; redacting it would destroy the
		// text while protecting nothing.
		scrubber := NewScrubber()
		scrubber.AddValue("")

		require.Zero(t, scrubber.Len())
		require.Equal(t, "unchanged", scrubber.Scrub("unchanged"))
	})

	t.Run("a value is registered once no matter how often it is added", func(t *testing.T) {
		secret := NewSecret(NewRef("env", "T"), "abc")

		scrubber := NewScrubber(secret, secret)
		before := scrubber.Len()

		scrubber.Add(secret)
		scrubber.AddValue("abc")

		require.Equal(t, before, scrubber.Len())
	})

	t.Run("several secrets are all redacted", func(t *testing.T) {
		scrubber := NewScrubber(
			NewSecret(NewRef("env", "A"), "first-value"),
			NewSecret(NewRef("env", "B"), "second-value"),
		)

		got := scrubber.Scrub("first-value then second-value")
		require.NotContains(t, got, "first-value")
		require.NotContains(t, got, "second-value")
	})

	t.Run("a longer value containing a shorter one leaves no fragment", func(t *testing.T) {
		// Needles are applied longest first, so redacting the short one cannot
		// break the long one into a fragment that survives.
		scrubber := NewScrubber(
			NewSecret(NewRef("env", "SHORT"), "abcd"),
			NewSecret(NewRef("env", "LONG"), "abcdefgh"),
		)

		got := scrubber.Scrub("value abcdefgh here")
		require.NotContains(t, got, "abcd")
		require.NotContains(t, got, "efgh")
	})
}

func Test_Scrubber_Contains(t *testing.T) {
	scrubber := NewScrubber(NewSecret(NewRef("env", "T"), "needle-value"))

	require.True(t, scrubber.Contains("a needle-value here"))
	require.False(t, scrubber.Contains("nothing to see"))
	require.False(t, NewScrubber().Contains("needle-value"))
}

func Test_Scrubber_ScrubError(t *testing.T) {
	const value = "tok-live-9f8e7d6c"

	scrubber := NewScrubber(NewSecret(NewRef("env", "TOKEN"), value))

	t.Run("nil stays nil", func(t *testing.T) {
		require.NoError(t, scrubber.ScrubError(nil))
	})

	t.Run("an error with nothing to redact is returned unchanged", func(t *testing.T) {
		original := errors.New("connection refused")
		require.Same(t, original, scrubber.ScrubError(original))
	})

	t.Run("the value is removed from the message", func(t *testing.T) {
		original := fmt.Errorf("request failed with token %s", value)

		scrubbed := scrubber.ScrubError(original)
		require.NotContains(t, scrubbed.Error(), value)
		require.Contains(t, scrubbed.Error(), Redacted)
		require.Contains(t, scrubbed.Error(), "request failed with token")
	})

	t.Run("classification survives scrubbing", func(t *testing.T) {
		// A caller still has to be able to tell what went wrong, or scrubbing would
		// cost the ability to handle the error.
		sentinel := errors.New("upstream rejected the credential")
		original := fmt.Errorf("token %s: %w", value, sentinel)

		scrubbed := scrubber.ScrubError(original)
		require.ErrorIs(t, scrubbed, sentinel)
		require.NotContains(t, scrubbed.Error(), value)
	})

	t.Run("errors.As deliberately cannot reach a typed error", func(t *testing.T) {
		// Typed errors carry unredacted text in exported fields — *url.Error holds
		// the full URL, query string included — so handing one out would undo the
		// scrubbing. errors.Is is what a caller needs, and that still works.
		original := fmt.Errorf("leaked %s: %w", value, &ResolveError{
			Ref: NewRef("env", "TOKEN"),
			Err: ErrNotFound,
		})

		scrubbed := scrubber.ScrubError(original)

		var resolveErr *ResolveError
		require.False(t, errors.As(scrubbed, &resolveErr),
			"typed extraction must not reach past the redaction")
		require.ErrorIs(t, scrubbed, ErrNotFound, "classification still works")
		require.NotContains(t, scrubbed.Error(), value)
	})

	t.Run("no level of the error chain carries the value", func(t *testing.T) {
		// The failure this pins: Temporal's failure converter walks the whole chain
		// with errors.Unwrap and writes every level's message into workflow history.
		// An Unwrap that reached the original would put the value there despite the
		// scrubbing, so the chain itself has to be clean.
		original := fmt.Errorf("outer %s: %w", value,
			fmt.Errorf("inner %s: %w", value, errors.New("root "+value)))

		scrubbed := scrubber.ScrubError(original)

		for level, err := 0, scrubbed; err != nil; level, err = level+1, errors.Unwrap(err) {
			require.NotContains(t, err.Error(), value,
				"level %d of the unwrap chain leaked the value", level)
			require.Less(t, level, 10, "unwrap chain did not terminate")
		}
	})

	t.Run("every formatting verb stays redacted", func(t *testing.T) {
		scrubbed := scrubber.ScrubError(fmt.Errorf("token %s failed", value))

		for _, verb := range []string{"%v", "%+v", "%s", "%q"} {
			require.NotContains(t, fmt.Sprintf(verb, scrubbed), value, "verb %s leaked", verb)
		}
	})

	t.Run("wrapping a scrubbed error keeps it redacted", func(t *testing.T) {
		// The scrubbed error is what gets returned up the stack, so it has to stay
		// safe as callers add their own context.
		scrubbed := scrubber.ScrubError(fmt.Errorf("token %s failed", value))
		wrapped := fmt.Errorf("step %q: %w", "fetch", scrubbed)

		require.NotContains(t, wrapped.Error(), value)
	})

	t.Run("logging a scrubbed error stays redacted", func(t *testing.T) {
		scrubbed := scrubber.ScrubError(fmt.Errorf("token %s failed", value))

		var buf bytes.Buffer
		slog.New(slog.NewJSONHandler(&buf, nil)).Error("step failed", "error", scrubbed)

		require.NotEmpty(t, buf.String())
		require.NotContains(t, buf.String(), value)
	})
}

// Test_Scrubber_neverLeaksItself covers the scrubber as an object. It holds every
// plaintext an activity resolved, which makes it the worst thing in the package to
// print by accident — worse than a bare Secret.
func Test_Scrubber_neverLeaksItself(t *testing.T) {
	const value = "tok-live-scrubber-9f8e"

	scrubber := NewScrubber(NewSecret(NewRef("env", "T"), value))

	type holder struct{ scrubber *Scrubber }

	subjects := map[string]any{
		"the scrubber":     scrubber,
		"unexported field": holder{scrubber: scrubber},
		"exported field":   struct{ Scrubber *Scrubber }{scrubber},
		"slice":            []*Scrubber{scrubber},
		"map":              map[string]*Scrubber{"a": scrubber},
		"any":              any(scrubber),
	}

	for name, subject := range subjects {
		t.Run(name, func(t *testing.T) {
			for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
				require.NotContains(t, fmt.Sprintf(verb, subject), value,
					"fmt %s of %s leaked a registered value", verb, name)
			}
		})
	}

	t.Run("logging", func(t *testing.T) {
		var buf bytes.Buffer
		slog.New(slog.NewTextHandler(&buf, nil)).Info("resolved", "scrubber", scrubber)

		require.NotEmpty(t, buf.String())
		require.NotContains(t, buf.String(), value)
	})

	t.Run("marshaling", func(t *testing.T) {
		out, err := json.Marshal(scrubber)
		require.NoError(t, err)
		require.NotContains(t, string(out), value)

		text, err := scrubber.MarshalText()
		require.NoError(t, err)
		require.NotContains(t, string(text), value)
	})

	t.Run("the count is reported, not the values", func(t *testing.T) {
		require.Contains(t, scrubber.String(), "secrets.Scrubber(")
		require.NotContains(t, scrubber.String(), value)

		// %#v reaches Format rather than GoString, so call it directly.
		require.Equal(t, scrubber.String(), scrubber.GoString())
	})
}

func Test_Scrubber_Reset(t *testing.T) {
	const value = "tok-live-reset-1a2b"

	scrubber := NewScrubber(NewSecret(NewRef("env", "T"), value))
	require.NotZero(t, scrubber.Len())
	require.NotContains(t, scrubber.Scrub("saw "+value), value)

	scrubber.Reset()

	require.Zero(t, scrubber.Len())
	require.Equal(t, "saw "+value, scrubber.Scrub("saw "+value),
		"a reset scrubber redacts nothing")

	// It remains usable afterwards.
	scrubber.AddValue(value)
	require.NotContains(t, scrubber.Scrub("saw "+value), value)
}

func Test_Scrubber_zeroValue(t *testing.T) {
	// The zero value must be usable rather than a nil-map panic waiting to happen.
	var scrubber Scrubber

	require.Zero(t, scrubber.Len())
	require.Equal(t, "text", scrubber.Scrub("text"))
	require.False(t, scrubber.Contains("text"))
	require.NoError(t, scrubber.ScrubError(nil))

	scrubber.AddValue("secret-value")
	require.NotContains(t, scrubber.Scrub("a secret-value here"), "secret-value")
}

func Test_encodedForms_hexAndCaseVariants(t *testing.T) {
	const value = "sk_live_abc123"

	forms := encodedForms(value)

	require.Contains(t, forms, hex.EncodeToString([]byte(value)), "lowercase hex")
	require.Contains(t, forms, strings.ToUpper(hex.EncodeToString([]byte(value))), "uppercase hex")

	// Go writes %2F where many servers write %2f, and an error quoting a URL back
	// reproduces whichever the server used.
	escaped := url.QueryEscape("a/b c")
	lowered := lowerPercentEscapes(escaped)
	require.NotEqual(t, escaped, lowered)
	require.Equal(t, "a%2fb+c", lowered)

	scrubber := NewScrubber(NewSecret(NewRef("env", "T"), "a/b c"))
	require.NotContains(t, scrubber.Scrub("q=a%2fb+c"), "a%2fb+c")
	require.NotContains(t, scrubber.Scrub("h="+hex.EncodeToString([]byte("a/b c"))), "612f6220")
}

func Test_Scrubber_concurrentUse(t *testing.T) {
	scrubber := NewScrubber()

	// A scrubber may be shared while secrets are still being registered, so both
	// halves must be safe together. Run under -race.
	var wg sync.WaitGroup

	for i := range 32 {
		wg.Go(func() {
			value := fmt.Sprintf("secret-value-%02d", i)
			scrubber.AddValue(value)

			require.NotContains(t, scrubber.Scrub("saw "+value), value)
			_ = scrubber.Len()
			_ = scrubber.Contains("probe")
			_ = scrubber.ScrubError(fmt.Errorf("failed with %s", value))
		})
	}

	wg.Wait()

	require.GreaterOrEqual(t, scrubber.Len(), 32)
}

func Test_encodedForms(t *testing.T) {
	t.Run("the literal value is always first", func(t *testing.T) {
		forms := encodedForms("abc")
		require.Equal(t, "abc", forms[0])
	})

	t.Run("encodings identical to the value are not duplicated", func(t *testing.T) {
		// A value needing no escaping produces the same string for several
		// encodings; registering it repeatedly would be pointless work.
		forms := encodedForms("abc")

		seen := map[string]bool{}
		for _, form := range forms {
			require.False(t, seen[form], "duplicate form %q", form)
			seen[form] = true
		}
	})

	t.Run("a value with special characters yields distinct encodings", func(t *testing.T) {
		forms := encodedForms("a b/c+d=")
		require.Greater(t, len(forms), 1)
		require.Contains(t, forms, url.QueryEscape("a b/c+d="))
		require.Contains(t, forms, base64.StdEncoding.EncodeToString([]byte("a b/c+d=")))
	})

	t.Run("JSON-escaped material is redacted before parsing can reveal it", func(t *testing.T) {
		const value = "pa<ss\twith\"quotes"

		body, err := json.Marshal(map[string]string{"secret": value})
		require.NoError(t, err)
		require.NotContains(t, string(body), value, "the reproducer must exercise an encoded spelling")

		scrubbed := NewScrubber(NewSecret(NewRef("env", "T"), value)).Scrub(string(body))
		var parsed map[string]string
		require.NoError(t, json.Unmarshal([]byte(scrubbed), &parsed))
		require.Equal(t, Redacted, parsed["secret"])
	})
}
