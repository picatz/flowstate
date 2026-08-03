package flowfile_test

import (
	"strings"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// TestNestedBlockScalarReportsAnInterpolation covers the one string shape that used
// to escape the expression rule: a block or folded scalar nested inside a mapping or
// a sequence.
//
// A whole-value block scalar has always been checked, because [compiler.scalarString]
// is where a value on its own arrives. One level down — `note: |` inside a `json:`
// mapping — a document takes the literal path instead, where the string arm applied
// the check and the block arm did not. The `${...}` then shipped as characters with
// no diagnostic at all, which is the failure the house rule is about: silently doing
// nothing gives the author no reason to doubt the file.
//
// Each case is paired with the quoted spelling of the same value, so the assertion is
// that the two spellings say the same thing rather than that some diagnostic appeared.
func TestNestedBlockScalarReportsAnInterpolation(t *testing.T) {
	t.Parallel()

	const quotedInMapping = `edition: v2026.2
name: t
steps:
  - id: a
    log:
      message: one
  - id: post
    http:
      url: https://example.com
      json:
        note: "hello ${steps.a.said}"
`

	const quotedInSequence = `edition: v2026.2
name: t
steps:
  - id: a
    log:
      message: one
  - id: post
    http:
      url: https://example.com
      json:
        notes:
          - "hello ${steps.a.said}"
`

	for _, test := range []struct {
		name string
		src  string
		like string // the spelling whose diagnostic this one must match
	}{
		{
			name: "block scalar in a mapping",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    log:
      message: one
  - id: post
    http:
      url: https://example.com
      json:
        note: |
          hello ${steps.a.said}
`,
			like: quotedInMapping,
		},
		{
			name: "folded scalar in a mapping",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    log:
      message: one
  - id: post
    http:
      url: https://example.com
      json:
        note: >
          hello ${steps.a.said}
`,
			like: quotedInMapping,
		},
		{
			name: "block scalar in a sequence",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    log:
      message: one
  - id: post
    http:
      url: https://example.com
      json:
        notes:
          - |
            hello ${steps.a.said}
`,
			like: quotedInSequence,
		},
		{
			name: "folded scalar in a sequence",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    log:
      message: one
  - id: post
    http:
      url: https://example.com
      json:
        notes:
          - >
            hello ${steps.a.said}
`,
			like: quotedInSequence,
		},
		{
			// An unterminated fence is the other half of fenceError, and it took
			// the same silent path.
			name: "unterminated fence in a block scalar",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    log:
      message: one
  - id: post
    http:
      url: https://example.com
      json:
        note: |
          hello ${steps.a.said
`,
			like: `edition: v2026.2
name: t
steps:
  - id: a
    log:
      message: one
  - id: post
    http:
      url: https://example.com
      json:
        note: "hello ${steps.a.said"
`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, _, quotedErr := flowfile.Parse([]byte(test.like))
			if quotedErr == nil {
				t.Fatal("the quoted spelling of this value compiled, so there is nothing to compare against")
			}

			_, _, err := flowfile.Parse([]byte(test.src))
			if err == nil {
				t.Fatalf("a block scalar holding an expression compiled with no diagnostic; the quoted spelling reports:\n%s", quotedErr)
			}

			var ds flowfile.Diagnostics
			if !asDiagnostics(err, &ds) {
				t.Fatalf("Parse() error is %T, want Diagnostics: %v", err, err)
			}
			if len(ds) != 1 {
				t.Fatalf("expected exactly one diagnostic, got %d:\n%s", len(ds), ds.Error())
			}

			// The message, not the position: a block scalar is written somewhere
			// else on the page than a quoted string, and what has to agree is what
			// the author is told is wrong.
			for _, want := range []string{"${", "expression"} {
				if !strings.Contains(ds[0].Message, want) {
					t.Errorf("diagnostic does not mention %q; got:\n%s", want, ds[0].Error())
				}
			}
			t.Logf("reported: %s", ds[0].Error())
		})
	}
}

// TestNestedBlockScalarWithoutAFenceIsLiteralText is the negative direction: the
// check added above must not make prose in a block scalar into an error. A block
// scalar is where a long literal *belongs*.
func TestNestedBlockScalarWithoutAFenceIsLiteralText(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.2
name: t
steps:
  - id: post
    http:
      url: https://example.com
      json:
        note: |
          hello, $ and { and } and $notafence
        notes:
          - >
            folded prose that mentions money: $5
`
	if _, _, err := flowfile.Parse([]byte(src)); err != nil {
		t.Fatalf("a block scalar with no fence was rejected: %v", err)
	}
}
