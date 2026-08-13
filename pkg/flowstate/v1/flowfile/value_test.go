package flowfile_test

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// TestNestedBlockScalarInterpolatesLikeAQuotedString covers the one string shape
// that used to escape the expression rule: a block or folded scalar nested inside
// a mapping or a sequence.
//
// A whole-value block scalar has always been checked, because [compiler.scalarString]
// is where a value on its own arrives. One level down — `note: |` inside a `json:`
// mapping — a document takes the literal path instead, where the string arm applied
// the check and the block arm did not. The `${...}` then shipped as characters with
// no diagnostic at all, which is the failure the house rule is about: silently doing
// nothing gives the author no reason to doubt the file.
//
// Since #413 a value mixing text with an expression is interpolation rather than a
// refusal, so what these cases pin turned from "both spellings report the same
// diagnostic" into the stronger claim underneath it, and the one #413 leaves open:
// a block scalar interpolates *identically* to the quoted spelling of the same
// text. Each case is therefore paired with the quoted string holding exactly the
// same characters — including the newline `|` and `>` both keep — and the assertion
// is that the two compile to the same workflow, byte for byte in the schema.
//
// Equality rather than "both compiled" is the point. Two spellings that both
// succeed while building different expressions is precisely the shape of a bug
// nobody would see until a message came out wrong at run time.
func TestNestedBlockScalarInterpolatesLikeAQuotedString(t *testing.T) {
	t.Parallel()

	const quotedInMapping = `edition: v2026.3
name: t
steps:
  - id: a
    log:
      message: one
  - id: post
    http:
      url: https://example.com
      json:
        note: "hello ${steps.a.said}\n"
`

	const quotedInSequence = `edition: v2026.3
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
          - "hello ${steps.a.said}\n"
`

	for _, test := range []struct {
		name    string
		src     string
		like    string // the quoted spelling of the same text, which this must match
		wantErr bool   // the pair is refused rather than compiled, and must be refused alike
	}{
		{
			name: "block scalar in a mapping",
			src: `edition: v2026.3
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
			src: `edition: v2026.3
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
			src: `edition: v2026.3
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
			src: `edition: v2026.3
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
			src: `edition: v2026.3
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
			wantErr: true,
			like: `edition: v2026.3
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

			if test.wantErr {
				// The unterminated half of the old rule survives it: a `${` with
				// no `}` is not literal text and never was, and the two spellings
				// still have to refuse it in the same words.
				_, _, quotedErr := flowfile.Parse([]byte(test.like))
				if quotedErr == nil {
					t.Fatal("the quoted spelling of this value compiled, so there is nothing to compare against")
				}

				_, _, err := flowfile.Parse([]byte(test.src))
				if err == nil {
					t.Fatalf("a block scalar holding an unterminated fence compiled with no diagnostic; the quoted spelling reports:\n%s", quotedErr)
				}

				var ds flowfile.Diagnostics
				if !asDiagnostics(err, &ds) {
					t.Fatalf("Parse() error is %T, want Diagnostics: %v", err, err)
				}
				if len(ds) != 1 {
					t.Fatalf("expected exactly one diagnostic, got %d:\n%s", len(ds), ds.Error())
				}
				for _, want := range []string{"${", "expression"} {
					if !strings.Contains(ds[0].Message, want) {
						t.Errorf("diagnostic does not mention %q; got:\n%s", want, ds[0].Error())
					}
				}
				t.Logf("reported: %s", ds[0].Error())
				return
			}

			quoted, err := flowfile.Unmarshal([]byte(test.like))
			if err != nil {
				t.Fatalf("the quoted spelling did not compile, so there is nothing to compare against: %v", err)
			}

			block, err := flowfile.Unmarshal([]byte(test.src))
			if err != nil {
				t.Fatalf("a block scalar holding an expression did not compile, but the quoted spelling of the same text did: %v", err)
			}

			if !proto.Equal(block, quoted) {
				t.Errorf("a block scalar compiled to something other than its quoted spelling:\n block: %v\nquoted: %v", block, quoted)
			}
		})
	}
}

// TestNestedBlockScalarWithoutAFenceIsLiteralText is the negative direction: the
// check added above must not make prose in a block scalar into an error. A block
// scalar is where a long literal *belongs*.
func TestNestedBlockScalarWithoutAFenceIsLiteralText(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
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
