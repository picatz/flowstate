package flowfile_test

import (
	"cmp"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// These cases hold `flow fmt` to what an author wrote: a short list stays on
// one line, a task's inputs come out in the order `flow tasks` lists them, and
// an interpolated string comes back as the string it was. Each is asserted in
// both directions (the form that must be kept, and the form that must not be
// produced), as bytes, as a fixed point, and as the same compiled workflow,
// because a formatter that rewrites meaning-bearing layout is the failure and
// a formatter that merely stays quiet is not the fix.
func TestFormatKeepsWhatTheAuthorWrote(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		src  string
		want string // the source itself when empty
	}{
		{
			name: "a short flow sequence stays on its line",
			src: `edition: v2026.4
name: w
steps:
  - id: build
    exec:
      argv: [make, build]
`,
		},
		{
			name: "a short block sequence is written on one line, so there is one form",
			src: `edition: v2026.4
name: w
steps:
  - id: build
    exec:
      argv:
        - make
        - build
`,
			want: `edition: v2026.4
name: w
steps:
  - id: build
    exec:
      argv: [make, build]
`,
		},
		{
			name: "a sequence too wide for a line stays a block",
			src: `edition: v2026.4
name: w
steps:
  - id: build
    exec:
      argv:
        - go-build-everything
        - --with-a-very-long-flag-name=1
        - --and-another-quite-long-flag=2
`,
		},
		{
			name: "text that needs quoting or holds a fence stays a block",
			src: `edition: v2026.4
name: w
inputs:
  who:
    type: string
    default: ada
steps:
  - id: build
    exec:
      argv:
        - echo
        - a, b
        - ${inputs.who}
`,
		},
		{
			name: "a sequence of mappings stays a block",
			src: `edition: v2026.4
name: w
steps:
  - id: call
    http:
      url: https://example.com
      json:
        - id: 1
        - id: 2
`,
		},
		{
			name: "a comment inside a short sequence keeps it a block, and the comment",
			src: `edition: v2026.4
name: w
steps:
  - id: build
    exec:
      argv:
        # the tool
        - make
        - build # the target
`,
		},
		{
			name: "a comment above the key leaves the sequence on its line",
			src: `edition: v2026.4
name: w
steps:
  - id: build
    exec:
      # what to run
      argv: [make, build]
`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertFormats(t, tc.src, cmp.Or(tc.want, tc.src))
		})
	}
}

func TestFormatOrdersTaskInputsLikeTheSchema(t *testing.T) {
	t.Parallel()

	// Written two ways that mean one thing; both must come out as the order
	// `flow tasks` lists, which is required inputs first and then the schema's
	// own order. Alphabetical would put `headers` and `method` above `url`.
	const canonical = `edition: v2026.4
name: w
steps:
  - id: fetch
    http:
      url: https://example.com
      method: POST
      headers:
        accept: application/json
      parse_json: true
`

	for _, src := range []string{
		canonical,
		`edition: v2026.4
name: w
steps:
  - id: fetch
    http:
      parse_json: true
      headers:
        accept: application/json
      method: POST
      url: https://example.com
`,
	} {
		assertFormats(t, src, canonical)
	}
}

func TestFormatKeepsInterpolationAsWritten(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		src  string
		want string
	}{
		{
			name: "interpolated text stays interpolated",
			src: `edition: v2026.4
name: w
vars:
  svc: api
steps:
  - id: build
    exec:
      argv: [env]
      env:
        - SERVICE=${vars.svc}
`,
		},
		{
			name: "the hand-written concatenation it compiles to is the same value, so it is written the same way",
			src: `edition: v2026.4
name: w
vars:
  svc: api
steps:
  - id: build
    log:
      message: ${"service " + string(vars.svc) + " built"}
`,
			want: `edition: v2026.4
name: w
vars:
  svc: api
steps:
  - id: build
    log:
      message: service ${vars.svc} built
`,
		},
		{
			name: "inside a mapping the keys and the interpolation both stay",
			src: `edition: v2026.4
name: w
vars:
  token: t
steps:
  - id: call
    http:
      url: https://example.com
      headers:
        authorization: Bearer ${vars.token}
`,
		},
		{
			name: "a concatenation that is not string() of each fence is left alone",
			src: `edition: v2026.4
name: w
vars:
  svc: api
steps:
  - id: build
    log:
      message: ${"service " + vars.svc}
`,
		},
		{
			name: "a lone conversion is a conversion, not an interpolation",
			src: `edition: v2026.4
name: w
vars:
  count: 1
steps:
  - id: build
    log:
      message: ${string(vars.count)}
`,
		},
		{
			name: "an escaped fence stays escaped",
			src: `edition: v2026.4
name: w
vars:
  svc: api
steps:
  - id: build
    log:
      message: literal $${not} then ${vars.svc}
`,
		},
		{
			name: "an expression field is source, so it never takes the interpolated spelling",
			src: `edition: v2026.4
name: w
vars:
  count: 1
steps:
  - id: pick
    switch:
      value: ${"v" + string(vars.count)}
      cases:
        - case: v1
          steps:
            - id: one
              log:
                message: one
`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertFormats(t, tc.src, cmp.Or(tc.want, tc.src))
		})
	}
}

// assertFormats holds one source to the three things a formatter owes: the
// bytes it was asked for, a second run that changes nothing, and the same
// compiled workflow before and after.
func assertFormats(t *testing.T, src, want string) {
	t.Helper()

	once := formatFile(t, src)
	assert.Equal(t, want, once)
	assert.Equal(t, once, formatFile(t, once), "formatting is not a fixed point")

	before, err := flowfile.Unmarshal([]byte(src))
	require.NoError(t, err)
	after, err := flowfile.Unmarshal([]byte(once))
	require.NoError(t, err)
	assert.True(t, proto.Equal(before, after), "formatting changed the workflow the file compiles to")
}

// A literal list of doubles is a plain-scalar list like any other.
func TestFormatWritesShortFloatListsOnOneLine(t *testing.T) {
	t.Parallel()

	assertFormats(t, `edition: v2026.4
name: w
vars:
  weights:
    - 1.5
    - 2.5
steps:
  - id: show
    log:
      message: ${string(vars.weights)}
`, `edition: v2026.4
name: w
vars:
  weights: [1.5, 2.5]
steps:
  - id: show
    log:
      message: ${string(vars.weights)}
`)
}

// Deciding whether a comment needs a sequence's lines to be a block must not
// cost the whole file's comments per sequence (invariant 5): many short lists
// beside many comments format in time linear in the file.
func TestFormatWithManyListsAndCommentsIsBounded(t *testing.T) {
	t.Parallel()

	var src strings.Builder
	src.WriteString("edition: v2026.4\nname: w\nsteps:\n")
	for i := range 3000 {
		fmt.Fprintf(&src, "  # step %d\n  - id: s%d\n    exec:\n      argv: [echo, a%d]\n", i, i, i)
	}
	start := time.Now()
	_ = formatFile(t, src.String())
	assert.Less(t, time.Since(start), 20*time.Second)
}
