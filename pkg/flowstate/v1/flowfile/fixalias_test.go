package flowfile_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `flow fix` inlining a whole-value alias is the migration path across a refusal
// the grammar makes permanently, so what these tests have to hold is stronger than
// "the output is legal".
//
// This repository has corrupted a valid file with `flow fix` twice, and both times
// the test that let it through asserted the output still validates. A rewrite that
// produces a *different but still legal* document passes that assertion and fails
// the author. So every case here asserts the exact output bytes, and then compiles
// the result and compares it against the same workflow written out by hand with no
// alias in it at all — spelling and meaning, checked separately, because the two
// failures look nothing alike.

// aliasCase is one file the rewrite acts on: what was written, the bytes that come
// back, and the alias-free file it is supposed to mean.
type aliasCase struct {
	name string

	// src holds anchors and aliases, so this build's compiler refuses it.
	src string

	// want is the exact output. Compared byte for byte, which is the contract:
	// everything the rewrite did not have to touch is copied through.
	want string

	// equivalent is the same workflow somebody would have written without ever
	// using an anchor. Compiled and compared against want's compiled form, which
	// is the assertion that the rewrite changed spelling and nothing else.
	equivalent string
}

func aliasCases() []aliasCase {
	return []aliasCase{
		{
			name: "a whole-value alias to a scalar",
			src: `edition: v2026.3
name: t
steps:
  - id: a
    log:
      message: &greeting hello
  - id: b
    log:
      message: *greeting
`,
			want: `edition: v2026.3
name: t
steps:
  - id: a
    log:
      message: hello
  - id: b
    log:
      message: hello
`,
			equivalent: `edition: v2026.3
name: t
steps:
  - id: a
    log:
      message: hello
  - id: b
    log:
      message: hello
`,
		},
		{
			// The anchored value is a mapping, so the replacement is a block of
			// lines under the key rather than a splice into it — and the comment
			// written among those lines travels with them, which is the property a
			// span-derived copy loses.
			name: "an anchor whose value is a mapping, copied with its comments",
			src: `edition: v2026.3
name: t
steps:
  - id: a
    http: &request
      url: https://example.com
      # the one the upstream team asked for
      method: GET
  - id: b
    http: *request
`,
			want: `edition: v2026.3
name: t
steps:
  - id: a
    http:
      url: https://example.com
      # the one the upstream team asked for
      method: GET
  - id: b
    http:
      url: https://example.com
      # the one the upstream team asked for
      method: GET
`,
			equivalent: `edition: v2026.3
name: t
steps:
  - id: a
    http:
      url: https://example.com
      method: GET
  - id: b
    http:
      url: https://example.com
      method: GET
`,
		},
		{
			name: "an anchor whose value is a sequence",
			src: `edition: v2026.3
name: t
vars:
  primary: &hosts
    - alpha
    - beta
  standby: *hosts
steps:
  - id: a
    log:
      message: hi
`,
			want: `edition: v2026.3
name: t
vars:
  primary:
    - alpha
    - beta
  standby:
    - alpha
    - beta
steps:
  - id: a
    log:
      message: hi
`,
			equivalent: `edition: v2026.3
name: t
vars:
  primary:
    - alpha
    - beta
  standby:
    - alpha
    - beta
steps:
  - id: a
    log:
      message: hi
`,
		},
		{
			// Three uses of one anchor, at three different indentations, one of
			// them nested inside another mapping. The copy is re-indented to where
			// it lands and nowhere else.
			name: "several aliases to one anchor, nested at different depths",
			src: `edition: v2026.3
name: t
vars:
  defaults: &defaults
    region: us-east-1
    tier: gold
  primary: *defaults
  standby:
    settings: *defaults
steps:
  - id: a
    log:
      message: hi
      fields: *defaults
`,
			want: `edition: v2026.3
name: t
vars:
  defaults:
    region: us-east-1
    tier: gold
  primary:
    region: us-east-1
    tier: gold
  standby:
    settings:
      region: us-east-1
      tier: gold
steps:
  - id: a
    log:
      message: hi
      fields:
        region: us-east-1
        tier: gold
`,
			equivalent: `edition: v2026.3
name: t
vars:
  defaults:
    region: us-east-1
    tier: gold
  primary:
    region: us-east-1
    tier: gold
  standby:
    settings:
      region: us-east-1
      tier: gold
steps:
  - id: a
    log:
      message: hi
      fields:
        region: us-east-1
        tier: gold
`,
		},
		{
			// An anchor whose own value holds an alias. One pass settles the whole
			// chain, because the copy of the outer value is taken *after* the inner
			// alias in it has been written out.
			name: "an alias chain",
			src: `edition: v2026.3
name: t
vars:
  base: &base
    region: us-east-1
  full: &full
    settings: *base
    tier: gold
  copy: *full
steps:
  - id: a
    log:
      message: hi
`,
			want: `edition: v2026.3
name: t
vars:
  base:
    region: us-east-1
  full:
    settings:
      region: us-east-1
    tier: gold
  copy:
    settings:
      region: us-east-1
    tier: gold
steps:
  - id: a
    log:
      message: hi
`,
			equivalent: `edition: v2026.3
name: t
vars:
  base:
    region: us-east-1
  full:
    settings:
      region: us-east-1
    tier: gold
  copy:
    settings:
      region: us-east-1
    tier: gold
steps:
  - id: a
    log:
      message: hi
`,
		},
		{
			// A list item, where the value goes beside the dash rather than under a
			// key. The two shapes are the whole of what this rewrite splices into,
			// and they indent differently.
			name: "an alias as a whole list item",
			src: `edition: v2026.3
name: t
vars:
  primary: &host
    name: alpha
    port: 8080
  pool:
    - *host
    - name: beta
      port: 9090
steps:
  - id: a
    log:
      message: hi
`,
			want: `edition: v2026.3
name: t
vars:
  primary:
    name: alpha
    port: 8080
  pool:
    - name: alpha
      port: 8080
    - name: beta
      port: 9090
steps:
  - id: a
    log:
      message: hi
`,
			equivalent: `edition: v2026.3
name: t
vars:
  primary:
    name: alpha
    port: 8080
  pool:
    - name: alpha
      port: 8080
    - name: beta
      port: 9090
steps:
  - id: a
    log:
      message: hi
`,
		},
		{
			// The alias's own line carries a trailing comment, and the anchor's
			// value is a scalar written with quotes it did not need. Both survive:
			// the comment because the splice is into the line rather than a rebuild
			// of it, the quoting because the value's own source bytes are copied
			// rather than re-rendered.
			name: "a comment and a hand-chosen quoting both survive",
			src: `edition: v2026.3
name: t
steps:
  - id: a
    log:
      message: &greeting "hello"
  - id: b
    log:
      message: *greeting # said twice on purpose
`,
			want: `edition: v2026.3
name: t
steps:
  - id: a
    log:
      message: "hello"
  - id: b
    log:
      message: "hello" # said twice on purpose
`,
			equivalent: `edition: v2026.3
name: t
steps:
  - id: a
    log:
      message: "hello"
  - id: b
    log:
      message: "hello"
`,
		},
		{
			// An anchor nothing refers to still has to go: the marker itself is not
			// part of the grammar, so a file that kept one would be a file `flow
			// validate` refuses after `flow fix` reported success.
			name: "an anchor with no alias to it",
			src: `edition: v2026.3
name: t
steps:
  - id: a
    log:
      message: &unused hello
`,
			want: `edition: v2026.3
name: t
steps:
  - id: a
    log:
      message: hello
`,
			equivalent: `edition: v2026.3
name: t
steps:
  - id: a
    log:
      message: hello
`,
		},
	}
}

// TestFixInlinesWholeValueAliases is the byte assertion, one case per shape.
func TestFixInlinesWholeValueAliases(t *testing.T) {
	t.Parallel()

	for _, tt := range aliasCases() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// The premise: this build's compiler refuses the input. Asserted rather
			// than assumed, because a fixture that quietly stopped holding an alias
			// would turn every assertion below into a test of nothing.
			_, _, err := flowfile.Parse([]byte(tt.src))
			require.Error(t, err, "the fixture is supposed to hold a construct the grammar refuses")

			result, err := flowfile.Fix([]byte(tt.src))
			require.NoError(t, err)
			require.Empty(t, result.Refusals, "nothing here should be refused")
			require.True(t, result.Complete())
			require.True(t, result.Changed())

			assert.Equal(t, tt.want, string(result.Source))
		})
	}
}

// TestFixInlinedAliasesMeanWhatTheyDid compiles the rewritten file and the same
// workflow written out by hand, and compares the protos.
//
// Byte equality above is the rewrite's contract; this is the meaning's. A rewrite
// that produced a legal document computing something else — the failure mode that
// got past `flow fix`'s tests twice — passes a "still validates" assertion and
// fails this one.
func TestFixInlinedAliasesMeanWhatTheyDid(t *testing.T) {
	t.Parallel()

	for _, tt := range aliasCases() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := flowfile.Fix([]byte(tt.src))
			require.NoError(t, err)
			require.True(t, result.Complete())

			rewritten, _, err := flowfile.Parse(result.Source)
			require.NoError(t, err, "the rewritten file has to compile")

			byHand, _, err := flowfile.Parse([]byte(tt.equivalent))
			require.NoError(t, err, "the hand-written equivalent has to compile")

			assert.True(t, proto.Equal(rewritten, byHand),
				"the rewritten file compiles to something other than the same workflow written without an alias:\n%v\n%v",
				rewritten, byHand)
		})
	}
}

// TestFixInlinedOutputIsAcceptedByValidate is the property `flow fix` exists to
// hold, over every fixture here.
//
// Exiting zero has to imply `flow validate` accepts the result. The alternative —
// `flow fix . && git commit` succeeding on a file the validator then rejects — is
// the outcome this command's own doc comment names as the one it exists to avoid,
// and inlining is the pass most able to produce it, because it writes bytes the
// author never wrote.
func TestFixInlinedOutputIsAcceptedByValidate(t *testing.T) {
	t.Parallel()

	for _, tt := range aliasCases() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := flowfile.Fix([]byte(tt.src))
			require.NoError(t, err)
			require.True(t, result.Complete(), "this fixture is supposed to be rewritable")

			diagnostics, err := flowfile.ValidateSource(result.Source)
			require.NoError(t, err)
			assert.Empty(t, diagnostics, "flow fix exited zero on a file flow validate refuses")
		})
	}
}

// TestFixInlinedOutputIsIdempotent runs the rewrite over its own output.
//
// The fixed-point loop in [flowfile.Fix] rests on every rule making progress
// toward a document it no longer changes, and a rewrite that rewrote its own
// output would spin to the round bound and refuse a file it had just fixed. Bytes
// again, not "it still validates": a second pass that reformatted something would
// pass the weaker assertion.
func TestFixInlinedOutputIsIdempotent(t *testing.T) {
	t.Parallel()

	for _, tt := range aliasCases() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := flowfile.Fix([]byte(tt.src))
			require.NoError(t, err)
			require.True(t, result.Complete())

			again, err := flowfile.Fix(result.Source)
			require.NoError(t, err)
			require.True(t, again.Complete())
			assert.False(t, again.Changed(), "fixing the fixed output changed it again")
			assert.Equal(t, string(result.Source), string(again.Source))
		})
	}
}

// TestFixRefusesWhatItCannotInlineByteForByte covers the other half, which is the
// half that keeps the command safe to run on anything.
//
// Every case asserts the output is the input, byte for byte. A rewrite that
// half-applied — some aliases written out, some anchors still declared — would
// leave a document where a surviving alias names an anchor that is gone, which is
// worse than the file it started from.
func TestFixRefusesWhatItCannotInlineByteForByte(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string

		// line and column are asserted because a diagnostic that cannot be
		// navigated to is prose, per this package's diagnostics standard.
		line, column int
		message      string
	}{
		{
			// The decision this issue turned on. `<<:` followed by sibling keys is
			// a precedence rule, and reproducing it means the rewriter deciding
			// which spelling of a key the author meant — judgment, which `flow fix`
			// does not exercise. Refused in the compiler's own words.
			name: "a merge key",
			src: `edition: v2026.3
name: t
vars:
  base: &base
    region: us-east-1
  merged:
    <<: *base
    region: eu-west-1
steps:
  - id: a
    log:
      message: hi
`,
			line:    7,
			column:  5,
			message: "a merge key (`<<:`) is not part of the Flowfile grammar",
		},
		{
			name: "an anchor that reaches itself",
			src: `edition: v2026.3
name: t
vars:
  cycle: &cycle
    inner: *cycle
steps:
  - id: a
    log:
      message: hi
`,
			line:    5,
			column:  12,
			message: "reaches itself through this alias",
		},
		{
			name: "an alias inside flow style",
			src: `edition: v2026.3
name: t
vars:
  base: &base 1
  flow: {value: *base}
steps:
  - id: a
    log:
      message: hi
`,
			line:    5,
			column:  17,
			message: "written in flow style",
		},
		{
			name: "an alias naming an anchor the document does not declare",
			src: `edition: v2026.3
name: t
steps:
  - id: a
    log:
      message: *missing
`,
			line:    6,
			column:  16,
			message: "does not declare",
		},
		{
			name: "the same anchor name declared twice",
			src: `edition: v2026.3
name: t
vars:
  first: &shared 1
  second: &shared 2
  third: *shared
steps:
  - id: a
    log:
      message: hi
`,
			line:    5,
			column:  11,
			message: "is declared more than once",
		},
		{
			name: "an anchored value that declares an anchor of its own",
			src: `edition: v2026.3
name: t
vars:
  outer: &outer
    inner: &inner 1
  copy: *outer
steps:
  - id: a
    log:
      message: hi
`,
			line:    6,
			column:  9,
			message: "declares an anchor of its own",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := flowfile.Fix([]byte(tt.src))
			require.NoError(t, err)

			assert.Equal(t, tt.src, string(result.Source), "a refused file has to come back byte for byte")
			assert.False(t, result.Complete())
			assert.False(t, result.Changed())

			require.NotEmpty(t, result.Refusals)
			found := false
			for _, refusal := range result.Refusals {
				if strings.Contains(refusal.Message, tt.message) {
					found = true
					assert.Equal(t, tt.line, refusal.Line)
					assert.Equal(t, tt.column, refusal.Column)
				}
			}
			assert.True(t, found, "no refusal said %q; got %v", tt.message, result.Refusals)
		})
	}
}

// TestFixRefusesAnAliasExpansionPastTheNodeBudget is the bound this rewrite exists
// on the wrong side of.
//
// Every other reader in this front end is safe from a billion-laughs document
// because it refuses the construct without following it. This one follows every
// alias, by design — so it is the one place the total-node budget is load-bearing
// rather than redundant, and the file it refuses is the file it would otherwise
// expand into millions of values.
func TestFixRefusesAnAliasExpansionPastTheNodeBudget(t *testing.T) {
	t.Parallel()

	// Eleven levels, eight references each: a few hundred bytes that name more
	// values than there are atoms worth counting. The shape is the point — its
	// alias *depth* is one per level, which is why a depth bound cannot see it and
	// the node budget can.
	var b strings.Builder
	b.WriteString("edition: v2026.3\nname: t\nvars:\n  level0: &level0\n    x: 1\n    y: 2\n")
	for level := 1; level <= 11; level++ {
		fmt.Fprintf(&b, "  level%d: &level%d\n", level, level)
		for use := range 8 {
			fmt.Fprintf(&b, "    use%d: *level%d\n", use, level-1)
		}
	}
	b.WriteString("steps:\n  - id: a\n    log:\n      message: hi\n")
	src := b.String()

	require.Less(t, len(src), 2048, "the input is supposed to be small; the expansion is what is not")

	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)

	assert.Equal(t, src, string(result.Source), "the file has to come back byte for byte")
	assert.False(t, result.Complete())
	require.NotEmpty(t, result.Refusals)
	assert.Contains(t, result.Refusals[0].Message, "more than 100000 values")
}

// TestFixLeavesAnAsteriskInsideAScalarAlone is the negative direction of "whole
// value".
//
// `message: hello *who` is a plain scalar that happens to hold an asterisk, and
// `"hi *who"` is a quoted one. Neither is an alias — YAML only reads `*` as one at
// the head of a node — and a rewriter that matched on the text rather than on what
// the parser built would rewrite both into somebody else's value. Which is the
// shape of every corruption `flow fix` has managed: knowing less about the grammar
// than the grammar does.
func TestFixLeavesAnAsteriskInsideAScalarAlone(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
steps:
  - id: a
    log:
      message: &who world
  - id: b
    log:
      message: hello *who
  - id: c
    log:
      message: "hi *who"
`
	want := `edition: v2026.3
name: t
steps:
  - id: a
    log:
      message: world
  - id: b
    log:
      message: hello *who
  - id: c
    log:
      message: "hi *who"
`

	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.True(t, result.Complete())

	assert.Equal(t, want, string(result.Source))
}
