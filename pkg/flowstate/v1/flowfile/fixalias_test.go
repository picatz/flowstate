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
			src: `edition: v2026.4
name: t
steps:
  - id: a
    log:
      message: &greeting hello
  - id: b
    log:
      message: *greeting
`,
			want: `edition: v2026.4
name: t
steps:
  - id: a
    log:
      message: hello
  - id: b
    log:
      message: hello
`,
			equivalent: `edition: v2026.4
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
			src: `edition: v2026.4
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
			want: `edition: v2026.4
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
			equivalent: `edition: v2026.4
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
			src: `edition: v2026.4
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
			want: `edition: v2026.4
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
			equivalent: `edition: v2026.4
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
			src: `edition: v2026.4
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
			want: `edition: v2026.4
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
			equivalent: `edition: v2026.4
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
			src: `edition: v2026.4
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
			want: `edition: v2026.4
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
			equivalent: `edition: v2026.4
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
			src: `edition: v2026.4
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
			want: `edition: v2026.4
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
			equivalent: `edition: v2026.4
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
			src: `edition: v2026.4
name: t
steps:
  - id: a
    log:
      message: &greeting "hello"
  - id: b
    log:
      message: *greeting # said twice on purpose
`,
			want: `edition: v2026.4
name: t
steps:
  - id: a
    log:
      message: "hello"
  - id: b
    log:
      message: "hello" # said twice on purpose
`,
			equivalent: `edition: v2026.4
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
			src: `edition: v2026.4
name: t
steps:
  - id: a
    log:
      message: &unused hello
`,
			want: `edition: v2026.4
name: t
steps:
  - id: a
    log:
      message: hello
`,
			equivalent: `edition: v2026.4
name: t
steps:
  - id: a
    log:
      message: hello
`,
		},
		{
			// #2102: the anchor's value is a flow-style mapping, `{…}`. spliceScalar
			// used to copy the span spanOfNode computed for it, and eachToken never
			// visits a MappingNode's own tokens at all — so that span ran from the
			// first entry to the last, missing both `{` and `}`.
			name: "a whole-value alias to a flow-style mapping",
			src: `edition: v2026.4
name: t
vars:
  a: &a {x: 1}
  u: *a
steps:
  - id: a
    log:
      message: hi
`,
			want: `edition: v2026.4
name: t
vars:
  a: {x: 1}
  u: {x: 1}
steps:
  - id: a
    log:
      message: hi
`,
			equivalent: `edition: v2026.4
name: t
vars:
  a: {x: 1}
  u: {x: 1}
steps:
  - id: a
    log:
      message: hi
`,
		},
		{
			// #2102's other shape: a flow-style sequence, `[…]`. eachToken visits a
			// SequenceNode's opening token but never a matching closing one, so the
			// computed span kept the `[` and dropped the `]`.
			name: "a whole-value alias to a flow-style sequence",
			src: `edition: v2026.4
name: t
vars:
  a: &a [1, 2]
  u: *a
steps:
  - id: a
    log:
      message: hi
`,
			want: `edition: v2026.4
name: t
vars:
  a: [1, 2]
  u: [1, 2]
steps:
  - id: a
    log:
      message: hi
`,
			equivalent: `edition: v2026.4
name: t
vars:
  a: [1, 2]
  u: [1, 2]
steps:
  - id: a
    log:
      message: hi
`,
		},
		{
			// #2102's acceptance criteria asks for a nested case too, since the gap
			// is structural rather than shape-specific: the outer mapping's own
			// delimiters are what this fix widens the span to, and the nested flow
			// mapping's `{`/`}` come along for free as bytes already inside that
			// range, with no second walk needed to find them.
			name: "a whole-value alias to a nested flow-style mapping",
			src: `edition: v2026.4
name: t
vars:
  a: &a {x: {y: 1}}
  u: *a
steps:
  - id: a
    log:
      message: hi
`,
			want: `edition: v2026.4
name: t
vars:
  a: {x: {y: 1}}
  u: {x: {y: 1}}
steps:
  - id: a
    log:
      message: hi
`,
			equivalent: `edition: v2026.4
name: t
vars:
  a: {x: {y: 1}}
  u: {x: {y: 1}}
steps:
  - id: a
    log:
      message: hi
`,
		},
		{
			name: "a whole-value alias to a nested flow-style sequence",
			src: `edition: v2026.4
name: t
vars:
  a: &a [[1, 2], 3]
  u: *a
steps:
  - id: a
    log:
      message: hi
`,
			want: `edition: v2026.4
name: t
vars:
  a: [[1, 2], 3]
  u: [[1, 2], 3]
steps:
  - id: a
    log:
      message: hi
`,
			equivalent: `edition: v2026.4
name: t
vars:
  a: [[1, 2], 3]
  u: [[1, 2], 3]
steps:
  - id: a
    log:
      message: hi
`,
		},
		{
			// #2102, F1's control direction: the anchor's own declaration is
			// not inside any flow collection here — `o:` opens a block
			// mapping, and `ports:` is a block key of it — even though the
			// anchor's *value* is flow-style. [aliasInliner.anchorInOuterFlow]
			// has to key off the anchor's own site, not off whether the
			// document holds flow style anywhere, or this control would be
			// refused right alongside the case it is meant to be kept
			// distinct from.
			name: "a flow-style anchor declared under a block key keeps inlining",
			src: `edition: v2026.4
name: t
vars:
  o:
    ports: &p [8080:80]
  u: *p
steps:
  - id: a
    log:
      message: hi
`,
			want: `edition: v2026.4
name: t
vars:
  o:
    ports: [8080:80]
  u: [8080:80]
steps:
  - id: a
    log:
      message: hi
`,
			equivalent: `edition: v2026.4
name: t
vars:
  o:
    ports: [8080:80]
  u: [8080:80]
steps:
  - id: a
    log:
      message: hi
`,
		},
		{
			// The delimiter widening this issue's own fix does — see
			// [widenForFlowDelimiters] — has no entries to fold in for an
			// *empty* flow mapping, since [ast.MappingNode] with no Values
			// gives [spanOfNode] nothing to walk and no End to compare
			// against; that used to leave the span's End unset and refuse
			// this with "not written on one line", a diagnostic naming the
			// wrong reason for a value that is very much on one line.
			name: "a whole-value alias to an empty flow-style mapping",
			src: `edition: v2026.4
name: t
vars:
  a: &a {}
  u: *a
steps:
  - id: a
    log:
      message: hi
`,
			want: `edition: v2026.4
name: t
vars:
  a: {}
  u: {}
steps:
  - id: a
    log:
      message: hi
`,
			equivalent: `edition: v2026.4
name: t
vars:
  a: {}
  u: {}
steps:
  - id: a
    log:
      message: hi
`,
		},
		{
			// The counterpart to the trailing-space refusals (#2119): a
			// trailing tab, or a trailing space after a quoted scalar, leaves
			// the parser's column where the value is written, so the copy is
			// the value itself and nothing is refused.
			name: "anchored scalars whose lines end in whitespace the parser positions correctly",
			src: "edition: v2026.4\nname: t\nvars:\n" +
				"  a: &a us-east-1\t\n  b: &b 12\t\n  c: &c \"x y\"\t\n  d: &d 'x y' \n" +
				"  e: *a\n  f: *b\n  g: *c\n  h: *d\n" +
				"steps:\n  - id: a\n    log:\n      message: hi\n",
			want: "edition: v2026.4\nname: t\nvars:\n" +
				"  a: us-east-1\t\n  b: 12\t\n  c: \"x y\"\t\n  d: 'x y' \n" +
				"  e: us-east-1\n  f: 12\n  g: \"x y\"\n  h: 'x y'\n" +
				"steps:\n  - id: a\n    log:\n      message: hi\n",
			equivalent: `edition: v2026.4
name: t
vars:
  a: us-east-1
  b: 12
  c: "x y"
  d: 'x y'
  e: us-east-1
  f: 12
  g: "x y"
  h: 'x y'
steps:
  - id: a
    log:
      message: hi
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
			src: `edition: v2026.4
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
			src: `edition: v2026.4
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
			src: `edition: v2026.4
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
			src: `edition: v2026.4
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
			src: `edition: v2026.4
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
			src: `edition: v2026.4
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
		{
			// #2102, F1: goccy reads a bare `key:value` scalar with no space
			// after the colon differently depending on context — nested
			// inside an outer flow mapping it decodes as a further mapping
			// entry, but beside a block key it is one string. Splicing this
			// anchor's flow-style sequence out from under the outer `{…}`
			// and into a block context would silently change which of those
			// two readings the value gets, so this refuses rather than
			// guess. On origin/main this same input already fails to
			// compile with a parse error rather than accepting anything.
			name: "a flow-style anchor declared inside an outer flow mapping",
			src: `edition: v2026.4
name: t
vars:
  o: {ports: &p [8080:80]}
  u: *p
steps:
  - id: a
    log:
      message: hi
`,
			line:    5,
			column:  6,
			message: "written inside an outer flow collection",
		},
		{
			// A tag above the outer flow mapping hides the anchor from the
			// walk that records what wraps each declaration; its missing
			// entry refuses rather than reading as "no outer flow" and
			// moving `[8080:80]` somewhere it means one string.
			name: "a flow-style anchor inside an outer flow mapping beneath a tag",
			src: `edition: v2026.4
name: t
vars:
  o: !!map
    a: {ports: &p [8080:80]}
  u: *p
steps:
  - id: a
    log:
      message: hi
`,
			line:    6,
			column:  6,
			message: "cannot tell whether an outer flow collection",
		},
		{
			name: "a flow-style anchor's own sequence entries read differently outside a flow mapping",
			src: `edition: v2026.4
name: t
vars:
  o: {k: &p [a:b, c]}
  u: *p
steps:
  - id: a
    log:
      message: hi
`,
			line:    5,
			column:  6,
			message: "written inside an outer flow collection",
		},
		{
			// #2102, F2: goccy reports the column of the token after a tag
			// or a literal tab inside a flow collection one short of where
			// it is actually written, independent of this rewrite's own
			// delimiter-widening logic — so the span this rewrite trusted
			// stopped one byte short of the value's own closing `]`, and
			// [aliasInliner.scalarValueOf]'s own check that the copied
			// bytes actually start and end with the delimiter tokens they
			// should is what catches it.
			name: "a tag before a flow sequence's own element",
			src: `edition: v2026.4
name: t
vars:
  o: &p [!!str 1]
  u: *p
steps:
  - id: a
    log:
      message: hi
`,
			line:    5,
			column:  6,
			message: "not written where it was read",
		},
		{
			// The copied value ends in the inner `]`, so a suffix-only check
			// mistakes it for the outer `]` that goccy's column omitted.
			name: "a tag in a nested flow sequence with a clipped outer delimiter",
			src: `edition: v2026.4
name: t
vars:
  o: &p [[!!str 1]]
  u: *p
steps:
  - id: a
    log:
      message: hi
`,
			line:    5,
			column:  6,
			message: "not written where it was read",
		},
		{
			name: "a tag before a flow mapping's own value",
			src: `edition: v2026.4
name: t
vars:
  o: &p {c: !!str 1}
  u: *p
steps:
  - id: a
    log:
      message: hi
`,
			line:    5,
			column:  6,
			message: "not written where it was read",
		},
		{
			name: "a tag inside a flow sequence nested in a flow mapping",
			src: `edition: v2026.4
name: t
vars:
  o: &p {a: [!!str 1]}
  u: *p
steps:
  - id: a
    log:
      message: hi
`,
			line:    5,
			column:  6,
			message: "not written where it was read",
		},
		{
			name: "a local tag before a flow sequence's own element",
			src: `edition: v2026.4
name: t
vars:
  o: &p [!foo x]
  u: *p
steps:
  - id: a
    log:
      message: hi
`,
			line:    5,
			column:  6,
			message: "not written where it was read",
		},
		{
			name:    "a literal tab as a flow sequence's own leading whitespace",
			src:     "edition: v2026.4\nname: t\nvars:\n  o: &p [\ta]\n  u: *p\nsteps:\n  - id: a\n    log:\n      message: hi\n",
			line:    5,
			column:  6,
			message: "not written where it was read",
		},
		{
			name:    "a literal tab between a flow sequence's own elements",
			src:     "edition: v2026.4\nname: t\nvars:\n  o: &p [a,\tb]\n  u: *p\nsteps:\n  - id: a\n    log:\n      message: hi\n",
			line:    5,
			column:  6,
			message: "not written where it was read",
		},
		{
			// goccy reports a plain scalar's column one to the right for each
			// space trailing it (#2119), which copied `s-east-1 ` here.
			name:    "a trailing space after an anchored plain scalar",
			src:     "edition: v2026.4\nname: t\nvars:\n  region: &r us-east-1 \n  other: *r\nsteps:\n  - id: a\n    log:\n      message: hi\n",
			line:    5,
			column:  10,
			message: "not written where it was read",
		},
		{
			// Copied as `rue `: a boolean silently turned into a string.
			name:    "a trailing space after an anchored boolean",
			src:     "edition: v2026.4\nname: t\nvars:\n  k: &a true \n  u: *a\nsteps:\n  - id: a\n    log:\n      message: hi\n",
			line:    5,
			column:  6,
			message: "not written where it was read",
		},
		{
			name:    "trailing spaces after an anchored integer",
			src:     "edition: v2026.4\nname: t\nvars:\n  k: &a 0x1f  \n  u: *a\nsteps:\n  - id: a\n    log:\n      message: hi\n",
			line:    5,
			column:  6,
			message: "not written where it was read",
		},
		{
			name:    "a space and a tab trailing an anchored plain scalar",
			src:     "edition: v2026.4\nname: t\nvars:\n  k: &a abc \t\n  u: *a\nsteps:\n  - id: a\n    log:\n      message: hi\n",
			line:    5,
			column:  6,
			message: "not written where it was read",
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

// TestFixRefusesARewriteThatChangesWhatTheDocumentMeans pins #2117: copying a
// value's exact bytes is not copying its meaning, because the parser reads a
// plain scalar like `8080:80` as the mapping `{8080: 80}` inside a flow
// mapping and as a string beside a block key. A rewrite whose output decodes
// differently from its input, aliases resolved, is refused whole, with the
// file coming back byte for byte, and one that changes nothing still inlines.
func TestFixRefusesARewriteThatChangesWhatTheDocumentMeans(t *testing.T) {
	t.Parallel()

	refused := []struct {
		name string
		src  string
	}{
		{"a colon scalar inside a flow mapping", "vars:\n  o: {k: &p 8080:80, m: 1}\n  u: *p\n"},
		{"a colon scalar in a later document", "vars:\n  a: 1\n---\nvars:\n  o: {k: &p 8080:80, m: 1}\n  u: *p\n"},
	}
	for _, tt := range refused {
		t.Run("refuses "+tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := flowfile.Fix([]byte(tt.src))
			require.NoError(t, err)

			assert.Equal(t, tt.src, string(result.Source), "a refused file has to come back byte for byte")
			assert.False(t, result.Changed())

			found := false
			for _, refusal := range result.Refusals {
				if strings.Contains(refusal.Message, "change what the document means") {
					found = true
					assert.Positive(t, refusal.Line, "the refusal has to be positioned")
				}
			}
			assert.True(t, found, "no refusal said the rewrite changes meaning; got %v", result.Refusals)
		})
	}

	// A `.nan` decodes to a value that is not equal to itself, which must not
	// make every rewrite of the document look like a change.
	t.Run("a NaN elsewhere in the document does not refuse an unrelated inline", func(t *testing.T) {
		t.Parallel()

		result, err := flowfile.Fix([]byte("vars:\n  a: &p 1\n  u: *p\n  n: .nan\n"))
		require.NoError(t, err)

		assert.Empty(t, result.Refusals)
		assert.Contains(t, string(result.Source), "u: 1")
	})

	// The same text beside a block key reads the same wherever it is copied,
	// so it still inlines.
	t.Run("a colon scalar beside block keys still inlines", func(t *testing.T) {
		t.Parallel()

		result, err := flowfile.Fix([]byte("vars:\n  a: &p 8080:80\n  u: *p\n"))
		require.NoError(t, err)

		assert.Empty(t, result.Refusals)
		assert.Contains(t, string(result.Source), "u: 8080:80")
	})
}

// TestFixRefusesAnAliasExpansionPastTheNodeOrByteBudget is the bound this
// rewrite exists on the wrong side of.
//
// Every other reader in this front end is safe from a billion-laughs document
// because it refuses the construct without following it. This one follows every
// alias, by design — so it is the one place these budgets are load-bearing
// rather than redundant, and the file it refuses is the file it would otherwise
// expand into millions of values or megabytes it was never asked to hold.
//
// Two budgets, because they bound two different resources and a fan-out chain
// can cross either first depending on how much text each level carries (#2045):
// a chain whose levels are wide enough to copy real bytes on every visit
// crosses maxBytes before maxNodes even notices, and #2045's own findings are
// why that has to be true by construction rather than by coincidence — a
// budget that only counted values could not have caught what caused them. The
// second case below is the one shape that still needs maxNodes on its own:
// many small values, referenced once, where nothing multiplies the bytes but
// the sheer count of values is the resource actually at risk.
func TestFixRefusesAnAliasExpansionPastTheNodeOrByteBudget(t *testing.T) {
	t.Parallel()

	t.Run("a wide fan-out crosses the byte budget", func(t *testing.T) {
		t.Parallel()

		// Eleven levels, eight references each: a few hundred bytes that
		// would copy megabytes were every alias followed all the way down.
		// The shape is the point — its alias *depth* is one per level, which
		// is why a depth bound cannot see it and these budgets can.
		var b strings.Builder
		b.WriteString("edition: v2026.4\nname: t\nvars:\n  level0: &level0\n    x: 1\n    y: 2\n")
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
		assert.Contains(t, result.Refusals[0].Message, fmt.Sprintf("more than %d bytes", maxFlowfileBytes),
			"this shape's fan-out copies enough text to cross the byte budget before the node budget ever sees it")
	})

	t.Run("many small values referenced once crosses the node budget", func(t *testing.T) {
		t.Parallel()

		// One anchor, one alias, no fan-out at all — every value this would
		// write out is the same value the source already holds, once, so
		// nothing here multiplies bytes the way the fan-out above does. What
		// crosses is the sheer count of values a single expansion holds,
		// which is exactly the resource the node budget and not the byte
		// budget is the answer to.
		//
		// 20,000 rather than a rounder, larger count: the document's own
		// declared entries alone hold about 60,000 nodes, comfortably under
		// maxNodes, so it is specifically the one alias use doubling that
		// count that crosses it — not the static document by itself, which
		// a far larger entry count would also refuse and say nothing about
		// the expansion.
		const entries = 20_000
		var b strings.Builder
		b.WriteString("edition: v2026.4\nname: t\nvars:\n  big: &big\n")
		for i := range entries {
			fmt.Fprintf(&b, "    a%d: 1\n", i)
		}
		b.WriteString("  use: *big\n")
		b.WriteString("steps:\n  - id: a\n    log:\n      message: hi\n")
		src := b.String()

		require.Less(t, len(src), maxFlowfileBytes,
			"the input on its own has to fit under the byte budget, so the refusal below is the node budget's and not the read cap's")

		result, err := flowfile.Fix([]byte(src))
		require.NoError(t, err)

		assert.Equal(t, src, string(result.Source), "the file has to come back byte for byte")
		assert.False(t, result.Complete())
		require.NotEmpty(t, result.Refusals)
		assert.Contains(t, result.Refusals[0].Message, "more than 100000 values")
	})
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

	src := `edition: v2026.4
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
	want := `edition: v2026.4
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

// TestFixDropsSeveralAnchorMarkersOnOneLine is #2106's two reproductions.
//
// The original single-anchor `dropMarker` removed each anchor's `&name`
// marker one at a time, in [aliasInliner.anchorNodes]'s own (left-to-right,
// document) order. Each removal edited the line it was on in place, so a
// second anchor further right was then located by the *original* column the
// parser read, which the first removal had already shifted left.
//
// The first case is the shape that used to fail safe: the shifted offset does
// not hold "&b", so the rewrite refused rather than guessed. It is included
// here as the positive direction of the same fix, since finding and removing
// every marker on the line together, through [dropMarkersFromLine], resolves
// it correctly instead of merely refusing it.
//
// The second case is the shape that did not fail safe: the anchor `&b`'s
// value is the quoted string `"&b"`, chosen so that once `&aa`'s marker
// shifts the line, `&b`'s stale column lands inside those quotes rather than
// past the end of the line — and the text there happens to read `&b` too, so
// the old code deleted it instead of refusing. That corrupts the document
// silently: `flow fix` reports success on a file that no longer holds the
// value the author wrote, and the real `&b` marker survives to be removed on
// the fixed-point loop's next round, which is what let the corruption through
// unnoticed by [FixResult.Refusals].
func TestFixDropsSeveralAnchorMarkersOnOneLine(t *testing.T) {
	t.Parallel()

	t.Run("two bare anchors sharing a line", func(t *testing.T) {
		t.Parallel()

		src := `edition: v2026.4
name: t
x: [&a 1, &b 2]
steps:
  - id: a
    log:
      message: hi
`
		want := `edition: v2026.4
name: t
x: [1, 2]
steps:
  - id: a
    log:
      message: hi
`

		result, err := flowfile.Fix([]byte(src))
		require.NoError(t, err)
		require.Empty(t, result.Refusals, "both anchors should be removable now that dropMarkersFromLine finds and removes them together")
		require.True(t, result.Complete())
		assert.Equal(t, want, string(result.Source))
	})

	t.Run("a marker's own text does not delete a look-alike inside a quoted value", func(t *testing.T) {
		t.Parallel()

		// &b's value is the quoted string "&b" — chosen so that removing &aa's
		// marker first shifts &b's stale column onto that quoted text rather
		// than off the end of the line, which is what let the old left-to-right
		// removal mistake the quoted bytes for the marker instead of refusing.
		src := `edition: v2026.4
name: t
x: [&aa 1, &b "&b"]
steps:
  - id: a
    log:
      message: hi
`
		want := `edition: v2026.4
name: t
x: [1, "&b"]
steps:
  - id: a
    log:
      message: hi
`

		result, err := flowfile.Fix([]byte(src))
		require.NoError(t, err)
		require.Empty(t, result.Refusals)
		require.True(t, result.Complete())
		assert.Equal(t, want, string(result.Source),
			"the quoted value \"&b\" must survive removing &aa's marker on the same line")
	})
}

func TestFixPreservesTabAfterTerminalAnchorMarker(t *testing.T) {
	t.Parallel()

	src := "edition: v2026.4\nname: t\nvars:\n  x: &request\t\n    url: https://example.com\nsteps:\n  - id: a\n    log:\n      message: hi\n"
	want := "edition: v2026.4\nname: t\nvars:\n  x: \t\n    url: https://example.com\nsteps:\n  - id: a\n    log:\n      message: hi\n"

	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.Empty(t, result.Refusals)
	require.True(t, result.Complete())
	assert.Equal(t, want, string(result.Source), "the marker's removal must not consume the tab")
}
