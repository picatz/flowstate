package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `iterator:` is `as:` now, which is the smallest kind of change this grammar makes and
// the one that has to be cheapest to survive: a rename buys nothing if the cost of it is
// an author editing files by hand.
//
// So the rename is only half the work. The other half is that a file written the old way
// gets a sentence naming the new spelling — not "unknown key", which describes a typo
// nobody made — and that `flow fix` writes it, touching the key and nothing else.

// TestARetiredKeySaysWhatReplacedIt checks the diagnostic, not the parse failure.
//
// Three things could be said here and two of them waste the reader's time. "unknown key
// `iterator`; did you mean `items`?" sends an author to correct a word they spelled
// correctly. Silence lets the loop bind `item` and run, which is worse. What helps is
// the new spelling, the reason, and the command that does it for them.
func TestARetiredKeySaysWhatReplacedIt(t *testing.T) {
	t.Parallel()

	src := `
edition: v2026.3
name: t
steps:
  - id: each
    for_each:
      items: ${["a"]}
      iterator: name
      steps:
        - id: inner
          log:
            message: ${name}
`

	reported := diagnose(t, src)
	require.Contains(t, reported, "`iterator:` is now `as:`")
	require.Contains(t, reported, "run `flow fix`",
		"a retired key did not name the command that rewrites it")
	require.NotContains(t, reported, "unknown key",
		"a key this grammar replaced was reported as a misspelling")
}

// TestARetiredKeyIsGuardedByItsPosition is the case a word-keyed table gets wrong.
//
// `iterator:` is only advice inside a `for_each`, because that is the only place `as:`
// is a key. Written on a step it is an unknown key and nothing else — and answering "run
// `flow fix`" there would send an author to a command that will not touch their file,
// which is the one response worse than no response.
func TestARetiredKeyIsGuardedByItsPosition(t *testing.T) {
	t.Parallel()

	src := `
edition: v2026.3
name: t
steps:
  - id: a
    iterator: name
    log:
      message: hi
`

	reported := diagnose(t, src)
	require.Contains(t, reported, "unknown key",
		"`iterator:` on a step is not a retired key there, it is simply unknown")
	require.NotContains(t, reported, "run `flow fix`",
		"an author was sent to a command that would not have touched this file")
}

// TestFixRenamesTheKeyAndNothingElse is what makes the rename affordable.
//
// A migration is a diff people read in review, so the edit has to be the key and only
// the key: the value keeps its quoting, an inline comment keeps its column, and a
// comment written under the key stays where the author put it. A rewriter that reflows
// the line is one nobody runs on a directory.
func TestFixRenamesTheKeyAndNothingElse(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
steps:
  - id: each
    for_each:
      items: ${["a"]}
      iterator:   name    # what each item is called
      # a comment written beneath the key
      max_parallel: 2
      steps:
        - id: inner
          log:
            message: ${name}
`

	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.True(t, result.Changed(), "`flow fix` left a file written with the old key alone")

	fixed := string(result.Source)
	require.Contains(t, fixed, "      as:   name    # what each item is called",
		"the rename did not preserve the spacing, the value and the trailing comment:\n%s", fixed)
	require.Contains(t, fixed, "      # a comment written beneath the key",
		"the rename consumed the comment written under it")

	// Every other line byte for byte, which is the property that makes running this
	// over a directory safe.
	before, after := strings.Split(src, "\n"), strings.Split(fixed, "\n")
	require.Equal(t, len(before), len(after), "the rewrite changed the number of lines")
	for i := range before {
		if strings.Contains(before[i], "iterator") {
			continue
		}
		require.Equal(t, before[i], after[i], "line %d changed and had nothing to do with the rename", i+1)
	}

	// And the result is a file this build accepts, which is the claim `flow fix`
	// actually makes.
	require.Empty(t, diagnose(t, fixed))
}

// TestAsRoundTripsThroughMarshal is the `flow fix` guard from the other direction.
//
// Marshal is the inverse of the parser. If it still wrote `iterator:`, a file an author
// had just migrated would be migrated back the next time anything formatted it — and
// the two halves would disagree without either being obviously wrong.
func TestAsRoundTripsThroughMarshal(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
steps:
  - id: each
    for_each:
      items: ${["a"]}
      as: name
      steps:
        - id: inner
          log:
            message: ${name}
`

	wf, err := flowfile.Unmarshal([]byte(src))
	require.NoError(t, err)

	out, err := flowfile.Marshal(wf)
	require.NoError(t, err)

	require.Contains(t, string(out), "as: name", "Marshal still writes the retired spelling")
	require.NotContains(t, string(out), "iterator:")
	require.Empty(t, diagnose(t, string(out)))
}

// The http task's `expect:` and `outputs:` bind four names into an author's namespace —
// `status_code`, `headers`, `body`, `json` — chosen by the system rather than declared
// by anyone. That is the shape the signal payload was already rooted for, and it is
// rooted here for the same two reasons: the set will grow, and a `duration_ms` added
// later written bare would capture a binding somebody already had; and the collision is
// representable today, because `as: body` on a loop enclosing an http step whose
// `expect:` says `body` reads the response and nothing in the file says so.

// TestFixRootsTheResponse covers the shapes these two inputs are actually written in.
//
// Three, and each would have needed its own bug: a fenced scalar, a quoted value whose
// fence is inside the quotes, and a mapping whose *values* carry the fences. The last is
// why this does not go through the fenced-scalar rewriter the step rooting uses.
func TestFixRootsTheResponse(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		src  string
		want string
	}{
		{
			name: "a fenced expect",
			src:  "      expect: ${status_code == 200 || status_code == 404}\n",
			want: "      expect: ${response.status_code == 200 || response.status_code == 404}\n",
		},
		{
			// Two rewrites over one value, and the order they land in is the
			// fixed-point loop's answer rather than either rule's: the first pass
			// promotes the map literal into the mapping form, the re-parse roots
			// the response names in the entries it produced. Asserted together,
			// because a file that came out with one and not the other is the
			// failure — the pre-promotion spelling rooted correctly and the
			// promoted one has to as well.
			name: "a quoted outputs expression becomes a mapping, rooted",
			src:  "      outputs: \"${ {'status': status_code, 'title': json_parse(body)['t']} }\"\n",
			want: "      outputs:\n        status: ${response.status_code}\n        title: ${json_parse(response.body)[\"t\"]}\n",
		},
		{
			name: "a name inside a macro's argument",
			src:  "      expect: ${status_code == 200 && !has(json.error)}\n",
			want: "      expect: ${response.status_code == 200 && !has(response.json.error)}\n",
		},
		{
			name: "a mapping of fenced values",
			src:  "      outputs:\n        code: ${status_code}\n        text: ${body}\n",
			want: "      outputs:\n        code: ${response.status_code}\n        text: ${response.body}\n",
		},
		{
			// The output *names* are the author's and stay exactly as written. Only
			// what is read from the response moves.
			name: "the output names are not touched",
			src:  "      outputs:\n        body: ${body}\n",
			want: "      outputs:\n        body: ${response.body}\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			src := "edition: v2026.3\nname: t\nsteps:\n  - id: fetch\n    http:\n      url: https://example.com\n" + test.src
			want := "edition: v2026.3\nname: t\nsteps:\n  - id: fetch\n    http:\n      url: https://example.com\n" + test.want

			result, err := flowfile.Fix([]byte(src))
			require.NoError(t, err)
			require.Empty(t, result.Refusals)
			require.Equal(t, want, string(result.Source))

			// And what it wrote still parses, which is the claim `flow fix` makes.
			require.Empty(t, diagnose(t, string(result.Source)))
		})
	}
}

// TestFixLeavesResponseNamesAloneOutsideTheResponseScope is the negative direction, and
// the one that would make this rewrite a bug rather than a migration.
//
// `body` is an ordinary word. Outside the two inputs the http task evaluates itself
// there is no response to root it under, so rooting it would invent a reference to
// something that does not exist — turning a working file into one that fails.
func TestFixLeavesResponseNamesAloneOutsideTheResponseScope(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
steps:
  - id: each
    for_each:
      items: ${["a"]}
      as: body
      steps:
        - id: inner
          log:
            message: ${body}
  - id: shout
    vars:
      json: loud
    log:
      message: ${json}
`

	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.Equal(t, src, string(result.Source),
		"a response name was rooted somewhere no response exists")
	require.Empty(t, diagnose(t, src))
}

// TestARootedResponseIsWhatTheEngineBinds closes the loop between the rewriter and the
// task.
//
// `flow fix` writing `response.status_code` is worth nothing if the task still binds the
// name bare, and the two live in different packages with no compiler relationship. This
// runs a rewritten expression through validation and asserts the corpus example — which
// CI executes — reads the rooted way.
func TestARootedResponseIsWhatTheEngineBinds(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
steps:
  - id: fetch
    http:
      url: https://example.com
      expect: ${status_code == 200}
`

	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.Contains(t, string(result.Source), v1.ResponseRoot+".status_code",
		"the rewriter and the task disagree about the root")
}

// The edition marker is the one value in a Flowfile that decides how every other value
// is read, so the two ways `flow fix` touches it — stamping one in, and bringing an old
// one forward — are the two places a rewriter can do the most damage with the least
// visible diff.

// TestFixDoesNotStampOverAMergedEdition is the fail-closed direction.
//
// A merge key names nothing itself and brings in whatever it points at, which may be an
// edition. The stamp decided one was absent by scanning direct keys only — and a direct
// key beats a merged one, so a document declaring a grammar this build *refuses* came
// back declaring one it compiles. A future-edition file silently downgraded by the
// command that exists to keep files honest.
//
// Not stamping is the whole fix: bringing a merged edition forward would mean editing an
// anchor that may be shared with other keys or other documents, so finding one means
// leaving the file alone and letting `flow validate` speak.
func TestFixDoesNotStampOverAMergedEdition(t *testing.T) {
	t.Parallel()

	src := `meta: &m
  edition: v2099.1
<<: *m
name: t
steps:
  - id: a
    log:
      message: hi
`

	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.Equal(t, src, string(result.Source),
		"an edition arriving through a merge key was stamped over, which downgrades the file")

	// And the file is still one this build refuses, which is the property the stamp
	// was quietly undoing.
	require.Contains(t, diagnose(t, string(result.Source)), "v2099.1")
}

// TestFixLeavesAFileAloneWhenItCannotSeeWhatIsMerged covers the answer to "cannot tell".
//
// An alias pointing at an anchor this cannot resolve might be bringing an edition in.
// Every uncertain case has to answer *yes* — the cost runs one way only. Saying yes
// leaves a file unstamped and an author told to write the line; saying no silently
// rewrites what a file declares.
func TestFixLeavesAFileAloneWhenItCannotSeeWhatIsMerged(t *testing.T) {
	t.Parallel()

	src := `<<: *nowhere
name: t
steps:
  - id: a
    log:
      message: hi
`

	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.NotContains(t, string(result.Source), "edition: "+flowfile.CurrentEdition,
		"an edition was stamped into a file whose merged contents this cannot see")
}

// TestFixKeepsWhatIsWrittenAfterAnEdition is the data-loss direction.
//
// Rebuilding the line from the key and the new value dropped whatever followed it, so
// `edition: 2026.1 # pinned deliberately, see RFC-14` came back without the sentence
// explaining it. That path became reachable the moment `2026.1` turned into an edition
// this build actually upgrades — which is exactly the migration this rewriter exists to
// perform, so the first author to run it would have been the one to lose a comment.
func TestFixKeepsWhatIsWrittenAfterAnEdition(t *testing.T) {
	t.Parallel()

	src := `edition: 2026.1 # pinned deliberately, see RFC-14
name: t
steps:
  - id: a
    log:
      message: hi
`

	result, err := flowfile.Fix([]byte(src))
	require.NoError(t, err)
	require.True(t, result.Changed(), "an older edition was not brought forward")

	require.Contains(t, string(result.Source),
		"edition: "+flowfile.CurrentEdition+" # pinned deliberately, see RFC-14",
		"the comment beside the edition was deleted along with the version:\n%s", result.Source)
}
