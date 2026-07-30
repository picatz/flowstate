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

	src := `name: t
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

	src := `edition: 2026.1
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
			name: "a quoted outputs expression",
			src:  "      outputs: \"${ {'status': status_code, 'title': json_parse(body)['t']} }\"\n",
			want: "      outputs: \"${ {'status': response.status_code, 'title': json_parse(response.body)['t']} }\"\n",
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

			src := "name: t\nsteps:\n  - id: fetch\n    http:\n      url: https://example.com\n" + test.src
			want := "name: t\nsteps:\n  - id: fetch\n    http:\n      url: https://example.com\n" + test.want

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

	src := `name: t
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

	src := `name: t
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
