package lsp

import (
	"strings"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// hoverSource is used by the hover tests. Positions are given as the text to point
// at rather than as coordinates, so the tests stay readable and stay correct when
// the source is edited.
// The cel step's `expr` reaches the response through `vars`, which is the only
// scope that input has. It said `json_parse(web.body)` while references were bare,
// and that was already wrong — a deferred input is not resolved against step
// outputs, so nothing would have reported it. Rooting is what made the difference
// visible: `steps.web.body` in the vars mapping is a reference the compiler
// checks, and `web.body` inside the expr never was one.
const hoverSource = `name: hover
steps:
  - id: web
    http:
      method: GET
      url: https://example.com
  - id: shout
    echo:
      message: ${steps.web.body}
  - id: parsed
    cel:
      libs: [json]
      expr: json_parse(vars.raw)
      vars:
        raw: ${steps.web.body}
`

func TestHover(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// at is the source text to place the cursor on; the cursor goes on its
		// first character unless offset moves it.
		at     string
		offset int
		// want are substrings the hover content must contain.
		want []string
		// notWant are substrings it must not contain.
		notWant []string
		// none asserts that there is no hover at all.
		none bool
	}{
		{
			// A step names its task directly, so the task name is the step's key
			// rather than the value of a `name:` under a `task:`. The token hover
			// has to describe is the same word in the same role; only its position
			// in the grammar moved, so the anchor moves from `http\n` (the old
			// scalar, which ended its line) to `http:` (the key it became).
			name: "task name shows summary and full signature",
			at:   "http:",
			want: []string{
				"task `http`",
				"Perform an HTTP request",
				"url", "string", "(required)",
				"status_code", "int",
				"headers", "map[string, string]",
				// A task is described with no step to hang its outputs off, so the
				// id is a placeholder — but the root is not one, and a copyable
				// form that omitted it would be a form `flow validate` refuses.
				"${steps.<id>.status_code}",
			},
		},
		{
			name: "required input shows its type and constraints",
			at:   "url:",
			want: []string{"`url`", "`string`", "required", "must be an absolute URI"},
		},
		{
			name: "optional input says so",
			at:   "method:",
			want: []string{"`method`", "`string`", "optional", "matches"},
		},
		{
			// The whole rooted reference is what hover names, because it is what
			// the author wrote and what they would copy. The offsets are measured
			// off the fixture rather than counted by hand, so the cursor keeps
			// landing on the segment each case is about.
			name: "expression reference names the producing step and output type",
			at:   "${steps.web.body}",
			// The cursor sits on the `web` segment.
			offset: len("${steps."),
			want:   []string{"`steps.web.body`", "`string`", "step `web`", "`http` task"},
		},
		{
			name:   "expression reference on the output name",
			at:     "${steps.web.body}",
			offset: len("${steps.web."),
			want:   []string{"`steps.web.body`", "`string`"},
		},
		{
			// Anywhere in a rooted reference describes the reference, including on
			// the root segment: `steps` there is one part of a name the author
			// wrote whole, and answering about the root alone would describe
			// something they are not pointing at.
			name:   "the root segment still describes the reference",
			at:     "${steps.web.body}",
			offset: 2,
			want:   []string{"`steps.web.body`", "`string`"},
		},
		{
			name: "cel library describes itself and what it provides",
			at:   "json]",
			want: []string{"CEL library `json`", "json_parse", "libs: [json]"},
		},
		{
			name: "step id summarizes the step",
			at:   "web\n",
			want: []string{"step `web`", "step 1", "`http` task", "${steps.web.body}"},
		},
		{
			name: "nothing to say about a plain literal value",
			at:   "GET",
			none: true,
		},
		{
			// This asserted that nothing was said about `steps:`, which recorded
			// where hover stopped rather than a decision: it resolved a key only
			// inside a step, so a document key produced nothing. `edition:` is the
			// key that makes the difference matter — a date says nothing about what
			// declaring it means — and once the document level answers at all, it
			// answers here too, out of the same table.
			name: "a document key describes itself",
			at:   "steps:",
			want: []string{"`steps`", "`list`", "The steps to run, in order"},
		},
		{
			// The key, not the line. A value is the author's own text and the table
			// has nothing to say about it.
			name: "nothing to say about a document key's value",
			at:   "hover\n",
			none: true,
		},
	}

	c := newClient(t)
	c.initialize()
	const uri = "file:///hover.yaml"
	c.open(uri, hoverSource)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pos := positionOf(t, hoverSource, tt.at, tt.offset)
			got := c.hover(uri, pos.Line, pos.Character)

			if tt.none {
				assert.Nil(t, got, "expected no hover, got %q", hoverText(got))
				return
			}
			require.NotNil(t, got, "expected hover content at %v", pos)
			require.NotNil(t, got.Range, "hover must report the range it describes")
			text := hoverText(got)
			for _, want := range tt.want {
				assert.Contains(t, text, want)
			}
			for _, notWant := range tt.notWant {
				assert.NotContains(t, text, notWant)
			}
		})
	}
}

// TestHoverOnAStepLeadsWithTheAuthorsProse covers a step's `description:`, which
// is the one thing about a step that cannot be derived.
//
// Everything else hover says about a step — the task, its summary, its outputs —
// is read out of the registry, and none of it can say why this step is in this
// workflow. It is also the only place the prose is shown at all: the outline has
// no field to put it in, which symbols.go argues at length.
func TestHoverOnAStepLeadsWithTheAuthorsProse(t *testing.T) {
	t.Parallel()

	const src = `name: described
steps:
  - id: roster
    description: Ask upstream who is on call, because the rota changes hourly.
    http:
      url: https://example.com/roster
  - id: quiet
    echo:
      message: hi
`
	c := newClient(t)
	c.initialize()
	const uri = "file:///described.yaml"
	require.Empty(t, messages(c.open(uri, src).Diagnostics))

	t.Run("hovering the id shows it", func(t *testing.T) {
		pos := positionOf(t, src, "roster\n", 0)
		got := c.hover(uri, pos.Line, pos.Character)
		require.NotNil(t, got)
		text := hoverText(got)
		assert.Contains(t, text, "Ask upstream who is on call")
		// Alongside what the step does, rather than in place of it.
		assert.Contains(t, text, "`http` task")
	})

	t.Run("hovering the key describes the key", func(t *testing.T) {
		// A key and its value answer different questions — what `description:`
		// means, and what this step is for — and pointing at the key asks the
		// first one.
		pos := positionOf(t, src, "description:", 0)
		got := c.hover(uri, pos.Line, pos.Character)
		require.NotNil(t, got)
		assert.Contains(t, hoverText(got), "why it is here")
	})

	t.Run("a step without one says nothing extra", func(t *testing.T) {
		pos := positionOf(t, src, "quiet\n", 0)
		got := c.hover(uri, pos.Line, pos.Character)
		require.NotNil(t, got)
		assert.NotContains(t, hoverText(got), "Ask upstream",
			"a step's prose must not follow the cursor to the next step")
	})
}

// TestHoverOnTheRootItself covers the one thing rooting added that an author can
// point at and get no answer from anywhere else: the word in front of every
// reference.
//
// It is not only decoration. The root is a value in its own right — the validator
// permits a bare `steps`, because `size(steps)` counts what has run — so a name
// that resolves is a name hover should describe.
func TestHoverOnTheRootItself(t *testing.T) {
	t.Parallel()

	const src = `name: root
steps:
  - id: a
    echo:
      message: hi
  - id: b
    echo:
      message: ${string(size(steps))}
`
	c := newClient(t)
	c.initialize()
	const uri = "file:///root.yaml"
	require.Empty(t, messages(c.open(uri, src).Diagnostics),
		"premise: the root is a name the compiler accepts on its own")

	pos := positionOf(t, src, "size(steps)", len("size("))
	got := c.hover(uri, pos.Line, pos.Character)
	require.NotNil(t, got, "no hover on the root")
	text := hoverText(got)
	assert.Contains(t, text, "keyed by step id")
	// It says what the root is *for*, which is the question an author hovering it
	// is really asking: why references gained a prefix.
	assert.Contains(t, text, "separate namespaces")

	require.NotNil(t, got.Range)
	assert.Equal(t, "steps", textInRange(src, *got.Range))
}

// TestHoverStaysQuietOnUnresolvableReference checks that hover says nothing about a
// reference the workflow cannot resolve. The diagnostics already explain it, and a
// popup describing a step that does not exist would contradict them.
func TestHoverStaysQuietOnUnresolvableReference(t *testing.T) {
	t.Parallel()

	const src = `name: quiet
steps:
  - id: a
    echo:
      message: ${steps.later.result}
  - id: later
    echo:
      message: hi
`
	c := newClient(t)
	c.initialize()
	c.open("file:///quiet.yaml", src)

	pos := positionOf(t, src, "${steps.later.result}", len("${steps."))
	assert.Nil(t, c.hover("file:///quiet.yaml", pos.Line, pos.Character))
}

// TestHoverStaysQuietOnTheRetiredSpelling is the negative direction of rooting, in
// the feature where saying something would be worst.
//
// A bare `${web.body}` is not a step reference in this grammar — it is a free name
// that means nothing — and the diagnostic on it names the migration that fixes the
// file. Describing it as though it resolved would tell an author the spelling
// works while the compiler is telling them it does not.
func TestHoverStaysQuietOnTheRetiredSpelling(t *testing.T) {
	t.Parallel()

	const src = `name: unmigrated
steps:
  - id: web
    http:
      url: https://example.com
  - id: out
    echo:
      message: ${web.body}
`
	c := newClient(t)
	c.initialize()
	const uri = "file:///unmigrated.yaml"

	// The premise: the compiler refuses this file and says what to run.
	reported := messages(c.open(uri, src).Diagnostics)
	require.Len(t, reported, 1)
	require.Contains(t, reported[0], "flow fix")

	for _, at := range []int{2, len("${web.")} {
		pos := positionOf(t, src, "${web.body}", at)
		assert.Nil(t, c.hover(uri, pos.Line, pos.Character),
			"hover described a reference in the spelling this grammar retired")
	}
}

// TestHoverOnQuestionableReferences checks what hover says about a reference that
// resolves to a step but not to anything that step produces. These are the cases
// where saying the wrong thing would actively mislead.
func TestHoverOnQuestionableReferences(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	t.Run("an output the task does not declare", func(t *testing.T) {
		const src = `name: wrong-output
steps:
  - id: web
    http:
      url: https://example.com
  - id: out
    echo:
      message: ${steps.web.stdout}
`
		c.open("file:///wrong-output.yaml", src)
		pos := positionOf(t, src, "${steps.web.stdout}", len("${steps.web."))
		got := c.hover("file:///wrong-output.yaml", pos.Line, pos.Character)
		require.NotNil(t, got)
		// It names what the task does produce rather than inventing a type for
		// an output that does not exist.
		assert.Contains(t, hoverText(got), "does not declare an output named `stdout`")
		assert.Contains(t, hoverText(got), "status_code")
	})

	t.Run("a step whose task is not registered", func(t *testing.T) {
		const src = `name: unknown-task
steps:
  - id: mystery
    shell:
      command: ls
  - id: out
    echo:
      message: ${steps.mystery.stdout}
`
		c.open("file:///unknown-producer.yaml", src)
		pos := positionOf(t, src, "${steps.mystery.stdout}", len("${steps."))
		got := c.hover("file:///unknown-producer.yaml", pos.Line, pos.Character)
		require.NotNil(t, got)
		assert.Contains(t, hoverText(got), "whose task `shell` is not registered")
	})

	// Named "a bare step reference" while `${web}` was how a step was addressed.
	// The reference is rooted now and the question is unchanged: what hover says
	// about a name that reaches a step but stops short of one of its outputs.
	t.Run("a step reference with no output", func(t *testing.T) {
		const src = `name: bare
steps:
  - id: web
    http:
      url: https://example.com
  - id: out
    cel:
      expr: "1"
      vars:
        v: ${steps.web}
`
		c.open("file:///bare.yaml", src)
		pos := positionOf(t, src, "${steps.web}", len("${steps."))
		got := c.hover("file:///bare.yaml", pos.Line, pos.Character)
		require.NotNil(t, got)
		assert.Contains(t, hoverText(got), "step 1")
		// A bare reference lists what is available rather than guessing.
		assert.Contains(t, hoverText(got), "Outputs:")
		assert.Contains(t, hoverText(got), "body")
	})
}

// TestHoverDescribesDeferredInputs checks that hover names the inputs a task
// evaluates itself, reading them from the task definition.
//
// The list is derived rather than asserted literally, because it grew: the http
// task gained `expect` alongside `outputs`, which both broke a hardcoded assertion
// and turned the sentence into "evaluates `outputs`, `expect` itself".
func TestHoverDescribesDeferredInputs(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	for _, def := range v1.DefaultRegistry().All() {
		if len(def.DeferredInputs) == 0 {
			continue
		}
		t.Run(def.Name, func(t *testing.T) {
			// The step names the task with a key of its own, so the fixture is the
			// step id and the task name and nothing else. The task's inputs would be
			// the mapping under that key; this case needs none of them, and an
			// unwritten one is what the hover has to describe anyway.
			src := "name: deferred\nsteps:\n  - id: a\n    " + def.Name + ":\n"
			uri := "file:///deferred-" + def.Name + ".yaml"
			c.open(uri, src)

			// The name is a key, so what follows it is a colon rather than the end
			// of the line the old `name: <task>` anchor relied on.
			pos := positionOf(t, src, def.Name+":", 0)
			got := c.hover(uri, pos.Line, pos.Character)
			require.NotNil(t, got)
			text := hoverText(got)

			for _, name := range def.DeferredInputs {
				assert.Contains(t, text, "`"+name+"`")
			}
			// The whole sentence is pinned, which fixes the joining exactly. An
			// assertion that the popup contains no comma-joined list anywhere would
			// be broader than its intent — the outputs line joins names that way
			// legitimately — and broader-than-intent is what made the last two of
			// these fail on a coincidence.
			assert.Contains(t, text, "The task evaluates "+joinNames(def.DeferredInputs)+" itself")
		})
	}
}

func TestJoinNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   []string
		want string
	}{
		{nil, ""},
		{[]string{"a"}, "`a`"},
		{[]string{"a", "b"}, "`a` and `b`"},
		{[]string{"a", "b", "c"}, "`a`, `b`, and `c`"},
		{[]string{"a", "b", "c", "d"}, "`a`, `b`, `c`, and `d`"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, joinNames(tt.in))
	}
}

// TestHoverOnSecretReferences covers the one thing an author cannot see from the
// text: what a ${secret('scheme:name')} marker actually names, and why it may only
// be a whole task input.
func TestHoverOnSecretReferences(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	t.Run("a valid reference", func(t *testing.T) {
		const src = `name: secrets
steps:
  - id: a
    echo:
      message: ${secret('env:API_KEY')}
`
		const uri = "file:///secret-ok.yaml"
		require.Empty(t, messages(c.open(uri, src).Diagnostics))

		pos := positionOf(t, src, "secret('env:API_KEY')", 3)
		got := c.hover(uri, pos.Line, pos.Character)
		require.NotNil(t, got)
		text := hoverText(got)
		assert.Contains(t, text, "env:API_KEY")
		assert.Contains(t, text, "resolved by whichever provider this deployment registers")
		assert.Contains(t, text, "never enters workflow history")
		// The scheme is described, never a concrete backend: which provider serves
		// a scheme is a deployment's choice, made worker-side.
		assert.NotContains(t, text, "environment variable")

		require.NotNil(t, got.Range)
		assert.Equal(t, "secret('env:API_KEY')", textInRange(src, *got.Range))
	})

	t.Run("a reference with no scheme says why", func(t *testing.T) {
		const src = `name: secrets
steps:
  - id: a
    echo:
      message: ${secret('API_KEY')}
`
		const uri = "file:///secret-bad.yaml"
		// The compiler reports it too; hover must agree rather than describe it as
		// if it worked.
		require.NotEmpty(t, messages(c.open(uri, src).Diagnostics))

		pos := positionOf(t, src, "secret('API_KEY')", 3)
		got := c.hover(uri, pos.Line, pos.Character)
		require.NotNil(t, got)
		assert.Contains(t, hoverText(got), "not usable as written")
		assert.Contains(t, hoverText(got), "has no provider")
	})

	t.Run("an opaque name is not interpreted", func(t *testing.T) {
		const src = `name: secrets
steps:
  - id: a
    echo:
      message: ${secret('vault:prod/api#token')}
`
		const uri = "file:///secret-vault.yaml"
		c.open(uri, src)

		pos := positionOf(t, src, "vault:prod/api#token", 2)
		got := c.hover(uri, pos.Line, pos.Character)
		require.NotNil(t, got)
		// Everything after the first colon belongs to the provider, so it is
		// reported verbatim rather than parsed into parts the DSL does not define.
		assert.Contains(t, hoverText(got), "prod/api#token")
		assert.Contains(t, hoverText(got), "`vault`")
	})
}

// TestReferenceAt covers the reference reader directly, because it is the whole of
// what hover and go-to-definition know about an expression and because most of its
// cases are ones a fixture cannot reach comfortably: a cursor on a boundary, a
// half-typed root, a chain deeper than the schema describes.
//
// The span matters as much as the names. It is what an editor underlines, so a
// span that runs one segment too far highlights a word the popup is not talking
// about — and the two namespaces stop at different places, which is exactly the
// kind of asymmetry a test of the names alone would not see.
func TestReferenceAt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		src    string
		cursor int
		want   reference
		// spans is the source text the reference's span covers, checked instead of
		// two integers so that a wrong span reads as the wrong word.
		spans string
	}{
		{
			name: "a rooted reference, cursor on the root",
			src:  "steps.web.body", cursor: 2,
			want: reference{step: "web", output: "body"}, spans: "steps.web.body",
		},
		{
			name: "a rooted reference, cursor on the id",
			src:  "steps.web.body", cursor: 7,
			want: reference{step: "web", output: "body"}, spans: "steps.web.body",
		},
		{
			name: "a rooted reference, cursor on the output",
			src:  "steps.web.body", cursor: 12,
			want: reference{step: "web", output: "body"}, spans: "steps.web.body",
		},
		{
			name: "a step with no output selected",
			src:  "steps.web", cursor: 7,
			want: reference{step: "web"}, spans: "steps.web",
		},
		{
			// Selecting into an output's own shape, which no schema here
			// describes. The reference is the part that is described and the rest
			// is left alone, so the popup and the underline agree.
			name: "deeper than an output",
			src:  "steps.web.json.title", cursor: 8,
			want: reference{step: "web", output: "json"}, spans: "steps.web.json",
		},
		{
			name: "inside a call",
			src:  "string(steps.web.status_code)", cursor: 10,
			want: reference{step: "web", output: "status_code"}, spans: "steps.web.status_code",
		},
		{
			// A bare name is a binding, and the span stops at the name: what
			// follows it selects into a value whose type is not statically known,
			// so there is nothing there to describe or to underline.
			name: "a bare binding",
			src:  "item", cursor: 2,
			want: reference{local: "item"}, spans: "item",
		},
		{
			name: "a bare binding with a field",
			src:  "item.name", cursor: 2,
			want: reference{local: "item"}, spans: "item",
		},
		{
			// The retired spelling. It reads as a binding rather than as a step,
			// which is what keeps hover quiet on it while the diagnostic explains
			// the migration.
			name: "the retired spelling is not a step",
			src:  "web.body", cursor: 1,
			want: reference{local: "web"}, spans: "web",
		},
		{
			name: "the root alone",
			src:  "size(steps)", cursor: 6,
			want: reference{local: "steps"}, spans: "steps",
		},
		{
			// What an author has typed the instant completion fires. It names no
			// step, so there is nothing to describe.
			name: "a half-typed root", src: "steps.", cursor: 6,
		},
		{name: "a trailing dot after an id", src: "steps.web.", cursor: 10,
			want: reference{step: "web"}, spans: "steps.web"},
		{name: "not on a word", src: "1 + 2", cursor: 2},
		{
			// A numeric literal is a run of word bytes, so it reads as a bare name
			// — and resolves to nothing, which is the same answer as reading it as
			// a number would give. Pinned because it is the one place the byte rule
			// and the grammar disagree, and the disagreement is harmless only for
			// as long as nothing bare is answered by position rather than by name.
			name: "a literal reads as a name that resolves to nothing",
			src:  "1 + 2", cursor: 0,
			want: reference{local: "1"}, spans: "1",
		},
		{name: "empty source", src: "", cursor: 0},
		{name: "cursor before the source", src: "steps.web", cursor: -1},
		{name: "cursor past the source", src: "steps.web", cursor: 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := referenceAt(tt.src, tt.cursor)

			assert.Equal(t, tt.want.step, got.step, "step")
			assert.Equal(t, tt.want.output, got.output, "output")
			assert.Equal(t, tt.want.local, got.local, "local")
			assert.Equal(t, tt.spans == "", got.empty(), "empty")

			if tt.spans == "" {
				return
			}
			require.LessOrEqual(t, got.span[1], len(tt.src), "span runs past the source")
			assert.Equal(t, tt.spans, tt.src[got.span[0]:got.span[1]], "span")
		})
	}
}

// TestReferenceAtSurvivesEveryCursor is the bound on a function that reads
// attacker-shaped input: an expression is whatever an author has typed, and the
// cursor is wherever an editor says it is.
//
// Every offset of every source is tried, and the invariant checked at each is the
// one every caller depends on — a span that indexes the source it came from. A
// panic here reaches the client as a failed request on every keystroke.
func TestReferenceAtSurvivesEveryCursor(t *testing.T) {
	t.Parallel()

	sources := []string{
		"", ".", "..", "...", "steps", "steps.", "steps..", ".steps.a",
		"steps.web.body", "steps.steps.steps", "item.name",
		"string(steps.a.b) + item", "secret('env:A')", "steps.🙂.body",
		"a.b.c.d.e.f.g", "_._._", "0.1.2",
	}
	for _, src := range sources {
		for cursor := -2; cursor <= len(src)+2; cursor++ {
			got := referenceAt(src, cursor)
			require.GreaterOrEqual(t, got.span[0], 0, "src %q cursor %d", src, cursor)
			require.LessOrEqual(t, got.span[0], got.span[1], "src %q cursor %d", src, cursor)
			require.LessOrEqual(t, got.span[1], len(src), "src %q cursor %d", src, cursor)
			if got.empty() {
				continue
			}
			// A non-empty reference must name something the source contains, or
			// the underline and the popup are describing different text.
			named := got.local
			if got.step != "" {
				named = got.step
			}
			require.Contains(t, src[got.span[0]:got.span[1]], named, "src %q cursor %d", src, cursor)
		}
	}
}

func TestSecretRefAt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		src     string
		cursor  int
		wantRef string
		wantErr bool
	}{
		{name: "cursor on the marker", src: `secret('env:A')`, cursor: 2, wantRef: "env:A"},
		{name: "cursor in the reference", src: `secret('env:A')`, cursor: 10, wantRef: "env:A"},
		{name: "cursor at the end", src: `secret('env:A')`, cursor: 15, wantRef: "env:A"},
		{name: "double quotes", src: `secret("env:A")`, cursor: 3, wantRef: "env:A"},
		{name: "not a secret call", src: `step.output`, cursor: 3, wantErr: true},
		{name: "cursor outside the call", src: `x + secret('env:A')`, cursor: 0, wantErr: true},
		{name: "unterminated quote", src: `secret('env:A`, cursor: 3, wantErr: true},
		{name: "no closing paren", src: `secret('env:A'`, cursor: 3, wantErr: true},
		{name: "empty", src: ``, cursor: 0, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ref, span, err := secretRefAt(tt.src, tt.cursor)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantRef, ref)
			assert.Equal(t, tt.src[span[0]:span[1]], tt.src[span[0]:span[1]])
			assert.Contains(t, tt.src[span[0]:span[1]], tt.wantRef)
		})
	}
}

// TestHoverUsesUTF16Columns is the position-correctness test. The reference sits
// after an astral-plane character, so a server counting bytes or code points
// instead of UTF-16 code units would resolve the cursor to the wrong text.
func TestHoverUsesUTF16Columns(t *testing.T) {
	t.Parallel()

	const src = "name: ünïcödé\n" +
		"steps:\n" +
		"  - id: first\n" +
		"    echo:\n" +
		"      message: \"héllo wörld\"\n" +
		"  - id: second\n" +
		"    http:\n" +
		"      url: https://example.com\n" +
		"      headers:\n" +
		"        X-🙂-Trace: ${steps.first.result}\n"

	// The three unit systems genuinely disagree on this line, which is the point.
	// Flattening dropped the `task:`/`inputs:` scaffolding, so the header sits one
	// level shallower — eight spaces of indent rather than ten — and the reference
	// two lines earlier. The counts below are what that line measures; the gaps
	// between them are what the test is about, and those are unchanged. Rooting
	// lengthens the reference and does not touch anything before the `$`, so they
	// are unchanged again.
	line := "        X-🙂-Trace: ${steps.first.result}"
	dollar := strings.Index(line, "$")
	require.Equal(t, 22, dollar, "byte column")
	require.Equal(t, 19, len([]rune(line[:dollar])), "code point column")
	require.Equal(t, 20, utf16Len(line[:dollar]), "UTF-16 column")
	require.Equal(t, line, strings.Split(src, "\n")[9],
		"the line the columns were measured on must be the line the cursor is put on")

	c := newClient(t)
	c.initialize()
	c.open("file:///unicode.yaml", src)

	// Point at the `first` segment inside the expression. The root is ASCII, so
	// its width is the same in every unit system and the offset is measured off
	// the source rather than counted a second time.
	got := c.hover("file:///unicode.yaml", 9, 20+len("${steps.")+1)
	require.NotNil(t, got, "hover must resolve a reference that follows non-ASCII text")
	assert.Contains(t, hoverText(got), "`steps.first.result`")
	// The declared type of the output, which only appears once the reference has
	// resolved all the way to the echo task's schema. Hover names the reference
	// even when it cannot resolve the producing step, so without this the fixture
	// could stop parsing as a task and nothing here would notice.
	assert.Contains(t, hoverText(got), "`string`")

	// The range it reports must come back in UTF-16 units too, covering exactly
	// the reference — which is the whole of it, root included, because that is
	// what the author wrote and what an editor should highlight.
	require.NotNil(t, got.Range)
	assert.Equal(t, "steps.first.result", textInRange(src, *got.Range))
}

// positionOf returns the position of the needle in src, advanced by offset UTF-16
// code units.
func positionOf(t *testing.T, src, needle string, offset int) lsp.Position {
	t.Helper()
	at := strings.Index(src, needle)
	require.GreaterOrEqual(t, at, 0, "test source does not contain %q", needle)
	ix := newLineIndex(src)
	pos := ix.positionOfOffset(at)
	pos.Character += offset
	return pos
}
