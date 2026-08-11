package lsp

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
)

// editionSuffix is the `edition:` marker these fixtures carry, written *last*.
//
// Unique to this package, and deliberate. A real file writes the marker first, and the
// examples in `examples/` do — but these tests assert (line, character) coordinates
// against their own fixtures, and a key added at the top renumbers every one of them.
// YAML does not care where a top-level key sits, so putting it at the end keeps each
// fixture's own geometry while still making it a document this build compiles.
//
// One exception, and it is not a matter of taste. A step's range runs to the line before
// the next step's dash, and the last step's end is walked back over *blank* lines only —
// so a top-level key written after the steps extends the last step past its own content.
// A fixture that asserts where the last step ends must write the marker above instead.
const editionSuffix = "edition: v2026.2\n"

// TestDSLKeysMatchTheDSL is the guard on the one list in this package that is not
// derived from a central definition.
//
// The document shape lives in unexported structs in the flowfile package, so
// completion carries a copy of it — and that copy silently fell behind when the DSL
// gained if, timeout, retry, and continue_on_error. This test derives the real key
// set by asking flowfile to marshal a workflow with every field populated, so the
// next addition fails here instead of quietly going unsupported.
//
// "Every field populated" is the whole load-bearing part, and it is where this test
// was weaker than it looked. The fixture held a single task step, so Marshal was
// never asked to render a wait — and the three keys that spell one, which had been
// shipped, reachable from a Flowfile, and exercised by examples in CI the whole
// time, were absent from the rendered document this compares against. A guard that
// derives the real key set from a fixture only knows the keys the fixture reaches.
//
// So the fixture is a workflow that uses every *kind* of step, not merely every
// field of one kind.
//
// Flattening added the other direction. The table used to carry a `task` level
// holding `name` and `inputs`, and a step key called `task`; the DSL stopped
// emitting all three at once. Nothing here could have noticed, because every
// assertion asked only whether an emitted key was known — a table entry for a key
// the language no longer has is invisible to that question, and completion would
// have gone on offering a word `flow validate` rejects. So the comparison runs both
// ways now: every key the DSL emits is one the table knows, and every key the table
// declares is one the DSL still emits.
//
// The step level also gained a second vocabulary, since a task's name is a step key
// in its own right. That one is not written down here at all — it comes from the
// registry, so a task registered tomorrow needs no change to this test — and it only
// stays unambiguous while it cannot collide with a step property, which is the last
// thing asserted.
//
// One key is outside what a round trip can see at all. `edition:` names a property
// of a file rather than of a workflow, so Marshal has nothing to render it from; it
// is added to the document below and proved against the compiler instead, which is
// the same guarantee by a different route.
func TestDSLKeysMatchTheDSL(t *testing.T) {
	t.Parallel()

	// A workflow exercising every field and every step kind the DSL can express.
	workflow := &v1.Workflow{
		Name:        "every-field",
		Description: ptr("described"),
		// Both positions of `vars:`, because they are separate keys in separate
		// tables and the table for one landed without the other. A fixture missing
		// a key is how this test stayed green through the drift it exists to catch.
		Vars: map[string]*v1.Value{"region": v1.NewValue("eu-west-1")},
		Steps: []*v1.Node{
			{
				Id:        "only",
				Condition: v1.NewExpr("true"),
				Vars:      map[string]*v1.Value{"greeting": v1.NewValue("hi")},
				Policy: &v1.StepPolicy{
					Timeout:         durationpb.New(30_000_000_000),
					ContinueOnError: true,
					Retry: &v1.RetryPolicy{
						MaxAttempts:        3,
						InitialInterval:    durationpb.New(1_000_000_000),
						BackoffCoefficient: 2,
						MaxInterval:        durationpb.New(60_000_000_000),
					},
				},
				// A compensation, so `undo:` is a key this fixture actually reaches.
				// The test compares against what Marshal emits from this workflow, so
				// a field the fixture leaves unset is invisible to both directions of
				// the comparison — which is how three wait keys were once missing
				// while this was green.
				Undo: &v1.Compensation{Task: &v1.Task{
					Name:   "log",
					Inputs: map[string]*v1.Value{"message": v1.NewValue("undone")},
				}},
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "log",
					Inputs: map[string]*v1.Value{"message": v1.NewValue("hi")},
				}},
			},
			{
				Id: "loop",
				// On a loop rather than on the task step above, because a
				// description is a property of a *step*: writing it here is what
				// makes the fixture reach the key at a step that runs no task at
				// all, which is the reading the table documents.
				Description: ptr("Do the thing once per item."),
				Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
					Items:       v1.NewLiteralList("a", "b"),
					Iterator:    "each",
					MaxParallel: 2,
					Body:        []*v1.Node{{Id: "inner", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}}}},
				}},
			},
			{
				// A `loop:`, so this fixture reaches its keys — `as`, `init`, `update`,
				// `until`, `max_iterations`, and its own `steps`. Its expressions read
				// only the carried state, so it round-trips and validates without
				// depending on any body output.
				Id: "poll",
				Kind: &v1.Node_Loop{Loop: &v1.Loop{
					State:         "cursor",
					Initial:       v1.NewExpr("0"),
					Update:        v1.NewExpr("cursor + 1"),
					Until:         v1.NewExpr("cursor >= 2"),
					MaxIterations: 10,
					Body:          []*v1.Node{{Id: "page", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}}}},
				}},
			},
			{
				// A `value:`, so this fixture reaches the key. Its expression
				// reads nothing outside itself, so it round-trips and validates
				// wherever it sits in the list.
				Id:   "named",
				Kind: &v1.Node_Value{Value: v1.NewExpr("1 + 1")},
			},
			{
				Id: "branches",
				Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{
					Branches: []*v1.Parallel_Branch{
						{Steps: []*v1.Node{{Id: "left", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}}}}},
					},
				}},
			},
			{
				Id:   "pause",
				Kind: &v1.Node_Wait{Wait: &v1.Wait{Kind: &v1.Wait_Duration{Duration: durationpb.New(30_000_000_000)}}},
			},
			{
				Id: "until",
				Kind: &v1.Node_Wait{Wait: &v1.Wait{
					Kind: &v1.Wait_Until{Until: v1.NewExpr("now + days(3)")},
				}},
			},
			{
				// The mapping form, not the scalar one: a bare
				// `wait_for_signal: name` renders no nested keys, so only a gate
				// carrying a timeout makes Marshal emit `name` and `timeout`.
				Id: "gate",
				Kind: &v1.Node_Wait{Wait: &v1.Wait{
					Kind: &v1.Wait_Signal{Signal: &v1.Signal{
						Name: "deploy-approved",
						// What the gate is asking for. Set here rather than left
						// out because this test only knows the keys its fixture
						// reaches - a key nothing populates is invisible to both
						// directions of the comparison, which is how three wait
						// keys once stayed missing while this was green.
						Prompt: v1.NewExpr(`"approve the deploy?"`),
						// The gate's own `outputs:` shaping, which is the fourth
						// key of this block and the only one whose values are
						// expressions in a scope of their own.
						Outputs: map[string]*v1.Value{
							"approved": v1.NewExpr("has(payload.approved) && payload.approved"),
						},
					}},
					Timeout: durationpb.New(3_600_000_000_000),
				}},
			},
			{
				// `call:` and `with:`, so this fixture reaches both keys. The
				// callee is written to a real file below rather than only held in
				// memory, because the round trip this test performs — Marshal,
				// then re-parse the result — has to resolve the path Marshal
				// writes back out, the same as it would for an author's file.
				Id: "provision",
				Kind: &v1.Node_Call{Call: &v1.Call{
					Workflow: &v1.Workflow{
						Name:    "callee",
						Profile: v1.CurrentProfile,
						DeclaredInputs: []*v1.InputDeclaration{
							{Name: "who", Type: v1.InputDeclaration_TYPE_STRING, Required: true},
						},
						Steps: []*v1.Node{{Id: "greet", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}}}},
					},
					Source:    "./callee.yaml",
					Arguments: map[string]*v1.Value{"who": v1.NewValue("world")},
				}},
			},
		},
	}

	// The callee `call:` names, on disk — Marshal writes back the path an author
	// wrote, never the embedded copy (see marshal.go), so re-parsing that output
	// needs a real file there to resolve it against, the same as it would for a
	// Flowfile someone actually wrote.
	dir := t.TempDir()
	calleeSrc, err := flowfile.Marshal(workflow.GetSteps()[len(workflow.GetSteps())-1].GetCall().GetWorkflow())
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "callee.yaml"), calleeSrc, 0o644))
	path := filepath.Join(dir, "workflow.yaml")

	rendered, err := flowfile.Marshal(workflow)
	require.NoError(t, err, "the DSL round trip must work for this test to mean anything")

	// A third case the two directions below cannot tell apart on their own: a key
	// the grammar has that Marshal structurally cannot write.
	//
	// Marshal renders a *spec*, and an edition is a property of a *file* — the
	// schema deliberately has no field for it and should not grow one, which
	// flowfile/edition.go says at length. So no workflow value makes `edition:`
	// appear in the rendered document, and a vocabulary derived from Marshal alone
	// would report the key as one the language dropped when nothing was dropped.
	//
	// Marshal writes the declaration itself now that one is required, so the document
	// under test already carries it — and that is proved rather than assumed below:
	// the compiler refuses an unknown document key, so a grammar that stopped
	// accepting `edition:` fails here, which is exactly the service the stale check
	// performs for every other key.
	source := string(rendered)
	require.NoError(t, os.WriteFile(path, rendered, 0o644))
	_, _, err = flowfile.ParseAt([]byte(source), path)
	require.NoError(t, err, "the DSL must accept the document this table is compared against:\n%s", source)

	// The keys the DSL actually writes, as a set: the same key legitimately appears
	// at more than one level (`steps`, `name`, `description`, `timeout` all do), and
	// it is the vocabulary that is being compared rather than any one occurrence.
	emitted := map[string]bool{}
	for _, line := range strings.Split(source, "\n") {
		if m := keyLine.FindStringSubmatch(line); m != nil {
			emitted[m[3]] = true
		}
	}

	// The keys the shape table declares, at any level.
	shape := map[string]bool{}
	for _, keys := range dslKeys {
		for _, k := range keys {
			shape[k.name] = true
		}
	}

	// The keys the registry accounts for. A step names its task directly, so a
	// task's name is a step key and its inputs are the keys beneath it — neither is
	// in the shape table, and neither should be: the registry and the task's schema
	// are where they are defined, and reading them here is what stops this test from
	// needing an edit every time a task is added.
	fromRegistry := map[string]bool{}
	for _, def := range v1.DefaultRegistry().All() {
		fromRegistry[def.Name] = true
		for _, name := range fieldNames(def.Inputs) {
			fromRegistry[name] = true
		}
	}

	// And the keys the *author* chose rather than the grammar. A `vars:` block is an
	// open mapping, so the names under it are no more part of the vocabulary than a
	// step id is — but they are rendered as keys and the scan above cannot tell the
	// difference.
	//
	// Collected from the fixture rather than skipped by name, so exempting them
	// cannot quietly exempt anything else: a real key that happened to be spelled
	// like one of these would still have to appear in the fixture's vars to slip
	// through, and it does not.
	authored := map[string]bool{}
	for name := range workflow.GetVars() {
		authored[name] = true
	}
	for _, node := range workflow.GetSteps() {
		for name := range node.GetVars() {
			authored[name] = true
		}
		// A call's arguments are author-named the same way `vars:` entries are —
		// `who:` here is a name `with:` binds, not a key the grammar has an
		// opinion about — and so is the callee's own declared input of the same
		// name, one level down.
		for name := range node.GetCall().GetArguments() {
			authored[name] = true
		}
		for _, declaration := range node.GetCall().GetWorkflow().GetDeclaredInputs() {
			authored[declaration.GetName()] = true
		}
		// A gate's `outputs:` shaping is an open mapping too: the author names
		// what the wait produces, exactly as they name a `vars:` binding.
		for name := range node.GetWait().GetSignal().GetOutputs() {
			authored[name] = true
		}
	}

	var missing []string
	for key := range emitted {
		if !shape[key] && !fromRegistry[key] && !authored[key] {
			missing = append(missing, key)
		}
	}
	slices.Sort(missing)
	assert.Empty(t, missing,
		"the Flowfile DSL emits keys this package does not know about; add them to dslKeys "+
			"(and to the parsed model if they carry expressions or durations).\nRendered:\n%s", source)

	// The other direction. A key here that the DSL does not emit is either a key
	// the language dropped — which completion would go on offering — or a gap in
	// the fixture above, and the two are worth telling apart by hand, so the failure
	// says so rather than picking one.
	var stale []string
	for key := range shape {
		if !emitted[key] {
			stale = append(stale, key)
		}
	}
	slices.Sort(stale)
	assert.Empty(t, stale,
		"dslKeys declares keys the DSL does not emit; either the language dropped them and the "+
			"entries should go, or the workflow above stopped reaching them and should be extended "+
			"to cover them again.\nRendered:\n%s", source)

	// A level of the table is a key of some other level: keys nest under a key. A
	// level left behind when its key went away is the shape the `task` block had on
	// the way out, and it is invisible to both comparisons above, which only look at
	// key names.
	for level := range dslKeys {
		if level == "" || level == "steps" {
			// The document itself, and a step, which are the two levels no key
			// opens: `steps:` holds a list of them.
			continue
		}
		found := false
		for _, keys := range dslKeys {
			found = found || slices.ContainsFunc(keys, func(k dslKey) bool { return k.name == level })
		}
		assert.True(t, found,
			"dslKeys nests keys under %q, but no key of the DSL is called that, so nothing "+
				"an author can write ever reaches them", level)
	}

	// A step key is either a property of the step or the name of a task, and every
	// reader in the repo tells them apart by asking the registry. That only works
	// while the two vocabularies are disjoint, which v1 enforces where the name is
	// chosen: Register refuses a reserved one. This is the same rule checked from
	// the other side — over the table completion actually offers — because a
	// property added here and not there would reopen the ambiguity from an angle
	// the registry cannot see.
	for _, k := range dslKeys["steps"] {
		assert.True(t, v1.IsReservedStepKey(k.name),
			"completion offers %[1]q as a step key, but v1 does not reserve it, so a task could "+
				"be registered under that name and `%[1]s:` on a step would have two legitimate "+
				"readings", k.name)
	}
}

// TestConditionsAreFirstClass checks that a step's `if` gets the same treatment as
// an input: its expressions are parsed, its references resolve, and completion works
// inside it. A conditional the editor ignores is a conditional nobody trusts.
func TestConditionsAreFirstClass(t *testing.T) {
	t.Parallel()

	const src = `name: conditions
steps:
  - id: web
    http:
      url: https://example.com
  - id: guarded
    if: ${steps.web.status_code == 200}
    log:
      message: ok
edition: v2026.2
`
	c := newClient(t)
	c.initialize()
	const uri = "file:///conditions.yaml"

	t.Run("a valid condition is clean", func(t *testing.T) {
		assert.Empty(t, messages(c.open(uri, src).Diagnostics))
	})

	t.Run("hover resolves a reference in a condition", func(t *testing.T) {
		pos := positionOf(t, src, "web.status_code ==", 1)
		got := c.hover(uri, pos.Line, pos.Character)
		require.NotNil(t, got)
		assert.Contains(t, hoverText(got), "`steps.web.status_code`")
		assert.Contains(t, hoverText(got), "`int`")
	})

	t.Run("go to definition works from a condition", func(t *testing.T) {
		pos := positionOf(t, src, "web.status_code ==", 1)
		got := c.definition(uri, pos.Line, pos.Character)
		require.Len(t, got, 1)
		assert.Equal(t, "web", textInRange(src, got[0].Range))
	})

	t.Run("hover documents the if key itself", func(t *testing.T) {
		pos := positionOf(t, src, "if:", 0)
		got := c.hover(uri, pos.Line, pos.Character)
		require.NotNil(t, got)
		assert.Contains(t, hoverText(got), "whether the step runs")
	})

	t.Run("a syntax error in a condition is reported precisely", func(t *testing.T) {
		broken := strings.Replace(src, "${steps.web.status_code == 200}", "${steps.web.status_code ==}", 1)
		params := c.change(uri, broken, 2)
		require.Len(t, params.Diagnostics, 1)
		assert.Equal(t, codeCELSyntax, params.Diagnostics[0].Code)
		assert.Contains(t, params.Diagnostics[0].Message, "Syntax error")
	})

	t.Run("a forward reference in a condition is reported", func(t *testing.T) {
		// A condition resolves against the same names as any input, so it is
		// reference-checked too. This was a gap in the shared validator; it is
		// covered here because the editor inherits the fix rather than carrying
		// its own copy of the rule.
		const forward = `name: fwd
steps:
  - id: a
    if: ${steps.later.body == "x"}
    log:
      message: hi
  - id: later
    http:
      url: https://example.com
edition: v2026.2
`
		params := c.open("file:///cond-fwd.yaml", forward)
		require.Len(t, params.Diagnostics, 1, "got %v", messages(params.Diagnostics))
		assert.Contains(t, params.Diagnostics[0].Message, `references step "later", which runs later`)
		// And it lands on the condition, not on the step.
		assert.Equal(t, `${steps.later.body == "x"}`, textInRange(forward, params.Diagnostics[0].Range))
	})

	t.Run("completion offers earlier steps inside a condition", func(t *testing.T) {
		// A condition is an ordinary expression, so both namespaces behave in it
		// exactly as they do in an input: the root bare, the step ids under it.
		const partial = `name: c
steps:
  - id: web
    http:
      url: https://example.com
  - id: guarded
    if: ${PLACEHOLDER
edition: v2026.2
`
		for _, tt := range []struct {
			name  string
			typed string
			want  []string
		}{
			{name: "at the start of the expression", typed: "", want: []string{"steps"}},
			{name: "under the root", typed: "steps.", want: []string{"web"}},
		} {
			t.Run(tt.name, func(t *testing.T) {
				text, pos := splitCursor(t, strings.Replace(partial, "PLACEHOLDER", tt.typed+"|", 1))
				uri := "file:///cond-complete-" + strings.ReplaceAll(tt.name, " ", "-") + ".yaml"
				c.open(uri, text)

				// A prefix: the bare menu continues with the profile's functions,
				// which are the same in every expression position and so say
				// nothing about the claim here, which is that a condition is an
				// ordinary expression.
				got := labels(c.complete(uri, pos.Line, pos.Character).Items)
				require.GreaterOrEqual(t, len(got), len(tt.want))
				assert.Equal(t, tt.want, got[:len(tt.want)])
			})
		}
	})
}

// TestWaitUntilIsFirstClass checks that a wait's expression gets the same
// treatment as an input's or a condition's.
//
// It did not. The positional model had no entry for `wait_until:`, so its
// expression was invisible to every feature that reads one: hover and
// go-to-definition stopped at the fence, and a CEL error was left to the
// validator, which could only report it against the position it worked out —
// landing on the closing brace rather than on the character at fault.
//
// Rooting is what turned that from untidy into expensive. A wait now commonly
// holds `${steps.<id>.<output>}` — a moment that arrived as data rather than one
// the workflow chose — so the one kind of step whose expression the editor could
// not read is the one whose expression most often names another step. Offering
// `now` in a place where hover then says nothing would have made it worse.
func TestWaitUntilIsFirstClass(t *testing.T) {
	t.Parallel()

	// The moment waited for arrives as data — an http step's body — which is the
	// case the commentary above is about. It used to be a `cel:` step whose result
	// the wait read; that task is retired, and an expression on its own no longer
	// needs a step, so the only shape left where a wait names another step is one
	// that fetches.
	const src = `edition: v2026.2
name: waits
steps:
  - id: embargo
    http:
      url: https://example.com
  - id: hold
    wait_until: ${timestamp(steps.embargo.body)}
  - id: window
    wait_until: ${now + days(3)}
`
	c := newClient(t)
	c.initialize()
	const uri = "file:///waits.yaml"

	t.Run("a valid wait is clean", func(t *testing.T) {
		assert.Empty(t, messages(c.open(uri, src).Diagnostics))
	})

	t.Run("hover resolves a reference in a wait", func(t *testing.T) {
		pos := positionOf(t, src, "steps.embargo.body", len("steps."))
		got := c.hover(uri, pos.Line, pos.Character)
		require.NotNil(t, got, "no hover on a wait's expression")
		assert.Contains(t, hoverText(got), "`steps.embargo.body`")
		assert.Contains(t, hoverText(got), "`http` task")
	})

	t.Run("go to definition works from a wait", func(t *testing.T) {
		pos := positionOf(t, src, "steps.embargo.body", len("steps."))
		got := c.definition(uri, pos.Line, pos.Character)
		require.Len(t, got, 1)
		assert.Equal(t, "embargo", textInRange(src, got[0].Range))
	})

	t.Run("hover describes the clock", func(t *testing.T) {
		pos := positionOf(t, src, "${now + days(3)}", 2)
		got := c.hover(uri, pos.Line, pos.Character)
		require.NotNil(t, got, "no hover on the one identifier whose availability depends on position")
		text := hoverText(got)
		assert.Contains(t, text, "the moment the wait is evaluated")
		// The duration builders come from the evaluator rather than a list here,
		// so a unit added to it appears without this test being touched.
		for _, unit := range v1.DurationUnits() {
			assert.Contains(t, text, "`"+unit+"`")
		}

		require.NotNil(t, got.Range)
		assert.Equal(t, "now", textInRange(src, *got.Range))
	})

	t.Run("scoping applies inside a wait", func(t *testing.T) {
		// A wait is a step like any other, so it sees what a step at its position
		// sees. A loop body's outputs are not that.
		const leaky = `name: leaky-wait
steps:
  - id: loop
    for_each:
      items: "${['a']}"
      steps:
        - id: inner
          http:
            url: https://example.com
  - id: hold
    wait_until: ${timestamp(steps.inner.body)}
edition: v2026.2
`
		const leakyURI = "file:///leaky-wait.yaml"
		params := c.open(leakyURI, leaky)
		require.Len(t, params.Diagnostics, 1, "got %v", messages(params.Diagnostics))
		assert.Contains(t, params.Diagnostics[0].Message, `references unknown step "inner"`)

		pos := positionOf(t, leaky, "steps.inner.body", len("steps."))
		assert.Nil(t, c.hover(leakyURI, pos.Line, pos.Character),
			"hover resolved a loop body step from a wait outside the loop")
		assert.Empty(t, c.definition(leakyURI, pos.Line, pos.Character))
	})

	t.Run("a forward reference in a wait is reported on the expression", func(t *testing.T) {
		const forward = `name: fwd-wait
steps:
  - id: hold
    wait_until: ${timestamp(steps.later.body)}
  - id: later
    http:
      url: https://example.com
edition: v2026.2
`
		params := c.open("file:///fwd-wait.yaml", forward)
		require.Len(t, params.Diagnostics, 1, "got %v", messages(params.Diagnostics))
		assert.Contains(t, params.Diagnostics[0].Message, `references step "later", which runs later`)
		assert.Equal(t, "${timestamp(steps.later.body)}", textInRange(forward, params.Diagnostics[0].Range))
	})
}

// TestWaitUntilSyntaxErrorLandsOnTheOffendingCharacter is the whole point of
// putting the wait's expression in the model, so it is asserted as a position
// rather than as "something was reported".
//
// Before, the validator's report was the only one, and it arrived with a position
// that resolved to the closing brace — a range that is inside the right value and
// under the wrong character, which is the failure mode a weaker assertion cannot
// tell from a fix. Now the expression is parsed here, where the offset of the
// error within the source is known.
func TestWaitUntilSyntaxErrorLandsOnTheOffendingCharacter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		wait string
		// underlines is the exact source text the diagnostic must cover.
		underlines string
	}{
		{
			name: "an extraneous identifier",
			wait: "${now b}",
			// The token at fault, not the fence and not the brace after it.
			underlines: "b",
		},
		{
			name:       "a doubled operator",
			wait:       "${now + + days(3)}",
			underlines: "+",
		},
		{
			// An error CEL reports at the end of the input has no character to
			// land on, so the whole expression is the tightest honest answer —
			// still narrower than the value and still inside the fence.
			name:       "an expression that stops early",
			wait:       "${now + }",
			underlines: "${now + }",
		},
	}

	c := newClient(t)
	c.initialize()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := "name: broken-wait\nsteps:\n  - id: hold\n    wait_until: " + tt.wait + "\n" + editionSuffix
			uri := "file:///broken-wait-" + strings.ReplaceAll(tt.name, " ", "-") + ".yaml"
			params := c.open(uri, src)

			// Exactly one. Both this package and the validator notice a wait that
			// will not parse, and a doubled squiggle is two reports of one
			// mistake — the tighter range is the one that survives.
			require.Len(t, params.Diagnostics, 1, "got %v", messages(params.Diagnostics))
			assert.Equal(t, codeCELSyntax, params.Diagnostics[0].Code)
			assert.Contains(t, params.Diagnostics[0].Message, "Syntax error")
			assert.Equal(t, tt.underlines, textInRange(src, params.Diagnostics[0].Range))

			// The second `+` is the one at fault, and it is the second occurrence
			// in the line. Asserting the text alone would pass on either, so the
			// column is checked against where the fixture actually puts it.
			if tt.name == "a doubled operator" {
				want := strings.Index(src, "+ days")
				assert.Equal(t, want, offsetOf(src, params.Diagnostics[0].Range.Start),
					"underlined the first operator rather than the one at fault")
			}
		})
	}
}

// TestNowIsExplainedTheSameWayTheValidatorRefusesIt guards the one duplication
// this change could not avoid.
//
// `flowfile` refuses `now` in a task input with a sentence explaining why the name
// is bound in a wait and nowhere else, and the editor now explains the same thing
// on hover and in completion. The string is unexported, so the editor cannot show
// the validator's own words; what it can do is fail when the two accounts diverge.
//
// Each clause below is asserted to appear in *both*, which is what makes this a
// guard rather than a third copy: rewording either side turns it red and whoever
// does the rewording sees the other.
func TestNowIsExplainedTheSameWayTheValidatorRefusesIt(t *testing.T) {
	t.Parallel()

	const src = `name: no-clock
steps:
  - id: a
    log:
      message: ${now}
edition: v2026.2
`
	c := newClient(t)
	c.initialize()
	const uri = "file:///no-clock.yaml"

	params := c.open(uri, src)
	require.Len(t, params.Diagnostics, 1, "got %v", messages(params.Diagnostics))
	refusal := params.Diagnostics[0].Message
	require.Contains(t, refusal, v1.NowIdentifier,
		"premise: the diagnostic under test must be the one about the clock")

	// The three claims the refusal rests on. They are what an author needs in
	// order to understand the rule rather than merely obey it.
	for _, claim := range []string{
		"the moment the wait is evaluated",
		"resolved inside an activity",
		"no clock that survives a retry",
	} {
		assert.Contains(t, refusal, claim,
			"the validator no longer makes this claim; the editor's copy in nowDoc has drifted from it")
		assert.Contains(t, nowDoc(), claim,
			"the editor no longer makes this claim; it has drifted from the validator's refusal")
	}

	// And the editor stays quiet where the name is not bound. Describing it here
	// would contradict the squiggle the author is looking at.
	pos := positionOf(t, src, "${now}", 2)
	assert.Nil(t, c.hover(uri, pos.Line, pos.Character),
		"hover described the clock in a task input, where the validator refuses it")

	// The editor's own two surfaces say one thing, which is the duplication it
	// *could* avoid and therefore must: an author who accepts the candidate and
	// then hovers what they accepted is looking at the same name twice.
	t.Run("completion and hover show one account of it", func(t *testing.T) {
		const bound = `name: c
steps:
  - id: window
    wait_until: ${now}
edition: v2026.2
`
		const boundURI = "file:///one-account.yaml"
		require.Empty(t, messages(c.open(boundURI, bound).Diagnostics),
			"premise: a wait naming the clock is a document the compiler accepts")

		// Completion, at the position where the name is being typed.
		typing := positionOf(t, bound, "${now}", len("${"))
		item := findItem(c.complete(boundURI, typing.Line, typing.Character).Items, v1.NowIdentifier)
		require.NotNil(t, item, "premise: the clock must be offered where it is bound")
		assert.Equal(t, plainText(nowDoc()), item.Documentation)

		// And hover, on the name once written. Trimmed because the harness joins
		// the protocol's content blocks with a newline apiece.
		got := c.hover(boundURI, typing.Line, typing.Character+1)
		require.NotNil(t, got)
		assert.Equal(t, nowDoc(), strings.TrimSpace(hoverText(got)))
	})
}

// TestWaitKeysAreDocumentedAtTheirOwnLevel is the regression guard for the class
// of mistake waitForSignalEntry exists to prevent, checked again now that a second
// wait key is in the model.
//
// `timeout:` means two different things one level apart — the step's bounds one
// attempt at it, a gate's bounds how long it waits before reporting `timed_out` —
// and hovering one used to answer with the other's documentation. Adding
// `wait_until` must not reopen that, and the way it could is by opening a level of
// its own; it does not, because its value is one expression rather than a mapping.
func TestWaitKeysAreDocumentedAtTheirOwnLevel(t *testing.T) {
	t.Parallel()

	// The step-level `timeout:` sits on the task step, because a waiting step may
	// not carry one — the validator says so, and a fixture that ignored it would
	// be testing hover against a document `flow validate` refuses.
	const src = `edition: v2026.2
name: waits
steps:
  - id: fetch
    timeout: 1m
    log:
      message: hi
  - id: nap
    sleep: 30s
  - id: window
    wait_until: ${now + days(3)}
  - id: approval
    wait_for_signal:
      name: deploy-approved
      timeout: 24h
`
	c := newClient(t)
	c.initialize()
	const uri = "file:///wait-keys.yaml"
	require.Empty(t, messages(c.open(uri, src).Diagnostics))

	// Each key's own documentation, taken from the table rather than quoted, so
	// that rewording an entry cannot leave this asserting text nothing shows.
	docFor := func(t *testing.T, level, name string) string {
		t.Helper()
		k, ok := lookupDSLKey(level, name)
		require.True(t, ok, "no %q key at the %q level", name, level)
		return k.docs
	}

	tests := []struct {
		name string
		// key, minIndent and after locate the declaration to hover.
		key       string
		minIndent int
		after     string
		// level is where the documentation shown must come from.
		level string
		// notLevel is the other level declaring the same key, whose
		// documentation must not be what is shown.
		notLevel string
	}{
		{name: "sleep", key: "sleep", minIndent: 4, level: "steps"},
		{name: "wait_until", key: "wait_until", minIndent: 4, level: "steps"},
		{
			name: "a step's own timeout", key: "timeout", minIndent: 4,
			level: "steps", notLevel: "wait_for_signal",
		},
		{
			name: "a gate's timeout", key: "timeout", minIndent: 6, after: "wait_for_signal:",
			level: "wait_for_signal", notLevel: "steps",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pos := positionOfKey(t, src, tt.key, tt.minIndent, tt.after)
			got := c.hover(uri, pos.Line, pos.Character)
			require.NotNil(t, got, "no hover for %q at %v", tt.key, pos)

			text := hoverText(got)
			assert.Contains(t, text, docFor(t, tt.level, tt.key))
			if tt.notLevel != "" {
				assert.NotContains(t, text, docFor(t, tt.notLevel, tt.key),
					"hovering %q answered with the documentation for the %q of the other level",
					tt.key, tt.key)
			}
		})
	}
}

// TestNestedStepsAreFirstClass checks that a step inside a for_each body or a
// parallel branch gets the same treatment as one at the top level.
//
// Steps nest now, and a feature that stops at the outer level is worse than absent:
// the author sees diagnostics and hover on some steps and silence on others, with
// nothing to explain the difference.
func TestNestedStepsAreFirstClass(t *testing.T) {
	t.Parallel()

	// The list the loop walks arrives from a fetch. It was a `cel:` step holding a
	// literal list, which is a `vars:` binding now — but the cases below need a
	// reference that *resolves to a step*, in the items expression and in a nested
	// input, so the producer has to remain a step.
	const src = `name: nested
steps:
  - id: targets
    http:
      url: https://example.com
  - id: loop
    for_each:
      items: ${steps.targets.json}
      as: one
      steps:
        - id: body
          log:
            mesage: ${steps.targets.json}
  - id: branches
    parallel:
      - steps:
          - id: left
            http:
              method: GET
edition: v2026.2
`
	c := newClient(t)
	c.initialize()
	const uri = "file:///nested.yaml"
	params := c.open(uri, src)

	t.Run("diagnostics reach inside a loop body", func(t *testing.T) {
		found := false
		for _, d := range params.Diagnostics {
			if d.Code == codeGeneral && textInRange(src, d.Range) == "mesage" {
				found = true
			}
		}
		assert.True(t, found, "the typo inside the loop body was not reported: %v", messages(params.Diagnostics))
	})

	t.Run("diagnostics reach inside a parallel branch", func(t *testing.T) {
		found := false
		for _, d := range params.Diagnostics {
			if strings.Contains(d.Message, `requires input "url"`) {
				found = true
			}
		}
		assert.True(t, found, "the missing input inside the branch was not reported: %v", messages(params.Diagnostics))
	})

	t.Run("hover works on a nested step's task", func(t *testing.T) {
		// This used to point at the `name:` key of the `task:` block two levels
		// inside the loop body, at an indent of 12. The block is gone: a nested
		// step names its task with a key of its own, so the same question — does
		// hover reach a step that is not at the top level? — is now asked of the
		// `log:` at an indent of 10. Only the loop body's task key is that deep;
		// the branch's `http:` is deeper still, and the first step's is at 4.
		pos := positionOfKey(t, src, "log", 10, "")
		got := c.hover(uri, pos.Line, pos.Character)
		require.NotNil(t, got, "no hover on a key inside a loop body")
		// And it is the task that was described, rather than whatever else happens
		// to sit at that position — which is what the old `name:` anchor could no
		// longer distinguish.
		assert.Contains(t, hoverText(got), "task `log`")
	})

	t.Run("a loop's items expression resolves references", func(t *testing.T) {
		pos := positionOf(t, src, "${steps.targets.json}", len("${steps."))
		got := c.hover(uri, pos.Line, pos.Character)
		require.NotNil(t, got, "no hover on the items expression")
		assert.Contains(t, hoverText(got), "`steps.targets.json`")

		def := c.definition(uri, pos.Line, pos.Character)
		require.Len(t, def, 1)
		assert.Equal(t, "targets", textInRange(src, def[0].Range))
	})

	t.Run("the outline includes nested steps and says where they are", func(t *testing.T) {
		got := c.symbols(uri)
		byName := map[string]string{}
		for _, s := range got {
			byName[s.Name] = s.ContainerName
		}
		assert.Equal(t, "http", byName["targets"])
		assert.Equal(t, "for_each", byName["loop"])
		assert.Equal(t, "parallel", byName["branches"])
		// Nesting is otherwise invisible in a flat outline.
		assert.Equal(t, "log in loop", byName["body"])
		assert.Equal(t, "http in branches", byName["left"])
	})

	t.Run("nested step ranges are disjoint so positions resolve to the innermost", func(t *testing.T) {
		got := c.symbols(uri)
		for i := range got {
			for j := i + 1; j < len(got); j++ {
				a, b := got[i].Location.Range, got[j].Location.Range
				assert.False(t, a.End.Line >= b.Start.Line && b.End.Line >= a.Start.Line,
					"%s and %s overlap", got[i].Name, got[j].Name)
			}
		}
	})
}

// TestReferenceScoping is the test that matters most for completion, because these
// are the cases where offering a name would be actively wrong rather than merely
// unhelpful.
//
// The rules are the engine's, mirrored from flowfile's validator: a loop body's
// outputs do not escape the loop, a parallel block's branch outputs do merge once
// it joins, and one branch cannot see a sibling's.
//
// Rooting adds a second axis to every one of them. A position is now in one of two
// namespaces, and each case is asked in both: the names bound bare where the
// cursor is, and the step ids under the root. Asking only one would leave the way
// the boundary actually breaks untested — not a name missing from a menu, but a
// name offered in the menu next door, where it cannot resolve. So each direction
// also asserts that the *other* namespace's names are absent, which is the
// negative direction CLAUDE.md asks for and the reason the two lists are written
// out separately rather than concatenated.
func TestReferenceScoping(t *testing.T) {
	t.Parallel()

	const src = `name: scoping
steps:
  - id: before
    log:
      message: hi
  - id: loop
    for_each:
      items: ${['a', 'b']}
      as: each
      steps:
        - id: body_one
          log:
            message: hi
        - id: body_two
          log:
            message: PLACEHOLDER_BODY
  - id: fan
    parallel:
      - steps:
          - id: left
            log:
              message: PLACEHOLDER_LEFT
      - steps:
          - id: right
            log:
              message: hi
  - id: after
    log:
      message: PLACEHOLDER_AFTER
edition: v2026.2
`

	tests := []struct {
		name string
		// at is the placeholder to put the cursor's ${ in place of.
		at string
		// bare is the candidate list at the start of an expression, up to where the
		// profile's functions begin: the
		// names bound where the cursor is, then the root.
		bare []string
		// rooted is the exact candidate list after `steps.`, nearest first.
		rooted []string
		// notWant names candidates that must appear in neither, with the reason.
		notWant map[string]string
	}{
		{
			name: "inside a loop body",
			at:   "PLACEHOLDER_BODY",
			// The loop binds its item here, and the root is how everything else
			// is reached.
			bare: []string{"each", "steps"},
			// The earlier body step, and the step before the loop.
			rooted: []string{"body_one", "before"},
			notWant: map[string]string{
				"body_two": "a step cannot reference itself",
				"loop":     "the enclosing loop has not finished, so it has no results yet",
				"left":     "a parallel branch that has not run yet",
				"after":    "a later step",
				"now":      "the clock is bound in a wait's expressions, not in a task input",
			},
		},
		{
			name: "after the loop, body steps are gone",
			at:   "PLACEHOLDER_AFTER",
			bare: []string{"steps"},
			// The loop reports its iterations through its own results output, so
			// only its id survives.
			// Nearest first is strict reverse document order, so the branch steps
			// come before the block that contains them.
			rooted: []string{"right", "left", "fan", "loop", "before"},
			notWant: map[string]string{
				"body_one": "a loop body's outputs do not escape the loop",
				"body_two": "a loop body's outputs do not escape the loop",
				"each":     "the iterator exists only inside the body",
			},
		},
		{
			name:   "inside a parallel branch, a sibling branch is invisible",
			at:     "PLACEHOLDER_LEFT",
			bare:   []string{"steps"},
			rooted: []string{"loop", "before"},
			notWant: map[string]string{
				"fan":      "the enclosing parallel block has not joined yet",
				"right":    "branches are unordered, so a sibling may not be referenced",
				"body_one": "a loop body's outputs do not escape the loop",
				"after":    "a later step",
				"each":     "a branch is not a loop body, so nothing binds an item here",
			},
		},
	}

	c := newClient(t)
	c.initialize()

	for _, tt := range tests {
		for _, direction := range []struct {
			name string
			// typed is what stands between the `${` and the cursor.
			typed string
			want  []string
			// absent is the other namespace's list, which must not appear here.
			absent []string
		}{
			{name: "bare", typed: "", want: tt.bare, absent: tt.rooted},
			{name: "rooted", typed: "steps.", want: tt.rooted, absent: tt.bare},
		} {
			t.Run(tt.name+", "+direction.name, func(t *testing.T) {
				// Only the case under test gets a cursor; the others become
				// literals.
				text := src
				for _, p := range []string{"PLACEHOLDER_BODY", "PLACEHOLDER_LEFT", "PLACEHOLDER_AFTER"} {
					if p == tt.at {
						text = strings.Replace(text, p, "${"+direction.typed+"|", 1)
						continue
					}
					text = strings.Replace(text, p, "hi", 1)
				}
				clean, pos := splitCursor(t, text)

				uri := "file:///scope-" + strings.ReplaceAll(tt.name+"-"+direction.name, " ", "-") + ".yaml"
				c.open(uri, clean)
				got := labels(c.complete(uri, pos.Line, pos.Character).Items)

				// A prefix rather than the whole list, because the bare menu
				// continues with the profile's functions — sixty names that are
				// the same wherever the cursor is, and so say nothing about
				// scoping, which is what this test is about. What matters here is
				// that the names in scope come first and in this order.
				require.GreaterOrEqual(t, len(got), len(direction.want),
					"fewer candidates than the names that must be in scope")
				assert.Equal(t, direction.want, got[:len(direction.want)])
				for name, why := range tt.notWant {
					assert.NotContains(t, got, name, why)
				}
				for _, name := range direction.absent {
					if name == "steps" {
						// The root is not a step id, so it is legitimately absent
						// from the rooted menu — and it is what the bare menu is
						// there to offer. Nothing to assert either way.
						continue
					}
					assert.NotContains(t, got, name,
						"%q belongs to the other namespace; offering it here produces a reference that cannot resolve", name)
				}
			})
		}
	}
}

// TestScopingAppliesToHoverAndDefinition checks that the same rules govern the
// features that resolve a reference, not only the one that offers it. Describing a
// reference the engine rejects would contradict the diagnostics.
func TestScopingAppliesToHoverAndDefinition(t *testing.T) {
	t.Parallel()

	const src = `name: leak
steps:
  - id: loop
    for_each:
      items: "${['a']}"
      steps:
        - id: inner
          http:
            url: https://example.com
  - id: after
    log:
      message: ${steps.inner.body}
edition: v2026.2
`
	c := newClient(t)
	c.initialize()
	const uri = "file:///leak.yaml"
	c.open(uri, src)

	pos := positionOf(t, src, "${steps.inner.body}", len("${steps."))

	// flowfile reports this as an unknown step; hover and definition must not
	// contradict it by resolving what the engine will not.
	assert.Nil(t, c.hover(uri, pos.Line, pos.Character),
		"hover resolved a loop body step from outside the loop")
	assert.Empty(t, c.definition(uri, pos.Line, pos.Character),
		"definition jumped to a loop body step from outside the loop")
}

// TestLoopIteratorIsDescribed checks hover and go-to-definition for a loop's
// iterator, which is a name that resolves to an item rather than to a step.
func TestLoopIteratorIsDescribed(t *testing.T) {
	t.Parallel()

	const src = `name: iterator
steps:
  - id: repeat
    for_each:
      items: "${['a', 'b']}"
      as: target
      steps:
        - id: body
          log:
            message: ${target}
edition: v2026.2
`
	c := newClient(t)
	c.initialize()
	const uri = "file:///iterator.yaml"
	require.Empty(t, messages(c.open(uri, src).Diagnostics))

	pos := positionOf(t, src, "${target}", 3)

	got := c.hover(uri, pos.Line, pos.Character)
	require.NotNil(t, got, "no hover on a loop iterator")
	assert.Contains(t, hoverText(got), "current item of the `repeat` loop")
	assert.Contains(t, hoverText(got), "do not escape")

	// The loop that binds it is the only declaration to jump to.
	def := c.definition(uri, pos.Line, pos.Character)
	require.Len(t, def, 1)
	assert.Equal(t, "repeat", textInRange(src, def[0].Range))
}

// TestDeferredInputsAreNotStepReferences guards the false-positive trap the http
// task's output shaping sets: its `outputs` expression resolves against the HTTP
// response, not against step outputs, so status_code and body are not step names.
func TestDeferredInputsAreNotStepReferences(t *testing.T) {
	t.Parallel()

	const src = `name: shaping
steps:
  - id: web
    http:
      method: GET
      url: https://example.com/json
      outputs: "${ {'status': status_code, 'title': body} }"
edition: v2026.2
`
	c := newClient(t)
	c.initialize()
	const uri = "file:///shaping.yaml"

	assert.Empty(t, messages(c.open(uri, src).Diagnostics),
		"output shaping must not be reported as unknown step references")

	// Hover over a response variable says nothing rather than inventing a step.
	pos := positionOf(t, src, "status_code", 2)
	assert.Nil(t, c.hover(uri, pos.Line, pos.Character))
}

// TestDurationsAreChecked covers the per-step policy durations.
//
// The validator owns the rule and the wording; what is checked here is that its
// report reaches the editor and lands on the offending value rather than on the
// step. Duplicating the rule was the earlier mistake — the editor then had its own
// idea of what a duration is.
func TestDurationsAreChecked(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policy     string
		wantText   string
		underlines string
	}{
		{
			name:   "valid timeout and retry",
			policy: "    timeout: 30s\n    retry:\n      attempts: 3\n      interval: 1s\n      max_interval: 1m\n",
		},
		{
			name:       "a timeout that is not a duration",
			policy:     "    timeout: soon\n",
			wantText:   `timeout "soon" is not a duration`,
			underlines: "soon",
		},
		{
			name:       "a zero timeout",
			policy:     "    timeout: 0s\n",
			wantText:   `timeout "0s" must be greater than zero`,
			underlines: "0s",
		},
		{
			name:       "a bad retry interval names the field",
			policy:     "    retry:\n      interval: whenever\n",
			wantText:   `retry interval "whenever" is not a duration`,
			underlines: "whenever",
		},
	}

	c := newClient(t)
	c.initialize()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := "name: durations\nsteps:\n  - id: a\n" + tt.policy +
				"    log:\n      message: hi\n" + editionSuffix
			uri := "file:///duration-" + strings.ReplaceAll(tt.name, " ", "-") + ".yaml"
			params := c.open(uri, src)

			if tt.wantText == "" {
				assert.Empty(t, messages(params.Diagnostics))
				return
			}
			require.Len(t, params.Diagnostics, 1, "got %v", messages(params.Diagnostics))
			assert.Equal(t, codeGeneral, params.Diagnostics[0].Code)
			assert.Contains(t, params.Diagnostics[0].Message, tt.wantText)
			assert.Equal(t, tt.underlines, textInRange(src, params.Diagnostics[0].Range))
		})
	}
}

// TestHoverDocumentsEveryDSLKey checks that every key the shape table declares is
// reachable through hover, so the table cannot contain an entry nothing shows.
//
// The document's own keys used to be excluded, on the grounds that hover only ever
// resolved a key inside a step. That was a description of the implementation rather
// than a reason: it held while every top-level key said what it meant in the value
// beside it, and `edition: 2026.1` does not. They are covered here now, which is
// what keeps the exclusion from quietly returning.
func TestHoverDocumentsEveryDSLKey(t *testing.T) {
	t.Parallel()

	// The edition is the one this build compiles rather than a literal, so the
	// fixture stays a document `flow validate` accepts when a new one is added.
	src := "edition: " + flowfile.CurrentEdition + "\n" + `name: all-keys
description: everything
vars:
  region: eu-west-1
steps:
  - id: a
    description: Say hello, so the rest of the run has something to say it about.
    if: ${true}
    vars:
      greeting: hi
    timeout: 30s
    continue_on_error: true
    retry:
      attempts: 3
      interval: 1s
      backoff: 2
      max_interval: 1m
    log:
      message: hi
    undo:
      log:
        message: undone
  - id: loop
    for_each:
      items: ${['a', 'b']}
      as: one
      max_parallel: 2
      steps:
        - id: body
          log:
            message: hi
  - id: poll
    loop:
      as: cursor
      init: ${0}
      update: ${cursor + 1}
      until: ${cursor >= 2}
      max_iterations: 10
      steps:
        - id: page
          log:
            message: hi
  - id: named
    value: ${1 + 1}
  - id: branches
    parallel:
      - steps:
          - id: left
            log:
              message: hi
  - id: pause
    sleep: 30s
  - id: window
    wait_until: ${now + days(3)}
  - id: approval
    wait_for_signal:
      name: deploy-approved
      prompt: approve the deploy?
      timeout: 24h
      outputs:
        approved: ${has(payload.approved) && payload.approved}
  - id: provision
    call: ./callee.yaml
    with:
      tenant: acme
`
	c := newClient(t)
	c.initialize()
	const uri = "file:///all-keys.yaml"
	c.open(uri, src)

	for level, keys := range dslKeys {
		for _, k := range keys {
			if level == "parallel" {
				// A parallel branch is a bare `- steps:` list, whose only key is
				// the same `steps` the document already declares at the top; hover
				// resolves it at the outer level, which says the same thing.
				continue
			}
			t.Run(level+"."+k.name, func(t *testing.T) {
				// Keys such as name, description, steps and timeout exist at more
				// than one level, so the search has to be told which one is meant.
				// Indentation separates most of them; `name` needs more, because a
				// task's and a gate's sit at the same depth, so the search starts
				// from the line that opens the block.
				minIndent := map[string]int{
					"steps": 4, "retry": 6, "for_each": 6, "wait_for_signal": 6,
				}[level]
				// The document's own keys sit under no key at all, so there is
				// nothing to search past for them — and asking to start after a
				// bare ":" would skip the first line of the file, which is where
				// an edition is written.
				after := ""
				if level != "" {
					after = level + ":"
				}
				pos := positionOfKey(t, src, k.name, minIndent, after)
				got := c.hover(uri, pos.Line, pos.Character)
				require.NotNil(t, got, "no hover for the %s key at %v", k.name, pos)
				assert.Contains(t, hoverText(got), k.docs)
			})
		}
	}
}

// positionOfKey returns a position on the name of the line declaring a key at or
// deeper than minIndent, which is how a key that exists at several levels — name and
// description do — is disambiguated.
func positionOfKey(t *testing.T, src, key string, minIndent int, after string) lsp.Position {
	t.Helper()

	// Nothing to skip past for a key whose level is the step itself.
	started := after == "" || !strings.Contains(src, after)

	for i, line := range strings.Split(src, "\n") {
		if !started {
			started = strings.Contains(line, after)
			continue
		}

		m := keyLine.FindStringSubmatch(line)
		if m == nil || m[3] != key || len(m[1])+len(m[2]) < minIndent {
			continue
		}
		return lsp.Position{
			Line:      i,
			Character: utf16Len(m[1]+m[2]) + 1,
		}
	}
	t.Fatalf("test source declares no key %q", key)
	return lsp.Position{}
}

// ptr returns a pointer to a string, for the schema's optional fields.
func ptr(s string) *string { return &s }
