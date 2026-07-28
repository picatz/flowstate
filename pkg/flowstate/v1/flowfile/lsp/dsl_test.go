package lsp

import (
	"strings"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
)

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
func TestDSLKeysMatchTheDSL(t *testing.T) {
	t.Parallel()

	// A workflow exercising every field and every step kind the DSL can express.
	workflow := &v1.Workflow{
		Name:        "every-field",
		Description: ptr("described"),
		Steps: []*v1.Node{
			{
				Id:        "only",
				Condition: v1.NewExpr("true"),
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
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name:        "echo",
					Description: ptr("a step"),
					Inputs:      map[string]*v1.Value{"message": v1.NewValue("hi")},
				}},
			},
			{
				Id: "loop",
				Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
					Items:       v1.NewLiteralList("a", "b"),
					Iterator:    "each",
					MaxParallel: 2,
					Body:        []*v1.Node{{Id: "inner", Kind: &v1.Node_Task{Task: &v1.Task{Name: "echo"}}}},
				}},
			},
			{
				Id: "branches",
				Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{
					Branches: []*v1.Parallel_Branch{
						{Steps: []*v1.Node{{Id: "left", Kind: &v1.Node_Task{Task: &v1.Task{Name: "echo"}}}}},
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
					Kind:    &v1.Wait_Signal{Signal: &v1.Signal{Name: "deploy-approved"}},
					Timeout: durationpb.New(3_600_000_000_000),
				}},
			},
		},
	}

	rendered, err := flowfile.Marshal(workflow)
	require.NoError(t, err, "the DSL round trip must work for this test to mean anything")

	// Every key the DSL emits must be one completion knows about, at some level.
	known := map[string]bool{}
	for _, keys := range dslKeys {
		for _, k := range keys {
			known[k.name] = true
		}
	}
	// Task input names come from the task's schema, not from the shape table.
	for _, def := range v1.DefaultRegistry().All() {
		for _, name := range fieldNames(def.Inputs) {
			known[name] = true
		}
	}

	var missing []string
	for _, line := range strings.Split(string(rendered), "\n") {
		m := keyLine.FindStringSubmatch(line)
		if m == nil || known[m[3]] {
			continue
		}
		missing = append(missing, m[3])
	}
	assert.Empty(t, missing,
		"the Flowfile DSL emits keys this package does not know about; add them to dslKeys "+
			"(and to the parsed model if they carry expressions or durations).\nRendered:\n%s", rendered)
}

// TestConditionsAreFirstClass checks that a step's `if` gets the same treatment as
// an input: its expressions are parsed, its references resolve, and completion works
// inside it. A conditional the editor ignores is a conditional nobody trusts.
func TestConditionsAreFirstClass(t *testing.T) {
	t.Parallel()

	const src = `name: conditions
steps:
  - id: web
    task:
      name: http
      inputs:
        url: https://example.com
  - id: guarded
    if: ${web.status_code == 200}
    task:
      name: echo
      inputs:
        message: ok
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
		assert.Contains(t, hoverText(got), "`web.status_code`")
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
		broken := strings.Replace(src, "${web.status_code == 200}", "${web.status_code ==}", 1)
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
    if: ${later.result == "x"}
    task:
      name: echo
      inputs:
        message: hi
  - id: later
    task:
      name: echo
      inputs:
        message: hi
`
		params := c.open("file:///cond-fwd.yaml", forward)
		require.Len(t, params.Diagnostics, 1, "got %v", messages(params.Diagnostics))
		assert.Contains(t, params.Diagnostics[0].Message, `references step "later", which runs later`)
		// And it lands on the condition, not on the step.
		assert.Equal(t, `${later.result == "x"}`, textInRange(forward, params.Diagnostics[0].Range))
	})

	t.Run("completion offers earlier steps inside a condition", func(t *testing.T) {
		partial, pos := splitCursor(t, `name: c
steps:
  - id: web
    task:
      name: http
      inputs:
        url: https://example.com
  - id: guarded
    if: ${|
`)
		c.open("file:///cond-complete.yaml", partial)
		got := c.complete("file:///cond-complete.yaml", pos.Line, pos.Character)
		assert.Equal(t, []string{"web"}, labels(got.Items))
	})
}

// TestNestedStepsAreFirstClass checks that a step inside a for_each body or a
// parallel branch gets the same treatment as one at the top level.
//
// Steps nest now, and a feature that stops at the outer level is worse than absent:
// the author sees diagnostics and hover on some steps and silence on others, with
// nothing to explain the difference.
func TestNestedStepsAreFirstClass(t *testing.T) {
	t.Parallel()

	const src = `name: nested
steps:
  - id: targets
    task:
      name: cel
      inputs:
        expr: "['a', 'b']"
  - id: loop
    for_each:
      items: ${targets.result}
      iterator: one
      steps:
        - id: body
          task:
            name: echo
            inputs:
              mesage: ${targets.result}
  - id: branches
    parallel:
      - steps:
          - id: left
            task:
              name: http
              inputs:
                method: GET
`
	c := newClient(t)
	c.initialize()
	const uri = "file:///nested.yaml"
	params := c.open(uri, src)

	t.Run("diagnostics reach inside a loop body", func(t *testing.T) {
		found := false
		for _, d := range params.Diagnostics {
			if d.Code == codeFlowfile && textInRange(src, d.Range) == "mesage" {
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
		pos := positionOfKey(t, src, "name", 12, "")
		got := c.hover(uri, pos.Line, pos.Character)
		require.NotNil(t, got, "no hover on a key inside a loop body")
	})

	t.Run("a loop's items expression resolves references", func(t *testing.T) {
		pos := positionOf(t, src, "${targets.result}", 3)
		got := c.hover(uri, pos.Line, pos.Character)
		require.NotNil(t, got, "no hover on the items expression")
		assert.Contains(t, hoverText(got), "`targets.result`")

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
		assert.Equal(t, "cel", byName["targets"])
		assert.Equal(t, "for_each", byName["loop"])
		assert.Equal(t, "parallel", byName["branches"])
		// Nesting is otherwise invisible in a flat outline.
		assert.Equal(t, "echo in loop", byName["body"])
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
func TestReferenceScoping(t *testing.T) {
	t.Parallel()

	const src = `name: scoping
steps:
  - id: before
    task:
      name: echo
      inputs:
        message: hi
  - id: loop
    for_each:
      items: ${before.result}
      iterator: each
      steps:
        - id: body_one
          task:
            name: echo
            inputs:
              message: hi
        - id: body_two
          task:
            name: echo
            inputs:
              message: PLACEHOLDER_BODY
  - id: fan
    parallel:
      - steps:
          - id: left
            task:
              name: echo
              inputs:
                message: PLACEHOLDER_LEFT
      - steps:
          - id: right
            task:
              name: echo
              inputs:
                message: hi
  - id: after
    task:
      name: echo
      inputs:
        message: PLACEHOLDER_AFTER
`

	tests := []struct {
		name string
		// at is the placeholder to put the cursor's ${ in place of.
		at string
		// want is the exact candidate list, nearest first.
		want []string
		// notWant names candidates that must never appear, with the reason.
		notWant map[string]string
	}{
		{
			name: "inside a loop body",
			at:   "PLACEHOLDER_BODY",
			// The iterator, the earlier body step, and the step before the loop.
			want: []string{"each", "body_one", "before"},
			notWant: map[string]string{
				"body_two": "a step cannot reference itself",
				"loop":     "the enclosing loop has not finished, so it has no results yet",
				"left":     "a parallel branch that has not run yet",
				"after":    "a later step",
			},
		},
		{
			name: "after the loop, body steps are gone",
			at:   "PLACEHOLDER_AFTER",
			// The loop reports its iterations through its own results output, so
			// only its id survives.
			// Nearest first is strict reverse document order, so the branch steps
			// come before the block that contains them.
			want: []string{"right", "left", "fan", "loop", "before"},
			notWant: map[string]string{
				"body_one": "a loop body's outputs do not escape the loop",
				"body_two": "a loop body's outputs do not escape the loop",
				"each":     "the iterator exists only inside the body",
			},
		},
		{
			name: "inside a parallel branch, a sibling branch is invisible",
			at:   "PLACEHOLDER_LEFT",
			want: []string{"loop", "before"},
			notWant: map[string]string{
				"fan":      "the enclosing parallel block has not joined yet",
				"right":    "branches are unordered, so a sibling may not be referenced",
				"body_one": "a loop body's outputs do not escape the loop",
				"after":    "a later step",
			},
		},
	}

	c := newClient(t)
	c.initialize()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Only the case under test gets a cursor; the others become literals.
			text := src
			for _, p := range []string{"PLACEHOLDER_BODY", "PLACEHOLDER_LEFT", "PLACEHOLDER_AFTER"} {
				if p == tt.at {
					text = strings.Replace(text, p, "${|", 1)
					continue
				}
				text = strings.Replace(text, p, "hi", 1)
			}
			clean, pos := splitCursor(t, text)

			uri := "file:///scope-" + strings.ReplaceAll(tt.name, " ", "-") + ".yaml"
			c.open(uri, clean)
			got := labels(c.complete(uri, pos.Line, pos.Character).Items)

			assert.Equal(t, tt.want, got)
			for name, why := range tt.notWant {
				assert.NotContains(t, got, name, why)
			}
		})
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
          task:
            name: echo
            inputs:
              message: hi
  - id: after
    task:
      name: echo
      inputs:
        message: ${inner.result}
`
	c := newClient(t)
	c.initialize()
	const uri = "file:///leak.yaml"
	c.open(uri, src)

	pos := positionOf(t, src, "${inner.result}", 3)

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
      iterator: target
      steps:
        - id: body
          task:
            name: echo
            inputs:
              message: ${target}
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
    task:
      name: http
      inputs:
        method: GET
        url: https://example.com/json
        outputs: "${ {'status': status_code, 'title': body} }"
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
				"    task:\n      name: echo\n      inputs:\n        message: hi\n"
			uri := "file:///duration-" + strings.ReplaceAll(tt.name, " ", "-") + ".yaml"
			params := c.open(uri, src)

			if tt.wantText == "" {
				assert.Empty(t, messages(params.Diagnostics))
				return
			}
			require.Len(t, params.Diagnostics, 1, "got %v", messages(params.Diagnostics))
			assert.Equal(t, codeFlowfile, params.Diagnostics[0].Code)
			assert.Contains(t, params.Diagnostics[0].Message, tt.wantText)
			assert.Equal(t, tt.underlines, textInRange(src, params.Diagnostics[0].Range))
		})
	}
}

// TestHoverDocumentsEveryDSLKey checks that every key the shape table declares is
// reachable through hover, so the table cannot contain an entry nothing shows.
func TestHoverDocumentsEveryDSLKey(t *testing.T) {
	t.Parallel()

	const src = `name: all-keys
description: everything
steps:
  - id: a
    if: ${true}
    timeout: 30s
    continue_on_error: true
    retry:
      attempts: 3
      interval: 1s
      backoff: 2
      max_interval: 1m
    task:
      name: echo
      description: a step
      inputs:
        message: hi
  - id: loop
    for_each:
      items: ${a.result}
      iterator: one
      max_parallel: 2
      steps:
        - id: body
          task:
            name: echo
            inputs:
              message: hi
  - id: branches
    parallel:
      - steps:
          - id: left
            task:
              name: echo
              inputs:
                message: hi
  - id: pause
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
	const uri = "file:///all-keys.yaml"
	c.open(uri, src)

	for level, keys := range dslKeys {
		for _, k := range keys {
			switch {
			case level == "":
				// Top-level keys are not inside a step, which is where hover
				// resolves document-shape keys; they are covered by completion.
				continue
			case level == "parallel":
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
					"steps": 4, "task": 6, "retry": 6, "for_each": 6, "wait_for_signal": 6,
				}[level]
				pos := positionOfKey(t, src, k.name, minIndent, level+":")
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
