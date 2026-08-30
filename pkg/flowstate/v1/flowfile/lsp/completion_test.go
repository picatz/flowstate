package lsp

import (
	"slices"
	"strings"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Completion is tested with the cursor marked in the source by "|", because that
// is how the case reads in an editor and because a document being completed is
// usually mid-edit and therefore invalid — which is exactly the state that must
// work.
func TestCompletion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// src contains a single "|" marking the cursor, which is removed before
		// the document is sent.
		src string
		// want are labels the result must contain, in this relative order.
		want []string
		// notWant are labels the result must not contain.
		notWant []string
		// exact, when set, requires the labels to be exactly this list.
		exact []string
		// first, when set, requires this label to be offered before every other.
		//
		// Separate from `want`, which asserts a relative order among the labels it
		// names and says nothing about what sits above them. Where a list is long —
		// an expression's is, now that it carries the profile's functions — what
		// matters to somebody typing is which name is at the top.
		first string
		// detailContains maps a label to a substring its detail must contain.
		detailContains map[string]string
	}{
		{
			// Every registered task is offered, carrying the registry's own summary.
			// A task name is now a step key rather than a value, so the step's own
			// keys are offered alongside — which is what makes this containment
			// rather than an exact list; the whole step-level menu is asserted by
			// "step keys" below.
			name: "task names are offered where a step's work is written",
			src: `name: c
steps:
  - |
edition: v2026.3
`,
			want: v1.TaskNames(),
			detailContains: map[string]string{
				"log":  "Emit a message for a person to read.",
				"http": "Perform an HTTP request",
			},
		},
		{
			// A prefix no step key shares narrows the step level back down to task
			// names, which is the only place the two halves can still be told apart.
			name: "task names filtered by what is typed",
			src: `name: c
steps:
  - ht|
edition: v2026.3
`,
			exact: []string{"http"},
		},
		{
			name: "input keys come from the task's schema, required first",
			src: `name: c
steps:
  - id: a
    http:
      |
edition: v2026.3
`,
			// url is the only required input, so it sorts ahead of the rest.
			want: []string{"url", "method", "headers", "body", "outputs"},
			detailContains: map[string]string{
				"url":     "string (required)",
				"headers": "map[string, string]",
				"outputs": "map[string, any]",
			},
		},
		{
			name: "input keys exclude ones already written",
			src: `name: c
steps:
  - id: a
    http:
      url: https://example.com
      method: GET
      |
edition: v2026.3
`,
			want:    []string{"headers", "body", "outputs"},
			notWant: []string{"url", "method"},
		},
		{
			name: "input keys of a different task are not offered",
			src: `name: c
steps:
  - id: a
    log:
      |
edition: v2026.3
`,
			exact: []string{"message", "level", "fields"},
		},
		{
			// An unregistered key still looks like a task from the outside, so the
			// question is what happens when the registry has no schema to answer
			// with: nothing, rather than the enclosing level's keys leaking in.
			name: "no input keys for a task that is not registered",
			src: `name: c
steps:
  - id: a
    shell:
      |
edition: v2026.3
`,
			exact: []string{},
		},
		{
			// The start of an expression is the *bare* namespace, and a step is not
			// in it: the only way to a step's outputs is through the root. Offering
			// `second` here would be offering the spelling this grammar retired.
			name: "an expression opens on the root, not on step ids",
			src: `name: c
steps:
  - id: alpha
    log:
      message: one
  - id: beta
    log:
      message: ${|}
edition: v2026.3
`,
			// The root is *first*, rather than the only thing offered. The
			// profile's functions are offered after it — an author who is stuck in
			// an expression is usually stuck on what they can write rather than on
			// what is in scope — and the ordering is what keeps the near thing near.
			first: "steps",
			want:  []string{"steps", "upperAscii", "math"},
			notWant: []string{
				"alpha", // a step, reachable only as steps.alpha
				"beta",  // and its own step besides
				"now",   // bound only inside a wait's expressions
			},
		},
		{
			// `$${` is a literal `${`, so the cursor here is in ordinary text that
			// the author means a reader to see — not in an expression. Locating the
			// open fence by searching back for the last `${` finds the escape's own
			// brace and offers the whole expression scope in the middle of prose,
			// which is the editor asserting a fence the compiler will not find.
			name: "no expression scope inside an escape",
			src: `name: c
steps:
  - id: alpha
    log:
      message: one
  - id: beta
    log:
      message: write $${steps.|
edition: v2026.3
`,
			notWant: []string{"steps", "inputs", "upperAscii", "alpha"},
		},
		{
			// The other half, so the fix cannot be "never complete after a `$`":
			// a real fence later in the same value still completes.
			name: "a real fence after an escape still completes",
			src: `name: c
steps:
  - id: alpha
    log:
      message: one
  - id: beta
    log:
      message: shows $${literal} and ${steps.|
edition: v2026.3
`,
			want: []string{"alpha"},
		},
		{
			name: "earlier step ids under the root",
			src: `name: c
steps:
  - id: first
    log:
      message: one
  - id: second
    log:
      message: two
  - id: third
    log:
      message: ${steps.|}
  - id: fourth
    log:
      message: four
edition: v2026.3
`,
			// Only steps that will have run. Offering `third` or `fourth` would
			// be offering a workflow the engine refuses.
			exact:   []string{"second", "first"},
			notWant: []string{"third", "fourth"},
		},
		{
			name: "step ids are offered nearest first",
			src: `name: c
steps:
  - id: alpha
    log:
      message: one
  - id: beta
    log:
      message: two
  - id: gamma
    log:
      message: ${steps.|}
edition: v2026.3
`,
			exact: []string{"beta", "alpha"},
		},
		{
			name: "step outputs after a dot come from the producing task",
			src: `name: c
steps:
  - id: web
    http:
      url: https://example.com
  - id: out
    log:
      message: ${steps.web.|}
edition: v2026.3
`,
			// Derived from the task's Outputs descriptor rather than listed here,
			// so an output added to the schema appears in completion without this
			// test — or the completion code — being touched. Hardcoding the list
			// meant this went red the moment the http task grew a `json` output.
			exact: taskOutputNames(t, "http"),
			detailContains: map[string]string{
				"status_code": "int",
				"body":        "string",
			},
		},
		{
			name: "step outputs filtered by what is typed",
			src: `name: c
steps:
  - id: web
    http:
      url: https://example.com
  - id: out
    log:
      message: ${steps.web.st|}
edition: v2026.3
`,
			exact: []string{"status_code"},
		},
		{
			// A step id is one segment of the reference, not the whole of it, so
			// there is nothing under a *fourth* segment: what an output holds is
			// not described by any schema this package can read.
			name: "nothing is offered past an output",
			src: `name: c
steps:
  - id: web
    http:
      url: https://example.com
  - id: out
    log:
      message: ${steps.web.body.|}
edition: v2026.3
`,
			exact: []string{},
		},
		{
			name: "no outputs offered for a later step",
			src: `name: c
steps:
  - id: out
    log:
      message: ${steps.web.|}
  - id: web
    http:
      url: https://example.com
edition: v2026.3
`,
			exact: []string{},
		},
		{
			name: "step references inside a function call",
			src: `name: c
steps:
  - id: web
    http:
      url: https://example.com
  - id: out
    log:
      message: ${string(steps.we|)}
edition: v2026.3
`,
			exact: []string{"web"},
		},
		{
			// In the order a file is written: the grammar it is written in, then
			// what the workflow is, then what it does.
			name:  "top level document keys",
			src:   `|`,
			exact: []string{"edition", "name", "description", "vars", "steps"},
		},
		{
			// Every kind of work a step can be is offered. The document-shape half is
			// written out on purpose: this is the assertion that would have caught
			// waiting being missing from completion, and deriving it from the same
			// place the code derives it from would make it agree with the code
			// rather than with the language.
			//
			// The task half is derived, for the opposite reason. Since flattening, a
			// task's name *is* a step key, and the registry is the only definition of
			// which names those are — there is no separate statement of the language
			// for a copy here to disagree with, only a list that would go stale the
			// next time a task is registered.
			name: "step keys",
			src: `name: c
steps:
  - |
edition: v2026.3
`,
			// The order is the order a step is written in, not the alphabet: the id
			// that names it, the prose saying why it is there, then the work it does,
			// then how that work runs. Tasks sit with the other kinds of work rather
			// than after `continue_on_error`, and ahead of them because running a task
			// is what most steps do.
			//
			// Asserted exactly, and with the task half derived from the registry: the
			// document-shape half written out is what caught `sleep` and `wait_until`
			// missing from the menu entirely, and a hand-copied task list would say
			// nothing the registry does not already say and would go stale at the next
			// MustRegister.
			exact: slices.Concat(
				[]string{"id", "description"},
				v1.TaskNames(),
				[]string{"for_each", "loop", "parallel", "sleep", "wait_until", "wait_for_signal", "wait_for_signals", "call", "value", "switch"},
				[]string{"if", "vars", "timeout", "total_timeout", "retry", "continue_on_error", "undo", "with"},
			),
		},
		{
			// The regression for a Codex finding on #665: once a step has committed
			// to a kind checkPolicyPlacement refuses timeout:/retry: on, completion
			// must stop recommending either — selecting one used to produce the
			// diagnostic that check writes immediately. Every other step key stays
			// offered, because the refusal is specific to those two.
			name: "timeout and retry are withheld once a step has chosen a refused kind",
			src: `name: c
steps:
  - id: fan
    for_each:
      items: ${[1, 2, 3]}
      steps: []
    |
edition: v2026.3
`,
			want:    []string{"id", "description", "if", "vars", "continue_on_error"},
			notWant: []string{"timeout", "retry"},
		},
		{
			// Fresh evidence after the fixture above: scanOutline ends a step's
			// line range where the next step begins at *any* depth, so with a
			// non-empty body the nested task's range runs all the way to this
			// sibling line — stepScope's `current` used to resolve to that nested
			// task rather than to `fan`, and completion offered timeout/retry
			// again exactly where checkPolicyPlacement refuses them on `fan`
			// itself. stepOwningKeyAt is what fixes it: it matches this line's
			// column against the step that actually opens at it.
			name: "timeout and retry stay withheld with a non-empty body",
			src: `name: c
steps:
  - id: fan
    for_each:
      items: ${[1, 2, 3]}
      steps:
        - id: inner
          log:
            message: hi
    |
edition: v2026.3
`,
			want:    []string{"id", "description", "if", "vars", "continue_on_error"},
			notWant: []string{"timeout", "retry"},
		},
		{
			// A task step's own kind key is the task name, which is not in
			// nonTaskKindKeys, so timeout:/retry: stay on the menu — the
			// unconditional case the withholding above must not over-reach into.
			name: "timeout and retry stay offered on an ordinary task step",
			src: `name: c
steps:
  - id: a
    log:
      message: hi
    |
edition: v2026.3
`,
			want: []string{"timeout", "retry"},
		},
		{
			// The mapping form of a gate. The scalar form takes a name directly, so
			// these two keys only exist for an author who needs a timeout — which is
			// the form worth offering, since the scalar one needs no help.
			name: "wait_for_signal keys",
			src: `name: c
steps:
  - id: approval
    wait_for_signal:
      |
edition: v2026.3
`,
			exact: []string{"name", "timeout", "prompt", "outputs"},
		},
		{
			name: "input keys inside a for_each body",
			// A nested step is where an author is just as likely to want a
			// suggestion, so the line scan descends into nested steps: blocks.
			src: `name: c
steps:
  - id: loop
    for_each:
      items: ${x}
      steps:
        - id: body
          http:
            |
edition: v2026.3
`,
			want: []string{"url", "method", "headers"},
		},
		{
			name: "input keys inside a parallel branch",
			src: `name: c
steps:
  - id: branches
    parallel:
      - steps:
          - id: left
            log:
              |
edition: v2026.3
`,
			exact: []string{"message", "level", "fields"},
		},
		{
			// The two namespaces, in the one place an author sees both. Bare, a
			// loop body offers the item it binds and the root — and nothing else,
			// because a step is not a bare name here any more than anywhere else.
			name: "a loop body offers its item and the root, bare",
			src: `name: c
steps:
  - id: outer
    log:
      message: hi
  - id: loop
    for_each:
      items: ${['a', 'b']}
      steps:
        - id: body
          log:
            message: ${|
edition: v2026.3
`,
			// Nearest first: the binding of the block the cursor stands in, then
			// the root spanning the whole document.
			first:   "item",
			want:    []string{"item", "steps"},
			notWant: []string{"outer", "body", "loop"},
			detailContains: map[string]string{
				"item": "loop item",
			},
		},
		{
			// And the other direction: under the root there are steps and only
			// steps. `item` is a binding, so offering it here would produce
			// `steps.item`, which resolves to nothing.
			name: "a loop body's root offers steps, not the iterator",
			src: `name: c
steps:
  - id: outer
    log:
      message: hi
  - id: loop
    for_each:
      items: ${['a', 'b']}
      steps:
        - id: body
          log:
            message: ${steps.|
edition: v2026.3
`,
			// The enclosing loop is excluded — it has not finished, so it has no
			// results yet — and so is the body step itself.
			exact:   []string{"outer"},
			notWant: []string{"item", "body", "loop"},
		},
		{
			// `now` is bound by the engine for this one key, so this is the one
			// place it is offered. Separating the namespaces is what made a clean
			// place for it: it is a bare name that is not a step, which the single
			// ordered list had no way to say.
			name: "wait_until offers the clock alongside the root",
			src: `name: c
steps:
  - id: before
    log:
      message: hi
  - id: window
    wait_until: ${|
edition: v2026.3
`,
			first: "now",
			want:  []string{"now", "steps"},
			// Not the step ids: they are still reached through the root here like
			// anywhere else.
			notWant: []string{"before", "window"},
			detailContains: map[string]string{
				"now": "timestamp",
			},
		},
		{
			// The negative direction, and the reason `now` is not simply in every
			// scope: a task input is resolved inside an activity, which has no
			// clock that survives a retry. The validator refuses it there, so
			// offering it would walk an author into a diagnostic.
			name: "a task input does not bind the clock",
			src: `name: c
steps:
  - id: before
    log:
      message: hi
  - id: after
    log:
      message: ${|
edition: v2026.3
`,
			first:   "steps",
			want:    []string{"steps"},
			notWant: []string{"now"},
		},
		{
			// And it is a bare name, so it is not under the root either.
			name: "the clock is not a step",
			src: `name: c
steps:
  - id: before
    log:
      message: hi
  - id: window
    wait_until: ${steps.|
edition: v2026.3
`,
			exact:   []string{"before"},
			notWant: []string{"now"},
		},
		{
			// A wait inside a loop body binds both: the loop's item because the
			// block binds it, and the clock because the key does.
			name: "a wait inside a loop body binds the item and the clock",
			src: `name: c
steps:
  - id: loop
    for_each:
      items: "${['a']}"
      as: each
      steps:
        - id: window
          wait_until: ${|
edition: v2026.3
`,
			first: "each",
			want:  []string{"each", "now", "steps"},
		},
		{
			// The word is not enough on its own: an input spelled the same way is
			// that task's input, resolved where there is no clock. The task here is
			// unregistered on purpose, since no registered one may take a name the
			// step grammar uses — which is exactly why the level, and not the
			// spelling, is what decides.
			name: "an input spelled wait_until does not bind the clock",
			src: `name: c
steps:
  - id: before
    log:
      message: hi
  - id: a
    shell:
      wait_until: ${|
edition: v2026.3
`,
			first:   "steps",
			want:    []string{"steps"},
			notWant: []string{"now"},
		},
		{
			name: "for_each keys",
			src: `name: c
steps:
  - id: a
    for_each:
      |
edition: v2026.3
`,
			exact: []string{"items", "as", "max_parallel", "steps"},
		},
		{
			name: "retry keys",
			src: `name: c
steps:
  - id: a
    retry:
      |
edition: v2026.3
`,
			exact: []string{"attempts", "interval", "backoff", "max_interval"},
		},
		{
			// This was "task keys", and asserted the `name`, `description`, and
			// `inputs` keys of the `task:` mapping. Flattening removed that mapping,
			// so the level it described no longer exists; what survives is the
			// question it was really asking — what sits directly under a task — and
			// the answer is now the task's inputs and nothing else. The three retired
			// keys are named negatively because reintroducing any of them, in a
			// document where they would parse as inputs, is the regression to catch.
			name: "a task's own key holds inputs, not the retired task block keys",
			src: `name: c
steps:
  - id: a
    log:
      |
edition: v2026.3
`,
			exact:   []string{"message", "level", "fields"},
			notWant: []string{"name", "description", "inputs"},
		},
		{
			name: "nothing to complete in a literal value",
			src: `name: c
steps:
  - id: a
    log:
      message: hello |
edition: v2026.3
`,
			exact: []string{},
		},
		{
			name: "nothing to complete for a step id",
			src: `name: c
steps:
  - id: |
edition: v2026.3
`,
			exact: []string{},
		},
	}

	c := newClient(t)
	c.initialize()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src, pos := splitCursor(t, tt.src)
			uri := "file:///completion-" + strings.ReplaceAll(tt.name, " ", "-") + ".yaml"
			c.open(uri, src)

			got := c.complete(uri, pos.Line, pos.Character)
			require.NotNil(t, got.Items, "items must be an array, never null")
			gotLabels := labels(got.Items)

			if tt.exact != nil {
				assert.Equal(t, tt.exact, gotLabels)
			}
			if tt.first != "" {
				require.NotEmpty(t, gotLabels)
				assert.Equal(t, tt.first, gotLabels[0],
					"the nearest name is not offered first, so an author scrolls past the rest to reach it")
			}
			for _, want := range tt.want {
				assert.Contains(t, gotLabels, want)
			}
			for _, notWant := range tt.notWant {
				assert.NotContains(t, gotLabels, notWant)
			}
			// Relative order of the wanted labels.
			if len(tt.want) > 1 {
				assert.Equal(t, tt.want, filterLabels(gotLabels, tt.want))
			}
			for label, substr := range tt.detailContains {
				item := findItem(got.Items, label)
				require.NotNil(t, item, "no candidate labeled %q", label)
				assert.Contains(t, item.Detail, substr)
			}
		})
	}
}

// TestGrammarKeysAreOfferedOnlyWhereTheyMeanSomething writes the direction the
// table above does not.
//
// Every case up there asks whether a level offers its own keys, and a key that
// leaked into every other level would satisfy all of them — the exact shape
// CLAUDE.md calls a functionality test wearing a security test's clothes. The two
// keys the grammar most recently gained are where that matters:
//
//   - `description` is a property of the *step*. The keys under a task's name are
//     that task's inputs, so offering it there offers an input called
//     `description` — a different key, with a different meaning, that reads
//     identically once written.
//   - `edition` names the grammar the *file* is written in. It means nothing
//     anywhere but the document's first level, so a menu offering it inside a step
//     is a menu whose suggestion `flow validate` refuses.
//
// A wrong candidate is worse than a missing one, because an author accepts it.
func TestGrammarKeysAreOfferedOnlyWhereTheyMeanSomething(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		// want are labels that must be offered here, notWant ones that must not.
		want    []string
		notWant []string
	}{
		{
			name:    "the document level is where both belong",
			src:     `|`,
			want:    []string{"edition", "description"},
			notWant: []string{"id", "if"},
		},
		{
			name: "a step carries prose but not an edition",
			src: `name: c
steps:
  - |
edition: v2026.3
`,
			want:    []string{"description"},
			notWant: []string{"edition", "name"},
		},
		{
			// Each nested block names a key it does offer, so that a case cannot
			// pass by reaching no menu at all — "absent from nothing" is the way a
			// negative assertion goes quietly vacuous.
			name: "a for_each block",
			src: `name: c
steps:
  - id: a
    for_each:
      |
edition: v2026.3
`,
			want:    []string{"items"},
			notWant: []string{"description", "edition"},
		},
		{
			name: "a retry block",
			src: `name: c
steps:
  - id: a
    retry:
      |
edition: v2026.3
`,
			want:    []string{"attempts"},
			notWant: []string{"description", "edition"},
		},
		{
			name: "a gate's own keys",
			src: `name: c
steps:
  - id: a
    wait_for_signal:
      |
edition: v2026.3
`,
			want:    []string{"name", "timeout"},
			notWant: []string{"description", "edition"},
		},
		{
			name: "a parallel block's branches",
			src: `name: c
steps:
  - id: a
    parallel:
      - |
edition: v2026.3
`,
			want:    []string{"steps"},
			notWant: []string{"description", "edition"},
		},
		{
			// A step nested in a loop body is still a step, and the level it sits
			// at says nothing about which keys mean something there.
			name: "a step inside a loop body",
			src: `name: c
steps:
  - id: loop
    for_each:
      items: ${x}
      steps:
        - |
edition: v2026.3
`,
			want:    []string{"description"},
			notWant: []string{"edition"},
		},
		{
			// Inside `${...}` the candidates are names in scope, which are steps
			// and bindings; a grammar key is not a value anything can reference.
			// Asked under the root, where the step ids are, so that the case
			// cannot pass by reaching an empty menu.
			name: "inside an expression",
			src: `name: c
steps:
  - id: first
    log:
      message: hi
  - id: second
    log:
      message: ${steps.|}
edition: v2026.3
`,
			want:    []string{"first"},
			notWant: []string{"description", "edition"},
		},
	}

	c := newClient(t)
	c.initialize()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src, pos := splitCursor(t, tt.src)
			uri := "file:///only-where-" + strings.ReplaceAll(tt.name, " ", "-") + ".yaml"
			c.open(uri, src)

			got := labels(c.complete(uri, pos.Line, pos.Character).Items)
			for _, want := range tt.want {
				assert.Contains(t, got, want)
			}
			for _, notWant := range tt.notWant {
				assert.NotContains(t, got, notWant,
					"%q is offered where it means nothing, and an author who accepts it gets a file `flow validate` rejects", notWant)
			}
		})
	}

	// The same question over every task rather than the two written above, because
	// the rule is that the keys under a task's name come from that task's schema
	// and from nowhere else. A table key leaking in would surface under whichever
	// task nobody happened to write a case for.
	for _, def := range v1.DefaultRegistry().All() {
		t.Run("inputs of "+def.Name, func(t *testing.T) {
			src, pos := splitCursor(t, "name: c\nsteps:\n  - id: a\n    "+def.Name+":\n      |\n")
			uri := "file:///only-where-inputs-" + def.Name + ".yaml"
			c.open(uri, src)

			got := labels(c.complete(uri, pos.Line, pos.Character).Items)
			if len(fieldNames(def.Inputs)) > 0 {
				require.NotEmpty(t, got,
					"premise: a task with inputs must offer them, or nothing below is being asserted about")
			}
			for _, key := range []string{"description", "edition"} {
				if slices.Contains(fieldNames(def.Inputs), key) {
					// The task declares an input spelled that way. It is the
					// schema's key then, not the shape table's, and offering it is
					// the whole job.
					continue
				}
				assert.NotContains(t, got, key,
					"the %s task's inputs are its schema's, but completion offers %q there", def.Name, key)
			}
		})
	}

	// And the table from the other side. `edition` at a second level would be a key
	// meaning two things, which is the ambiguity the step grammar spends
	// v1.ReservedStepKeys avoiding elsewhere; here nothing would catch it, because
	// a level's own cases only ever ask what that level offers.
	for level, keys := range dslKeys {
		if level == "" {
			continue
		}
		assert.False(t,
			slices.ContainsFunc(keys, func(k dslKey) bool { return k.name == "edition" }),
			"dslKeys offers `edition` at the %q level, but an edition is a property of a file "+
				"rather than of anything inside one", level)
	}
}

// TestCompletionReplacesThePartialWord checks the text edit each candidate carries.
// Without an explicit range an editor guesses at the word boundary, and inserting
// alongside a partial word instead of replacing it produces "htthttp".
func TestCompletionReplacesThePartialWord(t *testing.T) {
	t.Parallel()

	src, pos := splitCursor(t, `name: c
steps:
  - ht|
edition: v2026.3
`)
	c := newClient(t)
	c.initialize()
	c.open("file:///replace.yaml", src)

	got := c.complete("file:///replace.yaml", pos.Line, pos.Character)
	require.Len(t, got.Items, 1)
	edit := got.Items[0].TextEdit
	require.NotNil(t, edit, "a candidate must say what it replaces")
	assert.Equal(t, "ht", textInRange(src, edit.Range))
	// A task's name is a key of the step, so it is inserted the way every other
	// key is: with its colon and a space. Accepting `http` and then typing the
	// colon by hand is friction an editor exists to remove.
	assert.Equal(t, "http: ", edit.NewText)
}

// TestAcceptingTheRootLeavesTheCursorWhereTheIdGoes pins what accepting the root
// actually writes.
//
// The root is never the whole of a reference — there is nothing an author can do
// with `${steps}` alone except count what has run — so accepting it and then
// typing the dot by hand is friction, and the dot is what opens the next menu. The
// label and the inserted text therefore differ, which is invisible in a list of
// labels and is the reason this is asserted rather than left to the table above.
func TestAcceptingTheRootLeavesTheCursorWhereTheIdGoes(t *testing.T) {
	t.Parallel()

	src, pos := splitCursor(t, `name: c
steps:
  - id: web
    http:
      url: https://example.com
  - id: out
    log:
      message: ${|}
edition: v2026.3
`)
	c := newClient(t)
	c.initialize()
	const uri = "file:///accept-root.yaml"
	c.open(uri, src)

	// The root is offered first — the profile's functions follow it, and this test
	// is about what accepting the root does rather than about what else is on the
	// menu.
	got := c.complete(uri, pos.Line, pos.Character)
	require.NotEmpty(t, got.Items, "the start of an expression offers nothing at all")
	root := got.Items[0]
	require.Equal(t, "steps", root.Label, "the root is not the first thing offered")
	require.NotNil(t, root.TextEdit, "a candidate must say what it replaces")
	assert.Equal(t, "steps.", root.TextEdit.NewText)

	// And what it replaces is nothing, since nothing was typed: the edit is an
	// insertion at the cursor. An edit that swallowed the `{` before it would
	// break the fence.
	assert.Equal(t, "", textInRange(src, root.TextEdit.Range))

	// Applying it produces the document whose next completion is the step ids,
	// which is the whole point of inserting the dot.
	applied := src[:offsetOf(src, root.TextEdit.Range.Start)] + root.TextEdit.NewText +
		src[offsetOf(src, root.TextEdit.Range.End):]
	c.open("file:///accept-root-applied.yaml", applied)
	after := c.complete("file:///accept-root-applied.yaml",
		pos.Line, pos.Character+utf16Len(root.TextEdit.NewText))
	assert.Equal(t, []string{"web"}, labels(after.Items))
}

// offsetOf returns the byte offset of a position in a source.
func offsetOf(src string, pos lsp.Position) int {
	return newLineIndex(src).offsetOfPosition(pos)
}

// TestCompletionWorksOnUnparseableDocument is the case that matters most in
// practice: a half-typed key is not valid YAML, and that is when completion is
// asked for.
func TestCompletionWorksOnUnparseableDocument(t *testing.T) {
	t.Parallel()

	src, pos := splitCursor(t, `name: c
steps:
  - id: a
    log:
      mes|
edition: v2026.3
`)
	// Confirm the premise: this document does not compile.
	require.NotEmpty(t, diagnose(newDocument("file:///x", 1, src, nil)),
		"premise: the half-typed document should have problems")

	c := newClient(t)
	c.initialize()
	c.open("file:///partial.yaml", src)

	got := c.complete("file:///partial.yaml", pos.Line, pos.Character)
	assert.Equal(t, []string{"message"}, labels(got.Items))
}

func TestCompletionWalksQuotedWorkflowKeysSemantically(t *testing.T) {
	t.Parallel()

	const src = "\"edition\": v2026.3\n" +
		"\"name\": quoted\n" +
		"\"st\\u0065ps\":\n" +
		"  - \"i\\u0064\": first\n" +
		"    \"l\\u006fg\":\n" +
		"      \n"
	c := newClient(t)
	c.initialize()
	c.open("file:///quoted-keys.yaml", src)

	got := labels(c.complete("file:///quoted-keys.yaml", 5, 6).Items)
	assert.Contains(t, got, "message",
		"the escaped task key was not decoded to the registry's log task")
}

// TestCompletionUsesUTF16Columns checks that a partial word after non-ASCII text is
// located and replaced correctly.
func TestCompletionUsesUTF16Columns(t *testing.T) {
	t.Parallel()

	// The partial word sits after an emoji on the same line, so a server counting
	// bytes or code points would replace the wrong span. The partial word is now
	// a *segment* of a rooted reference rather than the whole of it, which is the
	// harder case for the same reason: the replaced span has to start after the
	// last dot, not at the start of the word.
	const cursorLine = "        X-🙂: ${steps.fi"
	src := "name: ünïcödé wörkflöw\n" +
		"steps:\n" +
		"  - id: first\n" +
		"    log:\n" +
		"      message: hi\n" +
		"  - id: second\n" +
		"    http:\n" +
		"      url: https://example.com\n" +
		"      headers:\n" +
		cursorLine + "\n"

	require.NotEqual(t, len(cursorLine), utf16Len(cursorLine),
		"premise: the cursor's line must contain non-ASCII")

	c := newClient(t)
	c.initialize()
	c.open("file:///unicode-completion.yaml", src)

	got := c.complete("file:///unicode-completion.yaml", 9, utf16Len(cursorLine))
	require.Len(t, got.Items, 1)
	assert.Equal(t, "first", got.Items[0].Label)
	require.NotNil(t, got.Items[0].TextEdit)
	assert.Equal(t, "fi", textInRange(src, got.Items[0].TextEdit.Range))
}

// splitCursor removes the "|" cursor marker from a source and returns the position
// it marked.
func splitCursor(t *testing.T, src string) (string, lsp.Position) {
	t.Helper()
	at := strings.Index(src, "|")
	require.GreaterOrEqual(t, at, 0, "test source has no | cursor marker")
	require.Equal(t, at, strings.LastIndex(src, "|"), "test source has more than one | marker")

	clean := src[:at] + src[at+1:]
	return clean, newLineIndex(clean).positionOfOffset(at)
}

// findItem returns the candidate with the given label.
func findItem(items []lsp.CompletionItem, label string) *lsp.CompletionItem {
	for i := range items {
		if items[i].Label == label {
			return &items[i]
		}
	}
	return nil
}

// filterLabels returns the elements of got that appear in keep, in got's order.
func filterLabels(got, keep []string) []string {
	wanted := make(map[string]bool, len(keep))
	for _, k := range keep {
		wanted[k] = true
	}
	out := make([]string, 0, len(keep))
	for _, g := range got {
		if wanted[g] {
			out = append(out, g)
		}
	}
	return out
}

// taskOutputNames returns the output names a registered task declares, in schema
// order, which is the order completion offers them in.
func taskOutputNames(t *testing.T, task string) []string {
	t.Helper()
	def, ok := v1.LookupTask(task)
	require.True(t, ok, "task %q is not registered", task)
	names := fieldNames(def.Outputs)
	require.NotEmpty(t, names, "task %q declares no outputs", task)
	return names
}

// TestAnExpressionCompletesTheProfilesFunctions covers what the editor gained when
// it stopped offering only what an expression can reference.
//
// Written as its own test rather than as rows in the table above because the table
// asserts whole menus and this is about two branches: what is offered bare, and what
// a namespace offers after its dot. The second did not exist — an unknown qualifier
// was treated as a binding and offered nothing, which was right when the only
// qualifiers were bindings and `steps`.
func TestAnExpressionCompletesTheProfilesFunctions(t *testing.T) {
	t.Parallel()

	const src = `name: c
steps:
  - id: web
    http:
      url: https://example.com
  - id: out
    log:
      message: ${PLACEHOLDER
edition: v2026.3
`

	c := newClient(t)
	c.initialize()

	for _, test := range []struct {
		name    string
		typed   string
		want    []string
		notWant []string
	}{
		{
			// Bare, with nothing typed: the names in scope come first and the
			// functions follow. Both halves asserted, because offering functions
			// *instead* of the scope would be the same mistake pointed the other way.
			name:  "bare offers the scope and then the functions",
			typed: "",
			want:  []string{"steps", "upperAscii", "json_parse", "math", "regex"},
		},
		{
			// A prefix an author is part-way through. This is the case the whole
			// feature is for: `up` means nothing without knowing `upperAscii` exists.
			name:  "a prefix narrows to the function",
			typed: "up",
			want:  []string{"upperAscii"},
			// A namespace whose *members* start with the prefix must not surface
			// here, because `up` is not the front of `math.abs`.
			notWant: []string{"math", "steps"},
		},
		{
			name:  "a namespace offers its members after the dot",
			typed: "math.",
			want:  []string{"abs", "ceil", "floor", "round", "sqrt"},
			// The qualifier itself is not repeated inside its own list, and step
			// ids are a different namespace entirely.
			notWant: []string{"math", "web"},
		},
		{
			name:    "another namespace offers only its own",
			typed:   "regex.",
			want:    []string{"extract", "extractAll", "replace"},
			notWant: []string{"abs", "upperAscii"},
		},
		{
			// The root still wins its own name. A namespace check that ran first
			// would have to be wrong about `steps` for this to fail, but the two
			// branches sit next to each other and the order between them is a
			// decision rather than an accident.
			name:    "the steps root still offers step ids",
			typed:   "steps.",
			want:    []string{"web"},
			notWant: []string{"abs", "upperAscii"},
		},
		{
			// A bare binding is not a namespace, and offering a function list for
			// one would be inventing members for a value whose type is unknown.
			name:    "a name that is not a namespace offers nothing",
			typed:   "web.",
			notWant: []string{"abs", "upperAscii", "extract"},
		},
	} {
		// Not parallel, and the rest of this file is not either. The harness wraps
		// the server in a jsonrpc2.AsyncHandler, so a request and the notification
		// it depends on are handled concurrently: a completion can reach the server
		// before the didOpen that put the document there, and the answer is an empty
		// list. Which is how this was found — one subtest failed once, on a document
		// it had opened a line earlier, and passed on the next run.
		t.Run(test.name, func(t *testing.T) {
			text, pos := splitCursor(t, strings.Replace(src, "PLACEHOLDER", test.typed+"|", 1))
			uri := "file:///fn-complete-" + strings.ReplaceAll(test.name, " ", "-") + ".yaml"
			c.open(uri, text)

			got := labels(c.complete(uri, pos.Line, pos.Character).Items)
			for _, want := range test.want {
				assert.Contains(t, got, want)
			}
			for _, notWant := range test.notWant {
				assert.NotContains(t, got, notWant)
			}
		})
	}
}
