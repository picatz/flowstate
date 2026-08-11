package flowfile_test

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/go-cmp/cmp"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

// TestParseReportsPositions covers the reason the compiler walks the document tree
// instead of decoding into structs: a diagnostic that cannot say where the problem
// is leaves the author to find it.
//
// Each case marks the offending token in the source with a caret comment so the
// expectation is readable next to the thing it is about.
func TestParseReportsPositions(t *testing.T) {
	tests := []struct {
		name string
		src  string
		line int
		col  int
		want string
	}{
		{
			name: "malformed duration",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    timeout: 30 seconds
    log:
`,
			line: 5, col: 14,
			want: `timeout "30 seconds" is not a duration; write it as 30s, 5m, 1h, or 7d`,
		},
		{
			name: "duration written as a number",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    timeout: 30
    log:
`,
			line: 5, col: 14,
			want: "must be a duration written as a string",
		},
		{
			name: "misspelled step key",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    timout: 30s
    log:
`,
			line: 5, col: 5,
			want: `unknown key "timout"; did you mean "timeout"?`,
		},
		{
			name: "unknown key with no near match",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    nonsense: 1
    log:
`,
			line: 5, col: 5,
			want: `unknown key "nonsense"; the keys here are id, description, if, vars, timeout, retry, continue_on_error, undo, with, digest, for_each, loop, parallel, sleep, wait_until, wait_for_signal, call, value, http, and log`,
		},
		{
			// The shape every Flowfile written before the flattening has, and the
			// shape a model trained on those will keep producing. Reported as what
			// it is rather than as `unknown task "task"` — which is true, and tells
			// an author only what their file is not.
			//
			// Said *once*, which this runner's exactly-one-diagnostic rule is what
			// actually checks: a step written the old way also has no kind, so the
			// obvious implementation reports the mistake a second time in words that
			// do not name the fix.
			name: "the retired task block says what to write instead",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    task:
      name: log
      inputs:
        message: hi
`,
			line: 5, col: 5,
			want: "`task:` is no longer a step key; a step names its task directly now",
		},
		{
			name: "unknown workflow key",
			src: `edition: v2026.2
name: t
labels:
  env: dev
steps:
  - id: a
    log:
`,
			line: 3, col: 1,
			want: `unknown key "labels"`,
		},
		{
			name: "two kinds of work in one step",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    log:
    for_each:
      items: ${[1]}
      steps:
        - id: b
          log:
`,
			line: 6, col: 5,
			want: "has both log and for_each; a step does exactly one kind of work",
		},
		{
			name: "no kind of work at all",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    timeout: 5s
`,
			line: 4, col: 5,
			want: "must have one of for_each, loop, parallel, sleep, wait_until, wait_for_signal, call, value, http, or log",
		},
		{
			name: "a step key that is not a string",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    42:
`,
			line: 5, col: 5,
			want: "keys must be strings, but a number was written here",
		},
		{
			name: "an expression cannot choose which task runs",
			// The task is the key, and a key is read literally. Choosing a task at
			// run time is not something the grammar can express — which is the
			// point, since a workload whose *shape* depends on its data cannot be
			// checked before it runs. It is reported as the unknown key it is.
			src: `edition: v2026.2
name: t
steps:
  - id: a
    ${chosen.task}:
`,
			line: 5, col: 5,
			want: `unknown key "${chosen.task}"`,
		},
		{
			name: "interpolated input",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    log:
      message: hello ${who.result}
`,
			line: 6, col: 16,
			want: "must be the whole value",
		},
		{
			name: "expression syntax error points inside the expression",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    log:
      message: ${a +}
`,
			// CEL reports the fault at the end of `a +`, which is the `}` in
			// column 23 — not the start of the step, and not the start of the
			// fence.
			line: 6, col: 21,
			want: "not a valid expression",
		},
		{
			name: "input with no value",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    log:
      message:
`,
			// The span is empty and sits where the value would have gone.
			line: 6, col: 16,
			want: "has no value; give it one or remove the key",
		},
		{
			name: "retry field out of place",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    retry:
      attemps: 3
    log:
`,
			line: 6, col: 7,
			want: `unknown key "attemps"; did you mean "attempts"?`,
		},
		{
			name: "for_each without items",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    for_each:
      as: x
      steps:
        - id: b
          log:
`,
			line: 6, col: 7,
			want: "for_each requires items",
		},
		{
			name: "parallel that is not a list",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    parallel:
      steps:
        - id: b
          log:
`,
			line: 6, col: 7,
			want: "parallel must be a list of branches",
		},
		{
			name: "problem inside a loop body names the body step",
			src: `edition: v2026.2
name: t
steps:
  - id: outer
    for_each:
      items: ${[1]}
      steps:
        - id: inner
          timeout: soon
          log:
`,
			line: 9, col: 20,
			want: `step "inner": timeout "soon" is not a duration`,
		},
		{
			name: "problem inside a parallel branch names the branch step",
			src: `edition: v2026.2
name: t
steps:
  - id: outer
    parallel:
      - steps:
          - id: inner
            log:
              message: hi ${there}
`,
			line: 9, col: 24,
			want: `step "inner" input "message"`,
		},
		{
			name: "nested input value inside a map",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    http:
      url: https://example.com
      headers:
        X-A: fine
        X-B: broken ${x}
`,
			line: 9, col: 14,
			want: `step "a" input "headers"`,
		},
		{
			name: "empty file",
			src:  "",
			line: 1, col: 1,
			want: "the file is empty",
		},
		{
			name: "more than one document",
			src: `edition: v2026.2
name: a
steps:
  - id: a
    log:
---
name: b
steps:
  - id: b
    log:
`,
			line: 7, col: 1,
			want: "a Flowfile holds one workflow",
		},
		{
			name: "steps is not a list",
			src: `edition: v2026.2
name: t
steps:
  a: 1
`,
			line: 4, col: 3,
			want: "must be a list of steps",
		},
		{
			name: "unknown alias",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    log: *base
`,
			line: 5, col: 10,
			want: "unknown alias *base",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := flowfile.Parse([]byte(tt.src))
			if err == nil {
				t.Fatal("Parse() succeeded, want a diagnostic")
			}

			var ds flowfile.Diagnostics
			if !asDiagnostics(err, &ds) {
				t.Fatalf("Parse() error is %T, want Diagnostics: %v", err, err)
			}
			if len(ds) != 1 {
				t.Fatalf("expected exactly one diagnostic, got %d:\n%s", len(ds), ds.Error())
			}

			got := ds[0]
			if got.Line != tt.line || got.Column != tt.col {
				t.Errorf("position = %d:%d, want %d:%d\nreported: %s",
					got.Line, got.Column, tt.line, tt.col, got.Error())
			}
			if !strings.Contains(got.Error(), tt.want) {
				t.Errorf("diagnostic does not mention %q; got:\n%s", tt.want, got.Error())
			}
			t.Logf("reported: %s", got.Error())
		})
	}
}

// TestParsePositionPaths covers the positional model directly: a caller with a
// workflow in hand has to be able to ask where any part of it was written.
func TestParsePositionPaths(t *testing.T) {
	src := `edition: v2026.2
name: positions
steps:
  - id: first
    timeout: 30s
    log:
      message: ${greeting.result}
  - id: second
    for_each:
      items: ${[1, 2]}
      as: n
      steps:
        - id: inner
          log:
            message: hello
`
	_, positions, err := flowfile.Parse([]byte(src))
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}

	tests := []struct {
		path  string
		start flowfile.Position
		end   flowfile.Position
	}{
		{path: "name", start: flowfile.Position{Line: 2, Column: 7}, end: flowfile.Position{Line: 2, Column: 16}},
		{path: "steps[0].id", start: flowfile.Position{Line: 4, Column: 9}, end: flowfile.Position{Line: 4, Column: 14}},
		{path: "steps[0].timeout", start: flowfile.Position{Line: 5, Column: 14}, end: flowfile.Position{Line: 5, Column: 17}},
		{path: "steps[0].log.message", start: flowfile.Position{Line: 7, Column: 16}, end: flowfile.Position{Line: 7, Column: 34}},
		{path: "steps[1].for_each.items", start: flowfile.Position{Line: 10, Column: 14}, end: flowfile.Position{Line: 10, Column: 23}},
		{path: "steps[1].for_each.as", start: flowfile.Position{Line: 11, Column: 11}, end: flowfile.Position{Line: 11, Column: 12}},
		{path: "steps[1].for_each.steps[0].log.message", start: flowfile.Position{Line: 15, Column: 22}, end: flowfile.Position{Line: 15, Column: 27}},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			span, ok := positions.At(tt.path)
			if !ok {
				t.Fatalf("At(%q) not recorded", tt.path)
			}
			if span.Start != tt.start || span.End != tt.end {
				t.Errorf("At(%q) = %s, want %s-%s", tt.path, span, tt.start, tt.end)
			}
		})
	}

	// An expression's span excludes the fence, which is what an editor underlines
	// for a problem with the expression itself.
	span, ok := positions.ExprAt("steps[0].log.message")
	if !ok {
		t.Fatal("ExprAt() not recorded for a fenced expression")
	}
	want := flowfile.Span{
		Start: flowfile.Position{Line: 7, Column: 18},
		End:   flowfile.Position{Line: 7, Column: 33},
	}
	if span != want {
		t.Errorf("ExprAt() = %s, want %s", span, want)
	}

	// Steps are addressable by id at any depth, which is what lets a diagnostic
	// naming only a step be placed.
	for id, wantPath := range map[string]string{
		"first":  "steps[0]",
		"second": "steps[1]",
		"inner":  "steps[1].for_each.steps[0]",
	} {
		got, ok := positions.StepPath(id)
		if !ok || got != wantPath {
			t.Errorf("StepPath(%q) = %q, %v; want %q", id, got, ok, wantPath)
		}
	}

	// Locate is what ValidateSource uses: narrow to the input when it names one,
	// and fall back to the step when it does not.
	if span, ok := positions.Locate("first", "message"); !ok || span.Start.Line != 7 {
		t.Errorf("Locate(first, message) = %s, %v; want line 7", span, ok)
	}
	if span, ok := positions.Locate("second", ""); !ok || span.Start.Line != 8 {
		t.Errorf("Locate(second, \"\") = %s, %v; want line 8", span, ok)
	}
	if span, ok := positions.Locate("second", "as"); !ok || span.Start.Line != 11 {
		t.Errorf("Locate(second, iterator) = %s, %v; want line 11", span, ok)
	}

	var none *flowfile.Positions
	if _, ok := none.At("name"); ok {
		t.Error("a nil *Positions should answer false")
	}
}

// TestParseExpressionContexts pins the rule that decides whether a value is an
// expression, which is the DSL's most confusing corner and so the one most worth
// pinning.
func TestParseExpressionContexts(t *testing.T) {
	tests := []struct {
		name string
		src  string
		// check is given the compiled workflow's first step.
		check func(t *testing.T, step *v1.Node)
	}{
		{
			name: "a bare condition is an expression",
			src:  stepWith("if: check.result == 'ready'"),
			check: func(t *testing.T, step *v1.Node) {
				requireExpr(t, step.GetCondition(), `check.result == "ready"`)
			},
		},
		{
			name: "a fenced condition is the same expression",
			src:  stepWith("if: ${check.result == 'ready'}"),
			check: func(t *testing.T, step *v1.Node) {
				requireExpr(t, step.GetCondition(), `check.result == "ready"`)
			},
		},
		{
			name: "a condition written as a bool is a literal",
			src:  stepWith("if: false"),
			check: func(t *testing.T, step *v1.Node) {
				if step.GetCondition().GetLiteral().GetBoolValue() {
					t.Error("condition should be a literal false")
				}
				if step.GetCondition().GetExpr() != nil {
					t.Error("a bool condition should not be an expression")
				}
			},
		},
		{
			name: "a bare input is a literal string",
			src:  taskInput("message: a.result"),
			check: func(t *testing.T, step *v1.Node) {
				got := step.GetTask().GetInputs()["message"]
				if got.GetLiteral().GetStringValue() != "a.result" {
					t.Errorf("input = %v, want the literal string a.result", got)
				}
			},
		},
		{
			name: "a fenced input is an expression",
			src:  taskInput("message: ${a.result}"),
			check: func(t *testing.T, step *v1.Node) {
				requireExpr(t, step.GetTask().GetInputs()["message"], "a.result")
			},
		},
		{
			name: "a bare items is an expression",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    for_each:
      items: targets.result
      steps:
        - id: b
          log:
            message: hi
`,
			check: func(t *testing.T, step *v1.Node) {
				requireExpr(t, step.GetForEach().GetItems(), "targets.result")
			},
		},
		{
			name: "a list items is a literal list",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    for_each:
      items: [1, 2]
      steps:
        - id: b
          log:
            message: hi
`,
			check: func(t *testing.T, step *v1.Node) {
				items := step.GetForEach().GetItems().GetLiteral().GetListValue().GetValues()
				if len(items) != 2 || items[0].GetInt64Value() != 1 {
					t.Errorf("items = %v, want a literal list of 1 and 2", items)
				}
			},
		},
		{
			name: "a structure with a nested expression becomes one expression",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    http:
      url: https://example.com
      headers:
        A: "1"
        B: ${string(2)}
`,
			check: func(t *testing.T, step *v1.Node) {
				requireExpr(t, step.GetTask().GetInputs()["headers"], `{"A": "1", "B": string(2)}`)
			},
		},
		{
			name: "a structure of literals stays a literal",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    http:
      url: https://example.com
      headers:
        A: "1"
        B: "2"
`,
			check: func(t *testing.T, step *v1.Node) {
				got := step.GetTask().GetInputs()["headers"]
				if got.GetExpr() != nil {
					t.Error("a map of literals should not become an expression")
				}
				entries := got.GetLiteral().GetMapValue().GetEntries()
				if len(entries) != 2 || entries[0].GetKey().GetStringValue() != "A" {
					t.Errorf("headers = %v, want entries in source order", entries)
				}
			},
		},
		{
			name: "spacing inside a fence does not change the expression",
			src:  taskInput(`message: "${ a.result }"`),
			check: func(t *testing.T, step *v1.Node) {
				requireExpr(t, step.GetTask().GetInputs()["message"], "a.result")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wf, err := flowfile.Unmarshal([]byte(tt.src))
			if err != nil {
				t.Fatalf("Unmarshal() error: %v", err)
			}
			tt.check(t, wf.GetSteps()[0])
		})
	}
}

// TestParseZeroValues pins that legitimately empty values survive compilation. The
// engine relies on them: an empty message is a message, and a step that spells out
// the default must compile to the same workflow as one that leaves it unsaid.
func TestParseZeroValues(t *testing.T) {
	src := `edition: v2026.2
name: zero
steps:
  - id: a
    continue_on_error: false
    log:
      message: ""
      args: [0, false, ""]
      count: 0
      enabled: false
`
	wf, err := flowfile.Unmarshal([]byte(src))
	if err != nil {
		t.Fatalf("Unmarshal() error: %v", err)
	}

	step := wf.GetSteps()[0]
	if step.GetPolicy() != nil {
		t.Errorf("policy = %v; a step that only spells out a default has declared no policy", step.GetPolicy())
	}

	inputs := step.GetTask().GetInputs()
	for name, check := range map[string]func(*v1.Value) bool{
		"message": func(v *v1.Value) bool { return v.GetLiteral().GetStringValue() == "" },
		"count":   func(v *v1.Value) bool { return v.GetLiteral().GetInt64Value() == 0 },
		"enabled": func(v *v1.Value) bool { return v.GetLiteral().GetBoolValue() == false },
	} {
		got, ok := inputs[name]
		if !ok {
			t.Fatalf("input %q was dropped", name)
		}
		if got.GetLiteral() == nil {
			t.Fatalf("input %q is not a literal: %v", name, got)
		}
		if !check(got) {
			t.Errorf("input %q = %v, want its zero value", name, got)
		}
	}

	args := inputs["args"].GetLiteral().GetListValue().GetValues()
	if len(args) != 3 {
		t.Fatalf("args = %v, want three elements", args)
	}
	if args[0].GetInt64Value() != 0 || args[1].GetBoolValue() != false || args[2].GetStringValue() != "" {
		t.Errorf("args = %v, want 0, false, and the empty string", args)
	}
}

// TestParseAnchorsAndMerge covers the YAML features a Flowfile inherits, which
// decoding into structs used to provide and a hand-written walk has to keep.
func TestParseAnchorsAndMerge(t *testing.T) {
	src := `edition: v2026.2
name: anchors
steps:
  - id: a
    retry: &policy
      attempts: 3
      interval: 1s
    log: &log
      message: one
  - id: b
    retry: *policy
    log: *log
  - id: c
    retry:
      <<: *policy
      attempts: 5
    log:
      message: three
`
	wf, err := flowfile.Unmarshal([]byte(src))
	if err != nil {
		t.Fatalf("Unmarshal() error: %v", err)
	}

	first, second, third := wf.GetSteps()[0], wf.GetSteps()[1], wf.GetSteps()[2]
	if !proto.Equal(first.GetPolicy().GetRetry(), second.GetPolicy().GetRetry()) {
		t.Errorf("an aliased retry policy should compile to the same policy:\n%s",
			cmp.Diff(first.GetPolicy().GetRetry(), second.GetPolicy().GetRetry(), protocmp.Transform()))
	}
	if second.GetTask().GetInputs()["message"].GetLiteral().GetStringValue() != "one" {
		t.Errorf("an aliased task should compile to the same task: %v", second.GetTask())
	}
	if got := third.GetPolicy().GetRetry(); got.GetMaxAttempts() != 5 || got.GetInitialInterval().AsDuration() != time.Second {
		t.Errorf("a merged retry policy = %v, want attempts 5 and the merged interval", got)
	}
}

// TestParseRejectsSelfReferentialAlias pins that a cyclic alias is reported rather
// than followed forever.
func TestParseRejectsSelfReferentialAlias(t *testing.T) {
	src := `edition: v2026.2
name: t
steps: &loop
  - id: a
    log:
      message: *loop
`
	if _, err := flowfile.Unmarshal([]byte(src)); err == nil {
		t.Fatal("expected a diagnostic for a self-referential alias")
	} else {
		t.Logf("reported: %v", err)
	}
}

// TestExamplesCompile checks every workflow shipped as an example, because they are
// the DSL's documentation: one that does not compile is a lie in the README.
func TestExamplesCompile(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "..", "examples", "*", "workflow.yaml"))
	if err != nil {
		t.Fatalf("finding examples: %v", err)
	}
	if len(paths) == 0 {
		t.Fatal("no examples found; the glob is wrong")
	}

	for _, path := range paths {
		t.Run(filepath.Base(filepath.Dir(path)), func(t *testing.T) {
			workflow, positions, err := flowfile.ParseFile(path)
			if err != nil {
				t.Fatalf("Parse() error: %v", err)
			}

			ds, err := flowfile.ValidateSourceFile(path)
			if err != nil {
				t.Fatalf("ValidateSource() error: %v", err)
			}
			if len(ds) != 0 {
				t.Fatalf("expected no diagnostics, got:\n%s", ds.Error())
			}

			// Every step is addressable, which is what a diagnostic about one needs.
			for _, step := range workflow.GetSteps() {
				if _, ok := positions.StepPath(step.GetId()); !ok {
					t.Errorf("no position recorded for step %q", step.GetId())
				}
			}

			requireRoundTripAt(t, workflow, path)
		})
	}
}

// TestRoundTripNestedControlFlow pins that a round trip preserves the parts most
// easily lost: a loop's body, a branch's steps, conditions, and policy.
func TestRoundTripNestedControlFlow(t *testing.T) {
	tests := []struct {
		name string
		src  string
	}{
		{
			name: "loop with a body",
			src: `edition: v2026.2
name: loop
steps:
  - id: outer
    for_each:
      items: ${[1, 2]}
      as: n
      max_parallel: 4
      steps:
        - id: inner
          if: ${n > 1}
          timeout: 5s
          log:
            message: ${string(n)}
`,
		},
		{
			name: "parallel branches",
			src: `edition: v2026.2
name: parallel
steps:
  - id: fan
    parallel:
      - steps:
          - id: left
            log:
              message: left
      - steps:
          - id: right
            continue_on_error: true
            log:
              message: right
`,
		},
		{
			name: "a loop inside a branch",
			src: `edition: v2026.2
name: nested
steps:
  - id: fan
    parallel:
      - steps:
          - id: repeat
            for_each:
              items: ${['a']}
              steps:
                - id: body
                  log:
                    message: ${item}
`,
		},
		{
			name: "policy in full",
			src: `edition: v2026.2
name: policy
steps:
  - id: a
    if: ${gate.result == 'go'}
    timeout: 30s
    retry:
      attempts: 3
      interval: 1s
      backoff: 2
      max_interval: 10s
    continue_on_error: true
    log:
      message: go
`,
		},
		{
			name: "retry asking only for the defaults",
			src: `edition: v2026.2
name: defaults
steps:
  - id: a
    retry: {}
    log:
      message: hi
`,
		},
		{
			name: "structures with nested expressions",
			src: `edition: v2026.2
name: shapes
steps:
  - id: a
    http:
      url: https://example.com
      headers:
        A: "1"
        B: ${string(2)}
      outputs: "${ {'status': status_code, 'body': body} }"
  - id: b
    vars:
      args:
        - ${steps.a.status}
        - 0
    log:
      message: ${string(args)}
`,
		},
		{
			name: "literal structures and zero values",
			src: `edition: v2026.2
name: literals
steps:
  - id: a
    vars:
      empty: ""
      list: [1, 2, 3]
      nested:
        k: v
        n: 0
    log:
      message: ${empty}
`,
		},
		{
			name: "description present but empty",
			src: `edition: v2026.2
name: described
description: ""
steps:
  - id: a
    log:
      message: hi
`,
		},
		{
			name: "input constraints, example and sensitive",
			src: `edition: v2026.2
name: constrained
inputs:
  email:
    type: string
    required: true
    example: someone@example.com
    sensitive: true
    must: this.matches('^[^@]+@[^@]+$')
    min_len: 3
    max_len: 128
  replicas:
    type: int
    default: 1
    must: this >= 1 && this <= 50
  regions:
    type: list
    required: true
    example: [us-east-1]
    min_items: 1
    max_items: 10
    must: this == this.distinct()
outputs:
  tracking:
    value: ${inputs.email}
    must: this != ""
    sensitive: true
steps:
  - id: a
    log:
      message: ${inputs.email}
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			workflow, err := flowfile.Unmarshal([]byte(tt.src))
			if err != nil {
				t.Fatalf("Unmarshal() error: %v", err)
			}
			requireRoundTrip(t, workflow)
		})
	}
}

// TestMarshalReportsWhatItCannotWrite pins that Marshal refuses to write a
// document that would read back as something else, rather than writing it.
//
// The comprehension case is a real limitation rather than a choice: the compiled
// expression no longer records the macro its source used, so cel-go cannot write it
// back. It is fixed by tracking macro calls in the shared CEL environment, and this
// test passes either way — once that lands, the expression round-trips instead.
func TestMarshalReportsWhatItCannotWrite(t *testing.T) {
	t.Run("comprehension", func(t *testing.T) {
		workflow, err := flowfile.Unmarshal([]byte(`edition: v2026.2
name: t
steps:
  - id: a
    log:
      message: ${['x'].map(v, v).size()}
`))
		if err != nil {
			t.Fatalf("Unmarshal() error: %v", err)
		}

		data, err := flowfile.Marshal(workflow)
		if err == nil {
			t.Logf("comprehensions now round-trip:\n%s", data)
			requireRoundTrip(t, workflow)
			return
		}
		if !strings.Contains(err.Error(), "macro") {
			t.Errorf("Marshal() = %v, want it to explain that a macro cannot be written back", err)
		}
		t.Logf("reported: %v", err)
	})

	t.Run("literal that would read back as an expression", func(t *testing.T) {
		workflow := &v1.Workflow{
			Name: "t",
			Steps: []*v1.Node{{
				Id: "a",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "log",
					Inputs: map[string]*v1.Value{"message": v1.NewLiteral("${a.result}")},
				}},
			}},
		}
		if _, err := flowfile.Marshal(workflow); err == nil {
			t.Error("Marshal() wrote a literal that would be read back as an expression")
		} else {
			t.Logf("reported: %v", err)
		}
	})

	t.Run("literal string as a condition", func(t *testing.T) {
		workflow := &v1.Workflow{
			Name: "t",
			Steps: []*v1.Node{{
				Id:        "a",
				Condition: v1.NewLiteral("ready"),
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "log",
					Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hi")},
				}},
			}},
		}
		if _, err := flowfile.Marshal(workflow); err == nil {
			t.Error("Marshal() wrote a literal string where a string means an expression")
		} else {
			t.Logf("reported: %v", err)
		}
	})
}

// requireRoundTrip asserts that writing a workflow out and reading it back yields
// the same workflow.
func requireRoundTrip(t *testing.T, workflow *v1.Workflow) {
	t.Helper()
	requireRoundTripAt(t, workflow, "")
}

// requireRoundTripAt is [requireRoundTrip] for a workflow that may contain a
// `call:` step, which Marshal writes back as the path it names — the
// specification's compiled copy, never the source — so re-parsing that output
// needs the same location a `call:` in it would have been compiled with.
func requireRoundTripAt(t *testing.T, workflow *v1.Workflow, path string) {
	t.Helper()

	data, err := flowfile.Marshal(workflow)
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}

	var again *v1.Workflow
	if path != "" {
		again, _, err = flowfile.ParseAt(data, path)
	} else {
		again, err = flowfile.Unmarshal(data)
	}
	if err != nil {
		t.Fatalf("Unmarshal(Marshal()) error: %v\n%s", err, data)
	}

	// The profile is not part of what a file round-trips, and cannot be.
	//
	// It records the vocabulary a spec was compiled against, and the compiler
	// stamps it — there is deliberately no `profile:` key, since an author choosing
	// a vocabulary per file is what one profile removes. So Marshal has nothing to
	// write it as, and Unmarshal always stamps the current one.
	//
	// That is correct rather than a gap. A Flowfile is the source and the spec is
	// the artifact: recompiling a file with a build whose profile is newer *should*
	// yield the newer profile, the same as recompiling picks up anything else that
	// build does. What must not move is the profile recorded in a spec already
	// executing, and nothing here touches that.
	//
	// Asserted explicitly rather than ignored, so a round trip that dropped the
	// stamp — or wrote one the compiler did not choose — still fails.
	if again.GetProfile() != v1.CurrentProfile {
		t.Errorf("round trip produced profile %q, want the compiler's %q",
			again.GetProfile(), v1.CurrentProfile)
	}
	normalized := proto.Clone(again).(*v1.Workflow)
	normalized.Profile = workflow.GetProfile()

	if !proto.Equal(workflow, normalized) {
		t.Errorf("round trip changed the workflow:\n%s\nwritten as:\n%s",
			cmp.Diff(workflow, normalized, protocmp.Transform()), data)
	}
}

// requireExpr asserts that a value is the expression with the given source.
func requireExpr(t *testing.T, value *v1.Value, want string) {
	t.Helper()

	if value == nil {
		t.Fatalf("value is missing, want the expression %q", want)
	}
	if value.GetExpr() == nil {
		t.Fatalf("value is %v, want the expression %q", value, want)
	}
	got, err := cel.AstToString(cel.ParsedExprToAst(value.GetExpr()))
	if err != nil {
		t.Fatalf("reading the expression back: %v", err)
	}
	if got != want {
		t.Errorf("expression = %q, want %q", got, want)
	}
}

// stepWith returns a workflow whose single step carries the given property line.
func stepWith(property string) string {
	return "edition: v2026.2\nname: t\nsteps:\n  - id: a\n    " + property + `
    log:
      message: hi
`
}

// taskInput returns a workflow whose single step has the given task input line.
func taskInput(input string) string {
	return `edition: v2026.2
name: t
steps:
  - id: a
    log:
      ` + input + "\n"
}

// asDiagnostics reports whether an error is a set of diagnostics.
func asDiagnostics(err error, into *flowfile.Diagnostics) bool {
	ds, ok := err.(flowfile.Diagnostics)
	if ok {
		*into = ds
	}
	return ok
}

// TestParseRejects covers the remaining ways a Flowfile can be wrong, where what
// matters is that the message names the problem rather than exactly where it is.
func TestParseRejects(t *testing.T) {
	tests := []struct {
		name string
		src  string
		want string
	}{
		{
			name: "iterator that is not a name",
			src: forEachWith(`items: ${[1]}
      as: 5`),
			want: "must be a string",
		},
		{
			name: "max_parallel that is not a number",
			src: forEachWith(`items: ${[1]}
      max_parallel: many`),
			want: "must be a whole number",
		},
		{
			name: "max_parallel beyond what the schema allows",
			src: forEachWith(`items: ${[1]}
      max_parallel: 5000`),
			want: "must be between 0 and 1000",
		},
		{
			name: "for_each with no body",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    for_each:
      items: ${[1]}
`,
			want: "for_each requires steps",
		},
		{
			name: "backoff that is not a number",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    retry:
      backoff: fast
    log:
`,
			want: "must be a number",
		},
		{
			name: "continue_on_error that is not a bool",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    continue_on_error: sometimes
    log:
`,
			want: "must be true or false",
		},
		{
			name: "timeout of zero",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    timeout: 0s
    log:
`,
			want: "must be greater than zero",
		},
		{
			name: "parallel branch with no steps",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    parallel:
      - {}
`,
			want: "parallel branch 1 requires steps",
		},
		{
			name: "parallel with no branches",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    parallel: []
`,
			want: "parallel must have at least one branch",
		},
		{
			name: "unknown key in a parallel branch",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    parallel:
      - step:
          - id: b
`,
			want: `unknown key "step"; did you mean "steps"?`,
		},
		{
			name: "a task whose inputs are not a mapping",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    log: hello
`,
			// The value under a task's name is its inputs, so a scalar there is an
			// author reaching for a shorthand the grammar does not have.
			want: "must be a mapping of keys to values, but a string was written here",
		},
		{
			name: "non-string key in an input map",
			src: taskInput(`headers:
        1: one`),
			want: "keys must be strings, but a number was written here",
		},
		{
			name: "unterminated expression",
			src:  taskInput(`message: ${a.result`),
			want: "unterminated expression",
		},
		{
			name: "two expressions in one value",
			src:  taskInput(`message: ${a.result} ${b.result}`),
			want: "must be the whole value",
		},
		{
			name: "expression in a workflow name",
			src: `edition: v2026.2
name: ${chosen}
steps:
  - id: a
    log:
`,
			want: "name: cannot be an expression",
		},
		{
			name: "nested value with no value",
			src: taskInput(`headers:
        A:`),
			want: "has no value",
		},
		{
			name: "a tagged value",
			src:  taskInput(`message: !!str 5`),
			want: "cannot be used as a value",
		},
		{
			name: "nested deeper than a Flowfile goes",
			src:  taskInput("message: " + strings.Repeat("[", 200) + strings.Repeat("]", 200)),
			want: "nests more than 64 levels deep",
		},
		{
			name: "an alias expanded past what a Flowfile holds",
			src: taskInput(`a: &a [x, x, x, x, x, x, x, x, x, x]
      b: &b [*a, *a, *a, *a, *a, *a, *a, *a, *a, *a]
      c: &c [*b, *b, *b, *b, *b, *b, *b, *b, *b, *b]
      d: &d [*c, *c, *c, *c, *c, *c, *c, *c, *c, *c]
      e: &e [*d, *d, *d, *d, *d, *d, *d, *d, *d, *d]
      f: [*e, *e, *e, *e, *e, *e, *e, *e, *e, *e]`),
			want: "holds more than 100000 values",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := flowfile.Unmarshal([]byte(tt.src))
			if err == nil {
				t.Fatal("Unmarshal() succeeded, want a diagnostic")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("diagnostics do not mention %q; got:\n%v", tt.want, err)
			}
			t.Logf("reported: %v", err)
		})
	}
}

// TestParseValueKinds covers every kind of value the DSL can hold, both as a
// literal and inside a structure that a nested expression turns into one
// expression. The two paths build values separately, so a type handled by one and
// not the other is exactly the kind of gap that survives review.
func TestParseValueKinds(t *testing.T) {
	literals := `edition: v2026.2
name: t
steps:
  - id: a
    vars:
      text: plain
      number: 7
      fraction: 1.5
      flag: true
      nothing: null
      big: 18446744073709551615
      list: [1, hello, false, 2.5]
      block: |
        two
        lines
      mapping:
        k: v
    log:
      message: ${text}
`
	workflow, err := flowfile.Unmarshal([]byte(literals))
	if err != nil {
		t.Fatalf("Unmarshal() error: %v", err)
	}
	values := workflow.GetSteps()[0].GetVars()
	for name, want := range map[string]string{
		"text":     "plain",
		"number":   "7",
		"fraction": "1.5",
		"flag":     "true",
		"nothing":  "null",
		"big":      "18446744073709551615",
		"block":    "two\nlines\n",
	} {
		got := values[name].GetLiteral()
		if got == nil {
			t.Errorf("var %q is not a literal: %v", name, values[name])
			continue
		}
		if text := literalText(got); text != want {
			t.Errorf("var %q = %s, want %s", name, text, want)
		}
	}
	requireRoundTrip(t, workflow)

	// The same values, in a structure that one expression makes an expression.
	computed := `edition: v2026.2
name: t
steps:
  - id: a
    vars:
      vals:
        text: plain
        number: 7
        fraction: 1.5
        flag: true
        nothing: null
        list: [1, hello]
        computed: ${1 + 1}
    log:
      message: ${string(vals)}
`
	workflow, err = flowfile.Unmarshal([]byte(computed))
	if err != nil {
		t.Fatalf("Unmarshal() error: %v", err)
	}
	value := workflow.GetSteps()[0].GetVars()["vals"]
	requireExpr(t, value,
		`{"text": "plain", "number": 7, "fraction": 1.5, "flag": true, "nothing": null, "list": [1, "hello"], "computed": 1 + 1}`)
	requireRoundTrip(t, workflow)
}

// literalText renders a literal for a message about one that is not what it should
// be.
func literalText(literal *expr.Value) string {
	switch kind := literal.GetKind().(type) {
	case *expr.Value_StringValue:
		return kind.StringValue
	case *expr.Value_Int64Value:
		return strconv.FormatInt(kind.Int64Value, 10)
	case *expr.Value_Uint64Value:
		return strconv.FormatUint(kind.Uint64Value, 10)
	case *expr.Value_DoubleValue:
		return strconv.FormatFloat(kind.DoubleValue, 'g', -1, 64)
	case *expr.Value_BoolValue:
		return strconv.FormatBool(kind.BoolValue)
	case *expr.Value_NullValue:
		return "null"
	default:
		return fmt.Sprintf("%v", literal)
	}
}

// forEachWith returns a workflow whose single step is a loop with the given body of
// for_each keys.
func forEachWith(keys string) string {
	return `edition: v2026.2
name: t
steps:
  - id: a
    for_each:
      ` + keys + `
      steps:
        - id: b
          log:
`
}

// TestVarsUnderATaskThatHasNoVarsIsReportedWhereItWasWritten pins a diagnostic's
// position, which is the thing this file treats as a feature.
//
// `vars:` is a step key, one level up from a task's inputs — so `vars:` written
// *under* a task is an input no task declares. The hoist that once flattened it for
// the cel task fired on the key name alone, for every task, so writing `vars:` under
// a task emptied it into the surrounding inputs and then reported the *contents* as
// unknown inputs:
//
//	step "a" input "greeting": task "log" has no such input; it accepts message
//	step "a": task "log" requires input "message" (a string)
//
// Neither line names `vars`, which is the only thing the author wrote wrong. They
// are sent looking for a mistake in a key they did not type, at a level they did
// not type it at, while the real one sits untouched above.
func TestVarsUnderATaskThatHasNoVarsIsReportedWhereItWasWritten(t *testing.T) {
	t.Parallel()

	const src = `edition: v2026.2
name: misplaced-vars
steps:
  - id: a
    log:
      message: hi
      vars:
        greeting: hello
`

	diagnostics, err := flowfile.ValidateSource([]byte(src))
	if err != nil {
		t.Fatalf("the file did not compile at all: %v", err)
	}
	if len(diagnostics) == 0 {
		t.Fatal("vars under a task that declares no vars input was accepted")
	}

	reported := diagnostics.Error()

	// It is reported as an input `log` does not have — which is exactly what it
	// is — and it is reported against `vars`, the key in the file.
	if !strings.Contains(reported, `"vars"`) {
		t.Errorf("the diagnostic does not name the key the author actually wrote:\n%s", reported)
	}

	// And not against its contents, which the author never wrote at that level.
	if strings.Contains(reported, "greeting") {
		t.Errorf("the diagnostic points inside vars, at a key that only exists after the hoist:\n%s", reported)
	}
}
