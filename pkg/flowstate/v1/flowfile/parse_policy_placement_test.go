package flowfile_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// policyPlacementInject is the two ways an author can bind step policy: a
// `retry:` block and a `timeout:` scalar, each written at the step's own
// indentation (four spaces beneath `- id: s`), matching where the wait tests
// inject them.
var policyPlacementInject = map[string]string{
	"retry":   "    retry:\n      attempts: 3\n",
	"timeout": "    timeout: 5m\n",
}

// minimalCalleeForPolicyPlacement is a callee with no declared inputs, so the
// caller fixture below needs no `with:` to stay clean.
const minimalCalleeForPolicyPlacement = `edition: v2026.3
name: callee
steps:
  - id: a
    log:
      message: hi
`

// policyPlacementFixture parses source for one non-task node kind, with extra
// injected at the step's own indentation, and returns the compile error — nil
// when the file is accepted.
type policyPlacementFixture struct {
	// subject is the exact phrase checkPolicyPlacement's diagnostic names for
	// this kind, so the test proves not just that some diagnostic fired but
	// that it named the right step.
	subject string
	parse   func(t *testing.T, extra string) error
}

// policyPlacementFixtures covers every kind checkPolicyPlacement refuses
// `retry:`/`timeout:` on, keyed by the node's oneof field name.
//
// TestPolicyPlacementCoversEveryNonTaskNodeKind below is what makes this
// exhaustive rather than a hand-kept guess: it walks `Node`'s `kind` oneof
// descriptor and fails if a field here has no entry, so a node kind added later
// without an answer here is caught by construction rather than by someone
// remembering to update this list.
var policyPlacementFixtures = map[string]policyPlacementFixture{
	"wait": {
		subject: "a waiting step",
		parse: func(t *testing.T, extra string) error {
			t.Helper()
			src := fmt.Sprintf(`edition: v2026.3
name: w
steps:
  - id: s
    sleep: 1h
%s`, extra)
			_, _, err := flowfile.Parse([]byte(src))
			return err
		},
	},
	"value": {
		subject: "a `value:` step",
		parse: func(t *testing.T, extra string) error {
			t.Helper()
			src := fmt.Sprintf(`edition: v2026.3
name: w
steps:
  - id: s
    value: ${1 + 1}
%s`, extra)
			_, _, err := flowfile.Parse([]byte(src))
			return err
		},
	},
	"for_each": {
		subject: "a `for_each:` step",
		parse: func(t *testing.T, extra string) error {
			t.Helper()
			src := fmt.Sprintf(`edition: v2026.3
name: w
steps:
  - id: s
    for_each:
      items: ${[1, 2, 3]}
      steps:
        - id: inner
          value: ${item}
%s`, extra)
			_, _, err := flowfile.Parse([]byte(src))
			return err
		},
	},
	"parallel": {
		subject: "a `parallel:` step",
		parse: func(t *testing.T, extra string) error {
			t.Helper()
			src := fmt.Sprintf(`edition: v2026.3
name: w
steps:
  - id: s
    parallel:
      - steps:
          - id: inner
            value: ${1}
%s`, extra)
			_, _, err := flowfile.Parse([]byte(src))
			return err
		},
	},
	"call": {
		subject: "a `call:` step",
		parse: func(t *testing.T, extra string) error {
			t.Helper()
			dir := t.TempDir()
			writeFile(t, dir, "callee.yaml", minimalCalleeForPolicyPlacement)
			caller := writeFile(t, dir, "caller.yaml", fmt.Sprintf(`edition: v2026.3
name: caller
steps:
  - id: s
    call: ./callee.yaml
%s`, extra))
			_, _, err := flowfile.ParseFile(caller)
			return err
		},
	},
	"loop": {
		subject: "a `loop:` step",
		parse: func(t *testing.T, extra string) error {
			t.Helper()
			src := fmt.Sprintf(`edition: v2026.3
name: w
steps:
  - id: s
    loop:
      as: cursor
      init: ${0}
      until: ${true}
      update: ${cursor}
      steps:
        - id: inner
          value: ${cursor}
%s`, extra)
			_, _, err := flowfile.Parse([]byte(src))
			return err
		},
	},
	"switch": {
		subject: "a `switch:` step",
		parse: func(t *testing.T, extra string) error {
			t.Helper()
			src := fmt.Sprintf(`edition: v2026.3
name: w
vars:
  mode: a
steps:
  - id: s
    switch:
      value: ${vars.mode}
      cases:
        - case: a
          steps:
            - id: inner
              value: ${1}
      default:
        steps:
          - id: fallback
            value: ${0}
%s`, extra)
			_, _, err := flowfile.Parse([]byte(src))
			return err
		},
	},
}

// TestPolicyPlacementCoversEveryNonTaskNodeKind makes the sweep below
// exhaustive by construction rather than by a hand-kept list of kind names.
//
// The issue this closes (flowstate#286) was itself found this way: the
// original report named three kinds accepting-and-ignoring `retry:`/
// `timeout:`, and reading `StepPolicy` back against the compiled node found
// two more, `loop:` and `switch:`, doing the identical thing. A fixed list of
// five strings here would repeat exactly that mistake for whatever kind is
// added seventh — walking the oneof's own descriptor cannot miss one.
func TestPolicyPlacementCoversEveryNonTaskNodeKind(t *testing.T) {
	t.Parallel()

	oneof := (&v1.Node{}).ProtoReflect().Descriptor().Oneofs().ByName("kind")
	require.NotNil(t, oneof, "Node has no \"kind\" oneof; the schema changed shape")

	fields := oneof.Fields()
	for i := range fields.Len() {
		name := string(fields.Get(i).Name())
		if name == "task" {
			// The one kind these keys actually bind to; covered separately by
			// TestRetryTimeoutStayOnTaskSteps and the nested variants below.
			continue
		}
		_, ok := policyPlacementFixtures[name]
		assert.True(t, ok, "node kind %q has no policyPlacementFixtures entry — "+
			"checkPolicyPlacement needs an arm for it (or a reason it does honour "+
			"retry/timeout), and this test needs a fixture proving it", name)
	}
}

// TestPolicyPlacementRefused is the sweep itself: for every fixture above,
// writing `retry:` or `timeout:` on that kind of step produces a positioned
// diagnostic naming that kind as the subject.
func TestPolicyPlacementRefused(t *testing.T) {
	t.Parallel()

	for kind, fixture := range policyPlacementFixtures {
		for key, extra := range policyPlacementInject {
			t.Run(kind+"/"+key, func(t *testing.T) {
				t.Parallel()

				err := fixture.parse(t, extra)
				require.Error(t, err, "%s on %s should be refused, not silently accepted", key, kind)
				require.Contains(t, err.Error(), "does nothing on "+fixture.subject,
					"the diagnostic should name %s as the subject", fixture.subject)

				// Diagnostics are a feature: a position is what makes this
				// actionable in an editor rather than merely true.
				require.Regexp(t, `\d+:\d+:`, err.Error(),
					"the diagnostic does not name a line and column")
			})
		}
	}
}

// TestPolicyPlacementLeavesTaskStepsAlone is the direction that matters more
// than the refusal itself: proof the five new arms do not over-reach into the
// steps *inside* a composite, which is exactly the shape a scope mistake in a
// rewriter or a check takes elsewhere in this package (see CLAUDE.md, "A
// rewriter has to know what the grammar binds"). `retry:`/`timeout:` on the
// task nested inside each composite's body must stay exactly as clean as one
// on a bare top-level task.
func TestPolicyPlacementLeavesTaskStepsAlone(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
	}{
		{
			name: "task nested inside for_each",
			src: `edition: v2026.3
name: w
steps:
  - id: fan
    for_each:
      items: ${[1, 2, 3]}
      steps:
        - id: inner
          log:
            message: hi
          retry:
            attempts: 3
          timeout: 5m
`,
		},
		{
			name: "task nested inside a parallel branch",
			src: `edition: v2026.3
name: w
steps:
  - id: par
    parallel:
      - steps:
          - id: inner
            log:
              message: hi
            retry:
              attempts: 3
            timeout: 5m
`,
		},
		{
			name: "task nested inside loop steps",
			src: `edition: v2026.3
name: w
steps:
  - id: accumulate
    loop:
      as: cursor
      init: ${0}
      until: ${true}
      update: ${cursor}
      steps:
        - id: inner
          log:
            message: hi
          retry:
            attempts: 3
          timeout: 5m
`,
		},
		{
			name: "task nested inside a switch case",
			src: `edition: v2026.3
name: w
vars:
  mode: a
steps:
  - id: pick
    switch:
      value: ${vars.mode}
      cases:
        - case: a
          steps:
            - id: inner
              log:
                message: hi
              retry:
                attempts: 3
              timeout: 5m
      default:
        steps: []
`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, _, err := flowfile.Parse([]byte(test.src))
			require.NoError(t, err, "retry/timeout on a task nested inside a composite should be accepted")

			ds, err := flowfile.ValidateSource([]byte(test.src))
			require.NoError(t, err)
			require.Empty(t, ds, "a nested task carrying retry/timeout should validate clean:\n%s", ds.Error())
		})
	}

	// The call fixture needs its own file on disk, for the same reason the
	// sweep above does.
	t.Run("task inside a called workflow", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		writeFile(t, dir, "callee.yaml", `edition: v2026.3
name: callee
steps:
  - id: a
    log:
      message: hi
    retry:
      attempts: 3
    timeout: 5m
`)
		caller := writeFile(t, dir, "caller.yaml", `edition: v2026.3
name: caller
steps:
  - id: s
    call: ./callee.yaml
`)

		_, _, err := flowfile.ParseFile(caller)
		require.NoError(t, err, "retry/timeout on a task inside a called workflow should be accepted")
	})
}

// TestMarshalRefusesUnrepresentablePolicyPlacement covers the path
// TestPolicyPlacementRefused cannot reach: a *v1.Workflow built directly in Go
// never passes through the compiler, so a `for_each`/`parallel`/`call`/`loop`/
// `switch`/`wait`/`value` node carrying a policy the parser would refuse can
// only be caught by [flowfile.Marshal] itself, which must not hand back a
// document that fails to parse.
func TestMarshalRefusesUnrepresentablePolicyPlacement(t *testing.T) {
	t.Parallel()

	forEachWithTimeout := &v1.Workflow{
		Name: "w",
		Steps: []*v1.Node{{
			Id: "fan",
			Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
				Items: v1.NewLiteralList(),
				Body: []*v1.Node{{
					Id:   "inner",
					Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}},
				}},
			}},
			Policy: &v1.StepPolicy{Timeout: durationpb.New(5 * time.Minute)},
		}},
	}

	_, err := flowfile.Marshal(forEachWithTimeout)
	require.Error(t, err, "Marshal must refuse a for_each step carrying a timeout it cannot write back")
	require.Contains(t, err.Error(), "for_each")

	// The ordinary case — a task with the identical policy — must keep working.
	taskWithTimeout := &v1.Workflow{
		Name: "w",
		Steps: []*v1.Node{{
			Id:     "a",
			Kind:   &v1.Node_Task{Task: &v1.Task{Name: "log"}},
			Policy: &v1.StepPolicy{Timeout: durationpb.New(5 * time.Minute)},
		}},
	}

	out, err := flowfile.Marshal(taskWithTimeout)
	require.NoError(t, err)
	require.Contains(t, string(out), "timeout:")
}
