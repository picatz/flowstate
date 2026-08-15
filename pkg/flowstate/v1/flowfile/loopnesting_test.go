package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A loop inside a loop is refused rather than accepted untested: the engine does
// not suspend below the top of a loop body, so an inner loop's Continue-As-New
// interaction across two carried-state frames is a shape docs/DSL.md defers and
// nothing exercises. Accepting it would hand an author de-facto semantics the
// project will not stand behind — a fail-closed violation. These lock the refusal,
// and guard against it becoming an over-refusal of the shapes that *are* supported.

func validateSource(t *testing.T, src string) flowfile.Diagnostics {
	t.Helper()
	wf, err := flowfile.Unmarshal([]byte(src))
	require.NoErrorf(t, err, "the fixture must compile for its validation to be under test:\n%s", src)
	return flowfile.Validate(wf)
}

const nestedLoopRefusal = "not supported in this edition"

// TestNestedLoopIsRefused covers the refusal in the shapes that share a loop's
// suspend scope, and the accepted shapes it must not catch.
func TestNestedLoopIsRefused(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		src     string
		refused bool
	}{
		{
			name:    "a loop directly inside a loop is refused",
			refused: true,
			src: `edition: v2026.3
name: t
steps:
  - id: outer
    loop:
      as: a
      init: ${0}
      update: ${a + 1}
      until: ${a >= 2}
      max_iterations: 5
      steps:
        - id: inner
          loop:
            as: b
            init: ${0}
            update: ${b + 1}
            until: ${b >= 1}
            max_iterations: 3
            steps:
              - id: tick
                log:
                  message: hi
`,
		},
		{
			name:    "a loop transitively inside a loop, through a for_each, is refused",
			refused: true,
			src: `edition: v2026.3
name: t
steps:
  - id: outer
    loop:
      as: a
      init: ${0}
      update: ${a + 1}
      until: ${a >= 2}
      max_iterations: 5
      steps:
        - id: fan
          for_each:
            items: ${['x']}
            as: item
            steps:
              - id: inner
                loop:
                  as: b
                  init: ${0}
                  update: ${b + 1}
                  until: ${b >= 1}
                  max_iterations: 3
                  steps:
                    - id: tick
                      log:
                        message: hi
`,
		},
		{
			name:    "a for_each inside a loop is accepted",
			refused: false,
			src: `edition: v2026.3
name: t
steps:
  - id: outer
    loop:
      as: a
      init: ${0}
      update: ${a + 1}
      until: ${a >= 2}
      max_iterations: 5
      steps:
        - id: fan
          for_each:
            items: ${['x', 'y']}
            as: item
            steps:
              - id: body
                log:
                  message: ${item}
`,
		},
		{
			name:    "a parallel inside a loop is accepted",
			refused: false,
			src: `edition: v2026.3
name: t
steps:
  - id: outer
    loop:
      as: a
      init: ${0}
      update: ${a + 1}
      until: ${a >= 2}
      max_iterations: 5
      steps:
        - id: branches
          parallel:
            - steps:
                - id: left
                  log:
                    message: left
`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ds := validateSource(t, tc.src)
			if tc.refused {
				require.Truef(t, containsMessage(ds, nestedLoopRefusal), "expected refusal, got %v", ds)
				return
			}
			require.Falsef(t, containsMessage(ds, nestedLoopRefusal), "must not over-refuse, got %v", ds)
			require.Emptyf(t, ds, "the accepted shape is valid, got %v", ds)
		})
	}
}

// TestLoopReachedThroughCallIsRefused pins the call-boundary case separately:
// multiplying two loop ceilings through a callee must not bypass the nesting
// refusal merely because the inner loop belongs to an isolated workflow scope.
func TestLoopReachedThroughCallIsRefused(t *testing.T) {
	t.Parallel()

	inner := &v1.Workflow{
		Name:    "inner",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id: "inner-loop",
			Kind: &v1.Node_Loop{Loop: &v1.Loop{
				State: "b", Initial: v1.NewLiteral(int64(0)), Update: v1.NewExpr("b + 1"),
				Until: v1.NewExpr("b >= 1"), MaxIterations: 3,
			}},
		}},
	}
	outer := &v1.Workflow{
		Name:    "outer",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id: "outer-loop",
			Kind: &v1.Node_Loop{Loop: &v1.Loop{
				State: "a", Initial: v1.NewLiteral(int64(0)), Update: v1.NewExpr("a + 1"),
				Until: v1.NewExpr("a >= 1"), MaxIterations: 3,
				Body: []*v1.Node{{Id: "callee", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: inner}}}},
			}},
		}},
	}

	ds := flowfile.Validate(outer)
	require.Truef(t, containsMessage(ds, nestedLoopRefusal), "expected refusal, got %v", ds)
}

// TestLoopAsNameOutputReferenceIsCaught covers ask #3's diagnostic: a reference to a
// loop's `as:` name from outside the loop names the carried value under a name it
// does not have out there, and is pointed at `state`.
func TestLoopAsNameOutputReferenceIsCaught(t *testing.T) {
	t.Parallel()

	loop := `edition: v2026.3
name: t
steps:
  - id: countup
    loop:
      as: acc
      init: ${0}
      update: ${acc + 1}
      until: ${acc >= 3}
      max_iterations: 5
      steps:
        - id: tick
          log:
            message: hi
`

	t.Run("referencing the as: name is caught and points at state", func(t *testing.T) {
		t.Parallel()
		ds := validateSource(t, loop+
			"  - id: report\n    log:\n      message: \"${'x' + string(steps.countup.acc)}\"\n")
		require.NotEmpty(t, ds)
		msg := ds.Error()
		require.Contains(t, msg, `has no output "acc"`)
		require.Contains(t, msg, "steps.countup.state", "the diagnostic must name the output that holds the carried value")
	})

	t.Run("state and results resolve", func(t *testing.T) {
		t.Parallel()
		ds := validateSource(t, loop+
			"  - id: report\n    log:\n      message: \"${string(steps.countup.state) + string(steps.countup.results.size())}\"\n")
		require.Emptyf(t, ds, "steps.<loop>.state and .results are the loop's real outputs, got %v", ds)
	})
}

func containsMessage(ds flowfile.Diagnostics, substr string) bool {
	for _, d := range ds {
		if strings.Contains(d.Message, substr) {
			return true
		}
	}
	return false
}
