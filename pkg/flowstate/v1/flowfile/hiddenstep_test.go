package flowfile_test

import (
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUnresolvedReferenceSaysWhereAHiddenStepLives holds the rule of #1433: a
// reference to a step that exists but is out of reach is described as that, in the
// words of the block that hides it, and a name declared nowhere stays "unknown".
func TestUnresolvedReferenceSaysWhereAHiddenStepLives(t *testing.T) {
	t.Parallel()

	const head = "edition: v2026.4\nname: hidden\nsteps:\n"
	tests := []struct {
		name string
		src  string
		want string
	}{
		{"other switch case", head + `  - id: pick
    switch:
      value: "${'a'}"
      cases:
        - case: a
          steps:
            - id: one
              log: {message: x}
        - case: b
          steps:
            - id: two
              log: {message: "${steps.one.result}"}
`, `which is not visible from here; it is declared in a case of switch "pick" (only that case's own steps can read it)`},
		{"loop body from a later step", head + `  - id: spin
    loop:
      until: "${true}"
      max_iterations: 2
      steps:
        - id: inner
          log: {message: x}
  - id: after
    log: {message: "${steps.inner.result}"}
`, "it is declared inside the body of loop \"spin\" (read its values through `steps.spin.results`)"},
		{"forward reference inside a case", head + `  - id: pick
    switch:
      value: "${'a'}"
      cases:
        - case: a
          steps:
            - id: one
              log: {message: "${steps.two.result}"}
            - id: two
              log: {message: x}
`, `references step "two", which runs later`},
		{"forward reference inside a loop body", head + `  - id: spin
    loop:
      until: "${true}"
      max_iterations: 2
      steps:
        - id: a1
          log: {message: "${steps.b1.result}"}
        - id: b1
          log: {message: x}
`, `references step "b1", which runs later`},
		{"forward reference inside a branch", head + `  - id: fan
    parallel:
      - steps:
          - id: a1
            log: {message: "${steps.b1.result}"}
          - id: b1
            log: {message: x}
`, `references step "b1", which runs later`},
		{"declared nowhere", head + `  - id: after
    log: {message: "${steps.nothing.result}"}
`, `references unknown step "nothing"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ds, err := flowfile.ValidateSource([]byte(tt.src))
			require.NoError(t, err)
			assert.Contains(t, ds.Error(), tt.want)
		})
	}
}
