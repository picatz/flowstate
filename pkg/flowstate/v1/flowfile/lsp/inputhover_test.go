package lsp

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

const inputHoverSource = `edition: ` + flowfile.CurrentEdition + `
name: input-hover
inputs:
  env:
    type: enum
    values: [staging, production]
    description: Where to deploy.
    required: true
    example: staging
  token:
    type: string
    sensitive: true
    min_len: 8
    must: this.startsWith("t-")
vars:
  region: us-east-1
steps:
  - id: show
    value: ${inputs.env == "staging" && vars.region == "us-east-1"}
`

// TestHoverDescribesADeclaredInputWhereverItIsMet checks that a workflow input
// reads the same on the declaration key and on a reference to it, with its enum
// members, bounds, constraint and example, which is the same rendering a callee's
// input gets across a `call:`.
func TestHoverDescribesADeclaredInputWhereverItIsMet(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()
	const uri = "file:///input-hover.yaml"
	require.Empty(t, messages(c.open(uri, inputHoverSource).Diagnostics), "premise: the file compiles")

	hover := func(needle string, offset int) string {
		pos := positionOf(t, inputHoverSource, needle, offset)
		got := c.hover(uri, pos.Line, pos.Character)
		require.NotNil(t, got, "no hover at %q+%d", needle, offset)

		return hoverText(got)
	}

	fromReference := hover("inputs.env", len("inputs.e"))
	fromDeclaration := hover("  env:", len("  e"))
	assert.Equal(t, fromReference, fromDeclaration, "the declaration and a use of it say one thing")
	assert.Contains(t, fromDeclaration, "**`env`** · `enum` · required")
	assert.Contains(t, fromDeclaration, "Where to deploy.")
	assert.Contains(t, fromDeclaration, "One of `staging`, `production`.")
	assert.Contains(t, fromDeclaration, "Example: `\"staging\"`.")

	token := hover("  token:", len("  t"))
	assert.Contains(t, token, "sensitive")
	assert.Contains(t, token, "Held to at least 8 characters.")
	assert.Contains(t, token, "Must satisfy `this.startsWith(\"t-\")`.")
}

// TestHoverDescribesAWorkflowVar checks that `vars.<name>` answers with the
// expression written for it.
func TestHoverDescribesAWorkflowVar(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()
	const uri = "file:///var-hover.yaml"
	c.open(uri, inputHoverSource)

	pos := positionOf(t, inputHoverSource, "vars.region", len("vars.r"))
	got := hoverText(c.hover(uri, pos.Line, pos.Character))
	assert.Contains(t, got, "**`vars.region`** · workflow var, declared on line 16")
	assert.Contains(t, got, "`us-east-1`")

	// A method on the var is the function's to describe, not the var's.
	src := strings.Replace(inputHoverSource, `vars.region == "us-east-1"`, `vars.region.upperAscii() == "US-EAST-1"`, 1)
	c.open("file:///var-method.yaml", src)
	pos = positionOf(t, src, "upperAscii", 2)
	got = hoverText(c.hover("file:///var-method.yaml", pos.Line, pos.Character))
	assert.NotContains(t, got, "workflow var")
}

// TestCompletionOffersTheDeclaredInputs checks that `${inputs.` completes to the
// file's own declarations even while the half-typed expression keeps the file
// from compiling, and that a file declaring none offers no inputs root.
func TestCompletionOffersTheDeclaredInputs(t *testing.T) {
	t.Parallel()

	src := `edition: ` + flowfile.CurrentEdition + `
name: input-completion
inputs:
  env:
    type: enum
    values: [staging, production]
    required: true
steps:
  - id: show
    value: ${inputs.}
`
	c := newClient(t)
	c.initialize()
	const uri = "file:///input-completion.yaml"
	c.open(uri, src)

	pos := positionOf(t, src, "inputs.", len("inputs."))
	var labels []string
	for _, it := range c.complete(uri, pos.Line, pos.Character).Items {
		labels = append(labels, it.Label)
	}
	assert.Equal(t, []string{"env"}, labels)

	// A file that declares none teaches no inputs root.
	bare := "edition: " + flowfile.CurrentEdition + "\nname: none\nsteps:\n  - id: show\n    value: ${inputs.}\n"
	c.open("file:///no-inputs.yaml", bare)
	pos = positionOf(t, bare, "inputs.", len("inputs."))
	assert.Empty(t, c.complete("file:///no-inputs.yaml", pos.Line, pos.Character).Items)
}
