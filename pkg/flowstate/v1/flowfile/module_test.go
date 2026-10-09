package flowfile_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// moduleSource is a module in canonical form: the types, functions and errors a
// workflow would import, and nothing that runs.
const moduleSource = `edition: ` + flowfile.CurrentEdition + `
name: ids
description: Identifiers shared by the billing workflows.
types:
  Uuid:
    type: string
    must: isUuid(this)
  Customer:
    fields:
      id:
        type: Uuid
        required: true
      email:
        type: string
errors:
  NotFound:
    description: The customer does not exist.
functions:
  isUuid:
    params:
      s: string
    returns: bool
    body: ${s.matches("^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")}
`

func writeModule(t *testing.T, src string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ids.yaml")
	require.NoError(t, os.WriteFile(path, []byte(src), 0o600))
	return path
}

// TestAModuleIsAFirstClassFile pins what a module is allowed to be: a file with
// no steps that validates, formats to itself, and is derived to be a module from
// the compiled workflow alone, with no marker to claim.
func TestAModuleIsAFirstClassFile(t *testing.T) {
	t.Parallel()

	wf, err := flowfile.Unmarshal([]byte(moduleSource))
	require.NoError(t, err)
	assert.True(t, v1.IsModule(wf))
	assert.Empty(t, wf.GetSteps())

	ds, err := flowfile.ValidateSource([]byte(moduleSource))
	require.NoError(t, err)
	assert.Empty(t, ds, "a module is accepted by validation")

	path := writeModule(t, moduleSource)
	ds, err = flowfile.ValidateSourceFile(path)
	require.NoError(t, err)
	assert.Empty(t, ds, "the file-aware door agrees, source digest stamp included")

	out, err := flowfile.Format([]byte(moduleSource), wf)
	require.NoError(t, err)
	assert.Equal(t, moduleSource, string(out), "fmt round trip is byte for byte")

	marshaled, err := flowfile.Marshal(wf)
	require.NoError(t, err)
	again, err := flowfile.Unmarshal(marshaled)
	require.NoError(t, err)
	assert.True(t, v1.IsModule(again), "marshal and unmarshal keep a module a module")
}

// TestAModuleIsNotRunnable is the refusal in the direction that matters: the
// loaders a run, a compile and a debugger go through turn a valid module away,
// and Validate, which answers "can this spec run", says why.
func TestAModuleIsNotRunnable(t *testing.T) {
	t.Parallel()

	path := writeModule(t, moduleSource)

	wf, _, ds, err := flowfile.ParseAndValidateFileAt(path)
	require.NoError(t, err)
	require.NotNil(t, wf)
	require.Len(t, ds, 1)
	assert.Contains(t, ds[0].Message, "is a module (no steps); import it with use:, don't run it")

	wf, err = flowfile.Unmarshal([]byte(moduleSource))
	require.NoError(t, err)
	ds = flowfile.Validate(wf)
	require.Len(t, ds, 1)
	assert.Equal(t, v1.ErrModule.Error(), ds[0].Message)
	assert.Error(t, v1.Validate(wf), "the schema independently refuses a spec with no steps")
}

// TestAModuleHoldsItsDeclarationsToTheirRules proves a module is validated, not
// waved through: a declaration error in a module is reported with a position.
func TestAModuleHoldsItsDeclarationsToTheirRules(t *testing.T) {
	t.Parallel()

	bad := `edition: ` + flowfile.CurrentEdition + `
name: ids
types:
  Uuid:
    type: string
    must: nope(this)
`
	ds, err := flowfile.ValidateSource([]byte(bad))
	require.NoError(t, err)
	require.NotEmpty(t, ds, "an undeclared function in a module's type is refused where it is written")
	assert.Equal(t, 4, ds[0].Line)
	assert.Contains(t, ds[0].Message, "nope")

	badName := `edition: ` + flowfile.CurrentEdition + `
name: not a name
errors:
  Gone: {}
`
	ds, err = flowfile.ValidateSource([]byte(badName))
	require.NoError(t, err)
	require.NotEmpty(t, ds)
	assert.Contains(t, ds.Error(), "name may not contain")
}

// TestOnlyDeclarationsMakeAModule is the boundary: a steps-less file that says
// anything else, or nothing at all, is still a workflow with no steps and is
// refused as one, so a mistake is not quietly promoted to a module.
func TestOnlyDeclarationsMakeAModule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want string
	}{
		{"nothing declared", "edition: " + flowfile.CurrentEdition + "\nname: empty\n", "workflow has no steps"},
		{"inputs beside declarations", "edition: " + flowfile.CurrentEdition + "\nname: m\ninputs:\n  a:\n    type: string\nerrors:\n  Gone: {}\n", "a file with no steps is a module"},
		{"vars beside declarations", "edition: " + flowfile.CurrentEdition + "\nname: m\nvars:\n  a: 1\nerrors:\n  Gone: {}\n", "a file with no steps is a module"},
		{"outputs beside declarations", "edition: " + flowfile.CurrentEdition + "\nname: m\noutputs:\n  a:\n    value: ${1}\nerrors:\n  Gone: {}\n", "a file with no steps is a module"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ds, err := flowfile.ValidateSource([]byte(tt.src))
			if err != nil {
				assert.Contains(t, err.Error(), tt.want)
				return
			}
			require.NotEmpty(t, ds)
			assert.Contains(t, ds.Error(), tt.want)
		})
	}

	assert.False(t, v1.IsModule(nil))
	assert.False(t, v1.IsModule(&v1.Workflow{Name: "x"}))
	assert.False(t, v1.IsModule(&v1.Workflow{
		Name:           "x",
		DeclaredErrors: []*v1.ErrorDeclaration{{Name: "Gone"}},
		Steps:          []*v1.Node{{Id: "a"}},
	}), "a spec with steps is never a module, whatever else it declares")

	// An empty block carries no content, so it does not disqualify a module.
	empty, err := flowfile.Unmarshal([]byte("edition: " + flowfile.CurrentEdition + "\nname: m\ninputs: {}\nerrors:\n  Gone: {}\n"))
	require.NoError(t, err)
	assert.True(t, v1.IsModule(empty), "inputs: {} beside a declaration is still a module")
}
