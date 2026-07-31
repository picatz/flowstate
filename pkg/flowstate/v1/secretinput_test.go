package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// `${secret(...)}` compiles — the parser has a spelling for it and `flow validate`
// accepts one — and no task input accepts the reference it produces. So a workflow
// that writes one validates and then fails at run time, which is the shape this
// repo keeps having to fix.
//
// The failure it produced said:
//
//	step "fetch": task "http": unsupported value type: *flowstatev1.Value
//
// A Go type name, from a `default:` branch, naming neither the input nor the
// secret — for a spelling the validator had just blessed. An author had no way to
// tell what they had written wrong, or where it could go instead.
//
// This does not make the reference work. Where a secret may be written is a
// question about which inputs declare that they resolve one, and that decision is
// the owner's; what this fixes is being unable to find out that it does not.

// TestASecretWhereItCannotGoSaysSo covers the message an author actually meets.
func TestASecretWhereItCannotGoSaysSo(t *testing.T) {
	t.Parallel()

	_, err := v1.Run(t.Context(), &v1.Workflow{
		Name:    "secret-in-a-body",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id: "fetch",
			Kind: &v1.Node_Task{Task: &v1.Task{Name: "http", Inputs: map[string]*v1.Value{
				"url": v1.NewLiteral("https://example.com/"),
				"body": {Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{
					Scheme: "env", Name: "API_KEY",
				}}},
			}}},
		}},
	})
	require.Error(t, err, "a secret reference reached a field that cannot hold one and nothing said so")

	// The three things the old message left out, and the ones an author needs: the
	// input they wrote it in, the reference they wrote, and why it did not work.
	assert.Contains(t, err.Error(), `"body"`,
		"the failure does not name the input, so an author with several cannot tell which")
	assert.Contains(t, err.Error(), "env:API_KEY",
		"the failure does not name the secret that was written")
	assert.Contains(t, err.Error(), "secret reference",
		"the failure does not say a secret is what went wrong")

	// About this field, not about the task. A singular `flowstate.v1.Value` field
	// takes one whole — `plugin/sdk/values.go` does exactly that — so a task-wide
	// claim would send an author away from another input that would have worked.
	assert.NotContains(t, err.Error(), "no input of this task",
		"the failure claims something about every input of the task, which is false for "+
			"any task declaring a flowstate.v1.Value field")
	assert.Contains(t, err.Error(), "flowstate.v1.Value",
		"the failure does not say which kind of field can hold one")

	assert.NotContains(t, err.Error(), "*flowstatev1.Value",
		"the failure still reports a Go type, which is a fact about this program rather "+
			"than about the file somebody wrote")
}
