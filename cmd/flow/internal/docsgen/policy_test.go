package docsgen

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const policyFixture = `package fixture

// ExamplePolicy is the file form of the example policy.
type ExamplePolicy struct {
	// Allow holds CEL allow rules.
	Allow []string ` + "`yaml:\"allow,omitempty\"`" + `

	// Limit bounds one evaluation.
	Limit *uint64 ` + "`yaml:\"limit,omitempty\"`" + `
}

const credential = "sensitive-policy-value"
`

func TestPolicyReferenceIsDerivedFromThePolicyStruct(t *testing.T) {
	reference, err := ParsePolicyReference([]byte(policyFixture), "ExamplePolicy")
	require.NoError(t, err)
	require.Equal(t, "ExamplePolicy is the file form of the example policy.", reference.Description)
	require.Equal(t, []PolicyField{
		{Name: "allow", Type: "sequence of string", Description: "Allow holds CEL allow rules."},
		{Name: "limit", Type: "integer", Description: "Limit bounds one evaluation."},
	}, reference.Fields)

	rendered := (&Generator{src: Sources{TaskPolicy: reference}}).renderTaskPolicyReference()
	require.Contains(t, rendered, "`allow`")
	require.Contains(t, rendered, "`limit`")
	require.NotContains(t, rendered, "sensitive-policy-value",
		"the reference must describe the policy type, never values or unrelated source")
}

func TestPolicyReferenceRefusesAnUndocumentedField(t *testing.T) {
	source := `package fixture

// ExamplePolicy is a policy.
type ExamplePolicy struct {
	Allow []string ` + "`yaml:\"allow,omitempty\"`" + `
}`

	_, err := ParsePolicyReference([]byte(source), "ExamplePolicy")
	require.ErrorContains(t, err, "ExamplePolicy.Allow has no field documentation")
}

func TestPolicyReferenceChangesWhenThePolicyShapeChanges(t *testing.T) {
	reference, err := ParsePolicyReference([]byte(policyFixture), "ExamplePolicy")
	require.NoError(t, err)
	before := (&Generator{src: Sources{TaskPolicy: reference}}).renderTaskPolicyReference()

	changed := `package fixture

// ExamplePolicy is the file form of the example policy.
type ExamplePolicy struct {
	// Allow holds CEL allow rules.
	Allow []string ` + "`yaml:\"allow,omitempty\"`" + `

	// Limit bounds one evaluation.
	Limit *uint64 ` + "`yaml:\"limit,omitempty\"`" + `

	// Deny refuses matching requests.
	Deny []string ` + "`yaml:\"deny,omitempty\"`" + `
}`

	afterReference, err := ParsePolicyReference([]byte(changed), "ExamplePolicy")
	require.NoError(t, err)
	after := (&Generator{src: Sources{TaskPolicy: afterReference}}).renderTaskPolicyReference()

	require.NotEqual(t, before, after,
		"a new decoded field must stale the committed generated document")
	require.Contains(t, after, "`deny`")
}
