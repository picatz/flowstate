package flowstatev1_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
)

// TestRunIdentityShapeLocal checks the local half of #206's second gap: a local
// run — `flow run local`, `flow test` — has no authenticated caller at all, and
// `run.identity` must say so honestly rather than merely leaving every field
// empty in a way an attested-but-anonymous caller would also produce. The
// durable half of this shared assertion is [TestRunIdentityShapeDurable] in
// engine/run_identity_test.go.
func TestRunIdentityShapeLocal(t *testing.T) {
	outputs, err := v1.Run(context.Background(), tests.RunIdentityWorkflow())
	require.NoError(t, err)

	tests.AssertRunIdentityShape(t, outputs, true, "")
}

// TestRunIdentityContainmentShapes checks the opposite direction from
// CLAUDE.md's secret-containment tests: `run.identity.subject` is exactly the
// value examples/approval-gate/workflow.yaml documents as deliberately *not*
// `sensitive:`, because an approval trail has to stay legible on a terminal.
// So where a secret must never survive `%v`, `%+v`, `%#v` or `%s` intact, an
// attested identity must — on the value itself, on a struct holding it, and on
// a slice of those — or something in the rendering path is treating audit
// evidence as if it were a credential.
func TestRunIdentityContainmentShapes(t *testing.T) {
	identity := &v1.WorkloadIdentity{
		Subject:   "release-requester@example.com",
		Issuer:    "flowstate:test",
		Namespace: "team-a",
	}
	scope := &v1.Scope{Identity: identity}

	// %#v is checked only on the value itself, not nested inside the scope or the
	// slice: Go's `%#v` does not recurse through a *pointer* field the way `%v` and
	// `%+v` do (it renders the address instead), for any struct with one, secret
	// or not — that is a property of the verb, not something specific to identity
	// containment, so holding it to the same bar there would be asserting a fact
	// about `fmt` rather than about this value.
	renderings := map[string]string{
		"%v on the identity":  fmt.Sprintf("%v", identity),
		"%+v on the identity": fmt.Sprintf("%+v", identity),
		"%#v on the identity": fmt.Sprintf("%#v", identity),
		"%v on the scope":     fmt.Sprintf("%v", scope),
		"%+v on the scope":    fmt.Sprintf("%+v", scope),
		"%v on a slice":       fmt.Sprintf("%v", []*v1.Scope{scope}),
		"%+v on a slice":      fmt.Sprintf("%+v", []*v1.Scope{scope}),
	}
	//lint:ignore S1025 the %s verb is one of the containment shapes under test, not a roundabout String()
	renderings["%s on the identity"] = fmt.Sprintf("%s", identity)

	for label, rendered := range renderings {
		if !strings.Contains(rendered, identity.GetSubject()) {
			t.Errorf("%s does not show the attested subject (%q); an approval trail must stay "+
				"legible, and this rendering hid it as if it were a secret: %s",
				label, identity.GetSubject(), rendered)
		}
	}
}
