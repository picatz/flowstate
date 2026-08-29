package flowdebug

import (
	"strings"
	"testing"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TestEveryComprehensionBindingIsAccountedFor (Codex, #1116).
//
// `comprehensionBindings` is a hand-written list of the names a comprehension
// binds, and the last three findings on this change were all one shape: a
// hand-written enumeration over somebody else's AST, missing a member. The
// third of them was `iter_var2`, which two-variable macros bind and this walk
// did not — so a macro-local name looked like one the step had to provide, and
// a true condition never fired.
//
// The set is closed, so this asserts it against the schema rather than
// trusting the list: every string field on the Comprehension message is either
// named here as a binding or named below as deliberately not one. A cel-go
// release that adds a fourth fails this test instead of quietly making
// conditions unaskable — the mold `NodeRecursionEdges` uses for the same
// problem one package over.
func TestEveryComprehensionBindingIsAccountedFor(t *testing.T) {
	t.Parallel()

	// The string fields that name a binding, in the order the accessors are
	// declared. Anything else on the message is a sub-expression or metadata.
	binding := map[string]bool{
		"iter_var":  true,
		"iter_var2": true,
		"accu_var":  true,
	}

	fields := (&expr.Expr_Comprehension{}).ProtoReflect().Descriptor().Fields()

	var unaccounted []string
	for i := range fields.Len() {
		field := fields.Get(i)
		if field.Kind() != protoreflect.StringKind {
			// A sub-expression or a message: walked, not bound.
			continue
		}
		if !binding[string(field.Name())] {
			unaccounted = append(unaccounted, string(field.Name()))
		}
	}

	if len(unaccounted) > 0 {
		t.Fatalf("Comprehension has string field(s) %s that comprehensionBindings does not name; "+
			"if they bind a name for the body, add them there, and if they do not, add them to this test's list — "+
			"a binding this walk does not know about makes a macro-local name look like one the step must provide",
			strings.Join(unaccounted, ", "))
	}

	// And the list is not merely a superset of the schema: each name it claims
	// has to exist, or a rename would leave it silently binding nothing.
	for name := range binding {
		if fields.ByName(protoreflect.Name(name)) == nil {
			t.Errorf("comprehensionBindings names %q, which Comprehension no longer has", name)
		}
	}
}
