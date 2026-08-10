package protodoc

import (
	"sort"
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// The service every public surface is derived from. Walking it is what makes
// this a pin on the schema rather than on this package.
const workflowService protoreflect.FullName = "flowstate.v1.WorkflowService"

// undocumented lists the schema names that carry no leading comment today.
//
// THIS LIST ONLY SHRINKS. It exists so the pin below can land green on the tree
// as it is rather than waiting for a prose sweep, and every entry is a promise
// to write a sentence, not a permission not to. Adding a name to it is adding an
// undocumented symbol to a public schema, which is the thing the pin exists to
// prevent; the review question for any diff that grows this list is why the
// symbol is being shipped without a description at all.
//
// It cannot go stale, because the pin fails both ways. A name here that gains a
// comment fails until it is deleted from the list, and a name here that no
// longer exists fails until it is deleted too, so a rename cannot leave a
// forgotten exemption standing ready to excuse the next symbol that inherits the
// name.
// It is empty, and keeping it that way is the point. Slice 2 of #424 emptied it:
// Run and Get gained the prose that had been living in cmd/flow's
// mcpDescriptions map, and the four empty responses and the Task wrapper gained
// the sentences their exemptions were promising. An entry appearing here again
// is a symbol somebody decided to ship undocumented.
var undocumented = []protoreflect.FullName{}

// TestEveryRPCFirstSentenceStandsAlone pins the one line, not just the presence
// of prose.
//
// Presence is not enough for the surfaces that read these comments. A tool list,
// a completion item and a table cell all show [FirstSentence] and nothing else,
// so an RPC whose comment opens with half a thought is documented and still
// useless in the place most readers meet it. What is asserted is what a reader
// gets: a sentence that names the RPC it describes, and that ends.
//
// Naming the RPC is the part that fails silently otherwise. "Returns a page of
// runs" is a fine sentence and a bad first one, because the contexts that show it
// show it beside a name the reader is trying to choose between.
func TestEveryRPCFirstSentenceStandsAlone(t *testing.T) {
	for _, m := range workflowServiceMethods(t) {
		comment, ok := CommentOf(m)
		if !ok {
			// TestSchemaProseIsPresent reports this; nothing to add here.
			continue
		}

		first := FirstSentence(comment)
		if !strings.HasSuffix(first, ".") {
			t.Errorf("rpc %s: first sentence does not end in a period, so a one-line context shows %q; write a complete opening sentence", m.FullName(), first)
		}
		if !strings.HasPrefix(first, string(m.Name())+" ") {
			t.Errorf("rpc %s: first sentence is %q, which does not open with the RPC's own name; a tool list shows this line beside the name a reader is choosing by", m.FullName(), first)
		}
	}
}

// workflowServiceMethods reads the service's RPCs from the embedded descriptors.
func workflowServiceMethods(t *testing.T) []protoreflect.MethodDescriptor {
	t.Helper()

	reg, err := Files()
	if err != nil {
		t.Fatalf("Files: %v", err)
	}
	desc, err := reg.FindDescriptorByName(workflowService)
	if err != nil {
		t.Fatalf("%s not found: %v", workflowService, err)
	}
	svc, ok := desc.(protoreflect.ServiceDescriptor)
	if !ok {
		t.Fatalf("%s is a %T, not a service", workflowService, desc)
	}

	var out []protoreflect.MethodDescriptor
	for i := 0; i < svc.Methods().Len(); i++ {
		out = append(out, svc.Methods().Get(i))
	}
	if len(out) == 0 {
		t.Fatalf("%s declares no methods; a walk over them proves nothing", workflowService)
	}

	return out
}

// TestSchemaProseIsPresent walks every RPC of WorkflowService and every
// top-level message reachable from those RPCs' request and response types, and
// fails on any that carries no leading comment.
//
// Presence enforced rather than hoped for. These descriptors are a public
// contract that plugin authors compile against and that agent-facing surfaces
// read directly, so a symbol with no description is not a blank line somebody
// eventually notices, it is a tool an agent is handed with nothing to say about
// what it does. The mechanism is the one that already keeps generated code
// honest: a check that fails, in the same list CI runs.
func TestSchemaProseIsPresent(t *testing.T) {
	reg, err := Files()
	if err != nil {
		t.Fatalf("Files: %v", err)
	}

	desc, err := reg.FindDescriptorByName(workflowService)
	if err != nil {
		t.Fatalf("%s not found: %v", workflowService, err)
	}
	svc, ok := desc.(protoreflect.ServiceDescriptor)
	if !ok {
		t.Fatalf("%s is a %T, not a service", workflowService, desc)
	}

	skip := map[protoreflect.FullName]bool{}
	for _, name := range undocumented {
		skip[name] = true
	}
	// Every exemption starts unused; anything still unused at the end is stale.
	used := map[protoreflect.FullName]bool{}

	check := func(d protoreflect.Descriptor, kind string) {
		name := d.FullName()
		_, documented := CommentOf(d)
		if skip[name] {
			used[name] = true
			if documented {
				t.Errorf("%s %s now has a leading comment; delete it from the undocumented list in this file (the list only shrinks)", kind, name)
			}
			return
		}
		if !documented {
			t.Errorf("%s %s has no leading comment; write one in proto/flowstate/v1/flowstate.proto", kind, name)
		}
	}

	seen := map[protoreflect.FullName]bool{}
	var messages []protoreflect.MessageDescriptor
	var visit func(m protoreflect.MessageDescriptor)
	visit = func(m protoreflect.MessageDescriptor) {
		// A placeholder is a type from an excluded import: it has a name and
		// nothing else, so there is no comment to demand and no fields to walk.
		if m == nil || m.IsPlaceholder() || seen[m.FullName()] {
			return
		}
		seen[m.FullName()] = true
		messages = append(messages, m)
		for i := 0; i < m.Fields().Len(); i++ {
			if f := m.Fields().Get(i).Message(); f != nil {
				visit(f)
			}
		}
	}

	methods := svc.Methods()
	if methods.Len() == 0 {
		t.Fatalf("%s declares no methods; the walk would pass by reaching nothing", workflowService)
	}
	for i := 0; i < methods.Len(); i++ {
		m := methods.Get(i)
		check(m, "rpc")
		visit(m.Input())
		visit(m.Output())
	}

	// Top-level only. A nested message is documented where its parent is, and a
	// map field's synthetic entry message is not a symbol anybody writes.
	reached := 0
	for _, m := range messages {
		if m.Parent() != m.ParentFile() {
			continue
		}
		reached++
		check(m, "message")
	}
	// Assert the walk actually got somewhere: a traversal that reaches nothing
	// satisfies every "no undocumented symbol" claim above it.
	if reached < 50 {
		t.Errorf("walk reached only %d top-level messages from %d RPCs, which is fewer than this schema has; the traversal is broken, not the schema", reached, methods.Len())
	}

	var stale []string
	for _, name := range undocumented {
		if !used[name] {
			stale = append(stale, string(name))
		}
	}
	sort.Strings(stale)
	for _, name := range stale {
		t.Errorf("%s is in the undocumented list but the walk never reached it; delete it (a renamed or removed symbol must not leave its exemption behind)", name)
	}
}
