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

// taskMessages is where the built-in tasks declare their shapes. Every field
// under it is a field an author points at in an editor.
const taskMessages protoreflect.FullName = "flowstate.v1.Task"

// waitResultSymbols are the schema symbols LSP hover reads for the names a
// `wait_for_signal:`'s `outputs:` shaping binds.
//
// Named here rather than left to the walk below because they are not task
// fields: they are the pair a delivery carries, and hover quotes the schema's
// sentence about them. `timed_out`, the third name a shaping binds, is
// deliberately absent because the schema declares no symbol for it at all (the
// engine synthesizes it), which is why that one sentence is still written in Go.
var waitResultSymbols = []protoreflect.FullName{
	"flowstate.v1.SignalDelivery.payload",
	"flowstate.v1.SignalDelivery.sender",
}

// TestEverySymbolHoverReadsIsDocumented is the presence ratchet pointed at the
// surface slice 3 of #424 migrated: LSP hover no longer writes its own prose
// about a task's inputs and outputs, so a field with no leading comment is now a
// blank paragraph in somebody's editor rather than a gap in a schema nobody
// reads.
//
// It walks the task messages rather than naming fields, so a task gaining an
// input fails here until the input says what it is for. Synthetic map entry
// messages are skipped: `HeadersEntry.key` is generated by protoc, not written
// by anybody, and no editor ever shows one.
//
// The first sentence is asserted to stand alone for the reason
// TestEveryRPCFirstSentenceStandsAlone asserts it of an RPC: completion shows one
// line, so a comment opening with half a thought documents the field and leaves
// the surface saying nothing usable. It is not required to open with the field's
// own name, because the surfaces here print the name themselves, immediately
// above the sentence.
func TestEverySymbolHoverReadsIsDocumented(t *testing.T) {
	reg, err := Files()
	if err != nil {
		t.Fatalf("Files: %v", err)
	}

	desc, err := reg.FindDescriptorByName(taskMessages)
	if err != nil {
		t.Fatalf("%s not found: %v", taskMessages, err)
	}
	task, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		t.Fatalf("%s is a %T, not a message", taskMessages, desc)
	}

	checked := 0
	check := func(d protoreflect.Descriptor) {
		checked++
		comment, documented := CommentOf(d)
		if !documented {
			t.Errorf("field %s has no leading comment; hover shows the schema's own prose for it now, so write one in the schema under proto/flowstate/v1/", d.FullName())

			return
		}
		if first := FirstSentence(comment); !strings.HasSuffix(first, ".") {
			t.Errorf("field %s: first sentence does not end in a period, so a one-line context shows %q; write a complete opening sentence", d.FullName(), first)
		}
	}

	var visit func(m protoreflect.MessageDescriptor)
	visit = func(m protoreflect.MessageDescriptor) {
		if m == nil || m.IsPlaceholder() || m.IsMapEntry() {
			return
		}
		for i := 0; i < m.Fields().Len(); i++ {
			check(m.Fields().Get(i))
		}
		for i := 0; i < m.Messages().Len(); i++ {
			visit(m.Messages().Get(i))
		}
	}
	// The task shapes only. Task's own fields (`name`, `inputs`) describe a step
	// in a spec rather than one task's parameters, and are covered by the walk in
	// TestSchemaProseIsPresent.
	for i := 0; i < task.Messages().Len(); i++ {
		visit(task.Messages().Get(i))
	}

	for _, name := range waitResultSymbols {
		d, err := reg.FindDescriptorByName(name)
		if err != nil {
			t.Errorf("%s not found: %v; hover quotes this symbol's sentence, so a rename here is a blank paragraph in an editor", name, err)

			continue
		}
		check(d)
	}

	// A walk that reached nothing satisfies every claim above it. The count is a
	// floor rather than a total, so adding an input does not edit this line.
	if checked < 20 {
		t.Errorf("walk checked only %d fields, which is fewer than the built-in tasks declare; the traversal is broken, not the schema", checked)
	}
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
			t.Errorf("%s %s has no leading comment; write one in the schema under proto/flowstate/v1/", kind, name)
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

// TestPluginProtocolProseIsPresent keeps the plugin boundary useful in generated
// API docs and Go docs. Unlike the workflow-service walk above, this checks the
// whole protocol file: plugin authors consume its manifests and task messages
// directly, not only the types reachable from one service.
func TestPluginProtocolProseIsPresent(t *testing.T) {
	reg, err := Files()
	if err != nil {
		t.Fatalf("Files: %v", err)
	}
	file, err := reg.FindFileByPath("flowstate/plugin/v1/plugin.proto")
	if err != nil {
		t.Fatalf("plugin protocol descriptor: %v", err)
	}

	checked := 0
	check := func(descriptor protoreflect.Descriptor) {
		checked++
		if _, ok := CommentOf(descriptor); !ok {
			t.Errorf("%T %s has no leading comment", descriptor, descriptor.FullName())
		}
	}
	checkEnum := func(enum protoreflect.EnumDescriptor) {
		check(enum)
		for i := 0; i < enum.Values().Len(); i++ {
			check(enum.Values().Get(i))
		}
	}
	var checkMessage func(protoreflect.MessageDescriptor)
	checkMessage = func(message protoreflect.MessageDescriptor) {
		if message.IsMapEntry() {
			return
		}
		check(message)
		for i := 0; i < message.Fields().Len(); i++ {
			check(message.Fields().Get(i))
		}
		for i := 0; i < message.Oneofs().Len(); i++ {
			if oneof := message.Oneofs().Get(i); !oneof.IsSynthetic() {
				check(oneof)
			}
		}
		for i := 0; i < message.Enums().Len(); i++ {
			checkEnum(message.Enums().Get(i))
		}
		for i := 0; i < message.Messages().Len(); i++ {
			checkMessage(message.Messages().Get(i))
		}
	}
	for i := 0; i < file.Messages().Len(); i++ {
		checkMessage(file.Messages().Get(i))
	}
	for i := 0; i < file.Enums().Len(); i++ {
		checkEnum(file.Enums().Get(i))
	}
	for i := 0; i < file.Services().Len(); i++ {
		service := file.Services().Get(i)
		check(service)
		for j := 0; j < service.Methods().Len(); j++ {
			check(service.Methods().Get(j))
		}
	}
	if checked < 70 {
		t.Errorf("walk checked only %d plugin protocol declarations; the traversal is incomplete", checked)
	}
}

// TestCatalogProseIsPresent keeps every catalog message and field useful to
// descriptor consumers, generated Go documentation, and editor hovers.
func TestCatalogProseIsPresent(t *testing.T) {
	reg, err := Files()
	if err != nil {
		t.Fatalf("Files: %v", err)
	}
	file, err := reg.FindFileByPath("flowstate/v1/catalog.proto")
	if err != nil {
		t.Fatalf("catalog descriptor: %v", err)
	}

	var missing []string
	messageCount := 0
	fieldCount := 0
	var checkMessage func(protoreflect.MessageDescriptor)
	checkMessage = func(message protoreflect.MessageDescriptor) {
		if message.IsMapEntry() {
			return
		}
		messageCount++
		if _, ok := CommentOf(message); !ok {
			missing = append(missing, string(message.FullName()))
		}
		for i := 0; i < message.Fields().Len(); i++ {
			field := message.Fields().Get(i)
			fieldCount++
			if _, ok := CommentOf(field); !ok {
				missing = append(missing, string(field.FullName()))
			}
		}
		for i := 0; i < message.Messages().Len(); i++ {
			checkMessage(message.Messages().Get(i))
		}
	}
	for i := 0; i < file.Messages().Len(); i++ {
		checkMessage(file.Messages().Get(i))
	}

	if messageCount != 6 || fieldCount != 43 {
		t.Errorf("catalog walk checked %d messages and %d fields; want 6 messages and 43 fields", messageCount, fieldCount)
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Errorf("catalog declarations missing leading comments: %s", strings.Join(missing, ", "))
	}
}
