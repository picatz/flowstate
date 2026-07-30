package flowfile_test

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Reachability, checked rather than remembered.
//
// Four capabilities here have now been complete, tested, and impossible to use:
// secrets that no workflow could reference, durable waiting no Flowfile could
// spell, a signal no terminal could send, and an empty signal payload the schema
// forbade. Each time the tests passed, because each half was green on its own — a
// schema field and an engine that honors it both look finished when nothing asks
// whether an author can join them up.
//
// So the question gets asked mechanically. The lists below come from the schema's
// descriptors and from the task registry, never from a literal written here: a
// capability added tomorrow fails this test tomorrow, rather than on the day
// somebody remembers this file exists. That is the whole point — a checklist that
// has to be updated by hand is the failure mode, not the fix.
//
// An example is the right evidence because examples run in CI. `flow validate`
// accepts them on every push, so a capability with an example is one that stays
// reachable, not one that was reachable once.

// TestEveryNodeKindAppearsInAnExample checks the step kinds an author can write.
func TestEveryNodeKindAppearsInAnExample(t *testing.T) {
	t.Parallel()

	kinds := oneofFields(t, (&v1.Node{}).ProtoReflect().Descriptor(), "kind")

	used := map[string]bool{}
	forEachExampleNode(t, func(_ string, node *v1.Node) {
		if name, ok := whichOneof(node, "kind"); ok {
			used[name] = true
		}
	})

	requireAllUsed(t, "node kind", kinds, used,
		"add an example whose step uses it, or the kind is something no author can reach")
}

// TestEveryWaitKindAppearsInAnExample checks the ways a workload can wait.
//
// This is the one that was wrong when it was written: `sleep:` and
// `wait_for_signal:` had an example and `wait_until:` had none, so a timestamp
// wait was authorable in principle and undemonstrated in fact.
func TestEveryWaitKindAppearsInAnExample(t *testing.T) {
	t.Parallel()

	kinds := oneofFields(t, (&v1.Wait{}).ProtoReflect().Descriptor(), "kind")

	used := map[string]bool{}
	forEachExampleNode(t, func(_ string, node *v1.Node) {
		wait := node.GetWait()
		if wait == nil {
			return
		}
		if name, ok := whichOneof(wait, "kind"); ok {
			used[name] = true
		}
	})

	requireAllUsed(t, "wait kind", kinds, used,
		"add an example whose step uses it: sleep is `sleep:`, until is `wait_until:`, signal is `wait_for_signal:`")
}

// TestEveryRegisteredTaskAppearsInAnExample checks the tasks a step can name.
//
// The registry is what the engine will actually execute, so a task in it with no
// example is a task whose authoring surface nobody has tried.
func TestEveryRegisteredTaskAppearsInAnExample(t *testing.T) {
	t.Parallel()

	registered := map[string]bool{}
	for _, def := range v1.DefaultRegistry().All() {
		registered[def.Name] = true
	}
	if len(registered) == 0 {
		t.Fatal("the task registry is empty; this test is checking nothing")
	}

	used := map[string]bool{}
	forEachExampleNode(t, func(_ string, node *v1.Node) {
		if name := node.GetTask().GetName(); name != "" {
			used[name] = true
		}
	})

	requireAllUsed(t, "registered task", registered, used,
		"add an example whose step names it, or drop it from the registry")
}

// requireAllUsed reports every capability that no example reached.
//
// It names what is missing and what to do about it, because a bare count tells
// whoever added a field that something is wrong and not what.
func requireAllUsed(t *testing.T, subject string, want, got map[string]bool, remedy string) {
	t.Helper()

	var missing []string
	for name := range want {
		if !got[name] {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)

	for _, name := range missing {
		t.Errorf("no example uses the %s %q\n  %s\n  examples run in CI, which is what keeps a capability reachable rather than merely implemented",
			subject, name, remedy)
	}
}

// oneofFields lists the field names of a oneof, so a new variant is picked up
// without this test being edited.
func oneofFields(t *testing.T, desc protoreflect.MessageDescriptor, name string) map[string]bool {
	t.Helper()

	oneof := desc.Oneofs().ByName(protoreflect.Name(name))
	if oneof == nil {
		t.Fatalf("%s has no oneof %q; the schema moved and this test did not", desc.FullName(), name)
	}

	fields := make(map[string]bool, oneof.Fields().Len())
	for i := range oneof.Fields().Len() {
		fields[string(oneof.Fields().Get(i).Name())] = true
	}
	if len(fields) == 0 {
		t.Fatalf("oneof %s.%s has no fields", desc.FullName(), name)
	}
	return fields
}

// whichOneof reports which variant of a oneof is set.
func whichOneof(m proto.Message, name string) (string, bool) {
	refl := m.ProtoReflect()

	oneof := refl.Descriptor().Oneofs().ByName(protoreflect.Name(name))
	if oneof == nil {
		return "", false
	}

	field := refl.WhichOneof(oneof)
	if field == nil {
		return "", false
	}
	return string(field.Name()), true
}

// forEachExampleNode parses every shipped example and visits every step in it,
// including the steps nested inside loops and branches.
func forEachExampleNode(t *testing.T, visit func(example string, node *v1.Node)) {
	t.Helper()

	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "..", "examples", "*", "workflow.yaml"))
	if err != nil {
		t.Fatalf("finding examples: %v", err)
	}
	if len(paths) == 0 {
		t.Fatal("no examples found; the glob is wrong")
	}

	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}

		workflow, err := flowfile.Unmarshal(data)
		if err != nil {
			t.Fatalf("%s does not compile: %v", path, err)
		}

		name := filepath.Base(filepath.Dir(path))
		visitNodes(workflow, func(node *v1.Node) { visit(name, node) })
	}
}

// visitNodes calls fn for every [v1.Node] anywhere inside a message.
//
// It walks by reflection rather than by naming the fields that hold steps, so a
// new container — a sub-workflow, say — is covered the day it is added instead of
// the day someone remembers to come back here. Naming the fields is how a
// coverage check quietly stops covering things.
func visitNodes(m proto.Message, fn func(*v1.Node)) {
	if node, ok := m.(*v1.Node); ok {
		fn(node)
	}

	m.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case fd.IsMap():
			if fd.MapValue().Kind() != protoreflect.MessageKind {
				return true
			}
			value.Map().Range(func(_ protoreflect.MapKey, v protoreflect.Value) bool {
				visitNodes(v.Message().Interface(), fn)
				return true
			})

		case fd.IsList():
			if fd.Kind() != protoreflect.MessageKind {
				return true
			}
			list := value.List()
			for i := range list.Len() {
				visitNodes(list.Get(i).Message().Interface(), fn)
			}

		case fd.Kind() == protoreflect.MessageKind:
			visitNodes(value.Message().Interface(), fn)
		}

		return true
	})
}

// `now` resolves in a wait and nowhere else, and the diagnostics have to say so.
//
// A name that works in one field and not another is the kind of thing an author
// hits once and remembers wrongly, so the message for the wrong placement names
// the right one rather than reporting an unknown step.

// TestNowResolvesInAWaitUntil checks the placement that works.
func TestNowResolvesInAWaitUntil(t *testing.T) {
	t.Parallel()

	workflow, err := flowfile.Unmarshal([]byte(
		"edition: v2026.2\nname: t\nsteps:\n  - id: hold\n    wait_until: ${now + days(1)}\n"))
	require.NoError(t, err)

	require.Empty(t, flowfile.Validate(workflow),
		"`now` was reported as unresolved inside the one field that binds it")
}

// TestNowInATaskInputSaysWhereItIsAvailable checks the placement that does not.
func TestNowInATaskInputSaysWhereItIsAvailable(t *testing.T) {
	t.Parallel()

	workflow, err := flowfile.Unmarshal([]byte(
		"edition: v2026.2\nname: t\nsteps:\n  - id: report\n    log:\n      message: ${now}\n"))
	require.NoError(t, err)

	diagnostics := flowfile.Validate(workflow)
	require.NotEmpty(t, diagnostics, "`now` resolved in a task input, where nothing binds it")

	message := diagnostics.Error()
	require.Contains(t, message, "wait_until",
		"the diagnostic does not say where `now` is available")
	require.NotContains(t, message, "unknown step",
		"`now` was reported as a missing step, which sends the author looking for one they never wrote")
}

// TestAStepNamedNowNoLongerNeedsRefusing is what that rule turned into.
//
// It used to be refused, because a bound name won over a step's outputs and a
// step called `now` would have worked everywhere except inside a `wait_until:`.
// Rooting removes the possibility rather than the permission: the step is
// `steps.now` and the clock is `now`, so neither can be written where the other
// was meant.
//
// Asserted as acceptance *plus* both names resolving in one file, because
// dropping the refusal on its own would also pass if the two had quietly become
// the same thing.
func TestAStepNamedNowNoLongerNeedsRefusing(t *testing.T) {
	t.Parallel()

	source := "edition: v2026.2\nname: t\nsteps:\n" +
		"  - id: now\n    http:\n      url: https://example.com\n" +
		"  - id: hold\n    wait_until: ${now + days(1)}\n" +
		"  - id: read\n    log:\n      message: ${steps.now.body}\n"

	workflow, err := flowfile.Unmarshal([]byte(source))
	require.NoError(t, err)
	require.Empty(t, flowfile.Validate(workflow),
		"a step called `now` beside a wait that uses the clock must be accepted")
}
