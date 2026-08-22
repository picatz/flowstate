package flowstatev1_test

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The examples charter (#165) says the portfolio is the product demo, and the
// acceptance shape it asks for is a coverage claim: every construct the language
// has, demonstrated by a file somebody can read and run.
//
// A prose list of what the portfolio covers is the failure CLAUDE.md legislates
// against directly — the same facts written down twice, in the venue where the
// copy cannot be checked. So the charter is spelled as this test instead, and
// both halves of it are *derived*: what must be covered comes from the schema's
// own `Node.kind` and `Wait.kind` oneofs and from the task registry, and what is
// covered comes from parsing the corpus. Adding a node kind, a wait kind, or a
// task therefore fails here on the day it lands, naming the thing with no
// example, rather than on the day a reader notices the portfolio never shows it.
//
// What this cannot check is the part that actually matters — whether the example
// is any good, whether a reader recognizes their own problem in it. That stays a
// review question, exactly as [TestEveryExampleHasATestFile] says of its own
// floor. This checks that the floor exists.

// exampleCorpusGlobs are the Flowfiles this charter reads: one per example
// directory, plus the callees a `call:` example ships beside it.
//
// Deliberately not the whole tree. Everything under `examples/plugins/` names a
// task the built-in registry does not have — that is the point of those files,
// and examples/README.md explains the placement — so they cannot be compiled by
// this process at all, and a construct demonstrated *only* there is a construct
// with no example a stock `flow` can run. Anything under `examples/embedding/`
// is out for the same reason: its task is registered by a Go program.
var exampleCorpusGlobs = []string{
	filepath.Join("..", "..", "..", "examples", "*", "workflow.yaml"),
	filepath.Join("..", "..", "..", "examples", "*", "workflows", "*.yaml"),
}

// constructsWithoutAnExample is the allowlist this test reads: language
// constructs the corpus is permitted not to demonstrate.
//
// Like [examplesWithoutTestFile] it is a list of decisions, never of gaps: an
// entry has to say why no Flowfile in the corpus can show the thing off, and
// "nobody has written one yet" is not such a reason — it is the finding this
// test exists to report.
var constructsWithoutAnExample = map[string]string{}

// TestEveryLanguageConstructHasAnExample is the examples charter, executable.
func TestEveryLanguageConstructHasAnExample(t *testing.T) {
	t.Parallel()

	demonstrated := map[string]string{} // construct -> the first example that shows it

	var paths []string
	for _, glob := range exampleCorpusGlobs {
		matched, err := filepath.Glob(glob)
		require.NoError(t, err)
		paths = append(paths, matched...)
	}
	require.NotEmpty(t, paths, "no examples found; the globs are wrong")

	for _, path := range paths {
		name := filepath.Base(filepath.Dir(path))
		if filepath.Base(filepath.Dir(path)) == "workflows" {
			name = filepath.Base(filepath.Dir(filepath.Dir(path)))
		}

		wf, _, err := flowfile.ParseFile(path)
		require.NoError(t, err, "%s does not compile", path)

		for _, node := range wf.GetSteps() {
			walkConstructs(node.ProtoReflect(), func(construct string) {
				if _, seen := demonstrated[construct]; !seen {
					demonstrated[construct] = name
				}
			})
		}
	}

	// What has to be covered, asked of the schema and the registry rather than
	// remembered. A construct added to either appears here the moment it exists.
	required := map[string]string{} // construct -> what an author writes to reach it
	for _, field := range oneofFields((&v1.Node{}).ProtoReflect().Descriptor(), "kind") {
		required["node."+string(field.Name())] = "a step of that kind"
	}
	for _, field := range oneofFields((&v1.Wait{}).ProtoReflect().Descriptor(), "kind") {
		required["wait."+string(field.Name())] = "a wait of that kind"
	}
	for _, task := range v1.DefaultRegistry().Names() {
		required["task."+task] = "a step naming that task"
	}
	require.NotEmpty(t, required)

	missing := make([]string, 0, len(required))
	for construct := range required {
		if _, ok := demonstrated[construct]; ok {
			continue
		}
		if reason, allowed := constructsWithoutAnExample[construct]; allowed {
			assert.NotEmpty(t, reason,
				"%s is allowlisted with no reason; an entry must be a decision, not a gap", construct)

			continue
		}
		missing = append(missing, construct)
	}
	sort.Strings(missing)

	assert.Empty(t, missing,
		"the portfolio demonstrates every language construct except these; write an example that "+
			"uses each (%s), or add it to constructsWithoutAnExample with the reason no example can",
		missing)

	// An allowlist entry for a construct the corpus does demonstrate, or for one
	// that no longer exists, is a decision about nothing.
	for construct := range constructsWithoutAnExample {
		if _, ok := required[construct]; !ok {
			t.Errorf("%s is allowlisted but is not a construct the schema or registry has; remove the entry", construct)

			continue
		}
		if where, ok := demonstrated[construct]; ok {
			t.Errorf("%s is allowlisted as having no example, but examples/%s demonstrates it; remove the entry",
				construct, where)
		}
	}
}

// walkConstructs reports every construct one compiled node reaches, itself and
// everything nested inside it.
//
// The recursion is over the *message*, not over the node shapes this package
// knows about: any field holding a nested node is followed, whatever message
// introduced it. A `switch:` case, a `parallel:` branch, a loop body, a
// compensation — all of them are reached because they hold nodes, and a node
// nesting invented tomorrow is reached for the same reason, with nothing here to
// update. Written the other way round — a switch over the kinds — this would be
// one more hand-kept list of the thing it is checking.
func walkConstructs(msg protoreflect.Message, report func(string)) {
	desc := msg.Descriptor()

	if kind := desc.Oneofs().ByName("kind"); kind != nil {
		if set := msg.WhichOneof(kind); set != nil {
			switch desc.FullName() {
			case (&v1.Node{}).ProtoReflect().Descriptor().FullName():
				report("node." + string(set.Name()))
			case (&v1.Wait{}).ProtoReflect().Descriptor().FullName():
				report("wait." + string(set.Name()))
			}
		}
	}

	if node, ok := msg.Interface().(*v1.Node); ok && node.GetTask() != nil {
		report("task." + node.GetTask().GetName())
	}

	msg.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.Kind() != protoreflect.MessageKind && field.Kind() != protoreflect.GroupKind {
			return true
		}

		switch {
		case field.IsList():
			list := value.List()
			for i := range list.Len() {
				walkConstructs(list.Get(i).Message(), report)
			}
		case field.IsMap():
			value.Map().Range(func(_ protoreflect.MapKey, entry protoreflect.Value) bool {
				if field.MapValue().Kind() == protoreflect.MessageKind {
					walkConstructs(entry.Message(), report)
				}

				return true
			})
		default:
			walkConstructs(value.Message(), report)
		}

		return true
	})
}

// oneofFields lists the fields of one named oneof, in declaration order.
func oneofFields(desc protoreflect.MessageDescriptor, name protoreflect.Name) []protoreflect.FieldDescriptor {
	oneof := desc.Oneofs().ByName(name)
	if oneof == nil {
		return nil
	}

	fields := make([]protoreflect.FieldDescriptor, 0, oneof.Fields().Len())
	for i := range oneof.Fields().Len() {
		fields = append(fields, oneof.Fields().Get(i))
	}

	return fields
}

// exampleCorpusIsReadable is a guard on the globs above: a rename that made them
// match nothing would otherwise make the charter pass by checking nothing.
func TestExampleCorpusGlobsMatchTheTree(t *testing.T) {
	t.Parallel()

	for _, glob := range exampleCorpusGlobs {
		matched, err := filepath.Glob(glob)
		require.NoError(t, err)
		assert.NotEmpty(t, matched, "%s matches nothing; the corpus moved", glob)

		for _, path := range matched {
			_, err := os.Stat(path)
			require.NoError(t, err)
		}
	}
}
