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

// The required set is derived from the schema three ways: the two `kind` oneofs
// (a node kind, a wait kind), the task registry, and — the part #901's review
// (r3837028388) added — the *writable fields* of the messages an author fills
// in. A field-shaped capability (`if:`, `retry:`, `undo:`, `async:`, an
// `inputs:`/`outputs:`/`triggers:`/`signals:` block) is not in any oneof, so
// deriving from the oneofs alone let one land with no example while this test
// stayed green. The fields below are enumerated from the descriptors and every
// one becomes required *unless* it is named in an exclusion map — so a new
// writable field is required the day it is added, and skipping it is a decision
// somebody has to write down here rather than a gap the derivation cannot see.

// nodeFieldExclusions are the [v1.Node] fields that are not author-facing
// capabilities. The `kind` oneof is excluded structurally (its members are the
// node-kind constructs, covered above); these are the rest.
var nodeFieldExclusions = map[string]string{
	"id":          "every node has one; an identifier is not a capability",
	"description": "prose attached to a step, not a behavior",
	"policy":      "a container; its own fields (timeout, retry) are the constructs, required via StepPolicy",
}

// policyFieldExclusions are the [v1.StepPolicy] fields to skip. None: both
// `timeout:` and `retry:` are constructs an example must show.
var policyFieldExclusions = map[string]string{}

// workflowFieldExclusions are the [v1.Workflow] fields that are not author-set
// DSL constructs the portfolio is responsible for.
var workflowFieldExclusions = map[string]string{
	"name":                "every workflow has one; structural",
	"description":         "prose",
	"steps":               "every workflow has them; structural",
	"profile":             "the CEL profile selector, an advanced deployment concern rather than a portfolio construct",
	"labels":              "run-selection metadata added by #904; demonstrating it belongs to that feature, and it is not a workflow-behavior construct",
	"plugin_requirements": "a `plugins:` block is only expressible in examples/plugins/, which this corpus excludes because those files name plugin tasks; the plugin examples' own tests demonstrate it",
	"resolved_plugins":    "set by the control plane at submit, never written by an author",
}

// messageWritableSpec pairs a message full-name with how its writable fields are
// labelled and which to skip, so both the demonstrated-walk and the required-set
// derivation read one source.
type messageWritableSpec struct {
	prefix  string
	exclude map[string]string
}

func writableSpecFor(name protoreflect.FullName) (messageWritableSpec, bool) {
	switch name {
	case (&v1.Node{}).ProtoReflect().Descriptor().FullName():
		return messageWritableSpec{prefix: "node", exclude: nodeFieldExclusions}, true
	case (&v1.StepPolicy{}).ProtoReflect().Descriptor().FullName():
		return messageWritableSpec{prefix: "policy", exclude: policyFieldExclusions}, true
	case (&v1.Workflow{}).ProtoReflect().Descriptor().FullName():
		return messageWritableSpec{prefix: "workflow", exclude: workflowFieldExclusions}, true
	}

	return messageWritableSpec{}, false
}

// writableRequired adds every non-excluded, non-oneof field of one message to
// the required set, labelled by its spec's prefix.
func writableRequired(required map[string]string, desc protoreflect.MessageDescriptor) {
	spec, ok := writableSpecFor(desc.FullName())
	if !ok {
		return
	}
	fields := desc.Fields()
	for i := range fields.Len() {
		field := fields.Get(i)
		if field.ContainingOneof() != nil {
			continue
		}
		if _, excluded := spec.exclude[string(field.Name())]; excluded {
			continue
		}
		required[spec.prefix+"."+string(field.Name())] = "an example setting that field"
	}
}

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

		report := func(construct string) {
			if _, seen := demonstrated[construct]; !seen {
				demonstrated[construct] = name
			}
		}
		for _, node := range wf.GetSteps() {
			walkConstructs(node.ProtoReflect(), report)
		}
		// Workflow-level writable constructs — `inputs:`, `outputs:`,
		// `triggers:`, `signals:`, `vars:` — live on the Workflow message, which
		// the node walk never reaches, so they are reported here from the same
		// spec the required set is derived from.
		reportWritableFields(wf.ProtoReflect(), report)
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
	writableRequired(required, (&v1.Node{}).ProtoReflect().Descriptor())
	writableRequired(required, (&v1.StepPolicy{}).ProtoReflect().Descriptor())
	writableRequired(required, (&v1.Workflow{}).ProtoReflect().Descriptor())
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
// knows about: every message-valued field is followed, whatever message
// introduced it. A `switch:` case, a `parallel:` branch, a loop body, a
// compensation — all of them are reached without being named here, and a nesting
// invented tomorrow is reached for the same reason, with nothing here to update.
// Written the other way round — a switch over the kinds — this would be one more
// hand-kept list of the thing it is checking.
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

	// Every [v1.Task], wherever it sits — a step's own, and the one a
	// compensation carries. `undo:` holds a Task directly rather than a Node, so
	// reading the name off the *node* would miss a task demonstrated only as the
	// way something is taken back.
	if task, ok := msg.Interface().(*v1.Task); ok {
		report("task." + task.GetName())
	}

	// Writable constructs set on this message — `if:`, `retry:`, `undo:`,
	// `async:` and the rest — reported from the same spec the required set is
	// derived from, so the two cannot describe different fields.
	reportWritableFields(msg, report)

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

// reportWritableFields reports each set, non-excluded, non-oneof field of one
// message as a writable construct, when the message has a writable spec. It is
// the demonstrated-side twin of [writableRequired]: both read [writableSpecFor],
// so a field the required set asks for is reported by exactly the same rule that
// required it. A field is "set" by protoreflect's own rule — Range yields only
// populated fields — so a bool like `async` counts only when true, and an
// `inputs:` block only when it has an entry.
func reportWritableFields(msg protoreflect.Message, report func(string)) {
	spec, ok := writableSpecFor(msg.Descriptor().FullName())
	if !ok {
		return
	}

	msg.Range(func(field protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		if field.ContainingOneof() != nil {
			return true
		}
		if _, excluded := spec.exclude[string(field.Name())]; excluded {
			return true
		}
		report(spec.prefix + "." + string(field.Name()))

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

// TestWalkConstructsSeesNestedConstructs is the guard on the walker itself.
//
// A charter that reports full coverage because its walk stops at the top level
// is worse than no charter: it is a green tick over the exact gap it was written
// to find. So the walk is held to a workflow whose interesting constructs are
// all *inside* something else — a wait and a task nested two blocks deep, and a
// task reachable only as a compensation — and asked to name them.
func TestWalkConstructsSeesNestedConstructs(t *testing.T) {
	t.Parallel()

	wf, _, err := flowfile.Parse([]byte(`
edition: v2026.3
name: nested
steps:
  - id: outer
    switch:
      value: ${1}
      cases:
        - case: 1
          steps:
            - id: inner
              loop:
                until: ${steps.gate.timed_out}
                max_iterations: 2
                steps:
                  - id: gate
                    wait_for_signal:
                      name: something
                      timeout: 1s
                  - id: work
                    log:
                      message: nested
                    undo:
                      log:
                        message: taken back
`))
	require.NoError(t, err)

	seen := map[string]bool{}
	for _, node := range wf.GetSteps() {
		walkConstructs(node.ProtoReflect(), func(construct string) { seen[construct] = true })
	}

	for _, construct := range []string{"node.switch", "node.loop", "node.wait", "wait.signal", "node.task", "task.log"} {
		assert.True(t, seen[construct],
			"the walk did not reach %s in a workflow that has one, so the charter would report coverage it never checked", construct)
	}
}

// TestCharterRequiresWritableConstructs pins the writable-field derivation to
// the specific constructs #901's review (r3837028388) said were slipping
// through. A derivation that regressed to empty — a renamed exclusion map, a
// spec lookup that stopped matching — would leave the charter green while
// checking none of these, so the strengthening is asserted rather than trusted.
func TestCharterRequiresWritableConstructs(t *testing.T) {
	t.Parallel()

	required := map[string]string{}
	writableRequired(required, (&v1.Node{}).ProtoReflect().Descriptor())
	writableRequired(required, (&v1.StepPolicy{}).ProtoReflect().Descriptor())
	writableRequired(required, (&v1.Workflow{}).ProtoReflect().Descriptor())

	for _, construct := range []string{
		"node.condition", "node.async", "node.undo", "node.vars",
		"policy.timeout", "policy.retry",
		"workflow.declared_inputs", "workflow.declared_outputs",
		"workflow.triggers", "workflow.signals", "workflow.vars",
	} {
		_, ok := required[construct]
		assert.True(t, ok,
			"%s is a writable construct the charter must require, but the derivation did not produce it", construct)
	}

	// The kind-oneof members must NOT appear as writable node fields — they are
	// covered by the oneof pass, and double-counting them here would be a
	// different construct name for the same thing.
	for _, construct := range []string{"node.task", "node.loop", "node.wait"} {
		_, ok := required[construct]
		assert.False(t, ok, "%s is a kind-oneof member and must not be derived as a writable field", construct)
	}
}

// TestExampleCorpusGlobsMatchTheTree is a guard on the globs above: a rename that
// made them match nothing would otherwise make the charter pass by checking
// nothing.
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
