package flowstatev1_test

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

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
// Like [examplesWithoutTestFile] it is a list of decisions, never of silence. An
// entry is one of exactly two things, and it has to say which:
//
//   - a construct no Flowfile in this corpus *can* demonstrate, with the reason;
//   - a real gap somebody has written down and tracked, naming the issue.
//
// The second kind is what this list gained when the derivation started walking
// the whole workflow message graph rather than three hand-picked messages: the
// seven below are constructs the language has and the portfolio has never shown,
// which is precisely the finding this test exists to produce. They are recorded
// rather than papered over, and filling them is portfolio work with its own
// issue — not something to jam into an unrelated example so that a list goes
// quiet.
var constructsWithoutAnExample = map[string]string{
	"input.values":                 "an enum-constrained input (`values:`); no example declares one — tracked in #969",
	"manual.denied":                "the deny half of a `manual:` trigger's principal rules; `trigger-context` shows `allowed_principals` only — tracked in #969",
	"schedule.every":               "the interval spelling of a schedule; `scheduled-report` uses `cron:` — tracked in #969",
	"schedule.start_at":            "a schedule's activation window start — tracked in #969",
	"schedule.end_at":              "a schedule's activation window end — tracked in #969",
	"signal_policy_rule.subject":   "a signals rule matching an exact subject; the enterprise examples gate on `claims:` — tracked in #969",
	"signal_policy_rule.namespace": "a signals rule matching a namespace — tracked in #969",

	// Per-*value* enum coverage, added because a key per field stays satisfied by
	// whichever value some example happened to use, so a new policy would land
	// unnoticed. These are the values nothing selects today.
	//
	// `input.type.TYPE_ENUM` is the same gap as `input.values` above, seen from
	// the other side: an enum-constrained input needs the type and the values
	// together, and one example closes both.
	"input.type.TYPE_ENUM":                     "an enum-constrained input; the same gap as input.values — tracked in #969",
	"schedule.overlap.OVERLAP_BUFFER_ONE":      "an overlap policy no example selects; scheduled-report shows one policy — tracked in #969",
	"schedule.overlap.OVERLAP_BUFFER_ALL":      "the same — tracked in #969",
	"schedule.overlap.OVERLAP_CANCEL_OTHER":    "the same — tracked in #969",
	"schedule.overlap.OVERLAP_TERMINATE_OTHER": "the same — tracked in #969",
	"schedule.overlap.OVERLAP_ALLOW_ALL":       "the same — tracked in #969",
}

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

// blockFieldExclusions are the fields of the block-node messages that are
// structural rather than capabilities an example demonstrates. A block's own
// `steps:` is the block, not a construct inside it, and the node-kind construct
// covering it is already required by the oneof pass.
var blockFieldExclusions = map[string]string{
	"steps":    "the body of a block node; the block itself is the construct, required through the kind oneof",
	"branches": "the same, for `parallel:`",
	"cases":    "the same, for `switch:`; an arm's own coverage is measured per-arm by `flow test`",
}

// writableSpecs are the messages whose author-written fields the charter
// requires an example for, keyed by message name.
//
// Everything an author fills in that is *not* selected by a oneof lives here.
// Codex's review of #901 (r3837028388) asked for exactly this generalization and
// the follow-up asked it to reach past the first three: a construct like a
// loop's `until:`, a `for_each`'s `max_parallel:`, or a retry's `max_interval:`
// is as author-facing as `if:` is, and deriving from `Node`, `StepPolicy` and
// `Workflow` alone left every one of them outside the check.
//
// The rule for adding one: a message belongs here when a Flowfile author types
// its fields. A message the *engine* fills in (a run's outputs, a resolved
// plugin) does not, and neither does a pure container whose leaves are already
// covered — `Compensation` holds one `task`, and the task is what an example
// shows.
func writableSpecs() map[protoreflect.FullName]messageWritableSpec {
	spec := func(m proto.Message, prefix string, exclude map[string]string) (protoreflect.FullName, messageWritableSpec) {
		return m.ProtoReflect().Descriptor().FullName(), messageWritableSpec{prefix: prefix, exclude: exclude}
	}

	specs := map[protoreflect.FullName]messageWritableSpec{}
	for _, entry := range []struct {
		msg     proto.Message
		prefix  string
		exclude map[string]string
	}{
		{&v1.Node{}, "node", nodeFieldExclusions},
		{&v1.StepPolicy{}, "policy", policyFieldExclusions},
		{&v1.Workflow{}, "workflow", workflowFieldExclusions},
		{&v1.Wait{}, "wait", nil},
		{&v1.ForEach{}, "for_each", blockFieldExclusions},
		{&v1.Loop{}, "loop", blockFieldExclusions},
		{&v1.Parallel{}, "parallel", blockFieldExclusions},
		{&v1.Switch{}, "switch", blockFieldExclusions},
		{&v1.Call{}, "call", nil},
		{&v1.RetryPolicy{}, "retry", nil},

		// The author-facing messages the graph walk found sitting outside the
		// hand-kept list — the finding that motivated deriving the message set
		// rather than listing it.
		{&v1.InputDeclaration{}, "input", nil},
		{&v1.OutputDeclaration{}, "output", nil},
		{&v1.Triggers{}, "triggers", nil},
		{&v1.WebhookTrigger{}, "webhook", nil},
		{&v1.ManualTrigger{}, "manual", nil},
		{&v1.ScheduleTrigger{}, "schedule", nil},
		{&v1.SignalPolicy{}, "signal_policy", nil},
		{&v1.SignalPolicyRule{}, "signal_policy_rule", nil},
		{&v1.Signal{}, "signal", nil},
		{&v1.Switch_Case{}, "switch_case", blockFieldExclusions},
		{&v1.Switch_Default{}, "switch_default", blockFieldExclusions},
	} {
		name, s := spec(entry.msg, entry.prefix, entry.exclude)
		specs[name] = s
	}

	return specs
}

func writableSpecFor(name protoreflect.FullName) (messageWritableSpec, bool) {
	spec, ok := writableSpecs()[name]

	return spec, ok
}

// messagesOutsideTheCharter are the messages reachable from [v1.Workflow] whose
// fields no example is responsible for, each against the reason why.
//
// It is the other half of [writableSpecs], and the two together have to account
// for *every* message the walk below reaches — an unclassified one fails
// [TestCharterClassifiesEveryReachableMessage] rather than being skipped, which
// is what stops a new author-facing message from landing outside the check. That
// inversion is the point: a list of what to include silently ignores what it has
// not heard of, and a list of what to exclude cannot.
var messagesOutsideTheCharter = map[protoreflect.FullName]string{

	"flowstate.v1.Value":             "the universal value wrapper: every expression and literal in the language is one, so its own fields are the encoding rather than a construct an example demonstrates",
	"flowstate.v1.Task":              "a task's identity is its name, and the charter requires an example per registered task through the registry pass; its `inputs` map is the task's own schema rather than a language construct",
	"flowstate.v1.Compensation":      "a container holding one task; `node.undo` is the construct and the task inside it is required through the registry pass",
	"flowstate.v1.ResolvedPlugin":    "written by the control plane at submit, never by an author",
	"flowstate.v1.PluginRequirement": "a `plugins:` block is only expressible under examples/plugins/, which this corpus excludes because those files name tasks a stock `flow` cannot resolve",

	// Encoding rather than language: the Value wrapper's own internals.
	"flowstate.v1.Value.Structure":      "part of the value encoding",
	"flowstate.v1.Value.Structure.Map":  "part of the value encoding",
	"flowstate.v1.Value.Structure.List": "part of the value encoding",
	"flowstate.v1.Value.Error":          "how a failed evaluation is carried, produced by the engine rather than typed by an author",

	// A secret is written as a secret() call inside an expression, so what an
	// example demonstrates is the call, not this message's fields.
	"flowstate.v1.SecretRef": "the author-facing spelling is the secret() call inside an expression, not this message",

	// A parallel branch holds only a body, and the block itself is already
	// required through the kind oneof.
	"flowstate.v1.Parallel.Branch": "a branch is its body; node.parallel is the construct",

	// The calendar sub-messages are the cron grammar inside a schedule trigger
	// rather than separate constructs; scheduled-report shows the whole trigger.
	"flowstate.v1.ScheduleTrigger.Calendar":       "the cron grammar inside a schedule trigger, demonstrated as part of it",
	"flowstate.v1.ScheduleTrigger.Calendar.Range": "the same",
}

// reachableFromWorkflow walks the descriptor graph from [v1.Workflow] and
// returns every message it can reach through a field, itself included.
//
// Derived rather than listed, which is the whole of Codex's follow-up finding on
// #901: a hand-kept table of messages goes stale exactly as a hand-kept table of
// fields did, and the messages an author types are precisely the ones a
// `Workflow` can hold.
func reachableFromWorkflow() map[protoreflect.FullName]protoreflect.MessageDescriptor {
	seen := map[protoreflect.FullName]protoreflect.MessageDescriptor{}

	var walk func(protoreflect.MessageDescriptor)
	walk = func(desc protoreflect.MessageDescriptor) {
		if _, ok := seen[desc.FullName()]; ok {
			return
		}
		seen[desc.FullName()] = desc

		fields := desc.Fields()
		for i := range fields.Len() {
			field := fields.Get(i)
			if field.IsMap() {
				if field.MapValue().Kind() == protoreflect.MessageKind {
					walk(field.MapValue().Message())
				}

				continue
			}
			if msg := field.Message(); msg != nil {
				walk(msg)
			}
		}
	}
	walk((&v1.Workflow{}).ProtoReflect().Descriptor())

	return seen
}

// TestCharterClassifiesEveryReachableMessage is the derivation's own guard: every
// message a workflow can hold is either one the charter requires examples for, or
// one written down as outside it, with a reason. Neither list may quietly not
// mention a message.
func TestCharterClassifiesEveryReachableMessage(t *testing.T) {
	t.Parallel()

	specs := writableSpecs()
	unclassified := []string{}

	for name := range reachableFromWorkflow() {
		// Anything outside this repository's own schema is not this language's
		// surface: the well-known types, and the CEL AST a compiled expression
		// carries. A rule rather than eighteen entries, so a CEL library bump
		// that adds a message does not need a line here.
		if !strings.HasPrefix(string(name), "flowstate.v1.") {
			continue
		}

		_, required := specs[name]
		reason, excluded := messagesOutsideTheCharter[name]
		switch {
		case required && excluded:
			t.Errorf("%s is both required and excluded; it can only be one", name)
		case excluded:
			assert.NotEmpty(t, reason,
				"%s is excluded with no reason; an entry must be a decision, not a gap", name)
		case !required:
			unclassified = append(unclassified, string(name))
		}
	}
	sort.Strings(unclassified)

	assert.Empty(t, unclassified,
		"these messages are reachable from a Workflow and the charter says nothing about them (%s); "+
			"either add one to writableSpecs so its fields need an example, or to "+
			"messagesOutsideTheCharter with the reason no example is responsible for it",
		unclassified)

	// An exclusion for something no longer reachable is a decision about nothing.
	reachable := reachableFromWorkflow()
	for name := range messagesOutsideTheCharter {
		_, ok := reachable[name]
		if !strings.HasPrefix(string(name), "flowstate.v1.") {
			continue
		}
		assert.True(t, ok, "%s is excluded but is not reachable from a Workflow; remove the entry", name)
	}
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

		// An enum field is not one construct, it is one per value an author may
		// select: a new `overlap:` policy or a new input type is a new capability,
		// and a key per *field* would stay satisfied by whichever value some
		// example already happened to use (Codex, #901). The zero value is
		// excluded because it spells "unset" rather than a choice.
		if field.Kind() == protoreflect.EnumKind {
			values := field.Enum().Values()
			for v := range values.Len() {
				value := values.Get(v)
				if value.Number() == 0 {
					continue
				}
				required[spec.prefix+"."+string(field.Name())+"."+string(value.Name())] = "an example selecting that value"
			}

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
		// From the Workflow itself rather than from its steps: `triggers:`,
		// `inputs:` and `outputs:` hang off the workflow message, and their own
		// sub-messages (a webhook's `verify:`, a schedule's `cron:`) hang off
		// those — none of which a walk that started at a node would ever reach.
		// The recursion covers the steps on its way through.
		walkConstructs(wf.ProtoReflect(), report)
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
		// A dotted name in the *default* registry is not a built-in an author
		// writes. Built-in tasks are undotted (`http`, `log`); the dot is the
		// mark of a plugin task, and a plugin task is either demonstrated in
		// examples/plugins/ (which this corpus excludes, since those files name
		// tasks a stock `flow` cannot resolve) or — as here — a conformance test
		// fixture some sibling test registered into the shared registry
		// (`test.plugin_inputs`, from plugintaskinputs_local_test.go). Requiring a
		// portfolio example for either would be requiring what this corpus is
		// defined not to contain, so a dotted registry entry is skipped for the
		// same reason the whole examples/plugins/ tree is.
		if strings.Contains(task, ".") {
			continue
		}
		required["task."+task] = "a step naming that task"
	}
	for name := range writableSpecs() {
		desc, err := protoregistry.GlobalFiles.FindDescriptorByName(name)
		require.NoError(t, err, "%s is not in the global registry", name)
		msg, ok := desc.(protoreflect.MessageDescriptor)
		require.True(t, ok, "%s is not a message", name)
		writableRequired(required, msg)
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
		if field.Kind() == protoreflect.EnumKind {
			if value := field.Enum().Values().ByNumber(msg.Get(field).Enum()); value != nil && value.Number() != 0 {
				report(spec.prefix + "." + string(field.Name()) + "." + string(value.Name()))
			}

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
	for name := range writableSpecs() {
		desc, err := protoregistry.GlobalFiles.FindDescriptorByName(name)
		require.NoError(t, err, "%s is not in the global registry", name)
		msg, ok := desc.(protoreflect.MessageDescriptor)
		require.True(t, ok, "%s is not a message", name)
		writableRequired(required, msg)
	}

	for name := range writableSpecs() {
		desc, err := protoregistry.GlobalFiles.FindDescriptorByName(name)
		require.NoError(t, err)
		msg, ok := desc.(protoreflect.MessageDescriptor)
		require.True(t, ok)
		writableRequired(required, msg)
	}

	// Named one by one rather than counted, because a count passes while
	// describing the wrong set. The second group is what the generalization past
	// Node/StepPolicy/Workflow buys: a loop's stop condition and its carried
	// state, a fan-out's bound, a retry's shape, a wait's deadline. Deriving from
	// three messages left every one of them unchecked.
	for _, construct := range []string{
		"node.condition", "node.async", "node.undo", "node.vars",
		"policy.timeout", "policy.retry",
		"workflow.declared_inputs", "workflow.declared_outputs",
		"workflow.triggers", "workflow.signals", "workflow.vars",

		// Named by their *schema* field, which is not always the YAML key an
		// author types: `attempts:` is `max_attempts`, `backoff:` is
		// `backoff_coefficient`, and a `call:`'s `with:` is `arguments`. The
		// charter keys off the descriptor, so this list has to as well — and
		// getting three of them wrong the first time is exactly why they are
		// pinned here rather than assumed.
		"loop.until", "loop.max_iterations", "loop.state", "loop.update",
		"for_each.max_parallel", "for_each.items",
		"retry.max_attempts", "retry.max_interval", "retry.backoff_coefficient",
		"wait.timeout", "call.arguments",
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
