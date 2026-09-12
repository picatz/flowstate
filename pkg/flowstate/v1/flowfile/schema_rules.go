package flowfile

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/reflect/protoreflect"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// schemaDiagnostics reports every rule the schema declares that the compiled
// workflow breaks, as diagnostics an author can find in their file.
//
// The compiler checks what the file means: unknown tasks, references that cannot
// resolve, duplicate ids. The schema's own rules — `max_items` on a step list,
// `max_len` on an id, a `pattern` on a field — are enforced by [v1.Validate], and
// nothing on the author's side ran it: `flow validate` said ok and `flow run local`
// ran a 4,000-step file the server refuses at submit with "workflow.steps: must
// contain no more than 100 item(s)" (#1757). That is the local driver executing
// a program the durable driver cannot accept, and this is the join that closes
// it: the same rules, the same words, before submit.
//
// A rule the compiler already reports in its own words is skipped, so one fault
// is not reported in two voices (see [compilerOwnsRule]); a rule that cannot be
// evaluated at all is reported once, as a problem with the validator rather than
// with the file, because a verdict was not reached and silence would read as one.
func schemaDiagnostics(wf *v1.Workflow, positions *Positions) Diagnostics {
	err := v1.Validate(wf)
	if err == nil {
		return nil
	}

	var invalid *v1.ValidationError
	if !errors.As(err, &invalid) {
		return Diagnostics{{Message: err.Error()}}
	}

	// The underlying violations carry what [v1.Violation] deliberately does not:
	// the rule's own value, which is how a list bound is turned into a position
	// on the first element past it.
	var cause *protovalidate.ValidationError
	errors.As(invalid, &cause)

	var ds Diagnostics
	for i, violation := range invalid.Violations {
		if compilerOwnsRule(violation.Field, violation.Rule) {
			continue
		}

		var bound protoreflect.Value
		if cause != nil && i < len(cause.Violations) {
			bound = cause.Violations[i].RuleValue
		}

		d := Diagnostic{
			Message: violation.String(),
			Code:    v1.DiagnosticCodeConstraintViolation,
		}
		positionSchemaViolation(&d, wf, violation, bound, positions)
		ds = append(ds, d)
	}

	return ds
}

// compilerOwnsRule reports whether the compiler already says what a schema rule
// would, in words that name the fix.
//
// An absence — a required field, an empty list, an empty string — is one the
// compiler reports for everything an author can leave out ("step has no id",
// "workflow has no steps", "steps is required: a loop needs a body"), and a
// compiled workflow missing a required field an author cannot write is a
// compiler defect rather than a file's. The name's pattern is reported by
// [Validate] with a name to paste. Everything else — a bound, a length, a pattern
// on any other field — is the schema's alone.
func compilerOwnsRule(field, rule string) bool {
	switch rule {
	case "required", "repeated.min_items", "string.min_len":
		return true
	case "string.pattern":
		return field == "name"
	}
	return false
}

// positionSchemaViolation places a schema violation in the source: on the step
// it is about, and as near to the field as the recorded positions reach.
//
// A violation names a field by its protobuf path (`steps[2].loop.body`), and the
// compiler recorded positions by the keys the author wrote (`steps[2].loop`,
// `steps[2].loop.steps[0]`). The walk from the workflow's root along the
// protobuf path finds the innermost step the field belongs to, which is what
// the diagnostic names, and translates the path into the author's spelling as
// far as it can. Whatever it cannot translate falls back to the nearest
// recorded ancestor, so a diagnostic lands on its step rather than on line 1.
//
// A list past its bound is placed on the first element over it — the 101st step
// — because that is the line an author has to do something about, and it is
// the element the bound names.
func positionSchemaViolation(d *Diagnostic, wf *v1.Workflow, violation v1.Violation, bound protoreflect.Value, positions *Positions) {
	walk := walkFieldPath(wf.ProtoReflect(), violation.Field)
	d.Step = walk.step

	candidates := walk.candidates

	// The element past a bound, when the violation is a list too long and the
	// rule says how long it may be.
	if violation.Rule == "repeated.max_items" && bound.IsValid() && walk.list.IsValid() {
		limit := int(bound.Uint())
		if list := walk.list.List(); limit < list.Len() {
			element := list.Get(limit)
			if id := nodeID(element); id != "" {
				d.Step = id
			}
			candidates = append([]string{fmt.Sprintf("%s[%d]", walk.authored, limit)}, candidates...)
		}
	}

	for _, candidate := range candidates {
		if span, ok := positions.At(candidate); ok {
			d.Line, d.Column = span.Start.Line, span.Start.Column
			return
		}
	}
	if span, ok := positions.Locate(d.Step, ""); ok {
		d.Line, d.Column = span.Start.Line, span.Start.Column
	}
}

// fieldWalk is what walking a protobuf field path over a workflow finds.
type fieldWalk struct {
	// step is the id of the innermost step the path passes through, or empty
	// when it names something outside every step.
	step string

	// authored is the path in the author's spelling, as far as the walk could
	// translate it: the same as the protobuf path for a step's own properties,
	// with a task's inputs under the task's name and a body's steps under
	// `steps`.
	authored string

	// candidates is every recorded path the violation could be placed at, most
	// specific first: the translated path, then each of its ancestors.
	candidates []string

	// list is the value at the end of the path when that is a list, so a bound
	// violation can find the element past it.
	list protoreflect.Value
}

// walkFieldPath follows a protobuf field path from a message, recording the
// steps it passes through and the author's spelling of the path.
//
// A component is a field name, optionally indexed (`steps[3]`) or keyed
// (`inputs["url"]`) the way protovalidate renders one. The walk stops, keeping
// what it has, at the first component it cannot follow: a path from a rule the
// schema evaluated is always followable, but a diagnostic positioned on a
// partial walk is still positioned on the right step.
func walkFieldPath(msg protoreflect.Message, path string) fieldWalk {
	var walk fieldWalk
	var authored []string

	remember := func() {
		walk.authored = strings.Join(authored, ".")
		walk.candidates = append([]string{walk.authored}, walk.candidates...)
	}

	for _, component := range strings.Split(path, ".") {
		name, index, key := splitPathComponent(component)
		if msg == nil {
			return walk
		}
		fd := msg.Descriptor().Fields().ByName(protoreflect.Name(name))
		if fd == nil {
			return walk
		}
		value := msg.Get(fd)

		// The author's spelling of this component. A task's inputs sit under the
		// task's own name (`steps[0].http.url`), and a body's steps under `steps`
		// whatever the schema calls the list; a parallel branch is the list
		// entry itself.
		switch {
		case fd.Name() == "task" && fd.Message() != nil && fd.Message().FullName() == taskMessageName:
			authored = append(authored, value.Message().Get(fd.Message().Fields().ByName("name")).String())
		case fd.Name() == "inputs" && fd.IsMap() && key != "":
			authored = append(authored, key)
		case fd.Name() == "body" && fd.IsList():
			authored = append(authored, indexed("steps", index))
		case fd.Name() == "branches" && fd.IsList():
			// `parallel.branches[1]` is written `parallel[1]`.
			if n := len(authored); n > 0 && index >= 0 {
				authored[n-1] = indexed(authored[n-1], index)
			}
		case fd.IsMap() && key != "":
			authored = append(authored, name, key)
		default:
			authored = append(authored, indexed(name, index))
		}

		switch {
		case fd.IsList():
			walk.list = value
			if index < 0 {
				remember()
				return walk
			}
			list := value.List()
			if index >= list.Len() {
				remember()
				return walk
			}
			value = list.Get(index)
			walk.list = protoreflect.Value{}
		case fd.IsMap():
			if key == "" {
				remember()
				return walk
			}
			value = value.Map().Get(protoreflect.ValueOfString(key).MapKey())
			if !value.IsValid() {
				remember()
				return walk
			}
		}
		remember()

		// A map field's own kind is the synthetic map-entry *message* protobuf
		// generates for it, whatever the entries hold, so asking the field
		// answers about the pair rather than about what the walk just selected.
		// `labels["..."]` selects a string, and descending into it as a message
		// panics — from a violation an author's own `labels:` produced, which is
		// every path through validation (#1119). The value's kind is the one
		// this step is about.
		kind := fd.Kind()
		if fd.IsMap() {
			kind = fd.MapValue().Kind()
		}

		if kind != protoreflect.MessageKind || (fd.IsList() && index < 0) {
			msg = nil
			continue
		}
		msg = value.Message()
		if id := nodeID(value); id != "" {
			walk.step = id
		}
	}

	return walk
}

// taskMessageName is the schema's name for a step's task, which is the one
// component the author writes as the task's own name rather than as `task`.
const taskMessageName = protoreflect.FullName("flowstate.v1.Task")

// nodeID is the id of a value that is a step, and empty for anything else.
func nodeID(value protoreflect.Value) string {
	if !value.IsValid() {
		return ""
	}
	msg, ok := value.Interface().(protoreflect.Message)
	if !ok || msg.Descriptor().FullName() != "flowstate.v1.Node" {
		return ""
	}
	return msg.Get(msg.Descriptor().Fields().ByName("id")).String()
}

// indexed renders a list component the way positions are recorded: `steps[3]`,
// or the bare name when there is no index.
func indexed(name string, index int) string {
	if index < 0 {
		return name
	}
	return name + "[" + strconv.Itoa(index) + "]"
}

// splitPathComponent reads one component of a protovalidate field path: the
// field's name, its list index (or -1), and its map key (or empty).
func splitPathComponent(component string) (name string, index int, key string) {
	index = -1
	open := strings.IndexByte(component, '[')
	if open < 0 || !strings.HasSuffix(component, "]") {
		return component, index, ""
	}
	name = component[:open]
	inside := component[open+1 : len(component)-1]
	if unquoted, err := strconv.Unquote(inside); err == nil {
		return name, index, unquoted
	}
	if n, err := strconv.Atoi(inside); err == nil {
		return name, n, ""
	}
	return name, index, inside
}
