package flowstatev1

import (
	"maps"
	"slices"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// The enum values a workflow's tasks can answer with, spelled by name.
//
// A task's output descriptor says which Protobuf enums its answer holds, and the
// run stores each as its number (see [TypeOfField]). Authors used to read the
// number: `answer.calibration == 2`, where the schema says
// CALIBRATION_SELF_REPORTED. The names are derived here from the same
// descriptors the registry already holds, so they stay what the Protobuf says
// and no second list of them exists to drift.
//
// The names are not a runtime feature. The Flowfile compiler replaces each name
// with its number before the specification exists, so a driver evaluates the
// integer a Protobuf enum is on the wire and both drivers agree by
// construction.

// maxEnumWalkMessages bounds how many message types one output descriptor is
// followed through. A descriptor is a plugin's, and a message graph can be wide
// and cyclic; the bound keeps a hostile one from costing more than a constant.
const maxEnumWalkMessages = 256

// OutputEnumValue is one value of a Protobuf enum that a task's output can hold.
type OutputEnumValue struct {
	// Name is the value's Protobuf name, such as CALIBRATION_SELF_REPORTED.
	Name string

	// Number is what a run stores for it.
	Number int64

	// Enum is the enum it belongs to.
	Enum protoreflect.EnumDescriptor
}

// OutputEnums is every enum value reachable from the outputs of the tasks a
// workflow runs, and which output fields hold an enum.
type OutputEnums struct {
	values map[string]OutputEnumValue

	// ambiguous are value names two enums define with different numbers.
	ambiguous map[string]bool

	// fields maps a field name to the enum every output field of that name holds.
	// A name some output field holds as anything else, or two enums share, is
	// absent: the name alone cannot say what a read of it is.
	fields map[string]protoreflect.EnumDescriptor
}

// OutputEnumsOf collects the enum values the outputs of wf's own task steps (and
// their `undo:` tasks) can hold, from tasks' descriptors; nil means
// [DefaultRegistry]. A `call:`'s callee is its own file and keeps its own names.
func OutputEnumsOf(wf *Workflow, tasks *Registry) *OutputEnums {
	enums := &OutputEnums{
		values:    map[string]OutputEnumValue{},
		ambiguous: map[string]bool{},
		fields:    map[string]protoreflect.EnumDescriptor{},
	}

	mixed := map[string]bool{}
	seen := map[string]bool{}
	visit := func(name string) {
		if name == "" || seen[name] {
			return
		}
		seen[name] = true

		if def, found := lookupTaskDef(tasks, name); found && def.Outputs != nil {
			enums.addMessage(def.Outputs, mixed, map[protoreflect.FullName]bool{})
		}
	}

	WalkNodes(wf.GetSteps(), Walk{Node: func(node *Node) {
		visit(node.GetTask().GetName())
		visit(node.GetUndo().GetTask().GetName())
	}})

	for name := range mixed {
		delete(enums.fields, name)
	}

	return enums
}

func (e *OutputEnums) addMessage(message protoreflect.MessageDescriptor, mixed map[string]bool, visited map[protoreflect.FullName]bool) {
	if visited[message.FullName()] || len(visited) >= maxEnumWalkMessages ||
		slices.Contains(dynamicValueMessages, message.FullName()) || message.ParentFile().Package() == "google.protobuf" {
		// A message that holds whatever an expression produced, and a well-known
		// type, say nothing about the enums a task answers with (`NULL_VALUE` is
		// how a null is spelled inside them).
		return
	}
	visited[message.FullName()] = true

	fields := message.Fields()
	for i := range fields.Len() {
		field := fields.Get(i)
		name := string(field.Name())

		kind := field.Kind()
		if field.IsMap() {
			// The field is the map and not the enum its values hold.
			mixed[name] = true
			kind = field.MapValue().Kind()
			field = field.MapValue()
		}

		switch kind {
		case protoreflect.EnumKind:
			e.addEnum(field.Enum())
			if prior, ok := e.fields[name]; ok && prior.FullName() != field.Enum().FullName() {
				mixed[name] = true
			} else {
				e.fields[name] = field.Enum()
			}
		case protoreflect.MessageKind, protoreflect.GroupKind:
			// A message-valued field of this name is a read of a record, so an
			// enum of the same name elsewhere cannot settle what it holds.
			mixed[name] = true
			e.addMessage(field.Message(), mixed, visited)
		default:
			mixed[name] = true
		}
	}
}

func (e *OutputEnums) addEnum(enum protoreflect.EnumDescriptor) {
	values := enum.Values()
	for i := range values.Len() {
		value := values.Get(i)
		name := string(value.Name())
		entry := OutputEnumValue{Name: name, Number: int64(value.Number()), Enum: enum}

		if prior, ok := e.values[name]; ok && (prior.Number != entry.Number || prior.Enum.FullName() != enum.FullName()) {
			e.ambiguous[name] = true
		}
		e.values[name] = entry
	}
}

// Value is the enum value spelled name, false where no task of the workflow has
// one of that name or two enums disagree about it.
func (e *OutputEnums) Value(name string) (OutputEnumValue, bool) {
	if e == nil || e.ambiguous[name] {
		return OutputEnumValue{}, false
	}
	value, ok := e.values[name]

	return value, ok
}

// Ambiguous reports a name two enums of the workflow's tasks define differently.
func (e *OutputEnums) Ambiguous(name string) bool {
	return e != nil && e.ambiguous[name]
}

// Names are the usable value names, sorted.
func (e *OutputEnums) Names() []string {
	if e == nil {
		return nil
	}

	return slices.DeleteFunc(slices.Sorted(maps.Keys(e.values)), func(name string) bool { return e.ambiguous[name] })
}

// FieldEnum is the enum every output field called field holds, false when no
// output has one of that name or the name is not one enum throughout.
func (e *OutputEnums) FieldEnum(field string) (protoreflect.EnumDescriptor, bool) {
	if e == nil {
		return nil, false
	}
	enum, ok := e.fields[field]

	return enum, ok
}

// Empty reports that no task of the workflow has an enum in its outputs.
func (e *OutputEnums) Empty() bool {
	return e == nil || len(e.values) == 0
}
