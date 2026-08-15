package flowstatev1

import (
	"fmt"
	"slices"
	"strings"

	validate "buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Reading a task's shape out of its schema, in one place.
//
// A [TaskDef] carries the descriptors of its input and output messages, and that
// is deliberately the only definition of what a task takes: the engine validates
// against it, `flow validate` reports against it, the editor completes from it,
// and `flow tasks` prints it. What that leaves is a handful of questions each of
// those has to ask — is this field required, what would an author call its type —
// and those were being answered separately.
//
// Two implementations of "required" existed, in flowfile and in the language
// server, agreeing today and written differently enough to stop agreeing later.
// One of them checked `HasMinItems() && GetMinItems() > 0` and the other only
// `GetMinItems() > 0`. Nothing turns on the difference right now, which is exactly
// the condition under which a copy survives long enough to matter.
//
// "What type is this" existed once, in the language server, which is why `flow
// tasks` could tell you a task exists and not what it takes.

// dynamicValueMessages are the schema messages that hold whatever an expression
// produced, rather than a fixed type.
//
// Naming the concrete message would be true and useless: an author writing a
// value there needs to know the shape is unconstrained, not which wrapper the
// engine stores it in.
var dynamicValueMessages = []protoreflect.FullName{
	"flowstate.v1.Value",
	"google.api.expr.v1alpha1.Value",
	"google.protobuf.Value",
}

// FieldRules returns the protovalidate rules attached to a field, or nil.
func FieldRules(fd protoreflect.FieldDescriptor) *validate.FieldRules {
	if fd == nil {
		return nil
	}
	rules, _ := proto.GetExtension(fd.Options(), validate.E_Field).(*validate.FieldRules)
	return rules
}

// RequiredInput reports whether the schema marks a field as one the task cannot
// run without.
//
// Read from protovalidate rather than from a list, so a field that becomes
// required in the schema is required everywhere at once — in the engine that
// rejects the run, in the diagnostic that explains it, and in the completion that
// offers it first.
//
// Two spellings mean the same thing and the schema uses both: `required` on a
// singular field, and `min_items: 1` on a repeated one, where "required" would be
// satisfied by an empty list.
func RequiredInput(fd protoreflect.FieldDescriptor) bool {
	rules := FieldRules(fd)
	if rules == nil {
		return false
	}
	if rules.GetRequired() {
		return true
	}

	repeated := rules.GetRepeated()

	return repeated != nil && repeated.GetMinItems() > 0
}

// FieldConstraints says what else the schema will accept in a field, as phrases
// an author reads.
//
// The same argument as [InputTypeName], one level in: the rules are already there,
// attached to the field the engine validates against, and the only thing missing
// was a sentence. `method` is `3 to 6 characters, matching
// ^(?i)(GET|POST|PUT|PATCH|DELETE)$` whether or not anything says so, and a person
// who has to run a step to discover that is being told by the wrong teacher.
//
// Requiredness is deliberately not among them. It has its own field on
// [InputField], is marked in the listing, and stating it twice would put the same
// fact in two places that can disagree about it.
//
// What it omits is as deliberate: `defined_only` on an enum says nothing
// [InputTypeName] has not already said by listing the choices, and a rule with no
// author-facing consequence is noise in a column somebody is scanning.
func FieldConstraints(fd protoreflect.FieldDescriptor) []string {
	return constraintPhrases(FieldRules(fd))
}

// DeclaredBounds says what a workflow's own `inputs:` declaration holds a value
// to, in the phrases [FieldConstraints] renders a schema field's rules in.
//
// One vocabulary for one fact. A declaration's `min_len`/`max_len` bounds a
// string exactly as a protovalidate `string.min_len` bounds a task input's, and
// an author reading a `with:` argument and an author reading a task input are
// reading the same language: two spellings of one bound is how an editor comes to
// disagree with a terminal about what a value must be. The editor's own copy said
// "at least 3 characters, at most 63 characters" for what every other surface
// calls "3 to 63 characters".
//
// Requiredness, a default and `must:` are deliberately absent, for
// [FieldConstraints]'s reason: each has its own field on the declaration and its
// own place in every surface that shows one, and stating it twice puts one fact
// in two places that can disagree about it.
func DeclaredBounds(declaration *InputDeclaration) []string {
	if declaration == nil {
		return nil
	}

	var out []string
	out = append(out, countPhrase("characters", declaration.MinLen, declaration.MaxLen)...)
	out = append(out, countPhrase("items", declaration.MinItems, declaration.MaxItems)...)

	return out
}

// constraintPhrases renders one rule set, and is recursive because a map's rules
// carry a whole rule set for its keys and another for its values.
func constraintPhrases(rules *validate.FieldRules) []string {
	if rules == nil {
		return nil
	}

	var out []string

	if s := rules.GetString(); s != nil {
		if s.HasLen() {
			out = append(out, fmt.Sprintf("exactly %d characters", s.GetLen()))
		} else {
			out = append(out, countPhrase("characters", s.MinLen, s.MaxLen)...)
		}
		if s.HasPattern() {
			out = append(out, "matching "+s.GetPattern())
		}
		// The well-known formats this schema actually uses. Named one at a time
		// rather than switched over every value protovalidate has, because each
		// phrase is a translation into what an author would write and there is no
		// generic rendering of one.
		switch {
		case s.GetUri():
			out = append(out, "a URI")
		case s.GetEmail():
			out = append(out, "an email address")
		case s.GetHostname():
			out = append(out, "a hostname")
		}
	}

	if r := rules.GetRepeated(); r != nil {
		out = append(out, countPhrase("items", r.MinItems, r.MaxItems)...)
	}

	if m := rules.GetMap(); m != nil {
		out = append(out, countPhrase("entries", m.MinPairs, m.MaxPairs)...)
		for _, nested := range []struct {
			label string
			rules *validate.FieldRules
		}{
			{"keys", m.GetKeys()},
			{"values", m.GetValues()},
		} {
			for _, phrase := range constraintPhrases(nested.rules) {
				out = append(out, nested.label+" "+phrase)
			}
		}
	}

	out = append(out, numericRangePhrases(rules)...)

	return out
}

// countPhrase renders a lower and an upper bound over the same unit as one phrase.
//
// One phrase rather than two, because `3 to 6 characters` is how the bound is
// thought about and `at least 3 characters, at most 6 characters` is the same fact
// made into a puzzle. Either half may be absent, and both absent says nothing.
func countPhrase(unit string, minimum, maximum *uint64) []string {
	switch {
	case minimum != nil && maximum != nil:
		return []string{fmt.Sprintf("%d to %d %s", *minimum, *maximum, unit)}
	case minimum != nil:
		return []string{fmt.Sprintf("at least %d %s", *minimum, unit)}
	case maximum != nil:
		return []string{fmt.Sprintf("at most %d %s", *maximum, unit)}
	default:
		return nil
	}
}

// numericRangePhrases renders whichever numeric rule set a field carries.
//
// Read through protoreflect rather than by switching over the twelve numeric rule
// messages protovalidate defines. They differ only in the Go type of the same four
// fields (`gte`, `lte`, `gt`, `lt`), so a switch would be twelve copies of one
// paragraph, and the thirteenth numeric type added upstream would be the one nobody
// remembered to add. Asking the descriptor for a field by name is the same lookup
// each of those copies would compile to, done once.
func numericRangePhrases(rules *validate.FieldRules) []string {
	message := rules.ProtoReflect()

	oneof := message.Descriptor().Oneofs().ByName("type")
	if oneof == nil {
		return nil
	}

	set := message.WhichOneof(oneof)
	if set == nil || set.Kind() != protoreflect.MessageKind {
		return nil
	}

	nested := message.Get(set).Message()

	read := func(name string) (string, bool) {
		fd := nested.Descriptor().Fields().ByName(protoreflect.Name(name))
		if fd == nil || !nested.Has(fd) {
			return "", false
		}

		// Formatted through the value's own String, which renders an int as an int
		// and a double as a double. Casting to one Go type here would print `100`
		// for a bound of `1e2` on a double field, or lose a large uint64 outright.
		return nested.Get(fd).String(), true
	}

	// Which rule matched decides the words, because gt and gte differ by
	// exactly the endpoint: a field constrained `gt: 0` refuses zero, and a
	// surface that says "at least 0" teaches an author the one value the
	// validator will reject.
	lower, hasLower := read("gte")
	lowerPhrase := "at least "
	if !hasLower {
		lower, hasLower = read("gt")
		lowerPhrase = "more than "
	}
	upper, hasUpper := read("lte")
	upperPhrase := "at most "
	if !hasUpper {
		upper, hasUpper = read("lt")
		upperPhrase = "less than "
	}

	switch {
	case hasLower && hasUpper:
		if lowerPhrase == "at least " && upperPhrase == "at most " {
			// Both endpoints included is the common case and reads as a range.
			return []string{lower + " to " + upper}
		}
		return []string{lowerPhrase + lower, upperPhrase + upper}
	case hasLower:
		return []string{lowerPhrase + lower}
	case hasUpper:
		return []string{upperPhrase + upper}
	default:
		return nil
	}
}

// InputTypeName names a field's type the way an author would say it.
//
// The DSL's vocabulary, not Protobuf's: an author writes YAML and thinks in
// `string`, `list[string]`, `map[string, string]`. Reporting `TYPE_STRING` or
// `repeated .flowstate.v1.Value` would be accurate about the schema and useless
// about the file being written.
func InputTypeName(fd protoreflect.FieldDescriptor) string {
	if fd == nil {
		return "unknown"
	}

	switch {
	case fd.IsMap():
		return fmt.Sprintf("map[%s, %s]", scalarTypeName(fd.MapKey()), scalarTypeName(fd.MapValue()))
	case fd.IsList():
		return fmt.Sprintf("list[%s]", scalarTypeName(fd))
	default:
		return scalarTypeName(fd)
	}
}

// scalarTypeName names the type of a single value of the field's element type.
func scalarTypeName(fd protoreflect.FieldDescriptor) string {
	switch fd.Kind() {
	case protoreflect.StringKind:
		return "string"
	case protoreflect.BoolKind:
		return "bool"
	case protoreflect.BytesKind:
		return "bytes"
	case protoreflect.Int32Kind, protoreflect.Int64Kind, protoreflect.Sint32Kind,
		protoreflect.Sint64Kind, protoreflect.Sfixed32Kind, protoreflect.Sfixed64Kind:
		return "int"
	case protoreflect.Uint32Kind, protoreflect.Uint64Kind, protoreflect.Fixed32Kind,
		protoreflect.Fixed64Kind:
		return "uint"
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return "double"
	case protoreflect.EnumKind:
		// The choices, not the type's name. `Level` tells an author which Go type
		// they will never see; `info | warn | error` tells them what to type, and it
		// is short enough that a list beats a name.
		return strings.Join(EnumValueNames(fd.Enum()), " | ")
	case protoreflect.MessageKind, protoreflect.GroupKind:
		if slices.Contains(dynamicValueMessages, fd.Message().FullName()) {
			// A CEL value: the concrete type is whatever the expression produces,
			// which is exactly what "any" tells the author.
			return "any"
		}
		return string(fd.Message().Name())
	default:
		return fd.Kind().String()
	}
}

// An InputField is one input a task accepts, described the way an author needs it.
type InputField struct {
	// Name is what the input is called in a Flowfile.
	Name string

	// Type is the DSL's name for what it holds.
	Type string

	// Required reports whether the task cannot run without it.
	Required bool

	// Deferred reports whether the task evaluates this input's expression itself,
	// against a scope the workflow does not have. The http task's `outputs` is
	// the example: it names response variables that exist only after the request.
	Deferred bool

	// Constraints are the rest of what may be written here, as phrases an author
	// reads: `3 to 6 characters`, `at most 32 entries`, `may hold a secret
	// reference`. See [FieldConstraints] for where each one comes from.
	//
	// Empty for a field the schema bounds no further, which is most of them.
	Constraints []string
}

// Inputs describes what a task accepts, required fields first.
//
// Required first because that is the order somebody needs them in: the inputs
// without which nothing works, and then the ones that tune it. Within each group
// the schema's own field order is kept, which is the order the person who defined
// the message chose to explain it in.
func Inputs(def TaskDef) []InputField {
	return describeFields(def.Inputs, def.DeferredInputs, taskInputNotes(def))
}

// taskInputNotes is what the *task* adds to a field's constraints, by input name.
//
// Read off the definition rather than restated: `ExpressionInputs` is already the
// list `flow validate` refuses a literal against, and the two secret-accepting
// lists are already what the compiler allows a reference in. A surface that
// described either of those separately would be a second answer to a question the
// engine decides.
func taskInputNotes(def TaskDef) map[string][]string {
	notes := map[string][]string{}

	for _, name := range def.ExpressionInputs {
		notes[name] = append(notes[name], "must be written as an expression")
	}

	// AuthorityInputs is the wrong list to read "takes a secret" from on its
	// own, because it answers a routing question: which inputs need the
	// identity-aware activity. A credential input needs that authority for JIT
	// exchange while its value is a literal target name, and the task refuses a
	// secret reference there. Saying "may hold a secret reference" about it
	// would teach an author the one spelling that fails at execution, so the
	// credential subset gets its own honest note and the secret note goes to
	// what remains plus the nested list.
	for _, name := range def.CredentialInputs {
		notes[name] = append(notes[name], "names a deployment credential target")
	}
	for _, name := range slices.Sorted(slices.Values(append(
		slices.Clone(def.AuthorityInputs), def.NestedSecretInputs...))) {
		if slices.Contains(def.CredentialInputs, name) ||
			slices.Contains(notes[name], "may hold a secret reference") {
			continue
		}
		notes[name] = append(notes[name], "may hold a secret reference")
	}

	return notes
}

// Outputs describes what a task produces.
//
// Required is not reported: an output the task always sets is not a thing the
// author has to supply, so the distinction says nothing here. Whether a field is
// present after a step ran is a question about that run.
func Outputs(def TaskDef) []InputField {
	fields := describeFields(def.Outputs, nil, nil)
	for i := range fields {
		fields[i].Required = false
	}
	return fields
}

// describeFields walks a message descriptor into the author's vocabulary.
func describeFields(md protoreflect.MessageDescriptor, deferred []string, notes map[string][]string) []InputField {
	if md == nil {
		return nil
	}

	fields := md.Fields()
	out := make([]InputField, 0, fields.Len())
	for i := range fields.Len() {
		fd := fields.Get(i)
		name := string(fd.Name())
		out = append(out, InputField{
			Name:        name,
			Type:        InputTypeName(fd),
			Required:    RequiredInput(fd),
			Deferred:    slices.Contains(deferred, name),
			Constraints: append(FieldConstraints(fd), notes[name]...),
		})
	}

	slices.SortStableFunc(out, func(a, b InputField) int {
		switch {
		case a.Required == b.Required:
			return 0
		case a.Required:
			return -1
		default:
			return 1
		}
	})

	return out
}

// Catalog describes everything this build can execute.
//
// Built from the registry rather than maintained, so a task added to it appears
// here, in `flow tasks`, in the editor's completion, and in whatever an agent
// reads, without any of those being touched. That is the whole argument for the
// registry being the single source of truth for capability, and this is the last
// surface that was not derived from it.
func Catalog() *TaskCatalog {
	defs := DefaultRegistry().All()

	catalog := &TaskCatalog{
		Tasks:         make([]*TaskDescription, 0, len(defs)),
		CelLibraries:  ExtensionLibraries(),
		CelFunctions:  catalogFunctions(),
		DurationUnits: DurationUnits(),
		NowIdentifier: NowIdentifier,
		ValueRoots:    []string{VarsRoot, StepsRoot, InputsRoot, RunRoot, TriggerRoot},
	}

	for _, def := range defs {
		catalog.Tasks = append(catalog.Tasks, DescribeTask(def))
	}

	return catalog
}

// DescribeTask renders one task into its schema form.
//
// Exported because a plugin's tasks are described here too, and a second
// rendering of the same thing is how the two would come to disagree about a task
// that is meant to be indistinguishable from a built-in. There is one way a task
// is described, and this is it.
func DescribeTask(def TaskDef) *TaskDescription {
	return &TaskDescription{
		Name:    def.Name,
		Summary: def.Summary,
		Inputs:  taskFields(Inputs(def)),
		Outputs: taskFields(Outputs(def)),
	}
}

// catalogFunctions renders the profile's functions into their schema form.
//
// The same set `flow tasks` prints and the editor completes from, because there is
// one [ProfileFunctions] and every surface reads it. A machine-readable catalog that
// listed a different set from the one a person is shown would be worse than not
// carrying them at all: the whole point of this message is that it is the contract.
func catalogFunctions() []*CELFunction {
	functions := ProfileFunctions(CurrentProfile)

	out := make([]*CELFunction, 0, len(functions))
	for _, fn := range functions {
		out = append(out, &CELFunction{
			Name:    fn.Name,
			Library: fn.Library,
			Macro:   fn.Macro,
			Example: fn.Example,
		})
	}

	return out
}

// taskFields converts the described fields into their schema form.
func taskFields(fields []InputField) []*TaskField {
	out := make([]*TaskField, 0, len(fields))
	for _, field := range fields {
		out = append(out, &TaskField{
			Name:        field.Name,
			Type:        field.Type,
			Required:    field.Required,
			Deferred:    field.Deferred,
			Constraints: field.Constraints,
		})
	}
	return out
}

// Enum-valued inputs, spelled the way a Flowfile writes them.
//
// A schema enum is written `LEVEL_WARN`, because a proto enum's values share a
// namespace with its siblings and buf's lint rules require the prefix. A Flowfile is
// not proto: an author writes `level: warn`, which is the name without the noise the
// namespace forced. These two functions are the only place that correspondence lives,
// so a task gaining an enum input gets the spelling, the diagnostic and the printed
// type without deciding any of it again.

// EnumValueNames returns the spellings a Flowfile may use for an enum, in declaration
// order.
//
// The zero value is omitted. Every proto3 enum must have one and buf requires it be
// named `_UNSPECIFIED`, which makes it the encoding of *absent* rather than a choice —
// offering it would invite `level: unspecified`, a way of writing nothing that reads
// like writing something.
func EnumValueNames(enum protoreflect.EnumDescriptor) []string {
	values := enum.Values()
	names := make([]string, 0, values.Len())
	for i := range values.Len() {
		value := values.Get(i)
		if value.Number() == 0 {
			continue
		}
		names = append(names, enumValueSpelling(enum, value))
	}

	return names
}

// EnumValueNumber resolves what an author wrote to an enum value, reporting whether it
// named one.
//
// Both spellings are accepted — `warn` and `LEVEL_WARN` — because the second is what a
// reader of the schema, or of a protojson payload, has in front of them, and refusing
// it would be the language pretending its own storage does not exist. Matching is
// case-insensitive for the same reason `flow` accepts `GET` and `get` from the http
// task: the case carries no meaning here, so enforcing one is a diagnostic that teaches
// nothing.
//
// The zero value is not resolvable by name, matching [EnumValueNames]: an input left
// out is how a Flowfile says "unspecified".
func EnumValueNumber(enum protoreflect.EnumDescriptor, written string) (protoreflect.EnumNumber, bool) {
	values := enum.Values()
	for i := range values.Len() {
		value := values.Get(i)
		if value.Number() == 0 {
			continue
		}
		if strings.EqualFold(written, enumValueSpelling(enum, value)) ||
			strings.EqualFold(written, string(value.Name())) {
			return value.Number(), true
		}
	}

	return 0, false
}

// enumValueSpelling strips the prefix proto requires from an enum value's name.
//
// Derived from the enum's own name rather than from a convention this file asserts:
// `Level` yields the prefix `LEVEL_`, so `LEVEL_WARN` becomes `warn`. A value not
// carrying the prefix is returned lowercased and otherwise untouched, which keeps this
// total — an enum that breaks the convention gets a worse spelling rather than a panic
// or an empty string.
func enumValueSpelling(enum protoreflect.EnumDescriptor, value protoreflect.EnumValueDescriptor) string {
	prefix := screamingSnake(string(enum.Name())) + "_"
	name := string(value.Name())

	return strings.ToLower(strings.TrimPrefix(name, prefix))
}

// screamingSnake converts a CamelCase proto identifier to the SCREAMING_SNAKE form its
// enum values are prefixed with.
func screamingSnake(name string) string {
	var b strings.Builder
	for i, r := range name {
		if i > 0 && r >= 'A' && r <= 'Z' {
			b.WriteByte('_')
		}
		b.WriteRune(r)
	}

	return strings.ToUpper(b.String())
}
