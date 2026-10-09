package flowstatev1

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// LiteralInputClaims lists the fields of a task's input message that declare
// the `literal` claim of the `flowstate.v1.input` option, as dotted paths from
// the message, in declaration order.
//
// The claim is valid on a string field, singular or repeated, or on a map whose
// values are strings, and the search descends through message fields (singular,
// repeated, or a map's values) so a claim on a nested message counts at any
// depth. A message that contains itself is entered once on the way down, and
// nesting beyond [MaxStructureDepth] is an error, so a descriptor an outside
// plugin chose cannot make this spend unbounded work.
//
// It fails closed: a claim on any other shape is an error rather than a claim
// read charitably, since a field the claim cannot constrain would look
// protected and not be.
func LiteralInputClaims(md protoreflect.MessageDescriptor) ([]string, error) {
	claims, err := InputClaims(md)
	if err != nil {
		return nil, err
	}

	var paths []string
	for _, c := range claims {
		paths = append(paths, c.Literal...)
	}

	return paths, nil
}

// collectFieldLiteralClaims appends the literal claims at or under fd. prefix is
// the dotted path of fd's parent, empty at the message's top level.
func collectFieldLiteralClaims(fd protoreflect.FieldDescriptor, prefix string, entering map[protoreflect.FullName]bool, depth int, out *[]string) error {
	path := prefix + string(fd.Name())
	element := fd
	if fd.IsMap() {
		element = fd.MapValue()
	}

	if literalClaimed(fd) {
		if element.Kind() != protoreflect.StringKind {
			return fmt.Errorf("field %q declares the literal claim but is not a string, a list of strings, or a map of strings", path)
		}
		*out = append(*out, path)

		return nil
	}

	if element.Kind() != protoreflect.MessageKind || element.Message() == nil {
		return nil
	}
	md := element.Message()
	if depth >= MaxStructureDepth {
		return fmt.Errorf("input message %q nests deeper than %d levels, which is more than literal claims are read through",
			md.FullName(), MaxStructureDepth)
	}
	// A message that contains itself is entered once on the way down.
	if entering[md.FullName()] {
		return nil
	}
	entering[md.FullName()] = true
	defer delete(entering, md.FullName())

	fields := md.Fields()
	for i := range fields.Len() {
		if err := collectFieldLiteralClaims(fields.Get(i), path+".", entering, depth+1, out); err != nil {
			return err
		}
	}

	return nil
}

// literalClaimed reports whether fd sets the literal claim itself.
func literalClaimed(fd protoreflect.FieldDescriptor) bool {
	input, _ := proto.GetExtension(fd.Options(), E_Input).(*InputOptions)

	return input.GetLiteral()
}

// A LiteralInputError is a task input that does not hold a literal where a
// field of it claims it must.
type LiteralInputError struct {
	// Input is the top-level input the value was written for, which is what a
	// Flowfile diagnostic positions on.
	Input string

	// Value is the dotted path of the value that is not a literal, starting at
	// the input. It equals Field when the claimed field itself is at fault, and
	// is an ancestor of it when an enclosing expression hides the field.
	Value string

	// Field is the dotted path of the field that claims it must be a literal.
	Field string

	// Found names what the value is instead: an expression, a secret reference,
	// a credential reference, or a structure the check cannot read.
	Found string
}

func (e *LiteralInputError) Error() string {
	if e.Value == e.Field {
		return fmt.Sprintf("%s must be written as a literal, but is %s", e.Field, e.Found)
	}

	return fmt.Sprintf("%s is %s, but %s inside it must be written as a literal; write the surrounding structure out so the field can be seen", e.Value, e.Found, e.Field)
}

// LiteralFieldViolation refuses a value written for one input of a task unless
// every field of it that claims `literal` holds one.
//
// A literal is text the author typed: a CEL literal value, or inside an
// expression that builds a mapping or a list, a constant string at exactly the
// claimed position. An expression, a secret or credential reference, or a
// structure holding either at that position is refused, and so is an
// expression that hides it: `text: ${steps.x.value}` could carry anything into
// a field nested inside it, so the claim cannot be shown to hold and the value
// is refused rather than guessed at. Siblings of a claimed field are not
// constrained, which is what lets a message's escaped arguments stay
// expressions beside a literal template.
//
// The shape of an input is the descriptor's, so a claim added to a task's
// schema is enforced without a list to keep in step. The result is nil for an
// input with no claim at or under it, and for one the task's schema does not
// declare, which is reported elsewhere. The first violation is returned.
func LiteralFieldViolation(md protoreflect.MessageDescriptor, name string, value *Value) *LiteralInputError {
	if md == nil || value == nil {
		return nil
	}
	fd := md.Fields().ByName(protoreflect.Name(name))
	if fd == nil {
		return nil
	}

	w := &literalWalker{input: name, claims: map[protoreflect.MessageDescriptor][]string{}}

	return w.field(fd, nodeOf(value), name, 0)
}

// LiteralFieldViolations applies [LiteralFieldViolation] to every input a task was
// given, in name order.
func LiteralFieldViolations(md protoreflect.MessageDescriptor, inputs map[string]*Value) []*LiteralInputError {
	if md == nil {
		return nil
	}

	var out []*LiteralInputError
	for _, name := range slices.Sorted(maps.Keys(inputs)) {
		if err := LiteralFieldViolation(md, name, inputs[name]); err != nil {
			out = append(out, err)
		}
	}

	return out
}

// checkNodeLiteralFields applies the literal claims to one task position, for
// the single admission walk [CheckRequiredSecretInputs] performs. position is
// the step key the task sits under, empty for the step's own task.
//
// Once an expression has been evaluated its result cannot be told from text the
// author typed, so a specification built by hand is held to the claim here,
// where the difference is still visible. The refusal names the step, the input,
// and the field path, never the value.
func checkNodeLiteralFields(stepID, position string, task *Task, registry *Registry) error {
	if task == nil {
		return nil
	}
	def, found := registry.Lookup(task.GetName())
	if !found {
		return nil
	}
	for _, violation := range LiteralFieldViolations(def.Inputs, task.GetInputs()) {
		step := fmt.Sprintf("step %q", stepID)
		if position != "" {
			step = fmt.Sprintf("step %q %s", stepID, position)
		}

		return fmt.Errorf("%s: task %q input %q: %w", step, def.Name, violation.Input, violation)
	}

	return nil
}

// literalNode is one place in a value an input was written as: either a Value
// or, inside an expression, one of its sub-expressions. Reading both through
// one shape is what lets the walk below be written once.
type literalNode struct {
	value *Value
	expr  *expr.Expr
}

// nodeOf reads a Value as a node: an expression is read through its syntax tree,
// since that is where a mapping that holds one keeps its entries.
func nodeOf(value *Value) literalNode {
	if parsed := value.GetExpr(); parsed != nil {
		return literalNode{value: value, expr: parsed.GetExpr()}
	}

	return literalNode{value: value}
}

// accepted reports whether the node is a literal outright: a CEL literal value,
// which cannot hold an expression or a reference at any depth, or a constant
// inside an expression.
func (n literalNode) accepted() bool {
	if n.expr != nil {
		return n.expr.GetConstExpr() != nil
	}
	_, ok := n.value.GetKind().(*Value_Literal)

	return ok
}

// found names what a node that is not a literal is.
func (n literalNode) found() string {
	if n.expr != nil {
		return "an expression"
	}
	switch n.value.GetKind().(type) {
	case *Value_Expr:
		return "an expression"
	case *Value_SecretRef:
		return "a secret reference"
	case *Value_CredentialRef:
		return "a credential reference"
	case *Value_Structure_:
		return "a structure the check cannot read"
	default:
		return "not a literal"
	}
}

// mapping returns a node's entries when it is a mapping.
func (n literalNode) mapping() ([]literalEntry, bool) {
	if n.expr != nil {
		create := n.expr.GetStructExpr()
		if create == nil {
			return nil, false
		}
		out := make([]literalEntry, 0, len(create.GetEntries()))
		for _, entry := range create.GetEntries() {
			var key string
			switch kind := entry.GetKeyKind().(type) {
			case *expr.Expr_CreateStruct_Entry_FieldKey:
				key = kind.FieldKey
			case *expr.Expr_CreateStruct_Entry_MapKey:
				constant := kind.MapKey.GetConstExpr()
				if constant == nil {
					// A computed key: which field the value lands in is not
					// knowable, so nothing under it can be shown literal.
					return nil, false
				}
				key = constant.GetStringValue()
			}
			out = append(out, literalEntry{key: key, node: literalNode{expr: entry.GetValue()}})
		}

		return out, true
	}

	entries, ok := StructureMap(n.value)
	if !ok {
		return nil, false
	}
	out := make([]literalEntry, 0, len(entries))
	for _, key := range slices.Sorted(maps.Keys(entries)) {
		out = append(out, literalEntry{key: key, node: nodeOf(entries[key])})
	}

	return out, true
}

// elements returns a node's items when it is a list.
func (n literalNode) elements() ([]literalNode, bool) {
	if n.expr != nil {
		list := n.expr.GetListExpr()
		if list == nil || len(list.GetOptionalIndices()) > 0 {
			return nil, false
		}
		out := make([]literalNode, 0, len(list.GetElements()))
		for _, element := range list.GetElements() {
			out = append(out, literalNode{expr: element})
		}

		return out, true
	}

	structure := n.value.GetStructure()
	list, ok := structure.GetKind().(*Value_Structure_List_)
	if !ok {
		return nil, false
	}
	out := make([]literalNode, 0, len(list.List.GetValues()))
	for _, element := range list.List.GetValues() {
		out = append(out, nodeOf(element))
	}

	return out, true
}

type literalEntry struct {
	key  string
	node literalNode
}

// A literalWalker holds the state of one [LiteralFieldViolation]: the input being
// checked, for the error, and the claims found under a message, since a
// repeated message is asked about once per element.
type literalWalker struct {
	input  string
	claims map[protoreflect.MessageDescriptor][]string
}

// claimsUnder returns the claimed paths beneath a message, relative to it.
func (w *literalWalker) claimsUnder(md protoreflect.MessageDescriptor) []string {
	if paths, done := w.claims[md]; done {
		return paths
	}
	// A descriptor whose claims are malformed is refused when its task loads;
	// here, unreadable claims are read as the claims there are.
	paths, _ := LiteralInputClaims(md)
	w.claims[md] = paths

	return paths
}

// field checks the value written for a field, unwrapping a list or a map.
func (w *literalWalker) field(fd protoreflect.FieldDescriptor, n literalNode, path string, depth int) *LiteralInputError {
	if depth > MaxStructureDepth {
		return &LiteralInputError{Input: w.input, Value: path, Field: path,
			Found: fmt.Sprintf("nested deeper than %d levels", MaxStructureDepth)}
	}

	claimed := literalClaimed(fd)
	switch {
	case fd.IsList():
		return w.list(fd, claimed, n, path, depth)
	case fd.IsMap():
		return w.mapOf(fd.MapValue(), claimed, n, path, depth)
	default:
		return w.one(fd, claimed, n, path, depth)
	}
}

func (w *literalWalker) list(fd protoreflect.FieldDescriptor, claimed bool, n literalNode, path string, depth int) *LiteralInputError {
	if n.accepted() || !w.has(fd, claimed) {
		return nil
	}
	elements, ok := n.elements()
	if !ok {
		return w.refuse(fd, claimed, n, path)
	}
	for i, element := range elements {
		if err := w.one(fd, claimed, element, fmt.Sprintf("%s[%d]", path, i), depth+1); err != nil {
			return err
		}
	}

	return nil
}

func (w *literalWalker) mapOf(value protoreflect.FieldDescriptor, claimed bool, n literalNode, path string, depth int) *LiteralInputError {
	if n.accepted() || !w.has(value, claimed) {
		return nil
	}
	entries, ok := n.mapping()
	if !ok {
		return w.refuse(value, claimed, n, path)
	}
	for _, entry := range entries {
		if err := w.one(value, claimed, entry.node, path+"."+entry.key, depth+1); err != nil {
			return err
		}
	}

	return nil
}

// one checks a single value: a string that claims to be a literal, or a message
// with claims somewhere under it.
func (w *literalWalker) one(fd protoreflect.FieldDescriptor, claimed bool, n literalNode, path string, depth int) *LiteralInputError {
	if depth > MaxStructureDepth {
		return &LiteralInputError{Input: w.input, Value: path, Field: path,
			Found: fmt.Sprintf("nested deeper than %d levels", MaxStructureDepth)}
	}
	if n.accepted() || !w.has(fd, claimed) {
		return nil
	}
	if claimed {
		return w.refuse(fd, claimed, n, path)
	}

	entries, ok := n.mapping()
	if !ok {
		return w.refuse(fd, claimed, n, path)
	}
	for _, entry := range entries {
		sub := fd.Message().Fields().ByName(protoreflect.Name(entry.key))
		if sub == nil {
			continue
		}
		if err := w.field(sub, entry.node, path+"."+entry.key, depth+1); err != nil {
			return err
		}
	}

	return nil
}

// has reports whether the field, or anything under it, claims to be a literal.
func (w *literalWalker) has(fd protoreflect.FieldDescriptor, claimed bool) bool {
	if claimed {
		return true
	}
	if fd.Kind() != protoreflect.MessageKind || fd.Message() == nil {
		return false
	}

	return len(w.claimsUnder(fd.Message())) > 0
}

// refuse builds the error for a value that is not a literal where it has to
// be, naming the first claimed field at or under it.
func (w *literalWalker) refuse(fd protoreflect.FieldDescriptor, claimed bool, n literalNode, path string) *LiteralInputError {
	field := path
	if !claimed && fd.Message() != nil {
		if under := w.claimsUnder(fd.Message()); len(under) > 0 {
			field = path + "." + strings.TrimSuffix(under[0], ".")
		}
	}

	return &LiteralInputError{Input: w.input, Value: path, Field: field, Found: n.found()}
}
