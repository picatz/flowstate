package plugin

import (
	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// stripCELRules removes every protovalidate rule that carries an expression from
// a plugin-supplied file, in place, and leaves the standard rules (`string.max_len`,
// `repeated.min_items`, `in`, ...) alone.
//
// protovalidate is a second CEL evaluator in the binary: its own environment, no
// cost limit, no interrupt, `now` bound, cross-type numerics on (#1531). A plugin's
// descriptor arrives over a socket and becomes the schema `flow validate`, the
// editor and the MCP validate tool run a literal through, so a `cel` rule written
// into it would be evaluated, by that second evaluator, on the author's machine and
// on every keystroke. The descriptor is peer-authored input, and the evaluator
// cannot be bounded from here, so the expressions are not let through: a plugin that
// wants a cross-field or computed rule enforces it in its own process, where its
// own limits apply.
//
// What is cleared, wherever protovalidate would read it:
//
//   - `(buf.validate.field).cel` and `.cel_expression`, including inside a
//     repeated rule's `items` and a map rule's `keys` and `values`, which are
//     themselves field rules;
//   - `(buf.validate.message).cel` and `.cel_expression`;
//   - `(buf.validate.predefined).cel` on a field, which is how a plugin declares
//     its own named rule over an extension of a standard rule message.
//
// Only options change. The message's fields, numbers and types are untouched, so
// what an author may write is the same schema with fewer checks, and the claims
// digest can say plainly that the host enforces standard rules only.
//
// Reports how many expression-bearing rules it removed, so the caller can say so.
func stripCELRules(file *descriptorpb.FileDescriptorProto) int {
	var removed int

	for _, message := range file.GetMessageType() {
		removed += stripMessageCEL(message)
	}
	for _, extension := range file.GetExtension() {
		removed += stripFieldCEL(extension)
	}

	return removed
}

// stripMessageCEL clears one message's rules and those of everything nested in it.
//
// Recursion depth is the descriptor's own nesting, which the protobuf decoder
// already bounds when it reads the file, so no second limit is kept here.
func stripMessageCEL(message *descriptorpb.DescriptorProto) int {
	var removed int

	if options := message.GetOptions(); options != nil && proto.HasExtension(options, validate.E_Message) {
		if rules, ok := proto.GetExtension(options, validate.E_Message).(*validate.MessageRules); ok && rules != nil {
			removed += len(rules.GetCel()) + len(rules.GetCelExpression())
			rules.Cel, rules.CelExpression = nil, nil
		}
	}

	for _, field := range message.GetField() {
		removed += stripFieldCEL(field)
	}
	for _, extension := range message.GetExtension() {
		removed += stripFieldCEL(extension)
	}
	for _, nested := range message.GetNestedType() {
		removed += stripMessageCEL(nested)
	}

	return removed
}

// stripFieldCEL clears one field's rules, and the predefined rules it declares
// when it is an extension of a standard rule message.
func stripFieldCEL(field *descriptorpb.FieldDescriptorProto) int {
	options := field.GetOptions()
	if options == nil {
		return 0
	}

	var removed int

	if proto.HasExtension(options, validate.E_Field) {
		if rules, ok := proto.GetExtension(options, validate.E_Field).(*validate.FieldRules); ok {
			removed += stripFieldRulesCEL(rules)
		}
	}
	if proto.HasExtension(options, validate.E_Predefined) {
		if rules, ok := proto.GetExtension(options, validate.E_Predefined).(*validate.PredefinedRules); ok && rules != nil {
			removed += len(rules.GetCel())
			rules.Cel = nil
		}
	}

	return removed
}

// stripFieldRulesCEL clears a field-rule message, and the field rules nested in
// it: a repeated rule's items and a map rule's keys and values are rules of the
// same shape, and carry `cel` of their own.
func stripFieldRulesCEL(rules *validate.FieldRules) int {
	if rules == nil {
		return 0
	}

	removed := len(rules.GetCel()) + len(rules.GetCelExpression())
	rules.Cel, rules.CelExpression = nil, nil

	removed += stripFieldRulesCEL(rules.GetRepeated().GetItems())
	removed += stripFieldRulesCEL(rules.GetMap().GetKeys())
	removed += stripFieldRulesCEL(rules.GetMap().GetValues())

	return removed
}
