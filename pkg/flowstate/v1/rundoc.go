package flowstatev1

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// A run's answer has two readers and one of them was never served.
//
// The schema is the source of truth and the wire format is not negotiable: proto
// field names are what the RPC surface speaks, and nothing here changes a byte of
// what a server sends or a worker stores. But the shape a *specification* wants and
// the shape a `jq` expression wants are not the same shape, and every surface that
// answered with a run was handing the first one to the second. Reaching a value a step produced read
//
//	.outputs.stepValues.greet.namedValues.result.literal.stringValue
//
// where the language the file itself is written in already spells that
// `${steps.greet.result}`. Five levels of it are wrapper nouns and kind tags: two
// messages whose whole job is to hold a map, and CEL's tagged-union encoding of a
// value, in which `3` is `{"literal":{"int64Value":"3"}}` — an object, then a tag,
// then a string, for an integer.
//
// So the friendly document is a *rendering*, and `--raw` writes the schema's own
// protojson for anything that wants the wire shape (picatz/flowstate#328).
//
// It lives here rather than in cmd/flow because the CLI is no longer its only
// reader: the MCP surface answers an agent with the same document, and while this
// was a cmd/flow-private function it could not, so one run had two answers
// depending on which door a reader came through (picatz/flowstate#1553).
//
// # Why this is not a second shape that drifts
//
// A hand-written mirror of the schema is this repository's most-repeated defect, so
// this is not one. Three rules do the whole job, and every one of them is read off
// the descriptors rather than written down:
//
//  1. A message whose only field is a map is that map. [Node_Outputs] holds
//     `named_values` and nothing else; [RunOutputs] holds `values` and nothing
//     else. Neither noun tells a reader anything the field it sits in did not
//     already say. The rule is structural: gain a second field tomorrow and the
//     wrapper comes back, honestly, rather than the renderer quietly dropping half
//     a message.
//
//  2. A [Value] holding a CEL literal is that literal, in JSON. The conversion
//     is [LiteralToGo], which is the repository's one spelling of it — flowtest
//     and embed already read a recorded value through it, and this is the same
//     value read the same way rather than a fourth switch over the same union.
//
//  3. One field is renamed — the transcript's `step_values` to `steps`, so the
//     path is the one the file itself writes — listed in [runDocumentNames] and
//     pinned by a test that fails if the schema stops having it.
//
// Everything outside those three rules is protojson's own output, verbatim — not
// re-derived, but literally the bytes protojson produced, carried through by
// [projectValue] returning the raw subtree for any message that contains nothing to
// project. That is what keeps `EmitUnpopulated`, int64-as-string, enum spellings,
// timestamp formatting and NaN handling identical to every other document written
// through [MarshalSchemaJSON]: they are not reimplemented here, they are passed
// through.
//
// # What is deliberately not elided
//
// #328 also asked for empty maps and nulls to be dropped. #544 settled that the
// other way for this document and it is the right answer: a `jq` expression that
// works on one run has to work on the next, so `"runOutputs": null` on a workflow
// that declares none is a stable answer to a stable question, where a missing key
// is a second question. The eliding belongs to the human surface, which is a
// different stream and already does it: see cmd/flow's writeRun, which is a
// rendering for a person rather than a document for a program.

// runDocumentNames are the field renames, keyed by the schema's own full field
// name so a rename can never be ambiguous about which message it applies to.
//
// One entry. A Flowfile reads a step's output as `${steps.greet.result}`, so
// `steps` is the noun every author of one already has for the transcript;
// `stepValues` is the schema's noun for it, chosen to be unambiguous inside a
// message that holds two maps, which is a different job.
//
// `run_outputs` deliberately is *not* renamed, and the temptation to spell it
// `outputs` — which is what a Flowfile calls the block that declares them — is
// where a second name for one thing would have got in. [GetResponse] already
// has an `outputs` field: the transcript, in its oneof. So a rename would have
// produced `.outputs` meaning the answer in the bare run document and the
// transcript in `-o json`, and a reader moving between the two would have to know
// which document they were holding to know what `.outputs` meant. That is the
// deciphering #544 is about, reintroduced by the change meant to remove it.
// `runOutputs` names the same thing in both, and it is the schema's own word.
//
// Keyed by full name rather than by bare field name for the same reason: a bare
// `step_values` would apply to any message that ever gains one.
// [TestRunDocumentRenamesResolve] checks that each key still names a field and
// that no rename lands on a name a sibling field already renders as.
var runDocumentNames = map[protoreflect.FullName]string{
	"flowstate.v1.Workflow.StepOutputs.step_values": "steps",
}

// celValueName is CEL's own value message, which is what a literal actually is.
const celValueName protoreflect.FullName = "google.api.expr.v1alpha1.Value"

// flowValueName is the schema's value wrapper, a oneof over "a literal", "an
// expression", "an error", "a secret reference" and "a structure".
const flowValueName protoreflect.FullName = "flowstate.v1.Value"

// flowstatePackage is the only package whose messages this rendering enters. See
// [projectionDecisions.walk] for why that is a safety rule.
const flowstatePackage protoreflect.FullName = "flowstate.v1"

// marshalRunDocument renders a run's answer in the vocabulary the DSL taught.
//
// raw writes protojson unchanged, which is what `--raw` asks for: the exact wire
// shape, for a consumer that wants the schema's nouns because it is generating
// against the schema.
// MarshalSchemaJSON renders a message the way the schema describes it.
//
// protojson rather than encoding/json, so the field names are the schema's and an
// enum is its name rather than the integer behind it — `"STATUS_COMPLETED"` reads,
// and survives a renumbering that `4` would not.
//
// EmitUnpopulated is deliberate: a consumer indexing `.closeTime` on a run that has
// not finished should find null rather than a missing key, because the two are the
// same question and only one of them is answerable without knowing the schema.
func MarshalSchemaJSON(message proto.Message, indent bool) ([]byte, error) {
	options := protojson.MarshalOptions{EmitUnpopulated: true}
	if indent {
		options.Indent = "  "
	}

	return options.Marshal(message)
}

// MarshalRunDocument is the one rendering of a run document that leaves this
// process.
//
// Exported so the MCP surface answers with the same bytes `--output json`
// prints rather than the schema's own protojson, which is a different dialect of
// the same run: `2` as `{"literal":{"int64Value":"2"}}`, `steps` as
// `stepValues.<id>.namedValues`. An agent reading one and a person reading the
// other could not share a jq filter, a schema, or an example (#1553).
//
// raw asks for the schema's protojson instead, for a reader that speaks it.
func MarshalRunDocument(message proto.Message, indent, raw bool) ([]byte, error) {
	encoded, err := MarshalSchemaJSON(message, false)
	if err != nil {
		return nil, err
	}

	if raw {
		if !indent {
			return encoded, nil
		}

		var buffer bytes.Buffer
		if err := json.Indent(&buffer, encoded, "", "  "); err != nil {
			return nil, fmt.Errorf("indenting the answer: %w", err)
		}

		return buffer.Bytes(), nil
	}

	// Decoded into an order-preserving tree rather than a map, so a field that is
	// carried through untouched comes out where protojson put it: in field-number
	// order, which is the order the schema declares and the order every other
	// document this renderer writes already uses.
	tree, err := decodeOrdered(encoded)
	if err != nil {
		return nil, fmt.Errorf("reading the answer back: %w", err)
	}

	return encodeJSON(projectValue(message.ProtoReflect(), tree), indent)
}

// projectValue renders one message, given the protojson tree protojson produced
// for it.
//
// The two travel together because each knows something the other does not. The
// tree carries every encoding decision protojson made — which fields it emitted,
// how it spelled an enum, that an int64 is a string — and the message carries the
// descriptors and the actual values the projection needs. Neither alone is enough,
// and reconstructing either from the other is how the two would drift.
//
// Returns raw untouched whenever the message contains nothing to project, which is
// most of them, and is what makes "unchanged" mean the literal bytes rather than a
// second encoder that happens to agree today.
func projectValue(message protoreflect.Message, raw any) any {
	if message == nil || !message.IsValid() {
		return raw
	}

	descriptor := message.Descriptor()

	switch descriptor.FullName() {
	case celValueName:
		if projected, ok := projectLiteral(message); ok {
			return projected
		}

		return raw

	case flowValueName:
		// Only the literal arm is a value. An error, a secret reference, an
		// unevaluated expression and a structure are all *about* a value rather
		// than being one, and flattening them would make `{"message": "..."}`
		// from a failure indistinguishable from a map somebody computed. Those
		// stay in the schema's own spelling, where the arm names itself.
		literal, ok := message.Interface().(*Value).GetKind().(*Value_Literal)
		if !ok {
			return raw
		}

		// The whole [Value] on the way out rather than the literal's own
		// subtree, because a value this cannot spell has to stay recognizable as a
		// value: `{"literal":{"doubleValue":"NaN"}}` says what it is, where the
		// inner `{"doubleValue":"NaN"}` alone is indistinguishable from a map some
		// workflow computed.
		projected, ok := projectLiteral(literal.Literal.ProtoReflect())
		if !ok {
			return raw
		}

		return projected
	}

	if !projects(descriptor) {
		return raw
	}

	if field, ok := soleMapField(descriptor); ok {
		return projectField(field, message.Get(field), rawFieldValue(raw, field))
	}

	object, ok := raw.(*orderedObject)
	if !ok {
		return raw
	}

	projected := &orderedObject{}
	for _, key := range object.keys {
		field := descriptor.Fields().ByJSONName(key)
		if field == nil {
			// A key protojson wrote that no field claims. Nothing produces one
			// today; carrying it through unchanged is the answer that cannot lose
			// data if something ever does.
			projected.set(key, object.get(key))
			continue
		}

		name := key
		if renamed, ok := runDocumentNames[field.FullName()]; ok {
			name = renamed
		}

		projected.set(name, projectField(field, message.Get(field), object.get(key)))
	}

	return projected
}

// projectField renders one field's value, pairing the protoreflect value with the
// protojson subtree the same way [projectValue] pairs a message with its tree.
func projectField(field protoreflect.FieldDescriptor, value protoreflect.Value, raw any) any {
	switch {
	case field.IsMap():
		object, ok := raw.(*orderedObject)
		if !ok {
			return raw
		}

		// Paired by the JSON spelling of each key, which is how protojson wrote
		// them. Sorted afterwards is not needed: the tree already carries
		// protojson's order and this rebuilds it in that order.
		entries := make(map[string]protoreflect.Value, object.len())
		value.Map().Range(func(key protoreflect.MapKey, entry protoreflect.Value) bool {
			entries[mapKeyJSON(key)] = entry
			return true
		})

		projected := &orderedObject{}
		for _, key := range object.keys {
			entry, ok := entries[key]
			if !ok {
				projected.set(key, object.get(key))
				continue
			}

			projected.set(key, projectElement(field.MapValue(), entry, object.get(key)))
		}

		return projected

	case field.IsList():
		items, ok := raw.([]any)
		if !ok {
			return raw
		}

		list := value.List()
		if list.Len() != len(items) {
			return raw
		}

		projected := make([]any, 0, len(items))
		for i := range items {
			projected = append(projected, projectElement(field, list.Get(i), items[i]))
		}

		return projected

	default:
		return projectElement(field, value, raw)
	}
}

// projectElement renders a single (non-repeated, non-map) value of a field.
func projectElement(field protoreflect.FieldDescriptor, value protoreflect.Value, raw any) any {
	if field.Kind() != protoreflect.MessageKind && field.Kind() != protoreflect.GroupKind {
		return raw
	}

	return projectValue(value.Message(), raw)
}

// projectLiteral renders a CEL literal as the value it is.
//
// Through [LiteralToGo], which is the repository's one conversion from a
// recorded literal to a plain Go value, rather than a switch of this file's own
// over the same union — a value with one meaning written down twice is the defect
// CLAUDE.md names first, and this one is already written down once.
//
// Anything that conversion refuses, and anything JSON cannot hold, is reported as
// not spellable and the caller keeps protojson's subtree. A type value, an enum, a
// NaN: each is rare, each is nothing a `jq` reader has a better spelling for, and
// each is honestly reported by leaving the schema's own encoding in place rather
// than by guessing.
func projectLiteral(message protoreflect.Message) (any, bool) {
	literal, ok := message.Interface().(*expr.Value)
	if !ok {
		return nil, false
	}

	native, err := LiteralToGo(literal)
	if err != nil {
		return nil, false
	}

	return jsonRepresentable(native)
}

// jsonRepresentable reports whether a converted literal can be written as JSON,
// and rewrites the two numeric kinds JSON spells differently than Go does.
//
// int64 and uint64 become [json.Number] rather than a float, because a run that
// counted 9007199254740993 things should say so: encoding/json writes a
// json.Number's digits verbatim, where a float64 would round it. This is the one
// place the projection deliberately differs from protojson, which writes a 64-bit
// *schema field* as a string — a value a workflow computed is a number, and
// `.outputs.hosts_placed == 3` is the expression somebody writes.
//
// NaN and the infinities have no JSON spelling at all, so they are refused here and
// the caller keeps protojson's `"NaN"`.
func jsonRepresentable(native any) (any, bool) {
	switch value := native.(type) {
	case nil, bool, string, []byte:
		return native, true

	case int64:
		return json.Number(strconv.FormatInt(value, 10)), true

	case uint64:
		return json.Number(strconv.FormatUint(value, 10)), true

	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, false
		}

		return native, true

	case []any:
		projected := make([]any, 0, len(value))
		for _, element := range value {
			converted, ok := jsonRepresentable(element)
			if !ok {
				return nil, false
			}
			projected = append(projected, converted)
		}

		return projected, true

	case map[string]any:
		projected := make(map[string]any, len(value))
		for key, element := range value {
			converted, ok := jsonRepresentable(element)
			if !ok {
				return nil, false
			}
			projected[key] = converted
		}

		return projected, true

	default:
		return nil, false
	}
}

// soleMapField answers rule 1: a message whose only field is a map is that map.
func soleMapField(descriptor protoreflect.MessageDescriptor) (protoreflect.FieldDescriptor, bool) {
	fields := descriptor.Fields()
	if fields.Len() != 1 {
		return nil, false
	}

	field := fields.Get(0)
	if !field.IsMap() {
		return nil, false
	}

	return field, true
}

// rawFieldValue pulls one field's subtree out of a message's protojson tree.
//
// Absent where protojson did not emit the field, which for a collapsed wrapper
// means the wrapper was empty: an empty map, which is what the caller then renders.
func rawFieldValue(raw any, field protoreflect.FieldDescriptor) any {
	object, ok := raw.(*orderedObject)
	if !ok {
		return raw
	}

	if value := object.get(field.JSONName()); value != nil {
		return value
	}

	return &orderedObject{}
}

// mapKeyJSON spells a map key the way protojson spells it, which is how the two
// halves of a map field are paired back up.
func mapKeyJSON(key protoreflect.MapKey) string {
	switch value := key.Interface().(type) {
	case string:
		return value
	case bool:
		return strconv.FormatBool(value)
	case int32:
		return strconv.FormatInt(int64(value), 10)
	case int64:
		return strconv.FormatInt(value, 10)
	case uint32:
		return strconv.FormatUint(uint64(value), 10)
	case uint64:
		return strconv.FormatUint(value, 10)
	default:
		return key.String()
	}
}

// projects reports whether a message contains anything this rendering touches.
//
// The whole of the fidelity guarantee rests here. A message that answers false is
// returned as the bytes protojson wrote, untouched and unvisited, so there is no
// second encoder to disagree with the first about a field this file has never heard
// of. Answering it needs the transitive question rather than the local one, because
// a [Value] two messages down is still a value to project.
//
// Memoized per message type, and cycle-safe: the schema is recursive (a Node holds
// Nodes), so a type already being decided answers false to itself and the enclosing
// decision stands on its other fields.
func projects(descriptor protoreflect.MessageDescriptor) bool {
	return projectionCache.decide(descriptor)
}

// projectionDecisions memoizes [projects]. A cold decision walks the schema; every
// document rendered after it reuses that decision.
//
// One process may render more than one document concurrently — `flow run` writing
// a transcript while a test in the same binary renders another — so the two maps
// backing the memoization are guarded by mu rather than left bare. Unguarded, two
// goroutines racing to decide the same message type is a concurrent map write:
// TestAPipedRunStillWritesTheTranscript and TestTheRenderingStaysOutOfWellKnownTypes
// running in parallel is exactly that race, caught by -race rather than by anything
// that inspects the answer.
type projectionDecisions struct {
	mu       sync.Mutex
	decided  map[protoreflect.FullName]bool
	deciding map[protoreflect.FullName]bool
}

var projectionCache = &projectionDecisions{
	decided:  map[protoreflect.FullName]bool{},
	deciding: map[protoreflect.FullName]bool{},
}

// decide is the locked entry point: it takes mu once and holds it for the whole
// recursive walk, since [projectionDecisions.walk] calls decideLocked on every
// message-typed field rather than re-entering decide, and a [sync.Mutex] is not
// reentrant.
func (d *projectionDecisions) decide(descriptor protoreflect.MessageDescriptor) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.decideLocked(descriptor)
}

func (d *projectionDecisions) decideLocked(descriptor protoreflect.MessageDescriptor) bool {
	name := descriptor.FullName()

	if answer, ok := d.decided[name]; ok {
		return answer
	}

	if d.deciding[name] {
		// A cycle. False is the answer that terminates and cannot be wrong on its
		// own: whichever type in the cycle actually holds something to project
		// answers true from its own fields, and every type reaching it inherits
		// that through the branch that is not the cycle.
		return false
	}

	d.deciding[name] = true
	answer := d.walk(descriptor)
	delete(d.deciding, name)
	d.decided[name] = answer

	return answer
}

func (d *projectionDecisions) walk(descriptor protoreflect.MessageDescriptor) bool {
	switch descriptor.FullName() {
	case celValueName, flowValueName:
		return true
	}

	// Nothing outside this repository's own schema is rendered, and that is a
	// safety rule rather than a scoping one. protojson renders several well-known
	// types as something other than an object — a Timestamp is a string, a
	// google.protobuf.Struct is a bare JSON object with no field names of its own —
	// so a structural rule applied to one of them would be reading a shape that is
	// not there. Struct is the sharp case: it is a message whose only field is a
	// map, which the collapse rule matches exactly, and its protojson form has no
	// `fields` key to collapse *from*, so the whole of its contents would be
	// replaced with `{}`. Nothing in flowstate.v1 imports Struct today; this is
	// what keeps the day somebody does from being a silent loss of somebody's data.
	if descriptor.ParentFile().Package() != flowstatePackage {
		return false
	}

	if _, ok := soleMapField(descriptor); ok {
		return true
	}

	fields := descriptor.Fields()
	for i := range fields.Len() {
		field := fields.Get(i)

		if _, ok := runDocumentNames[field.FullName()]; ok {
			return true
		}

		if field.IsMap() {
			if value := field.MapValue(); value.Message() != nil && d.decideLocked(value.Message()) {
				return true
			}

			continue
		}

		if field.Message() != nil && d.decideLocked(field.Message()) {
			return true
		}
	}

	return false
}

// encodeJSON writes the projected tree.
//
// HTML escaping off, because encoding/json's default would turn a URL's `&` into
// `&` in a document protojson writes plainly, and a run's answer is full of
// URLs. Indented for `-o json`, which a person reads as often as a program does,
// and compact for everything else.
func encodeJSON(tree any, indent bool) ([]byte, error) {
	var buffer bytes.Buffer

	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if indent {
		encoder.SetIndent("", "  ")
	}

	if err := encoder.Encode(tree); err != nil {
		return nil, fmt.Errorf("writing the answer: %w", err)
	}

	// Encode appends a newline; every caller writes its own.
	return bytes.TrimRight(buffer.Bytes(), "\n"), nil
}

// orderedObject is a JSON object that remembers the order its keys arrived in.
//
// encoding/json sorts a map's keys, which would reorder every document this
// renders — including the parts it carries through untouched, where the whole
// promise is that they are unchanged. protojson writes fields in field-number
// order, which is the schema's declaration order, so keeping that order is keeping
// the document a reader already knows.
type orderedObject struct {
	keys   []string
	values map[string]any
}

func (o *orderedObject) set(key string, value any) {
	if o.values == nil {
		o.values = map[string]any{}
	}

	if _, ok := o.values[key]; !ok {
		o.keys = append(o.keys, key)
	}

	o.values[key] = value
}

func (o *orderedObject) get(key string) any { return o.values[key] }

func (o *orderedObject) len() int { return len(o.keys) }

// MarshalJSON writes the object in key order.
//
// Each value goes through an encoder with HTML escaping off, matching
// [encodeJSON]: encoding/json compacts a MarshalJSON result under the *outer*
// encoder's escaping setting, so a nested value encoded with escaping on would keep
// its `&` through a compaction that has escaping off.
func (o *orderedObject) MarshalJSON() ([]byte, error) {
	var buffer bytes.Buffer

	buffer.WriteByte('{')

	for i, key := range o.keys {
		if i > 0 {
			buffer.WriteByte(',')
		}

		name, err := encodeJSON(key, false)
		if err != nil {
			return nil, err
		}

		value, err := encodeJSON(o.values[key], false)
		if err != nil {
			return nil, err
		}

		buffer.Write(name)
		buffer.WriteByte(':')
		buffer.Write(value)
	}

	buffer.WriteByte('}')

	return buffer.Bytes(), nil
}

// decodeOrdered reads protojson's output into a tree that remembers key order and
// keeps every number exactly as it was written.
//
// json.Number rather than float64 throughout, because the tree is re-encoded and a
// number that made a round trip through a float64 is a number this renderer
// changed. A
// timestamp's nanoseconds and a 64-bit count are both in that category.
func decodeOrdered(encoded []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()

	value, err := decodeOrderedValue(decoder)
	if err != nil {
		return nil, err
	}

	return value, nil
}

func decodeOrderedValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}

	return decodeOrderedToken(decoder, token)
}

func decodeOrderedToken(decoder *json.Decoder, token json.Token) (any, error) {
	delimiter, ok := token.(json.Delim)
	if !ok {
		return token, nil
	}

	switch delimiter {
	case '{':
		object := &orderedObject{}
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return nil, err
			}

			name, ok := key.(string)
			if !ok {
				return nil, fmt.Errorf("object key %v is not a string", key)
			}

			value, err := decodeOrderedValue(decoder)
			if err != nil {
				return nil, err
			}

			object.set(name, value)
		}

		if _, err := decoder.Token(); err != nil { // the closing brace
			return nil, err
		}

		return object, nil

	case '[':
		list := []any{}
		for decoder.More() {
			value, err := decodeOrderedValue(decoder)
			if err != nil {
				return nil, err
			}

			list = append(list, value)
		}

		if _, err := decoder.Token(); err != nil { // the closing bracket
			return nil, err
		}

		return list, nil

	default:
		return nil, fmt.Errorf("unexpected %s in the answer", strings.TrimSpace(string(delimiter)))
	}
}
