package flowfile

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	yaml "github.com/goccy/go-yaml"
	"github.com/google/cel-go/cel"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Writing a workflow back out is the inverse of parsing it, and has to stay that
// way: `flow fmt` and the language server both rely on Marshal(Unmarshal(x))
// meaning the same thing as x. The document is assembled as ordered mappings so
// that keys come out in the order a reader expects rather than alphabetically, and
// so that an empty mapping stays distinguishable from an absent one.

// Marshal writes a workflow as a Flowfile.
//
// It reports an error rather than writing a document that would read back as
// something else. That happens for a value the DSL cannot express: a literal
// string containing ${, which would be read as an expression, and an expression
// whose source used a macro, which cel-go cannot write back.
func Marshal(wf *v1.Workflow) ([]byte, error) {
	// The edition is a property of a *file* and the schema deliberately has no field
	// for it, so there is nothing to round-trip — but a document without one is a
	// document this build refuses, and Marshal's whole contract is that its output
	// reads back as the same workflow.
	//
	// So it is written as the current edition, which is not a guess: this build
	// compiles one grammar, and anything it writes is in that grammar by construction.
	// Omitting it would leave whoever writes `flow fmt` with a formatter that quietly
	// invalidates every file it touches.
	doc := yaml.MapSlice{
		{Key: "edition", Value: CurrentEdition},
		{Key: "name", Value: wf.GetName()},
	}

	if wf.Description != nil {
		doc = append(doc, yaml.MapItem{Key: "description", Value: wf.GetDescription()})
	}

	// What the run takes, above everything that reads it — the order the parser
	// reads these in, and the order a reader meets them in.
	if len(wf.GetDeclaredInputs()) > 0 {
		inputs, err := declaredInputsToYAML(wf.GetDeclaredInputs())
		if err != nil {
			return nil, err
		}
		doc = append(doc, yaml.MapItem{Key: "inputs", Value: inputs})
	}

	// How it starts on its own, in the same position the parser reads it and an
	// author writes it: after what the run takes, above what it does.
	if triggers := wf.GetTriggers(); triggers != nil {
		written, err := triggersToYAML(triggers)
		if err != nil {
			return nil, err
		}
		doc = append(doc, yaml.MapItem{Key: "triggers", Value: written})
	}

	// Written before steps, which is where an author writes it and so where a reader
	// looks for it: a value every step can reach belongs above the steps that reach it.
	if len(wf.GetVars()) > 0 {
		vars, err := varsToYAML(wf.GetVars())
		if err != nil {
			return nil, err
		}
		doc = append(doc, yaml.MapItem{Key: "vars", Value: vars})
	}

	// A workflow with no steps is not a usable one — [Validate] says so — but
	// writing `steps: []` for it would be worse than leaving the key out: reading
	// that back is an author asking for an empty list, which is a different
	// mistake and gets a different diagnostic.
	if len(wf.GetSteps()) > 0 {
		steps, err := stepsToYAML(wf.GetSteps())
		if err != nil {
			return nil, err
		}
		doc = append(doc, yaml.MapItem{Key: "steps", Value: steps})
	}

	// Below the steps, because that is what they are written against: an output is
	// evaluated once every step has finished, and reads as the answer at the bottom
	// of the file rather than as a promise at the top of it.
	if len(wf.GetDeclaredOutputs()) > 0 {
		outputs, err := declaredOutputsToYAML(wf.GetDeclaredOutputs())
		if err != nil {
			return nil, err
		}
		doc = append(doc, yaml.MapItem{Key: "outputs", Value: outputs})
	}

	return yaml.Marshal(doc)
}

// stepsToYAML writes a list of steps, recursing through nested control flow so
// that a loop body and a parallel branch come back out as they went in.
func stepsToYAML(nodes []*v1.Node) ([]any, error) {
	steps := make([]any, 0, len(nodes))
	for _, node := range nodes {
		step, err := stepToYAML(node)
		if err != nil {
			return nil, err
		}
		steps = append(steps, step)
	}
	return steps, nil
}

// stepToYAML writes one step.
func stepToYAML(node *v1.Node) (yaml.MapSlice, error) {
	step := yaml.MapSlice{{Key: "id", Value: node.GetId()}}

	// Written second, right under the id, because prose about a step is read
	// before the mechanics of it. Only when set, so an absent description and an
	// empty one stay distinguishable through a round trip.
	if node.Description != nil {
		step = append(step, yaml.MapItem{Key: "description", Value: node.GetDescription()})
	}

	if condition := node.GetCondition(); condition != nil {
		value, err := exprValueToYAML(condition)
		if err != nil {
			return nil, fmt.Errorf("step %q if: %w", node.GetId(), err)
		}
		step = append(step, yaml.MapItem{Key: "if", Value: value})
	}

	// Above the policy and the work, because a name is read before its uses. The same
	// order the parser accepts them in, so `flow fix` moves nothing that was already
	// where it belongs.
	if vars := node.GetVars(); len(vars) > 0 {
		value, err := varsToYAML(vars)
		if err != nil {
			return nil, fmt.Errorf("step %q vars: %w", node.GetId(), err)
		}
		step = append(step, yaml.MapItem{Key: "vars", Value: value})
	}

	if policy := node.GetPolicy(); policy != nil {
		if timeout := policy.GetTimeout(); timeout != nil {
			step = append(step, yaml.MapItem{Key: "timeout", Value: durationToYAML(timeout)})
		}
		if retry := policy.GetRetry(); retry != nil {
			step = append(step, yaml.MapItem{Key: "retry", Value: retryToYAML(retry)})
		}
		if policy.GetContinueOnError() {
			step = append(step, yaml.MapItem{Key: "continue_on_error", Value: true})
		}
	}

	switch kind := node.GetKind().(type) {
	case *v1.Node_Task:
		inputs, err := taskInputsToYAML(kind.Task)
		if err != nil {
			return nil, fmt.Errorf("step %q: %w", node.GetId(), err)
		}
		step = append(step, yaml.MapItem{Key: kind.Task.GetName(), Value: inputs})

	case *v1.Node_ForEach:
		loop, err := forEachToYAML(kind.ForEach)
		if err != nil {
			return nil, fmt.Errorf("step %q for_each: %w", node.GetId(), err)
		}
		step = append(step, yaml.MapItem{Key: "for_each", Value: loop})

	case *v1.Node_Parallel:
		branches := make([]any, 0, len(kind.Parallel.GetBranches()))
		for i, branch := range kind.Parallel.GetBranches() {
			steps, err := stepsToYAML(branch.GetSteps())
			if err != nil {
				return nil, fmt.Errorf("step %q parallel branch %d: %w", node.GetId(), i+1, err)
			}
			branches = append(branches, yaml.MapSlice{{Key: "steps", Value: steps}})
		}
		step = append(step, yaml.MapItem{Key: "parallel", Value: branches})

	case *v1.Node_Wait:
		key, value, err := waitToYAML(kind.Wait)
		if err != nil {
			return nil, fmt.Errorf("step %q: %w", node.GetId(), err)
		}
		step = append(step, yaml.MapItem{Key: key, Value: value})

	case *v1.Node_Call:
		// Written as the path an author wrote, never as the callee it resolved
		// to. The compiled node carries the whole callee inline (see [v1.Call]'s
		// doc on why), but that is what compiling means, not what writing means
		// — round-tripping the embedded copy back out would turn `call:
		// ./tenant.yaml` into the tenant workflow's entire body pasted in place,
		// which `flow fmt` on any file with a call would silently rewrite into
		// something enormous and no longer pointing at the file it named.
		source := kind.Call.GetSource()
		if source == "" {
			return nil, fmt.Errorf("step %q: call has no source path to write", node.GetId())
		}
		step = append(step, yaml.MapItem{Key: "call", Value: source})

		if args := kind.Call.GetArguments(); len(args) > 0 {
			value, err := varsToYAML(args)
			if err != nil {
				return nil, fmt.Errorf("step %q with: %w", node.GetId(), err)
			}
			step = append(step, yaml.MapItem{Key: "with", Value: value})
		}

	default:
		return nil, fmt.Errorf("step %q: has no %s", node.GetId(), stepKindList())
	}

	// Last, under the work it undoes, because that is where a reader wants it: the
	// step says what it does and then how to take it back. It is also the order the
	// parser accepts, so `flow fix` moves nothing already written this way.
	if undo := node.GetUndo(); undo != nil {
		task := undo.GetTask()
		if task.GetName() == "" {
			return nil, fmt.Errorf("step %q undo: compensation has no task", node.GetId())
		}
		inputs, err := taskInputsToYAML(task)
		if err != nil {
			return nil, fmt.Errorf("step %q undo: %w", node.GetId(), err)
		}
		step = append(step, yaml.MapItem{
			Key:   "undo",
			Value: yaml.MapSlice{{Key: task.GetName(), Value: inputs}},
		})
	}

	return step, nil
}

// taskInputsToYAML writes a task's inputs, which under the step's own name is
// the whole of what a task step is.
//
// An empty mapping is written for a task with no inputs rather than nothing at
// all. `echo:` with no value reads back as the same workflow — but `echo: {}`
// says the inputs are empty on purpose, and a key with nothing after it reads as
// an unfinished line. A formatter should not produce something that looks like a
// mistake.
func taskInputsToYAML(task *v1.Task) (yaml.MapSlice, error) {
	// A task used to carry a description, and this function refused one rather than
	// dropping it: silently discarding a field would make Marshal(Unmarshal(x))
	// mean something other than x, which is the property this file exists to hold.
	// The field is gone from the schema now, so there is nothing left to refuse —
	// prose about a step is `Node.description`, and `flow fix` moves it there when
	// it rewrites an older file.

	inputs := yaml.MapSlice{}
	// Input names come from a protobuf map, whose order is not defined, so they are
	// sorted: the same workflow has to produce the same document every time for a
	// formatter to be usable or a diff to be readable.
	for _, name := range slices.Sorted(maps.Keys(task.GetInputs())) {
		value, err := inputValueToYAML(task.GetInputs()[name])
		if err != nil {
			return nil, fmt.Errorf("input %q: %w", name, err)
		}
		inputs = append(inputs, yaml.MapItem{Key: name, Value: value})
	}

	return inputs, nil
}

// varsToYAML writes a `vars:` mapping.
//
// Sorted, because the names come from a protobuf map and a formatter that emitted them
// in a different order each run would make every `flow fix` a diff.
//
// It exists at all because Marshal is the inverse of Unmarshal and `flow fix` rewrites
// files through it: a block this did not write would be a block `flow fix` *deleted*.
// TestExamplesCompile caught exactly that, on the example added with the feature.
func varsToYAML(vars map[string]*v1.Value) (yaml.MapSlice, error) {
	out := yaml.MapSlice{}
	for _, name := range slices.Sorted(maps.Keys(vars)) {
		value, err := inputValueToYAML(vars[name])
		if err != nil {
			return nil, fmt.Errorf("var %q: %w", name, err)
		}
		out = append(out, yaml.MapItem{Key: name, Value: value})
	}

	return out, nil
}

// forEachToYAML writes a loop and its body.
func forEachToYAML(loop *v1.ForEach) (yaml.MapSlice, error) {
	items, err := exprValueToYAML(loop.GetItems())
	if err != nil {
		return nil, fmt.Errorf("items: %w", err)
	}

	out := yaml.MapSlice{{Key: "items", Value: items}}
	if iterator := loop.GetIterator(); iterator != "" {
		out = append(out, yaml.MapItem{Key: "as", Value: iterator})
	}
	if maxParallel := loop.GetMaxParallel(); maxParallel != 0 {
		out = append(out, yaml.MapItem{Key: "max_parallel", Value: maxParallel})
	}

	steps, err := stepsToYAML(loop.GetBody())
	if err != nil {
		return nil, err
	}
	return append(out, yaml.MapItem{Key: "steps", Value: steps}), nil
}

// retryToYAML writes a retry policy, leaving out what was never set so that the
// engine's defaults keep applying to it.
func retryToYAML(retry *v1.RetryPolicy) yaml.MapSlice {
	out := yaml.MapSlice{}
	if attempts := retry.GetMaxAttempts(); attempts != 0 {
		out = append(out, yaml.MapItem{Key: "attempts", Value: attempts})
	}
	if interval := retry.GetInitialInterval(); interval != nil {
		out = append(out, yaml.MapItem{Key: "interval", Value: durationToYAML(interval)})
	}
	if backoff := retry.GetBackoffCoefficient(); backoff != 0 {
		out = append(out, yaml.MapItem{Key: "backoff", Value: backoff})
	}
	if maxInterval := retry.GetMaxInterval(); maxInterval != nil {
		out = append(out, yaml.MapItem{Key: "max_interval", Value: durationToYAML(maxInterval)})
	}
	return out
}

// durationToYAML writes a duration the way the DSL reads one.
func durationToYAML(d *durationpb.Duration) string {
	return d.AsDuration().String()
}

// inputValueToYAML writes a task input.
//
// An expression is written fenced. The fence is optional when reading a field the
// schema types as an expression, but an input can be either, so writing it is what
// makes the document mean what it meant.
func inputValueToYAML(value *v1.Value) (any, error) {
	switch kind := value.GetKind().(type) {
	case *v1.Value_Expr:
		text, err := exprToText(kind.Expr)
		if err != nil {
			return nil, err
		}
		return fenceOpen + text + fenceClose, nil
	case *v1.Value_Literal:
		return literalToYAML(kind.Literal)
	case *v1.Value_SecretRef:
		return secretRefToDSL(kind.SecretRef)
	case *v1.Value_Structure_:
		return structureToYAML(kind.Structure)
	default:
		return nil, fmt.Errorf("cannot be written as YAML: %w", value.Error())
	}
}

// structureToYAML writes a list or a mapping whose entries are values.
//
// Each entry is written the way a whole input is, which is what makes the round
// trip a fixed point: a reference inside comes back out as `${secret('...')}`, and
// reading that document produces this structure again. There is no other spelling —
// a structure exists precisely because its entries could not be flattened into one
// expression.
func structureToYAML(structure *v1.Value_Structure) (any, error) {
	switch kind := structure.GetKind().(type) {
	case *v1.Value_Structure_List_:
		values := kind.List.GetValues()
		out := make([]any, 0, len(values))
		for i, element := range values {
			written, err := inputValueToYAML(element)
			if err != nil {
				return nil, fmt.Errorf("element %d: %w", i, err)
			}
			out = append(out, written)
		}
		return out, nil

	case *v1.Value_Structure_Map_:
		entries := kind.Map.GetEntries()
		// A MapSlice in sorted key order, for the two reasons everything else here
		// is ordered: a protobuf map has no order of its own, and a file that
		// rewrites its own keys into a different arrangement on every `flow fmt` is
		// a diff nobody made.
		out := make(yaml.MapSlice, 0, len(entries))
		for _, name := range slices.Sorted(maps.Keys(entries)) {
			written, err := inputValueToYAML(entries[name])
			if err != nil {
				return nil, fmt.Errorf("key %q: %w", name, err)
			}
			out = append(out, yaml.MapItem{Key: name, Value: written})
		}
		return out, nil

	default:
		return nil, fmt.Errorf("a structure is a list or a mapping, and this is neither")
	}
}

// exprValueToYAML writes a value in a field the schema types as an expression: a
// step's condition, or a loop's items.
//
// A literal is written as itself, which is unambiguous because a non-string YAML
// value in one of these fields is read back as a literal. A literal string is the
// one thing that cannot be written, since a string there is expression source.
func exprValueToYAML(value *v1.Value) (any, error) {
	if literal := value.GetLiteral(); literal != nil {
		if _, isString := literal.GetKind().(*expr.Value_StringValue); isString {
			return nil, fmt.Errorf(
				"is the literal string %q, which cannot be written here: a string in this field is read as an expression",
				literal.GetStringValue())
		}
	}
	if reference := value.GetSecretRef(); reference != nil {
		// The compiler refuses one here, so this is a specification built by hand.
		// Writing it would produce a Flowfile that does not compile.
		return nil, fmt.Errorf("is a secret reference, which cannot be written here: %s", notEvaluableHelp)
	}
	return inputValueToYAML(value)
}

// exprToText renders an expression back into source.
func exprToText(parsed *expr.ParsedExpr) (string, error) {
	text, err := cel.AstToString(cel.ParsedExprToAst(parsed))
	if err != nil {
		// cel-go refuses to write back a comprehension, because the parsed form no
		// longer records the macro it came from. Saying so is better than writing
		// the expanded form, which is valid CEL that no author would recognize.
		return "", fmt.Errorf("expression cannot be written back as source, "+
			"which happens when it was written with a macro such as a comprehension: %w", err)
	}
	return text, nil
}

// literalToYAML writes a literal value, keeping the order of a map's entries so
// that reading the document back produces the same literal.
func literalToYAML(literal *expr.Value) (any, error) {
	switch kind := literal.GetKind().(type) {
	case *expr.Value_StringValue:
		if containsFence(kind.StringValue) {
			return nil, fmt.Errorf(
				"is the literal string %q, which cannot be written: ${ marks an expression, so it would be read back as one",
				kind.StringValue)
		}
		return kind.StringValue, nil
	case *expr.Value_Int64Value:
		return kind.Int64Value, nil
	case *expr.Value_Uint64Value:
		return kind.Uint64Value, nil
	case *expr.Value_DoubleValue:
		return kind.DoubleValue, nil
	case *expr.Value_BoolValue:
		return kind.BoolValue, nil
	case *expr.Value_NullValue:
		return nil, nil
	case *expr.Value_ListValue:
		out := make([]any, 0, len(kind.ListValue.GetValues()))
		for i, elem := range kind.ListValue.GetValues() {
			value, err := literalToYAML(elem)
			if err != nil {
				return nil, fmt.Errorf("element %d: %w", i, err)
			}
			out = append(out, value)
		}
		return out, nil
	case *expr.Value_MapValue:
		out := make(yaml.MapSlice, 0, len(kind.MapValue.GetEntries()))
		for _, entry := range kind.MapValue.GetEntries() {
			key, isString := entry.GetKey().GetKind().(*expr.Value_StringValue)
			if !isString {
				return nil, fmt.Errorf("map keys must be strings, but one is %s", literalKind(entry.GetKey()))
			}
			value, err := literalToYAML(entry.GetValue())
			if err != nil {
				return nil, fmt.Errorf("key %q: %w", key.StringValue, err)
			}
			out = append(out, yaml.MapItem{Key: key.StringValue, Value: value})
		}
		return out, nil
	default:
		return nil, fmt.Errorf("cannot be written as YAML: it is %s", literalKind(literal))
	}
}

// literalKind names a literal's type the way a Flowfile author would, for a message
// about one that is not what was wanted — whether that is a value Marshal cannot
// write or an input whose type a task does not accept.
func literalKind(literal *expr.Value) string {
	switch literal.GetKind().(type) {
	case nil:
		return "nothing"
	case *expr.Value_StringValue:
		return "a string"
	case *expr.Value_BytesValue:
		return "a string of bytes"
	case *expr.Value_BoolValue:
		return "true or false"
	case *expr.Value_Int64Value, *expr.Value_Uint64Value:
		return "a whole number"
	case *expr.Value_DoubleValue:
		return "a number"
	case *expr.Value_NullValue:
		return "null"
	case *expr.Value_ListValue:
		return "a list"
	case *expr.Value_MapValue:
		return "a mapping"
	default:
		name := fmt.Sprintf("%T", literal.GetKind())
		if i := strings.LastIndex(name, "_"); i >= 0 {
			name = name[i+1:]
		}
		return strings.ToLower(strings.TrimSuffix(name, "Value"))
	}
}

// waitToYAML renders a wait back into the key an author wrote it as.
//
// Which key it was is recoverable from the wait itself, because each kind of wait
// is a different member of the oneof — so a round trip returns the spelling that
// was written rather than a canonical one, and a file that goes through `flow fmt`
// does not silently change shape.
func waitToYAML(wait *v1.Wait) (string, any, error) {
	switch kind := wait.GetKind().(type) {
	case *v1.Wait_Duration:
		return "sleep", durationToYAML(kind.Duration), nil

	case *v1.Wait_Until:
		value, err := exprValueToYAML(kind.Until)
		if err != nil {
			return "", nil, fmt.Errorf("wait_until: %w", err)
		}
		return "wait_until", value, nil

	case *v1.Wait_Signal:
		// The scalar form when there is nothing else to say, which is what an
		// author most often wrote and what reads best coming back.
		if wait.GetTimeout() == nil {
			return "wait_for_signal", kind.Signal.GetName(), nil
		}
		return "wait_for_signal", yaml.MapSlice{
			{Key: "name", Value: kind.Signal.GetName()},
			{Key: "timeout", Value: durationToYAML(wait.GetTimeout())},
		}, nil

	default:
		return "", nil, fmt.Errorf("wait has no sleep, wait_until, or wait_for_signal")
	}
}

// declaredInputsToYAML writes the `inputs:` block.
//
// In declaration order rather than sorted, unlike `vars:`, and the difference is
// the schema's: these are a repeated field, so their order is a fact the document
// carries and not an artifact of a protobuf map. Sorting them would make `flow fix`
// reorder a file somebody arranged on purpose.
func declaredInputsToYAML(declarations []*v1.InputDeclaration) (yaml.MapSlice, error) {
	out := make(yaml.MapSlice, 0, len(declarations))
	for _, declaration := range declarations {
		entry := yaml.MapSlice{{Key: "type", Value: v1.DeclaredTypeName(declaration.GetType())}}

		// Only when true. An input that says `required: false` has asked for the
		// default, and writing it back would make two identical workflows produce
		// different documents depending on whether one spelled the default out — the
		// same rule `continue_on_error:` follows.
		if declaration.GetRequired() {
			entry = append(entry, yaml.MapItem{Key: "required", Value: true})
		}
		if declaration.GetDefault() != nil {
			value, err := inputValueToYAML(declaration.GetDefault())
			if err != nil {
				return nil, fmt.Errorf("input %q default: %w", declaration.GetName(), err)
			}
			entry = append(entry, yaml.MapItem{Key: "default", Value: value})
		}
		if declaration.Description != nil {
			entry = append(entry, yaml.MapItem{Key: "description", Value: declaration.GetDescription()})
		}

		out = append(out, yaml.MapItem{Key: declaration.GetName(), Value: entry})
	}

	return out, nil
}

// declaredOutputsToYAML writes the `outputs:` block, in declaration order for the
// reason the inputs are.
func declaredOutputsToYAML(declarations []*v1.OutputDeclaration) (yaml.MapSlice, error) {
	out := make(yaml.MapSlice, 0, len(declarations))
	for _, declaration := range declarations {
		value, err := exprValueToYAML(declaration.GetValue())
		if err != nil {
			return nil, fmt.Errorf("output %q: %w", declaration.GetName(), err)
		}

		entry := yaml.MapSlice{{Key: "value", Value: value}}
		if declaration.Description != nil {
			entry = append(entry, yaml.MapItem{Key: "description", Value: declaration.GetDescription()})
		}

		out = append(out, yaml.MapItem{Key: declaration.GetName(), Value: entry})
	}

	return out, nil
}
