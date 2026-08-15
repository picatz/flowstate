package flowfile

import (
	"encoding/json"
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
		{Key: "name", Value: textToYAML(wf.GetName())},
	}

	if wf.Description != nil {
		doc = append(doc, yaml.MapItem{Key: "description", Value: textToYAML(wf.GetDescription())})
	}

	// What the workflow needs from the deployment that runs it, above the inputs the
	// run itself takes: the same position both examples in examples/plugins/ write
	// it in, directly under the description and before anything the run computes.
	if requirements := wf.GetPluginRequirements(); len(requirements) > 0 {
		plugins, err := pluginRequirementsToYAML(requirements)
		if err != nil {
			return nil, err
		}
		doc = append(doc, yaml.MapItem{Key: "plugins", Value: plugins})
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

	// Same position the parser reads it: alongside `triggers:`, a fact about the
	// whole workflow's relationship with the outside world.
	if signals := wf.GetSignals(); len(signals) > 0 {
		written, err := signalsToYAML(signals)
		if err != nil {
			return nil, err
		}
		doc = append(doc, yaml.MapItem{Key: "signals", Value: written})
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

// pluginRequirementsToYAML writes the `plugins:` block.
//
// In declaration order rather than sorted, for the reason `declaredInputsToYAML`
// gives: [v1.Workflow.PluginRequirements] is a repeated field, so the order is a
// fact the document carries rather than an artifact of a protobuf map, and
// sorting it would make `flow fix` reorder a file an author arranged on purpose.
//
// A version [v1.ValidPluginVersion] refuses is refused here too, rather than
// written: a parsed file cannot carry one (parse.go rejects it with a positioned
// diagnostic), but Marshal also serves hand-built specifications, and emitting
// a version the parser will reject produces a document that cannot round-trip.
// The same rule Marshal already applies to a call with no source path.
func pluginRequirementsToYAML(requirements []*v1.PluginRequirement) (yaml.MapSlice, error) {
	out := make(yaml.MapSlice, 0, len(requirements))
	for _, requirement := range requirements {
		if !v1.ValidPluginVersion(requirement.GetMinimumVersion()) {
			return nil, fmt.Errorf("plugin %q: minimum version %q is not a semantic version written as vMAJOR.MINOR.PATCH, so the parser would reject the marshalled file", requirement.GetName(), requirement.GetMinimumVersion())
		}
		out = append(out, yaml.MapItem{Key: requirement.GetName(), Value: requirement.GetMinimumVersion()})
	}
	return out, nil
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
	step := yaml.MapSlice{{Key: "id", Value: textToYAML(node.GetId())}}

	// Written second, right under the id, because prose about a step is read
	// before the mechanics of it. Only when set, so an absent description and an
	// empty one stay distinguishable through a round trip.
	if node.Description != nil {
		step = append(step, yaml.MapItem{Key: "description", Value: textToYAML(node.GetDescription())})
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

	// Above the policy and the work, the position the parser reads it in: how a
	// step relates to the ones around it is read before how long it may take.
	// Only when true, so an absent marker and an explicit `async: false` stay one
	// value through a round trip.
	if node.GetAsync() {
		step = append(step, yaml.MapItem{Key: "async", Value: true})
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

	case *v1.Node_Loop:
		loop, err := loopToYAML(kind.Loop)
		if err != nil {
			return nil, fmt.Errorf("step %q loop: %w", node.GetId(), err)
		}
		step = append(step, yaml.MapItem{Key: "loop", Value: loop})

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

	case *v1.Node_Switch:
		value, err := switchToYAML(kind.Switch)
		if err != nil {
			return nil, fmt.Errorf("step %q switch: %w", node.GetId(), err)
		}
		step = append(step, yaml.MapItem{Key: "switch", Value: value})

	case *v1.Node_Value:
		// Through [exprValueToYAML], the same writer the condition above uses,
		// because the parser reads both positions the same fence-optional way.
		// Writing it any other way would make the round trip lossy for exactly
		// the values an author is most likely to write here.
		value, err := exprValueToYAML(kind.Value)
		if err != nil {
			return nil, fmt.Errorf("step %q value: %w", node.GetId(), err)
		}
		step = append(step, yaml.MapItem{Key: "value", Value: value})

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
		out = append(out, yaml.MapItem{Key: "as", Value: textToYAML(iterator)})
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

// loopToYAML writes a `loop:` node in the order an author reads it: what it carries,
// then when it stops, then how far it may go, then its body.
//
// `until:` is written fenced as an expression; `init:` and `update:` go through
// [inputValueToYAML] because either may be a literal or an expression, exactly as a
// task input is. The order here is the order the acceptance example is written in, so
// a marshalled loop reads the way an author would have written one.
func loopToYAML(loop *v1.Loop) (yaml.MapSlice, error) {
	out := yaml.MapSlice{}

	if state := loop.GetState(); state != "" {
		out = append(out, yaml.MapItem{Key: "as", Value: textToYAML(state)})

		initial, err := inputValueToYAML(loop.GetInitial())
		if err != nil {
			return nil, fmt.Errorf("init: %w", err)
		}
		out = append(out, yaml.MapItem{Key: "init", Value: initial})

		update, err := inputValueToYAML(loop.GetUpdate())
		if err != nil {
			return nil, fmt.Errorf("update: %w", err)
		}
		out = append(out, yaml.MapItem{Key: "update", Value: update})
	}

	until, err := exprValueToYAML(loop.GetUntil())
	if err != nil {
		return nil, fmt.Errorf("until: %w", err)
	}
	out = append(out, yaml.MapItem{Key: "until", Value: until})

	if maxIterations := loop.GetMaxIterations(); maxIterations != 0 {
		out = append(out, yaml.MapItem{Key: "max_iterations", Value: maxIterations})
	}

	steps, err := stepsToYAML(loop.GetBody())
	if err != nil {
		return nil, err
	}
	return append(out, yaml.MapItem{Key: "steps", Value: steps}), nil
}

// switchToYAML writes a `switch:` node in reading order: the value it
// dispatches on, the cases, then the default when one exists.
//
// Built key-by-key, because a key nothing writes back is a key `flow fix`
// silently removes. A single-value case is written as the scalar and a
// multi-value case as the list, which under fmt's semantic contract makes
// `case: [x]` canonicalize to `case: x` — legitimate for fmt, and `flow fix`'s
// byte-for-byte surface never rewrites a file it is not changing. `steps:` is
// always written, `[]` included: an empty body is written-down ignoring, and
// dropping the key would turn it into a parse error on the way back in.
func switchToYAML(sw *v1.Switch) (yaml.MapSlice, error) {
	value, err := exprValueToYAML(sw.GetValue())
	if err != nil {
		return nil, fmt.Errorf("value: %w", err)
	}
	out := yaml.MapSlice{{Key: "value", Value: value}}

	cases := make([]any, 0, len(sw.GetCases()))
	for i, c := range sw.GetCases() {
		var caseValue any
		values := c.GetValues()
		if len(values) == 1 {
			caseValue, err = inputValueToYAML(values[0])
			if err != nil {
				return nil, fmt.Errorf("case %d: %w", i+1, err)
			}
		} else {
			list := make([]any, 0, len(values))
			for j, v := range values {
				element, err := inputValueToYAML(v)
				if err != nil {
					return nil, fmt.Errorf("case %d value %d: %w", i+1, j+1, err)
				}
				list = append(list, element)
			}
			caseValue = list
		}

		steps, err := stepsToYAML(c.GetSteps())
		if err != nil {
			return nil, fmt.Errorf("case %d: %w", i+1, err)
		}
		cases = append(cases, yaml.MapSlice{
			{Key: "case", Value: caseValue},
			{Key: "steps", Value: steps},
		})
	}
	out = append(out, yaml.MapItem{Key: "cases", Value: cases})

	if def := sw.GetDefault(); def != nil {
		steps, err := stepsToYAML(def.GetSteps())
		if err != nil {
			return nil, fmt.Errorf("default: %w", err)
		}
		out = append(out, yaml.MapItem{Key: "default", Value: yaml.MapSlice{{Key: "steps", Value: steps}}})
	}

	return out, nil
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

// fencedExprToYAML writes an expression back into a field where the fence is what
// makes it one.
//
// [exprValueToYAML] is the other rule, for a field the schema types as an
// expression outright: there an unfenced string *is* source, so writing the fence
// would be noise. In a duration position an unfenced string is a literal `30s`, so
// the fence is meaning rather than punctuation and has to come back.
//
// A literal reaching here is a specification built by hand — the compiler puts a
// duration written as data in the message's literal field instead — and it is
// refused rather than written, because writing it would produce a Flowfile that
// says something else.
func fencedExprToYAML(value *v1.Value) (any, error) {
	parsed := value.GetExpr()
	if parsed == nil {
		if reference := value.GetSecretRef(); reference != nil {
			return nil, fmt.Errorf("is a secret reference, which cannot be written here: %s", notEvaluableHelp)
		}

		return nil, fmt.Errorf(
			"is not an expression; a duration written as data belongs in the literal field, not the computed one")
	}

	text, err := exprToText(parsed)
	if err != nil {
		return nil, err
	}

	return fenceOpen + text + fenceClose, nil
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

// textToYAML writes a compile-time text field — anything [compiler.text] reads.
//
// It is that function's inverse and exists to be applied at every one of its
// write sites, which is the whole of the point. `compiler.text` resolves `$${`
// to a literal `${`, so a `description: show $${TOKEN}` is held in the workflow
// as `show ${TOKEN}`; writing those bytes back unescaped produces a real fence,
// and the next compile refuses the file as an expression in a position that
// cannot hold one. `flow fmt` is [Marshal] plus the source's comments, so that
// is formatting a valid file into an invalid one — the thing a formatter must
// never do.
//
// Applied to every such field rather than to the ones whose values can plausibly
// contain a `${` today, because on a string that holds none it is the identity,
// and because the alternative is a list of exceptions that has to be revisited
// each time a field is added. A field whose grammar excludes `$` loses nothing
// by being escaped; a field that quietly gains free text loses a round trip by
// being left out. [TestMarshalRoundTripsEveryScalarPosition] is what keeps the
// two sides in step.
// # Quoting is checked, not assumed
//
// Escaping the fences is not enough on its own, because the emitter still gets
// to choose whether the result needs quotes, and it can choose wrong. A
// description of `? 000` is written plain, and `? ` at the head of a scalar is
// YAML's explicit-key indicator, so the document that comes back says "mapping
// value is not allowed in this context" — a valid file formatted into one that
// does not parse, which is the same failure the escaping above exists to
// prevent, arriving by a different road. FuzzMarshalRoundTrip found it; the
// corpus entry is testdata/fuzz/FuzzMarshalRoundTrip/explicit_key_indicator.
//
// So the plain form is tried and *verified* rather than trusted, and anything
// that does not survive is written explicitly quoted. That is deliberately a
// property of the round trip rather than a list of dangerous prefixes: a list
// would have said `? ` today and been silent about whatever the next emitter
// version, or the next field, gets wrong. #533 reached the same conclusion
// about promoted scalars for the same reason.
func textToYAML(s string) any {
	escaped := escapeFences(s)
	if plainScalarSurvives(escaped) {
		return escaped
	}

	return quotedScalar(escaped)
}

// plainScalarSurvives reports whether the emitter's own rendering of s reads
// back as s.
//
// One scalar in a one-entry mapping, which is the shape every caller of
// [textToYAML] writes it in: the question is what the emitter does with this
// value in a value position, and asking it directly is cheaper than modelling
// its rules.
func plainScalarSurvives(s string) bool {
	encoded, err := yaml.Marshal(yaml.MapSlice{{Key: "v", Value: s}})
	if err != nil {
		return false
	}

	var back yaml.MapSlice
	if err := yaml.Unmarshal(encoded, &back); err != nil {
		return false
	}

	return len(back) == 1 && back[0].Value == s
}

// quotedScalar is a string written in YAML's double-quoted style whatever the
// emitter would have chosen for it.
//
// A [yaml.BytesMarshaler], so the bytes are placed as the value rather than
// being re-analysed: the whole point is to take the choice away from the
// emitter for the values it gets wrong.
type quotedScalar string

// MarshalYAML writes the double-quoted form, escaped by the JSON rules YAML
// shares for this style.
func (q quotedScalar) MarshalYAML() ([]byte, error) {
	encoded, err := json.Marshal(string(q))
	if err != nil {
		return nil, fmt.Errorf("quoting %q: %w", string(q), err)
	}

	return encoded, nil
}

// literalToYAML writes a literal value, keeping the order of a map's entries so
// that reading the document back produces the same literal.
func literalToYAML(literal *expr.Value) (any, error) {
	switch kind := literal.GetKind().(type) {
	case *expr.Value_StringValue:
		// Written with its fences escaped rather than refused. Until #413 there
		// was no spelling for a literal `${` in a value, so a workflow built in
		// Go holding one could not be written out at all and saying so was the
		// only honest answer. `$${` is that spelling, and reading the result back
		// produces this string again — which is what Marshal owes.
		return escapeFences(kind.StringValue), nil
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

	case *v1.Wait_DurationExpr:
		// Written back fenced, always. A computed sleep is the one duration
		// position where the fence is not decoration: unfenced, `inputs.grace` is
		// the literal five-character string, which is not a duration and which
		// `flow validate` would then refuse — so dropping the fence on a round trip
		// would be `flow fmt` corrupting a file, the thing that must not happen.
		value, err := fencedExprToYAML(kind.DurationExpr)
		if err != nil {
			return "", nil, fmt.Errorf("sleep: %w", err)
		}
		return "sleep", value, nil

	case *v1.Wait_Until:
		value, err := exprValueToYAML(kind.Until)
		if err != nil {
			return "", nil, fmt.Errorf("wait_until: %w", err)
		}
		return "wait_until", value, nil

	case *v1.Wait_Signal:
		// The mapping is built key by key rather than in a switch over the
		// combinations, because there are now three optional keys and a switch
		// over them is where a formatter *deletes* one: `flow fix` rewrites
		// through this function, so a key nothing writes back is a key the
		// command silently removes. `varsToYAML` records the same lesson.
		mapping := yaml.MapSlice{{Key: "name", Value: textToYAML(kind.Signal.GetName())}}

		// Directly after the name, because that is the order the two read in: what
		// the gate is called, then what it is asking. Written back through
		// [inputValueToYAML], the same writer `outputs:` entries use, so a fenced
		// prompt comes back fenced and a plain sentence comes back plain - the
		// round trip `signals.go` records the lesson of, where a key nothing wrote
		// back was a key `flow fmt` silently deleted.
		if prompt := kind.Signal.GetPrompt(); prompt != nil {
			value, err := inputValueToYAML(prompt)
			if err != nil {
				return "", nil, fmt.Errorf("wait_for_signal prompt: %w", err)
			}
			mapping = append(mapping, yaml.MapItem{Key: "prompt", Value: value})
		}

		switch {
		case wait.GetTimeoutExpr() != nil:
			value, err := fencedExprToYAML(wait.GetTimeoutExpr())
			if err != nil {
				return "", nil, fmt.Errorf("wait_for_signal timeout: %w", err)
			}
			mapping = append(mapping, yaml.MapItem{Key: "timeout", Value: value})

		case wait.GetTimeout() != nil:
			mapping = append(mapping,
				yaml.MapItem{Key: "timeout", Value: durationToYAML(wait.GetTimeout())})
		}

		if shaped := kind.Signal.GetOutputs(); len(shaped) > 0 {
			// Sorted, for the reason `varsToYAML` sorts: the names come out of a
			// protobuf map, and a formatter emitting them in a different order each
			// run would make every `flow fix` a diff.
			out := make(yaml.MapSlice, 0, len(shaped))
			for _, name := range slices.Sorted(maps.Keys(shaped)) {
				value, err := inputValueToYAML(shaped[name])
				if err != nil {
					return "", nil, fmt.Errorf("wait_for_signal outputs %q: %w", name, err)
				}
				out = append(out, yaml.MapItem{Key: name, Value: value})
			}
			mapping = append(mapping, yaml.MapItem{Key: "outputs", Value: out})
		}

		// The scalar form when there is nothing else to say, which is what an
		// author most often wrote and what reads best coming back.
		if len(mapping) == 1 {
			return "wait_for_signal", kind.Signal.GetName(), nil
		}

		return "wait_for_signal", mapping, nil

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
			entry = append(entry, yaml.MapItem{Key: "description", Value: textToYAML(declaration.GetDescription())})
		}
		if declaration.GetExample() != nil {
			value, err := inputValueToYAML(declaration.GetExample())
			if err != nil {
				return nil, fmt.Errorf("input %q example: %w", declaration.GetName(), err)
			}
			entry = append(entry, yaml.MapItem{Key: "example", Value: value})
		}
		if declaration.GetSensitive() {
			entry = append(entry, yaml.MapItem{Key: "sensitive", Value: true})
		}
		if declaration.MinLen != nil {
			entry = append(entry, yaml.MapItem{Key: "min_len", Value: declaration.GetMinLen()})
		}
		if declaration.MaxLen != nil {
			entry = append(entry, yaml.MapItem{Key: "max_len", Value: declaration.GetMaxLen()})
		}
		if declaration.MinItems != nil {
			entry = append(entry, yaml.MapItem{Key: "min_items", Value: declaration.GetMinItems()})
		}
		if declaration.MaxItems != nil {
			entry = append(entry, yaml.MapItem{Key: "max_items", Value: declaration.GetMaxItems()})
		}
		if declaration.Must != nil {
			entry = append(entry, yaml.MapItem{Key: "must", Value: textToYAML(declaration.GetMust())})
		}
		if len(declaration.GetValues()) > 0 {
			entry = append(entry, yaml.MapItem{Key: "values", Value: declaration.GetValues()})
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
			entry = append(entry, yaml.MapItem{Key: "description", Value: textToYAML(declaration.GetDescription())})
		}
		if declaration.Must != nil {
			entry = append(entry, yaml.MapItem{Key: "must", Value: textToYAML(declaration.GetMust())})
		}
		if declaration.GetSensitive() {
			entry = append(entry, yaml.MapItem{Key: "sensitive", Value: true})
		}

		out = append(out, yaml.MapItem{Key: declaration.GetName(), Value: entry})
	}

	return out, nil
}
