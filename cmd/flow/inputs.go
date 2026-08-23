package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Arguments, from a command line into the map a run is started with.
//
// A workflow declares what it takes — a name, a type, and either a default or that
// it is required — and [v1.BindRunInputs] is what decides whether a submission
// satisfies that. This file is not a second copy of that decision. It has exactly
// one job the binder cannot do: a shell hands over strings, and `replicas=3` is
// three characters until something says the declaration calls it an int.
//
// So the rule here is *honest coercion*. The declaration decides how a word is read,
// the value goes over as a value, and every question about whether it is allowed
// stays where both drivers already ask it. A CLI that decided for itself that an
// undeclared name was fine, or that a float would do for an int, would be a third
// opinion about a contract that has two implementations agreeing on purpose.
//
// The check that does run here runs for the message, not for the control — the same
// standing `flow signal`'s client-side [v1.Validate] has. `flow run` refusing a
// missing argument before the round trip means the author reads about their command
// line rather than about a remote invalid-argument, and the server refuses it again
// regardless, because a client-side check is a convenience and never a control.

// addInputFlags declares the two ways a run is given its arguments.
//
// Two rather than one because they answer different questions. `--input k=v` is what
// somebody types, and it is the whole surface for the flat case. `--input-file` is
// what a script sends: JSON has types of its own, so a struct or a list arrives as
// itself rather than as a word to be re-parsed, and a set of arguments that outgrew
// a command line has somewhere to live.
//
// Given both, a flag wins over the file, which is the precedence every tool with a
// config file and flags has taught people to expect: the file is the baseline, and
// the thing you typed is the exception you meant this once.
func addInputFlags(cmd *cobra.Command) {
	cmd.Flags().StringArray("input", nil,
		"an argument this run is started with, as name=value (repeatable). The workflow's "+
			"`inputs:` declaration decides how the value is read: an int is parsed as a number, "+
			"a bool as true/false, and a list or struct as JSON")

	cmd.Flags().String("input-file", "",
		"a JSON object of arguments, keyed by input name. Values arrive with the types JSON "+
			"gives them; a --input flag of the same name wins over the file")
}

// runInputs assembles what a run is started with, from the flags and the file.
//
// Nil when neither was given and the workflow declares nothing, so a workflow with
// no `inputs:` block is submitted exactly as it was before this existed.
func runInputs(cmd *cobra.Command, workflow *v1.Workflow) (map[string]*v1.Value, error) {
	return collectInputs(cmd, declaredInputs(workflow))
}

// collectInputs is the grammar itself, over whatever declared the names.
//
// Split out from [runInputs] because a workflow's `inputs:` block is not the only
// thing that can play the declaring role. `flow task run` invokes one task, and
// there the declaration is the task's own input schema: same flags, same
// precedence, same type-reading rules, decided by a declaration that came from a
// different place. Reaching this function is what makes that literally true rather
// than true by inspection: a second reader of --input is how one grammar becomes
// two, and this repository has the scars to prove it (see inputsFromJSON's comment
// about the MCP tool's own `inputs` argument).
func collectInputs(cmd *cobra.Command, declared map[string]*v1.InputDeclaration) (map[string]*v1.Value, error) {
	inputs := map[string]*v1.Value{}

	if path, _ := cmd.Flags().GetString("input-file"); path != "" {
		fromFile, err := inputsFromFile(path, declared)
		if err != nil {
			return nil, err
		}
		maps.Copy(inputs, fromFile)
	}

	flags, _ := cmd.Flags().GetStringArray("input")
	for _, flag := range flags {
		name, value, err := parseInputFlag(flag, declared)
		if err != nil {
			return nil, err
		}
		inputs[name] = value
	}

	if len(inputs) == 0 {
		// Absent rather than empty: "no arguments" is what a workflow declaring none
		// is started with, and an empty map would travel as a field that was set.
		return nil, nil
	}

	return inputs, nil
}

// declaredInputs indexes a workflow's declarations by name.
func declaredInputs(workflow *v1.Workflow) map[string]*v1.InputDeclaration {
	declared := make(map[string]*v1.InputDeclaration, len(workflow.GetDeclaredInputs()))
	for _, declaration := range workflow.GetDeclaredInputs() {
		declared[declaration.GetName()] = declaration
	}

	return declared
}

// checkRunInputs reports an argument problem before the run is submitted.
//
// The binder's own answer, asked early. It is asked at all because of where the two
// drivers ask it otherwise: `flow run` would carry a missing argument across the
// wire and read it back as a remote refusal, and `flow run local` would report it
// wrapped in "error running workflow locally", which describes the run rather than
// the command line. The text is the binder's either way — one refusal, worded once,
// so the CLI cannot come to disagree with what the server enforces.
func checkRunInputs(workflow *v1.Workflow, inputs map[string]*v1.Value) error {
	if _, err := v1.BindRunInputs(workflow, inputs); err != nil {
		return fmt.Errorf("%w\n  arguments are given with --input name=value or --input-file inputs.json", err)
	}

	return nil
}

// parseInputFlag reads one --input name=value flag.
func parseInputFlag(flag string, declared map[string]*v1.InputDeclaration) (string, *v1.Value, error) {
	name, raw, found := strings.Cut(flag, "=")
	if !found {
		return "", nil, fmt.Errorf(
			"--input %q needs a name and a value, as name=value, e.g. --input region=eu-west-1", flag)
	}

	name = strings.TrimSpace(name)
	if name == "" {
		return "", nil, fmt.Errorf("--input %q names no input", flag)
	}

	value, err := coerceInput(name, raw, declared[name])
	if err != nil {
		return "", nil, err
	}

	return name, value, nil
}

// coerceInput reads a typed value out of the word a shell handed over.
//
// The declaration decides, which is the only thing that can. `--input flag=true` is
// the string "true" for a string input and the boolean true for a bool one, and
// guessing from the characters would make the meaning of an argument depend on what
// it happens to look like — so `--input version=1.0` would arrive as a number for a
// workflow that declared a string, and the run would be refused for a reason nobody
// wrote down.
//
// An undeclared name is coerced as a string and left to travel, deliberately. The
// refusal it earns names the workflow and lists what it does declare, which is a
// better answer than anything this function could invent about a name it knows
// nothing about — and it is the same refusal the server gives.
func coerceInput(name, raw string, declaration *v1.InputDeclaration) (*v1.Value, error) {
	switch declaration.GetType() {
	case v1.InputDeclaration_TYPE_INT:
		number, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
		if err != nil {
			return nil, inputCoercionError(name, raw, declaration, "a whole number, e.g. 3")
		}

		return v1.NewLiteral(number), nil

	case v1.InputDeclaration_TYPE_FLOAT:
		number, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
		if err != nil {
			return nil, inputCoercionError(name, raw, declaration, "a number, e.g. 1.5")
		}

		return v1.NewLiteral(number), nil

	case v1.InputDeclaration_TYPE_BOOL:
		yes, err := strconv.ParseBool(strings.TrimSpace(raw))
		if err != nil {
			return nil, inputCoercionError(name, raw, declaration, "true or false")
		}

		return v1.NewLiteral(yes), nil

	case v1.InputDeclaration_TYPE_LIST, v1.InputDeclaration_TYPE_STRUCT:
		// The one place the flag form borrows the file's notation, because a list is
		// not a thing a shell word can be and inventing a separator here would be
		// inventing a second data format for the occasion — one that could not
		// express a value containing that separator anyway.
		decoded, err := decodeInputJSON(raw)
		if err != nil {
			return nil, inputCoercionError(name, raw, declaration, exampleJSONFor(declaration.GetType()))
		}

		return valueFromJSON(name, decoded, declaration)

	default:
		// A string, and an undeclared name: the characters as given.
		return v1.NewLiteral(raw), nil
	}
}

// inputCoercionError says what was given, what the file declared, and what the
// declared type looks like written down.
func inputCoercionError(name, raw string, declaration *v1.InputDeclaration, wants string) error {
	return fmt.Errorf("--input %s=%s: %q is declared %s, which is written as %s%s",
		name, raw, name, v1.DeclaredTypeName(declaration.GetType()), wants, describedAs(declaration))
}

// describedAs renders a declaration's description as a clause, for the reason the
// binder carries one: it is the only part of a declaration written for whoever
// supplies the value, so a refusal about that value is exactly when it is worth
// repeating.
func describedAs(declaration *v1.InputDeclaration) string {
	if declaration.GetDescription() == "" {
		return ""
	}

	return " (" + declaration.GetDescription() + ")"
}

// exampleJSONFor is how a structured type is written on a command line.
func exampleJSONFor(t v1.InputDeclaration_Type) string {
	if t == v1.InputDeclaration_TYPE_STRUCT {
		return `JSON, e.g. --input labels='{"team":"payments"}'`
	}

	return `JSON, e.g. --input targets='["alpha","beta"]'`
}

// inputsFromFile reads a JSON object of arguments.
func inputsFromFile(path string, declared map[string]*v1.InputDeclaration) (map[string]*v1.Value, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading --input-file: %w", err)
	}

	return inputsFromJSON(path, data, declared)
}

// inputsFromJSON turns a JSON object into the map a run is started with.
//
// Shared by `--input-file` and by the MCP tool's `inputs` argument, so a document
// means the same thing whichever surface it arrived on — a second decoder is how
// two surfaces of one contract start disagreeing, which is the divergence this
// repository keeps rediscovering.
//
// The source names where the document came from, because a file path and "the
// arguments of a tool call" are different things to go and look at.
func inputsFromJSON(source string, data []byte, declared map[string]*v1.InputDeclaration) (map[string]*v1.Value, error) {
	fields, err := decodeInputJSONObject(data)
	if err != nil {
		return nil, fmt.Errorf("%s is not a JSON object of arguments keyed by input name: %w", source, err)
	}

	inputs := make(map[string]*v1.Value, len(fields))

	// Sorted so a document with two bad values reports the same one first every
	// time, whatever order the decoder happened to walk the object in.
	for _, name := range slices.Sorted(maps.Keys(fields)) {
		value, err := valueFromJSON(name, fields[name], declared[name])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", source, err)
		}
		inputs[name] = value
	}

	return inputs, nil
}

// decodeInputJSONObject decodes a JSON object with its numbers left as written.
func decodeInputJSONObject(data []byte) (map[string]any, error) {
	decoded, err := decodeInputJSON(string(data))
	if err != nil {
		return nil, err
	}

	fields, ok := decoded.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("the document is %s rather than an object", jsonKindName(decoded))
	}

	return fields, nil
}

// decodeInputJSON decodes one JSON document, keeping numbers as written.
//
// UseNumber, because `json.Unmarshal` reads every number as a float64 — so a
// `replicas: 2` in a file would arrive as 2.0 and be refused against an `int`
// declaration for a difference the document does not contain. What a number is
// belongs to the declaration, and [valueFromJSON] is where that is decided.
func decodeInputJSON(raw string) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader([]byte(raw)))
	decoder.UseNumber()

	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}

	// A trailing token means the caller sent two documents where one was expected,
	// which is worth refusing rather than silently reading the first.
	if decoder.More() {
		return nil, fmt.Errorf("there is more than one JSON document here")
	}

	return decoded, nil
}

// valueFromJSON turns a decoded JSON value into the [v1.Value] a run carries.
//
// The declaration decides what a number is, for the reason [coerceInput] gives one
// level up: `3` is an int where an int was declared and a float where a float was,
// and JSON cannot tell them apart. Everything else is passed through as parsed —
// which is the whole reason a file exists beside the flags.
//
// A number nested inside a list or a struct has no declaration of its own, so it is
// read as written: whole numbers are ints and the rest are floats. That is the same
// rule the YAML parser applies to a literal in a file.
func valueFromJSON(name string, decoded any, declaration *v1.InputDeclaration) (*v1.Value, error) {
	if number, ok := decoded.(json.Number); ok && declaration.GetType() == v1.InputDeclaration_TYPE_FLOAT {
		asFloat, err := number.Float64()
		if err != nil {
			return nil, fmt.Errorf("input %q is %s, which is not a number this can carry", name, number)
		}

		return v1.NewLiteral(asFloat), nil
	}

	normalized, err := normalizeJSON(name, decoded)
	if err != nil {
		return nil, err
	}

	return v1.NewValue(normalized), nil
}

// normalizeJSON replaces every json.Number with the Go number it stands for.
//
// Recursive, because a struct input's fields and a list input's items are values
// too, and a number left as a json.Number reaches [v1.NewValue] as a string kind it
// does not know — which would arrive as an error value inside an otherwise fine
// argument rather than as a refusal anyone could read.
func normalizeJSON(name string, decoded any) (any, error) {
	switch value := decoded.(type) {
	case json.Number:
		if whole, err := value.Int64(); err == nil {
			return whole, nil
		}

		asFloat, err := value.Float64()
		if err != nil {
			return nil, fmt.Errorf("input %q holds %s, which is not a number this can carry", name, value)
		}

		return asFloat, nil

	case map[string]any:
		normalized := make(map[string]any, len(value))
		for key, held := range value {
			converted, err := normalizeJSON(name, held)
			if err != nil {
				return nil, err
			}
			normalized[key] = converted
		}

		return normalized, nil

	case []any:
		normalized := make([]any, 0, len(value))
		for _, held := range value {
			converted, err := normalizeJSON(name, held)
			if err != nil {
				return nil, err
			}
			normalized = append(normalized, converted)
		}

		return normalized, nil

	default:
		return value, nil
	}
}

// jsonKindName names what a decoded document turned out to be, for a message about
// it being the wrong thing.
func jsonKindName(decoded any) string {
	switch decoded.(type) {
	case nil:
		return "null"
	case bool:
		return "a boolean"
	case json.Number:
		return "a number"
	case string:
		return "a string"
	case []any:
		return "a list"
	default:
		return "something else"
	}
}

// runArgumentFlags re-renders the input flags this invocation carried, quoted
// for a shell, so a suggested recovery command starts the workload that was
// asked for. A workflow with required inputs refuses the flagless spelling
// outright, and optional inputs silently start a different workload, which is
// worse. Values the workflow declared sensitive are replaced with the same
// marker as every other CLI display surface unless --reveal-sensitive was
// explicitly requested. The `--flag=value` form is used because a value with a
// leading dash would otherwise read as the next flag.
func runArgumentFlags(cmd *cobra.Command, workflow *v1.Workflow) []string {
	var arguments []string

	if file, _ := cmd.Flags().GetString("input-file"); file != "" {
		arguments = append(arguments, "--input-file="+shellArgument(file))
	}
	flags, _ := cmd.Flags().GetStringArray("input")
	declared := declaredInputs(workflow)
	reveal := revealSensitiveRequested(cmd)
	for _, flag := range flags {
		if name, _, ok := strings.Cut(flag, "="); ok && declared[name].GetSensitive() && !reveal {
			flag = name + "=" + redactedMarker(name)
		}
		arguments = append(arguments, "--input="+shellArgument(flag))
	}

	return arguments
}
