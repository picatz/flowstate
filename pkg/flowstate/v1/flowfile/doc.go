// Package flowfile compiles a Flowfile — the YAML and CEL surface authors write —
// into the protobuf workflow the engine executes, and writes one back out again.
//
// The compiled workflow is the real artifact. A Flowfile is sugar over
// [flowstatev1.Workflow]: everything it can say, the schema already says, so this
// package parses the document tree straight into that schema rather than into Go
// types shaped like the YAML. There is no second model of a workflow here to keep
// in step with the first by hand.
//
// [Parse] returns the source position of everything it read, so a problem can be
// reported at the line and column it is on, and an editor can underline the token
// at fault rather than the whole step. [Unmarshal] is the same thing for a caller
// that only wants the workflow; [Marshal] is the inverse; [Validate] and
// [ValidateSource] report the problems that compiling cannot catch.
//
// # Expressions
//
// A Flowfile mixes data with CEL expressions, and whether a given value is one or
// the other is the DSL's most confusing corner. The rule is short, and it comes
// from the schema rather than from a convention layered on top of it.
//
// The schema types some fields as expressions. A step's `if` is a condition, and a
// loop's `items` is the list to iterate: both are evaluated, so a string written
// there is expression source. The ${...} fence is optional, and both spellings mean
// the same thing:
//
//	if: check.result == 'ready'
//	if: ${check.result == 'ready'}
//
// A value that is not a string in one of those fields is data, because YAML already
// distinguishes them and there is nothing to evaluate:
//
//	if: false                 # a literal false: the step never runs
//	items: [alpha, beta]      # a literal list of two strings
//
// A task input can be either, since what it holds is whatever the task declares.
// There the fence is what makes an expression, and a bare string is text:
//
//	message: hello world      # the string "hello world"
//	message: ${a.result}      # the output of step a
//	message: a.result         # the string "a.result", not a reference
//
// That asymmetry is deliberate. `message: hello world` has to stay a message, so an
// input cannot read a bare string as an expression; a condition has to be a
// boolean, so reading a bare string there as text would only ever be wrong.
//
// Inside a list or a mapping, a fenced value is an expression and everything else
// is data. A structure containing one anywhere becomes a single expression building
// the whole structure, which is what lets one key of a map be computed:
//
//	headers:
//	  X-Trace: ${run.id}      # the map compiles to {'X-Trace': run.id}
//	  X-Env: production
//
// The fence has to span the whole value. There is no string interpolation, so
// "hello ${name.result}" is reported rather than quietly shipped as literal text;
// write it as one expression instead, as ${'hello ' + name.result}. Where the fence
// ends is decided by compiling its contents, not by counting braces, so an
// expression containing braces of its own is fine: "${ {'k': 1} }" is one
// expression. [ExprSource] and [ExprError] answer the question directly, for tooling
// that has to make the same decision.
//
// Fields the schema does not type as expressions — a step's `id`, a task's `name`, a
// `timeout` — are read when the workflow is compiled, before anything could be
// evaluated. A ${...} written in one of those is reported rather than accepted as
// the literal text of an expression that will never run.
//
// [flowstatev1.Workflow]: https://pkg.go.dev/github.com/picatz/flowstate/pkg/flowstate/v1#Workflow
package flowfile
