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
//	message: hello world          # the string "hello world"
//	message: ${steps.a.result}    # the output of step a
//	message: steps.a.result       # the string "steps.a.result", not a reference
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
//	  X-Trace: ${steps.trace.result}  # compiles to {'X-Trace': steps.trace.result}
//	  X-Env: production
//
// The fence has to span the whole value. There is no string interpolation, so
// "hello ${steps.name.result}" is reported rather than quietly shipped as literal
// text; write it as one expression instead, as ${'hello ' + steps.name.result}.
// Where the fence
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
// # Secrets
//
// A secret is referenced, never written:
//
//	token: ${secret('vault:prod/api#token')}
//
// It looks like a function and is not one. The marker is recognized when the
// workflow is compiled and becomes a reference in the specification, so nothing
// evaluates a call by that name — which is the point, because a function returns its
// value to workflow code, and everything workflow code produces is written to
// durable, broadly readable history. Only the worker running the step resolves the
// reference, and the value exists for that call alone.
//
// A reference is `scheme:name`. The scheme selects the backend; everything after the
// first colon is the name, and the DSL treats it as opaque — `vault:prod/api#token`
// names `prod/api#token` to the vault backend, and what the `#` means there is
// vault's business, not the DSL's.
//
// A reference has to be the whole value of a task input, or of one entry of a list
// or a mapping written in one, and each way of writing it somewhere else is a
// compile error naming the line and column it is on. It cannot be combined with
// anything, so ${'Bearer ' + secret('env:TOKEN')} is refused rather than compiled
// into an expression that fails at run time. It cannot appear in an `if` or a
// loop's `items`, which the workflow evaluates itself.
//
// The first input built to receive one was the http task's `bearer:`:
//
//	http:
//	  url: https://api.example.com/events
//	  bearer: ${secret('vault:prod/api#token')}
//
// The worker resolves it inside the activity that makes the request and sets
// `Authorization: Bearer <value>`, so the value exists for that call and nowhere
// else. The same is true one level down, inside the inputs the http task turns
// into request bytes itself — `headers:`, `form:` and `json:`:
//
//	http:
//	  url: https://api.example.com/events
//	  headers:
//	    Accept: application/json
//	    Authorization: ${secret('env:API_TOKEN')}
//
// Nothing about the reference changes by being nested. The mapping compiles to a
// structure whose entries are values, the entry stays a reference in the
// specification and in the activity payload, and the worker resolves it as it sets
// the header.
//
// # Where a reference is still refused, and why
//
// The rule is one question asked of the input: does the *task* apply this input's
// entries itself, inside the activity? An input the workflow resolves is one whose
// resolved value rides the activity payload into history, so no reference may go
// there however the file spells it.
//
//   - The http task's `query:` is refused although it is the same kind of map as
//     `form:`. The destination is what refuses it: a query string is written to
//     access logs, kept in browser history, and forwarded in a Referer header by
//     anything that follows a redirect.
//   - The raw string `body:`, a `log:` field, a step's `vars:`, an `if:`, a loop's
//     `items:`, an `outputs:` declaration, a wait's payload — all of these are
//     values the workflow itself reads, and reading a secret in workflow code is
//     the one thing this whole arrangement exists to prevent.
//   - An expression sharing a list or a mapping with a reference. The entries of a
//     structure holding a reference travel as they were written, and an expression
//     among them would have to be evaluated by the workflow to get there; a
//     mapping of expressions with no reference in it still compiles to the single
//     expression it always did. For an Authorization header this costs nothing —
//     `bearer:` takes the credential and leaves the rest of `headers:` free.
//
// Each refusal names the inputs of that task which would have worked, read from
// the task's own definition, so an input built to accept a reference is offered on
// the day it is built.
//
// [flowstatev1.Workflow]: https://pkg.go.dev/github.com/picatz/flowstate/pkg/flowstate/v1#Workflow
package flowfile
