package flowfile

import (
	"fmt"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// An input declared `sensitive:` is one the author has told the system to keep
// out of the clear — it is redacted in output, and a run resolves it as a
// reference rather than carrying its value through history. A `log:` message
// undoes all of that: its whole purpose is to emit text, and that text is
// written to Temporal history — durable and broadly readable — and to the
// worker's stdout. So a message that interpolates a sensitive input writes the
// value everywhere the sensitive declaration exists to keep it out of.
//
// This is #234's V5: a lint that is a property of the file, cheap, and refuses
// exactly the leak the declaration is supposed to prevent. It follows
// negation.go's shape — a positioned [Diagnostics] from a whole-file walk — and
// negation.go's discipline about what it will and will not claim.
//
// # The boundary: direct surfacing only
//
// The hard part is not catching the leak; it is not crying wolf. A sensitive
// value that passes through a transformation is, deliberately, a different
// thing — `${inputs.token != ""}` is a boolean the author derived on purpose to
// say "a token was supplied" without saying which, and `${hash(inputs.token)}`
// is a digest chosen precisely so the value does not appear. Refusing those
// would train an author to disable the lint, which CLAUDE.md names as worse than
// missing the real case: a false "this is a leak" on a value the author took
// care to derive teaches them the lint is wrong.
//
// So this refuses only *direct* surfacing, where the raw value reaches the log
// verbatim:
//
//   - the message is the bare reference — `${inputs.token}`; or
//   - the reference is a direct operand of string concatenation —
//     `${"token: " + inputs.token}`, at any depth of the `+` chain.
//
// Anything wrapped in a call — `${string(inputs.token)}`, `${inputs.token[:4]}`,
// `${inputs.token.lowerAscii()}`, a comparison, a membership test — is treated
// as derived and left alone. `string(inputs.token)` is a known, accepted gap:
// it does surface the value, but it is an author reaching past the obvious
// spelling, not the accidental `${inputs.token}` this exists to catch, and the
// alternative is guessing which of the CEL functions redact and which do not.
// The line is drawn where the value is unambiguously in the clear.
//
// # Scope: the `log:` message, and only that, for now
//
// The `log:` task is the clear first sink: its message and its `fields:` exist
// to be written as text. This covers the `message` input — the one #234 names —
// and no more. Two larger questions are deliberately left open rather than
// half-answered here:
//
//   - `log:`'s `fields:` map is the same sink and is not checked. A structured
//     field holding `${inputs.token}` compiles to a map-building expression, not
//     a bare reference or a `+` chain, so the direct-surfacing rule above does
//     not see it. Covering it wants its own decision about what "direct" means
//     inside a structure.
//   - "a sensitive value reaching any non-secret sink" is a whole-program taint
//     question, much bigger than a file-local lint, and the http task already
//     has its own mechanism for secrets (`${secret(...)}`). This does not attempt
//     it.
//
// Keeping V5 to the message the issue describes is the point: a small rule that
// is right beats a broad one that guesses.

// checkSensitiveLog reports a `log:` message that writes an input declared
// `sensitive:` into the log in the clear. See this file's doc for the boundary
// it draws and the two sinks it deliberately does not cover.
func checkSensitiveLog(wf *v1.Workflow) Diagnostics {
	sensitive := v1.SensitiveInputNames(wf)
	if len(sensitive) == 0 {
		// No sensitive inputs, nothing this lint can fire on — and the common
		// case, so it costs one map build and returns.
		return nil
	}
	return sensitiveLogInNodes(wf.GetSteps(), sensitive)
}

// sensitiveLogInNodes reports every `log:` in a tree of steps that surfaces a
// sensitive input, its own and its compensations'.
//
// The tree comes from [v1.WalkNodes], the one traversal (#508). This lint is one
// of the five walks that motivated it, and the one whose omission had the sharpest
// consequence: `Node.undo` landed after this was written, nothing added it here,
// and a compensation's `log:` could print a value the declaration exists to keep
// out of the clear — in the position where it matters most, since compensation
// runs when something has already gone wrong, which is when logs get read closely
// (#509).
//
// `inputs.<name>` means the same workflow input at any nesting depth, so one walk
// from the top covers the whole tree. A callee is not followed: it is a different
// workflow with its own declared inputs, validated in its own right.
func sensitiveLogInNodes(nodes []*v1.Node, sensitive map[string]bool) Diagnostics {
	var ds Diagnostics

	v1.WalkNodes(nodes, v1.Walk{
		Node: func(node *v1.Node) {
			if d, ok := sensitiveLogInTask(node.GetId(), node.GetTask(), sensitive); ok {
				ds = append(ds, d)
			}

			// A compensation runs the same tasks a step runs, so a `log:` that
			// surfaces a sensitive input is the same mistake written in the `undo:`
			// position. The field is the `undo:` key rather than `message` for the
			// reason validateUndoInputs documents — a field naming the inner input
			// would be looked up against the step's own task — so the inner field
			// moves into the sentence instead.
			//
			// Kind is set alongside Field so [validateParsed] routes the position
			// through [Positions.LocateKind] rather than [Positions.Locate]: the
			// step's own primary task may declare an input literally named `undo`
			// (a plugin task's input names come from its own descriptor, so `undo`
			// is not reserved), and Locate's candidate search tries every registered
			// task's `.undo` input before the step's own `<step>.undo`. On such a
			// step, Field alone would underline that unrelated primary-task input
			// instead of the compensation. LocateKind addresses `<step>.undo`
			// exactly, with no candidate search to go wrong.
			if d, ok := sensitiveLogInTask(node.GetId(), node.GetUndo().GetTask(), sensitive); ok {
				d.Field = "undo"
				d.Kind = "undo"
				d.Message = "in this step's compensation: " + d.Message
				ds = append(ds, d)
			}
		},
	})

	return ds
}

// sensitiveLogInTask reports the diagnostic for one step when it is a `log:`
// whose message directly surfaces a sensitive input, and ok=false otherwise.
func sensitiveLogInTask(stepID string, task *v1.Task, sensitive map[string]bool) (Diagnostic, bool) {
	if task.GetName() != "log" {
		return Diagnostic{}, false
	}

	message := task.GetInputs()["message"]
	root := message.GetExpr().GetExpr()
	if root == nil {
		// A literal message — plain text, no expression, nothing to reference.
		return Diagnostic{}, false
	}

	name, ok := directlySurfaced(root, sensitive)
	if !ok {
		return Diagnostic{}, false
	}

	return Diagnostic{
		Step:  stepID,
		Field: "message",
		Code:  v1.DiagnosticCodeSensitiveInLog,
		Message: fmt.Sprintf(
			"input %q is declared `sensitive:`, and a `log:` message is written to run "+
				"history and to stdout in the clear, so interpolating it here writes the "+
				"value everywhere that log can be read, which is exactly what `sensitive:` "+
				"exists to prevent. Log something derived from it instead of the value "+
				"itself: a boolean like ${inputs.%s != \"\"} to record that it was supplied, "+
				"a length, or a redaction, or drop it from the message.",
			name, name),
	}, true
}

// directlySurfaced reports whether e writes a sensitive input into the log
// verbatim: the whole expression is the bare reference, or the reference is a
// direct operand of a string-concatenation chain. A reference wrapped in any
// call is derived and not reported. See this file's doc for why the line is
// there.
func directlySurfaced(e *expr.Expr, sensitive map[string]bool) (string, bool) {
	if name := inputReference(e); name != "" && sensitive[name] {
		return name, true
	}

	if isStringConcat(e) {
		for _, operand := range flattenConcat(e) {
			if name := inputReference(operand); name != "" && sensitive[name] {
				return name, true
			}
		}
	}

	return "", false
}

// inputReference returns the input name when e is a direct reference to
// `inputs.<name>` — the `inputs.token` select, or the `inputs["token"]` index
// with a constant key — and empty for anything else, including a `has(inputs.x)`
// existence test, which is a guard rather than a surfacing.
func inputReference(e *expr.Expr) string {
	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_SelectExpr:
		sel := kind.SelectExpr
		if sel.GetTestOnly() {
			// `has(inputs.token)` — presence, not the value.
			return ""
		}
		if identName(sel.GetOperand()) == v1.InputsRoot {
			return sel.GetField()
		}
	case *expr.Expr_CallExpr:
		call := kind.CallExpr
		if call.GetFunction() == "_[_]" && len(call.GetArgs()) == 2 {
			if identName(call.GetArgs()[0]) == v1.InputsRoot {
				return stringConstant(call.GetArgs()[1])
			}
		}
	}
	return ""
}

// isStringConcat reports whether e is a binary `+`, the operator string
// concatenation compiles to.
func isStringConcat(e *expr.Expr) bool {
	call, ok := e.GetExprKind().(*expr.Expr_CallExpr)
	return ok && call.CallExpr.GetFunction() == "_+_" && len(call.CallExpr.GetArgs()) == 2
}

// flattenConcat unwraps a `+` chain into its operands, left to right. An operand
// that is not itself a `+` is returned whole — so `"a" + inputs.token + "b"`
// yields the three leaves, and a call other than the one below is returned as the
// call it is, never descended into: a value inside a call is derived, and this
// reports only what a `+` puts in the clear.
//
// The one exception is `string(x)` in an operand position, which is unwrapped to
// x. That is not a softening of the derived rule; it is what keeps the rule
// meaning the same thing after #413. A message written `token: ${inputs.token}`
// is interpolation, and interpolation desugars to `'token: ' + string(inputs.token)`
// — the `string()` is the compiler's, put there because CEL's `+` needs two
// strings, not the author's. Reading it as a derivation would mean the plainest
// possible spelling of this leak, the one the lint exists to catch, stopped being
// caught the moment the language grew a nicer way to write it.
//
// It follows that the same unwrapping applies to a `string()` an author typed
// inside a concatenation, since after compilation the two are the same
// expression and nothing can tell them apart. That is the right answer anyway:
// `${'token: ' + string(inputs.token)}` does put the value in the log verbatim.
// What is deliberately unchanged is `${string(inputs.token)}` written as the
// whole message, with no concatenation at all — the known, accepted gap this
// file's doc records — because that one never reaches here.
func flattenConcat(e *expr.Expr) []*expr.Expr {
	if isStringConcat(e) {
		args := e.GetExprKind().(*expr.Expr_CallExpr).CallExpr.GetArgs()
		return append(flattenConcat(args[0]), flattenConcat(args[1])...)
	}
	return []*expr.Expr{unwrapStringConversion(e)}
}

// unwrapStringConversion returns the argument of a global `string(x)` call, and e
// itself for anything else. See [flattenConcat] for why one call, and only in an
// operand position, is transparent here.
func unwrapStringConversion(e *expr.Expr) *expr.Expr {
	call, ok := e.GetExprKind().(*expr.Expr_CallExpr)
	if !ok {
		return e
	}
	c := call.CallExpr
	if c.GetTarget() != nil || c.GetFunction() != "string" || len(c.GetArgs()) != 1 {
		return e
	}
	return c.GetArgs()[0]
}

// identName returns the name of e when it is a bare identifier, empty otherwise.
func identName(e *expr.Expr) string {
	if id, ok := e.GetExprKind().(*expr.Expr_IdentExpr); ok {
		return id.IdentExpr.GetName()
	}
	return ""
}

// stringConstant returns the value of e when it is a string literal, empty
// otherwise — so `inputs["token"]` names token and `inputs[k]` names nothing.
func stringConstant(e *expr.Expr) string {
	if c, ok := e.GetExprKind().(*expr.Expr_ConstExpr); ok {
		if s, ok := c.ConstExpr.GetConstantKind().(*expr.Constant_StringValue); ok {
			return s.StringValue
		}
	}
	return ""
}
