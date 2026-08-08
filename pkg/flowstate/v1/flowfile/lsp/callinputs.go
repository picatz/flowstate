package lsp

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/sourcegraph/go-lsp"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A `call:` step binds its arguments under `with:`, and what those arguments may
// be named is written down in another file. That makes `with:` the one block in a
// Flowfile whose keys the editor cannot answer for from anything it already has
// open: a task's inputs come from the registry, a step's own keys come from the
// grammar, and a call's arguments come from the callee's `inputs:` declarations.
//
// So completion and hover here read that file, under the three rules
// go-to-definition already follows, and one of its own:
//
//   - The path is resolved by [flowfile.ResolveCallTarget] and nothing else, so
//     the names offered are the callee's the compiler would type-check against.
//     See callDefinition for why a second path rule is the failure to avoid.
//   - The read is bounded by [maxDocumentBytes], the bound an open document
//     gets. A callee is a file an outside party chose, and nothing about being
//     on the far end of a `call:` makes it smaller.
//   - Nothing is offered on a target that does not resolve, is not there, or does
//     not compile. A guess about what another file declares is worse than an empty
//     menu: it is a name the compiler will refuse, suggested by the tool that is
//     supposed to know.
//   - A `digest:` pin is deliberately not consulted. A mismatch means the callee
//     changed since the pin was written, which is a diagnostic the validator
//     already raises against the pin, and the author fixing it is reading the
//     file as it is now. An editor that went silent on the mismatch would remove
//     the help exactly when it is being used, so completion and hover answer from
//     the bytes on disk either way.
//
// The reads are on an explicit completion or hover request rather than on every
// keystroke of a diagnostic pass, which is the same distinction that keeps the
// stat in callDefinition on the right side of the rule keeping I/O out of the
// validator.

// readCalleeSource reads the file a `call:` names, bounded the way an open
// document is.
//
// One reader for every question asked about a callee, where its `name:` is and
// what inputs it declares, so a bound tightened for one is tightened for both.
func readCalleeSource(path string) ([]byte, bool) {
	f, err := os.Open(path)
	if err != nil {
		return nil, false
	}
	defer f.Close()

	// One byte past the bound, so that a file at exactly the limit is read whole
	// and one above it is recognizable as over rather than silently truncated
	// into a document that parses as something its author did not write.
	data, err := io.ReadAll(io.LimitReader(f, maxDocumentBytes+1))
	if err != nil || len(data) > maxDocumentBytes {
		return nil, false
	}
	return data, true
}

// A calledWorkflow is a resolved `call:` target: the workflow the callee compiles
// to, and where on disk it was read from.
type calledWorkflow struct {
	workflow *v1.Workflow
	path     string
}

// callee resolves the target a `call:` names and compiles it, or reports false
// when there is nothing an editor may answer from.
//
// Compiled through [flowfile.ParseAt] rather than read by a reader of this
// package's own, because what is wanted is the callee's *declarations*: a type,
// whether it is required, a default, a `must:`. Those are what the grammar says
// they are. A second reader of `inputs:` here is how an editor comes to
// describe an input differently from the compiler that checks the argument.
func callee(doc *document, target string) (calledWorkflow, bool) {
	if target == "" {
		return calledWorkflow{}, false
	}

	callerPath, ok := doc.filesystemPath()
	if !ok {
		// An untitled buffer has no directory a relative path could mean anything
		// against, the same answer callDefinition gives it.
		return calledWorkflow{}, false
	}

	located := flowfile.ResolveCallTarget(callerPath, target)
	if located.Refusal != flowfile.CallTargetResolved {
		return calledWorkflow{}, false
	}

	data, ok := readCalleeSource(located.Path)
	if !ok {
		return calledWorkflow{}, false
	}

	workflow, _, err := flowfile.ParseAt(data, located.Path)
	if err != nil || workflow == nil {
		// A callee too broken to compile declares nothing this can be sure of.
		// Go-to-definition still navigates there, because arriving in a broken
		// file is useful; offering names read out of one is not.
		return calledWorkflow{}, false
	}

	return calledWorkflow{workflow: workflow, path: located.Path}, true
}

// input returns the callee's declaration of one input, or nil when it declares no
// such name.
func (c calledWorkflow) input(name string) *v1.InputDeclaration {
	for _, declaration := range c.workflow.GetDeclaredInputs() {
		if declaration.GetName() == name {
			return declaration
		}
	}
	return nil
}

// hoverCallArgument describes the `with:` key at a position from the callee's own
// declaration of that input.
//
// Nothing is said about a key the callee does not declare. The compiler already
// reports it, naming the inputs the workflow does take, and a popup inventing a
// meaning for it would contradict the diagnostic sitting on the same key. That is
// the rule an undeclared task input follows a few lines up in hoverAt.
func hoverCallArgument(doc *document, step *parsedStep, pos lsp.Position) *lsp.Hover {
	if step.withEntry == nil || step.callEntry == nil {
		return nil
	}

	for _, argument := range nestedEntries(step.withEntry) {
		if !contains(argument.keyRange, pos) {
			continue
		}
		called, ok := callee(doc, step.callEntry.valueText())
		if !ok {
			return nil
		}
		declaration := called.input(argument.key)
		if declaration == nil {
			return nil
		}
		return markdownHover(callInputDoc(declaration, called), argument.keyRange)
	}
	return nil
}

// callArgumentCandidates offers the inputs the callee declares, required ones
// first, omitting those this `with:` has already bound.
//
// Already-bound names are left out for the reason a task's written inputs are:
// the menu is a list of what is left to write. The name being typed is the
// exception, because a client re-requests completion on a key it has just
// completed and the candidate disappearing under the cursor reads as the editor
// losing track of the language.
func callArgumentCandidates(doc *document, step *outlineStep, prefix string, replace lsp.Range) []lsp.CompletionItem {
	if step == nil {
		return nil
	}
	called, ok := callee(doc, step.callTarget)
	if !ok {
		return nil
	}

	bound := make(map[string]bool, len(step.withKeys))
	for _, k := range step.withKeys {
		bound[k] = true
	}

	var items []lsp.CompletionItem
	for i, declaration := range called.workflow.GetDeclaredInputs() {
		name := declaration.GetName()
		if !strings.HasPrefix(name, prefix) || (bound[name] && name != prefix) {
			continue
		}
		order := "1"
		if mustBeBound(declaration) {
			order = "0"
		}
		items = append(items, lsp.CompletionItem{
			Label:         name,
			Kind:          lsp.CIKProperty,
			Detail:        callInputDetail(declaration),
			Documentation: plainText(callInputDoc(declaration, called)),
			SortText:      order + fmt.Sprintf("%04d", i) + name,
			// The colon is included for the reason an input key's is: the key is
			// never written without one.
			TextEdit: &lsp.TextEdit{Range: replace, NewText: name + ": "},
		})
	}
	return items
}

// mustBeBound reports whether `with:` has to bind an input for the call to
// compile.
//
// The compiler's own rule, from [flowfile]'s call step: a required input with a
// default is already answered for, so only a required one without a default is
// an argument the author still owes. Sorting by it is what puts the names a call
// cannot do without at the top of the menu.
func mustBeBound(declaration *v1.InputDeclaration) bool {
	return declaration.GetRequired() && declaration.GetDefault() == nil
}

// callInputDetail renders the one-line summary beside a candidate: what the
// argument must be, and whether the call needs it.
func callInputDetail(declaration *v1.InputDeclaration) string {
	detail := v1.DeclaredTypeName(declaration.GetType())
	switch {
	case mustBeBound(declaration):
		detail += " (required)"
	case declaration.GetDefault() != nil:
		if text, ok := declaredValueText(declaration.GetDefault()); ok {
			detail += " (default " + text + ")"
		} else {
			detail += " (has a default)"
		}
	}
	if description := declaration.GetDescription(); description != "" {
		detail += " · " + description
	}
	return detail
}

// callInputDoc renders one of the callee's input declarations: what a value for
// it must be, whether the call has to supply one, the constraint it is held to,
// and which file said so.
//
// The path is part of the answer rather than decoration. An argument's meaning is
// written in another file, and a reader hovering a `with:` key is being told
// something they cannot see on the screen in front of them, and naming where it
// came from is what makes the answer checkable.
func callInputDoc(declaration *v1.InputDeclaration, called calledWorkflow) string {
	var b strings.Builder
	fmt.Fprintf(&b, "**`%s`** · `%s`", declaration.GetName(), v1.DeclaredTypeName(declaration.GetType()))
	if declaration.GetRequired() {
		b.WriteString(" · required")
	} else {
		b.WriteString(" · optional")
	}
	if declaration.GetDefault() != nil {
		if text, ok := declaredValueText(declaration.GetDefault()); ok {
			fmt.Fprintf(&b, " · default `%s`", text)
		} else {
			b.WriteString(" · has a default")
		}
	}

	fmt.Fprintf(&b, "\n\nInput of workflow `%s`, declared in `%s`.",
		called.workflow.GetName(), called.path)

	if description := declaration.GetDescription(); description != "" {
		fmt.Fprintf(&b, "\n\n%s", description)
	}
	if must := declaration.GetMust(); must != "" {
		fmt.Fprintf(&b, "\n\nMust satisfy `%s`.", must)
	}
	return b.String()
}

// declaredValueText renders a declared literal, a default, for a popup, and
// reports false for one there is no short spelling of.
//
// Scalars only, deliberately. A list or a mapping written as a default is a
// document rather than a word, and a popup line is not where it is read; the
// sentence says there is one instead, which is the fact a reader is deciding on.
// It is also why this is not a fifth renderer of the same values: nothing here
// pretends to be the YAML the author wrote, only enough to recognize it by.
func declaredValueText(value *v1.Value) (string, bool) {
	literal := value.GetLiteral()
	if literal == nil {
		// Never an expression or a secret reference: a declaration's default is a
		// literal or the file does not compile, and this function is only ever
		// reached through one that did.
		return "", false
	}

	switch kind := literal.GetKind().(type) {
	case *expr.Value_StringValue:
		return strconv.Quote(kind.StringValue), true
	case *expr.Value_Int64Value:
		return strconv.FormatInt(kind.Int64Value, 10), true
	case *expr.Value_Uint64Value:
		return strconv.FormatUint(kind.Uint64Value, 10), true
	case *expr.Value_DoubleValue:
		return strconv.FormatFloat(kind.DoubleValue, 'g', -1, 64), true
	case *expr.Value_BoolValue:
		return strconv.FormatBool(kind.BoolValue), true
	case *expr.Value_NullValue:
		return "null", true
	}
	return "", false
}
