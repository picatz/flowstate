package flowstatev1

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/google/cel-go/cel"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// MaxFunctions is the most functions one workflow declares. It matches the
// schema's bound on [Workflow.DeclaredFunctions] and is enforced again here for
// a specification that never passed the schema.
const MaxFunctions = 64

// MaxFunctionParameters is the most parameters one function takes.
const MaxFunctionParameters = 16

// A FunctionError is one reason a declaration cannot be used, naming the function
// it is about so a compiler can point at the definition rather than at a use.
type FunctionError struct {
	// Function is the declared name.
	Function string
	// Err says what is wrong with the definition, in the checker's own words where
	// the checker found it.
	Err error
}

func (e *FunctionError) Error() string { return fmt.Sprintf("function %q: %v", e.Function, e.Err) }
func (e *FunctionError) Unwrap() error { return e.Err }

// A FunctionSet is a workflow's declared functions, checked and ready to be
// inlined into the expressions that call them.
//
// It is the compile-time half of [FunctionDeclaration] and nothing in it exists at
// run time: [FunctionSet.Expand] replaces each call with the callee's body, binds
// the arguments once through `cel.bind`, and leaves an expression of ordinary CEL.
// There is one way to inline a computation and one place its bounds are stated.
//
// Immutable once built and safe for concurrent use.
type FunctionSet struct {
	profile      string
	checked      map[string]checkedFunction
	declarations []cel.EnvOption
}

// NewFunctionSet checks declared in the profile's environment.
//
// Each declaration is checked once: its body against its own parameters and
// declared result, so a mistake is reported at the definition one time and not at
// every use. A body sees its parameters and the profile and nothing else, and may
// call another declared function. A function that calls itself, directly or
// through others, is refused; the inliner has no way to write a recursive
// expansion, and that is the bound.
//
// The set holds every declaration that checked, and the errors say why the others
// did not, in declaration order. A caller that gets errors should still use the
// set: the functions that are fine remain callable, and a call to one that is not
// is reported as a call to an undeclared function.
func NewFunctionSet(profile string, declared []*FunctionDeclaration) (*FunctionSet, []*FunctionError) {
	if profile == "" {
		profile = CurrentProfile
	}
	set := &FunctionSet{profile: profile, checked: map[string]checkedFunction{}}
	if len(declared) == 0 {
		return set, nil
	}
	if len(declared) > MaxFunctions {
		return set, []*FunctionError{{Function: "", Err: fmt.Errorf("a workflow declares at most %d functions, this one declares %d", MaxFunctions, len(declared))}}
	}

	checker, err := newHelperChecker(profile)
	if err != nil {
		return set, []*FunctionError{{Err: err}}
	}
	checker.budget = MaxFunctionExpansionNodes
	reserved := profileFunctionNames(checker.base)

	byName := make(map[string]*FunctionDeclaration, len(declared))
	var errs []*FunctionError
	fail := func(name string, format string, args ...any) {
		errs = append(errs, &FunctionError{Function: name, Err: fmt.Errorf(format, args...)})
	}
	for _, f := range declared {
		name := f.GetName()
		switch {
		case f == nil || name == "":
			fail(name, "has no name")
		case byName[name] != nil:
			fail(name, "is declared more than once")
		case reserved[name]:
			fail(name, "is a name the language already has; a function cannot replace or overload it, so choose another name")
		case len(f.GetParameters()) > MaxFunctionParameters:
			fail(name, "takes %d parameters; the most a function takes is %d", len(f.GetParameters()), MaxFunctionParameters)
		case f.GetBody().GetExpr() == nil:
			fail(name, "has no body")
		default:
			byName[name] = f
		}
	}

	// Dependencies first: a body is checked in an environment that declares the
	// functions it calls, so those have to have been checked before it.
	order, cycles := orderFunctions(declared, byName)
	broken := map[string]bool{}
	for _, cycle := range cycles {
		fail(cycle[0], "is recursive: %s; a function cannot call itself, directly or through another, because an inlined call has no end to expand to", strings.Join(append(slices.Clone(cycle), cycle[0]), " calls "))
		for _, name := range cycle {
			broken[name] = true
		}
	}
	for _, name := range order {
		f := byName[name]
		if callee, bad := brokenCallee(f, broken); bad {
			broken[name] = true
			fail(name, "calls %q, which is not a valid function", callee)
			continue
		}
		definition := helperDefinition{
			name:       name,
			label:      fmt.Sprintf("function %q", name),
			result:     CELType(f.GetResult()),
			resultName: TypeString(f.GetResult()),
			body:       f.GetBody(),
		}
		for _, parameter := range f.GetParameters() {
			definition.params = append(definition.params, helperParameter{name: parameter.GetName(), typ: CELType(parameter.GetType())})
		}
		if err := checker.add(definition); err != nil {
			broken[name] = true
			errs = append(errs, &FunctionError{Function: name, Err: trimFunctionLabel(err, definition.label)})
		}
	}

	set.checked = checker.checked
	set.declarations = checker.declarations
	// Reported in the order the functions were written, whatever order they were
	// checked in.
	index := make(map[string]int, len(declared))
	for i, f := range declared {
		if _, seen := index[f.GetName()]; !seen {
			index[f.GetName()] = i
		}
	}
	slices.SortStableFunc(errs, func(a, b *FunctionError) int { return index[a.Function] - index[b.Function] })
	return set, errs
}

// trimFunctionLabel drops the leading `function "name" ` an error from the shared
// checker starts with, since a [FunctionError] already names the function.
func trimFunctionLabel(err error, label string) error {
	msg := strings.TrimPrefix(err.Error(), label+" ")
	if msg == err.Error() {
		return err
	}
	return errors.New(msg)
}

// Names returns the declared names in the set, sorted.
func (s *FunctionSet) Names() []string {
	if s == nil {
		return nil
	}
	return slices.Sorted(maps.Keys(s.checked))
}

// Calls reports whether parsed calls any function in the set.
func (s *FunctionSet) Calls(parsed *exprpb.ParsedExpr) bool {
	if s == nil || len(s.checked) == 0 {
		return false
	}
	found := false
	walkParsed(parsed.GetExpr(), func(e *exprpb.Expr) {
		if call := e.GetCallExpr(); call != nil && call.GetTarget() == nil {
			if _, ok := s.checked[call.GetFunction()]; ok {
				found = true
			}
		}
	})
	return found
}

// Declarations returns the typed signatures of the set's functions as environment
// options, so a checker can judge a call as written, argument types and all.
func (s *FunctionSet) Declarations() []cel.EnvOption {
	if s == nil {
		return nil
	}
	return slices.Clone(s.declarations)
}

// Retains reports whether parsed holds a call to a function in the set as the
// macro call an expansion recorded ([FunctionSet.Expand]); an expression that was
// never expanded holds the call itself and is [FunctionSet.Calls]'s question.
//
// What a checker does with it: unparsing an expression that retains calls writes
// them back as the author wrote them, `slug(inputs.title)`, and that text checks
// against [FunctionSet.Declarations] with whatever types the file states for the
// names it reads, which the expanded tree, a `cel.bind` over untyped arguments,
// cannot.
func (s *FunctionSet) Retains(parsed *exprpb.ParsedExpr) bool {
	if s == nil || len(s.checked) == 0 {
		return false
	}
	for _, e := range parsed.GetSourceInfo().GetMacroCalls() {
		if call := e.GetCallExpr(); call != nil && call.GetTarget() == nil {
			if _, ok := s.checked[call.GetFunction()]; ok {
				return true
			}
		}
	}
	return false
}

// Expand replaces every call to a function in the set, in the expression value
// holds, with that function's body. A value that is not an expression, or calls
// none, is left exactly as it was.
//
// The expression is checked against the declared signatures first, so an argument
// of a type the parameter cannot take is refused here with the checker's sentence.
// Names the expression reads that the set does not declare are taken to exist, as
// `dyn`, because whether they do is the reference walk's question and not this
// one's. The expansion is bounded by the number of calls and by the size of what
// they expand to; a refusal leaves value unchanged.
//
// It returns the node count of the expression it produced, 0 when it changed
// nothing, so a compiler can keep one budget across every expression of a file:
// the per-expression bound alone lets a few hundred small uses of a large
// composed function spend gigabytes.
func (s *FunctionSet) Expand(value *Value) (int, error) {
	if !s.Calls(value.GetExpr()) {
		return 0, nil
	}
	if err := expandHelpersInValue(s.profile, value, s.checked, s.declarations); err != nil {
		return 0, err
	}
	nodes := 0
	walkParsed(value.GetExpr().GetExpr(), func(*exprpb.Expr) { nodes++ })

	return nodes, nil
}

// MaxFunctionExpansionNodes is the most CEL nodes the expansions in one file may
// add up to. A compiler holds it as a budget across every expression it expands.
const MaxFunctionExpansionNodes = 100_000

// profileFunctionNames is every name the profile gives a function or a macro,
// which a declared function may not take.
func profileFunctionNames(env *cel.Env) map[string]bool {
	names := map[string]bool{}
	for name := range env.Functions() {
		names[name] = true
	}
	for _, m := range env.Macros() {
		names[m.Function()] = true
	}
	return names
}

// brokenCallee returns a function f calls that did not check.
func brokenCallee(f *FunctionDeclaration, broken map[string]bool) (string, bool) {
	var callee string
	walkParsed(f.GetBody().GetExpr(), func(e *exprpb.Expr) {
		if call := e.GetCallExpr(); callee == "" && call != nil && call.GetTarget() == nil && broken[call.GetFunction()] {
			callee = call.GetFunction()
		}
	})
	return callee, callee != ""
}

// orderFunctions returns the names in an order where every function follows the
// ones it calls, and the cycles it found, each as the names along it starting at
// the first one reached. A function on a cycle is left out of the order; one that
// only calls into a cycle is kept, and its caller finds the callee broken.
func orderFunctions(declared []*FunctionDeclaration, byName map[string]*FunctionDeclaration) (order []string, cycles [][]string) {
	const (
		unvisited = iota
		inProgress
		done
	)
	state := make(map[string]int, len(byName))
	onCycle := map[string]bool{}
	var path []string

	var visit func(name string)
	visit = func(name string) {
		state[name] = inProgress
		path = append(path, name)
		for _, callee := range calledFunctions(byName[name], byName) {
			switch state[callee] {
			case unvisited:
				visit(callee)
			case inProgress:
				cycle := slices.Clone(path[slices.Index(path, callee):])
				cycles = append(cycles, cycle)
				for _, member := range cycle {
					onCycle[member] = true
				}
			}
		}
		path = path[:len(path)-1]
		state[name] = done
		if !onCycle[name] {
			order = append(order, name)
		}
	}
	for _, f := range declared {
		if _, ok := byName[f.GetName()]; ok && state[f.GetName()] == unvisited {
			visit(f.GetName())
		}
	}
	return order, cycles
}

// calledFunctions returns the declared functions f's body calls, in the order a
// reader meets them.
func calledFunctions(f *FunctionDeclaration, byName map[string]*FunctionDeclaration) []string {
	var callees []string
	walkParsed(f.GetBody().GetExpr(), func(e *exprpb.Expr) {
		call := e.GetCallExpr()
		if call == nil || call.GetTarget() != nil {
			return
		}
		if _, ok := byName[call.GetFunction()]; ok && !slices.Contains(callees, call.GetFunction()) {
			callees = append(callees, call.GetFunction())
		}
	})
	return callees
}

// walkParsed calls visit for every node of an expression tree, parents before
// children, iteratively so a deeply nested expression costs heap rather than stack.
func walkParsed(root *exprpb.Expr, visit func(*exprpb.Expr)) {
	if root == nil {
		return
	}
	stack := []*exprpb.Expr{root}
	for len(stack) > 0 {
		e := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if e == nil {
			continue
		}
		visit(e)
		switch kind := e.GetExprKind().(type) {
		case *exprpb.Expr_SelectExpr:
			stack = append(stack, kind.SelectExpr.GetOperand())
		case *exprpb.Expr_CallExpr:
			stack = append(stack, kind.CallExpr.GetTarget())
			stack = append(stack, kind.CallExpr.GetArgs()...)
		case *exprpb.Expr_ListExpr:
			stack = append(stack, kind.ListExpr.GetElements()...)
		case *exprpb.Expr_StructExpr:
			for _, entry := range kind.StructExpr.GetEntries() {
				stack = append(stack, entry.GetMapKey(), entry.GetValue())
			}
		case *exprpb.Expr_ComprehensionExpr:
			c := kind.ComprehensionExpr
			stack = append(stack, c.GetIterRange(), c.GetAccuInit(), c.GetLoopCondition(), c.GetLoopStep(), c.GetResult())
		}
	}
}
