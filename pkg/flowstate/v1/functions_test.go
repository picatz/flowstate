package flowstatev1_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func scalarOf(s v1.Type_Scalar) *v1.Type {
	return &v1.Type{Kind: &v1.Type_Scalar_{Scalar: s}}
}

func fn(name, body string, result *v1.Type, params ...*v1.FunctionParameter) *v1.FunctionDeclaration {
	return &v1.FunctionDeclaration{Name: name, Parameters: params, Result: result, Body: v1.NewExpr(body).GetExpr()}
}

func param(name string, t *v1.Type) *v1.FunctionParameter {
	return &v1.FunctionParameter{Name: name, Type: t}
}

var (
	tString = scalarOf(v1.Type_SCALAR_STRING)
	tInt    = scalarOf(v1.Type_SCALAR_INT)
	tBool   = scalarOf(v1.Type_SCALAR_BOOL)
)

// evalExpanded expands src through set and evaluates the result with no function
// declared anywhere, which is what a worker does.
func evalExpanded(t *testing.T, set *v1.FunctionSet, src string, activation map[string]any) any {
	t.Helper()
	value := v1.NewExpr(src)
	_, err := set.Expand(value)
	require.NoError(t, err)
	require.False(t, set.Calls(value.GetExpr()), "a declared name survived expansion")

	libs, err := v1.ProfileLibraries(v1.CurrentProfile)
	require.NoError(t, err)
	env, err := v1.DefaultEvaluator().Env(libs...)
	require.NoError(t, err)
	opts := make([]cel.EnvOption, 0, len(activation))
	for name := range activation {
		opts = append(opts, cel.Variable(name, cel.DynType))
	}
	env, err = env.Extend(opts...)
	require.NoError(t, err)
	out, err := v1.DefaultEvaluator().EvalParsed(t.Context(), env, value.GetExpr(), activation)
	require.NoError(t, err)

	native, err := out.ConvertToNative(reflect.TypeFor[*structpb.Value]())
	require.NoError(t, err)

	return native.(*structpb.Value).AsInterface()
}

func TestFunctionSetInlinesAndEvaluatesWithNoFunctionDeclared(t *testing.T) {
	set, errs := v1.NewFunctionSet("", []*v1.FunctionDeclaration{
		fn("slug", "s.trim().lowerAscii().replace(' ', '-')", tString, param("s", tString)),
	})
	require.Empty(t, errs)
	require.Equal(t, []string{"slug"}, set.Names())

	got := evalExpanded(t, set, "slug(inputs.title) + '/' + slug(' Other Thing ')", map[string]any{"inputs": map[string]any{"title": "  Hello World "}})
	require.Equal(t, "hello-world/other-thing", got)
}

func TestFunctionSetComposesAndOrdersByDependency(t *testing.T) {
	// `outer` is written first and calls `inner`, which is written after it.
	set, errs := v1.NewFunctionSet("", []*v1.FunctionDeclaration{
		fn("outer", "inner(n) + inner(n + 1)", tInt, param("n", tInt)),
		fn("inner", "n * 2", tInt, param("n", tInt)),
	})
	require.Empty(t, errs)
	require.EqualValues(t, 2*3+2*4, evalExpanded(t, set, "outer(3)", nil))
	require.EqualValues(t, (2*3+2*4)*10, evalExpanded(t, set, "outer(3) * 10", nil))
}

func TestFunctionSetRefusesRecursionAtTheDefinition(t *testing.T) {
	t.Run("itself", func(t *testing.T) {
		_, errs := v1.NewFunctionSet("", []*v1.FunctionDeclaration{
			fn("loopy", "loopy(n - 1)", tInt, param("n", tInt)),
		})
		require.Len(t, errs, 1)
		require.Equal(t, "loopy", errs[0].Function)
		require.ErrorContains(t, errs[0], "is recursive: loopy calls loopy")
	})

	t.Run("each other", func(t *testing.T) {
		set, errs := v1.NewFunctionSet("", []*v1.FunctionDeclaration{
			fn("ping", "pong(n)", tInt, param("n", tInt)),
			fn("pong", "ping(n)", tInt, param("n", tInt)),
			fn("fine", "n + 1", tInt, param("n", tInt)),
			fn("caller", "ping(n)", tInt, param("n", tInt)),
		})
		require.Len(t, errs, 2, "the cycle is reported once, and so is its caller")
		require.Equal(t, "ping", errs[0].Function)
		require.ErrorContains(t, errs[0], "ping calls pong calls ping")
		require.Equal(t, "caller", errs[1].Function)
		require.ErrorContains(t, errs[1], `calls "ping", which is not a valid function`)
		require.Equal(t, []string{"fine"}, set.Names(), "a function unrelated to the cycle stays callable")
	})
}

func TestFunctionSetReportsABodyOnceAtTheDefinition(t *testing.T) {
	set, errs := v1.NewFunctionSet("", []*v1.FunctionDeclaration{
		fn("bad", "n + 'x'", tInt, param("n", tInt)),
		fn("leaky", "inputs.secret", tString),
		fn("wrongResult", "n > 1", tInt, param("n", tInt)),
		fn("good", "n", tInt, param("n", tInt)),
	})
	require.Len(t, errs, 3)
	require.Equal(t, "bad", errs[0].Function)
	require.ErrorContains(t, errs[0], "body does not type-check")
	require.Equal(t, "leaky", errs[1].Function)
	require.ErrorContains(t, errs[1], "undeclared reference to 'inputs'")
	require.Equal(t, "wrongResult", errs[2].Function)
	require.ErrorContains(t, errs[2], "declares result int but its body produces bool")
	require.Equal(t, []string{"good"}, set.Names())
}

func TestFunctionSetRefusesANameTheProfileHas(t *testing.T) {
	for _, name := range []string{"size", "has", "string", "sum", "reduce", "sortBy"} {
		t.Run(name, func(t *testing.T) {
			set, errs := v1.NewFunctionSet("", []*v1.FunctionDeclaration{fn(name, "1", tInt)})
			require.Len(t, errs, 1)
			require.ErrorContains(t, errs[0], "a name the language already has")
			require.Empty(t, set.Names())
		})
	}
}

func TestFunctionSetRefusesDuplicatesAndTooManyParameters(t *testing.T) {
	_, errs := v1.NewFunctionSet("", []*v1.FunctionDeclaration{
		fn("twice", "1", tInt),
		fn("twice", "2", tInt),
	})
	require.Len(t, errs, 1)
	require.ErrorContains(t, errs[0], "declared more than once")

	params := make([]*v1.FunctionParameter, v1.MaxFunctionParameters+1)
	for i := range params {
		params[i] = param(fmt.Sprintf("p%d", i), tInt)
	}
	_, errs = v1.NewFunctionSet("", []*v1.FunctionDeclaration{fn("wide", "1", tInt, params...)})
	require.Len(t, errs, 1)
	require.ErrorContains(t, errs[0], "takes 17 parameters")

	_, errs = v1.NewFunctionSet("", []*v1.FunctionDeclaration{fn("dup", "a", tInt, param("a", tInt), param("a", tInt))})
	require.Len(t, errs, 1)
	require.ErrorContains(t, errs[0], `declares parameter "a" more than once`)
}

func TestFunctionSetChecksEachCallAgainstTheDeclaredSignature(t *testing.T) {
	set, errs := v1.NewFunctionSet("", []*v1.FunctionDeclaration{
		fn("twice", "n * 2", tInt, param("n", tInt)),
	})
	require.Empty(t, errs)

	for src, want := range map[string]string{
		"twice('x')":    "no matching overload",
		"twice(1, 2)":   "no matching overload",
		"twice() + 1":   "no matching overload",
		"twice(1) + ''": "no matching overload",
	} {
		value := v1.NewExpr(src)
		_, err := set.Expand(value)
		require.ErrorContains(t, err, want, src)
	}

	// An argument the file cannot type is taken to fit; the run decides.
	require.EqualValues(t, 14, evalExpanded(t, set, "twice(inputs.n)", map[string]any{"inputs": map[string]any{"n": int64(7)}}))
}

func TestFunctionSetEvaluatesArgumentsOnceAndInOrder(t *testing.T) {
	set, errs := v1.NewFunctionSet("", []*v1.FunctionDeclaration{
		fn("both", "a + a + b", tInt, param("a", tInt), param("b", tInt)),
	})
	require.Empty(t, errs)

	value := v1.NewExpr("both(1 + 2, 4)")
	_, err := set.Expand(value)
	require.NoError(t, err)
	text, err := cel.AstToString(cel.ParsedExprToAst(value.GetExpr()))
	require.NoError(t, err)
	require.Equal(t, 1, strings.Count(text, "1 + 2"), "an argument used twice by the body is written once: %s", text)
	require.EqualValues(t, 10, evalExpanded(t, set, "both(1 + 2, 4)", nil))
}

// A later argument is evaluated where an earlier parameter is already bound, so
// an argument spelled like that parameter must still read the caller's own name.
func TestFunctionSetArgumentCannotBeCapturedByAnEarlierParameter(t *testing.T) {
	set, errs := v1.NewFunctionSet("", []*v1.FunctionDeclaration{
		fn("sub", "numerator - denominator", tInt, param("numerator", tInt), param("denominator", tInt)),
	})
	require.Empty(t, errs)

	// Here `numerator` in the second argument is the loop's, which is 5, 6 or 7.
	got := evalExpanded(t, set, "[5, 6, 7].map(numerator, sub(100, numerator))", nil)
	require.Equal(t, []any{95.0, 94.0, 93.0}, got)

	got = evalExpanded(t, set, "[5, 6, 7].map(x, sub(x, [1].map(numerator, numerator + x)[0]))", nil)
	require.Equal(t, []any{-1.0, -1.0, -1.0}, got)
}

func TestFunctionSetBodyKeepsCELShortCircuiting(t *testing.T) {
	set, errs := v1.NewFunctionSet("", []*v1.FunctionDeclaration{
		fn("safeRatio", "d == 0 || n / d > 1", tBool, param("n", tInt), param("d", tInt)),
	})
	require.Empty(t, errs)
	require.Equal(t, true, evalExpanded(t, set, "safeRatio(1, 0)", nil))
	require.Equal(t, false, evalExpanded(t, set, "safeRatio(1, 3)", nil))
}

func TestFunctionSetNestedCallsAndMemberCalls(t *testing.T) {
	set, errs := v1.NewFunctionSet("", []*v1.FunctionDeclaration{
		fn("twice", "n * 2", tInt, param("n", tInt)),
	})
	require.Empty(t, errs)
	require.EqualValues(t, 12, evalExpanded(t, set, "twice(twice(3))", nil))
	require.EqualValues(t, 24, evalExpanded(t, set, "twice(twice(twice(3)))", nil))

	// `x.twice()` is a member call, which a global function is not.
	require.False(t, set.Calls(v1.NewExpr("inputs.twice()").GetExpr()))
}

func TestFunctionSetExpansionIsBoundedBeforeItIsBuilt(t *testing.T) {
	t.Run("calls in one expression", func(t *testing.T) {
		set, errs := v1.NewFunctionSet("", []*v1.FunctionDeclaration{fn("one", "n", tInt, param("n", tInt))})
		require.Empty(t, errs)
		calls := make([]string, 1025)
		for i := range calls {
			calls[i] = "one(1)"
		}
		value := v1.NewExpr("[" + strings.Join(calls, ",") + "]")
		before := value.GetExpr().String()
		_, err := set.Expand(value)
		require.ErrorContains(t, err, "more than 1024 times")
		require.Equal(t, before, value.GetExpr().String(), "a refused expansion changed the expression")
	})

	t.Run("a chain that doubles", func(t *testing.T) {
		declared := []*v1.FunctionDeclaration{fn("f0", "n + n", tInt, param("n", tInt))}
		for i := 1; i < 40; i++ {
			declared = append(declared, fn(fmt.Sprintf("f%d", i), fmt.Sprintf("f%d(n) + f%d(n + 1)", i-1, i-1), tInt, param("n", tInt)))
		}
		set, errs := v1.NewFunctionSet("", declared)
		require.NotEmpty(t, errs, "an exponential chain must be refused while the set is built")
		require.Regexp(t, "CEL nodes once the functions it calls are inlined|altogether", errs[0].Error())
		require.NotContains(t, set.Names(), "f39")
	})
}

func TestFunctionSetDeclaredCountIsBounded(t *testing.T) {
	declared := make([]*v1.FunctionDeclaration, v1.MaxFunctions+1)
	for i := range declared {
		declared[i] = fn(fmt.Sprintf("f%d", i), "1", tInt)
	}
	_, errs := v1.NewFunctionSet("", declared)
	require.Len(t, errs, 1)
	require.ErrorContains(t, errs[0], "at most 64 functions")
}

func TestFunctionSetLeavesAnExpressionWithoutCallsAlone(t *testing.T) {
	set, errs := v1.NewFunctionSet("", []*v1.FunctionDeclaration{fn("one", "1", tInt)})
	require.Empty(t, errs)
	value := v1.NewExpr("inputs.a.b + nosuch(1)")
	before := value.GetExpr().String()
	_, err := set.Expand(value)
	require.NoError(t, err)
	require.Equal(t, before, value.GetExpr().String())
	_, err = set.Expand(v1.NewLiteral("text"))
	require.NoError(t, err)
}

// The alias the expander binds an argument to must not be a name the caller wrote.
func TestFunctionSetAliasCannotCollideWithACallersIdentifier(t *testing.T) {
	set, errs := v1.NewFunctionSet("", []*v1.FunctionDeclaration{
		fn("sub", "numerator - x", tInt, param("numerator", tInt), param("x", tInt)),
		// The spelling the expander would pick for `sub`'s first parameter, written
		// by the author as a parameter of their own.
		fn("capture", "sub(100, numerator + __sub_1_numerator)", tInt, param("numerator", tInt), param("__sub_1_numerator", tInt)),
	})
	require.Empty(t, errs)

	got := evalExpanded(t, set, "capture(1, 10)", nil)
	require.EqualValues(t, 100-(1+10), got)
}

// Sixty-four wrappers each within the per-body bound are not within the file's.
func TestFunctionSetDefinitionsShareOneExpansionBudget(t *testing.T) {
	declared := []*v1.FunctionDeclaration{fn("f0", "["+strings.Repeat("n, ", 3900)+"n].size()", tInt, param("n", tInt))}
	for i := 1; i < 40; i++ {
		declared = append(declared, fn(fmt.Sprintf("f%d", i), fmt.Sprintf("f%d(n)", i-1), tInt, param("n", tInt)))
	}
	set, errs := v1.NewFunctionSet("", declared)
	require.NotEmpty(t, errs, "each wrapper is within its own bound and together they are not")
	require.ErrorContains(t, errs[0], "altogether")
	require.Less(t, len(set.Names()), len(declared))
	require.Contains(t, set.Names(), "f0")
}
