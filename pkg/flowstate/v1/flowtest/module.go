package flowtest

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/google/cel-go/cel"
	celast "github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/operators"
	"github.com/google/cel-go/parser"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A module has no steps, so a case that names one as its `workflow:` has nothing
// to run. What it can do is use the module's own vocabulary directly, which is the
// only way to test a function or a scalar type without a workflow that imports
// it:
//
//   - `expect.check:` claims are read with the module's functions in scope, by
//     their bare names, as they are declared. A claim is expanded by the same
//     [v1.FunctionSet] the compiler expands a call with and evaluated by the same
//     evaluator every other claim is, so a function means here what it means
//     inlined into a workflow.
//   - `expect.types:` puts values to a scalar type: the case binds each as an
//     input declared with the type's base and rule, the shape the compiler lowers
//     an import of the type to, through [v1.BindRunInputs].
//
// Nothing else a case can say has a meaning without a run (inputs, stubs,
// signals, outputs, steps), so a module case that states any of it is refused
// rather than quietly ignored. There is no driver here to disagree with another:
// a function is plain CEL after inlining and both drivers evaluate that, so the
// local evaluator is the whole of what there is to test.

// maxModuleFailures bounds the failures one module case reports. The resource is
// lines in a report an author did not size: a table of two hundred values can
// fail every one.
const maxModuleFailures = 16

// moduleCaseFields are the exported fields of a [Test] and of its [Expectation]
// that a module case may state. Every other exported field must be zero, so a
// field added later is refused for a module until someone decides what it means
// there.
var (
	moduleTestFields        = map[string]bool{"Name": true, "Skip": true, "Workflow": true, "Expect": true}
	moduleExpectationFields = map[string]bool{"Check": true, "Types": true}
)

// moduleUnderTest is a parsed module with the two lookups a case needs, built
// once per suite and path.
type moduleUnderTest struct {
	spec      *v1.Workflow
	libs      []string
	functions *v1.FunctionSet
	// imported are the qualified names of functions the module takes from its own
	// `use:`. They are in functions so the module's bodies expand, and a claim may
	// not call them directly: they are not the module's to test.
	imported map[string]bool
	// types are the module's own scalar types by declared name. A carried
	// declaration (`alias.Name`, a type a module reached through another) is not
	// the module's to test and is not listed.
	types map[string]*v1.TypeDeclaration
	// err is why the module cannot be tested at all, reported on every case.
	err error
}

func newModuleUnderTest(spec *v1.Workflow) *moduleUnderTest {
	m := &moduleUnderTest{spec: spec, types: map[string]*v1.TypeDeclaration{}}

	profile := cmp.Or(spec.GetProfile(), v1.CurrentProfile)
	libs, err := v1.ProfileLibraries(profile)
	if err != nil {
		m.err = fmt.Errorf("resolving the module's profile: %w", err)
		return m
	}
	m.libs = libs

	// Every declaration, imported ones included, as the compiler builds the set: a
	// body here may call `ids.isUuid`, and leaving it out would make the module's
	// own function an unchecked name.
	m.imported = map[string]bool{}
	for _, f := range spec.GetDeclaredFunctions() {
		if v1.IsCarried(f.GetName()) {
			m.imported[f.GetName()] = true
		}
	}
	set, errs := v1.NewFunctionSet(profile, spec.GetDeclaredFunctions())
	if len(errs) > 0 {
		m.err = fmt.Errorf("the module's functions do not check: %w", errs[0])
		return m
	}
	m.functions = set

	for _, t := range spec.GetDeclaredTypes() {
		if !v1.IsCarried(t.GetName()) {
			m.types[t.GetName()] = t
		}
	}

	return m
}

// run produces one module case's verdict. The caller owns everything that is the
// suite's and not the case's: placing failures in the file, the redaction posture,
// the budget on warnings, and the fail-fast decision.
func (m *moduleUnderTest) run(ctx context.Context, test *Test, vars fileVars) *v1.TestCase {
	started := time.Now()
	result := &v1.TestCase{Name: test.Name}
	defer func() { result.Duration = durationpb.New(time.Since(started)) }()

	if m.err != nil {
		result.Error = m.err.Error()
		return result
	}
	if err := refuseNonModuleClaims(test); err != nil {
		result.Error = err.Error()
		return result
	}
	if values := typeValues(test.Expect.Types); values > MaxModuleValuesPerTest {
		result.Error = fmt.Sprintf("puts %d values to its types, more than the limit of %d", values, MaxModuleValuesPerTest)
		return result
	}

	if len(test.Expect.Check) == 0 && typeValues(test.Expect.Types) == 0 {
		// A type named with no values, or an empty claim list, asserts nothing;
		// passing it would be a green that checked nothing.
		result.Error = "claims nothing: state at least one `expect.check:` claim or one value under `expect.types.<Type>.admits:` / `refuses:`"
		return result
	}

	if name := emptyTypeClaim(test.Expect.Types); name != "" {
		result.Error = fmt.Sprintf("expect.types.%s names no value to admit or refuse", name)
		return result
	}

	failures := m.assertChecks(ctx, test.Expect.Check, vars)
	failures = append(failures, m.assertTypes(test.Expect.Types)...)
	if len(failures) > maxModuleFailures {
		more := len(failures) - maxModuleFailures
		failures = append(failures[:maxModuleFailures:maxModuleFailures],
			&v1.Diagnostic{Field: "expect", Message: fmt.Sprintf("(and %d more failures)", more)})
	}
	result.Failures = failures
	result.Passed = len(failures) == 0

	return result
}

// refuseNonModuleClaims names everything a case states that a module cannot act
// on, in the words of the file.
func refuseNonModuleClaims(test *Test) error {
	var unsupported []string
	collect := func(prefix string, v reflect.Value, allowed map[string]bool) {
		for i := range v.NumField() {
			field := v.Type().Field(i)
			if field.IsExported() && !allowed[field.Name] && !v.Field(i).IsZero() {
				key, _, _ := strings.Cut(field.Tag.Get("yaml"), ",")
				unsupported = append(unsupported, prefix+cmp.Or(key, strings.ToLower(field.Name)))
			}
		}
	}
	collect("", reflect.ValueOf(*test), moduleTestFields)
	collect("expect.", reflect.ValueOf(test.Expect), moduleExpectationFields)
	if len(unsupported) == 0 {
		return nil
	}
	slices.Sort(unsupported)

	return fmt.Errorf("the workflow is a module, which has no steps to run, so a case can state only `expect.check:` and `expect.types:`; "+
		"this one also states %s", strings.Join(unsupported, ", "))
}

// typeValues counts the values a case puts to its types.
func typeValues(types map[string]TypeClaim) int {
	n := 0
	for _, claim := range types {
		n += len(claim.Admits) + len(claim.Refuses)
	}

	return n
}

// assertChecks evaluates each claim with the module's functions inlined.
func (m *moduleUnderTest) assertChecks(ctx context.Context, claims []CheckClaim, vars fileVars) []*v1.Diagnostic {
	ev := v1.DefaultEvaluator()
	// The file's vars bind as they do for any check (postRunExtras).
	activation := map[string]any{"vars": map[string]any{}}
	if len(vars.values) > 0 {
		activation["vars"] = vars.values
	}
	var failures []*v1.Diagnostic
	for i, claim := range claims {
		field := fmt.Sprintf("expect.check[%d]", i)

		if name, ok := m.importedCall(ev, claim.That); ok {
			failures = append(failures, &v1.Diagnostic{Field: field,
				Message: fmt.Sprintf("check calls %s, which the module imports; a module's cases call its own functions, "+
					"so test %s in the module that declares it: %s", name, name, claim.That)})
			continue
		}
		expanded, _, err := m.functions.ExpandText(claim.That)
		if err != nil {
			failures = append(failures, &v1.Diagnostic{Field: field,
				Message: fmt.Sprintf("check could not call the module's functions: %s\n           %s", claim.That, err)})
			continue
		}
		out, err := ev.EvalString(ctx, expanded, m.libs, activation)
		if err != nil {
			failures = append(failures, &v1.Diagnostic{Field: field,
				Message: fmt.Sprintf("check errored: %s\n           %s", claim.That,
					checkErrorText(ev, m.libs, err, claim.That, vars.withheld, sensitiveInputs{}))})
			continue
		}
		held, ok := out.Value().(bool)
		if !ok {
			failures = append(failures, &v1.Diagnostic{Field: field,
				Message: fmt.Sprintf("check must evaluate to a boolean, got %s: %s", out.Type(), claim.That)})
			continue
		}
		if held {
			continue
		}

		message := "check failed: " + claim.That
		if claim.Because != "" {
			message += "\n           because: " + claim.Because
		}
		// Not for a claim reading a var the file withholds: the value would derive from it.
		if _, reads := claimReadsWithheld(ev, m.libs, claim.That, vars.withheld); !reads {
			if got, ok := m.comparedValue(ctx, ev, claim.That, activation); ok {
				message += "\n           " + got
			}
		}
		failures = append(failures, &v1.Diagnostic{Field: field, Message: message})
	}

	return failures
}

// comparedValue says what the left side of a failed `==` or `!=` claim came to,
// which for `clamp(40, 1, 5) == 4` is the fact the author needs: the function
// answered 5. A claim of any other shape has no single value to name.
func (m *moduleUnderTest) comparedValue(ctx context.Context, ev *v1.Evaluator, claim string, activation map[string]any) (string, bool) {
	env, err := ev.Env(m.libs...)
	if err != nil {
		return "", false
	}
	parsed, issues := env.Parse(claim)
	if issues != nil && issues.Err() != nil {
		return "", false
	}
	native := parsed.NativeRep()
	root := native.Expr()
	if root.Kind() != celast.CallKind {
		return "", false
	}
	call := root.AsCall()
	if (call.FunctionName() != operators.Equals && call.FunctionName() != operators.NotEquals) || len(call.Args()) != 2 {
		return "", false
	}
	left, err := parser.Unparse(call.Args()[0], native.SourceInfo())
	if err != nil {
		return "", false
	}
	expanded, _, err := m.functions.ExpandText(left)
	if err != nil {
		return "", false
	}
	out, err := ev.EvalString(ctx, expanded, m.libs, activation)
	if err != nil {
		return "", false
	}
	lit, err := cel.RefValueToValue(out)
	if err != nil {
		return "", false
	}
	got, err := literalToGo(lit)
	if err != nil {
		return "", false
	}

	return fmt.Sprintf("%s = %s", left, redactedScalarText(got, sensitiveInputs{})), true
}

// assertTypes puts each claimed value to its type, in the order the types and
// values were written (types by name, since a map has no order).
func (m *moduleUnderTest) assertTypes(claims map[string]TypeClaim) []*v1.Diagnostic {
	var failures []*v1.Diagnostic
	for _, name := range slices.Sorted(maps.Keys(claims)) {
		claim := claims[name]
		field := "expect.types." + name

		declared, ok := m.types[name]
		if !ok {
			failures = append(failures, &v1.Diagnostic{Field: field,
				Message: fmt.Sprintf("the module declares no type %q%s", name, m.typeHint())})
			continue
		}
		if !declared.IsScalar() {
			failures = append(failures, &v1.Diagnostic{Field: field,
				Message: fmt.Sprintf("type %q is a record, and only a constrained scalar type can be put values; "+
					"test a record's rules through a workflow that takes it", name)})
			continue
		}

		for _, value := range claim.Admits {
			if err := m.admits(declared, value); err != nil {
				failures = append(failures, &v1.Diagnostic{Field: field + ".admits",
					Message: fmt.Sprintf("type %s must admit %s, but refused it: %s", name, typedText(value, sensitiveInputs{}), err)})
			}
		}
		for _, value := range claim.Refuses {
			if err := m.admits(declared, value); err == nil {
				failures = append(failures, &v1.Diagnostic{Field: field + ".refuses",
					Message: fmt.Sprintf("type %s must refuse %s, but admitted it (its rule is `%s`)",
						name, typedText(value, sensitiveInputs{}), cmp.Or(declared.GetMustSource(), declared.GetMust()))})
			}
		}
	}

	return failures
}

func (m *moduleUnderTest) typeHint() string {
	names := slices.Sorted(maps.Keys(m.types))
	if len(names) == 0 {
		return "; it declares no types"
	}

	return "; it declares " + strings.Join(names, ", ")
}

// admits binds value as the input a use of the type lowers to and answers the
// refusal a run would have been given, or nil when the value is admitted.
func (m *moduleUnderTest) admits(declared *v1.TypeDeclaration, value any) error {
	name := declared.GetName()
	probe := &v1.Workflow{
		Name:    name,
		Profile: m.spec.GetProfile(),
		DeclaredInputs: []*v1.InputDeclaration{{
			Name:     name,
			Type:     declared.GetBase(),
			Must:     proto.String(declared.GetMust()),
			Required: true,
		}},
	}
	_, err := v1.BindRunInputs(probe, map[string]*v1.Value{name: v1.NewValue(value)})

	return err
}

// emptyTypeClaim is the first type (by name) a case names with no value to admit
// or refuse, or "".
func emptyTypeClaim(types map[string]TypeClaim) string {
	for _, name := range slices.Sorted(maps.Keys(types)) {
		if claim := types[name]; len(claim.Admits)+len(claim.Refuses) == 0 {
			return name
		}
	}

	return ""
}

// importedCall names the first call in claim to a function the module imports
// through its own `use:` (`ids.isUuid(x)`), which a case against this module does
// not test.
func (m *moduleUnderTest) importedCall(ev *v1.Evaluator, claim string) (string, bool) {
	if len(m.imported) == 0 {
		return "", false
	}
	env, err := ev.Env(m.libs...)
	if err != nil {
		return "", false
	}
	parsed, issues := env.Parse(claim)
	if issues != nil && issues.Err() != nil {
		return "", false
	}
	found := ""
	celast.PreOrderVisit(parsed.NativeRep().Expr(), celast.NewExprVisitor(func(e celast.Expr) {
		if found != "" || e.Kind() != celast.CallKind {
			return
		}
		call := e.AsCall()
		if !call.IsMemberFunction() || call.Target().Kind() != celast.IdentKind {
			return
		}
		if name := v1.QualifiedName(call.Target().AsIdent(), call.FunctionName()); m.imported[name] {
			found = name
		}
	}))

	return found, found != ""
}
