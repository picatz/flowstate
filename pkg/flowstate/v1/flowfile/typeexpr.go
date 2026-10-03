package flowfile

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/google/cel-go/cel"
	celast "github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/types"
	celref "github.com/google/cel-go/common/types/ref"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A type expression is how a Flowfile spells a [v1.Type]: the way CEL spells
// its own types, so `list(string)`, `map(string, int)`, `timestamp`. The
// decision and its reasons are D1 in docs/plans/2026-09-whole-system-review.md.
//
// It is parsed and checked by the one CEL parser, in a type environment that is
// distinct from every run environment. `list` and `map` are functions from
// types to types there and nowhere else, so a type value can never be written
// into a run expression: CEL erases parameters at run time, and
// `type(xs) == list(string)` would be false and read as a bug.

// typeEnvNames are the scalar and marker identifiers the type environment
// declares, in the order [FormatType] prefers when listing alternatives.
//
// `timestamp` and `duration` are declared here because the standard
// environment gives those words to conversion functions and names the types
// google.protobuf.Timestamp and google.protobuf.Duration.
var typeEnvIdents = map[string]*types.Type{
	"string":    types.StringType,
	"int":       types.IntType,
	"double":    types.DoubleType,
	"bool":      types.BoolType,
	"bytes":     types.BytesType,
	"timestamp": types.TimestampType,
	"duration":  types.DurationType,
	"null_type": types.NullType,
	"dyn":       types.DynType,
}

var typeEnv = sync.OnceValues(func() (*cel.Env, error) { return newTypeEnv(nil) })

// newTypeEnv builds the type environment, with each of records declared as one
// more type: the name of a record type is a type value there, the way `string`
// is, so `list(Order)` parses with the one parser and nothing else. The names
// are the file's own; the shared environment declares none.
func newTypeEnv(records map[string]bool) (*cel.Env, error) {
	opts := []cel.EnvOption{cel.ClearMacros()}
	for name, t := range typeEnvIdents {
		opts = append(opts, cel.Constant(name, cel.TypeType, t))
	}
	for name := range records {
		if _, builtin := typeEnvIdents[name]; builtin {
			continue
		}
		opts = append(opts, cel.Constant(name, cel.TypeType, types.NewObjectType(name)))
	}
	opts = append(opts,
		cel.Function("list",
			cel.Overload("list_of_type", []*cel.Type{cel.TypeType}, cel.TypeType,
				cel.UnaryBinding(func(elem celref.Val) celref.Val {
					t, ok := elem.(*types.Type)
					if !ok {
						return types.NewErr("list wants a type")
					}
					return types.NewListType(t)
				}))),
		cel.Function("map",
			cel.Overload("map_of_types", []*cel.Type{cel.TypeType, cel.TypeType}, cel.TypeType,
				cel.BinaryBinding(func(key, value celref.Val) celref.Val {
					k, kok := key.(*types.Type)
					v, vok := value.(*types.Type)
					if !kok || !vok {
						return types.NewErr("map wants types")
					}
					return types.NewMapType(k, v)
				}))),
	)
	return cel.NewCustomEnv(opts...)
}

// ParseType reads a type expression and returns the [v1.Type] it spells.
//
// `enum` is deliberately not an expression: its closed set lives in the
// declaration's `values:` sibling, so the declaration compiler owns that word.
// A map's key is always `string` ([v1.Type_Map] fixes it, because run values
// cross JSON boundaries), and any other key is refused here rather than
// dropped. The error names the offending column, one-based, of src.
func ParseType(src string) (*v1.Type, error) {
	return parseType(nil, src)
}

// parseType is [ParseType] in the type environment env, which a file that declares
// record types builds once ([newTypeEnv]) and reuses for every declaration; nil is
// the shared environment with no records.
func parseType(env *cel.Env, src string) (*v1.Type, error) {
	if strings.TrimSpace(src) == "" {
		return nil, errors.New("is empty; write a type such as `string` or `list(string)`")
	}
	if env == nil {
		var err error
		if env, err = typeEnv(); err != nil {
			return nil, fmt.Errorf("type environment: %w", err)
		}
	}
	ast, iss := env.Parse(src)
	if iss.Err() != nil {
		return nil, iss.Err()
	}
	checked, iss := env.Check(ast)
	if iss.Err() != nil {
		return nil, iss.Err()
	}
	if err := refuseBareContainers(checked); err != nil {
		return nil, err
	}
	prg, err := env.Program(checked)
	if err != nil {
		return nil, err
	}
	out, _, err := prg.Eval(map[string]any{})
	if err != nil {
		return nil, err
	}
	t, ok := out.(*types.Type)
	if !ok {
		return nil, fmt.Errorf("is not a type: %s", src)
	}
	return typeFromCEL(t)
}

// refuseBareContainers reports `list` and `map` written without their
// parameters. CEL's checker resolves both words as type identifiers whatever
// the environment declares, so the environment cannot refuse them; the
// expression can. The legacy bare words die at the edition boundary, and the
// message names what `flow fix` rewrites them to.
func refuseBareContainers(checked *cel.Ast) error {
	var err error
	// A map's key is validated here, where the key's own column is known; the
	// evaluated type has already lost it. Only the identifier `string` can
	// evaluate to the string type, so anything else in the key slot is refused.
	for _, e := range celast.MatchDescendants(celast.NavigateAST(checked.NativeRep()), celast.KindMatcher(celast.CallKind)) {
		call := e.AsCall()
		if call.FunctionName() != "map" || len(call.Args()) != 2 {
			continue
		}
		if key := call.Args()[0]; key.Kind() != celast.IdentKind || key.AsIdent() != "string" {
			err = errors.Join(err, fmt.Errorf("%s: a map's keys are strings; write map(string, ...)", position(checked, key)))
		}
	}
	for _, e := range celast.MatchDescendants(celast.NavigateAST(checked.NativeRep()), celast.KindMatcher(celast.IdentKind)) {
		switch name := e.AsIdent(); name {
		case "list":
			err = errors.Join(err, fmt.Errorf("%s: `list` needs its element type; write list(dyn) for a list of anything", position(checked, e)))
		case "map":
			err = errors.Join(err, fmt.Errorf("%s: `map` needs its types; write map(string, dyn) for a map of anything", position(checked, e)))
		}
	}
	return err
}

// position is e's line:column in the form cel-go's own diagnostics use.
func position(checked *cel.Ast, e celast.Expr) string {
	loc := checked.NativeRep().SourceInfo().GetStartLocation(e.ID())
	return fmt.Sprintf("%d:%d", loc.Line(), loc.Column()+1)
}

func typeFromCEL(t *types.Type) (*v1.Type, error) {
	scalar := func(s v1.Type_Scalar) *v1.Type {
		return &v1.Type{Kind: &v1.Type_Scalar_{Scalar: s}}
	}
	switch t.Kind() {
	case types.StringKind:
		return scalar(v1.Type_SCALAR_STRING), nil
	case types.IntKind:
		return scalar(v1.Type_SCALAR_INT), nil
	case types.DoubleKind:
		return scalar(v1.Type_SCALAR_DOUBLE), nil
	case types.BoolKind:
		return scalar(v1.Type_SCALAR_BOOL), nil
	case types.BytesKind:
		return scalar(v1.Type_SCALAR_BYTES), nil
	case types.TimestampKind:
		return scalar(v1.Type_SCALAR_TIMESTAMP), nil
	case types.DurationKind:
		return scalar(v1.Type_SCALAR_DURATION), nil
	case types.NullTypeKind:
		return scalar(v1.Type_SCALAR_NULL_TYPE), nil
	case types.DynKind:
		return &v1.Type{Kind: &v1.Type_Dyn{Dyn: true}}, nil
	case types.ListKind:
		elem, err := typeFromCEL(t.Parameters()[0])
		if err != nil {
			return nil, err
		}
		return &v1.Type{Kind: &v1.Type_List{List: elem}}, nil
	case types.MapKind:
		params := t.Parameters()
		if params[0].Kind() != types.StringKind {
			return nil, fmt.Errorf("a map's keys are strings, not %s; write map(string, %s)",
				params[0], params[1])
		}
		value, err := typeFromCEL(params[1])
		if err != nil {
			return nil, err
		}
		return &v1.Type{Kind: &v1.Type_Map_{Map: &v1.Type_Map{Value: value}}}, nil
	case types.StructKind:
		// Only a declared record name reaches here: it is the one object type the
		// type environment declares.
		return &v1.Type{Kind: &v1.Type_Message{Message: t.TypeName()}}, nil
	default:
		return nil, fmt.Errorf("%s is not a type a declaration can have", t)
	}
}

// FormatType prints t in the spelling [ParseType] reads, so that
// print-then-parse is the identity. A type with no Flowfile spelling — the
// enum marker, whose members belong to `values:`, and a message reserved for
// descriptor-backed types — is reported rather than printed as something else.
func FormatType(t *v1.Type) (string, error) {
	return formatType(t, v1.MaxStructureDepth)
}

// formatType prints t with depth levels of nesting left, so a type built past
// [v1.MaxStructureDepth], or one that points back at itself, is an error rather
// than a stack overflow.
func formatType(t *v1.Type, depth int) (string, error) {
	if depth < 0 {
		return "", fmt.Errorf("type nests deeper than %d levels", v1.MaxStructureDepth)
	}
	switch k := t.GetKind().(type) {
	case *v1.Type_Scalar_:
		switch k.Scalar {
		case v1.Type_SCALAR_STRING:
			return "string", nil
		case v1.Type_SCALAR_INT:
			return "int", nil
		case v1.Type_SCALAR_DOUBLE:
			return "double", nil
		case v1.Type_SCALAR_BOOL:
			return "bool", nil
		case v1.Type_SCALAR_BYTES:
			return "bytes", nil
		case v1.Type_SCALAR_TIMESTAMP:
			return "timestamp", nil
		case v1.Type_SCALAR_DURATION:
			return "duration", nil
		case v1.Type_SCALAR_NULL_TYPE:
			return "null_type", nil
		}
		return "", fmt.Errorf("scalar %s has no spelling", k.Scalar)
	case *v1.Type_Dyn:
		if !k.Dyn {
			return "", errors.New("dyn marker is false; the marker must be true")
		}
		return "dyn", nil
	case *v1.Type_List:
		elem, err := formatType(k.List, depth-1)
		if err != nil {
			return "", err
		}
		return "list(" + elem + ")", nil
	case *v1.Type_Map_:
		value, err := formatType(k.Map.GetValue(), depth-1)
		if err != nil {
			return "", err
		}
		return "map(string, " + value + ")", nil
	case *v1.Type_Enum:
		return "", errors.New("enum is spelled `enum` with its members in `values:`, not as a type expression")
	case *v1.Type_Message:
		// A record the file declares is spelled by its name. A descriptor-backed
		// message, whose name is qualified, has no spelling until `.proto` import
		// gives it one.
		if strings.Contains(k.Message, ".") {
			return "", errors.New("a descriptor-backed message type has no Flowfile spelling yet")
		}
		return k.Message, nil
	}
	return "", errors.New("type has no kind")
}

// declareType reads the text of a declaration's `type:` into the two
// representations a declaration carries: the legacy enum and, when the text is
// a type expression rather than one of the legacy words, the structural
// `value_type`.
//
// The legacy words are tried first and stay exactly as they were; only text that
// is not one is parsed as a type expression, so a file that compiled before
// compiles to the same message. A type expression must also project into the
// legacy enum, because the schema requires the two to agree: a typed
// `list(string)` carries TYPE_LIST, and `map(string, T)` TYPE_STRUCT. A type with
// no legacy word (`bytes`, `timestamp`, `duration`, `null_type`, `dyn`) is
// refused until the edition boundary makes a structural-only declaration safe,
// since the schema requires a legacy type beside every `value_type`.
//
// A worker built before the agreement rule was relaxed refuses a typed
// container when the run starts, because that rule demanded `dyn` elements: it
// fails closed rather than running the declaration untyped, and it means every
// worker must be upgraded before a typed declaration is submitted.
//
// A type nested past [v1.MaxStructureDepth] is refused here, where the author
// can see it, rather than when the run starts.
func declareType(env *cel.Env, text string) (legacy v1.InputDeclaration_Type, structural *v1.Type, err error) {
	if declared, known := v1.ParseDeclaredType(text); known {
		return declared, nil, nil
	}

	structural, err = parseType(env, text)
	if err != nil {
		return v1.InputDeclaration_TYPE_UNSPECIFIED, nil, err
	}

	if _, err := FormatType(structural); err != nil {
		return v1.InputDeclaration_TYPE_UNSPECIFIED, nil, err
	}

	switch k := structural.GetKind().(type) {
	case *v1.Type_List:
		return v1.InputDeclaration_TYPE_LIST, structural, nil
	case *v1.Type_Map_, *v1.Type_Message:
		// A record is a map at run time, so it carries what an older reader
		// enforces of any `struct`.
		return v1.InputDeclaration_TYPE_STRUCT, structural, nil
	case *v1.Type_Scalar_:
		switch k.Scalar {
		case v1.Type_SCALAR_STRING:
			return v1.InputDeclaration_TYPE_STRING, structural, nil
		case v1.Type_SCALAR_INT:
			return v1.InputDeclaration_TYPE_INT, structural, nil
		case v1.Type_SCALAR_DOUBLE:
			return v1.InputDeclaration_TYPE_FLOAT, structural, nil
		case v1.Type_SCALAR_BOOL:
			return v1.InputDeclaration_TYPE_BOOL, structural, nil
		}
	}

	return v1.InputDeclaration_TYPE_UNSPECIFIED, nil, fmt.Errorf(
		"%s cannot be declared yet: a declaration also carries the legacy type older workers read, and this one has none", text)
}

// declaredTypeText is how a declaration's type is written back into a Flowfile:
// the type expression when the declaration carries a structural type, so a
// `list(string)` survives a compile-and-marshal round trip, and the legacy word
// otherwise.
func declaredTypeText(legacy v1.InputDeclaration_Type, structural *v1.Type) string {
	if structural != nil {
		if text, err := FormatType(structural); err == nil {
			return text
		}
	}

	return v1.DeclaredTypeName(legacy)
}
