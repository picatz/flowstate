package flowstatev1

import (
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/ast"
)

// mapKeyValidatorName names the validator in the CEL environment's registry.
const mapKeyValidatorName = "flowstate.map_key_kinds"

// validateMapKeyKinds refuses a map literal whose key is provably a kind the
// ordered traversal cannot sequence (#1859). The admitted kinds are an allow-list,
// so a list, map, struct or other key is refused with the rest.
//
// Every comprehension that ranges over a map visits its keys in one order so
// both drivers agree (#1359); that order is defined for bool, int, uint and
// string keys only (orderedMapKey). cel-go's checker admits any key type, so
// without this a map such as {1.5: 'a'} validated clean and failed at run, in
// the first map, filter or all that walked it. A literal's key type is known
// after checking, so the refusal can be given at the key, and it covers a map
// built inside a comprehension (items.map(i, {i.at: i})) as well as a written
// one. A key whose type the checker cannot decide (dyn, a type parameter) is left to the runtime refusal, which stays.
func validateMapKeyKinds() cel.ASTValidator { return mapKeyValidator{} }

type mapKeyValidator struct{}

func (mapKeyValidator) Name() string { return mapKeyValidatorName }

func (mapKeyValidator) Validate(_ *cel.Env, _ cel.ValidatorConfig, a *ast.AST, iss *cel.Issues) {
	for _, expr := range ast.MatchDescendants(ast.NavigateAST(a), ast.KindMatcher(ast.MapKind)) {
		for _, entry := range expr.AsMap().Entries() {
			key := entry.AsMapEntry().Key()
			typ := a.GetType(key.ID())
			if typ == nil {
				continue
			}
			switch typ.Kind() {
			case cel.BoolKind, cel.IntKind, cel.UintKind, cel.StringKind,
				cel.DynKind, cel.AnyKind, cel.TypeParamKind:
				// Orderable, or not decidable here: left to the runtime refusal.
			default:
				iss.ReportErrorAtID(key.ID(),
					"map key has unsupported CEL type %s: map keys must be bool, int, uint or string", typ)
			}
		}
	}
}
