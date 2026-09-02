package flowstatev1

import (
	"fmt"
	"math/bits"
	"slices"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
)

// orderedMapFunction is inserted around every comprehension range immediately
// before a CEL program is built. Its @-bearing identifier cannot be written by
// an author, so the overload remains evaluator machinery rather than language
// surface.
const orderedMapFunction = "flowstate.@orderedMap"

func orderedMapEnvOption(costLimit uint64) cel.EnvOption {
	return cel.Function(orderedMapFunction,
		cel.Overload(orderedMapFunction, []*cel.Type{cel.DynType}, cel.DynType,
			cel.UnaryBinding(func(value ref.Val) ref.Val {
				return orderMapWithinCost(value, costLimit)
			})))
}

func orderMap(value ref.Val) ref.Val {
	return orderMapWithinCost(value, 0)
}

func orderMapWithinCost(value ref.Val, costLimit uint64) ref.Val {
	if _, ok := value.(orderedMap); ok {
		return value
	}
	mapper, ok := value.(traits.Mapper)
	if !ok {
		return value
	}
	keys, cost, err := orderedMapKeys(mapper, costLimit)
	if err != nil {
		return types.NewErr("%v", err)
	}
	return orderedMap{Mapper: mapper, keys: keys, cost: cost}
}

// orderMapComprehensions clones parsed and wraps every comprehension range in
// orderedMapFunction. CEL comprehensions are the only language operation that
// observes map traversal order; applying the rule there covers map literals,
// activation values, JSON values, and maps produced by another comprehension
// without replacing CEL's evaluator or changing the specification that travels.
func orderMapComprehensions(parsed *expr.ParsedExpr) *expr.ParsedExpr {
	if parsed == nil || parsed.GetExpr() == nil {
		return parsed
	}

	ordered := proto.Clone(parsed).(*expr.ParsedExpr)
	orderMapComprehensionExpr(ordered.GetExpr())
	return ordered
}

// orderMapComprehensionsAST is the checked-AST counterpart to
// orderMapComprehensions. Eval accepts both parsed and checked ASTs; preserving
// the checked type and overload maps keeps cel-go's size-aware overload costs
// and custom-type dispatch intact for callers that paid to check first.
func orderMapComprehensionsAST(ast *cel.Ast) (*cel.Ast, error) {
	if !ast.IsChecked() {
		parsed, err := cel.AstToParsedExpr(ast)
		if err != nil {
			return nil, err
		}
		return cel.ParsedExprToAst(orderMapComprehensions(parsed)), nil
	}

	checked, err := cel.AstToCheckedExpr(ast)
	if err != nil {
		return nil, err
	}
	ordered := proto.Clone(checked).(*expr.CheckedExpr)
	for id, rangeID := range orderMapComprehensionExpr(ordered.GetExpr()) {
		if rangeType := ordered.GetTypeMap()[rangeID]; rangeType != nil {
			ordered.TypeMap[id] = proto.Clone(rangeType).(*expr.Type)
		}
		ordered.ReferenceMap[id] = &expr.Reference{
			Name:       orderedMapFunction,
			OverloadId: []string{orderedMapFunction},
		}
	}
	return cel.CheckedExprToAst(ordered), nil
}

// orderMapComprehensionExpr mutates root and reports each synthetic call id's
// original range id, which a checked AST uses to preserve the range's type.
func orderMapComprehensionExpr(root *expr.Expr) map[int64]int64 {
	var nextID int64
	visitCELExpr(root, func(current *expr.Expr) {
		if current.GetId() >= nextID {
			nextID = current.GetId() + 1
		}
	})
	inserted := map[int64]int64{}
	visitCELExpr(root, func(current *expr.Expr) {
		comprehension := current.GetComprehensionExpr()
		if comprehension == nil {
			return
		}
		rangeID := comprehension.GetIterRange().GetId()
		comprehension.IterRange = &expr.Expr{
			Id: nextID,
			ExprKind: &expr.Expr_CallExpr{CallExpr: &expr.Expr_Call{
				Function: orderedMapFunction,
				Args:     []*expr.Expr{comprehension.GetIterRange()},
			}},
		}
		inserted[nextID] = rangeID
		nextID++
	})
	return inserted
}

// visitCELExpr walks children before their parent, so a nested comprehension's
// original range is wrapped before an enclosing range captures that expression.
func visitCELExpr(current *expr.Expr, visit func(*expr.Expr)) {
	if current == nil {
		return
	}
	switch kind := current.GetExprKind().(type) {
	case *expr.Expr_SelectExpr:
		visitCELExpr(kind.SelectExpr.GetOperand(), visit)
	case *expr.Expr_CallExpr:
		visitCELExpr(kind.CallExpr.GetTarget(), visit)
		for _, arg := range kind.CallExpr.GetArgs() {
			visitCELExpr(arg, visit)
		}
	case *expr.Expr_ListExpr:
		for _, element := range kind.ListExpr.GetElements() {
			visitCELExpr(element, visit)
		}
	case *expr.Expr_StructExpr:
		for _, entry := range kind.StructExpr.GetEntries() {
			visitCELExpr(entry.GetMapKey(), visit)
			visitCELExpr(entry.GetValue(), visit)
		}
	case *expr.Expr_ComprehensionExpr:
		comprehension := kind.ComprehensionExpr
		visitCELExpr(comprehension.GetIterRange(), visit)
		visitCELExpr(comprehension.GetAccuInit(), visit)
		visitCELExpr(comprehension.GetLoopCondition(), visit)
		visitCELExpr(comprehension.GetLoopStep(), visit)
		visitCELExpr(comprehension.GetResult(), visit)
	}
	visit(current)
}

// orderedMap keeps every map operation except traversal exactly as CEL defines
// it. Both one-variable and two-variable comprehensions observe the same sorted
// keys through Iterator and Fold respectively.
type orderedMap struct {
	traits.Mapper
	keys []ref.Val
	cost uint64
}

func (m orderedMap) Iterator() traits.Iterator {
	return types.NewRefValList(TypeAdapter, m.keys).Iterator()
}

func (m orderedMap) Fold(folder traits.Folder) {
	for _, key := range m.keys {
		if !folder.FoldEntry(key, m.Get(key)) {
			return
		}
	}
}

// orderedMapKeys rejects the entry-count floor before allocating or iterating,
// then accounts for string-key bytes while collecting and before sorting. The
// runtime cost observer charges the same returned cost after the call; this
// preflight is what bounds the indivisible work that happens before it can.
func orderedMapKeys(mapper traits.Mapper, costLimit uint64) ([]ref.Val, uint64, error) {
	size, ok := mapper.Size().(types.Int)
	if !ok || size < 0 {
		return nil, 0, fmt.Errorf("map reported an invalid size")
	}
	n := uint64(size)
	cost := mapOrderingCost(n, 0)
	if costLimit > 0 && cost > costLimit {
		return nil, 0, fmt.Errorf("map ordering cost %d exceeds CEL cost limit %d", cost, costLimit)
	}
	keys := make([]ref.Val, 0, int(size))
	var stringBytes uint64
	for iterator := mapper.Iterator(); iterator.HasNext() == types.True; {
		key := iterator.Next()
		if !orderedMapKey(key) {
			return nil, 0, fmt.Errorf("map key has unsupported CEL type %s", key.Type().TypeName())
		}
		if key, ok := key.(types.String); ok {
			length := uint64(len(key))
			if ^uint64(0)-stringBytes < length {
				stringBytes = ^uint64(0)
			} else {
				stringBytes += length
			}
			cost = mapOrderingCost(n, stringBytes)
			if costLimit > 0 && cost > costLimit {
				return nil, 0, fmt.Errorf("map ordering cost %d exceeds CEL cost limit %d", cost, costLimit)
			}
		}
		keys = append(keys, key)
	}
	slices.SortFunc(keys, compareMapKeys)
	return keys, cost, nil
}

func mapOrderingCost(entries, stringBytes uint64) uint64 {
	if entries <= 1 {
		return 0
	}
	levels := uint64(bits.Len64(entries - 1))
	if ^uint64(0)-entries < stringBytes {
		return ^uint64(0)
	}
	work := entries + stringBytes
	if work > ^uint64(0)/levels {
		return ^uint64(0)
	}
	return work * levels
}

func orderedMapKey(key ref.Val) bool {
	switch key.(type) {
	case types.Bool, types.Int, types.Uint, types.String:
		return true
	default:
		return false
	}
}

func compareMapKeys(left, right ref.Val) int {
	leftRank, rightRank := mapKeyRank(left), mapKeyRank(right)
	if leftRank != rightRank {
		return compareOrdered(leftRank, rightRank)
	}
	switch left := left.(type) {
	case types.Bool:
		right := right.(types.Bool)
		if left == right {
			return 0
		}
		if !bool(left) {
			return -1
		}
		return 1
	case types.Int:
		return compareOrdered(int64(left), int64(right.(types.Int)))
	case types.Uint:
		return compareOrdered(uint64(left), uint64(right.(types.Uint)))
	case types.String:
		return compareOrdered(string(left), string(right.(types.String)))
	default:
		return 0
	}
}

func mapKeyRank(key ref.Val) int64 {
	switch key.(type) {
	case types.Bool:
		return 0
	case types.Int:
		return 1
	case types.Uint:
		return 2
	case types.String:
		return 3
	default:
		return 4
	}
}

func compareOrdered[T ~int64 | ~uint64 | ~string](left, right T) int {
	if left == right {
		return 0
	}
	if left < right {
		return -1
	}
	return 1
}
