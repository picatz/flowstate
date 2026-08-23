package flowfile

import (
	"strings"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// normalizePasses counts the parse-unparse passes one expression needs before
// parsing what it writes gives back the expression that wrote it.
//
// It is a measurement rather than a second copy of [normalizeExpr]: what it
// reports is a property of cel-go's parser and unparser, which is the thing
// [maxNormalizePasses] is a bound on.
func normalizePasses(t *testing.T, source string) int {
	t.Helper()

	val := v1.NewExpr(source)
	require.NoError(t, val.Error(), "the case's own expression must parse")

	for passes := 1; passes <= 64; passes++ {
		text, err := cel.AstToString(cel.ParsedExprToAst(val.GetExpr()))
		require.NoError(t, err)

		next := v1.NewExpr(text)
		require.NoError(t, next.Error())

		if proto.Equal(next.GetExpr(), val.GetExpr()) {
			return passes
		}
		val = next
	}
	t.Fatalf("%q never settled", source)
	return 0
}

// TestNormalizeExprReachesAFixedPoint is the post-condition #880 broke, asserted
// where it can be stated exactly: what [normalizeExpr] returns has to be what
// parsing Marshal's rendering of it produces. Marshal writes
// `cel.AstToString(stored)`; the compiler reads that back with [v1.NewExpr]; if
// those two disagree the workflow changes across a round trip, however well the
// bytes parse.
func TestNormalizeExprReachesAFixedPoint(t *testing.T) {
	t.Parallel()

	sources := []string{
		"-----0", "-0", "--0", "---0", "- - -0", "-(0)", "-(-(0))", "-(-(-(0)))",
		"(-0)", "((((-0))))", "-0.0", "-----0.0", "0x0", "-0x0",
		"!!true", "!!!true", "!(true)", "!(!(true))", "!(!(!(!(true))))",
		"1  +  1", "(1)", "((1))", "1 - -0", "[-0]", "{'a': -0}", "-0 + 1",
		"'hello'", "steps.who.value", "steps.who.value == 'hello'",

		// Deep nesting, because the bound is a constant and the obvious worry
		// about a constant is that the number of passes grows with the input. It
		// does not: each pass folds the whole tree rather than one layer of it,
		// so a hundred negations settle in the same three passes five do.
		strings.Repeat("-", 101) + "0",
		strings.Repeat("-(", 50) + "0" + strings.Repeat(")", 50),
		strings.Repeat("!(", 50) + "true" + strings.Repeat(")", 50),
	}

	worst := 0
	for _, source := range sources {
		t.Run(source, func(t *testing.T) {
			val := v1.NewExpr(source)
			require.NoError(t, val.Error())

			normalized := normalizeExpr(val)

			text, err := cel.AstToString(cel.ParsedExprToAst(normalized.GetExpr()))
			require.NoError(t, err)

			reparsed := v1.NewExpr(text)
			require.NoError(t, reparsed.Error())
			require.True(t, proto.Equal(reparsed.GetExpr(), normalized.GetExpr()),
				"%q normalizes to %q, which parses to a different expression: "+
					"Marshal would write bytes that read back as another workflow",
				source, text)
		})

		if passes := normalizePasses(t, source); passes > worst {
			worst = passes
		}
	}

	// The bound was written with headroom over what these shapes need, and both
	// halves of that are worth asserting. More than one pass, because a bound
	// nothing reaches is a bound nothing tests — a single pass is exactly what
	// #880 was. Fewer than the bound, so that the cases are settling rather than
	// being cut off at it, which would leave the post-condition above holding by
	// luck.
	require.Greater(t, worst, 1,
		"every case settles in one pass, so this table no longer covers the defect")
	require.Less(t, worst, maxNormalizePasses,
		"a case needs %d passes against a bound of %d, which is no headroom at all",
		worst, maxNormalizePasses)
	t.Logf("worst case settles in %d passes, bound is %d", worst, maxNormalizePasses)
}
