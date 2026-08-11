package flowfile_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// TestValidateForEachLiteralItemsCeiling covers the static half of the
// `for_each` trip-count ceiling: a list written out in the file, long enough
// that no run is needed to know it is over [v1.MaxForEachItems].
//
// The static half is deliberately the smaller one. `items:` is usually an
// expression, and how long the list it produces will be is a property of the run
// rather than of the file, which is why the load-bearing check is the runtime one
// both drivers apply. What this covers is the case the file itself decides, and
// it reports it where the author wrote it.
func TestValidateForEachLiteralItemsCeiling(t *testing.T) {
	t.Run("a literal list past the ceiling is reported", func(t *testing.T) {
		ds, err := flowfile.ValidateSource([]byte(forEachOverLiteralSource(v1.MaxForEachItems + 1)))
		require.NoError(t, err)
		require.NotEmpty(t, ds, "a literal items list past the ceiling must be reported")

		got := ds.Error()
		require.Contains(t, got, strconv.Itoa(v1.MaxForEachItems+1), "the diagnostic must name the count")
		require.Contains(t, got, strconv.Itoa(v1.MaxForEachItems), "the diagnostic must name the ceiling")
		require.Contains(t, got, "fan", "the diagnostic must name the step")
		require.NotEqual(t, 0, ds[0].Line, "the diagnostic must carry a source position")
	})

	t.Run("a literal list at the ceiling validates", func(t *testing.T) {
		// The boundary from the other side, for the same reason the runtime cases
		// run one at the ceiling: a check written with the comparison the wrong way
		// round refuses the largest legitimate list, and only this notices.
		ds, err := flowfile.ValidateSource([]byte(forEachOverLiteralSource(v1.MaxForEachItems)))
		require.NoError(t, err)
		require.Empty(t, ds, "a literal items list at the ceiling must validate: %s", ds.Error())
	})
}

// forEachOverLiteralSource writes a Flowfile whose `for_each` iterates a literal
// list of n entries.
func forEachOverLiteralSource(n int) string {
	entries := make([]string, n)
	for i := range entries {
		entries[i] = strconv.Itoa(i)
	}

	return `edition: v2026.2
name: literal-items
steps:
  - id: fan
    for_each:
      items: [` + strings.Join(entries, ", ") + `]
      as: item
      steps:
        - id: body
          log:
            message: ${string(item)}
`
}
