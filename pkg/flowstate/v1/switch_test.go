package flowstatev1_test

import (
	"testing"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestSwitchLiteralsEqualNumericPrecision pins case matching to cel-go's own
// numeric equality, at exactly the magnitudes a float64 common type conflates.
//
// The regression it guards: matching used to convert every number to float64,
// so the two int64 values 9007199254740992 and 9007199254740993 — 2^53 and
// 2^53+1, distinct integers one float64 apart from nothing — compared equal.
// `x == 9007199254740993` in the `if:` a switch replaces distinguishes them,
// and the duplicate-case validator reads the same function, so the lossy
// comparison both dispatched wrongly and flagged non-duplicate cases as
// duplicates.
//
// Literals are built as bare expr values rather than through [v1.NewLiteral],
// because the interesting uint magnitudes are exactly the ones NewLiteral
// deliberately refuses.
func TestSwitchLiteralsEqualNumericPrecision(t *testing.T) {
	t.Parallel()

	intv := func(v int64) *expr.Value { return &expr.Value{Kind: &expr.Value_Int64Value{Int64Value: v}} }
	uintv := func(v uint64) *expr.Value { return &expr.Value{Kind: &expr.Value_Uint64Value{Uint64Value: v}} }
	dbl := func(v float64) *expr.Value { return &expr.Value{Kind: &expr.Value_DoubleValue{DoubleValue: v}} }
	str := func(s string) *expr.Value { return &expr.Value{Kind: &expr.Value_StringValue{StringValue: s}} }

	for _, tc := range []struct {
		name string
		a, b *expr.Value
		want bool
	}{
		// Same-typed integers compare exactly, above float64's exact range.
		{"int 2^53 vs int 2^53+1 are distinct", intv(9007199254740992), intv(9007199254740993), false},
		{"int 2^53+1 vs itself matches", intv(9007199254740993), intv(9007199254740993), true},
		{"uint 2^63 vs uint 2^63+1 are distinct", uintv(9223372036854775808), uintv(9223372036854775809), false},
		{"uint 2^63 vs itself matches", uintv(9223372036854775808), uintv(9223372036854775808), true},

		// int against uint compares by value, with the range checked rather
		// than rounded: a uint above int64 range equals no int, not even the
		// int a float64 would round both of them to.
		{"uint 2^63 vs int max are distinct", uintv(9223372036854775808), intv(9223372036854775807), false},
		{"uint 2 vs int 2 matches", uintv(2), intv(2), true},
		{"int -1 vs uint max are distinct", intv(-1), uintv(18446744073709551615), false},

		// The settled cross-type semantic the shared cases pin: `case: 1`
		// takes a discriminant of 1.0.
		{"int 1 vs double 1.0 matches", intv(1), dbl(1), true},
		{"double 1.5 vs int 1 are distinct", dbl(1.5), intv(1), false},
		{"double above int64 range vs int max are distinct", dbl(1e19), intv(9223372036854775807), false},

		// Numbers still do not match non-numbers.
		{"int 1 vs string 1 are distinct", intv(1), str("1"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := v1.SwitchLiteralsEqual(tc.a, tc.b); got != tc.want {
				t.Fatalf("SwitchLiteralsEqual(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
			// Equality is symmetric; a one-directional answer would make the
			// duplicate-case check depend on written order.
			if got := v1.SwitchLiteralsEqual(tc.b, tc.a); got != tc.want {
				t.Fatalf("SwitchLiteralsEqual(%v, %v) = %v, want %v (asymmetric)", tc.b, tc.a, got, tc.want)
			}
		})
	}
}
