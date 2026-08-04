package main

import (
	"testing"
	"time"
)

func TestConvertColumnValueHandlesEveryDocumentedShape(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want any
	}{
		{"nil", nil, nil},
		{"bool", true, true},
		{"int64", int64(42), int64(42)},
		{"float64", 3.5, 3.5},
		{"string", "hi", "hi"},
		{"bytes", []byte("blob"), "blob"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, _, err := convertColumnValue(c.in)
			if err != nil {
				t.Fatalf("convertColumnValue(%v): unexpected error: %v", c.in, err)
			}
			if got != c.want {
				t.Errorf("convertColumnValue(%v) = %v, want %v", c.in, got, c.want)
			}
		})
	}
}

func TestConvertColumnValueFormatsTimeAsRFC3339(t *testing.T) {
	when := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	got, n, err := convertColumnValue(when)
	if err != nil {
		t.Fatalf("convertColumnValue(time.Time): unexpected error: %v", err)
	}
	want := when.Format(time.RFC3339Nano)
	if got != want {
		t.Errorf("convertColumnValue(time.Time) = %v, want %v", got, want)
	}
	if n != len(want) {
		t.Errorf("reported size = %d, want %d", n, len(want))
	}
}

// TestConvertColumnValueRefusesAnUnrecognizedType proves this task fails
// closed on a Go type outside database/sql's own documented scan shapes,
// rather than silently stringifying something that might not be text.
func TestConvertColumnValueRefusesAnUnrecognizedType(t *testing.T) {
	type notAScanShape struct{ X int }
	if _, _, err := convertColumnValue(notAScanShape{X: 1}); err == nil {
		t.Fatal("convertColumnValue with an unrecognized type: got no error, want one")
	}
}
