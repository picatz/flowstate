package server

import "testing"

// TestDeclaredKindNameAdmitsAModulesQualifiedError: an error a module declares is
// reported under its alias, and nothing wider than one alias prefix is.
func TestDeclaredKindNameAdmitsAModulesQualifiedError(t *testing.T) {
	t.Parallel()

	for name, want := range map[string]bool{
		"NotFound":     true,
		"ids.NotFound": true,
		"a_b.NotFound": true,
		"ids.notFound": false,
		"Ids.NotFound": false,
		"a.b.NotFound": false,
		".NotFound":    false,
		"ids.":         false,
		"ids_.Not":     false,
	} {
		if got := declaredKindName.MatchString(name); got != want {
			t.Errorf("declaredKindName(%q) = %v, want %v", name, got, want)
		}
	}
}
