package registry

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestLookupReturnsWhatWasRegistered(t *testing.T) {
	tab := newTable()
	tab.registerFile("a/v1/a.proto", []Comment{
		{Name: "a.v1.Thing", Leading: " Thing is a thing.\n"},
		{Name: "a.v1.Thing.name", Leading: " Name names it.\n"},
	})

	got, path, ok := tab.lookup("a.v1.Thing.name")
	if !ok || got != " Name names it.\n" || path != "a/v1/a.proto" {
		t.Errorf("lookup = %q, %q, %v; want the registered comment and its file", got, path, ok)
	}
}

// Fail closed: nothing registered, an empty name, and an empty comment all
// answer the same way.
func TestLookupOfNothingReportsFalse(t *testing.T) {
	tab := newTable()
	tab.registerFile("a/v1/a.proto", []Comment{
		{Name: "a.v1.Empty", Leading: ""},
		{Name: "", Leading: " orphan\n"},
	})

	for _, name := range []protoreflect.FullName{"", "a.v1.Empty", "a.v1.Missing"} {
		if got, path, ok := tab.lookup(name); ok || got != "" || path != "" {
			t.Errorf("lookup(%q) = %q, %q, %v; want \"\", \"\", false", name, got, path, ok)
		}
	}
}

// The same file registered twice, as two copies of one generated package
// would, changes nothing.
func TestIdenticalRegistrationIsIdempotent(t *testing.T) {
	tab := newTable()
	comments := []Comment{{Name: "a.v1.Thing", Leading: " Thing.\n"}}
	tab.registerFile("a/v1/a.proto", comments)
	tab.registerFile("a/v1/a.proto", comments)

	if _, _, ok := tab.lookup("a.v1.Thing"); !ok {
		t.Error("an identical second registration made the name unreadable")
	}
}

// A name two files disagree about is not answered, whichever registered first.
func TestConflictingRegistrationIsAmbiguous(t *testing.T) {
	for _, tc := range []struct {
		name   string
		second Comment
		path   string
	}{
		{"different text", Comment{Name: "a.v1.Thing", Leading: " Something else.\n"}, "a/v1/a.proto"},
		{"different file", Comment{Name: "a.v1.Thing", Leading: " Thing.\n"}, "b/v1/b.proto"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tab := newTable()
			tab.registerFile("a/v1/a.proto", []Comment{{Name: "a.v1.Thing", Leading: " Thing.\n"}})
			tab.registerFile(tc.path, []Comment{tc.second})

			if got, _, ok := tab.lookup("a.v1.Thing"); ok {
				t.Errorf("lookup = %q, true; want an ambiguous name to report false", got)
			}
			// And it stays ambiguous: a third, matching registration does not
			// resolve a conflict already seen.
			tab.registerFile("a/v1/a.proto", []Comment{{Name: "a.v1.Thing", Leading: " Thing.\n"}})
			if _, _, ok := tab.lookup("a.v1.Thing"); ok {
				t.Error("a later registration resolved an ambiguity")
			}
		})
	}
}
