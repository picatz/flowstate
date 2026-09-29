package protodocimpl

import (
	"slices"
	"strings"
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

// Nothing disagrees, nothing is listed: the same file registered twice, and
// different names in different files, are not conflicts.
func TestConflictsIsEmptyWhenRegistrationsAgree(t *testing.T) {
	tab := newTable()
	comments := []Comment{{Name: "a.v1.Thing", Leading: " Thing.\n"}}
	tab.registerFile("a/v1/a.proto", comments)
	tab.registerFile("a/v1/a.proto", comments)
	tab.registerFile("b/v1/b.proto", []Comment{{Name: "b.v1.Other", Leading: " Other.\n"}})

	if got := tab.conflicts(); len(got) != 0 {
		t.Errorf("conflicts = %v; want none", got)
	}
}

// A conflict is listed with every file that took part, whichever order they
// registered in, and Lookup stays closed for the same name: the diagnostic
// reports what the fail-closed answer hides, and does not reopen it.
func TestConflictsListsTheFilesThatDisagree(t *testing.T) {
	tab := newTable()
	tab.registerFile("b/v1/b.proto", []Comment{{Name: "a.v1.Thing", Leading: " From b.\n"}})
	tab.registerFile("a/v1/a.proto", []Comment{{Name: "a.v1.Thing", Leading: " From a.\n"}})
	tab.registerFile("a/v1/a.proto", []Comment{{Name: "a.v1.Thing", Leading: " From a.\n"}}) // a repeat adds nothing
	tab.registerFile("c/v1/c.proto", []Comment{{Name: "a.v1.Fine", Leading: " Fine.\n"}})

	got := tab.conflicts()
	if len(got) != 1 || got[0].Name != "a.v1.Thing" {
		t.Fatalf("conflicts = %v; want exactly a.v1.Thing", got)
	}
	if want := []string{"a/v1/a.proto", "b/v1/b.proto"}; !slices.Equal(got[0].Paths, want) {
		t.Errorf("paths = %v; want %v", got[0].Paths, want)
	}
	if _, _, ok := tab.lookup("a.v1.Thing"); ok {
		t.Error("lookup answered for a name conflicts lists")
	}
}

// One file registered with two different comments is a conflict naming that
// one file, which is what two generations of one generated package look like.
func TestConflictsNamesASingleFileRegisteredTwoWays(t *testing.T) {
	tab := newTable()
	tab.registerFile("a/v1/a.proto", []Comment{{Name: "a.v1.Thing", Leading: " Old.\n"}})
	tab.registerFile("a/v1/a.proto", []Comment{{Name: "a.v1.Thing", Leading: " New.\n"}})

	got := tab.conflicts()
	if len(got) != 1 || !slices.Equal(got[0].Paths, []string{"a/v1/a.proto"}) {
		t.Fatalf("conflicts = %v; want a.v1.Thing from one file", got)
	}
	if s := got[0].String(); !strings.Contains(s, "a.v1.Thing") || !strings.Contains(s, "different comments") {
		t.Errorf("String() = %q; want the name and the reason", s)
	}
}

// The list is in name order, so a failure message reads the same every run.
func TestConflictsAreInNameOrder(t *testing.T) {
	tab := newTable()
	for _, name := range []protoreflect.FullName{"z.v1.Z", "a.v1.A", "m.v1.M"} {
		tab.registerFile("one.proto", []Comment{{Name: name, Leading: " one\n"}})
		tab.registerFile("two.proto", []Comment{{Name: name, Leading: " two\n"}})
	}

	var names []protoreflect.FullName
	for _, c := range tab.conflicts() {
		names = append(names, c.Name)
	}
	if want := []protoreflect.FullName{"a.v1.A", "m.v1.M", "z.v1.Z"}; !slices.Equal(names, want) {
		t.Errorf("names = %v; want %v", names, want)
	}
}
