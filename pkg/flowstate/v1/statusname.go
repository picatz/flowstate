package flowstatev1

import (
	"sort"
	"strings"
)

// The short name of a run's status, in one place.
//
// `STATUS_FAILED` is the schema's name for the value and nobody says it out loud.
// Every surface a person reads — `flow get`, `flow list`, a filter expression —
// wants `FAILED`, and until this existed each of them trimmed the prefix itself.
// Two of them doing that is the arrangement CLAUDE.md describes as a value with
// one meaning written down twice; a filter comparing against a name one surface
// spells differently is exactly how that stops being harmless.
//
// Derived from the enum descriptor rather than listed here, which is the part that
// matters. A status added to the schema is immediately printable, filterable, and
// named in the diagnostic that lists the valid ones — with nobody remembering to
// come here.

// StatusName returns the short name of a status: `FAILED`, not `STATUS_FAILED`.
func StatusName(status RunResponse_Status) string {
	return strings.TrimPrefix(status.String(), statusPrefix)
}

// StatusNames returns the set of short status names a run can have.
//
// UNSPECIFIED is excluded, and that is a decision rather than tidiness: it is the
// zero value protobuf gives a field nobody set, not a state a run is ever in. A
// caller filtering for it would be asking for runs the server failed to describe,
// which is not a question about their workloads — and offering it in a diagnostic
// would invite exactly that.
func StatusNames() map[string]bool {
	values := RunResponse_Status(0).Descriptor().Values()

	names := make(map[string]bool, values.Len())
	for i := range values.Len() {
		name := strings.TrimPrefix(string(values.Get(i).Name()), statusPrefix)
		if name == unspecifiedStatus {
			continue
		}
		names[name] = true
	}

	return names
}

// sortedNames renders a name set for a diagnostic, in a stable order.
//
// Sorted rather than in declaration order because a map has none, and a diagnostic
// whose wording changes between runs is one nobody can test and everybody
// distrusts.
func sortedNames(names map[string]bool) []string {
	out := make([]string, 0, len(names))
	for name := range names {
		out = append(out, name)
	}
	sort.Strings(out)

	return out
}

const (
	// statusPrefix is the protobuf enum prefix the schema requires and no reader
	// wants. `buf lint` enforces that every value of an enum is prefixed with the
	// enum's own name, which is why it is there at all.
	statusPrefix = "STATUS_"

	// unspecifiedStatus is the zero value, excluded from the vocabulary.
	unspecifiedStatus = "UNSPECIFIED"
)
