package flowtest

import "fmt"

// Table entries (#924 slice 2): a `tests:` entry that declares `cases:` is a
// template, and each row under it is one run merged over that template.
//
// The mechanism is `defaults:` applied one level down, deliberately and
// literally — [mergeRow] hands the entry to [mergeDefaults] as the [Defaults]
// it is standing in for, so the four fields that block describes merge by
// rules already written, tested and documented rather than by a second set
// that could come to disagree with them (#416's answers, inherited verbatim
// rather than re-decided). Only the fields `defaults:` has no opinion about
// are merged here, and all of them take the same one direction: the row's own
// value wins, and the entry's is what a row that stated none inherits.

// expandTableEntries turns every entry that declares `cases:` into its rows,
// leaving entries that declare none exactly as they were.
//
// The result is a flat list of effective cases, which is what lets the rest of
// this package stay unaware that a table is a thing an author can write.
func expandTableEntries(tests []Test) ([]Test, error) {
	// Nothing in this file writes a table: the overwhelmingly common shape,
	// and worth not allocating for.
	tabled := false
	for i := range tests {
		if tests[i].Cases != nil {
			tabled = true

			break
		}
	}
	if !tabled {
		return tests, nil
	}

	expanded := make([]Test, 0, len(tests))
	for _, entry := range tests {
		if entry.Cases == nil {
			expanded = append(expanded, entry)

			continue
		}

		// Named here rather than by the loop below, because an unnamed entry
		// with rows has no way to name its rows either: `<entry>/<row>` needs
		// both halves, and "test 3/the fast one" is not an identity anyone
		// can act on.
		if entry.Name == "" {
			return nil, fmt.Errorf("a `tests:` entry declaring `cases:` has no name, and a row's " +
				"identity is `<entry name>/<row name>`; name the entry")
		}
		// An empty table is refused rather than silently running nothing: an
		// author who wrote `cases: []` meant to write rows, and a file that
		// quietly ran zero cases where one was expected is the "green by not
		// running" failure this repository legislates against. Distinguishable
		// from an absent `cases:` because YAML gives an empty sequence a
		// non-nil slice and an absent key a nil one.
		if len(entry.Cases) == 0 {
			return nil, fmt.Errorf("test %q declares `cases:` with no rows, so it would run nothing; "+
				"write a row, or drop the `cases:` key to run the entry itself", entry.Name)
		}

		for i, row := range entry.Cases {
			if row.Name == "" {
				return nil, fmt.Errorf("test %q case %d has no name", entry.Name, i+1)
			}
			if row.Cases != nil {
				return nil, fmt.Errorf("test %q case %q declares its own `cases:`, and a table is one "+
					"level deep; move the rows up, or split the entry in two", entry.Name, row.Name)
			}
			expanded = append(expanded, mergeRow(entry, row))
		}
	}

	return expanded, nil
}

// mergeRow produces the effective case one row runs as.
func mergeRow(entry, row Test) Test {
	// The four fields `defaults:` describes, merged by the rules that block
	// already states — the entry standing in as the Defaults it is. Sender is
	// left nil because an entry has no `sender:` of its own: a signal's sender
	// is carried on the signal, and a row that states no `signals:` inherits
	// the entry's whole list below, senders and all.
	merged := mergeDefaults(&Defaults{
		Workflow: entry.Workflow,
		Inputs:   entry.Inputs,
		Stubs:    entry.Stubs,
	}, row)

	// Everything else, in the one direction: stated beats inherited. These
	// four are inherited whole or replaced whole — a row that writes any
	// `signals:` writes all of them — because each is a list or a record
	// whose halves are not independently meaningful. `expect:` is the
	// exception and is merged field by field; see [mergeExpectation].
	if merged.Trigger == nil {
		merged.Trigger = entry.Trigger
	}
	if merged.Starter == nil {
		merged.Starter = entry.Starter
	}
	if len(merged.Signals) == 0 {
		merged.Signals = entry.Signals
	}
	if len(merged.Secrets) == 0 {
		merged.Secrets = entry.Secrets
	}
	merged.Expect = mergeExpectation(entry.Expect, row.Expect)

	// `<entry>/<row>`, the two-level identity a Go table gets from t.Run.
	merged.Name = entry.Name + "/" + row.Name
	// A row is never itself a table; expandTableEntries refused that above,
	// and clearing it keeps the effective case indistinguishable from a case
	// somebody wrote by hand.
	merged.Cases = nil

	return merged
}

// mergeExpectation merges a row's expectation over its entry's, one field at
// a time: the row's value where it wrote one, the entry's where it did not.
//
// Field by field rather than whole-or-nothing, and the reason is that this is
// the rule this file already applies one level up. `defaults.inputs` merges
// into a case's `inputs:` key by key, so a table's `expect:` merging field by
// field is the same rule at the next level down rather than a second one
// beside it — which is what #924 asks for and what keeps an author from having
// to learn two things.
//
// The shape that decided it is the common one. Three rows asserting a refusal
// share `failed: true` and differ only in `error_contains:`; under
// whole-or-nothing every row restates `failed: true`, which is the
// restatement a table exists to remove. An earlier draft of this function did
// exactly that, and the first example converted to a table showed it
// immediately.
//
// The cost, stated: a row cannot assert *less* than its entry. An entry that
// pins `outputs:` pins it for every row that does not overwrite it, and a row
// whose run legitimately produces something else has no spelling for "forget
// that". The answer is to move the claim down into the rows that want it —
// an entry should hold what is true of every row — and the failure is loud
// rather than silent, because the row fails against a value nobody claimed
// for it.
//
// A nil slice and an empty one are deliberately different here. `ran: []` is
// the assertion that no step ran, which an author writes on purpose, so
// emptiness cannot mean "unset": absence is nil, and only nil inherits.
//
// A field added to [Expectation] needs a line here, and
// TestEveryExpectationFieldIsMerged fails until it gets one.
func mergeExpectation(entry, row Expectation) Expectation {
	merged := row
	if merged.Outputs == nil {
		merged.Outputs = entry.Outputs
	}
	if merged.Inputs == nil {
		merged.Inputs = entry.Inputs
	}
	if merged.Refused == nil {
		merged.Refused = entry.Refused
	}
	if merged.IdempotencyKey == "" {
		merged.IdempotencyKey = entry.IdempotencyKey
	}
	if merged.Failed == nil {
		merged.Failed = entry.Failed
	}
	if merged.ErrorContains == "" {
		merged.ErrorContains = entry.ErrorContains
	}
	if merged.Compensated == nil {
		merged.Compensated = entry.Compensated
	}
	if merged.Ran == nil {
		merged.Ran = entry.Ran
	}
	if merged.Skipped == nil {
		merged.Skipped = entry.Skipped
	}
	if merged.Others == "" {
		merged.Others = entry.Others
	}

	return merged
}
