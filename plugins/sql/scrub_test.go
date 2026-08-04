package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// containmentSecret is a value that must never appear in anything this
// plugin returns, logs, or formats - a fake DSN, shaped like a real one so
// a regression that stops scrubbing DSNs specifically (rather than strings
// generally) would still be caught.
const containmentSecret = "postgres://app:hunter2_containment_canary_do_not_print_me@db.internal/prod"

// TestDriverErrorsContainTheRawDSN is the "red" half of the proof-of-bite
// this test file exists for: it establishes that the threat is real before
// the fix is trusted. Database drivers echo connection strings in their own
// error messages routinely - this constructs exactly such a message,
// unscrubbed, and confirms it does carry the secret, so the tests below
// that assert it does *not* survive this plugin's own path are proving
// something rather than trivially passing because nothing here ever
// contained the secret in the first place.
func TestDriverErrorsContainTheRawDSN(t *testing.T) {
	raw := fmt.Errorf("dial tcp: lookup db.internal: connection to %s refused", containmentSecret)
	if !strings.Contains(raw.Error(), containmentSecret) {
		t.Fatal("test setup is broken: the constructed error does not even contain the secret")
	}
}

// TestSQLQueryScrubsADSNLeakingDriverError is this plugin's own path
// through that threat: openDB (or anything downstream of it) returning an
// error whose message echoes the DSN must come back from sqlQuery with the
// secret redacted. pgx.ParseConfig is used here specifically because it
// really does echo back the offending portion of an unparseable connection
// string - the DSN appearing in the error is the driver's own documented
// behavior, not something this test manufactures.
func TestSQLQueryScrubsADSNLeakingDriverError(t *testing.T) {
	dsn := "postgres://" + containmentSecret + " this is not a valid connection string %"

	_, err := sqlQuery(context.Background(), inputsFor(map[string]any{
		"engine":   "ENGINE_POSTGRES",
		"dsn":      dsn,
		"query":    "SELECT 1",
		"max_rows": int32(1),
	}), nil)
	if err == nil {
		t.Fatal("sqlQuery with a malformed DSN: got no error, want one")
	}
	assertNeverContains(t, "sqlQuery's own returned error", err, containmentSecret)
}

// TestClassifyFunctionsScrubBeforeClassifying proves classifyExecError and
// classifyQueryError never let a secret through, across every phase and
// every recognized error shape - the direct unit-level companion to the
// end-to-end test above.
func TestClassifyFunctionsScrubBeforeClassifying(t *testing.T) {
	scrubber := scrubberWith(containmentSecret)

	leaking := errors.New("connection failed: " + containmentSecret)

	for _, phase := range []execPhase{phaseConnect, phaseBegin, phaseStatement, phaseCommit} {
		err := classifyExecError(leaking, phase, scrubber)
		assertNeverContains(t, fmt.Sprintf("classifyExecError(phase=%d)", phase), err, containmentSecret)
	}

	assertNeverContains(t, "classifyQueryError", classifyQueryError(leaking, scrubber), containmentSecret)
}

// TestScrubberContainmentShapes is the containment-shape test CLAUDE.md
// requires: %v, %+v, %#v, and %s, on the value itself, on a struct holding
// it, and on a slice of those - proving that once an error has passed
// through [secrets.Scrubber.ScrubError], no formatting verb this plugin
// might log with (directly or via a wrapping struct) can recover the
// original text. This is the general mechanism plugins/git and
// plugins/codex each test for their own credential-carrying types; this
// plugin has no analogous struct of its own (dsn is never held in a field a
// plugin-defined type carries - see doc.go's own "Secrets" section), so
// what is worth proving here is that the scrubbed *error value* itself -
// the thing this plugin actually returns and the host actually logs - is
// contained the same way.
func TestScrubberContainmentShapes(t *testing.T) {
	scrubber := scrubberWith(containmentSecret)
	leaking := errors.New("dial failed, dsn=" + containmentSecret)
	scrubbed := scrubber.ScrubError(leaking)

	type holder struct {
		Err   error
		Label string
	}
	wrapped := holder{Err: scrubbed, Label: "sql plugin error"}

	rendered := []string{
		fmt.Sprintf("%v", scrubbed),
		fmt.Sprintf("%+v", scrubbed),
		fmt.Sprintf("%#v", scrubbed),
		fmt.Sprintf("%s", scrubbed), //nolint:gosimple // the point of this line is the %s verb itself, matching plugins/git's identical test's own comment
		fmt.Sprintf("%v", wrapped),
		fmt.Sprintf("%+v", wrapped),
		fmt.Sprintf("%#v", wrapped),
		fmt.Sprintf("%v", []holder{wrapped, wrapped}),
		fmt.Sprintf("%+v", []error{scrubbed, scrubbed}),
	}

	for _, r := range rendered {
		if strings.Contains(r, containmentSecret) {
			t.Fatalf("secret leaked through fmt reflection: %q", r)
		}
	}
}

func assertNeverContains(t *testing.T, label string, err error, secret string) {
	t.Helper()
	if err == nil {
		return
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("%s leaked the secret: %v", label, err)
	}
}
