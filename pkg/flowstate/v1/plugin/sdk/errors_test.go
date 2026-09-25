package sdk

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestTheClassificationPredicatesReadThroughWrapping covers the family as a
// whole: each constructor answers true for its own predicate and false for
// every other, and a classification keeps answering once a caller has given it
// context with fmt.Errorf - which is what a plugin does on the way out.
func TestTheClassificationPredicatesReadThroughWrapping(t *testing.T) {
	t.Parallel()

	predicates := map[string]func(error) bool{
		"not found":         IsNotFound,
		"permission denied": IsPermissionDenied,
		"invalid input":     IsInvalidInput,
		"conflict":          IsConflict,
		"unavailable":       IsUnavailable,
		"outcome unknown":   IsOutcomeUnknown,
	}

	for name, err := range map[string]error{
		"not found":         NotFound("gone"),
		"permission denied": PermissionDenied("no"),
		"invalid input":     InvalidInput("bad"),
		"conflict":          Conflict("raced"),
		"unavailable":       Unavailable("down"),
		"outcome unknown":   OutcomeUnknown("maybe"),
	} {
		for predicateName, predicate := range predicates {
			want := predicateName == name
			if got := predicate(err); got != want {
				t.Errorf("Is%s(%s) = %v, want %v", predicateName, name, got, want)
			}
			if got := predicate(fmt.Errorf("while doing the thing: %w", err)); got != want {
				t.Errorf("Is%s(wrapped %s) = %v, want %v", predicateName, name, got, want)
			}
		}
	}

	// Failed and OutcomeUnknown share a code and differ in their verdict on
	// retrying, which is the distinction IsOutcomeUnknown has to make.
	if IsOutcomeUnknown(Failed("plain")) {
		t.Error("IsOutcomeUnknown(Failed(...)) = true; a permanent failure is not a call that may have taken effect")
	}

	// An error from outside this SDK answers false everywhere rather than
	// defaulting into a classification it never claimed.
	for predicateName, predicate := range predicates {
		if predicate(errors.New("something else")) {
			t.Errorf("Is%s(an unclassified error) = true", predicateName)
		}
		if predicate(nil) {
			t.Errorf("Is%s(nil) = true", predicateName)
		}
	}

	// UnavailableAfter is Unavailable with a delay, not a third thing.
	if !IsUnavailable(UnavailableAfter(time.Second, "later")) {
		t.Error("IsUnavailable(UnavailableAfter(...)) = false")
	}
}
