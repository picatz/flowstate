package mcp

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// FitResult brings an answer under [MaxResultBytes] by trying a ladder of
// progressively smaller documents, and reports which rung it settled on.
//
// This is the one shape all three shrinking answers on this surface share, and
// it is here because they were three copies of it. `flowstate_run_local` drops
// logs, then the step transcript, then the declared outputs;
// `flowstate_test` caps each failure's message, then reduces the report to
// per-case verdicts; and `flowstate_get` drops the transcript, then a running
// run's carried state, then the declared outputs. What they *drop* is a
// property of the message and cannot be shared — a TestReport has no step
// transcript and a GetResponse has no cases — but the discipline around the
// dropping is identical, and it is the part with the bug in it if it is written
// out by hand a fourth time:
//
//   - re-encode and re-measure after every rung, because a rung that drops the
//     wrong field is a rung that changed nothing, and a ladder that measures
//     once cannot tell;
//   - stop at the *first* rung that fits, so an answer loses the least it can
//     rather than everything the ladder knows how to drop;
//   - return the last rung whether or not it fits, because a document that is
//     still too large is an answer a reader can act on and an empty result is
//     not. Every rung's document parses, so no caller is ever handed JSON cut
//     in half.
//
// The first rung is the untouched answer, so a document already under the bound
// is returned exactly as it was encoded and rung 0 is reported: a caller can
// tell "nothing was dropped" from "the first reduction was enough" without
// comparing bytes.
//
// An encoding error stops the ladder rather than falling through to the next
// rung. A rung that cannot be encoded is a defect in this surface, not a large
// answer, and quietly reporting the next-smaller document would hide it.
func FitResult(rungs ...func() ([]byte, error)) ([]byte, int, error) {
	if len(rungs) == 0 {
		// Unreachable from this package; loud rather than a nil answer if a
		// future caller builds an empty ladder.
		return nil, 0, errors.New("fitting an answer: a ladder needs at least one rung")
	}

	var encoded []byte

	for i, rung := range rungs {
		var err error

		encoded, err = rung()
		if err != nil {
			return nil, i, err
		}

		if len(encoded) <= MaxResultBytes {
			return encoded, i, nil
		}
	}

	return encoded, len(rungs) - 1, nil
}

// getResponseLadder is how an oversized [v1.GetResponse] is reduced, in order
// of what a reader can most afford to lose.
//
// Only this one response message gets a ladder, and the reason is worth stating
// because the absence looks like an oversight otherwise: a GetResponse is the
// one answer on this surface whose *size is chosen by the workload rather than
// by the schema*. Its step transcript, its carried state and its declared
// outputs are all values a submitted workflow computed, and they can approach
// what Temporal will carry, which is nearly two megabytes. Everything else the
// service answers is either bounded by the schema (a status, two ids, two
// timestamps) or is the caller's own submission handed back.
//
// The order is [renderRunLocalResult]'s, extended by the one field a durable
// run has and a local one never does:
//
//  1. the step transcript, which is commentary on the answer;
//  2. a RUNNING run's carried state, which is a snapshot of work in progress;
//  3. the declared outputs, which are the answer itself and so the last to go —
//     and which a single `outputs:` expression is enough to carry past the
//     bound on its own, which is why the ladder does not stop before them.
//
// Each rung returns the note that says what left, why, and what to do instead.
// The floor keeps the status, the ids, the timing and any failure message: what
// remains is bounded by the schema, so there is nothing further to drop that
// would not take the answer with it.
//
// The rungs are cumulative and must be evaluated in order, each dropping its
// field from what the one before it left — which is what [FitResult] does, and
// the only way they are called. Rung 2 on its own would answer with a document
// that still carries the transcript rung 1 was supposed to have taken.
func getResponseLadder(response *v1.GetResponse, encode func(proto.Message) ([]byte, error)) (rungs []func() ([]byte, error), notes []string) {
	// One clone, reduced further at each rung. Cloned rather than mutated
	// because the response belongs to the caller of dispatch — the local
	// server may hand back a message it still holds.
	trimmed, ok := proto.Clone(response).(*v1.GetResponse)
	if !ok {
		// Unreachable: proto.Clone of a *v1.GetResponse is a *v1.GetResponse.
		return []func() ([]byte, error){func() ([]byte, error) { return encode(response) }}, []string{""}
	}

	drops := []struct {
		drop func()
		note string
	}{
		{
			drop: func() {
				// Only when the oneof is carrying the transcript: clearing Kind
				// when it holds an `error` would drop the reason the run failed,
				// which is the most valuable thing in a failed run's document.
				if trimmed.GetOutputs() != nil {
					trimmed.Kind = nil
				}
			},
			note: "the step transcript was dropped: this run's answer exceeded %d bytes. " +
				"The status, timing and declared outputs below are complete; read the transcript " +
				"with `flow get`, or have the workflow carry less between steps",
		},
		{
			drop: func() { trimmed.EntityState = nil },
			note: "the step transcript and this run's carried state were dropped: the answer exceeded " +
				"%d bytes. The status, timing and declared outputs below are complete; read the state " +
				"with `flow get`, or have the workflow carry less in its `vars:` and `loop:` values",
		},
		{
			drop: func() { trimmed.RunOutputs = nil },
			note: "the step transcript, carried state and declared outputs were all dropped: the answer " +
				"exceeded %d bytes even without the first two, so what remains is this run's status, " +
				"ids and timing. Read what it produced with `flow get`, or have the workflow answer " +
				"with less",
		},
	}

	// Rung 0 is the untouched answer, and carries no note.
	rungs = append(rungs, func() ([]byte, error) { return encode(response) })
	notes = append(notes, "")

	for _, d := range drops {
		rungs = append(rungs, func() ([]byte, error) {
			d.drop()

			return encode(trimmed)
		})
		notes = append(notes, fmt.Sprintf(d.note, MaxResultBytes))
	}

	return rungs, notes
}
