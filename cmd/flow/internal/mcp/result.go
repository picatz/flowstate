package mcp

import (
	"errors"
	"fmt"
	"slices"
	"strings"

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
// # The floor's contract: it fits, it parses, and it validates
//
// Two of those had to be bought rather than assumed, and both were found by
// review on #853.
//
// **It validates.** `GetResponse.kind` is a *required* oneof
// (proto/flowstate/v1/service.proto:256-259), so the obvious way to shed a
// transcript — clearing `Kind` — answers with a document `protojson.Unmarshal`
// happily accepts and [v1.Validate] rejects. A schema-validating consumer would
// have been handed a malformed GetResponse by a successful call. The oneof is
// therefore never cleared: the transcript arm is *reduced* to a bounded subset
// of real steps ([ReduceTranscript]), and the error arm to a bounded prefix of
// the real message ([CapErrorMessage]). Every "empty" spelling was measured and
// is invalid too — an absent `step_values`, an empty one, a step with no
// `named_values`, an error with no `message` — which is why the reduction keeps
// something real rather than something blank.
//
// **It fits.** A failed run's `RunResponse.Error.message` has no `max_len` in
// the schema and is workload-chosen, so before [CapErrorMessage] every rung of
// a failure-only response was a no-op — no transcript to reduce, nil carried
// state, nil declared outputs — and the floor came back over the bound as
// though it had fitted. That is the attacker-controlled resource escaping the
// bound, which is the one thing this whole file exists to prevent. The message
// cap closes it, and [dispatch] additionally refuses outright if a floor is
// somehow still oversized, so the byte bound holds even if a future rung is
// added that cannot shrink some new field.
//
// The rungs never make a document *less* valid than it arrived. If a transcript
// holds no step that could be kept validly, the reduction is a no-op and the
// size is left to the refusal above rather than repaired into something the
// schema rejects: an answer that arrived invalid is not this ladder's to fix.
//
// # Notes
//
// notes is filled in *as the rungs run*, not up front, because what a rung has
// to say depends on what it found — "reduced to 3 of 47 steps" is not knowable
// until the reduction happens. [FitResult] evaluates rungs in order and stops at
// the one it settles on, so notes[rung] is written by the time it returns, and
// each entry describes everything given up to reach that rung rather than only
// that rung's own step.
//
// The rungs are cumulative and must be evaluated in order, each reducing what
// the one before it left — which is what [FitResult] does, and the only way they
// are called. Rung 2 on its own would answer with a document that still carries
// the transcript rung 1 was supposed to have reduced.
func getResponseLadder(response *v1.GetResponse, encode func(proto.Message) ([]byte, error)) (rungs []func() ([]byte, error), notes []string) {
	// One clone, reduced further at each rung. Cloned rather than mutated
	// because the response belongs to the caller of dispatch — the local
	// server may hand back a message it still holds.
	trimmed, ok := proto.Clone(response).(*v1.GetResponse)
	if !ok {
		// Unreachable: proto.Clone of a *v1.GetResponse is a *v1.GetResponse.
		return []func() ([]byte, error){func() ([]byte, error) { return encode(response) }}, []string{""}
	}

	// What has been given up so far, in the order it was given up.
	var given []string

	reductions := []func() string{
		// Bound whichever arm of the required oneof this run is carrying. Both
		// are workload-chosen text, and neither may be cleared.
		func() string {
			switch {
			case trimmed.GetOutputs() != nil:
				kept, total := ReduceTranscript(trimmed.GetOutputs())
				if kept == total {
					return ""
				}

				return fmt.Sprintf("the step transcript was reduced to %d of its %d steps "+
					"(the steps shown are real and complete; the rest were omitted)", kept, total)

			case trimmed.GetError() != nil:
				if !CapErrorMessage(trimmed.GetError()) {
					return ""
				}

				return fmt.Sprintf("this run's failure message was truncated to its first %d bytes",
					maxReducedErrorBytes)
			}

			return ""
		},

		func() string {
			if trimmed.GetEntityState() == nil {
				return ""
			}
			trimmed.EntityState = nil

			return "the carried state of this running run was dropped"
		},

		func() string {
			if !DropDeclaredOutputs(trimmed) {
				return ""
			}

			return "the run's declared outputs were dropped"
		},
	}

	// Rung 0 is the untouched answer, and carries no note.
	rungs = append(rungs, func() ([]byte, error) { return encode(response) })
	notes = append(notes, "")

	for i, reduce := range reductions {
		rung := i + 1

		rungs = append(rungs, func() ([]byte, error) {
			if described := reduce(); described != "" {
				given = append(given, described)
			}
			notes[rung] = describeReduction(given)

			return encode(trimmed)
		})
		notes = append(notes, "")
	}

	return rungs, notes
}

// describeReduction is the note a reduced answer carries: what was given up,
// why, and what to do about it.
func describeReduction(given []string) string {
	if len(given) == 0 {
		return ""
	}

	return fmt.Sprintf("%s: this run's answer exceeded this surface's %d byte limit. "+
		"Everything still present below is real and unmodified — read the run in full with "+
		"`flow get`, or have the workflow carry less.",
		capitalizeFirst(strings.Join(given, ", and ")), MaxResultBytes)
}

// capitalizeFirst starts the note as a sentence.
func capitalizeFirst(s string) string {
	if s == "" {
		return s
	}

	return strings.ToUpper(s[:1]) + s[1:]
}

// maxReducedErrorBytes bounds a failure message the ladder had to truncate.
//
// A run's failure message is workload-chosen — it can carry a task's or an
// application's own error — and `RunResponse.Error.message` has no `max_len` in
// the schema, so this is where that text stops being unbounded on this surface.
// Four kilobytes for the same reason `maxTestFailureMessageBytes` uses it: it is
// enough to carry the sentence a reader needs and the first frames of whatever
// produced it.
//
// Truncated rather than dropped, deliberately. The reason a run failed is the
// single most valuable thing in a failed run's document, and a shortened reason
// is worth far more than none — so the error arm always survives, and always
// survives as *its own first bytes* rather than as a placeholder.
const maxReducedErrorBytes = 4 << 10

// capErrorMessage truncates a run's failure message to [maxReducedErrorBytes],
// reporting whether it had to.
//
// The cut is made on a rune boundary. A string field carrying invalid UTF-8 is
// one protojson refuses to marshal at all, so slicing mid-rune would turn a
// large answer into an encoding error — the failure this ladder exists to
// replace, arriving by a different door.
func CapErrorMessage(runError *v1.RunResponse_Error) bool {
	// Nil when the oneof is carrying the transcript rather than an error, which
	// is an ordinary call from a ladder that does not know which arm it has.
	if runError == nil {
		return false
	}

	message := runError.GetMessage()
	if len(message) <= maxReducedErrorBytes {
		return false
	}

	runError.Message = strings.ToValidUTF8(message[:maxReducedErrorBytes], "") +
		fmt.Sprintf("... (truncated, exceeded %d bytes)", maxReducedErrorBytes)

	return true
}

// DropDeclaredOutputs removes what a run declared it would answer with,
// reporting whether there was anything to remove.
//
// Both places, which is the whole reason this is a function rather than one
// assignment. A run's declared outputs reach a reader either as
// `GetResponse.run_outputs` or nested inside the transcript arm as
// `GetResponse.outputs.run_outputs`, and which one a given driver populates is
// not something a ladder should have to know: the durable path sets the former,
// while a local run's `localRun` carries the whole `Workflow.StepOutputs` it got
// back from the engine, declared outputs included.
//
// Missing the nested one is not a small leak — it is the entire declared output,
// which is the field a single `outputs:` expression can make megabytes wide. It
// went unnoticed while the transcript rung cleared the oneof outright, because
// dropping the arm took the nested copy with it; reducing the arm instead rather
// than clearing it (see the floor's contract above) left the copy behind, and
// TestTheRunLocalAnswerIsBoundedByItsDeclaredOutputs caught it immediately.
func DropDeclaredOutputs(response *v1.GetResponse) bool {
	dropped := false

	if response.GetRunOutputs() != nil {
		response.RunOutputs = nil
		dropped = true
	}

	if outputs := response.GetOutputs(); outputs.GetRunOutputs() != nil {
		outputs.RunOutputs = nil
		dropped = true
	}

	return dropped
}

// maxReducedTranscriptBytes bounds the step transcript a reduced answer keeps.
//
// Half the surface's budget: enough that a reduced transcript is worth reading,
// while leaving room for the status, the timing and the declared outputs, which
// is the part of the document a reader needs most and the part this ladder gives
// up last.
const maxReducedTranscriptBytes = MaxResultBytes / 2

// reduceTranscript keeps as much of a step transcript as fits in
// [maxReducedTranscriptBytes], reporting how many of its steps survived.
//
// Smallest steps first, so a reduced transcript holds as many whole steps as it
// can rather than one enormous one, and ties are broken by name so that the same
// run always reduces to the same document — a map's iteration order is not an
// answer to give a caller twice and have differ.
//
// Every step kept is *real and unmodified*. Nothing is synthesized to stand in
// for what was omitted: a fabricated entry in a transcript is indistinguishable
// from a step the workflow actually ran, and a reader cannot be expected to know
// which of its own steps this surface invented. The count in the note is how the
// omission is reported instead.
//
// At least one step is always kept when any step can be kept at all, because
// `step_values` is required and an empty transcript is a document the schema
// rejects. A step carrying no `named_values` is skipped entirely for the same
// reason — keeping one would answer with something invalid. If that leaves
// nothing keepable, the transcript is returned exactly as it arrived: it is not
// this function's business to repair a document that was already invalid.
func ReduceTranscript(outputs *v1.Workflow_StepOutputs) (kept, total int) {
	steps := outputs.GetStepValues()
	total = len(steps)

	type entry struct {
		name string
		size int
	}

	entries := make([]entry, 0, total)

	for name, node := range steps {
		if len(node.GetNamedValues()) == 0 {
			continue
		}

		entries = append(entries, entry{name: name, size: proto.Size(node)})
	}

	if len(entries) == 0 {
		return total, total
	}

	slices.SortFunc(entries, func(a, b entry) int {
		if a.size != b.size {
			return a.size - b.size
		}

		return strings.Compare(a.name, b.name)
	})

	reduced := make(map[string]*v1.Node_Outputs, len(entries))
	budget := maxReducedTranscriptBytes

	for i, e := range entries {
		// The first is kept whatever it costs: a transcript with no steps is
		// invalid, so "too big to keep even one" is still one.
		if i > 0 && e.size > budget {
			break
		}

		reduced[e.name] = steps[e.name]
		budget -= e.size
	}

	outputs.StepValues = reduced

	return len(reduced), total
}
