package main

import (
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// Bounds this plugin enforces on a run, in one file for the reason
// plugins/vcs/validate.go gives for doing the same: every one of these has
// to match the shape of what an attacker (a Flowfile author, a prompt, or
// the model's own output) actually controls, and a bound nobody can find is
// a bound nobody can review.
const (
	// maxPromptBytes and maxModelBytes bound two strings this plugin passes
	// to the subprocess - the prompt over stdin, the model as a --model
	// flag value - before either ever reaches os/exec. Generous relative to
	// anything real: a prompt worth sending an agent is rarely near a
	// megabyte, and a model identifier is a short name.
	maxPromptBytes = 1 << 20 // 1 MiB
	maxModelBytes  = 256

	// maxWorkingContextBytes bounds the working_context path string itself,
	// independent of binary.go's own check that the resolved path sits
	// inside the configured root.
	maxWorkingContextBytes = 4096

	// defaultMaxOutputBytes and maxMaxOutputBytes bound the combined size of
	// text this task reads out of a run - the final message, the computed
	// patch, and every event summary together. A request for more than the
	// ceiling is refused rather than silently clamped, the same reasoning
	// plugins/vcs/validate.go's clampMaxCommits gives: a silently reduced
	// bound looks like a working request that quietly returns less than it
	// asked for.
	defaultMaxOutputBytes = 256 << 10 // 256 KiB
	maxMaxOutputBytes     = 4 << 20   // 4 MiB

	// defaultMaxEvents and maxMaxEvents bound how many EventSummary entries
	// codex.exec returns, independent of maxOutputBytes: a run with a
	// two-line final message can still emit hundreds of small events (a
	// command that produces output on every line updates its item
	// repeatedly), and a run with few events can each have a large summary.
	// One bound cannot stand in for the other, the same split
	// plugins/vcs/proto draws between DiffOutputs.patch and .files.
	defaultMaxEvents = 200
	maxMaxEvents     = 2000

	// maxEventSummaryBytes bounds one EventSummary.summary field.
	maxEventSummaryBytes = 2048

	// maxFinalMessageBytes bounds ExecOutputs.final_message on its own,
	// tighter than the overall output budget alone would allow, so that one
	// enormous agent message cannot consume the whole of max_output_bytes
	// and leave nothing for the patch or the event summaries.
	maxFinalMessageBytes = 128 << 10 // 128 KiB

	// maxPatchBytes and maxDiffFiles bound codex.exec's own computed patch
	// output, mirroring plugins/vcs/validate.go's identical constants and
	// identical reasoning: a rename-heavy run can have many file entries and
	// a small patch, or a few files with an enormous diff, so both bounds
	// are enforced independently.
	maxPatchBytes = 1 << 20 // 1 MiB
	maxDiffFiles  = 500

	// maxGitConfigBytes bounds the key listing safeDiffArgs reads to discover
	// the content filters it must disable. Its own bound rather than
	// maxPatchBytes, because the two answer different questions and sharing
	// one couples them the wrong way round: a repository with a great many
	// config keys would truncate the listing, and a truncated listing is
	// fail-closed - no patch at all - for a reason that has nothing to do with
	// how large the patch is. Keys are short and a repository has few, so this
	// is generous enough that reaching it means something is wrong rather than
	// merely large.
	maxGitConfigBytes = 1 << 20 // 1 MiB

	// maxSubprocessBytes bounds the codex CLI's combined stdout before this
	// task ever decodes a line of it - the backstop underneath every other
	// bound in this file, the same role plugins/vcs's maxResponseBytes plays
	// for a git transport response. See exec.go's boundedReader.
	maxSubprocessBytes = 32 << 20 // 32 MiB

	// runTimeout backstops a codex run that hangs, overriding nothing a
	// step's own `timeout:` already provides - the same relationship
	// plugins/vcs's requestTimeout has to a step's own deadline. It exists
	// so that a workflow author who forgot to set one is not relying on this
	// process waiting forever for a subprocess that will never exit.
	runTimeout = 10 * time.Minute
)

// clampMaxOutputBytes applies codex.exec's default and ceiling to a
// requested output budget, refusing anything over the ceiling rather than
// silently reducing it - see this file's own doc comment on
// defaultMaxOutputBytes for why.
func clampMaxOutputBytes(requested int32) (int, error) {
	if requested == 0 {
		return defaultMaxOutputBytes, nil
	}
	if requested < 0 {
		return 0, sdk.InvalidInput("max_output_bytes must not be negative")
	}
	if requested > maxMaxOutputBytes {
		return 0, sdk.InvalidInput("max_output_bytes is %d, over the %d byte ceiling this task enforces", requested, maxMaxOutputBytes)
	}
	return int(requested), nil
}

// clampMaxEvents applies codex.exec's default and ceiling to a requested
// event count.
func clampMaxEvents(requested int32) (int, error) {
	if requested == 0 {
		return defaultMaxEvents, nil
	}
	if requested < 0 {
		return 0, sdk.InvalidInput("max_events must not be negative")
	}
	if requested > maxMaxEvents {
		return 0, sdk.InvalidInput("max_events is %d, over the %d ceiling this task enforces", requested, maxMaxEvents)
	}
	return int(requested), nil
}
