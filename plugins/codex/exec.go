package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"strings"

	codex "github.com/picatz/openai/codex"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"

	codexv1 "github.com/picatz/flowstate/plugins/codex/gen/codex/v1"
)

// defaultSandboxMode is what an unspecified sandbox_mode input becomes - the
// most restricted level the codex CLI has, never the most permissive. See
// codex.proto's own doc comment on SandboxMode for why this is the fail-closed
// direction CLAUDE.md asks of every policy surface in this repository.
const defaultSandboxMode = codexv1.SandboxMode_SANDBOX_MODE_READ_ONLY

// codexExec implements codex.exec: one bounded agentic run.
func codexExec(ctx context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
	var in codexv1.ExecInputs
	if err := sdk.DecodeInputs(inputs, &in); err != nil {
		return nil, sdk.InvalidInput("%v", err)
	}

	prompt, err := validatePrompt(in.GetPrompt())
	if err != nil {
		return nil, err
	}
	model, err := validateModel(in.GetModel())
	if err != nil {
		return nil, err
	}

	sandbox := in.GetSandboxMode()
	if sandbox == codexv1.SandboxMode_SANDBOX_MODE_UNSPECIFIED {
		sandbox = defaultSandboxMode
	}
	sandboxArg, err := sandboxCLIValue(sandbox)
	if err != nil {
		return nil, err
	}

	// Layer 2 and layer 3 of the three-layer design (see policy.go's own
	// doc comment on policyEnv): the operator's own ceiling, and this
	// call's request checked against it. Both checks are refusals, never
	// downgrades - a request over the ceiling is wrong, not "clamped and
	// still ran."
	policy, err := loadOperatorPolicy()
	if err != nil {
		return nil, err
	}
	if err := narrowSandbox(sandbox, policy); err != nil {
		return nil, err
	}
	allowNetwork := in.GetAllowNetwork()
	mutatingSandbox := sandbox == codexv1.SandboxMode_SANDBOX_MODE_WORKSPACE_WRITE
	if mutatingSandbox {
		if err := narrowNetwork(allowNetwork, policy); err != nil {
			return nil, err
		}
	}

	workDir, err := resolveWorkingContext(in.GetWorkingContext())
	if err != nil {
		return nil, err
	}

	// A writable run must say where it may write. Without a working_context
	// there is no --cd and no cmd.Dir, so the child would inherit this plugin
	// process's own current directory - which the host sets to the private
	// plugin socket directory (pkg/flowstate/v1/plugin/launch.go), putting
	// host-managed files inside a writable agent's reach and entirely outside
	// the operator's configured root. Refused rather than silently jailed
	// somewhere invented: a writable run with nowhere declared to write is an
	// authoring mistake, and the author knows which directory was meant.
	if sandbox != codexv1.SandboxMode_SANDBOX_MODE_READ_ONLY && workDir == "" {
		return nil, sdk.InvalidInput(
			"sandbox_mode %s writes, so working_context is required: name the directory this run "+
				"may write in (it must resolve inside the worker's configured root), or use "+
				"SANDBOX_MODE_READ_ONLY", sandbox.String())
	}

	maxOutput, err := clampMaxOutputBytes(in.GetMaxOutputBytes())
	if err != nil {
		return nil, err
	}
	maxEvents, err := clampMaxEvents(in.GetMaxEvents())
	if err != nil {
		return nil, err
	}

	binPath, err := resolveCodexBinary()
	if err != nil {
		return nil, err
	}

	// Registered before the key is ever used, so that every error path below
	// - including one raised before the run starts - is scrubbed by the time
	// it can leave this function. See errors.go's own note on why the
	// classification is built from an already-scrubbed message rather than
	// scrubbing the finished *classified error.
	scrubber := secrets.NewScrubber()
	apiKey, err := apiKeyFromValue(in.GetApiKey())
	if err != nil {
		return nil, err
	}
	scrubber.AddValue(apiKey)

	// The ephemeral CODEX_HOME this run's subprocess sees - never the
	// worker user's own ~/.codex - built fresh per call and torn down
	// before this function returns. See ephemeral.go and process.go's own
	// doc comments for why this plugin does not use the codex library's
	// default subprocess environment.
	codexHome, cleanupHome, err := buildEphemeralHome(policy)
	if err != nil {
		return nil, err
	}
	defer cleanupHome()

	runCtx, cancel := context.WithTimeout(ctx, runTimeout)
	defer cancel()

	argv := buildArgs(model, sandboxArg, workDir, allowNetwork, mutatingSandbox)
	env := childEnv(apiKey, codexHome)

	mutating := workDir != "" && sandbox != codexv1.SandboxMode_SANDBOX_MODE_READ_ONLY

	// Hardening is computed once, before the *first* Git command touches the
	// repository - including the baseline read immediately below - and
	// shared with computePatch after the run finishes. See
	// prepareHardenedGit's doc comment for why splitting this in two (each
	// side computing its own) left the baseline read exposed to whatever the
	// later hardening was built to stop.
	var gitBin string
	var hardened []string
	if mutating {
		var cleanup func()
		gitBin, hardened, cleanup, _ = prepareHardenedGit(runCtx, workDir)
		if cleanup != nil {
			defer cleanup()
		}
	}

	// Recorded before the subprocess runs: afterwards, an edit that was
	// already there and an edit this run made are indistinguishable. See
	// workspaceBaseline.
	baseline := observeWorkspace(runCtx, gitBin, hardened, workDir, mutating)

	flowstatev1.ReportProgress(ctx, flowstatev1.PhaseRequesting)

	proc, err := startCodexProcess(runCtx, binPath, workDir, codexHome, argv, env, prompt)
	if err != nil {
		return nil, sdk.Failed("starting codex: %s", scrubber.Scrub(err.Error()))
	}

	flowstatev1.ReportProgress(ctx, flowstatev1.PhaseReadingResponse)

	run, err := readRun(runCtx, proc, maxEvents)
	waitErr := proc.Wait()

	if err == nil {
		err = waitErr
	}

	if run.turnFailed != "" {
		return nil, sdk.Failed("codex run failed: %s", scrubber.Scrub(run.turnFailed))
	}
	if err != nil {
		return nil, classifyRunError(err, run.sawSideEffect, mutating, scrubber)
	}

	patch, filesChanged, patchTruncated := computePatch(runCtx, gitBin, hardened, workDir, mutating, baseline, run.filesChanged)

	// max_output_bytes bounds everything this task returns, not each field
	// separately: the per-field caps below it are ceilings a single field may
	// not exceed even when the total would allow it, and this is the allowance
	// they are spent against. Allocated in a fixed order - final message, then
	// patch, then events - so what survives truncation is deterministic.
	remaining := maxOutput

	finalMessage, finalTruncated := truncateBytes(scrubber.Scrub(run.finalMessage), min(maxFinalMessageBytes, remaining))
	remaining -= len(finalMessage)

	if len(patch) > remaining {
		patch, patchTruncated = truncateBytes(patch, max(remaining, 0))
	}
	remaining -= len(patch)

	events, eventsTruncated := boundEvents(run.events, maxEvents, remaining, scrubber)

	truncated := run.streamTruncated || finalTruncated || patchTruncated || eventsTruncated || run.eventsTruncated

	return sdk.EncodeOutputs(&codexv1.ExecOutputs{
		FinalMessage:      finalMessage,
		Patch:             patch,
		FilesChanged:      filesChangedValue(filesChanged),
		ThreadId:          run.threadID,
		InputTokens:       run.inputTokens,
		CachedInputTokens: run.cachedInputTokens,
		OutputTokens:      run.outputTokens,
		Events:            eventsValue(events),
		Truncated:         truncated,
	})
}

// fileChange is this task's own in-memory shape for one changed file - not
// a proto message, because [sdk.EncodeOutputs] does not convert a repeated
// field of a plugin-defined message type (see codex.proto's own comment on
// ExecOutputs.files_changed for why that field is carried as
// flowstate.v1.Value instead, built from this type by filesChangedValue).
type fileChange struct {
	Path       string
	OldPath    string
	ChangeType string
}

// filesChangedValue renders a slice of fileChange as the map shape
// codex.proto's own comment on files_changed documents.
func filesChangedValue(files []fileChange) *flowstatev1.Value {
	items := make([]any, 0, len(files))
	for _, f := range files {
		items = append(items, map[string]any{
			"path":        f.Path,
			"old_path":    f.OldPath,
			"change_type": f.ChangeType,
		})
	}
	return &flowstatev1.Value{Kind: &flowstatev1.Value_Literal{Literal: sdk.Literal(items)}}
}

// eventsValue renders a slice of eventLine as the map shape codex.proto's
// own comment on events documents.
func eventsValue(lines []eventLine) *flowstatev1.Value {
	items := make([]any, 0, len(lines))
	for _, l := range lines {
		items = append(items, map[string]any{
			"kind":    l.kind,
			"summary": l.summary,
		})
	}
	return &flowstatev1.Value{Kind: &flowstatev1.Value_Literal{Literal: sdk.Literal(items)}}
}

// runResult accumulates what one run's event stream reported.
type runResult struct {
	threadID     string
	finalMessage string
	turnFailed   string

	inputTokens       int64
	cachedInputTokens int64
	outputTokens      int64

	filesChanged []fileChange

	events          []eventLine
	eventsTruncated bool

	// sawSideEffect reports whether an item that could have changed
	// something outside this process - a command execution or a file
	// change - was at least started, which is what errors.go's
	// classifyRunError needs to decide between "safe to retry" and
	// "may have already happened."
	sawSideEffect bool

	// streamTruncated reports whether boundedReader cut off the
	// subprocess's own stdout before it reached a natural end.
	streamTruncated bool
}

type eventLine struct {
	kind    string
	summary string
}

// readRun decodes codex's JSON event stream, bounded below the library the
// same way CLAUDE.md's connect-go example asks every bound to be: on the
// transport, not inside a library call this plugin does not control the
// error paths of. maxSubprocessBytes (see bounds.go) is the byte cap on the
// child's combined stdout; maxEvents bounds how many event lines this
// function keeps in memory at all, so a chatty run cannot make this task
// hold an unbounded slice before EncodeOutputs ever applies its own cap.
func readRun(ctx context.Context, proc *codexProcess, maxEvents int) (runResult, error) {
	var result runResult

	reader := &boundedReader{r: proc.stdout, remaining: maxSubprocessBytes}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64*1024), 4<<20)

	for scanner.Scan() {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var event codex.ThreadEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			// A line this build's copy of the library cannot parse is
			// recorded as an unknown-shaped event summary rather than
			// aborting the whole run over one line neither side controls
			// the format of.
			appendEvent(&result, maxEvents, eventLine{kind: "unparsed", summary: "(one event line could not be decoded)"})
			continue
		}

		applyEvent(&result, &event, maxEvents)

		if result.turnFailed != "" {
			break
		}
	}

	if reader.truncated {
		result.streamTruncated = true

		// Nothing will read this child's stdout again, so it is about to block
		// forever on a full pipe and Wait would not return until the run
		// timeout killed it. Killed here instead, which is what makes the
		// output bound bound the run's duration and not only its bytes.
		proc.Kill()
	}

	if err := scanner.Err(); err != nil {
		return result, err
	}

	return result, nil
}

// applyEvent folds one decoded event into the accumulating result.
func applyEvent(result *runResult, event *codex.ThreadEvent, maxEvents int) {
	switch event.Type {
	case codex.EventTypeThreadStarted:
		result.threadID = event.ThreadID
		appendEvent(result, maxEvents, eventLine{kind: string(event.Type), summary: "thread started"})

	case codex.EventTypeTurnStarted:
		appendEvent(result, maxEvents, eventLine{kind: string(event.Type), summary: "turn started"})

	case codex.EventTypeTurnCompleted:
		if usage := event.Usage; usage != nil {
			result.inputTokens = int64(usage.InputTokens)
			result.cachedInputTokens = int64(usage.CachedInputTokens)
			result.outputTokens = int64(usage.OutputTokens)
		}
		appendEvent(result, maxEvents, eventLine{kind: string(event.Type), summary: "turn completed"})

	case codex.EventTypeTurnFailed:
		msg := "turn failed"
		if event.Error != nil && event.Error.Message != "" {
			msg = event.Error.Message
		}
		result.turnFailed = msg
		appendEvent(result, maxEvents, eventLine{kind: string(event.Type), summary: truncateRunes(msg, maxEventSummaryBytes)})

	case codex.EventTypeItemStarted, codex.EventTypeItemUpdated, codex.EventTypeItemCompleted:
		applyItemEvent(result, event, maxEvents)

	case codex.EventTypeError:
		msg := event.Message
		if msg == "" {
			msg = "error"
		}
		appendEvent(result, maxEvents, eventLine{kind: string(event.Type), summary: truncateRunes(msg, maxEventSummaryBytes)})

	default:
		appendEvent(result, maxEvents, eventLine{kind: string(event.Type), summary: ""})
	}
}

// applyItemEvent handles the item.* events, which carry the polymorphic
// ThreadItem payload the codex library decodes for us.
func applyItemEvent(result *runResult, event *codex.ThreadEvent, maxEvents int) {
	item := event.Item

	kind := "unknown"
	summary := ""

	switch v := item.(type) {
	case *codex.AgentMessageItem:
		kind = string(codex.ItemTypeAgentMessage)
		summary = truncateRunes(v.Text, maxEventSummaryBytes)
		if event.Type == codex.EventTypeItemCompleted {
			result.finalMessage = v.Text
		}

	case *codex.ReasoningItem:
		kind = string(codex.ItemTypeReasoning)
		summary = truncateRunes(v.Text, maxEventSummaryBytes)

	case *codex.CommandExecutionItem:
		kind = string(codex.ItemTypeCommandExecution)
		summary = truncateRunes("command: "+v.Command+" ("+string(v.Status)+")", maxEventSummaryBytes)
		result.sawSideEffect = true

	case *codex.FileChangeItem:
		kind = string(codex.ItemTypeFileChange)
		summary = truncateRunes("file change ("+string(v.Status)+"), "+itoaLen(len(v.Changes))+" file(s)", maxEventSummaryBytes)
		result.sawSideEffect = true
		if event.Type == codex.EventTypeItemCompleted && v.Status == codex.PatchApplyStatusCompleted {
			for _, change := range v.Changes {
				if len(result.filesChanged) >= maxDiffFiles {
					break
				}
				result.filesChanged = append(result.filesChanged, fileChange{
					Path:       change.Path,
					ChangeType: string(change.Kind),
				})
			}
		}

	case *codex.McpToolCallItem:
		kind = string(codex.ItemTypeMcpToolCall)
		summary = truncateRunes(v.Server+"."+v.Tool+" ("+string(v.Status)+")", maxEventSummaryBytes)
		result.sawSideEffect = true

	case *codex.WebSearchItem:
		kind = string(codex.ItemTypeWebSearch)
		summary = truncateRunes("search: "+v.Query, maxEventSummaryBytes)

	case *codex.TodoListItem:
		kind = string(codex.ItemTypeTodoList)
		summary = itoaLen(len(v.Items)) + " item(s)"

	case *codex.ErrorItem:
		kind = string(codex.ItemTypeError)
		summary = truncateRunes(v.Message, maxEventSummaryBytes)

	case *codex.UnknownThreadItem:
		kind = string(v.Type)
	}

	appendEvent(result, maxEvents, eventLine{kind: kind, summary: summary})
}

// appendEvent enforces maxEvents at the point of collection - not only when
// EncodeOutputs later trims the slice - so a run with far more events than
// the ceiling never holds them all in memory at once.
func appendEvent(result *runResult, maxEvents int, e eventLine) {
	if len(result.events) >= maxEvents {
		result.eventsTruncated = true
		return
	}
	result.events = append(result.events, e)
}

// boundEvents converts the accumulated event lines into the wire type,
// scrubbing each summary and stopping early if the remaining output budget
// runs out - independent of the count-based cap appendEvent already applied.
func boundEvents(lines []eventLine, maxEvents, budget int, scrubber *secrets.Scrubber) ([]eventLine, bool) {
	out := make([]eventLine, 0, len(lines))
	truncated := false
	spent := 0

	for i, line := range lines {
		if i >= maxEvents {
			truncated = true
			break
		}
		summary := scrubber.Scrub(line.summary)
		// A non-positive budget means nothing more fits, never "unbounded".
		// The remainder handed here is what max_output_bytes has left after
		// the final message and patch, so it legitimately arrives at or below
		// zero - and treating that as "no limit" is how the one bound a caller
		// asked for gets exceeded by the field most likely to be large.
		if budget <= 0 || spent+len(summary) > budget {
			truncated = true
			break
		}
		spent += len(summary)
		out = append(out, eventLine{kind: line.kind, summary: summary})
	}

	return out, truncated
}

// boundedReader caps the total bytes read from r, so decoding a run's event
// stream cannot allocate without limit regardless of how much the codex CLI
// writes. See exec.go's readRun for how a truncation here leads to the
// subprocess being cancelled rather than left to block on a full pipe nobody
// is draining.
type boundedReader struct {
	r         io.Reader
	remaining int
	truncated bool
}

func (b *boundedReader) Read(p []byte) (int, error) {
	if b.remaining <= 0 {
		b.truncated = true
		return 0, io.EOF
	}
	if len(p) > b.remaining {
		p = p[:b.remaining]
	}
	n, err := b.r.Read(p)
	b.remaining -= n
	return n, err
}

// sandboxCLIValue maps this task's own enum onto the exact strings the
// codex CLI's `--sandbox` flag accepts (codex-rs/utils/cli/src/
// sandbox_mode_cli_arg.rs upstream, kebab-case - matching
// github.com/picatz/openai/codex's own SandboxMode constants, which this
// function used to return directly before this plugin stopped depending on
// that library for the subprocess launch itself; see process.go), refusing
// an enum number this build does not recognize rather than guessing - the
// same closed-set reasoning validate.go's scalar conversion for an EnumKind
// field applies everywhere else in this schema.
func sandboxCLIValue(mode codexv1.SandboxMode) (string, error) {
	switch mode {
	case codexv1.SandboxMode_SANDBOX_MODE_READ_ONLY:
		return string(codex.SandboxModeReadOnly), nil
	case codexv1.SandboxMode_SANDBOX_MODE_WORKSPACE_WRITE:
		return string(codex.SandboxModeWorkspaceWrite), nil
	case codexv1.SandboxMode_SANDBOX_MODE_DANGER_FULL_ACCESS:
		return string(codex.SandboxModeDangerFullAccess), nil
	default:
		return "", sdk.InvalidInput("sandbox_mode is not a value this task recognizes")
	}
}

// apiKeyFromValue extracts the resolved credential from the api_key input.
//
// By the time this task's Fn runs, api_key holds either a literal string (an
// author wrote one directly - discouraged, not refused here; see doc.go,
// "Secrets," for why this task cannot tell that case apart from the one
// below) or the value the host resolved from a secret reference this task
// declared in secret_inputs (main.go) - both arrive as the same
// [flowstatev1.Value_Literal] shape. A [flowstatev1.Value_SecretRef] should
// never reach here at all (the host refuses to forward one for a declared
// input without resolving it first), and is refused defensively rather than
// trusted to already be impossible.
func apiKeyFromValue(v *flowstatev1.Value) (string, error) {
	if v == nil {
		return "", nil
	}

	switch kind := v.GetKind().(type) {
	case nil:
		return "", nil
	case *flowstatev1.Value_Literal:
		s, ok := kind.Literal.GetKind().(*expr.Value_StringValue)
		if !ok {
			return "", sdk.InvalidInput("api_key must be a string")
		}
		return s.StringValue, nil
	case *flowstatev1.Value_SecretRef:
		return "", sdk.Failed(
			"api_key reached this task still holding a secret reference; the host is supposed to " +
				"resolve every declared secret_inputs entry before calling this task, so this is a " +
				"bug in the host or in this task's own manifest, not something a Flowfile author caused")
	default:
		return "", sdk.InvalidInput("api_key cannot be a %T", kind)
	}
}
