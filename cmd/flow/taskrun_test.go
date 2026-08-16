package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
)

// `flow task run` is a real execution wearing a small command's clothes, and every
// test here is about that sentence being true rather than advertised.
//
// The ones that matter most are the negative directions: an invocation the egress
// policy refuses has to be refused *the same way* `flow run local` refuses it, an
// input the task cannot run without has to be refused before anything runs at all,
// and a value somebody marked sensitive must not reach a terminal because a
// convenience command happened to be the one rendering it.

// taskRun runs the real command and returns its two streams separately, which is
// the whole point: a test that merged them could not see the mistake the stream
// discipline exists to prevent.
//
// Through [execute] rather than root.Execute, for the reason [runLocal] records:
// that is where SilenceUsage is set and where a failure is rendered, so calling
// Execute directly tests a CLI nobody runs.
func taskRun(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()

	return taskRunUnder(t, t.Context(), args...)
}

// taskRunUnder is the same, under a context the caller controls.
func taskRunUnder(t *testing.T, ctx context.Context, args ...string) (stdout, stderr string, err error) {
	t.Helper()

	root := newRootCommand()
	var out, errOut strings.Builder
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(append([]string{"task", "run"}, args...))

	err = execute(ctx, root)

	return out.String(), errOut.String(), err
}

// TestRunningOneTaskWritesItsOutputsToStdout is the property a pipe depends on.
//
// The answer is on stdout, one line per output, and what the task narrated is not:
// a task that logs must not be able to break `flow task run ... | cut -f2` by
// existing, which is the same rule `flow run local` follows for the same reason.
func TestRunningOneTaskWritesItsOutputsToStdout(t *testing.T) {
	stdout, stderr, err := taskRun(t, "log", "--input", "message=hello from a task")
	require.NoError(t, err, stderr)

	assert.Empty(t, stdout,
		"the log task produces no outputs, so there was no answer to write")
	assert.Contains(t, stderr, "hello from a task",
		"what the task logged did not reach the stream the account goes to")
	assert.Contains(t, stderr, "COMPLETED",
		"the invocation did not say how it went, in the word the rest of the CLI uses")
}

// TestTheAnswerIsOneLinePerOutput pins the shape of the text answer.
//
// One line per output whatever the value holds, which is why a string is written in
// JSON notation: a response body has newlines in it, and written bare it would run
// over several lines with the next output's name appearing to be part of it.
func TestTheAnswerIsOneLinePerOutput(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "first\nsecond\n")
	}))
	defer server.Close()

	stdout, stderr, err := taskRun(t, "http",
		"--input", "url="+server.URL, "--egress-policy", loopbackEgressPolicy(t))
	require.NoError(t, err, stderr)

	lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	require.Len(t, lines, 3, "the http task's three outputs did not arrive as three lines:\n%s", stdout)

	answers := map[string]string{}
	for _, line := range lines {
		name, value, found := strings.Cut(line, "\t")
		require.True(t, found, "an answer line carries no tab between the name and the value: %q", line)
		answers[name] = value
	}

	assert.Equal(t, "200", answers["status_code"])
	assert.Equal(t, `"first\nsecond\n"`, answers["body"],
		"a body with newlines in it was not written as one line")
}

// TestRawWritesTheDocumentWithoutDashOJSON is the regression for the gap
// Codex's review found: --raw's own help promises the schema's protojson
// "instead of the run document", which is a request for a document on its
// own, not a modifier that only takes effect once -o json is also given.
// Before writeTaskOutputs asked runRendering.WantsDocument instead of just
// Machine(), --raw with the default text format wrote the ordinary
// tab-separated line-per-output shape here — silently ignoring the flag a
// caller who wanted the schema's own encoding had just set.
func TestRawWritesTheDocumentWithoutDashOJSON(t *testing.T) {
	stdout, stderr, err := taskRun(t, "log", "--input", "message=hi", "--raw")
	require.NoError(t, err, stderr)

	var document struct {
		Status string `json:"status"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &document),
		"--raw without -o json must still write a JSON document, not the tab-separated shape:\n%s", stdout)
	assert.Equal(t, "STATUS_COMPLETED", document.Status)
}

// TestTheMachineShapeIsTheDocumentTheLocalDriverWrites is the both-drivers rule
// applied to this verb's output.
//
// A document shaped only for `flow task run` would be a third shape for one answer.
// So this asserts the fields a caller of `flow run local -o json` already indexes
// into are the fields here, status, and the transcript keyed by step id.
func TestTheMachineShapeIsTheDocumentTheLocalDriverWrites(t *testing.T) {
	stdout, stderr, err := taskRun(t, "log", "--input", "message=hi", "-o", "json")
	require.NoError(t, err, stderr)

	var document struct {
		Status  string `json:"status"`
		Outputs struct {
			Steps map[string]map[string]any `json:"steps"`
		} `json:"outputs"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &document),
		"stdout is not a single JSON document:\n%s", stdout)

	assert.Equal(t, "STATUS_COMPLETED", document.Status,
		"the machine shape does not carry the status the local driver's document carries")
	assert.Contains(t, document.Outputs.Steps, "log",
		"the transcript is not keyed by the step id a Flowfile would have written")
}

// TestTheMachineShapeIsOneCompactLineForJSONL pins the picatz/flowstate#396
// contract for this verb: `-o jsonl` is the same document `-o json` writes,
// compacted to the one line a single invocation's answer has.
//
// Mutation-proven: reverting writeTaskOutputs's format.Machine() branch to a
// hardcoded writeJSON(surface, FormatJSON, response) makes this fail on line
// count.
func TestTheMachineShapeIsOneCompactLineForJSONL(t *testing.T) {
	stdout, stderr, err := taskRun(t, "log", "--input", "message=hi", "-o", "jsonl")
	require.NoError(t, err, stderr)

	lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	require.Len(t, lines, 1, "jsonl wrote more than the one document a task invocation answers with:\n%s", stdout)
	require.True(t, json.Valid([]byte(lines[0])), "the line is not a single JSON value: %q", lines[0])

	var document struct {
		Status string `json:"status"`
	}
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &document))
	assert.Equal(t, "STATUS_COMPLETED", document.Status)
}

// TestAnInvocationTheEgressPolicyRefusesIsRefusedExactlyAsALocalRunIs is the
// negative direction that decides whether this is the local driver's executor or a
// second one wearing its name.
//
// A second execution path would reach the task function with no policy in front of
// it and answer 200 where a run answers a refusal, the rehearsal that lies. So the
// same URL is put through both commands under the same posture and the refusals are
// compared, rather than each being asserted to contain some word about denial.
func TestAnInvocationTheEgressPolicyRefusesIsRefusedExactlyAsALocalRunIs(t *testing.T) {
	// Bound to a port nothing is listening on: the policy has to refuse before a
	// connection is attempted, so the refusal is about the address rather than
	// about anything a server did.
	const target = "http://127.0.0.1:9/"

	withShippedEgressPosture(t)

	_, _, taskErr := taskRun(t, "http", "--input", "url="+target)
	require.Error(t, taskErr, "the default egress policy allowed a loopback request")

	_, _, runErr := runLocal(t, `edition: v2026.3
name: fetches
steps:
  - id: http
    http:
      url: `+target+`
`)
	require.Error(t, runErr, "the default egress policy allowed a loopback request from a run")

	assert.Contains(t, taskErr.Error(), "denied by egress policy",
		"a task invocation was refused for some reason other than the policy")
	assert.Equal(t, refusalReason(runErr), refusalReason(taskErr),
		"one task invocation and the same step in a workflow are refused differently, "+
			"which means they did not go through the same executor")
}

// refusalReason is an error's text with the command's own framing removed, so two
// commands that wrap one refusal differently can still be compared on the refusal.
func refusalReason(err error) string {
	text := err.Error()
	if _, reason, found := strings.Cut(text, `step "http": `); found {
		return reason
	}

	return text
}

// TestAMissingRequiredInputIsRefusedBeforeAnythingRuns is the other negative
// direction, and the reason it is worth a test is the word *before*.
//
// The refusal has to arrive from the schema rather than from the task discovering
// the gap mid-flight, because a task that has already made a request cannot be
// un-made. So the task chosen is one whose only effect is visible: nothing was
// logged, which is the evidence that nothing ran.
func TestAMissingRequiredInputIsRefusedBeforeAnythingRuns(t *testing.T) {
	stdout, stderr, err := taskRun(t, "log", "--input", "level=warn")
	require.Error(t, err, "the log task ran without the message it cannot run without")

	assert.Empty(t, stdout, "a refused invocation wrote to the stream a pipe reads")
	assert.Contains(t, err.Error(), `requires input "message"`,
		"the refusal does not name the input that is missing")
	assert.NotContains(t, stderr, "WARN",
		"the task emitted its line before the invocation was refused")
}

// TestAnUnknownTaskNameSuggestsTheNearestOne is the did-you-mean, which matters
// here more than in a file: a name typed on a command line is typed from memory.
func TestAnUnknownTaskNameSuggestsTheNearestOne(t *testing.T) {
	_, _, err := taskRun(t, "htpp", "--input", "url=https://example.com")
	require.Error(t, err)

	assert.Contains(t, err.Error(), `did you mean "http"?`,
		"an unknown task one keystroke from a real one got no suggestion: %v", err)

	// A name nothing is near lists what there is instead of inventing a suggestion.
	_, _, err = taskRun(t, "kubernetes-apply")
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "did you mean",
		"a name nothing resembles was answered with a suggestion anyway: %v", err)
	assert.Contains(t, err.Error(), "available tasks are",
		"a name nothing resembles was not answered with what there is: %v", err)

	// A dotted name is a plugin's, and this process genuinely cannot tell a typo
	// from a plugin it was never pointed at.
	_, _, err = taskRun(t, "slack.post")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--plugin-dir",
		"a plugin task nothing registered was diagnosed as a spelling mistake: %v", err)
}

// TestAnInputTheTaskDoesNotDeclareIsRefusedAtTheFlag checks the position as much as
// the refusal: a diagnostic about a file names a step and a field, and the thing
// this reader can actually edit is a flag.
func TestAnInputTheTaskDoesNotDeclareIsRefusedAtTheFlag(t *testing.T) {
	_, _, err := taskRun(t, "log", "--input", "mesage=hi")
	require.Error(t, err)

	assert.Contains(t, err.Error(), "--input mesage",
		"the refusal is not addressed to the flag that carries the mistake: %v", err)
	assert.Contains(t, err.Error(), `did you mean "message"?`,
		"a misspelled input got no suggestion: %v", err)
}

// TestATypeIsReadFromTheTasksOwnSchema is the "one grammar, learned once" claim
// made checkable: the same --input flag reads `level=warn` as an enum name and
// `fields={...}` as a structure, decided by the task's schema rather than by
// anything this command knows about the log task.
func TestATypeIsReadFromTheTasksOwnSchema(t *testing.T) {
	_, stderr, err := taskRun(t, "log",
		"--input", "message=structured", "--input", "level=warn",
		"--input", `fields={"team":"payments"}`)
	require.NoError(t, err, stderr)

	assert.Contains(t, stderr, "WARN", "the level was not read as the enum the schema declares")
	assert.Contains(t, stderr, "team=payments", "the fields were not read as a structure")

	// And the other direction: a word the enum does not have is refused rather than
	// carried through to the task.
	_, _, err = taskRun(t, "log", "--input", "message=hi", "--input", "level=shout")
	require.Error(t, err, "a level the schema does not declare was accepted")
}

// TestAnInputFileIsReadTheSameWayARunReadsOne pins that the second half of the
// grammar is reached rather than reimplemented.
func TestAnInputFileIsReadTheSameWayARunReadsOne(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inputs.json")
	require.NoError(t, os.WriteFile(path,
		[]byte(`{"message": "from a file", "fields": {"team": "payments"}}`), 0o600))

	_, stderr, err := taskRun(t, "log", "--input-file", path)
	require.NoError(t, err, stderr)

	assert.Contains(t, stderr, "from a file")
	assert.Contains(t, stderr, "team=payments")

	// A flag wins over the file, which is the precedence every tool with a config
	// file has taught people to expect, and it is the same precedence `flow run`
	// has because it is the same function deciding it.
	_, stderr, err = taskRun(t, "log", "--input-file", path, "--input", "message=from the flag")
	require.NoError(t, err, stderr)
	assert.Contains(t, stderr, "from the flag")
	assert.NotContains(t, stderr, "from a file")
}

// TestAnExpressionInputIsWrittenTheWayAFileWritesIt covers the half of the grammar
// that belongs to the language rather than to the flags: `expect:` is evaluated by
// the task in its own scope, and it has to be *written* as an expression.
func TestAnExpressionInputIsWrittenTheWayAFileWritesIt(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))
	defer server.Close()

	policy := loopbackEgressPolicy(t)

	// The response the expression accepts.
	_, stderr, err := taskRun(t, "http", "--input", "url="+server.URL,
		"--input", "expect=${response.status_code == 418}", "--egress-policy", policy)
	require.NoError(t, err, stderr)

	// The one it does not, which is the whole point of being able to try it.
	_, _, err = taskRun(t, "http", "--input", "url="+server.URL,
		"--input", "expect=${response.status_code == 200}", "--egress-policy", policy)
	require.Error(t, err, "an expect expression the response does not satisfy was accepted")

	// And a literal where the task will only evaluate an expression is refused
	// before the request rather than after it, by the same rule `flow validate`
	// applies to the same input in a file.
	_, _, err = taskRun(t, "http", "--input", "url="+server.URL,
		"--input", "expect=200", "--egress-policy", policy)
	require.Error(t, err, "a literal expect was accepted")
	assert.Contains(t, err.Error(), "${...}",
		"the refusal does not say how to write it: %v", err)
}

// TestAnExpressionInsideAStructureIsRefusedRatherThanCarriedAsText is the
// diagnostics rule applied to the one thing a command line cannot compile.
//
// A Flowfile turns a mapping holding `${...}` into one expression that builds it.
// Nothing here walks a structure, so the same JSON would travel as the literal
// characters and the step would answer with them, a wrong result rather than a
// refusal, which is worse than a missing feature.
func TestAnExpressionInsideAStructureIsRefusedRatherThanCarriedAsText(t *testing.T) {
	_, _, err := taskRun(t, "log", "--input", "message=hi",
		"--input", `fields={"code":"${response.status_code}"}`)
	require.Error(t, err, "an expression nested in a structure was quietly carried as text")

	assert.Contains(t, err.Error(), "one expression",
		"the refusal does not say what to write instead: %v", err)
}

// TestASensitiveInputIsWithheldFromTheEchoUnlessRevealed is the sensitive contract,
// in both directions.
//
// The invocation echo is the one surface this command renders an input on, and it
// is exactly where somebody first pastes a token. The reveal is typed on purpose,
// every invocation, and says so on stderr when it is used.
func TestASensitiveInputIsWithheldFromTheEchoUnlessRevealed(t *testing.T) {
	const material = "hunter2-not-in-a-terminal"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, material, r.Header.Get("X-Token"))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	policy := loopbackEgressPolicy(t)
	headers := `{"X-Token":"` + material + `"}`

	stdout, stderr, err := taskRun(t, "http", "--input", "url="+server.URL,
		"--input", "headers="+headers, "--sensitive", "headers", "--egress-policy", policy)
	require.NoError(t, err, stderr)

	assert.NotContains(t, stdout, material,
		"a value marked sensitive reached the stream a pipe reads")
	assert.NotContains(t, stderr, material,
		"a value marked sensitive was echoed in the clear")
	assert.Contains(t, stderr, "[redacted: headers]",
		"the echo does not say a value was withheld, which reads as it never having been given")

	// Typed on purpose, and the invocation says so beside the effect.
	_, stderr, err = taskRun(t, "http", "--input", "url="+server.URL,
		"--input", "headers="+headers, "--sensitive", "headers",
		"--reveal-sensitive", "--egress-policy", policy)
	require.NoError(t, err, stderr)
	assert.Contains(t, stderr, material, "--reveal-sensitive withheld the value anyway")
	assert.Contains(t, stderr, "--reveal-sensitive",
		"a transcript carries the revealed value without the choice that revealed it")
}

// TestAnInputTheSchemaCallsAuthorityIsSensitiveWithoutBeingNamed is the half of the
// rule nobody should have to remember.
//
// The http task's `bearer:` is declared an authority input by the schema, which is
// the schema saying a value there *is* a credential. Making somebody also pass
// --sensitive for it would be a default that leaks for everybody who did not know
// to type it.
func TestAnInputTheSchemaCallsAuthorityIsSensitiveWithoutBeingNamed(t *testing.T) {
	const material = "bearer-material-nobody-typed-sensitive-for"

	_, stderr, _ := taskRun(t, "http",
		"--input", "url=https://example.invalid/", "--input", "bearer="+material)

	assert.NotContains(t, stderr, material,
		"a value at an input the schema calls authority was echoed in the clear")
	assert.Contains(t, stderr, "[redacted: bearer]",
		"the echo does not say the bearer was withheld")
}

// TestASecretReferenceNeedsTheSameOptInsARunNeeds is the fail-closed direction for
// the containment mechanism, as opposed to the display one above.
//
// The same `${secret(...)}` spelling a file uses, resolved only where the opt-ins
// that authorize it are present, and refused where they are not, rather than
// resolved because a convenience command was the one asking.
func TestASecretReferenceNeedsTheSameOptInsARunNeeds(t *testing.T) {
	const material = "resolved-only-inside-the-activity"

	var authorized string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorized = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	t.Setenv("FLOWSTATE_SECRET_API_TOKEN", material)

	access := filepath.Join(t.TempDir(), "auth.yaml")
	require.NoError(t, os.WriteFile(access, []byte(`issuers:
  - name: local
    issuer: https://issuer.example
    audiences: [flowstate]
    algorithms: [RS256]
secrets:
  allow:
    - 'true'
`), 0o600))

	policy := loopbackEgressPolicy(t)
	reference := `bearer=${secret("env:API_TOKEN")}`

	stdout, stderr, err := taskRun(t, "http", "--input", "url="+server.URL,
		"--input", reference, "--egress-policy", policy,
		"--auth-policy", access, "--secret-env", "API_TOKEN")
	require.NoError(t, err, stderr)

	assert.Equal(t, "Bearer "+material, authorized,
		"the reference did not resolve to the secret inside the request")
	assert.NotContains(t, stdout, material, "a resolved secret reached stdout")
	assert.NotContains(t, stderr, material, "a resolved secret reached stderr")

	// Without the opt-ins: refused, not resolved.
	_, _, err = taskRun(t, "http", "--input", "url="+server.URL,
		"--input", reference, "--egress-policy", policy)
	require.Error(t, err, "a secret reference resolved with no provider configured and no policy allowing it")
}

// TestTheWorkedExampleRuns is the house gate: the example in the help is executed
// verbatim out of the constant the help renders from.
//
// An example nothing checks is a promise nobody keeps, and the promise this one
// makes is the strongest kind, because it is the first thing a person will paste.
// Only the invocations that need no network are run, which is why the log one is
// first in the list.
func TestTheWorkedExampleRuns(t *testing.T) {
	invocations := offlineExampleInvocations(t, taskRunExample)
	require.NotEmpty(t, invocations, "no runnable invocation was found in the worked example")

	for _, args := range invocations {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			stdout, stderr, err := taskRun(t, args...)
			require.NoError(t, err, "the worked example does not run: %s\n%s", stderr, stdout)
		})
	}
}

// offlineExampleInvocations returns the `flow task run` lines of an example that
// reach nothing outside this process, split into arguments.
//
// Deliberately conservative: a line naming a URL, a plugin directory or a policy
// file needs something this test cannot supply, and running it would fail for a
// reason that says nothing about the example being right. What is left is the line
// somebody pastes first, which is the one worth guaranteeing.
func offlineExampleInvocations(t *testing.T, example string) [][]string {
	t.Helper()

	var invocations [][]string
	for _, line := range strings.Split(example, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "flow task run ") {
			continue
		}
		if strings.ContainsAny(line, "|") || strings.Contains(line, "://") ||
			strings.Contains(line, "--plugin-dir") || strings.Contains(line, "--auth-policy") {
			continue
		}

		invocations = append(invocations, splitExampleArgs(strings.TrimPrefix(line, "flow task run ")))
	}

	return invocations
}

// splitExampleArgs splits a command line on spaces, keeping a single-quoted run
// together, the quoting the examples actually use, and nothing more, because a
// general shell parser here would be a second thing to get wrong.
func splitExampleArgs(line string) []string {
	var (
		args    []string
		current strings.Builder
		quoted  bool
	)

	for _, r := range line {
		switch {
		case r == '\'':
			quoted = !quoted
		case r == ' ' && !quoted:
			if current.Len() > 0 {
				args = append(args, current.String())
				current.Reset()
			}
		default:
			current.WriteRune(r)
		}
	}
	if current.Len() > 0 {
		args = append(args, current.String())
	}

	return args
}

// TestAPluginsTaskRunsThroughTheSameDiscoveryAWorkerUses is #380's biggest
// beneficiary, proven rather than declared.
//
// A plugin author's loop was build, install, write a Flowfile, run, read history.
// This collapses it to one line, and because the line goes through the real
// discovery, handshake, descriptor rebuild and wire codec, the debugging tool
// doubles as the conformance probe.
//
// Registration into the default registry is a one-way door, so the absence is
// asserted first: after this test runs, `example.greet` is in this binary's
// registry for good. That is the tradeoff [TestPluginDirWiresPluginTasksIntoTheMCPSurface]
// already accepts for the same reason.
func TestAPluginsTaskRunsThroughTheSameDiscoveryAWorkerUses(t *testing.T) {
	_, _, err := taskRun(t, "example.greet", "--input", "name=world")
	require.Error(t, err, "a plugin task resolved before any plugin directory was given")

	dir := buildExamplePluginDir(t)

	stdout, stderr, err := taskRun(t, "example.greet", "--input", "name=world", "--plugin-dir", dir)
	require.NoError(t, err, stderr)

	assert.Contains(t, stdout, "Hello, world!",
		"the plugin's answer did not reach the stream a pipe reads:\n%s", stdout)
	assert.NotContains(t, stdout, "loaded plugin",
		"the plugin host's own commentary reached the stream a pipe reads")
}

// TestAPluginBinaryIsNotLaunchedWithoutADirectory guards the discovery seam in the
// direction that costs something: this command must not start reading a search path
// nobody configured.
func TestAPluginBinaryIsNotLaunchedWithoutADirectory(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "launched")

	// A "plugin" that would leave a trace if anything ran it.
	binary := filepath.Join(dir, plugin.BinaryPrefix+"tripwire")
	require.NoError(t, os.WriteFile(binary,
		[]byte("#!/bin/sh\ntouch "+marker+"\n"), 0o700))

	t.Setenv(pluginSearchPathEnv, "")

	_, _, err := taskRun(t, "log", "--input", "message=hi")
	require.NoError(t, err)

	_, statErr := os.Stat(marker)
	assert.True(t, os.IsNotExist(statErr),
		"a plugin binary was launched by an invocation that configured no plugin directory")
}

// withShippedEgressPosture puts the http task back to the policy this build ships,
// for the duration of one test.
//
// It is needed because `--egress-policy` registers over the http task in
// [v1.DefaultRegistry], which is process-wide and has no Unregister, so the first
// test anywhere in this binary that passes a loopback-allowing policy leaves every
// later test running under it. `cmd/flow`'s own secrets test does exactly that, and
// alphabetically it runs first, which is why a test asserting the *default* posture
// cannot simply not configure one: by the time it runs, one is configured.
//
// [v1.DefaultEgressPolicy] is the constant this build ships rather than whatever
// the registry currently holds, the distinction that function's own doc draws, and
// the reason it exists. The previous definition is captured and put back, so this
// leaves the binary as it found it rather than making the next test's posture
// depend on whether this one ran.
func withShippedEgressPosture(t *testing.T) {
	t.Helper()

	previous, found := v1.LookupTask("http")
	require.True(t, found, "this build has no http task to restore")

	require.NoError(t, v1.DefaultRegistry().Register(v1.HTTPTaskDef(v1.DefaultEgressPolicy())))

	t.Cleanup(func() {
		require.NoError(t, v1.DefaultRegistry().Register(previous))
	})
}

// loopbackEgressPolicy writes the policy a test server needs to be reachable, and
// is the reason every http test here passes --egress-policy.
//
// The default policy denies loopback, which is exactly the behaviour
// [TestAnInvocationTheEgressPolicyRefusesIsRefusedExactlyAsALocalRunIs] pins. So a
// test that wants a request to succeed says so through the same flag an operator
// would use, rather than through anything that reaches around the policy.
func loopbackEgressPolicy(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "egress.yaml")
	require.NoError(t, os.WriteFile(path, []byte("egress:\n  allow_loopback: true\n"), 0o600))

	return path
}
