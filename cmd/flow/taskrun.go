package main

import (
	"fmt"
	"log"
	"log/slog"
	"maps"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/nearest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// Invoking one task, which is the smallest unit of execution this system has and
// the one a person could not reach until now: the way to try the http task's
// `expect:`, or to see what a plugin's task actually returns, was to write a
// Flowfile, run it, and read the answer back through the run machinery.
//
// # It is the local driver's executor, and that is the whole design
//
// One task invocation is a degenerate one-step workflow, so this compiles exactly
// that, a [v1.Workflow] with one [v1.Node] holding one [v1.Task], and hands it to
// [v1.RunWithInputs], the same function `flow run local` reaches at
// runlocal.go's own submit boundary. Everything the engine does for a step
// therefore happens here without this file asking for any of it: the retry policy
// and its defaults, a step's timeout, the egress policy the http task enforces, the
// task-shape policy checked at dispatch, secret resolution inside the activity that
// needs the value, and the identity a plugin task is called with.
//
// A second execution path, reaching into [v1.TaskDef.Fn] directly, which is four
// lines and looks tempting, would be a rehearsal that lies. It would run a task
// with no policy seam, no retry, no timeout and no runtime, and report an answer
// that a real step would not have produced. The both-drivers rule says a local run
// exists to tell an author what production will do; a *third* path exists to tell
// them what nothing does.
//
// # What is deliberately not here
//
// No state between invocations and no session. Composition is a pipe and then a
// Flowfile: the moment two invocations need to share memory, the answer is `flow
// run local`, and [taskRunLong] says so in those words.

// runTaskRun invokes one task and reports what it answered.
//
// The stream discipline is the CLI's, and it is sharper here than anywhere else
// because the whole point of this verb is composition: stdout is the answer and
// nothing else, so the invocation echo, the status line and anything a task logs
// all go to stderr.
func runTaskRun(cmd *cobra.Command, args []string) error {
	rendering, err := resolveRunRendering(cmd)
	if err != nil {
		return err
	}

	format := rendering.format

	// And telemetry, for the same reason and in the same position `flow run local`
	// starts it — see [runLocalWorkflow], which carries the argument. This verb
	// had the identical hole, and the comment further down claiming a task's log
	// lines "reach a configured collector the same way" was describing something
	// that could not happen: nothing on this path ever started the providers, so
	// the same way was no way at all.
	if _, err := startTelemetry(cmd.Context()); err != nil {
		log.Printf("WARNING: telemetry is configured but could not be started, "+
			"so this task invocation emits no trace: %v", err)
	}

	// The same three policy surfaces `flow run local` applies, in the same order
	// and through the same functions. A task invocation is a real execution, so it
	// gets the real gates: an egress policy that cannot load refuses this command
	// exactly as it refuses a run, and with nothing configured the built-in http
	// task denies internal and loopback addresses here precisely as it denies them
	// there.
	if err := applyEgressPolicy(cmd); err != nil {
		return err
	}
	if err := applyTaskPolicy(cmd); err != nil {
		return err
	}
	if _, err := localPayloadCodec(); err != nil {
		return err
	}

	// Before the task is looked up, because a plugin's tasks are not in the
	// registry until its process is running. This is the same [startPlugins] every other
	// plugin-aware verb calls, so `flow task run sql.query` resolves through the
	// discovery, the descriptor contract and the wire codec a worker would use.
	_, closePlugins, err := startPlugins(cmd, nil)
	if err != nil {
		return err
	}
	defer closePlugins()

	name := args[0]

	def, found := v1.LookupTask(name)
	if !found {
		return newUsageError(unknownTaskRunError(name))
	}

	// The task's own input schema plays the role a workflow's `inputs:` block plays
	// for `flow run`, which is what lets the flags be reached rather than reread.
	// See [collectInputs] and [taskInputDeclarations].
	declared := taskInputDeclarations(def)

	supplied, err := collectInputs(cmd, declared)
	if err != nil {
		return newUsageError(err)
	}

	values, written, err := taskInputValues(def, supplied)
	if err != nil {
		return newUsageError(err)
	}

	workflow := syntheticTaskWorkflow(def, values)

	// Everything the schema can say about these inputs, said before anything runs:
	// a name the task does not declare (with the nearest one it does), a required
	// input left out, a literal of the wrong type, an input that has to be written
	// as an expression, and whatever the task itself refuses about a literal such
	// as `ftp://` at the http task's `url:`. All of it from [flowfile.Validate], which
	// is the function `flow validate` calls, so a refusal here and a refusal about
	// a Flowfile are the same refusal.
	if err := checkTaskInputs(def, workflow); err != nil {
		return newUsageError(err)
	}

	surface := newSurface(cmd)
	reveal := revealSensitiveRequested(cmd)
	sensitive := sensitiveTaskInputs(cmd, def)

	ctx, closeSecretProviders, err := withLocalTaskRuntime(cmd, cmd.Context(), workflow)
	if err != nil {
		return err
	}
	defer closeSecretProviders()

	// Where a task's own log lines land, and how: the same handler `flow run local`
	// installs, so a `log:` task narrates onto stderr here exactly as it does there
	// and reaches a configured collector the same way.
	ctx = v1.ContextWithLogger(ctx,
		slog.New(telemetryLogHandler(newRunLogHandler(surface.Err, surface.ErrTheme))))

	if format == FormatText {
		if reveal {
			noteRevealedSensitiveValues(surface)
		}
		writeTaskInvocation(surface, def, written, sensitive, reveal)
	}

	started := time.Now()

	// The local driver's submit boundary, reached with no arguments because a task
	// invocation has none: the values are in the step, where a Flowfile would have
	// written them under `with:`. What this still buys is every check that boundary
	// makes about a specification, made about this one.
	outputs, runErr := v1.RunWithInputs(ctx, workflow, nil)
	response := localRun(outputs, runErr, cmd.Context().Err(), started, time.Now())

	if runErr != nil {
		// The half of the failure path `flow run local` had to learn: a caller that
		// asked for a document is owed one about the failure too, or it has to
		// recover the reason by parsing prose off stderr. The text shape writes
		// nothing, because there an empty stdout is the meaningful value: the
		// answer is the outputs, and a task that failed produced none.
		if rendering.WantsDocument() {
			if err := writeRunJSON(surface, rendering, response); err != nil {
				return err
			}
		}

		return fmt.Errorf("running task %s: %w", def.Name, runErr)
	}

	if format == FormatText {
		fmt.Fprintf(surface.Err, "%s task %s\n",
			surface.ErrTheme.Pill(statusTone(response.GetStatus()), statusLabel(response.GetStatus())),
			def.Name)
	}

	return writeTaskOutputs(surface, rendering, def, response)
}

// addLocalRehearsalFlags declares the identity a rehearsal runs as, and the
// policy and key that identity is read against.
//
// One declaration for the two commands that execute in this process, rather than a
// copy each. They are the same flags answering one question: who is running, so
// that secret rules, task-shape rules and a plugin task see somebody. Their help
// text also carries the limit that makes them a rehearsal rather than an
// attestation, which is exactly the sentence that must not come to be worded two
// ways in two places.
//
// The worker declares `--auth-policy` and `--identity-key` separately and on
// purpose: there they are what a deployment *is*, not what a rehearsal stands in
// for, and the help says so in different words.
func addLocalRehearsalFlags(cmd *cobra.Command) {
	cmd.Flags().String("as-subject", "local-user",
		"authenticated subject to rehearse policy as (local runs only)")
	cmd.Flags().String("as-issuer", "flowstate:local",
		"authenticated issuer to rehearse policy as (local runs only)")
	cmd.Flags().String("as-namespace", "",
		"tenant namespace to rehearse policy as (local runs only)")
	cmd.Flags().String("as-deployment", "local",
		"Flowstate deployment name to rehearse policy as (local runs only)")
	cmd.Flags().StringArray("as-claim", nil,
		"authenticated string claim NAME=VALUE to rehearse policy as (repeatable)")
	cmd.Flags().String("auth-policy", os.Getenv("FLOWSTATE_AUTH_POLICY"),
		"path to an access policy whose secrets rules authorize this local rehearsal")
	cmd.Flags().String("identity-key", os.Getenv("FLOWSTATE_IDENTITY_KEY"),
		"PKCS#8 PEM key used to mint short-lived workload assertions for federation targets")
}

// newTaskCommand builds `flow task` and the one verb under it.
//
// A group with a single member reads like ceremony and is not: `flow task run` is
// the spelling #380 asked for, and the noun is where the family grows. A `flow
// task` verb that describes rather than executes belongs beside this one, not
// under a different word.
//
// Beside `flow tasks` rather than under it, deliberately. The plural is the
// *index*: it answers what exists. The singular is the *verb*: it does something,
// with policy, secrets and a real executor behind it. Folding an execution verb
// into a listing command would make `flow tasks http --input url=...` run a request
// from a command whose whole reputation is that it only prints.
func newTaskCommand() *cobra.Command {
	taskCmd := &cobra.Command{
		Use:   "task",
		Short: "Work with a single task",
		Long: "Work with one task on its own, rather than through a workflow that contains it. " +
			"`flow tasks` says what exists; this is the verb that runs one.",
	}

	taskRunCmd := &cobra.Command{
		Use:     "run [task-name]",
		Short:   "Run one task, without writing a workflow",
		Long:    taskRunLong,
		Args:    cobra.ExactArgs(1),
		RunE:    runTaskRun,
		Example: taskRunExample,
	}

	addOutputFlag(taskRunCmd)
	addRawOutputFlag(taskRunCmd)
	addInputFlags(taskRunCmd)
	addRevealSensitiveFlag(taskRunCmd)
	addEgressPolicyFlag(taskRunCmd)
	addTaskPolicyFlag(taskRunCmd)
	addSecretFlags(taskRunCmd)
	addPluginFlags(taskRunCmd)
	addLocalRehearsalFlags(taskRunCmd)

	taskRunCmd.Flags().StringArray(sensitiveInputFlagName, nil,
		"treat this input as `sensitive: true` is treated in a file: withheld from the "+
			"invocation echo unless --reveal-sensitive is typed (repeatable). An input the "+
			"task's own schema declares as carrying authority is withheld without being named "+
			"here. Display etiquette only: the value still reaches the task, and a value that "+
			"must not is a ${secret(...)} reference instead")

	// The names a person is choosing between, from the registry this command
	// executes against, including whatever --plugin-dir added, since completion
	// that stops at the built-ins is completion that hides the tasks somebody
	// installed a plugin to get.
	taskRunCmd.ValidArgsFunction = func(cmd *cobra.Command, args []string, _ string) ([]string, cobra.ShellCompDirective) {
		if len(args) > 0 {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}

		_, closePlugins, err := startPlugins(cmd, nil)
		if err != nil {
			return v1.TaskNames(), cobra.ShellCompDirectiveNoFileComp
		}
		defer closePlugins()

		return v1.TaskNames(), cobra.ShellCompDirectiveNoFileComp
	}

	taskCmd.AddCommand(taskRunCmd)

	return taskCmd
}

// taskRunLong is the help, and it carries the two things a reader has to know
// before the flags mean anything: that this is a real execution under real policy,
// and where the scope ends.
const taskRunLong = "Run one task on its own, with no workflow and no server.\n\n" +
	"A task invocation is a one-step workflow, and that is how this executes it: the " +
	"inputs are compiled into a single step and handed to the same engine `flow run local` " +
	"runs a file with. So it is a real execution and gets the real gates: the egress " +
	"policy denies internal and loopback addresses here exactly as it denies them there, " +
	"a ${secret(...)} reference needs the same --secret-env and --auth-policy opt-ins, and " +
	"retries, timeouts and the task-shape policy behave as they will in production.\n\n" +
	"Arguments are given the way `flow run` takes them (--input name=value or " +
	"--input-file inputs.json), and the task's own input schema plays the role a " +
	"workflow's `inputs:` block plays there: it decides how a word is read, which inputs " +
	"are required, and what a value may hold. A whole value written as ${...} is an " +
	"expression, and ${secret('env:NAME')} is a reference, exactly as in a file.\n\n" +
	"stdout is the answer and stderr is the account of it, so a task invocation pipes. " +
	"--output json writes the same document `flow run local -o json` writes for a " +
	"finished run." + runDocumentHelp + "\n\n" +
	"There is no state between invocations and no session, on purpose. Composition is a " +
	"pipe and then a file: the moment two invocations need to share memory, the answer is " +
	"`flow run local`."

// taskRunExample is worked rather than illustrative: the log invocation below runs
// offline, and TestTheWorkedExampleRuns executes it verbatim out of this constant.
// An example nothing checks is a promise nobody keeps.
const taskRunExample = `# Run the log task, which needs nothing but a message:
flow task run log --input message='hello from a task'

# Fetch something, and read one output:
flow task run http --input url=https://example.com --output json | jq .outputs.steps.http.status_code

# Say what a good response looks like, the way a step's expect: does:
flow task run http --input url=https://example.com --input expect='${response.status_code == 200}'

# Send a bearer token without it reaching the terminal, or history:
flow task run http --input url=https://api.example.com/me --input bearer='${secret("env:API_TOKEN")}' --secret-env API_TOKEN --auth-policy policy.yaml

# Run a task a plugin provides, through the same discovery a worker uses:
flow task run example.greet --input name=world --plugin-dir ./plugins`

// unknownTaskRunError says what was typed, what was probably meant, and what to
// run to find out.
//
// A suggestion because a name typed on a command line is a name typed from memory:
// `flow task run htpp` is one keystroke from working, and listing forty tasks is a
// worse answer than naming the one. A dotted name is diagnosed differently for the
// reason `flowfile`'s own unknown-task message gives, `slack.post` may be exactly
// right on a worker that has the plugin, and this process, which launched only what
// --plugin-dir pointed it at, genuinely cannot tell that from a typo.
func unknownTaskRunError(name string) error {
	if plugin, _, dotted := strings.Cut(name, "."); dotted {
		return fmt.Errorf("no plugin task %q is registered here; if the %q plugin is installed, "+
			"point at it with --plugin-dir (or $%s) and `flow plugins` will show what it provides",
			name, plugin, pluginSearchPathEnv)
	}

	known := v1.TaskNames()
	if suggestion, ok := nearest.Name(name, known); ok {
		return fmt.Errorf("unknown task %q; did you mean %q?", name, suggestion)
	}

	return fmt.Errorf("unknown task %q; available tasks are %s (`flow tasks` describes each one)",
		name, strings.Join(known, ", "))
}

// taskInputDeclarations reads the task's input schema as the declarations the
// --input grammar takes.
//
// This is the whole of what makes "one grammar, learned once" true rather than
// claimed. [coerceInput] decides how a shell word is read from a declaration's
// type (`replicas=3` is three characters until a declaration calls it an int),
// and a task's inputs are a generated message whose descriptor carries exactly
// that. So the descriptor is rendered into the declaration shape and handed to the
// same function, rather than a second reader of --input being written for tasks.
//
// A field whose type is dynamic, the schema's [v1.Value], which an author writes
// as `any`, gets no declared type on purpose. That is [coerceInput]'s default arm:
// the characters as given, which is the only honest reading of a word for a field
// that will hold whatever it is handed.
func taskInputDeclarations(def v1.TaskDef) map[string]*v1.InputDeclaration {
	if def.Inputs == nil {
		return nil
	}

	fields := def.Inputs.Fields()
	declared := make(map[string]*v1.InputDeclaration, fields.Len())
	for i := range fields.Len() {
		field := fields.Get(i)
		name := string(field.Name())
		declared[name] = &v1.InputDeclaration{
			Name:     name,
			Type:     declaredTypeOfField(field),
			Required: v1.RequiredInput(field),
			// The type as an author would say it, which [describedAs] then repeats
			// in a coercion refusal. `list[string]` says more about what to write
			// than `list` does, and it is the same phrase `flow tasks` prints.
			Description: proto.String("the " + def.Name + " task's " + v1.InputTypeName(field)),
		}
	}

	return declared
}

// declaredTypeOfField maps one schema field onto the type vocabulary a declaration
// speaks.
//
// The two sets are deliberately not converted by a cast, [v1.inputTypeOf] makes
// the same point about the same pair, because they are different vocabularies that
// happen to overlap. A map or a message is a struct to an author; a repeated field
// is a list; and a field holding a dynamic value is left unspecified, which reads
// downstream as "whatever was written".
func declaredTypeOfField(field protoreflect.FieldDescriptor) v1.InputDeclaration_Type {
	switch {
	case field.IsMap():
		return v1.InputDeclaration_TYPE_STRUCT
	case field.IsList():
		return v1.InputDeclaration_TYPE_LIST
	}

	switch field.Kind() {
	case protoreflect.StringKind, protoreflect.BytesKind:
		return v1.InputDeclaration_TYPE_STRING
	case protoreflect.BoolKind:
		return v1.InputDeclaration_TYPE_BOOL
	case protoreflect.Int32Kind, protoreflect.Int64Kind, protoreflect.Sint32Kind,
		protoreflect.Sint64Kind, protoreflect.Sfixed32Kind, protoreflect.Sfixed64Kind,
		protoreflect.Uint32Kind, protoreflect.Uint64Kind, protoreflect.Fixed32Kind,
		protoreflect.Fixed64Kind:
		return v1.InputDeclaration_TYPE_INT
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return v1.InputDeclaration_TYPE_FLOAT
	case protoreflect.EnumKind:
		// An enum is written as one of its names, which is a string on the way in;
		// the schema's own rules refuse a name it does not have.
		return v1.InputDeclaration_TYPE_STRING
	default:
		// A message, including the dynamic [v1.Value]. Unspecified rather than
		// struct: a `bearer:` given a word is that word, not an object.
		return v1.InputDeclaration_TYPE_UNSPECIFIED
	}
}

// taskInputValues turns what the flags produced into what a step carries, and
// reports what each one looked like written down.
//
// One thing happens here that [collectInputs] cannot do, because it belongs to the
// language rather than to the flags: a value written as a whole-value `${...}` is
// an expression, and a `${secret(...)}` is a reference. Both are what the same
// characters mean in a Flowfile at the same position, which is the property this
// verb exists to have, a working invocation pastes into a step almost
// character-for-character, so the two must read a value the same way.
//
// The second return is what to echo: the value as it was written, never the value
// as it resolved. A secret reference echoes as the reference, which is the whole
// point of it being one.
func taskInputValues(def v1.TaskDef, supplied map[string]*v1.Value) (map[string]*v1.Value, map[string]string, error) {
	if len(supplied) == 0 {
		return nil, nil, nil
	}

	values := make(map[string]*v1.Value, len(supplied))
	written := make(map[string]string, len(supplied))

	// Sorted so a command line with two bad values reports the same one first every
	// time, which is [inputsFromJSON]'s reason applied one level up.
	for _, name := range slices.Sorted(maps.Keys(supplied)) {
		value := supplied[name]

		text, isText := literalString(value)
		if !isText {
			if err := refuseNestedFence(def, name, value.GetLiteral()); err != nil {
				return nil, nil, err
			}
			values[name] = value
			written[name] = renderLiteral(value.GetLiteral())

			continue
		}

		source, fenced := flowfile.SplitFence(text)
		if !fenced {
			values[name] = value
			written[name] = text

			continue
		}
		if err := flowfile.ExprError(text); err != nil {
			return nil, nil, fmt.Errorf("--input %s: %w", name, err)
		}

		reference, isSecret, err := secretReference(source)
		if err != nil {
			return nil, nil, fmt.Errorf("--input %s: %w", name, err)
		}
		if isSecret {
			values[name] = reference
			written[name] = text

			continue
		}

		values[name] = v1.NewExpr(source)
		written[name] = text
	}

	return values, written, nil
}

// literalString reports the text of a value that is a string literal.
func literalString(value *v1.Value) (string, bool) {
	literal, ok := value.GetLiteral().GetKind().(*expr.Value_StringValue)
	if !ok {
		return "", false
	}

	return literal.StringValue, true
}

// refuseNestedFence refuses an expression buried inside a structure rather than
// quietly carrying it as text.
//
// A Flowfile compiles `{code: "${status_code}"}` into one expression that builds
// the mapping, because the compiler walks the structure. Nothing here does, so the
// same JSON on a command line would travel as the literal seven characters
// `${status_code}` and the step would report that string as its answer: a wrong
// result rather than a refusal, which is the failure mode CLAUDE.md's diagnostics
// section calls worse than a missing feature. So it is named, with the spelling
// that does work: one expression over the whole input.
func refuseNestedFence(def v1.TaskDef, name string, literal *expr.Value) error {
	if !literalHoldsFence(literal, 0) {
		return nil
	}

	return fmt.Errorf(
		"--input %s holds a ${...} expression inside a structure, which is compiled by a "+
			"Flowfile and not by a command line; write the whole input as one expression "+
			"instead, e.g. --input %s='${{\"code\": status_code}}', or put the step in a "+
			"file and run it with `flow run local`", name, name)
}

// maxLiteralFenceDepth bounds the walk below.
//
// A value on a command line is input an outside party chooses, and this walk is
// recursive over a structure whose nesting that party decides, so it is bounded by
// the resource the party controls, per CLAUDE.md. The bound is generous against any
// structure a person types and cheap against one meant to exhaust a stack.
const maxLiteralFenceDepth = 32

// literalHoldsFence reports whether any string anywhere inside a literal is written
// as a whole-value expression.
func literalHoldsFence(literal *expr.Value, depth int) bool {
	if depth > maxLiteralFenceDepth {
		// Refused rather than cleared, per fail closed: past this the walk cannot
		// say there is no fence down there, and a check that cannot decide must not
		// allow.
		return true
	}

	switch kind := literal.GetKind().(type) {
	case *expr.Value_StringValue:
		_, fenced := flowfile.SplitFence(kind.StringValue)

		return fenced

	case *expr.Value_ListValue:
		for _, item := range kind.ListValue.GetValues() {
			if literalHoldsFence(item, depth+1) {
				return true
			}
		}

	case *expr.Value_MapValue:
		for _, entry := range kind.MapValue.GetEntries() {
			if literalHoldsFence(entry.GetValue(), depth+1) {
				return true
			}
		}
	}

	return false
}

// secretReference compiles a whole-value `${secret('scheme:name')}` into the
// reference a step carries, and reports whether the expression was one.
//
// Whole-value only, which is narrower than the compiler and deliberately so: a
// reference buried in a longer expression is a placement question the compiler
// answers per task input with a positioned diagnostic, and a command line has no
// position to answer it against. The reference itself is validated by the package
// that resolves them, exactly as `flowfile`'s own compiler does, so a command line
// cannot name a reference a worker would then refuse.
func secretReference(source string) (*v1.Value, bool, error) {
	parsed := v1.NewExpr(source)
	if err := parsed.Error(); err != nil {
		return nil, false, err
	}

	call := parsed.GetExpr().GetExpr().GetCallExpr()
	if call.GetFunction() != flowfile.SecretMarker || call.GetTarget() != nil || len(call.GetArgs()) != 1 {
		return nil, false, nil
	}

	text := call.GetArgs()[0].GetConstExpr().GetStringValue()
	if text == "" {
		return nil, false, fmt.Errorf(
			"secret(...) takes one reference written out in full, as secret('env:API_TOKEN')")
	}

	reference, err := secrets.ParseRef(text)
	if err != nil {
		return nil, false, err
	}

	return &v1.Value{Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{
		Scheme: reference.GetScheme(),
		Name:   reference.GetName(),
	}}}, true, nil
}

// syntheticTaskWorkflow is the one-step specification this invocation is.
//
// A real [v1.Workflow], built here rather than parsed, because there is no file.
// And everything downstream is then the ordinary path: [flowfile.Validate] checks
// it as it checks any specification the server accepts over the API, and
// [v1.RunWithInputs] executes it as it executes a rehearsal.
//
// Profile is set rather than left empty. It is a property of a *run* and it
// travels; empty means "compiled before this field existed", which is a
// compatibility arm and not a thing a specification compiled by this build may
// claim. So this says which vocabulary its expressions were compiled against, the
// same way the Flowfile compiler does.
func syntheticTaskWorkflow(def v1.TaskDef, values map[string]*v1.Value) *v1.Workflow {
	return &v1.Workflow{
		Name:    "task-run-" + taskStepID(def.Name),
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id:   taskStepID(def.Name),
			Kind: &v1.Node_Task{Task: &v1.Task{Name: def.Name, Inputs: values}},
		}},
	}
}

// taskStepID is the task's name as a step id.
//
// The task's own name rather than a fixed word, so the transcript a machine format
// writes is addressed the way the Flowfile this invocation is a rehearsal for would
// address it: `.stepValues.http`. A plugin's dot is not an identifier character, so
// `sql.query` becomes `sql_query`, an id has to be a name CEL can parse, since
// `${steps.<id>}` is how anything reads a step's outputs.
func taskStepID(name string) string {
	id := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
			return r
		default:
			return '_'
		}
	}, name)

	if id == "" || (id[0] >= '0' && id[0] <= '9') {
		id = "task_" + id
	}

	// A name an expression resolves as a root would be shadowed by this step rather
	// than resolving, which [flowfile.Validate] refuses outright. No task may be
	// called any of these today; suffixing keeps that from becoming this command's
	// problem if one ever is.
	if slices.Contains([]string{v1.StepsRoot, v1.VarsRoot, "inputs", "outputs", "run", v1.NowIdentifier}, id) {
		id += "_task"
	}

	return id
}

// checkTaskInputs reports what the schema says is wrong with this invocation,
// before anything runs.
//
// Through [flowfile.Validate] rather than a set of checks written here, because
// every one of them already exists there and is what `flow validate` reports about
// the same task in a file: an input the task does not declare (with the nearest one
// it does), a required input left out, a literal the field cannot hold, an input
// that has to be written as an expression, and the task's own [v1.TaskDef.CheckLiteral]
// refusals. A second implementation would be a second opinion about one contract.
//
// What changes is where the reader is standing. A diagnostic names a step and a
// field, which is right for a file and wrong for a command line, so each one is
// re-addressed to the flag that carries it, the same information, positioned at
// the thing the reader can actually edit.
func checkTaskInputs(def v1.TaskDef, workflow *v1.Workflow) error {
	diagnostics := flowfile.Validate(workflow)
	if len(diagnostics) == 0 {
		return nil
	}

	lines := make([]string, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		if diagnostic.Field != "" {
			lines = append(lines, fmt.Sprintf("--input %s: %s", diagnostic.Field, diagnostic.Message))

			continue
		}
		lines = append(lines, diagnostic.Message)
	}

	if len(lines) == 1 {
		return fmt.Errorf("task %s: %s", def.Name, lines[0])
	}

	return fmt.Errorf("task %s:\n  %s", def.Name, strings.Join(lines, "\n  "))
}

// sensitiveTaskInputs is the set of this invocation's inputs whose value must not
// be echoed in the clear.
//
// Two sources, because there are two ways to know. The schema knows about the
// positions that carry a credential by construction, [v1.TaskDef.AuthorityInputs]
// is the http task's `bearer:` and `credential:`, which are named there precisely
// because a value in one is authority, and nobody should have to remember to say
// so on a command line for the case this verb is most likely to meet first.
// Everything else is the author's call, and in a Flowfile it is `sensitive: true`
// on the input's declaration; there is no file here, so it is a flag.
//
// It governs the echo and nothing else, which is exactly what `sensitive:` governs
// everywhere else in this system: it is display etiquette, not containment, see
// sensitive.go's own header, which this must not be read as extending. The value
// still reaches the task, and a value that must never do that is a
// `${secret(...)}` reference and not this.
func sensitiveTaskInputs(cmd *cobra.Command, def v1.TaskDef) map[string]bool {
	names := make(map[string]bool, len(def.AuthorityInputs))
	for _, name := range def.AuthorityInputs {
		names[name] = true
	}

	marked, _ := cmd.Flags().GetStringArray(sensitiveInputFlagName)
	for _, name := range marked {
		names[strings.TrimSpace(name)] = true
	}

	return names
}

// sensitiveInputFlagName is the command-line spelling of an input declaration's
// `sensitive: true`.
const sensitiveInputFlagName = "sensitive"

// writeTaskInvocation echoes what is about to run, on the account stream.
//
// It is worth printing for the reason this verb exists: the invocation is the thing
// that pastes into a Flowfile step, so seeing the resolved values beside the answer
// is half of what a person came for. It is worth *redacting* for the reason a
// schedule's bound arguments are redacted where they are rendered, a task
// invocation is precisely where somebody first pastes a bearer token, and the
// terminal it lands in is a surface like any other.
//
// To stderr, where every account of a run already goes, so nothing here can reach
// the stream a pipe reads.
func writeTaskInvocation(surface *ui.UI, def v1.TaskDef, written map[string]string, sensitive map[string]bool, reveal bool) {
	if len(written) == 0 {
		return
	}

	fmt.Fprintf(surface.Err, "%s %s\n", surface.ErrTheme.Header.Render("inputs"),
		surface.ErrTheme.Muted.Render("("+def.Name+")"))

	for _, name := range slices.Sorted(maps.Keys(written)) {
		value := written[name]
		if sensitive[name] && !reveal {
			value = redactedMarker(name)
		}

		fmt.Fprintf(surface.Err, "  %s %s\n",
			surface.ErrTheme.Strong.Render(name),
			surface.ErrTheme.Muted.Render(value))
	}
}

// writeTaskOutputs writes the answer.
//
// The two formats differ the way [writeRun] makes them differ, and for the same
// reason. A person has just been told on stderr what ran and how it went, so what
// they are handed is the answer itself: the task's outputs, one per line, in the
// vocabulary the schema gives them. A program was not watching, and gets the
// document the local driver writes for a finished run, the same [v1.GetResponse]
// `flow run local -o json` writes, from [localRun], so one jq expression works
// against a task invocation, a local run and a durable one alike.
//
// That is a deliberate departure from the sketch in #380, which showed `flow task
// run http -o json | jq .json` reading the task's outputs at the top level. A
// document shaped only for this verb would be a third shape for one answer, which
// is the thing the machine formats exist to avoid; when #328's projection lands,
// this changes with the two drivers rather than beside them.
func writeTaskOutputs(surface *ui.UI, rendering runRendering, def v1.TaskDef, response *v1.GetResponse) error {
	if rendering.WantsDocument() {
		return writeRunJSON(surface, rendering, response)
	}

	// One line per output, name and value separated by a tab, and the value in JSON
	// notation, [quotedLiteral] rather than [renderLiteral], which is the one place
	// this deliberately differs from how the same value is summarised on stderr.
	//
	// A response body has newlines in it. Written bare, one output would run over
	// several lines and the next output's name would appear to be part of it, so a
	// reader (and `cut -f2`) could not tell where a value ended. Quoting is what
	// makes "one line per output" a property rather than a hope.
	values := response.GetOutputs().GetStepValues()[taskStepID(def.Name)].GetNamedValues()
	for _, name := range slices.Sorted(maps.Keys(values)) {
		if _, err := fmt.Fprintf(surface.Out, "%s\t%s\n", name, quotedLiteral(values[name].GetLiteral())); err != nil {
			return fmt.Errorf("writing the outputs of task %s: %w", def.Name, err)
		}
	}

	return nil
}
