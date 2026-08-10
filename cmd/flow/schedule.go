package main

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// `flow schedule` — the act a `triggers:` block does not perform.
//
// A Flowfile may declare the cadence it is meant to run at, and that declaration
// starts nothing. Creating the schedule is this command, typed by a person, and
// that separation is the whole design: a file that begins running on its own the
// moment it merges is a surprise, and a surprise whose first firing looks exactly
// like somebody having meant it. `flow run` therefore does not create schedules,
// and never will.
//
// The verbs are Temporal's, because Temporal's Schedules are what this is a
// surface over: create, list, describe, delete, pause, resume, trigger. `resume`
// rather than `unpause` is the one deliberate departure — the SDK's spelling is
// `Unpause`, and nobody types that.
//
// Output discipline per docs/CLI.md: the answer goes to stdout and every account
// of what happened goes to stderr, so `flow schedule list -o json | jq` is a
// document and not a document with a sentence in it.

// runScheduleCreate arranges for a workflow to run on a cadence.
func runScheduleCreate(cmd *cobra.Command, args []string) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	workflow, err := loadWorkflow(args[0])
	if err != nil {
		return err
	}

	// Refused here as well as at the server, for the message rather than for the
	// control — the standing rule for a client-side check in this CLI. A file with
	// no `triggers:` block is the mistake somebody will actually make, and being
	// told what to write in the file beats being told what the API rejected.
	if workflow.GetTriggers().GetSchedule() == nil {
		return fmt.Errorf("%s declares no schedule, so there is nothing to create: add a `triggers:` "+
			"block with a `schedule:` under it, giving `cron:` or `every:`", args[0])
	}
	if err := v1.CheckScheduleTrigger(workflow.GetTriggers().GetSchedule()); err != nil {
		return err
	}

	// The same arguments a run takes, coerced against the same declarations, through
	// the same code. A schedule is a run somebody arranged in advance, so it would be
	// strange for `--input` to mean something else here — and a second coercion path
	// is how two surfaces of one contract start disagreeing.
	inputs, err := runInputs(cmd, workflow)
	if err != nil {
		return err
	}
	if err := checkRunInputs(workflow, inputs); err != nil {
		return err
	}

	name, _ := cmd.Flags().GetString("name")
	paused, _ := cmd.Flags().GetBool("paused")
	backfills, err := scheduleBackfillFlags(cmd)
	if err != nil {
		return err
	}

	request := &v1.CreateScheduleRequest{
		Workflow: workflow,
		Inputs:   inputs,
		Name:     name,
		Paused:   paused,
		Backfill: backfills,
	}
	if err := v1.Validate(request); err != nil {
		return err
	}

	surface := newSurface(cmd)
	server := serverFlagsOf(cmd)

	response, err := newWorkflowServiceClient(server).CreateSchedule(cmd.Context(), connect.NewRequest(request))
	if err != nil {
		return refusedSchedule("creating", v1.ScheduleNameFor(name, workflow), server, err)
	}

	schedule := response.Msg.GetSchedule()

	if format.Machine() {
		return writeJSON(surface, format, schedule)
	}

	// The next firing times, unasked for, on stdout with the rest of the answer. A
	// cadence meaning something other than what was intended is almost always
	// visible in the first two of these and almost never visible in the expression
	// that produced them, so this is the moment to show them.
	return writeScheduleText(surface, schedule)
}

// scheduleBackfillFlags reads `--backfill START..END`, repeatable.
//
// Bounded here through [v1.CheckScheduleBackfill], which is the same function the
// server applies to the request that arrives. Here for the message, there for the
// control: a bound that only the CLI applies is not a bound, since the RPC is
// public and the caller a bound exists for is the one that is not this program.
// Sharing the function is what keeps the sentence an operator reads and the
// refusal a caller receives from drifting into two different rules.
func scheduleBackfillFlags(cmd *cobra.Command) ([]*v1.ScheduleBackfill, error) {
	values, _ := cmd.Flags().GetStringSlice("backfill")

	out := make([]*v1.ScheduleBackfill, 0, len(values))
	for _, value := range values {
		startText, endText, ok := strings.Cut(value, "..")
		if !ok {
			return nil, fmt.Errorf("backfill %q must be START..END using RFC3339 timestamps, "+
				"as in 2026-08-01T00:00:00Z..2026-08-02T00:00:00Z", value)
		}

		start, err := time.Parse(time.RFC3339, startText)
		if err != nil {
			return nil, fmt.Errorf("backfill start %q: %w", startText, err)
		}

		end, err := time.Parse(time.RFC3339, endText)
		if err != nil {
			return nil, fmt.Errorf("backfill end %q: %w", endText, err)
		}

		out = append(out, &v1.ScheduleBackfill{StartAt: timestamppb.New(start), EndAt: timestamppb.New(end)})
	}

	if err := v1.CheckScheduleBackfill(out); err != nil {
		return nil, err
	}

	return out, nil
}

// runScheduleList reports the schedules belonging to the caller.
func runScheduleList(cmd *cobra.Command, args []string) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	surface := newSurface(cmd)
	server := serverFlagsOf(cmd)

	response, err := newWorkflowServiceClient(server).
		ListSchedules(cmd.Context(), connect.NewRequest(&v1.ListSchedulesRequest{}))
	if err != nil {
		return refusedScheduleList(server, err)
	}

	schedules := response.Msg.GetSchedules()

	switch format {
	case FormatJSON:
		if err := writeJSON(surface, format, response.Msg); err != nil {
			return err
		}
	case FormatJSONL:
		for _, schedule := range schedules {
			if err := writeJSON(surface, format, schedule); err != nil {
				return err
			}
		}
	default:
		if len(schedules) > 0 {
			table := tabwriter.NewWriter(surface.Out, 0, 8, 2, ' ', 0)
			fmt.Fprintln(table, "NAME\tSTATE\tNEXT\tNOTE")
			for _, schedule := range schedules {
				state := "live"
				if schedule.GetPaused() {
					state = "paused"
				}
				fmt.Fprintf(table, "%s\t%s\t%s\t%s\n",
					schedule.GetName(), state,
					formatRunTime(schedule.GetNextRunTime().AsTime(), schedule.GetNextRunTime() != nil),
					schedule.GetNote())
			}
			if err := table.Flush(); err != nil {
				return err
			}
		}

		if len(schedules) == 0 {
			fmt.Fprintln(surface.Err, "no schedules")
		}
	}

	// Said on stderr and only to a person: the machine forms carry `truncated` in
	// the document, and telling a program the same thing in prose it would have to
	// parse is worse than saying nothing.
	if response.Msg.GetTruncated() && format == FormatText {
		fmt.Fprintln(surface.Err,
			"this listing stopped at the server's scan bound, so it is not all of your schedules")
	}

	return nil
}

// runScheduleDescribe reports one schedule in full.
func runScheduleDescribe(cmd *cobra.Command, args []string) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	name := args[0]
	server := serverFlagsOf(cmd)
	surface := newSurface(cmd)

	request := &v1.DescribeScheduleRequest{Name: name}
	if err := v1.Validate(request); err != nil {
		return err
	}

	response, err := newWorkflowServiceClient(server).DescribeSchedule(cmd.Context(), connect.NewRequest(request))
	if err != nil {
		return refusedSchedule("describing", name, server, err)
	}

	if format.Machine() {
		return writeJSON(surface, format, response.Msg.GetSchedule())
	}

	return writeScheduleText(surface, response.Msg.GetSchedule())
}

// runScheduleDelete removes a schedule.
func runScheduleDelete(cmd *cobra.Command, args []string) error {
	name := args[0]

	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	server := serverFlagsOf(cmd)

	request := &v1.DeleteScheduleRequest{Name: name}
	if err := v1.Validate(request); err != nil {
		return err
	}

	if _, err := newWorkflowServiceClient(server).DeleteSchedule(cmd.Context(), connect.NewRequest(request)); err != nil {
		return refusedSchedule("deleting", name, server, err)
	}

	if format.Machine() {
		return writeMutationResult(newSurface(cmd), format, &v1.MutationResult{
			Verb:         "schedule delete",
			ScheduleName: name,
			Result:       resultApplied,
		})
	}

	// Stated because what it does *not* do is the part people get wrong: deleting a
	// schedule stops future firings and touches nothing already running.
	fmt.Fprintf(cmd.ErrOrStderr(),
		"deleted schedule %s; runs it already started keep going, and `flow cancel` is what stops one\n", name)

	return nil
}

// runSchedulePause stops a schedule firing without removing it.
func runSchedulePause(cmd *cobra.Command, args []string) error {
	name := args[0]

	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	note, _ := cmd.Flags().GetString("note")
	server := serverFlagsOf(cmd)

	request := &v1.PauseScheduleRequest{Name: name, Note: note}
	if err := v1.Validate(request); err != nil {
		return err
	}

	if _, err := newWorkflowServiceClient(server).PauseSchedule(cmd.Context(), connect.NewRequest(request)); err != nil {
		return refusedSchedule("pausing", name, server, err)
	}

	// Applied, and deliberately silent about whether it was already paused: the
	// server answers the same for a live schedule and for one paused an hour ago,
	// so a document distinguishing them would be guessing. That is the second fact
	// picatz/flowstate#374 wants these responses to start carrying.
	if format.Machine() {
		return writeMutationResult(newSurface(cmd), format, &v1.MutationResult{
			Verb:         "schedule pause",
			ScheduleName: name,
			Result:       resultApplied,
		})
	}

	fmt.Fprintf(cmd.ErrOrStderr(), "paused schedule %s; `flow schedule resume %s` starts it firing again\n", name, name)

	return nil
}

// runScheduleResume lets a paused schedule fire again.
func runScheduleResume(cmd *cobra.Command, args []string) error {
	name := args[0]

	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	note, _ := cmd.Flags().GetString("note")
	server := serverFlagsOf(cmd)

	request := &v1.ResumeScheduleRequest{Name: name, Note: note}
	if err := v1.Validate(request); err != nil {
		return err
	}

	if _, err := newWorkflowServiceClient(server).ResumeSchedule(cmd.Context(), connect.NewRequest(request)); err != nil {
		return refusedSchedule("resuming", name, server, err)
	}

	if format.Machine() {
		return writeMutationResult(newSurface(cmd), format, &v1.MutationResult{
			Verb:         "schedule resume",
			ScheduleName: name,
			Result:       resultApplied,
		})
	}

	fmt.Fprintf(cmd.ErrOrStderr(), "resumed schedule %s\n", name)

	return nil
}

// runScheduleTrigger fires a schedule now.
func runScheduleTrigger(cmd *cobra.Command, args []string) error {
	name := args[0]

	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	server := serverFlagsOf(cmd)

	request := &v1.TriggerScheduleRequest{Name: name}
	if err := v1.Validate(request); err != nil {
		return err
	}

	if _, err := newWorkflowServiceClient(server).TriggerSchedule(cmd.Context(), connect.NewRequest(request)); err != nil {
		return refusedSchedule("triggering", name, server, err)
	}

	// Requested, and `workflowId` stays empty for the reason the prose below gives:
	// the cluster starts the run after answering, so there is no id to report and
	// inventing one would be worse than the round trip through
	// `flow schedule describe`. The started run id is the field
	// picatz/flowstate#374 most wants TriggerScheduleResponse to carry, since it is
	// what a caller needs for the follow-up `flow watch`.
	if format.Machine() {
		return writeMutationResult(newSurface(cmd), format, &v1.MutationResult{
			Verb:         "schedule trigger",
			ScheduleName: name,
			Result:       resultRequested,
		})
	}

	// No run id, because there is not one yet: the cluster takes the action after
	// answering, so the honest thing is to say where it will appear rather than to
	// wait for it and pretend the wait was the request.
	fmt.Fprintf(cmd.ErrOrStderr(),
		"asked schedule %s to fire; `flow schedule describe %s` lists it under recent runs once it has\n",
		name, name)

	return nil
}

// writeScheduleText renders a schedule for a person.
//
// Everything here is the answer, so all of it goes to stdout. What is deliberately
// absent is the specification: a schedule carries a whole workflow, and printing it
// would bury the four facts somebody is actually asking about.
func writeScheduleText(surface *ui.UI, schedule *v1.ScheduleDescription) error {
	table := tabwriter.NewWriter(surface.Out, 0, 8, 2, ' ', 0)

	fmt.Fprintf(table, "NAME\t%s\n", schedule.GetName())
	if workflow := schedule.GetWorkflowName(); workflow != "" {
		fmt.Fprintf(table, "WORKFLOW\t%s\n", workflow)
	}

	state := "live"
	if schedule.GetPaused() {
		state = "paused"
	}
	fmt.Fprintf(table, "STATE\t%s\n", state)

	if note := schedule.GetNote(); note != "" {
		fmt.Fprintf(table, "NOTE\t%s\n", note)
	}

	if cadence := describeCadence(schedule.GetTrigger()); cadence != "" {
		fmt.Fprintf(table, "CADENCE\t%s\n", cadence)
	}

	fmt.Fprintf(table, "RUNS TAKEN\t%d\n", schedule.GetNumActions())

	for i, at := range schedule.GetNextRunTimes() {
		label := "NEXT"
		if i > 0 {
			label = ""
		}
		fmt.Fprintf(table, "%s\t%s\n", label, at.AsTime().UTC().Format(time.RFC3339))
	}

	for i, run := range schedule.GetRecentRuns() {
		label := "RECENT"
		if i > 0 {
			label = ""
		}
		fmt.Fprintf(table, "%s\t%s  %s\n", label,
			run.GetActualTime().AsTime().UTC().Format(time.RFC3339), run.GetWorkflowId())
	}

	// Sorted, so two describes of one schedule print the same thing: a protobuf map
	// has no order, and a table that reshuffles between reads is one nobody can diff.
	for _, name := range slices.Sorted(maps.Keys(schedule.GetInputs())) {
		fmt.Fprintf(table, "INPUT %s\t%s\n", name, renderOutputValue(schedule.GetInputs()[name]))
	}

	return table.Flush()
}

// describeCadence renders a trigger as one line.
func describeCadence(trigger *v1.ScheduleTrigger) string {
	if trigger == nil {
		return ""
	}

	parts := make([]string, 0, 4)

	if expressions := trigger.GetCron(); len(expressions) > 0 {
		parts = append(parts, strings.Join(expressions, ", "))
	}
	if every := trigger.GetEvery(); every != nil {
		parts = append(parts, "every "+every.AsDuration().String())
	}
	if zone := trigger.GetTimeZone(); zone != "" {
		parts = append(parts, "in "+zone)
	}
	if jitter := trigger.GetJitter(); jitter != nil {
		parts = append(parts, "jittered by up to "+jitter.AsDuration().String())
	}
	if overlap := trigger.GetOverlap(); overlap != v1.ScheduleTrigger_OVERLAP_UNSPECIFIED {
		parts = append(parts, "on overlap "+v1.OverlapName(overlap))
	}
	for _, calendar := range trigger.GetCalendars() {
		parts = append(parts, describeCalendar(calendar))
	}
	if start := trigger.GetStartAt(); start != nil {
		parts = append(parts, "from "+start.AsTime().UTC().Format(time.RFC3339))
	}
	if end := trigger.GetEndAt(); end != nil {
		parts = append(parts, "through "+end.AsTime().UTC().Format(time.RFC3339))
	}
	if window := trigger.GetCatchupWindow(); window != nil {
		parts = append(parts, "catch up within "+window.AsDuration().String())
	}
	if trigger.GetPauseOnFailure() {
		parts = append(parts, "pause on failure")
	}

	return strings.Join(parts, ", ")
}

// describeCalendar renders one calendar in the notation it was written in.
//
// The values rather than a count of them, because the question an operator asks a
// describe is "is this the schedule I meant", and "1 calendar specification(s)"
// answers a different question. Absent fields stay absent rather than being
// rendered as the default they take, so what is printed is what the file said.
func describeCalendar(calendar *v1.ScheduleTrigger_Calendar) string {
	fields := [][]*v1.ScheduleTrigger_Calendar_Range{
		calendar.GetSecond(), calendar.GetMinute(), calendar.GetHour(), calendar.GetDayOfMonth(),
		calendar.GetMonth(), calendar.GetYear(), calendar.GetDayOfWeek(),
	}

	written := make([]string, 0, len(fields))
	for i, name := range v1.ScheduleCalendarFieldNames() {
		if len(fields[i]) == 0 {
			continue
		}

		values := make([]string, 0, len(fields[i]))
		for _, r := range fields[i] {
			values = append(values, describeCalendarRange(r))
		}

		written = append(written, name+" "+strings.Join(values, ","))
	}

	if comment := calendar.GetComment(); comment != "" {
		written = append(written, "("+comment+")")
	}

	return "calendar " + strings.Join(written, " ")
}

// describeCalendarRange renders one range the way a Flowfile writes it.
func describeCalendarRange(r *v1.ScheduleTrigger_Calendar_Range) string {
	text := strconv.Itoa(int(r.GetStart()))
	if r.GetEnd() != 0 {
		text += "-" + strconv.Itoa(int(r.GetEnd()))
	}
	if r.GetStep() != 0 {
		text += "/" + strconv.Itoa(int(r.GetStep()))
	}

	return text
}

// refusedSchedule explains a refused request about one schedule.
//
// The not-found case restates the server's deliberate ambiguity rather than
// resolving it, exactly as [refusedRun] does: the server answers "no such schedule"
// for one that does not exist and for one belonging to another tenant, because
// distinguishing them would confirm the second. That is right for the wire and
// unhelpful on a terminal, so all the causes are named.
func refusedSchedule(verb, name string, server serverFlags, err error) error {
	switch connect.CodeOf(err) {
	case connect.CodeNotFound:
		return fmt.Errorf("no schedule %q is addressable: check the name with `flow schedule list`, "+
			"or it belongs to a tenant your credentials do not establish", name)
	case connect.CodeAlreadyExists:
		return fmt.Errorf("%w", err)
	case connect.CodeUnauthenticated, connect.CodePermissionDenied:
		return fmt.Errorf("refused while %s schedule %q: %w", verb, name, err)
	case connect.CodeUnavailable:
		return unreachableServer(server, "", err)
	default:
		return fmt.Errorf("%s schedule %q: %w", verb, name, err)
	}
}

// refusedScheduleList explains a refused listing, which names no schedule.
func refusedScheduleList(server serverFlags, err error) error {
	switch connect.CodeOf(err) {
	case connect.CodeUnauthenticated, connect.CodePermissionDenied:
		return fmt.Errorf("refused while listing schedules: %w", err)
	case connect.CodeUnavailable:
		return unreachableServer(server, "", err)
	default:
		return fmt.Errorf("listing schedules: %w", err)
	}
}

// newScheduleCommand builds the `flow schedule` verb group.
//
// A group with sub-verbs rather than seven top-level commands, because these are
// all about one kind of object and reading `flow --help` should not be reading a
// list of them. The run verbs are top-level for the opposite reason: a run is the
// thing this tool is about.
func newScheduleCommand() *cobra.Command {
	scheduleCmd := &cobra.Command{
		Use:   "schedule",
		Short: "Create and manage schedules that run workflows on a cadence",
		Long: "Create and manage schedules. A Flowfile declares the cadence it is meant to run " +
			"at in a `triggers:` block, and that declaration does nothing until `flow schedule create` " +
			"is run against it — a file that starts running on its own when it merges is a surprise, " +
			"and `flow run` therefore never creates one.\n\n" +
			"A schedule belongs to your tenant and is named within it, so two teams may both have a " +
			"`nightly-report` without either learning of the other. Firings act as the identity that " +
			"created the schedule, frozen at that moment.",
	}

	createCmd := &cobra.Command{
		Use:   "create [file]",
		Short: "Create a schedule from a Flowfile's triggers block",
		Long: "Create a schedule that runs a Flowfile's workflow on the cadence its `triggers:` " +
			"block declares. The specification, its arguments and the cadence are all checked here, " +
			"while you are present to be told — nothing is left to fail at three in the morning.",
		Args: cobra.ExactArgs(1),
		RunE: runScheduleCreate,
		Example: `# Create the schedule a file declares:
flow schedule create examples/scheduled-report/workflow.yaml

# Create it without letting it fire, look at when it would, then start it:
flow schedule create workflow.yaml --paused
flow schedule describe my-workflow
flow schedule resume my-workflow

# One workflow, two cadences, with the arguments each is for:
flow schedule create report.yaml --name report-eu --input region=eu-west-1
flow schedule create report.yaml --name report-us --input region=us-east-1`,
	}

	addOutputFlag(createCmd)
	addInputFlags(createCmd)
	createCmd.Flags().String("name", "",
		"what to call the schedule; unset takes the workflow's own name, which is what one cadence per workflow wants")
	createCmd.Flags().Bool("paused", false,
		"create the schedule without letting it fire, so its next firing times can be read before it takes one")
	createCmd.Flags().StringSlice("backfill", nil,
		"a missed window to recover at creation, START..END in RFC3339, repeatable up to 10 times and 31 days "+
			"in total. Temporal evaluates the cadence after START and up to END, so write START a moment before "+
			"the first firing you want back")

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List your schedules",
		Long:  "List the schedules belonging to your tenant, with whether each is live and when it next fires.",
		Args:  cobra.NoArgs,
		RunE:  runScheduleList,
		Example: `# List your schedules:
flow schedule list

# Just the names, which every other schedule verb takes:
flow schedule list -o jsonl | jq -r .name

# Which of them are paused?
flow schedule list -o json | jq -r '.schedules[] | select(.paused) | .name'`,
	}

	addOutputFlag(listCmd)

	describeCmd := &cobra.Command{
		Use:   "describe [name]",
		Short: "Show one schedule: its cadence, arguments, next firings and recent runs",
		Long: "Show one schedule in full — the cadence as the file declared it, the arguments every " +
			"firing starts its run with, when it next fires, and what it has run lately.",
		Args: cobra.ExactArgs(1),
		RunE: runScheduleDescribe,
		Example: `# What is this schedule going to do, and what has it done?
flow schedule describe nightly-report

# The run ids it started, which flow get takes:
flow schedule describe nightly-report -o json | jq -r '.recentRuns[].workflowId'`,
	}

	addOutputFlag(describeCmd)

	deleteCmd := &cobra.Command{
		Use:   "delete [name]",
		Short: "Delete a schedule",
		Long: "Delete a schedule. Future firings stop; runs it has already started are ordinary " +
			"workloads and keep going, so stopping one of those is `flow cancel`. Prefer `flow schedule " +
			"pause` when the arrangement should survive whatever is wrong right now." + mutationFlagHelp +
			"\n\n`result` is \"applied\": the schedule is gone when the server answers.",
		Args: cobra.ExactArgs(1),
		RunE: runScheduleDelete,
		Example: `# Delete a schedule:
flow schedule delete nightly-report

# Delete it from a script, which reads the outcome rather than the exit code alone:
flow schedule delete nightly-report -o json | jq -r '.scheduleName, .result'`,
	}

	addOutputFlag(deleteCmd)

	pauseCmd := &cobra.Command{
		Use:   "pause [name]",
		Short: "Stop a schedule firing, without deleting it",
		Long: "Stop a schedule firing while leaving it in place, which is what an incident wants: " +
			"the arrangement is still there and still reviewable, and it is not running." +
			mutationFlagHelp +
			"\n\n`result` is \"applied\" whether or not the schedule was already paused: the " +
			"server answers the same either way, so the document does not guess.",
		Args: cobra.ExactArgs(1),
		RunE: runSchedulePause,
		Example: `# Pause a schedule, saying why:
flow schedule pause nightly-report --note "upstream API is down, INC-4471"

# Pause several from a script and record what was acted on:
flow schedule pause nightly-report --note "INC-4471" -o json | jq -r .scheduleName`,
	}

	addOutputFlag(pauseCmd)

	pauseCmd.Flags().String("note", "",
		"recorded on the schedule and shown by list and describe; a paused schedule found by "+
			"somebody else has no explanation attached unless this is written")

	resumeCmd := &cobra.Command{
		Use:   "resume [name]",
		Short: "Let a paused schedule fire again",
		Long: "Let a paused schedule fire again, from its next scheduled time. Firings missed " +
			"while it was paused are not made up." + mutationFlagHelp +
			"\n\n`result` is \"applied\": the schedule is live when the server answers.",
		Args: cobra.ExactArgs(1),
		RunE: runScheduleResume,
		Example: `flow schedule resume nightly-report --note "upstream recovered"

# Resume from a script, confirming which schedule was acted on:
flow schedule resume nightly-report -o json | jq -r '.scheduleName, .result'`,
	}

	addOutputFlag(resumeCmd)

	resumeCmd.Flags().String("note", "",
		"replaces the message on the schedule, which is usually still the reason it was paused")

	triggerCmd := &cobra.Command{
		Use:   "trigger [name]",
		Short: "Fire a schedule now, without waiting for its cadence",
		Long: "Fire a schedule now. This is what makes a schedule testable: it exercises the " +
			"arguments the schedule stored, the tenant it records on the runs it starts and the queue " +
			"it puts them on, none of which running the workflow by hand would prove. A paused " +
			"schedule fires too, which is what `create --paused`, `trigger`, `resume` is for." +
			mutationFlagHelp +
			"\n\n`result` is \"requested\" and `workflowId` is empty, because the cluster starts " +
			"the run after answering: `flow schedule describe` is what names the run once it " +
			"exists.",
		Args: cobra.ExactArgs(1),
		RunE: runScheduleTrigger,
		Example: `# Fire it now and watch what it started:
flow schedule trigger nightly-report
flow schedule describe nightly-report

# Fire it from a script, then go looking for the run it starts:
flow schedule trigger nightly-report -o json | jq -r .result
flow schedule describe nightly-report -o json | jq -r '.recentRuns[0].workflowId'`,
	}

	addOutputFlag(triggerCmd)

	for _, c := range []*cobra.Command{createCmd, listCmd, describeCmd, deleteCmd, pauseCmd, resumeCmd, triggerCmd} {
		addServerFlags(c)
		scheduleCmd.AddCommand(c)
	}

	return scheduleCmd
}
