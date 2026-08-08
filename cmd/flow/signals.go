package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The flags of `flow signal`, which addresses a durable run rather than a local
// one.
var ()

// withLocalSignals attaches a signal source to a local run, seeded with whatever
// the caller supplied.
//
// A local run is a process, so there is nothing for a person to signal after it
// starts — which is why the answers are given up front. That is enough to make an
// approval gate something an author can exercise while writing it, and exercising
// it locally is the point: a gate whose first real run is in production is a gate
// nobody has tested.
//
// A waiter is attached even when nothing was supplied, so that a workload reaching
// a gate with no answer times out or blocks exactly as it would in production,
// rather than failing with a message about local tooling.
//
// # Enforcing the same policy the server enforces (#207 slice 2)
//
// Every `--signal` delivered through this command goes through
// [v1.SignalPolicyCheck] before it reaches a waiting step, exactly as a durable
// `flow signal` does through `FlowstateServer.Signal` — see
// [v1.NewPolicedLocalSignals].
//
// # Rehearsing who sent it (#349)
//
// A delivery used to carry [v1.LocalSignalSender] always - unattested, which no
// `allow:` rule a real deployment writes can match - so a workflow whose gate
// declares a `signals:` policy could only ever be rehearsed as the case where
// the approval is refused. --signal-as-subject and its siblings name the
// approver a delivery stands in for, and the same [v1.SignalPolicyCheck] then
// admits or refuses it here for the same reason production would. The identity
// is a rehearsal and says so: it is delivered as [v1.RehearsalSignalSender],
// whose `local` marker the durable path refuses outright.
//
// Named for the whole run rather than per --signal, and that is a limit worth
// stating: a workflow with two gates expecting two different approvers can be
// rehearsed one approver at a time. The alternative spelling - a sender bound
// to each --signal - buys that case at the cost of a second value syntax on a
// flag whose first one is already a shell-quoted JSON document, and the flags
// here read the way `--as-subject` already reads for the starter.
//
// --as-subject/--as-issuer/--as-namespace/--as-claim name this run's own
// starter, which is what a `distinct_from_starter:` policy compares a sender
// against - so a rehearsal whose --signal-as-subject equals its --as-subject is
// refused here exactly as production refuses an approver approving their own
// request. A separate starter identity from [WorkloadIdentity]'s zero value
// distinguishes "this local run started as nobody" from "this local run started
// as somebody, but who is unknown" - see [v1.NewPolicedLocalSignals]'s
// hasStarter parameter.
func withLocalSignals(ctx context.Context, cmd *cobra.Command, workflow *v1.Workflow, inputs map[string]*v1.Value, flags []string) (context.Context, error) {
	policies, err := resolvedLocalSignalPolicies(ctx, workflow, inputs)
	if err != nil {
		return nil, err
	}

	starter, err := localWorkloadIdentity(cmd)
	if err != nil {
		return nil, err
	}

	sender, err := rehearsalSignalSender(cmd, len(flags))
	if err != nil {
		return nil, err
	}

	signals := v1.NewPolicedLocalSignals(policies, &v1.WorkloadIdentity{
		Subject:   starter.Subject,
		Issuer:    starter.Issuer,
		Claims:    starter.Claims,
		Namespace: starter.Namespace,
	}, true)

	reportRehearsalSender(cmd.OutOrStdout(), sender)

	for _, flag := range flags {
		name, payload, err := parseSignalFlag(flag)
		if err != nil {
			return nil, err
		}
		if err := signals.DeliverFrom(name, payload, sender); err != nil {
			return nil, refusedLocalSignal(name, sender, err)
		}
	}

	return v1.NewContextWithSignalWaiter(ctx, signals), nil
}

// rehearsalSignalSender reads --signal-as-subject and its siblings into the
// sender every --signal delivery of this run carries.
//
// [v1.LocalSignalSender] when none of them was given, which is the behavior
// every local run had before these flags existed: a delivery that stands in for
// nobody. Naming any of them asks for [v1.RehearsalSignalSender] instead, which
// stands in for the named approver and is still marked local.
//
// delivered is how many --signal flags this run carries, and naming a sender
// for a run that delivers no signal at all is refused rather than ignored:
// there is no reading of it that does anything, and an author who typed it
// meant to answer a gate. Silence there would let a rehearsal that never
// exercised its gate look exactly like one that did - see CLAUDE.md's
// "diagnostics are a feature."
func rehearsalSignalSender(cmd *cobra.Command, delivered int) (*v1.SignalSender, error) {
	subject, _ := cmd.Flags().GetString("signal-as-subject")
	issuer, _ := cmd.Flags().GetString("signal-as-issuer")
	namespace, _ := cmd.Flags().GetString("signal-as-namespace")
	entries, _ := cmd.Flags().GetStringArray("signal-as-claim")

	if subject == "" && issuer == "" && namespace == "" && len(entries) == 0 {
		return v1.LocalSignalSender(), nil
	}

	if delivered == 0 {
		return nil, fmt.Errorf(
			"--signal-as-* names who a signal came from, but this run delivers no --signal for it to " +
				"come from; add the delivery, e.g. --signal deploy-approved='{\"approved\": true}'")
	}

	// A subject and an issuer travel together or not at all, because a rule
	// matches the two joined ([v1.QualifiedSubject]) and never a bare subject:
	// a subject is only unique within its issuer. Refusing half of the pair
	// here reports the mistake as itself, rather than as a gate that mystifies
	// an author by timing out against a policy their subject genuinely matches.
	if (subject == "") != (issuer == "") {
		return nil, fmt.Errorf(
			"--signal-as-subject and --signal-as-issuer are given together or not at all: an `allow:` "+
				"rule matches %q, never a bare subject, because a subject is only unique within its "+
				"issuer", v1.QualifiedSubject("<issuer>", "<subject>"))
	}

	claims := make(map[string]string, len(entries))
	for _, entry := range entries {
		name, value, found := strings.Cut(entry, "=")
		if !found || name == "" || value == "" {
			return nil, fmt.Errorf("invalid --signal-as-claim %q: want NAME=VALUE", entry)
		}
		if _, duplicate := claims[name]; duplicate {
			return nil, fmt.Errorf("duplicate --signal-as-claim %q", name)
		}
		claims[name] = value
	}

	return v1.RehearsalSignalSender(&v1.WorkloadIdentity{
		Subject:   subject,
		Issuer:    issuer,
		Namespace: namespace,
		Claims:    claims,
	}), nil
}

// reportRehearsalSender says whose approval this run is standing in for,
// before the run starts.
//
// A rehearsal identity is not a real one, and the run's own answer already says
// so - `${approval.sender.local}` reads true either way - but the answer is
// read after the fact, by whoever thinks to look. An author who has just
// watched a policed gate open wants to know on the spot that it opened for an
// identity this command asserted rather than one anybody authenticated.
func reportRehearsalSender(out io.Writer, sender *v1.SignalSender) {
	if !v1.IsRehearsalSignalSender(sender) {
		return
	}

	identity := sender.GetIdentity()

	described := v1.QualifiedSubject(identity.GetIssuer(), identity.GetSubject())
	if identity.GetSubject() == "" {
		described = "an approver with no subject"
	}

	fmt.Fprintf(out,
		"rehearsing --signal deliveries as %s; nothing attested this, and the gate's sender.local "+
			"output still reports true\n", described)
}

// refusedLocalSignal explains a delivery this workflow's own `signals:` policy
// refused, in terms of what production would have done with it.
//
// The refusal itself is the point of the feature rather than a failure of it:
// the same [v1.SignalPolicyCheck] runs on both drivers, so a rehearsal refused
// here is an approval that would come back as PermissionDenied in production.
// What differs is when an author finds out, and that is worth saying: durably
// the sender is refused and the run keeps waiting, while here the answers are
// given up front, so there is nothing left to wait for and no reason to hold
// the terminal open until the gate's own timeout to prove it.
func refusedLocalSignal(name string, sender *v1.SignalSender, err error) error {
	if v1.IsRehearsalSignalSender(sender) {
		return fmt.Errorf("%w\n  --signal-as-* asserts who this delivery is from; `flow signal` in "+
			"production would be refused with PermissionDenied for the same reason, and the run would "+
			"go on waiting", err)
	}

	return fmt.Errorf("%w\n  this delivery attests nobody, which no `allow:` rule matches; "+
		"--signal-as-subject and --signal-as-issuer name the approver %q stands in for", err, name)
}

// resolvedLocalSignalPolicies is workflow's declared `signals:` — if any —
// resolved against inputs the same way submit resolves them
// ([v1.ResolveSignalPolicySubjects]), suitable for [v1.NewPolicedLocalSignals].
//
// inputs is bound first ([v1.BindRunInputs]), matching what
// [v1.ResolveSignalPolicySubjects] itself expects: a rule's `subject_from`
// expression may read a defaulted input, not only one the caller typed. A bind
// failure here is not reported directly — [v1.RunWithInputs] performs the
// identical bind moments later and is what actually decides whether this run
// proceeds; this function only needs bound inputs when there is a policy to
// resolve; when binding fails, the run is about to fail anyway, so an empty,
// unpoliced result is returned rather than a second, differently-shaped error.
func resolvedLocalSignalPolicies(ctx context.Context, workflow *v1.Workflow, inputs map[string]*v1.Value) (map[string]*v1.SignalPolicy, error) {
	if len(workflow.GetSignals()) == 0 {
		return nil, nil
	}

	bound, err := v1.BindRunInputs(workflow, inputs)
	if err != nil {
		return nil, nil
	}

	return v1.ResolveSignalPolicySubjects(ctx, workflow, bound)
}

// reportUnansweredGates warns about gates this run will block on.
//
// A local run with no answer for a gate is not wrong — it waits, exactly as
// production would, until the gate's own timeout. But it looks like a hang, and an
// author watching a terminal do nothing for a day will conclude the feature is
// broken rather than that they forgot a flag. So the run says what it is waiting
// for and what would release it, before it starts waiting.
func reportUnansweredGates(out io.Writer, workflow *v1.Workflow, flags []string) {
	answered := make(map[string]bool, len(flags))
	for _, flag := range flags {
		if name, _, found := strings.Cut(flag, "="); found {
			answered[strings.TrimSpace(name)] = true
		}
	}

	for _, name := range v1.SignalNames(workflow) {
		if answered[name] {
			continue
		}
		fmt.Fprintf(out,
			"this workload waits for signal %q, which nothing here will send; it will block until that wait times out\n"+
				"  to answer it now:  --signal %s='{\"approved\": true}'\n",
			name, name)
	}
}

// parseSignalFlag reads one --signal name=json flag.
//
// The payload becomes the waiting step's outputs, so the JSON keys are what a later
// step reads as ${approval.approved}. Reporting a malformed one names the flag and
// what was wrong with it, because a quoting mistake in a shell is the most likely
// way to get here.
func parseSignalFlag(flag string) (string, *v1.Node_Outputs, error) {
	name, raw, found := strings.Cut(flag, "=")
	if !found {
		return "", nil, fmt.Errorf(
			"--signal %q needs a name and a payload, as name=json, e.g. --signal deploy-approved='{\"approved\": true}'", flag)
	}

	name = strings.TrimSpace(name)
	if name == "" {
		return "", nil, fmt.Errorf("--signal %q names no signal", flag)
	}

	payload, err := parseSignalPayload("--signal "+name, raw)
	if err != nil {
		return "", nil, err
	}

	return name, payload, nil
}

// parseSignalPayload turns a JSON object into a waiting step's outputs.
//
// Shared by the local flag and by `flow signal`, so that a payload means exactly
// the same thing whichever driver receives it — a rehearsal that reads its
// answer differently from production is a rehearsal of the wrong thing.
//
// The source names where the payload came from, because the keys here become what
// a later step reads as ${approval.approved}: a quoting mistake is otherwise
// indistinguishable from a workflow bug.
func parseSignalPayload(source, raw string) (*v1.Node_Outputs, error) {
	// An empty payload is a signal that carries nothing, which is a reasonable
	// thing to send: the wait still completes and still reports timed_out false.
	if strings.TrimSpace(raw) == "" {
		return &v1.Node_Outputs{NamedValues: map[string]*v1.Value{}}, nil
	}

	var fields map[string]any
	if err := json.Unmarshal([]byte(raw), &fields); err != nil {
		return nil, fmt.Errorf("%s: payload is not a JSON object: %w", source, err)
	}

	outputs := &v1.Node_Outputs{NamedValues: make(map[string]*v1.Value, len(fields))}
	for key, value := range fields {
		outputs.NamedValues[key] = v1.NewValue(value)
	}

	return outputs, nil
}

// runSignal delivers a signal to a run waiting for one.
//
// This is the other half of --signal on a local run: the same payload with the
// same meaning, addressed to a workload already waiting somewhere rather than to
// a process about to start.
//
// A sender names the workload, not a run. The run may have been waiting for a
// week and may have been continued as new several times since it started, and
// neither is something an approver knows or should have to: they are approving
// the deploy, not one attempt at it. --run-id is there for the case where
// somebody genuinely means one attempt.
func runSignal(cmd *cobra.Command, args []string) error {
	workflowID, name := args[0], args[1]

	server := serverFlagsOf(cmd)
	data, _ := cmd.Flags().GetString("data")
	runID, _ := cmd.Flags().GetString("run-id")

	payload, err := parseSignalPayload("--data", data)
	if err != nil {
		return err
	}

	// A signal that carries nothing travels as an *absent* payload rather than an
	// empty one, because Node.Outputs.named_values is required: an empty map is not
	// something the schema lets a message say, and sending one is refused before it
	// leaves. The server turns absent back into empty outputs, which is what keeps
	// ${approval.timed_out} resolving on a gate somebody answered with nothing to
	// add.
	if len(payload.GetNamedValues()) == 0 {
		payload = nil
	}

	request := &v1.SignalRequest{
		WorkflowId: workflowID,
		RunId:      runID,
		Name:       name,
		Payload:    payload,
	}

	// The schema's own rules, read off the descriptor rather than restated here,
	// so this cannot drift from what the server enforces. It runs before the round
	// trip because a mistyped signal name is worth reporting on the spot rather
	// than as a remote invalid-argument. The server validates again regardless: a
	// client-side check is a convenience, never a control.
	if err := v1.Validate(request); err != nil {
		// The rule is the schema's, but a pattern is not a thing to hand somebody
		// as advice, so the hint says what the rule is for rather than restating it.
		return fmt.Errorf("%w\n  a signal name is the one its wait_for_signal step declares: "+
			"a letter or digit, then letters, digits, - or _", err)
	}

	if _, err := newWorkflowServiceClient(server).Signal(cmd.Context(), connect.NewRequest(request)); err != nil {
		return refusedRun("signalling", workflowID, server, err)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "delivered %s to %s\n", name, workflowID)
	return nil
}
