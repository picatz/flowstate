package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/policycheck"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// `flow signals check` is the static form of "who may act".
//
// Policy used to be testable one way: run the workflow as one identity and see
// whether its gate opened. This asks the same decisions - the ones the server
// and `flow run local` make - for any number of identities, without running a
// step. The decision logic is [policycheck]'s, which calls the engine's own
// check functions; this file is flags, files and rendering.
//
// It is `signals` rather than another flag on `flow signal` because `signal`
// delivers to a durable run and takes a server; this takes a file and contacts
// nothing. The two share the vocabulary an author already types: --signal-as-*
// names the sender exactly as `flow run local` does, and --input reads as it
// does there.

// errPolicyCheckMismatch is the exit status of a check whose answers contradicted
// what was expected. The answers have already been printed.
var errPolicyCheckMismatch = errors.New("the answers did not match what was expected")

// newSignalsCommand builds `flow signals`, the verbs about a workflow's
// authorization gates that need no run.
func newSignalsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "signals",
		Short: "Check who may act on a workflow, without running it",
		Long: "The verbs about a workflow's authorization gates that read a Flowfile and run nothing. " +
			"`flow signal` (singular) delivers to a run that is already waiting.",
	}

	cmd.AddCommand(newSignalsCheckCommand())

	return cmd
}

func newSignalsCheckCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "check <workflow-file>",
		Short: "Ask each authorization gate whether an identity may act",
		Long: "Compile a Flowfile and ask its authorization gates whether an identity would be " +
			"admitted, executing no step and contacting no server. Each gate is decided by the " +
			"function the engine decides it with: `signals:` by the check the server applies to a " +
			"delivery, `debug:` by the check a debug lease is granted with, and `triggers.manual` by " +
			"the check a manual start is held to. A refusal is the engine's own sentence, which never " +
			"quotes a claim or an input.\n\n" +
			"With none of `--signal`, `--debug` and `--manual`, every declared signal is checked; " +
			"naming any of them checks only what is named. One line is written per gate, `admitted` " +
			"or `refused`. A signal no `signals:` policy governs is admitted for any sender, and the " +
			"line says so, so it is not mistaken for a gate that was passed.\n\n" +
			"The sender is named as `flow run local` names the approver a `--signal` stands in for: " +
			"`--signal-as-subject` with `--signal-as-issuer` (given together or not at all), " +
			"`--signal-as-namespace` and `--signal-as-claim`. Name none and the sender is " +
			"unauthenticated, which no `allow:` predicate a deployment writes admits, and which " +
			"`triggers.manual` refuses outright. `--starter-*` names who started the run, which a " +
			"predicate reads as `run.identity`; name none and the starter is unknown, which refuses " +
			"any predicate that reads `run.identity`, as the engine does for a run with no recorded " +
			"starter. `--starter-anonymous` says the run was started by nobody authenticated, which is " +
			"how `flow run local` models a run started with no `--as-*` flags.\n\n" +
			"Arguments are given as `flow run` takes them and are bound against the workflow's " +
			"`inputs:` as a start binds them, so a predicate reads defaults too. Nothing prints an " +
			"input's value.\n\n" +
			"`--expect admitted|refused` makes the answer an assertion: the exit status is 1 when " +
			"any decision differs, which is what makes this usable in CI. Without it the exit status " +
			"is 0 whatever the answers, and non-zero only for a usage or compile error.\n\n" +
			"`--matrix FILE` asks the same gates about many identities at once and prints a " +
			"senders-by-gates table. The file is a strict YAML document:\n\n" +
			"  identities:\n" +
			"    - name: sre-lead\n" +
			"      subject: sre-lead@example.com\n" +
			"      issuer: https://issuer.example.com\n" +
			"      claims: {team: release-managers}\n" +
			"      starter: {subject: dev@example.com, issuer: https://issuer.example.com}\n" +
			"      inputs: {expected_approver: sre-lead@example.com}\n" +
			"      expect: admitted\n\n" +
			"`expect` is one outcome for every gate, or a map from gate (`signals.NAME`, `debug`, " +
			"`triggers.manual`) to its outcome. A row's `inputs` replace, by name, the `--input` " +
			"arguments given for every row; a row with no `starter` takes the `--starter-*` flags, " +
			"and one with no `expect` takes `--expect`. A mismatching row makes the exit status 1. " +
			fmt.Sprintf("A matrix is bounded at %d identities and %d KiB.",
				policycheck.MaxMatrixRows, policycheck.MaxMatrixBytes/1024) +
			"\n\nNothing here runs the workflow: a decision that depends on state only a run has " +
			"(a retry, a signal already consumed) is not modelled, and `triggers.manual` is decided " +
			"over the caller and the inputs alone, as the server decides it, with no `run`.",
		Args:          cobra.ExactArgs(1),
		RunE:          runSignalsCheck,
		SilenceErrors: true,
		SilenceUsage:  true,
		Example: `# Who may answer the approval gate? Ask as the approver it names:
flow signals check examples/approval-gate/workflow.yaml \
  --input-file examples/approval-gate/inputs.json \
  --starter-subject dev@example.com \
  --starter-issuer https://issuer.example.com \
  --signal-as-subject sre-lead@example.com \
  --signal-as-issuer https://issuer.example.com \
  --signal-as-claim team=release-managers

# Assert, in CI, that the requester cannot approve their own deploy:
flow signals check examples/approval-gate/workflow.yaml \
  --input-file examples/approval-gate/inputs.json --expect refused \
  --starter-subject sre-lead@example.com \
  --starter-issuer https://issuer.example.com \
  --signal-as-subject sre-lead@example.com \
  --signal-as-issuer https://issuer.example.com \
  --signal-as-claim team=release-managers

# Every gate against a table of identities, with expectations:
flow signals check examples/approval-gate/workflow.yaml \
  --debug --matrix who.yaml \
  --input-file examples/approval-gate/inputs.json`,
	}

	addOutputFlag(cmd)
	addInputFlags(cmd)

	cmd.Flags().StringArray("signal", nil,
		"check this declared signal (repeatable); by default every declared signal is checked")
	cmd.Flags().Bool("debug", false, "check who may hold a debug lease, decided by the `debug:` stanza")
	cmd.Flags().Bool("manual", false, "check who may start the workflow by hand, decided by `triggers.manual`")

	cmd.Flags().String("signal-as-subject", "",
		"authenticated subject attempting the act, with `--signal-as-issuer`")
	cmd.Flags().String("signal-as-issuer", "",
		"authenticated issuer attempting the act, with `--signal-as-subject`")
	cmd.Flags().String("signal-as-namespace", "",
		"tenant namespace of the identity attempting the act")
	cmd.Flags().StringArray("signal-as-claim", nil,
		"authenticated string claim NAME=VALUE of the identity attempting the act (repeatable)")

	cmd.Flags().String("starter-subject", "",
		"subject that started the run, read as `run.identity`, with `--starter-issuer`")
	cmd.Flags().String("starter-issuer", "",
		"issuer of the subject that started the run, with `--starter-subject`")
	cmd.Flags().String("starter-namespace", "",
		"tenant namespace of whoever started the run")
	cmd.Flags().StringArray("starter-claim", nil,
		"authenticated string claim NAME=VALUE of whoever started the run (repeatable)")
	cmd.Flags().Bool("starter-anonymous", false,
		"the run was started by nobody authenticated, rather than by an unknown starter")

	cmd.Flags().String("reason", "",
		"the reason a manual start would carry, for a `manual:` block that requires one")
	cmd.Flags().String("expect", "",
		"assert every decision is `admitted` or `refused`; exit 1 when one differs")
	cmd.Flags().String("matrix", "",
		"a YAML file of named identities to check as a senders-by-gates table")

	return cmd
}

// runSignalsCheck compiles the workflow, resolves the gates and subjects, asks
// the engine, and renders the answers.
func runSignalsCheck(cmd *cobra.Command, args []string) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	workflow, err := loadWorkflow(args[0])
	if err != nil {
		return err
	}

	signals, _ := cmd.Flags().GetStringArray("signal")
	debug, _ := cmd.Flags().GetBool("debug")
	manual, _ := cmd.Flags().GetBool("manual")

	gates, err := policycheck.Gates(workflow, signals, debug, manual)
	if err != nil {
		return newUsageError(err)
	}

	expect, err := expectFlag(cmd)
	if err != nil {
		return newUsageError(err)
	}

	base, err := checkSubjectFlags(cmd)
	if err != nil {
		return newUsageError(err)
	}

	inputs, err := runInputs(cmd, workflow)
	if err != nil {
		return refusedCheckInputs(cmd, workflow, nil, err)
	}
	base.Inputs = inputs

	var results []policycheck.Result

	matrixPath, _ := cmd.Flags().GetString("matrix")

	if matrixPath != "" {
		results, err = matrixResults(cmd, workflow, gates, matrixPath, base, expect)
	} else {
		var decisions []policycheck.Decision
		if decisions, err = policycheck.Evaluate(cmd.Context(), workflow, gates, base); err == nil {
			results = []policycheck.Result{{Decisions: decisions, Expect: policycheck.Expectation{All: expect}}}
		} else {
			err = refusedCheckInputs(cmd, workflow, inputs, err)
		}
	}
	if err != nil {
		return err
	}

	report := policycheck.NewReport(gates, results)

	if format.Machine() {
		if err := writeCheckJSON(cmd, format, report); err != nil {
			return err
		}
	} else {
		out := cmd.OutOrStdout()

		if matrixPath != "" {
			err = policycheck.WriteTable(out, gates, results)
		} else {
			err = policycheck.WriteLines(out, results[0])
		}
		if err != nil {
			return err
		}
	}

	if !report.Matches {
		return newQuietError(errPolicyCheckMismatch)
	}

	return nil
}

// expectFlag reads --expect, or "" for none.
func expectFlag(cmd *cobra.Command) (policycheck.Outcome, error) {
	word, _ := cmd.Flags().GetString("expect")
	if word == "" {
		return "", nil
	}

	outcome, err := policycheck.ParseOutcome(word)
	if err != nil {
		return "", fmt.Errorf("--expect: %w", err)
	}

	return outcome, nil
}

// checkSubjectFlags reads the sender and starter flags into the subject every
// decision of this command is asked about (or, with --matrix, the defaults a row
// that gives none of its own takes).
//
// The sender flags are the spelling `flow run local` uses ([rehearsalSignalSender]),
// and the half-pair rule is the one a test file's identities load with
// ([flowtest.ScriptedIdentity.Check]) rather than a second copy of it.
func checkSubjectFlags(cmd *cobra.Command) (policycheck.Subject, error) {
	var subject policycheck.Subject

	matrix, _ := cmd.Flags().GetString("matrix")

	sender, err := identityFromFlags(cmd, "signal-as", "--signal-as-subject and --signal-as-issuer")
	if err != nil {
		return subject, err
	}
	if sender != nil && matrix != "" {
		return subject, errors.New("--matrix names its own senders; drop the --signal-as-* flags or the matrix")
	}
	subject.Sender = sender

	anonymous, _ := cmd.Flags().GetBool("starter-anonymous")

	starter, err := identityFromFlags(cmd, "starter", "--starter-subject and --starter-issuer")
	if err != nil {
		return subject, err
	}

	switch {
	case anonymous && starter != nil:
		return subject, errors.New("--starter-anonymous says nobody started the run, which --starter-* contradicts")
	case anonymous:
		starter = &flowtest.ScriptedIdentity{}
	}
	subject.Starter = starter

	subject.Reason, _ = cmd.Flags().GetString("reason")

	return subject, nil
}

// identityFromFlags reads `--<prefix>-subject`, `-issuer`, `-namespace` and
// `-claim` into an identity, or nil when none of them was given.
func identityFromFlags(cmd *cobra.Command, prefix, pair string) (*flowtest.ScriptedIdentity, error) {
	subject, _ := cmd.Flags().GetString(prefix + "-subject")
	issuer, _ := cmd.Flags().GetString(prefix + "-issuer")
	namespace, _ := cmd.Flags().GetString(prefix + "-namespace")
	entries, _ := cmd.Flags().GetStringArray(prefix + "-claim")

	if subject == "" && issuer == "" && namespace == "" && len(entries) == 0 {
		return nil, nil
	}

	claims, err := parseIdentityClaimFlags(prefix+"-claim", entries)
	if err != nil {
		return nil, err
	}

	identity := &flowtest.ScriptedIdentity{Subject: subject, Issuer: issuer, Namespace: namespace, Claims: claims}

	if err := identity.Check(pair + " are given together or not at all:"); err != nil {
		return nil, err
	}

	return identity, nil
}

// matrixResults decides every row of a matrix file.
func matrixResults(cmd *cobra.Command, workflow *v1.Workflow, gates []policycheck.Gate, path string, base policycheck.Subject, expect policycheck.Outcome) ([]policycheck.Result, error) {
	data, err := readBoundedFile(path, "an identity matrix", policycheck.MaxMatrixBytes)
	if err != nil {
		return nil, fmt.Errorf("reading --matrix: %w", err)
	}

	matrix, err := policycheck.ParseMatrix(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	if err := matrix.CheckGates(gates); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	declared := declaredInputs(workflow)

	results := make([]policycheck.Result, 0, len(matrix.Identities))

	for _, row := range matrix.Identities {
		subject := base
		subject.Sender = &row.ScriptedIdentity

		// An identity with nothing in it is no identity: an unauthenticated
		// caller, not an authenticated one with empty fields.
		if row.Subject == "" && row.Issuer == "" && row.Namespace == "" && len(row.Claims) == 0 {
			subject.Sender = nil
		}

		if row.Starter != nil {
			subject.Starter = row.Starter
		}

		if len(row.Inputs) > 0 {
			own, err := rowInputs(row, declared)
			if err != nil {
				return nil, refusedCheckInputs(cmd, workflow, base.Inputs, err)
			}

			subject.Inputs = maps.Clone(base.Inputs)
			if subject.Inputs == nil {
				subject.Inputs = map[string]*v1.Value{}
			}
			maps.Copy(subject.Inputs, own)
		}

		decisions, err := policycheck.Evaluate(cmd.Context(), workflow, gates, subject)
		if err != nil {
			return nil, refusedCheckInputs(cmd, workflow, subject.Inputs, fmt.Errorf("identity %q: %w", row.Name, err))
		}

		want := row.Expect
		if want.All == "" && len(want.ByGate) == 0 {
			want.All = expect
		}

		results = append(results, policycheck.Result{Name: row.Name, Decisions: decisions, Expect: want})
	}

	return results, nil
}

// rowInputs reads a row's arguments the way `--input-file` reads a document: a
// JSON object, each value read against its declaration by the one reader the
// other surfaces share ([inputsFromDecoded]).
func rowInputs(row policycheck.Row, declared map[string]*v1.InputDeclaration) (map[string]*v1.Value, error) {
	encoded, err := json.Marshal(row.Inputs)
	if err != nil {
		return nil, fmt.Errorf("identity %q: inputs are not plain data: %w", row.Name, err)
	}

	fields, err := decodeInputJSONObject(encoded)
	if err != nil {
		return nil, fmt.Errorf("identity %q: inputs: %w", row.Name, err)
	}

	return inputsFromDecoded(fmt.Sprintf("identity %q", row.Name), fields, declared)
}

// refusedCheckInputs reports an argument problem with every sensitive value the
// refusal could quote removed, through the seam every other run command uses
// ([refusedRunSensitiveValues]). It never reveals: this command has no
// --reveal-sensitive, because it prints no value to reveal.
func refusedCheckInputs(cmd *cobra.Command, workflow *v1.Workflow, submitted map[string]*v1.Value, err error) error {
	return redactFailureError(err, refusedRunSensitiveValues(cmd, workflow, submitted, err, false))
}

// writeCheckJSON writes the report in the format a job reads.
func writeCheckJSON(cmd *cobra.Command, format OutputFormat, report policycheck.Report) error {
	var (
		encoded []byte
		err     error
	)
	if format == FormatJSON {
		encoded, err = json.MarshalIndent(report, "", "  ")
	} else {
		encoded, err = json.Marshal(report)
	}
	if err != nil {
		return fmt.Errorf("rendering the answers as %s: %w", format, err)
	}

	_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s\n", encoded)

	return err
}

// parseIdentityClaimFlags reads repeated NAME=VALUE claim flags, refusing a malformed or
// repeated one. Shared by every command that names an identity with a
// `--<prefix>-claim` flag, so the spelling and the refusals are one.
func parseIdentityClaimFlags(flag string, entries []string) (map[string]string, error) {
	claims := make(map[string]string, len(entries))

	for _, entry := range entries {
		name, value, found := strings.Cut(entry, "=")
		if !found || name == "" || value == "" {
			return nil, fmt.Errorf("invalid --%s %q: want NAME=VALUE", flag, entry)
		}
		if _, duplicate := claims[name]; duplicate {
			return nil, fmt.Errorf("duplicate --%s %q", flag, name)
		}
		claims[name] = value
	}

	return claims, nil
}
