package main

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/policytest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// `flow policy test` is the test verb a deployment policy lacked.
//
// `flow test` runs workflows and `flow signals check` rehearses a workflow's own
// signal predicates; nothing put cases to the policies an operator writes for a
// worker (#548). This does, for the surfaces whose decision needs no server. The
// decision is the engine's own evaluator for each ([policytest] names them); this
// file is flags, files and the exit status.

// errPolicyTestFailed is the exit status of a suite with a failing case. The
// report has already been written.
var errPolicyTestFailed = errors.New("a policy test case did not get the outcome it expected")

// newPolicyCommand builds `flow policy`, the verbs about deployment policy files.
func newPolicyCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "policy",
		Short: "Test the policy files a deployment is configured with",
		Long: "The verbs about the policy files an operator hands a worker (`--egress-policy`, " +
			"`--task-policy`, `--exec-policy`). They read files and contact nothing.",
	}

	cmd.AddCommand(newPolicyTestCommand())

	return cmd
}

func newPolicyTestCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "test <policy-file> <cases-file>",
		Short: "Put cases to an egress, task-shape or exec policy and report which rule denied",
		Long: "Load a deployment policy exactly as `flow worker` loads it and put each case in a " +
			"cases file to it, asserting that the policy allows or denies. Every case is decided by " +
			"the function the engine enforces that policy with, so a pass says what the worker would " +
			"do, not what a copy of its rules does. Nothing is started and no server is contacted.\n\n" +
			"The cases file is a strict YAML document; a misspelled key is a refusal, because a " +
			"misspelled `expect:` would otherwise assert nothing. It names the policy surface once: " +
			"`egress`, `task` or `exec`.\n\n" +
			"  surface: egress\n" +
			"  cases:\n" +
			"    - name: team-b is refused team-a's partner API\n" +
			"      principal: {namespace: team-b}\n" +
			"      request: {url: https://partner-a.example.com/v1}\n" +
			"      expect: deny\n" +
			"      rule: 'identity.namespace == \"team-a\" && host == \"partner-a.example.com\"'\n\n" +
			"`principal` is the caller the case is made as, the same Principal a run records: " +
			"`subject` and `issuer` (together), `namespace`, `kind` (`human`, `workload` or `agent`), " +
			"`claims` of any shape (a `groups` list included), `actions` and `actors`, which every " +
			"surface's rules read as `identity.<field>`. Absent is no attested caller, and a case " +
			"carries only what it names: a rule on a `kind` or an action the case did not give it " +
			"does not match. Nothing is attested. `request` " +
			"depends on the surface: `url`, `method` (default GET) and optionally `ip` for egress; " +
			"`task` for task shape; `argv`, `dir` and `env` for exec. A request carrying another " +
			"surface's fields is refused.\n\n" +
			"`expect` is `allow` or `deny` and is required. A denial can say which rule denied with " +
			"`rule:`, which is the deny rule's source text exactly as the policy writes it; for a " +
			"denial no deny rule made it is the reason (`allow rules` when no allow rule matched, " +
			"`rule error`, and `scheme`, `port` or `address` for egress; `executable`, `argv`, `dir` or " +
			"`env` for exec). A policy that denies for a different reason fails the case. Every " +
			"denial, expected or not, is reported with the rule or reason that made it.\n\n" +
			"A rule that cannot be evaluated denies, as it does on a worker, and a case that expects " +
			"deny passes on it; name `rule: rule error` to assert that is what happened. A suite " +
			"that expects no denial at all is reported with a warning: it cannot catch a policy that " +
			"allows too much.\n\n" +
			"Egress is decided before DNS: the scheme, port and request rules, and the address checks " +
			"for a host that is an IP literal or a case's `ip`. Rules over the connection's `ip` and " +
			"the control-plane reservation need a dial and are not asked. An exec case is resolved " +
			"against this machine, so an allowed program is allowed on the machine the suite runs on. " +
			"Task-shape rules read the task name and identity only.\n\n" +
			fmt.Sprintf("A cases file is bounded at %d cases and %d KiB. ", policytest.MaxCases, policytest.MaxSuiteBytes/1024) +
			"The exit status is 1 when any case does not get the outcome it expects, and non-zero " +
			"for a policy or cases file that cannot be loaded.",
		Args:          cobra.ExactArgs(2),
		RunE:          runPolicyTest,
		SilenceErrors: true,
		SilenceUsage:  true,
		Example: `# Does the egress policy keep team-b away from team-a's partner API?
flow policy test examples/egress-policy.yaml \
  examples/policy-test/egress-cases.yaml

# The same for a task-shape policy, as a document a job can read:
flow policy test examples/task-shape-policy/task-policy.yaml \
  examples/policy-test/task-cases.yaml -o json`,
	}

	addOutputFlag(cmd)

	return cmd
}

func runPolicyTest(cmd *cobra.Command, args []string) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	policyPath, casesPath := args[0], args[1]

	data, err := readBoundedFile(casesPath, "a policy test cases file", policytest.MaxSuiteBytes)
	if err != nil {
		return fmt.Errorf("reading cases: %w", err)
	}

	suite, err := policytest.ParseSuite(data)
	if err != nil {
		return fmt.Errorf("%s: %w", casesPath, err)
	}

	decider, err := loadPolicyDecider(suite.Surface, policyPath)
	if err != nil {
		return err
	}

	report := policytest.Run(cmd.Context(), suite, decider)

	if format.Machine() {
		err = writeCheckJSON(cmd, format, report)
	} else {
		err = policytest.WriteText(cmd.OutOrStdout(), report)
	}
	if err != nil {
		return err
	}

	if !report.Matches {
		return newQuietError(errPolicyTestFailed)
	}

	return nil
}

// loadPolicyDecider loads the policy file as the surface's own flag does and
// wraps it as the evaluator that surface is enforced with. A file that does not
// load is refused, never tested as though it were an empty policy.
func loadPolicyDecider(surface, path string) (policytest.Decider, error) {
	switch surface {
	case policytest.SurfaceEgress:
		data, policy, err := loadEgressPolicy(path)
		if err != nil {
			return nil, err
		}

		// With a proxy the policy resolves the target host itself before it
		// judges a request, so a verdict would depend on live DNS and not on
		// the suite. Refused rather than answered from a lookup.
		cfg, err := netpolicy.ParseConfig(data)
		if err != nil {
			return nil, fmt.Errorf("parsing egress policy %s: %w", path, err)
		}

		if cfg.Egress.ProxyFromEnvironment {
			return nil, fmt.Errorf("egress policy %s sets proxy_from_environment; a proxied request is judged by resolving its host, which a policy test does not do, so test the policy with the key removed", path)
		}

		return policytest.Egress{Policy: policy}, nil
	case policytest.SurfaceTask:
		policy, err := loadTaskPolicy(path)
		if err != nil {
			return nil, err
		}

		return policytest.Task{Policy: policy}, nil
	case policytest.SurfaceExec:
		policy, err := loadExecPolicy(path)
		if err != nil {
			return nil, err
		}

		return policytest.Exec{Policy: policy}, nil
	}

	return nil, fmt.Errorf("unknown policy surface %q", surface)
}
