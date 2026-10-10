package policytest

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/execpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// Decision is what a policy answered for one case.
type Decision struct {
	Allowed bool

	// Reason is the category of the denial, in the evaluator's own words:
	// `deny rule`, `allow rules`, `rule error`, `address`, `executable`.
	// Empty when allowed.
	Reason string

	// Rule is what a case's `rule:` is compared with: the source text of the
	// deny rule that matched, and for every other denial the reason.
	Rule string

	// Detail is the evaluator's own account of the denial.
	Detail string
}

// Decider asks one policy surface's evaluator. An error is a decision that was
// not reached, never a denial.
type Decider interface {
	Decide(ctx context.Context, c Case) (Decision, error)
}

// denied builds a denial. A deny rule is named by its source, which is what an
// author can write back into a case; any other denial by its category, because
// there is no rule to name.
func denied(reason, detail string, denyRule bool) Decision {
	rule := reason
	if denyRule {
		rule = detail
	}

	return Decision{Reason: reason, Rule: rule, Detail: detail}
}

// unclassified is a denial for an evaluator error that is not one of its own
// refusals. The engine denies on an error it cannot type, so a test does too:
// reading it as an allow, or dropping the case, would answer more kindly than
// the engine does.
func unclassified(err error) Decision {
	return denied("error", err.Error(), false)
}

// Egress decides cases with the evaluator the http task is held to.
type Egress struct{ Policy *netpolicy.Policy }

// Decide asks [netpolicy.Policy.CheckURL], and then, when the case has an
// address, [netpolicy.Policy.CheckAddr]. The first refusal is the answer, in the
// order a request meets them. Connection-scoped `ip` rules and the control-plane
// reservation are not asked: they need a dial, which is the engine's.
func (e Egress) Decide(ctx context.Context, c Case) (Decision, error) {
	ctx = netpolicy.ContextWithIdentity(ctx, c.subject.egress)
	ctx = netpolicy.ContextWithCredentials(ctx, c.req.GetCredentials())

	err := e.Policy.CheckURL(ctx, methodOf(c), c.url)
	if err == nil && c.addr.IsValid() {
		err = e.Policy.CheckAddr(c.addr)
	}

	if err == nil {
		return Decision{Allowed: true}, nil
	}

	if undecided, ok := errors.AsType[*netpolicy.UndecidedError](err); ok {
		return Decision{}, undecided
	}

	if deny, ok := errors.AsType[*netpolicy.DenyError](err); ok {
		return denied(string(deny.Reason), deny.Detail, deny.Reason == netpolicy.ReasonDenyRule), nil
	}

	return unclassified(err), nil
}

// methodOf is the method the http task would send: the case's spelling
// verbatim, because the task passes it unchanged and egress rules read it
// unchanged, so `method: get` is judged as `get`. Only an absent method is GET.
func methodOf(c Case) string {
	return cmp.Or(c.req.GetMethod(), http.MethodGet)
}

// Task decides cases with the check every dispatch is held to.
type Task struct{ Policy *v1.TaskPolicy }

// Decide asks [v1.TaskPolicy.Check].
func (t Task) Decide(ctx context.Context, c Case) (Decision, error) {
	err := t.Policy.Check(ctx, c.req.GetTask(), c.subject.task)
	if err == nil {
		return Decision{Allowed: true}, nil
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return Decision{}, ctxErr
	}

	if deny, ok := errors.AsType[*v1.TaskPolicyDeniedError](err); ok {
		return denied(string(deny.Reason), deny.Detail, deny.Reason == v1.TaskPolicyReasonDenyRule), nil
	}

	return unclassified(err), nil
}

// Exec decides cases with the check the exec task is held to.
type Exec struct{ Policy *execpolicy.Policy }

// Decide asks [execpolicy.Policy.Check], which resolves the program, the
// directory and the environment against this machine and starts nothing. An
// allowed case is allowed on the machine the suite runs on.
func (e Exec) Decide(ctx context.Context, c Case) (Decision, error) {
	_, err := e.Policy.Check(ctx, execpolicy.Request{
		Argv:     c.req.GetArgv(),
		Dir:      c.req.GetDir(),
		Env:      c.req.GetEnv(),
		Identity: c.subject.egress,
	})
	if err == nil {
		return Decision{Allowed: true}, nil
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return Decision{}, ctxErr
	}

	if deny, ok := errors.AsType[*execpolicy.DeniedError](err); ok {
		return denied(string(deny.Reason), deny.Detail, deny.Reason == execpolicy.ReasonDenyRule), nil
	}

	return unclassified(err), nil
}

// Result is the outcome of one case.
type Result struct {
	Name   string `json:"name"`
	Expect string `json:"expect"`
	Got    string `json:"got,omitempty"`
	Passed bool   `json:"passed"`

	// Reason, Rule and Detail say which rule denied, for every denial, whether
	// or not the case expected it.
	Reason string `json:"reason,omitempty"`
	Rule   string `json:"rule,omitempty"`
	Detail string `json:"detail,omitempty"`

	// Failure says why the case did not pass.
	Failure string `json:"failure,omitempty"`
}

// Report is the outcome of a suite.
type Report struct {
	Surface string   `json:"surface"`
	Total   int      `json:"total"`
	Passed  int      `json:"passed"`
	Failed  int      `json:"failed"`
	Denials int      `json:"deny_cases"`
	Named   int      `json:"deny_cases_naming_rule"`
	Matches bool     `json:"matches"`
	Cases   []Result `json:"cases"`

	// Warnings are things true of the suite that a green result hides.
	Warnings []string `json:"warnings,omitempty"`
}

// Run puts every case to the decider and judges the answers. A case whose
// decision was not reached fails and says so; the rest are still asked.
func Run(ctx context.Context, suite *Suite, decider Decider) Report {
	report := Report{Surface: suite.Surface, Total: len(suite.Cases), Cases: make([]Result, 0, len(suite.Cases))}

	for _, c := range suite.Cases {
		result := judge(ctx, c, decider)

		if c.Expect == Deny {
			report.Denials++
			if c.Rule != "" {
				report.Named++
			}
		}

		if result.Passed {
			report.Passed++
		} else {
			report.Failed++
		}

		report.Cases = append(report.Cases, result)
	}

	report.Matches = report.Failed == 0

	if report.Denials == 0 {
		report.Warnings = append(report.Warnings,
			"no case expects deny: this suite cannot catch a policy that allows too much; "+
				"add a case that must be refused")
	}

	return report
}

func judge(ctx context.Context, c Case, decider Decider) Result {
	result := Result{Name: c.Name, Expect: c.Expect}

	decision, err := decider.Decide(ctx, c)
	if err != nil {
		result.Failure = "not decided: " + err.Error()
		return result
	}

	if decision.Allowed {
		result.Got = Allow
	} else {
		result.Got = Deny
		result.Reason, result.Rule, result.Detail = decision.Reason, decision.Rule, decision.Detail
	}

	switch {
	case result.Got != c.Expect:
		result.Failure = fmt.Sprintf("expected %s, got %s", c.Expect, result.Got)
	case c.Rule != "" && c.Rule != decision.Rule:
		result.Failure = fmt.Sprintf("expected the denial to name %q, got %q", c.Rule, decision.Rule)
	default:
		result.Passed = true
	}

	return result
}

// WriteText renders a report for a person: one line per case, the rule that
// denied under every denial, and a summary.
func WriteText(w io.Writer, report Report) error {
	var b strings.Builder

	for _, r := range report.Cases {
		status := "ok  "
		if !r.Passed {
			status = "FAIL"
		}

		negative := ""
		if r.Expect == Deny {
			negative = "  (deny)"
		}

		fmt.Fprintf(&b, "%s  %s%s\n", status, r.Name, negative)

		if r.Failure != "" {
			fmt.Fprintf(&b, "      %s\n", r.Failure)
		}

		if r.Got == Deny {
			fmt.Fprintf(&b, "      denied: %s: %s\n", r.Reason, r.Detail)
		}
	}

	fmt.Fprintf(&b, "\n%d cases (%s policy): %d passed, %d failed; %d expect deny, %d of them naming the rule\n",
		report.Total, report.Surface, report.Passed, report.Failed, report.Denials, report.Named)

	for _, warning := range report.Warnings {
		fmt.Fprintf(&b, "warning: %s\n", warning)
	}

	_, err := io.WriteString(w, b.String())

	return err
}
