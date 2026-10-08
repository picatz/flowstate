package main

import (
	"fmt"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// addValidateAuthPolicyFlags adds `flow validate`'s cross-check against a
// deployment's auth policy: with it, a claim an expression reads that no issuer
// entry carries is a diagnostic, instead of a rule that denies in production
// with nothing to say why.
func addValidateAuthPolicyFlags(cmd *cobra.Command) {
	cmd.Flags().String("auth-policy", "",
		"path to the deployment's auth policy (YAML); when given, every identity expression in the checked "+
			"files (`signals:`, `debug:`, `triggers: manual:`) and in the policy's own `secrets:` and "+
			"`federation:` rules is checked against what its issuer entries carry (`carry_claims`, "+
			"`groups_claim`), and a claim no entry carries is reported: a rule requiring it can never match; a rule "+
			"reading `actors` or `delegated` is reported when no entry has a `delegation:` stanza")
}

// claimCheck is `flow validate --auth-policy`: the claims the policy's issuer
// entries carry, and the rule files named beside it.
type claimCheck struct {
	// carriedBy maps each carried claim name to the entries carrying it.
	carriedBy map[string][]string

	// delegating names the entries with a `delegation:` stanza, the only ones
	// whose callers can have `identity.actors`.
	delegating []string

	// policyFiles are the expressions the auth policy's own rules hold, which
	// are checked once per invocation and not once per Flowfile.
	policyFiles []policyFileExpressions
}

// policyFileExpressions are the identity expressions one policy file holds.
type policyFileExpressions struct {
	path        string
	expressions []v1.PolicyExpression
}

// claimCheckOf loads the policy named on the command line, or returns nil when
// none was named. A policy that cannot be read or does not validate fails the
// command: carrying on would report every claim as uncarried, a false report
// about the files.
func claimCheckOf(cmd *cobra.Command) (*claimCheck, error) {
	path, _ := cmd.Flags().GetString("auth-policy")
	if path == "" {
		return nil, nil
	}

	data, err := readBoundedFile(path, "an auth policy", maxPolicyFileBytes)
	if err != nil {
		return nil, fmt.Errorf("reading --auth-policy: %w", err)
	}
	policy, err := auth.ParsePolicy(data)
	if err != nil {
		return nil, fmt.Errorf("--auth-policy %s: %w", path, err)
	}

	check := &claimCheck{carriedBy: map[string][]string{}}
	for _, entry := range policy.Issuers {
		for _, name := range entry.ClaimNames() {
			check.carriedBy[name] = append(check.carriedBy[name], entry.Name)
		}
		if entry.Delegation != nil {
			check.delegating = append(check.delegating, entry.Name)
		}
	}

	// The policy's own secret and assumption rules are read against the entries
	// of the same file.
	var own []v1.PolicyExpression
	add := func(where string, rules []string) {
		for i, rule := range rules {
			own = append(own, v1.PolicyExpression{Where: fmt.Sprintf("%s[%d]", where, i), Source: rule})
		}
	}
	if policy.Secrets != nil {
		add("secrets.allow", policy.Secrets.Allow)
		add("secrets.deny", policy.Secrets.Deny)
	}
	if policy.Federation != nil {
		add("federation.allow", policy.Federation.Allow)
		add("federation.deny", policy.Federation.Deny)
	}
	check.policyFiles = append(check.policyFiles, policyFileExpressions{path: path, expressions: own})

	return check, nil
}

// diagnose reports each claim an expression reads that no issuer entry carries,
// and each read of the caller's actor chain when no issuer entry has a
// `delegation:` stanza, at the expression's position when the span lookup knows
// it.
func (c *claimCheck) diagnose(expressions []v1.PolicyExpression, span func(where string) (line, column int)) flowfile.Diagnostics {
	var out flowfile.Diagnostics

	for _, expression := range expressions {
		if reads, err := v1.IdentityReadsActors(expression.Source); err == nil && reads && len(c.delegating) == 0 {
			line, column := 0, 0
			if span != nil {
				line, column = span(expression.Where)
			}
			out = append(out, flowfile.Diagnostic{
				Line: line, Column: column,
				Message: fmt.Sprintf("%s reads the caller's `actors` or `delegated`, but no issuer entry in the auth policy has a "+
					"`delegation:` stanza, so a token's `act` chain is refused and no caller is ever delegated: `delegated` is "+
					"always false and `actors` is always empty, and a rule requiring an actor can never match; add `delegation:` "+
					"to the entry that admits the delegating callers",
					expression.Where),
			})
		}

		reads, err := v1.IdentityClaimReads(expression.Source)
		if err != nil {
			// An expression that does not parse is the file's own diagnostic,
			// reported by the validator that compiled it.
			continue
		}

		for _, name := range reads {
			if _, carried := c.carriedBy[name]; carried {
				continue
			}

			line, column := 0, 0
			if span != nil {
				line, column = span(expression.Where)
			}
			out = append(out, flowfile.Diagnostic{
				Line: line, Column: column,
				Message: fmt.Sprintf("%s reads the caller's claim %q, which no issuer entry in the auth policy carries (%s), "+
					"so a caller never has it and a rule requiring it can never match; add it to `carry_claims` "+
					"(or `groups_claim`, for groups) on the entry that admits these callers",
					expression.Where, name, c.carriedList()),
			})
		}
	}

	return out
}

// carriedList names what the policy does carry, so the author sees the near
// miss (`team` against `teams`) beside the diagnostic.
func (c *claimCheck) carriedList() string {
	if len(c.carriedBy) == 0 {
		return "it carries none"
	}
	names := make([]string, 0, len(c.carriedBy))
	for name := range c.carriedBy {
		names = append(names, name)
	}
	slices.Sort(names)

	return "it carries " + strings.Join(names, ", ")
}

// workflow checks one Flowfile target. It is a no-op for a file that does not
// parse: [validateWorkflowTarget] has reported that.
func (c *claimCheck) workflow(target validateTarget) flowfile.Diagnostics {
	if c == nil {
		return nil
	}

	var (
		wf  *v1.Workflow
		pos *flowfile.Positions
		err error
	)
	if target.data != nil {
		wf, pos, err = flowfile.Parse(target.data)
	} else {
		wf, pos, err = flowfile.ParseFile(target.path)
	}
	if err != nil {
		return nil
	}

	return c.diagnose(v1.WorkflowIdentityExpressions(wf), func(where string) (int, int) {
		if span, ok := pos.ExprAt(where); ok {
			return span.Start.Line, span.Start.Column
		}
		// `triggers:` is a list whose entry holds the `manual:` block.
		if where == "triggers.manual.allow" {
			for i := range 8 {
				if span, ok := pos.ExprAt(fmt.Sprintf("triggers[%d].manual.allow", i)); ok {
					return span.Start.Line, span.Start.Column
				}
			}
		}

		return 0, 0
	})
}

// policies checks the expressions of the policy files named beside
// --auth-policy, once per invocation: each result is a report for that file.
func (c *claimCheck) policies() []fileDiagnostics {
	if c == nil {
		return nil
	}

	var out []fileDiagnostics
	for _, file := range c.policyFiles {
		if diagnostics := c.diagnose(file.expressions, nil); len(diagnostics) > 0 {
			out = append(out, fileDiagnostics{path: file.path, diagnostics: diagnostics})
		}
	}

	return out
}

// fileDiagnostics is what the cross-check found in one policy file.
type fileDiagnostics struct {
	path        string
	diagnostics flowfile.Diagnostics
}
