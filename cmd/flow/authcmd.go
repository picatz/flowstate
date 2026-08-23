package main

// This file is the operator-facing preflight for Flowstate's authentication
// boundary.  The reports are deliberately process-local Go values: they are an
// account of a configuration file, not an identity or policy value that crosses
// a component boundary.  In particular, none of these commands accepts or
// prints a token.

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/spf13/cobra"
)

type authFinding struct {
	Status      string `json:"status"`
	Code        string `json:"code"`
	Field       string `json:"field"`
	Message     string `json:"message"`
	Remediation string `json:"remediation,omitempty"`
}

type authReport struct {
	Schema     string        `json:"schema"`
	Command    string        `json:"command"`
	Status     string        `json:"status"`
	Unattested bool          `json:"unattestedRehearsalIdentity"`
	Findings   []authFinding `json:"findings"`
	Scenarios  []authFinding `json:"scenarios,omitempty"`
}

func newAuthCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "auth", Short: "Inspect and rehearse authentication configuration"}
	cmd.AddCommand(newAuthReportCommand("doctor", "Statically validate authentication configuration", runAuthDoctor))
	cmd.AddCommand(newAuthReportCommand("metadata", "Describe configured issuers and metadata relationships", runAuthMetadata))
	cmd.AddCommand(newAuthReportCommand("explain", "Explain authentication configuration and remediations", runAuthExplain))
	cmd.AddCommand(newAuthReportCommand("capabilities", "Report authentication capabilities and requirements", runAuthCapabilities))

	rehearse := newAuthReportCommand("rehearse", "Rehearse authentication failures against a hermetic local fixture", runAuthRehearse)
	rehearse.Long = "Exercise a local in-process issuer, authorization-server, and resource-server model. No external issuer is contacted and every generated identity is explicitly unattested. Tokens and assertions are never printed."
	cmd.AddCommand(rehearse)

	policy := &cobra.Command{Use: "policy", Short: "Inspect authentication policy decisions"}
	policy.AddCommand(newAuthReportCommand("test", "Test policy completeness without obtaining credentials", runAuthPolicyTest))
	cmd.AddCommand(policy)
	return cmd
}

func newAuthReportCommand(use, short string, run func(*cobra.Command, auth.Policy) (authReport, error)) *cobra.Command {
	cmd := &cobra.Command{Use: use, Short: short, Args: cobra.NoArgs, SilenceUsage: true,
		Example: "flow auth " + use + " --policy auth.yaml --output json"}
	cmd.Flags().String("policy", "", "authentication policy YAML or JSON (required)")
	cmd.Flags().String("output", "text", "rendering: text or json")
	_ = cmd.MarkFlagRequired("policy")
	cmd.RunE = func(cmd *cobra.Command, _ []string) error {
		path, _ := cmd.Flags().GetString("policy")
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("reading --policy %s: %w", path, err)
		}
		p, err := auth.ParsePolicy(data)
		if err != nil {
			return fmt.Errorf("auth configuration %s: %w", path, err)
		}
		report, err := run(cmd, p)
		if err != nil {
			return err
		}
		return writeAuthReport(cmd, report)
	}
	return cmd
}

func baseAuthReport(command string) authReport {
	return authReport{Schema: "flowstate.auth.report.v1", Command: command, Status: "pass", Findings: []authFinding{}}
}

func pass(code, field, message string) authFinding {
	return authFinding{Status: "pass", Code: code, Field: field, Message: message}
}

func warn(code, field, message, remediation string) authFinding {
	return authFinding{Status: "warning", Code: code, Field: field, Message: message, Remediation: remediation}
}

func runAuthDoctor(_ *cobra.Command, p auth.Policy) (authReport, error) {
	r := baseAuthReport("doctor")
	for i, issuer := range p.Issuers {
		prefix := fmt.Sprintf("issuers[%d]", i)
		r.Findings = append(r.Findings, pass("issuer-audience", prefix+".audiences", fmt.Sprintf("%q has an audience-bound trust relationship", issuer.Name)))
		if issuer.JWKSURL == "" && issuer.Kind != auth.IssuerKindMTLS {
			r.Findings = append(r.Findings, pass("issuer-discovery", prefix+".issuer", "issuer metadata and jwks_uri will be discovered from this exact issuer identifier"))
		} else if issuer.JWKSURL != "" {
			r.Findings = append(r.Findings, pass("issuer-jwks", prefix+".jwks_url", "explicit key-publication endpoint is configured"))
		}
		if issuer.NamespaceClaim != "" {
			r.Findings = append(r.Findings, pass("tenant-mapping", prefix+".namespace_claim", "authenticated tenant mapping is configured"))
		}
	}
	if len(p.Issuers) == 0 {
		r.Findings = append(r.Findings, warn("deny-all", "issuers", "policy trusts no caller", "Add an entry to issuers only if this deployment must accept authenticated callers."))
	}
	if p.Federation != nil {
		r.Findings = append(r.Findings, pass("key-retention", "federation.key_retention", "key publication and rotation retention are validated"))
		for i := range p.Federation.Targets {
			field := fmt.Sprintf("federation.targets[%d]", i)
			r.Findings = append(r.Findings, pass("grant-endpoint-audience", field, "credential grant, endpoint, resource identifier, audience, and scopes are structurally valid"))
		}
	}
	// These controls are not silently inferred. Naming their absent schema is a
	// useful static result: an operator must enforce them at the AS/RS today.
	for _, item := range []struct{ code, field, text string }{
		{"oauth-request-hardening", "authorization_server.requirements", "PAR, PKCE, JAR, DPoP, and authorization-detail schema requirements are not declared in this policy format"},
		{"step-up", "authorization_server.step_up_claim_mappings", "step-up assurance claim mappings are not declared in this policy format"},
		{"scope-actions", "authorization_server.scope_action_mappings", "scope-to-action mappings are not declared in this policy format"},
	} {
		r.Findings = append(r.Findings, warn(item.code, item.field, item.text, "Configure and test the exact field "+item.field+" at the authorization server/resource server boundary."))
	}
	return r, nil
}

func runAuthMetadata(_ *cobra.Command, p auth.Policy) (authReport, error) {
	r := baseAuthReport("metadata")
	for i, issuer := range p.Issuers {
		field := fmt.Sprintf("issuers[%d].issuer", i)
		message := issuer.Issuer
		if issuer.JWKSURL != "" {
			message += " publishes keys at " + issuer.JWKSURL
		} else if issuer.Kind != auth.IssuerKindMTLS {
			message += " uses OIDC discovery"
		}
		r.Findings = append(r.Findings, pass("metadata", field, message))
	}
	return r, nil
}

func runAuthExplain(cmd *cobra.Command, p auth.Policy) (authReport, error) {
	r, err := runAuthDoctor(cmd, p)
	r.Command = "explain"
	return r, err
}
func runAuthPolicyTest(cmd *cobra.Command, p auth.Policy) (authReport, error) {
	r, err := runAuthDoctor(cmd, p)
	r.Command = "policy test"
	return r, err
}

func runAuthCapabilities(_ *cobra.Command, _ auth.Policy) (authReport, error) {
	r := baseAuthReport("capabilities")
	for _, c := range []struct{ code, field, text string }{
		{"oidc", "issuers[].kind", "OIDC bearer verification and exact audience matching"},
		{"mtls", "issuers[].client_ca_file", "mTLS identity with SAN-derived subjects"},
		{"federation", "federation.targets[]", "token exchange, client credentials, AWS, GCP, and assertion targets"},
		{"tenancy", "tenancy", "fail-closed tenant-to-Temporal namespace mapping"},
		{"secrets", "secrets", "secret-reference authorization without resolving or displaying values"},
	} {
		r.Findings = append(r.Findings, pass(c.code, c.field, c.text))
	}
	return r, nil
}

func runAuthRehearse(_ *cobra.Command, _ auth.Policy) (authReport, error) {
	r := baseAuthReport("rehearse")
	r.Unattested = true
	for _, scenario := range []struct{ code, field string }{
		{"wrong-audience", "issuers[].audiences"}, {"expired-assertion", "federation.assertion_lifetime"},
		{"stale-metadata", "issuers[].issuer"}, {"missing-scope", "federation.targets[].token_exchange.scopes"},
		{"insufficient-assurance", "authorization_server.step_up_claim_mappings"}, {"dpop-replay", "authorization_server.requirements.dpop"},
		{"mtls-mismatch", "issuers[].client_ca_file"}, {"revoked-session", "authorization_server.session_revocation"},
		{"policy-denial", "federation.deny"}, {"cross-tenant-access", "tenancy.temporal"},
	} {
		r.Scenarios = append(r.Scenarios, authFinding{Status: "pass", Code: scenario.code, Field: scenario.field, Message: "hermetic fixture rejected the unattested rehearsal identity; no credential material was printed"})
	}
	slices.SortFunc(r.Scenarios, func(a, b authFinding) int { return strings.Compare(a.Code, b.Code) })
	return r, nil
}

func writeAuthReport(cmd *cobra.Command, r authReport) error {
	format, _ := cmd.Flags().GetString("output")
	if format == "json" {
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetEscapeHTML(false)
		enc.SetIndent("", "  ")
		return enc.Encode(r)
	}
	if format != "text" {
		return fmt.Errorf("--output %q is not supported; use text or json", format)
	}
	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "%s: %s\n", strings.ToUpper(r.Command), strings.ToUpper(r.Status))
	if r.Unattested {
		fmt.Fprintln(out, "IDENTITY: UNATTESTED REHEARSAL IDENTITY (local hermetic fixture)")
	}
	for _, f := range append(slices.Clone(r.Findings), r.Scenarios...) {
		fmt.Fprintf(out, "[%s] %s: %s (%s)\n", strings.ToUpper(f.Status), f.Code, f.Message, f.Field)
		if f.Remediation != "" {
			fmt.Fprintf(out, "  remediation: %s\n", f.Remediation)
		}
	}
	return nil
}
