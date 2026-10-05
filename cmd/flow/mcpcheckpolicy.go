package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	flowmcp "github.com/picatz/flowstate/cmd/flow/internal/mcp"
	"github.com/picatz/flowstate/cmd/flow/internal/policycheck"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// flowstate_check_policy is `flow signals check` as a tool.
//
// The decision is [policycheck]'s, which calls the engine's own check functions;
// this file is argument decoding and one answer, the [policycheck.Report] the
// command's `-o json` writes. There is no second evaluator here (invariant 2),
// and nothing in the answer is built from an argument: a refusal is the engine's
// fixed sentence, which quotes no claim, no subject and no input.
//
// It reaches nothing - no step runs, no server is dialed, no secret is resolved
// - but it is not served by `flow mcp serve`: that surface's tool list is a
// golden set a deliberate change edits, and serving a new tool to remote callers
// is a decision of its own.

// Bounds on an identity argument, mirroring the identity in
// [v1.PolicyCheckMatrix] so the two surfaces refuse the same sizes. The schema
// the tool advertises says them too, but a schema is advice to the caller and
// these are the check.
const (
	maxCheckPolicyIdentityField = 1024
	maxCheckPolicyClaims        = 64
	maxCheckPolicyClaimName     = 256
	maxCheckPolicyClaimValue    = 4096
	maxCheckPolicySignalName    = 256
)

// checkPolicyIdentity is one identity argument: the fields of a test file's
// `sender:`.
type checkPolicyIdentity struct {
	Subject   string            `json:"subject,omitempty"`
	Issuer    string            `json:"issuer,omitempty"`
	Namespace string            `json:"namespace,omitempty"`
	Claims    map[string]string `json:"claims,omitempty"`
}

// checkPolicyArguments is the tool's whole input surface.
type checkPolicyArguments struct {
	Source  string                     `json:"source"`
	Gate    string                     `json:"gate"`
	Signal  string                     `json:"signal,omitempty"`
	Sender  *checkPolicyIdentity       `json:"sender,omitempty"`
	Starter *checkPolicyIdentity       `json:"starter,omitempty"`
	Inputs  map[string]json.RawMessage `json:"inputs,omitempty"`
	Reason  string                     `json:"reason,omitempty"`
}

// scripted reads an identity argument as the identity a check is asked about.
// An absent one is nil, and so is a sender with nothing in it, as a matrix row
// with nothing in it is: an unauthenticated caller, not an authenticated one
// with empty fields. A starter keeps its emptiness, which is the known
// "started by nobody authenticated" the CLI's --starter-anonymous names.
//
// An error never quotes a value.
func (id *checkPolicyIdentity) scripted(what string, emptyIsNil bool) (*flowtest.ScriptedIdentity, error) {
	if id == nil {
		return nil, nil
	}

	for field, value := range map[string]string{"subject": id.Subject, "issuer": id.Issuer, "namespace": id.Namespace} {
		if utf8.RuneCountInString(value) > maxCheckPolicyIdentityField {
			return nil, fmt.Errorf("%s %s is over the %d character limit", what, field, maxCheckPolicyIdentityField)
		}
	}

	if len(id.Claims) > maxCheckPolicyClaims {
		return nil, fmt.Errorf("%s names %d claims, over the limit of %d", what, len(id.Claims), maxCheckPolicyClaims)
	}

	for name, value := range id.Claims {
		if utf8.RuneCountInString(name) > maxCheckPolicyClaimName || utf8.RuneCountInString(value) > maxCheckPolicyClaimValue {
			return nil, fmt.Errorf("%s has a claim over the %d character name or %d character value limit",
				what, maxCheckPolicyClaimName, maxCheckPolicyClaimValue)
		}
	}

	if emptyIsNil && id.Subject == "" && id.Issuer == "" && id.Namespace == "" && len(id.Claims) == 0 {
		return nil, nil
	}

	identity := &flowtest.ScriptedIdentity{Subject: id.Subject, Issuer: id.Issuer, Namespace: id.Namespace, Claims: id.Claims}

	// The rule a test file's identities load with, not a second copy of it.
	if err := identity.Check(what); err != nil {
		return nil, err
	}

	return identity, nil
}

// checkPolicyToolHandler answers one who-may-act question.
//
// Fail closed: every way the question cannot be put is a tool error carrying no
// verdict, so an agent cannot read half an answer as "admitted". A refusal, by
// contrast, is the answer and is not an error.
func checkPolicyToolHandler() mcp.ToolHandler {
	posture := defaultLocalRunPosture()

	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var args checkPolicyArguments

		if raw := req.Params.Arguments; len(raw) > 0 {
			decoder := json.NewDecoder(bytes.NewReader(raw))

			// The mirror of the schema's additionalProperties:false, for the
			// reason every tool on this surface has it.
			decoder.DisallowUnknownFields()

			if err := decoder.Decode(&args); err != nil {
				return flowmcp.ToolError(fmt.Errorf("arguments do not match %s: %w", flowmcp.CheckPolicyToolName, err)), nil
			}
		}

		if strings.TrimSpace(args.Source) == "" {
			return flowmcp.ToolError(errors.New(
				"source is required: pass the Flowfile YAML whose policy is asked")), nil
		}

		if utf8.RuneCountInString(args.Signal) > maxCheckPolicySignalName {
			return flowmcp.ToolError(fmt.Errorf("signal is over the %d character limit", maxCheckPolicySignalName)), nil
		}

		var signals []string

		switch args.Gate {
		case "signal":
			if args.Signal != "" {
				signals = []string{args.Signal}
			}
		case "debug", "manual":
			if args.Signal != "" {
				return flowmcp.ToolError(errors.New("signal names a signal, which only gate \"signal\" asks about")), nil
			}
		default:
			return flowmcp.ToolError(errors.New(`gate is required: one of "signal", "debug" or "manual"`)), nil
		}

		if args.Reason != "" && args.Gate != "manual" {
			return flowmcp.ToolError(errors.New(`reason is read only by gate "manual"`)), nil
		}

		sender, err := args.Sender.scripted("the sender", true)
		if err != nil {
			return flowmcp.ToolError(err), nil
		}

		starter, err := args.Starter.scripted("the starter", false)
		if err != nil {
			return flowmcp.ToolError(err), nil
		}

		workflow, err := parseFlowfileSource([]byte(args.Source))
		if err != nil {
			return flowmcp.ToolError(err), nil
		}

		gates, err := policycheck.Gates(workflow, signals, args.Gate == "debug", args.Gate == "manual")
		if err != nil {
			return flowmcp.ToolError(err), nil
		}

		// The refusal is the binder's own text, which may quote a value the
		// caller typed: through the seam every run surface redacts a
		// `sensitive:` input with, never revealing (this tool prints no value
		// to reveal).
		inputs, err := runLocalToolInputs(workflow, args.Inputs)
		if err != nil {
			return flowmcp.ToolError(refusedCheckInputs(posture, workflow, inputs, err)), nil
		}

		decisions, err := policycheck.Evaluate(ctx, workflow, gates, policycheck.Subject{
			Sender:  sender,
			Starter: starter,
			Inputs:  inputs,
			Reason:  args.Reason,
		})
		if err != nil {
			return flowmcp.ToolError(refusedCheckInputs(posture, workflow, inputs, err)), nil
		}

		report := policycheck.NewReport(gates, []policycheck.Result{{Decisions: decisions}})

		encoded, err := json.Marshal(report)
		if err != nil {
			return flowmcp.ToolError(fmt.Errorf("rendering the answers: %w", err)), nil
		}

		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(encoded)}}}, nil
	}
}
