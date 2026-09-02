package main

import (
	"errors"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// errNoOperatorEgressPolicy is the refusal a deployment can act on: it names the
// flag that grants what this plugin is asking for, because "denied" without it
// leaves an operator guessing which of several controls decided.
var errNoOperatorEgressPolicy = errors.New(
	"postgres requires an operator egress policy passed with --egress-policy; the worker's own default " +
		"policy is not one, and the SQL plugin denies network access without it")

// egressPolicy is the deployment's egress policy, granted to this process at
// launch: an immutable snapshot of the same --egress-policy bytes already
// parsed for the built-in http task. PostgreSQL is not HTTP, so this plugin
// applies the policy on its own dial path rather than through sdk.HTTPClient;
// the grant it reads is the same one every plugin receives.
//
// Nil means PostgreSQL is denied, and [egressRefusal] says why. SQLite is
// unaffected: it opens a file, not a socket.
var egressPolicy *netpolicy.Policy

// egressRefusal is why there is no policy. It is the message a postgres task
// refuses with, so an operator reads what would grant it rather than a bare
// denial.
var egressRefusal error

// installEgressPolicy takes the deployment's grant, or records why PostgreSQL
// will be refused without it.
//
// It goes through [sdk.EgressPolicy] rather than reading, decoding and parsing
// the variable here (#1332): the SDK bounds the encoded and decoded lengths of
// an environment it did not build and refuses by name, and a second decode path
// beside it is a second set of answers to keep correct.
//
// The deployment default is refused, and this is the one first-party plugin that
// refuses it. A database is not an HTTP fetch: the destination is the whole
// meaning of the credential this task carries, and a worker's built-in default
// policy is what a deployment runs under when nobody has decided anything about
// destinations — which is not the same as a decision to permit this one. That is
// #1320's rule, kept intact now that a default worker grants a policy rather
// than nothing (see [sdk.EgressPolicyIsDeploymentDefault] for the two postures
// and why `git`, `vcs`, `github` and `slack` take the other).
//
// Neither refusal stops the process. The deployment default is a grant this
// plugin declines to act on rather than one it cannot read, and a launch by
// something that is not a Flowstate worker grants nothing at all; a policy that
// cannot be parsed or built never reaches here, being refused when the CLI reads
// the operator's file and again by plugin.NewHost. In every one of those states
// catalog and validation-only launches keep working, without pretending this
// plugin can connect anywhere.
func installEgressPolicy() {
	policy, err := sdk.EgressPolicy()
	if err != nil {
		egressRefusal = err
		return
	}

	isDefault, err := sdk.EgressPolicyIsDeploymentDefault()
	if err != nil {
		egressRefusal = err
		return
	}
	if isDefault {
		egressRefusal = errNoOperatorEgressPolicy
		return
	}

	egressPolicy = policy
}

// postgresRefusal is why a postgres task cannot connect.
//
// [installEgressPolicy] records the reason beside the policy it could not take,
// so the fallback covers only a nil policy that arrived some other way, where
// the honest answer is still that no operator policy is in force.
func postgresRefusal() error {
	if egressRefusal != nil {
		return egressRefusal
	}

	return errNoOperatorEgressPolicy
}

// classifyEgressCheck turns a policy check's error into this task's own.
//
// A denial is a decision: this destination is not permitted, no retry changes
// that, and the message names no host because the DSN is the caller's secret
// material. Anything else is not a decision at all, and is returned as itself so
// the caller's retry classification sees what actually happened rather than a
// permanent denial the operator's policy never made.
//
// The case that matters is a rule interrupted before it decided, which netpolicy
// returns as [netpolicy.UndecidedError] (#1379). It deliberately does not wrap
// [netpolicy.ErrDenied] — "no decision" and "denied" are different facts — so it
// needs no arm of its own here: matching only the denial is what keeps it out,
// and an arm returning it unchanged would restate the default. That is worth
// stating rather than leaving to be inferred, because the shape of this function
// is the whole of the distinction.
func classifyEgressCheck(err error) error {
	var denied *netpolicy.DenyError
	if errors.As(err, &denied) {
		return sdk.PermissionDenied("postgres destination is denied by deployment egress policy")
	}

	return err
}
