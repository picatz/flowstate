package flowfile

import (
	"errors"

	yaml "github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/ast"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// `step_defaults:` — the `timeout:`, `total_timeout:` and `retry:` a file states
// once for every step that does work and does not state its own.
//
// A file that bounded five calls the same way wrote the same lines five times,
// and a change to the bound was five edits that could miss one. The block is the
// one place to say it:
//
//	step_defaults:
//	  timeout: 30s
//	  retry: { attempts: 4, interval: 2s }
//
// # One policy per step, resolved when the file compiles
//
// The compiler writes the defaults into each eligible step's policy, so a driver
// reads the one policy the step has and the two drivers cannot differ on which
// applies (invariant 3). [v1.Workflow.StepDefaults] keeps the block only so that
// [Marshal] can factor the values back out; no driver reads it.
//
// # Precedence
//
// Per key, the step wins, and a key is replaced whole: a step's `retry:` is not
// merged field by field with the default's. A step that wants the engine's own
// retry behaviour back writes `retry:` with nothing under it. A step cannot
// remove a default `timeout:`, because a bound is what a default is for.
//
// # What it reaches
//
// The steps that schedule a single activity, which are the only ones `timeout:`
// and `retry:` are accepted on ([unrepresentablePolicySubject]), at any depth of
// a `for_each:`, `loop:`, `parallel:` or `switch:` body. A `call:` is another
// file: its steps take that file's `step_defaults:`, not the caller's. A
// compensation under `undo:` has no policy of its own and is not reached.
//
// `continue_on_error:` is not a default. Tolerating a failure is a decision about
// one step's failure, and a blanket one hides the failures the rest of the file
// branches on.
//
// Parsing, resolving, factoring and validating are together here, as in
// `concurrency.go`: [factorStepDefaults] is the inverse of [applyStepDefaults],
// and a rule one knew and the other did not would be a `flow fmt` that rewrites
// meaning.

// stepDefaultsKeys are what the block may say.
var stepDefaultsKeys = []string{"timeout", "total_timeout", "retry"}

// stepDefaults compiles the top-level `step_defaults:` block.
func (c *compiler) stepDefaults(n ast.Node, path string, r ref) *v1.StepPolicy {
	c.pos.record(path, spanOfNode(c.resolveQuiet(n)))

	fields, ok := c.fields(n, path, r, stepDefaultsKeys)
	if !ok {
		c.report(spanOfNode(n), r,
			"is a mapping of the `timeout:`, `total_timeout:` and `retry:` every step that does work takes "+
				"unless it states its own")
		return nil
	}

	defaults := c.policy(fields, path, r)
	if defaults == nil {
		c.report(spanOfNode(n), r, "declares nothing: write `timeout:`, `total_timeout:` or `retry:`, or remove the block")
	}
	return defaults
}

// eligibleForStepDefaults reports whether a step takes the file's defaults: it
// schedules a single activity, which is what `timeout:` and `retry:` bind.
func eligibleForStepDefaults(node *v1.Node) bool {
	_, refused := unrepresentablePolicySubject(node)
	return !refused
}

// applyStepDefaults writes the defaults into every eligible step under nodes
// that does not state the key itself. It returns the steps whose resolved policy
// says a total shorter than one attempt, which an inherited key can cause
// without any one line of the file being wrong.
func applyStepDefaults(nodes []*v1.Node, defaults *v1.StepPolicy) (contradictory []*v1.Node) {
	if defaults == nil {
		return nil
	}

	v1.WalkNodes(nodes, v1.Walk{Node: func(node *v1.Node) {
		if !eligibleForStepDefaults(node) {
			return
		}

		policy := node.GetPolicy()
		if policy == nil {
			policy = &v1.StepPolicy{}
		}

		inherited := false
		if policy.Timeout == nil && defaults.Timeout != nil {
			policy.Timeout, inherited = proto.Clone(defaults.Timeout).(*durationpb.Duration), true
		}
		if policy.TotalTimeout == nil && defaults.TotalTimeout != nil {
			policy.TotalTimeout, inherited = proto.Clone(defaults.TotalTimeout).(*durationpb.Duration), true
		}
		if policy.Retry == nil && defaults.Retry != nil {
			policy.Retry, inherited = proto.Clone(defaults.Retry).(*v1.RetryPolicy), true
		}
		if !inherited {
			return
		}

		node.Policy = policy
		if total, one := policy.GetTotalTimeout().AsDuration(), policy.GetTimeout().AsDuration(); one > 0 && total > 0 && total < one {
			contradictory = append(contradictory, node)
		}
	}})
	return contradictory
}

// factorStepDefaults is [applyStepDefaults] run backwards on a copy: it removes
// from every eligible step the keys that equal the file's defaults, so the
// document says them once. A step that states a key equal to the default is
// written without it, which resolves to the same policy when the file is read
// again.
func factorStepDefaults(wf *v1.Workflow) *v1.Workflow {
	defaults := wf.GetStepDefaults()
	if defaults == nil {
		return wf
	}

	wf = proto.Clone(wf).(*v1.Workflow)
	v1.WalkNodes(wf.GetSteps(), v1.Walk{Node: func(node *v1.Node) {
		policy := node.GetPolicy()
		if policy == nil || !eligibleForStepDefaults(node) {
			return
		}

		if defaults.Timeout != nil && proto.Equal(policy.Timeout, defaults.Timeout) {
			policy.Timeout = nil
		}
		if defaults.TotalTimeout != nil && proto.Equal(policy.TotalTimeout, defaults.TotalTimeout) {
			policy.TotalTimeout = nil
		}
		if defaults.Retry != nil && proto.Equal(policy.Retry, defaults.Retry) {
			policy.Retry = nil
		}
		if proto.Equal(policy, &v1.StepPolicy{}) {
			node.Policy = nil
		}
	}})
	return wf
}

// stepDefaultsToYAML writes the block in the order a step writes the same keys.
func stepDefaultsToYAML(defaults *v1.StepPolicy) (yaml.MapSlice, error) {
	if defaults.GetContinueOnError() || len(defaults.GetToleratedKinds()) > 0 {
		return nil, errors.New("`step_defaults:` cannot carry `continue_on_error:`, so the parser would reject the marshalled file")
	}

	out := yaml.MapSlice{}
	if timeout := defaults.GetTimeout(); timeout != nil {
		out = append(out, yaml.MapItem{Key: "timeout", Value: durationToYAML(timeout)})
	}
	if total := defaults.GetTotalTimeout(); total != nil {
		out = append(out, yaml.MapItem{Key: "total_timeout", Value: durationToYAML(total)})
	}
	if retry := defaults.GetRetry(); retry != nil {
		out = append(out, yaml.MapItem{Key: "retry", Value: retryToYAML(retry)})
	}
	return out, nil
}

// validateStepDefaults holds a specification's block to what the grammar allows,
// since a specification built by hand never met the parser.
func validateStepDefaults(wf *v1.Workflow) Diagnostics {
	defaults := wf.GetStepDefaults()
	if defaults == nil {
		return nil
	}

	var ds Diagnostics
	if defaults.GetContinueOnError() || len(defaults.GetToleratedKinds()) > 0 {
		ds = append(ds, Diagnostic{Field: "step_defaults", Message: "carries `continue_on_error:`, which is not a default: " +
			"tolerating a failure is a decision about one step, and a blanket one hides the failures the rest of the file branches on"})
	}
	if proto.Equal(defaults, &v1.StepPolicy{}) {
		ds = append(ds, Diagnostic{Field: "step_defaults", Message: "declares nothing: write `timeout:`, `total_timeout:` or `retry:`, or remove the block"})
	}
	if timeout, total := defaults.GetTimeout().AsDuration(), defaults.GetTotalTimeout().AsDuration(); timeout > 0 && total > 0 && total < timeout {
		ds = append(ds, Diagnostic{Field: "step_defaults.total_timeout", Message: "is shorter than `timeout:`, so every step's whole budget expires inside its first attempt"})
	}
	return ds
}
