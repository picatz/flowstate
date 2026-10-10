package flowfile

import (
	"fmt"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// checkSourceForms refuses a workflow whose `allow_source`, `must_source` or
// `type_source` would be written back as something other than what runs.
//
// Those fields exist so [Marshal] can write the call form an author wrote. Nothing
// evaluates them, so a hand-built or stale specification can carry a source that
// expands to a different rule than the lowered `allow` or `must` beside it, and
// `flow fmt` would then rewrite the file into a policy that is not the one that
// ran. Each source is expanded again with the workflow's own functions, the way the
// compiler lowered it, and must equal the lowered text. The check is bounded by the
// expansion budget the function set already enforces.
func checkSourceForms(wf *v1.Workflow) error {
	set, errs := v1.NewFunctionSet(wf.GetProfile(), wf.GetDeclaredFunctions())
	if len(errs) > 0 {
		return fmt.Errorf("source forms cannot be checked: %w", errs[0].Err)
	}

	same := func(what, source, lowered string) error {
		if source == "" {
			return nil
		}
		expanded, _, err := set.ExpandText(source)
		if err != nil {
			return fmt.Errorf("%s: its source form does not expand: %w", what, err)
		}
		if expanded != lowered {
			return fmt.Errorf("%s: its source form expands to something other than the rule that runs; drop the source form or recompile the file", what)
		}

		return nil
	}

	for name, policy := range wf.GetSignals() {
		if err := same(fmt.Sprintf("signal %q allow", name), policy.GetAllowSource(), policy.GetAllow()); err != nil {
			return err
		}
	}
	if err := same("debug allow", wf.GetDebug().GetAllowSource(), wf.GetDebug().GetAllow()); err != nil {
		return err
	}
	manual := wf.GetTriggers().GetManual()
	if err := same("triggers manual allow", manual.GetAllowSource(), manual.GetAllow()); err != nil {
		return err
	}

	declaration := func(what string, must, mustSource, typeSource *string) error {
		if typeSource != nil && must == nil {
			return fmt.Errorf("%s: names the scalar type %q but carries no rule", what, *typeSource)
		}
		if mustSource == nil {
			return nil
		}
		if typeSource == nil {
			return same(what+" must", *mustSource, deref(must))
		}
		// A constrained scalar stores the type's rule conjoined with the author's own,
		// so the author's rule is the tail of what runs.
		own, _, err := set.ExpandText(*mustSource)
		if err != nil {
			return fmt.Errorf("%s must: its source form does not expand: %w", what, err)
		}
		if *must != own && !strings.HasSuffix(*must, " && ("+own+")") {
			return fmt.Errorf("%s must: its source form expands to something other than the rule that runs; drop the source form or recompile the file", what)
		}

		return nil
	}
	for _, d := range wf.GetDeclaredInputs() {
		if err := declaration("input "+d.GetName(), d.Must, d.MustSource, d.TypeSource); err != nil {
			return err
		}
	}
	for _, d := range wf.GetDeclaredOutputs() {
		if err := declaration("output "+d.GetName(), d.Must, d.MustSource, d.TypeSource); err != nil {
			return err
		}
	}
	for _, t := range wf.GetDeclaredTypes() {
		if err := same("type "+t.GetName()+" must", t.GetMustSource(), t.GetMust()); err != nil {
			return err
		}
		for _, f := range t.GetFields() {
			if err := declaration("type "+t.GetName()+" field "+f.GetName(), f.Must, f.MustSource, f.TypeSource); err != nil {
				return err
			}
		}
	}

	return nil
}
