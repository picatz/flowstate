// Package telemetrypolicy compiles and applies the protobuf telemetry policy.
// It has no dependency on cmd/flow or an OpenTelemetry SDK, avoiding a cycle
// while remaining usable by every Flowstate-owned emitter.
package telemetrypolicy

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"unicode/utf8"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

const defaultCostLimit = 1000

// Field is a non-serializing boundary value: producers translate it to their
// SDK's attribute type only after Normalize returns.
type Field struct{ Key, Value string }

type signalPolicy struct {
	allow          map[string]struct{}
	max            int
	permit, redact []cel.Program
}

// Policy is an immutable, compiled TelemetryPolicy.
type Policy struct {
	signals map[flowstatev1.TelemetrySignalKind]signalPolicy
	redact  map[string]struct{}
}

// New validates and compiles CEL before an emitter starts. Invalid policy is a
// startup error, never an expression silently ignored at emission time.
func New(spec *flowstatev1.TelemetryPolicy) (*Policy, error) {
	p := &Policy{signals: map[flowstatev1.TelemetrySignalKind]signalPolicy{}, redact: map[string]struct{}{}}
	if spec == nil {
		return p, nil
	}
	if err := flowstatev1.Validate(spec); err != nil {
		return nil, fmt.Errorf("invalid telemetry policy: %w", err)
	}
	for _, key := range spec.GetRedactedKeys() {
		p.redact[key] = struct{}{}
	}
	env, err := cel.NewEnv(
		cel.Variable("signal", cel.StringType), cel.Variable("key", cel.StringType),
		cel.Variable("value", cel.StringType), cel.Variable("attributes", cel.MapType(cel.StringType, cel.StringType)))
	if err != nil {
		return nil, err
	}
	for _, cfg := range spec.GetSignals() {
		if cfg.GetKind() == flowstatev1.TelemetrySignalKind_TELEMETRY_SIGNAL_KIND_UNSPECIFIED {
			return nil, fmt.Errorf("telemetry signal kind is required")
		}
		if _, exists := p.signals[cfg.GetKind()]; exists {
			return nil, fmt.Errorf("duplicate telemetry signal %s", cfg.GetKind())
		}
		sp := signalPolicy{allow: map[string]struct{}{}, max: int(cfg.GetMaxValueBytes())}
		for _, key := range cfg.GetAllowedKeys() {
			sp.allow[key] = struct{}{}
		}
		limit := cfg.GetRuleCostLimit()
		if limit == 0 {
			limit = defaultCostLimit
		}
		compile := func(expressions []string) ([]cel.Program, error) {
			out := make([]cel.Program, 0, len(expressions))
			for _, expression := range expressions {
				ast, issues := env.Compile(expression)
				if issues.Err() != nil {
					return nil, issues.Err()
				}
				if ast.OutputType() != cel.BoolType {
					return nil, fmt.Errorf("telemetry rule %q returns %s, want bool", expression, ast.OutputType())
				}
				program, err := env.Program(ast, cel.CostLimit(limit))
				if err != nil {
					return nil, err
				}
				out = append(out, program)
			}
			return out, nil
		}
		if sp.permit, err = compile(cfg.GetAllowIf()); err != nil {
			return nil, fmt.Errorf("%s allow_if: %w", cfg.GetKind(), err)
		}
		if sp.redact, err = compile(cfg.GetRedactIf()); err != nil {
			return nil, fmt.Errorf("%s redact_if: %w", cfg.GetKind(), err)
		}
		p.signals[cfg.GetKind()] = sp
	}
	return p, nil
}

// Normalize applies static and CEL rules, redaction, and byte bounds. Duplicate
// keys are first-wins so verified identity fields supplied first cannot be
// replaced by baggage or request attributes.
func (p *Policy) Normalize(kind flowstatev1.TelemetrySignalKind, candidates ...Field) []Field {
	sp, ok := p.signals[kind]
	if !ok || sp.max <= 0 {
		return nil
	}
	all := make(map[string]string, len(candidates))
	for _, f := range candidates {
		if _, exists := all[f.Key]; !exists {
			all[f.Key] = f.Value
		}
	}
	values := map[string]string{}
	for _, f := range candidates {
		if _, exists := values[f.Key]; exists {
			continue
		}
		if _, ok := sp.allow[f.Key]; !ok {
			continue
		}
		if _, deny := p.redact[f.Key]; deny {
			continue
		}
		activation := map[string]any{"signal": kind.String(), "key": f.Key, "value": f.Value, "attributes": all}
		if !allRulesAnswer(sp.permit, activation) || anyRuleAnswers(sp.redact, activation) {
			continue
		}
		values[f.Key] = truncate(f.Value, sp.max)
	}
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]Field, 0, len(keys))
	for _, k := range keys {
		out = append(out, Field{k, values[k]})
	}
	return out
}

func allRulesAnswer(rules []cel.Program, activation map[string]any) bool {
	for _, rule := range rules {
		result, _, err := rule.Eval(activation)
		if err != nil || !boolValue(result) {
			return false
		}
	}
	return true
}

func anyRuleAnswers(rules []cel.Program, activation map[string]any) bool {
	for _, rule := range rules {
		result, _, err := rule.Eval(activation)
		// A redaction rule fails closed: an evaluation failure drops the field.
		if err != nil || boolValue(result) {
			return true
		}
	}
	return false
}
func boolValue(value ref.Val) bool { return value == types.True }

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	sum := sha256.Sum256([]byte(value))
	marker := "~truncated:" + hex.EncodeToString(sum[:8])
	if len(marker) >= limit {
		return marker[:limit]
	}
	keep := limit - len(marker)
	for keep > 0 && !utf8.ValidString(value[:keep]) {
		keep--
	}
	return value[:keep] + marker
}
