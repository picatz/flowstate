package flowstatev1_test

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func choiceQuestion(name string, options ...string) *v1.Question {
	return &v1.Question{Name: name, Kind: &v1.Question_Choice_{Choice: &v1.Question_Choice{Options: options}}}
}

func scoreQuestion(name string, levels ...string) *v1.Question {
	return &v1.Question{Name: name, Kind: &v1.Question_Score_{Score: &v1.Question_Score{Levels: levels}}}
}

func predicateQuestion(name string) *v1.Question {
	return &v1.Question{Name: name, Kind: &v1.Question_Predicate_{Predicate: &v1.Question_Predicate{}}}
}

func choiceAnswer(name, selected string, c v1.Calibration) *v1.Answer {
	return &v1.Answer{Name: name, Result: &v1.Answer_Choice{Choice: selected}, Calibration: c}
}

func predicateAnswer(name string, value bool, c v1.Calibration, d map[string]float64) *v1.Answer {
	return &v1.Answer{Name: name, Result: &v1.Answer_Predicate{Predicate: value}, Calibration: c, Distribution: d}
}

// withNumbers returns a copy of a carrying the given confidence (nil for
// none) and distribution.
func withNumbers(a *v1.Answer, confidence *float64, distribution map[string]float64) *v1.Answer {
	a = proto.Clone(a).(*v1.Answer)
	a.Confidence, a.Distribution = confidence, distribution
	return a
}

// requireRule asserts that err is a validation failure that includes rule.
func requireRule(t *testing.T, err error, rule string) {
	t.Helper()
	var invalid *v1.ValidationError
	require.ErrorAsf(t, err, &invalid, "want a *v1.ValidationError, got %[1]T: %[1]v", err)
	rules := make([]string, 0, len(invalid.Violations))
	for _, v := range invalid.Violations {
		rules = append(rules, v.Rule)
	}
	require.Containsf(t, rules, rule, "violations: %v", invalid)
}

// TestDecisionRules proves each rule of the decision schema in both
// directions: a message that satisfies it is accepted, and the one change that
// violates it is rejected by exactly that rule.
func TestDecisionRules(t *testing.T) {
	const (
		modelProb = v1.Calibration_CALIBRATION_MODEL_PROBABILITY
		self      = v1.Calibration_CALIBRATION_SELF_REPORTED
		none      = v1.Calibration_CALIBRATION_NONE
	)
	probs := map[string]float64{"low": 0.25, "high": 0.75}
	q := choiceQuestion("severity", "low", "high")
	ok := withNumbers(choiceAnswer("severity", "high", modelProb), ptr(0.75), probs)
	selfAnswer := func(confidence float64) *v1.Answer {
		return withNumbers(choiceAnswer("a", "x", self), &confidence, nil)
	}

	cases := []struct {
		name string
		msg  proto.Message
		rule string // empty: must be accepted
	}{
		// Question names: non-empty, and unique within a set.
		{"question name accepted", predicateQuestion("is-spam"), ""},
		{"question name empty", predicateQuestion(""), "required"},
		{"question name malformed", predicateQuestion("-bad name"), "string.pattern"},
		{"question set accepted", &v1.QuestionSet{Questions: []*v1.Question{predicateQuestion("a"), predicateQuestion("b")}}, ""},
		{"question set empty", &v1.QuestionSet{}, "repeated.min_items"},
		{"question set duplicate names", &v1.QuestionSet{Questions: []*v1.Question{predicateQuestion("a"), choiceQuestion("a", "x")}}, "question_set.unique_names"},
		{"question kind required", &v1.Question{Name: "a"}, "required"},

		// Options and levels: non-empty set, non-empty unique members.
		{"options accepted", choiceQuestion("a", "x", "y"), ""},
		{"options empty", choiceQuestion("a"), "repeated.min_items"},
		{"option empty string", choiceQuestion("a", "x", ""), "string.min_len"},
		{"options duplicate", choiceQuestion("a", "x", "x"), "repeated.unique"},
		{"levels accepted", scoreQuestion("a", "poor", "fair", "good"), ""},
		{"levels empty", scoreQuestion("a"), "repeated.min_items"},
		{"level empty string", scoreQuestion("a", ""), "string.min_len"},
		{"levels duplicate", scoreQuestion("a", "poor", "poor"), "repeated.unique"},

		// Confidence in [0,1].
		{"confidence at lower bound", selfAnswer(0), ""},
		{"confidence at upper bound", selfAnswer(1), ""},
		{"confidence above 1", selfAnswer(1.01), "double.gte_lte"},
		{"confidence below 0", selfAnswer(-0.01), "double.gte_lte"},

		// Calibration is stated, and NONE carries no numbers.
		{"calibration none bare", choiceAnswer("a", "x", none), ""},
		{"calibration none with confidence", withNumbers(choiceAnswer("a", "x", none), ptr(0.5), nil), "answer.calibration_none_has_no_numbers"},
		{"calibration none with distribution", withNumbers(choiceAnswer("a", "x", none), nil, map[string]float64{"x": 1}), "answer.calibration_none_has_no_numbers"},
		{"calibration unspecified", choiceAnswer("a", "x", v1.Calibration_CALIBRATION_UNSPECIFIED), "required"},
		{"result required", &v1.Answer{Name: "a", Calibration: none}, "required"},

		// Distribution: sums to ~1, entries in [0,1], covers the result.
		{"distribution accepted", ok, ""},
		{"distribution within tolerance", withNumbers(ok, nil, map[string]float64{"low": 0.2504, "high": 0.75}), ""},
		{"distribution sums low", withNumbers(ok, nil, map[string]float64{"low": 0.25, "high": 0.5}), "answer.distribution_sums_to_one"},
		{"distribution sums high", withNumbers(ok, nil, map[string]float64{"low": 0.5, "high": 0.75}), "answer.distribution_sums_to_one"},
		{"distribution entry out of range", withNumbers(ok, nil, map[string]float64{"low": -0.5, "high": 1.5}), "double.gte_lte"},
		{"distribution omits selected", withNumbers(ok, nil, map[string]float64{"low": 1}), "answer.distribution_covers_result"},
		{"predicate distribution accepted", predicateAnswer("a", true, modelProb, map[string]float64{"true": 0.9, "false": 0.1}), ""},
		{"predicate distribution wrong keys", predicateAnswer("a", true, modelProb, map[string]float64{"yes": 0.9, "no": 0.1}), "answer.distribution_covers_result"},

		// A decision relates the answer to its question.
		{"decision accepted", &v1.Decision{Question: q, Answer: ok}, ""},
		{"decision score accepted", &v1.Decision{Question: scoreQuestion("q", "poor", "good"), Answer: &v1.Answer{Name: "q", Result: &v1.Answer_Score{Score: "good"}, Calibration: none}}, ""},
		{"decision predicate accepted", &v1.Decision{Question: predicateQuestion("q"), Answer: predicateAnswer("q", false, none, nil)}, ""},
		{"decision selected not an option", &v1.Decision{Question: q, Answer: choiceAnswer("severity", "medium", none)}, "decision.selected_is_an_option"},
		{"decision level not on the scale", &v1.Decision{Question: scoreQuestion("q", "poor", "good"), Answer: &v1.Answer{Name: "q", Result: &v1.Answer_Score{Score: "great"}, Calibration: none}}, "decision.selected_is_an_option"},
		{"decision name mismatch", &v1.Decision{Question: q, Answer: choiceAnswer("other", "high", none)}, "decision.names_match"},
		{"decision kind mismatch", &v1.Decision{Question: q, Answer: predicateAnswer("severity", true, none, nil)}, "decision.result_matches_kind"},
		{"decision distribution has extra key", &v1.Decision{Question: q, Answer: withNumbers(choiceAnswer("severity", "high", modelProb), nil, map[string]float64{"low": 0.1, "high": 0.8, "medium": 0.1})}, "decision.distribution_is_over_the_options"},
		{"decision distribution misses an option", &v1.Decision{Question: choiceQuestion("severity", "low", "mid", "high"), Answer: ok}, "decision.distribution_is_over_the_options"},
		{"decision question required", &v1.Decision{Answer: ok}, "required"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := v1.Validate(tc.msg)
			if tc.rule == "" {
				require.NoError(t, err)
				return
			}
			requireRule(t, err, tc.rule)
		})
	}
}

// TestDecisionDistributionBound proves the unrolled sum covers every entry a
// distribution may hold: a full 32-entry distribution that sums to 1 is
// accepted, one whose last entry is zeroed no longer sums to 1 and is
// rejected, and a 33rd option is refused by the bound itself.
func TestDecisionDistributionBound(t *testing.T) {
	const n = 32
	options := make([]string, 0, n)
	for i := range n {
		options = append(options, fmt.Sprintf("o%02d", i))
	}
	even := make(map[string]float64, n)
	for _, o := range options {
		even[o] = 1.0 / n
	}
	decision := func(d map[string]float64, options ...string) *v1.Decision {
		return &v1.Decision{
			Question: choiceQuestion("q", options...),
			Answer:   withNumbers(choiceAnswer("q", options[0], v1.Calibration_CALIBRATION_MODEL_PROBABILITY), nil, d),
		}
	}
	require.NoError(t, v1.Validate(decision(even, options...)))

	short := maps.Clone(even)
	short[slices.Max(options)] = 0
	requireRule(t, v1.Validate(decision(short, options...)), "answer.distribution_sums_to_one")

	requireRule(t, v1.Validate(decision(nil, append(slices.Clone(options), "extra")...)), "repeated.max_items")
}

// TestDecisionBounds proves the size and finiteness limits in both
// directions: a message at the limit is accepted and one past it is rejected
// by the limit's rule. They are the schema's work bounds, so a limit that is
// loosened or dropped has to fail a test.
func TestDecisionBounds(t *testing.T) {
	const none = v1.Calibration_CALIBRATION_NONE
	const modelProb = v1.Calibration_CALIBRATION_MODEL_PROBABILITY
	names := func(n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf("o%d", i)
		}
		return out
	}
	questions := func(n int) []*v1.Question {
		out := make([]*v1.Question, n)
		for i := range out {
			out[i] = predicateQuestion(fmt.Sprintf("q%d", i))
		}
		return out
	}
	// distribution spreads 1 evenly over keys, so it sums to 1 at any size.
	distribution := func(keys []string) map[string]float64 {
		d := make(map[string]float64, len(keys))
		for _, k := range keys {
			d[k] = 1 / float64(len(keys))
		}
		return d
	}
	long := func(n int) string { return strings.Repeat("a", n) }
	nan, inf := math.NaN(), math.Inf(1)

	cases := []struct {
		name string
		msg  proto.Message
		rule string // empty: must be accepted
	}{
		{"question set at the maximum", &v1.QuestionSet{Questions: questions(32)}, ""},
		{"question set past the maximum", &v1.QuestionSet{Questions: questions(33)}, "repeated.max_items"},
		{"options at the maximum", choiceQuestion("a", names(32)...), ""},
		{"options past the maximum", choiceQuestion("a", names(33)...), "repeated.max_items"},
		{"levels at the maximum", scoreQuestion("a", names(32)...), ""},
		{"levels past the maximum", scoreQuestion("a", names(33)...), "repeated.max_items"},

		{"question name at the limit", predicateQuestion(long(128)), ""},
		{"question name past the limit", predicateQuestion(long(129)), "string.max_len"},
		{"option at the limit", choiceQuestion("a", long(128)), ""},
		{"option past the limit", choiceQuestion("a", long(129)), "string.max_len"},
		{"level at the limit", scoreQuestion("a", long(128)), ""},
		{"level past the limit", scoreQuestion("a", long(129)), "string.max_len"},
		{"instructions at the limit", &v1.Question{Name: "a", Instructions: long(16384), Kind: &v1.Question_Predicate_{Predicate: &v1.Question_Predicate{}}}, ""},
		{"instructions past the limit", &v1.Question{Name: "a", Instructions: long(16385), Kind: &v1.Question_Predicate_{Predicate: &v1.Question_Predicate{}}}, "string.max_bytes"},

		{"answer name past the limit", choiceAnswer(long(129), "x", none), "string.max_len"},
		{"selected choice past the limit", choiceAnswer("a", long(129), none), "string.max_len"},
		{"selected level past the limit", &v1.Answer{Name: "a", Result: &v1.Answer_Score{Score: long(129)}, Calibration: none}, "string.max_len"},

		{"distribution at the maximum", withNumbers(choiceAnswer("a", "o0", modelProb), nil, distribution(names(32))), ""},
		{"distribution past the maximum", withNumbers(choiceAnswer("a", "o0", modelProb), nil, distribution(names(33))), "map.max_pairs"},
		{"distribution key at the limit", withNumbers(choiceAnswer("a", long(128), modelProb), nil, map[string]float64{long(128): 1}), ""},
		{"distribution key past the limit", withNumbers(choiceAnswer("a", "x", modelProb), nil, map[string]float64{"x": 0.5, long(129): 0.5}), "string.max_len"},
		{"distribution key empty", withNumbers(choiceAnswer("a", "x", modelProb), nil, map[string]float64{"x": 0.5, "": 0.5}), "string.min_len"},

		{"confidence NaN", withNumbers(choiceAnswer("a", "x", modelProb), ptr(nan), nil), "double.finite"},
		{"confidence infinite", withNumbers(choiceAnswer("a", "x", modelProb), ptr(inf), nil), "double.finite"},
		{"distribution NaN", withNumbers(choiceAnswer("a", "x", modelProb), nil, map[string]float64{"x": nan}), "double.finite"},
		{"distribution infinite", withNumbers(choiceAnswer("a", "x", modelProb), nil, map[string]float64{"x": inf}), "double.finite"},

		{"calibration outside the enum", choiceAnswer("a", "x", v1.Calibration(99)), "enum.defined_only"},
		{"decision answer required", &v1.Decision{Question: predicateQuestion("q")}, "required"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := v1.Validate(tc.msg)
			if tc.rule == "" {
				require.NoError(t, err)
				return
			}
			requireRule(t, err, tc.rule)
		})
	}
}
