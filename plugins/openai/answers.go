package main

import (
	"encoding/json"
	"math"
	"slices"

	decisionv1 "github.com/picatz/flowstate/pkg/flowstate/decision/v1"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// The answer types the API returns. A question's type is the neutral question's
// kind, so an answer of any other type for it is a malformed reply.
const (
	kindPredicate = "predicate"
	kindChoice    = "choice"
	kindScore     = "score"
	kindRefusal   = "refusal"
)

// wireAnswer is every field any answer type can carry, all optional so a field
// that is absent can be told apart from one that is zero. Which fields a type
// requires is decided in [answersFromReply], not by the decoder.
type wireAnswer struct {
	Type          string     `json:"type"`
	Name          string     `json:"name"`
	Probability   *float64   `json:"probability"`
	Choice        *string    `json:"choice"`
	Score         *float64   `json:"score"`
	Confidence    *float64   `json:"confidence"`
	Probabilities []wireProb `json:"probabilities"`
}

// wireProb is one entry of `probabilities`: a choice's entries are keyed by
// `value` and a score's by `label`. A score's entry also has a `value`, which
// is the level's position and not a name, so it is held undecoded and read only
// where it is the key.
type wireProb struct {
	Value       json.RawMessage `json:"value"`
	Label       *string         `json:"label"`
	Probability *float64        `json:"probability"`
}

// choiceKey is a choice entry's option, or nil when it is not a string.
func choiceKey(p wireProb) *string {
	var value string
	if json.Unmarshal(p.Value, &value) != nil {
		return nil
	}
	return &value
}

// scoreKey is a score entry's level label.
func scoreKey(p wireProb) *string { return p.Label }

// maxScaleValue bounds a score level's number, which counts positions in a
// list of at most 32 levels and so never needs to be large.
const maxScaleValue = 1 << 20

// sumTolerance matches the decision schema's own rule, so a distribution this
// plugin accepts is one the schema accepts, and the error says which rule.
const sumTolerance = 1e-3

// answersFromReply turns the API's answers into one validated
// flowstate.decision.v1.Answer per question, in the question set's order, all
// CALIBRATION_MODEL_PROBABILITY.
//
// It is strict in every direction a provider can be sloppy, and a refusal is
// not an answer: a refused question, a missing, extra or repeated name, a type
// that is not the question's kind, a value the question did not offer, a
// probability outside 0 to 1 or distribution that does not sum to 1 or leaves
// the offered set all fail the whole call. Nothing is repaired and nothing is
// returned partially. Each answer is then validated as a flowstate.decision.v1.Decision
// with its question, which checks the neutral contract independently.
//
// No text from the reply is repeated in an error except a question name this
// request itself sent.
func answersFromReply(set *decisionv1.QuestionSet, wire []wireAnswer) ([]*decisionv1.Answer, error) {
	if len(wire) > len(set.GetQuestions()) {
		return nil, sdk.Failed("OpenAI's response carried more answers than there were questions")
	}

	byName := make(map[string]*wireAnswer, len(wire))
	for i := range wire {
		if _, repeated := byName[wire[i].Name]; repeated {
			return nil, sdk.Failed("OpenAI's response answered one question more than once")
		}
		byName[wire[i].Name] = &wire[i]
	}
	asked := make(map[string]bool, len(set.GetQuestions()))
	for _, q := range set.GetQuestions() {
		asked[q.GetName()] = true
	}
	for name := range byName {
		if !asked[name] {
			return nil, sdk.Failed("OpenAI's response answered a question that was not asked")
		}
	}

	answers := make([]*decisionv1.Answer, 0, len(set.GetQuestions()))
	for _, q := range set.GetQuestions() {
		w := byName[q.GetName()]
		if w == nil {
			return nil, sdk.Failed("OpenAI's response did not answer question %q", q.GetName())
		}
		if w.Type == kindRefusal {
			return nil, sdk.Failed("OpenAI refused question %q", q.GetName())
		}

		a := &decisionv1.Answer{Name: q.GetName(), Calibration: decisionv1.Calibration_CALIBRATION_MODEL_PROBABILITY}
		var err error
		switch kind := q.GetKind().(type) {
		case *decisionv1.Question_Predicate_:
			err = predicateAnswer(a, w)
		case *decisionv1.Question_Choice_:
			err = choiceAnswer(a, w, kind.Choice.GetOptions())
		case *decisionv1.Question_Score_:
			err = scoreAnswer(a, w, kind.Score.GetLevels())
		}
		if err != nil {
			return nil, sdk.Failed("OpenAI's answer to question %q was refused: %v", q.GetName(), err)
		}

		decision := &decisionv1.Decision{Question: q, Answer: a}
		if err := flowstatev1.Validate(decision); err != nil {
			return nil, sdk.Failed("OpenAI's answer to question %q does not satisfy the decision schema: %s", q.GetName(), violations(err))
		}
		answers = append(answers, a)
	}
	return answers, nil
}

// reason is a reason that is a fixed phrase of this package's, never reply text.
type reason string

func (r reason) Error() string { return string(r) }

// predicateAnswer maps a probability onto the neutral bool: true at 0.5 and
// above, with the distribution that probability implies and no confidence,
// because the API gives none beyond the probability itself.
func predicateAnswer(a *decisionv1.Answer, w *wireAnswer) error {
	if w.Type != kindPredicate {
		return reason("its type is not predicate")
	}
	if w.Probability == nil || !unit(*w.Probability) {
		return reason("its probability is missing or outside 0 to 1")
	}
	if w.Choice != nil || w.Score != nil || w.Confidence != nil || len(w.Probabilities) != 0 {
		return reason("it carries fields a predicate does not have")
	}
	p := *w.Probability
	a.Result = &decisionv1.Answer_Predicate{Predicate: p >= 0.5}
	a.Distribution = map[string]float64{"true": p, "false": 1 - p}
	return nil
}

func choiceAnswer(a *decisionv1.Answer, w *wireAnswer, options []string) error {
	if w.Type != kindChoice {
		return reason("its type is not choice")
	}
	if w.Probability != nil || w.Score != nil {
		return reason("it carries fields a choice does not have")
	}
	if w.Choice == nil || !slices.Contains(options, *w.Choice) {
		return reason("its choice is missing or not an offered option")
	}
	if w.Confidence == nil || !unit(*w.Confidence) {
		return reason("its confidence is missing or outside 0 to 1")
	}
	distribution, err := distributionOver(options, w.Probabilities, choiceKey)
	if err != nil {
		return err
	}
	a.Result = &decisionv1.Answer_Choice{Choice: *w.Choice}
	a.Confidence = w.Confidence
	a.Distribution = distribution
	return nil
}

// scoreAnswer selects the level the API put the most probability on. The API's
// own `score` is a probability-weighted average over the levels, which is not a
// level, so it is checked for sanity and not carried: the neutral answer
// selects one. The first of equally likely levels, the lowest, is taken so the
// same reply always selects the same level.
func scoreAnswer(a *decisionv1.Answer, w *wireAnswer, levels []string) error {
	if w.Type != kindScore {
		return reason("its type is not score")
	}
	if w.Probability != nil || w.Choice != nil {
		return reason("it carries fields a score does not have")
	}
	if w.Score == nil || math.IsNaN(*w.Score) || math.IsInf(*w.Score, 0) {
		return reason("its score is missing or not a number")
	}
	if w.Confidence == nil || !unit(*w.Confidence) {
		return reason("its confidence is missing or outside 0 to 1")
	}
	distribution, err := distributionOver(levels, w.Probabilities, scoreKey)
	if err != nil {
		return err
	}
	// The average is taken over the levels' own numbers, so it can lie nowhere
	// outside them. Whether the API counts from 0 or 1 is not something to
	// guess, so the scale is read from the reply: each level's value must be an
	// integer one more than the level before it in the author's order.
	lo, err := scoreScale(levels, w.Probabilities)
	if err != nil {
		return err
	}
	if *w.Score < lo || *w.Score > lo+float64(len(levels)-1) {
		return reason("its score is outside the scale of its levels")
	}

	selected := levels[0]
	for _, level := range levels {
		if distribution[level] > distribution[selected] {
			selected = level
		}
	}
	a.Result = &decisionv1.Answer_Score{Score: selected}
	a.Confidence = w.Confidence
	a.Distribution = distribution
	return nil
}

// scoreScale returns the number the API gave the first level, after checking
// that every level carries an integer `value` and that the values count up by
// one in the order the levels were offered. A missing, fractional, repeated or
// out-of-order value means the reply does not describe the scale that was
// asked about. distributionOver has already established that each label is an
// offered level, once.
func scoreScale(levels []string, probabilities []wireProb) (float64, error) {
	byLabel := make(map[string]float64, len(levels))
	for _, p := range probabilities {
		// null decodes into a float64 without error and without setting it, so
		// it is refused by name rather than read as 0.
		var v float64
		if string(p.Value) == "null" || json.Unmarshal(p.Value, &v) != nil {
			return 0, reason("a level has no numeric value")
		}
		// Past 2^53 adding one to a float64 changes nothing, so "counts up by
		// one" would hold for any value; no real scale is anywhere near that.
		if v != math.Trunc(v) || math.Abs(v) > maxScaleValue {
			return 0, reason("a level value is not a small integer")
		}
		byLabel[*p.Label] = v
	}
	lo := byLabel[levels[0]]
	for i, level := range levels {
		if byLabel[level] != lo+float64(i) {
			return 0, reason("its level values do not count up in the order offered")
		}
	}
	return lo, nil
}

// distributionOver keys probabilities by the offered strings. Every offered
// string must appear exactly once, none may appear that was not offered, each
// probability must be in range, and the total must be 1 within the schema's
// tolerance.
func distributionOver(offered []string, probabilities []wireProb, key func(wireProb) *string) (map[string]float64, error) {
	if len(probabilities) != len(offered) {
		return nil, reason("its probabilities do not cover exactly the offered values")
	}
	distribution := make(map[string]float64, len(offered))
	var sum float64
	for _, p := range probabilities {
		k := key(p)
		if k == nil || !slices.Contains(offered, *k) {
			return nil, reason("a probability names a value that was not offered")
		}
		if _, repeated := distribution[*k]; repeated {
			return nil, reason("a value has more than one probability")
		}
		if p.Probability == nil || !unit(*p.Probability) {
			return nil, reason("a probability is missing or outside 0 to 1")
		}
		distribution[*k] = *p.Probability
		sum += *p.Probability
	}
	if sum <= 1-sumTolerance || sum >= 1+sumTolerance {
		return nil, reason("its probabilities do not sum to 1")
	}
	return distribution, nil
}

// unit reports whether v is a finite number from 0 to 1.
func unit(v float64) bool {
	return !math.IsNaN(v) && v >= 0 && v <= 1
}
