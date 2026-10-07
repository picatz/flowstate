package main

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"

	decisionv1 "github.com/picatz/flowstate/pkg/flowstate/decision/v1"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// parseQuestionSet reads the author's mapping as a flowstate.decision.v1.QuestionSet and
// validates it against the schema's own rules, so a set the contract would call
// malformed never becomes a request.
//
// The conversion goes through protojson with unknown fields refused, which is
// what makes a misspelt `options` an error rather than a question with no
// options that the validator then has to describe.
func parseQuestionSet(v *flowstatev1.Value) (*decisionv1.QuestionSet, error) {
	literal, ok := v.GetKind().(*flowstatev1.Value_Literal)
	if !ok {
		return nil, sdk.InvalidInput("question_set is required and must be a mapping with a questions list")
	}
	native, err := flowstatev1.LiteralToGo(literal.Literal)
	if err != nil {
		return nil, sdk.InvalidInput("question_set is not plain data: %v", err)
	}
	encoded, err := json.Marshal(native)
	if err != nil {
		return nil, sdk.InvalidInput("question_set is not plain data: %v", err)
	}

	var set decisionv1.QuestionSet
	if err := (protojson.UnmarshalOptions{}).Unmarshal(encoded, &set); err != nil {
		return nil, sdk.InvalidInput("question_set is not a flowstate.decision.v1.QuestionSet: %s", bounded(err.Error()))
	}
	if err := flowstatev1.Validate(&set); err != nil {
		return nil, sdk.InvalidInput("question_set violates the decision schema: %s", violations(err))
	}
	return &set, nil
}

// violations names the rules a validation error broke and the fields they
// belong to, and nothing else. A violation's message can quote a value, and a
// value here may be something a model wrote, so only the rule identifiers and
// field paths are reported.
func violations(err error) string {
	var invalid *flowstatev1.ValidationError
	if !errors.As(err, &invalid) {
		return "the decision schema could not be evaluated"
	}
	parts := make([]string, 0, len(invalid.Violations))
	for _, v := range invalid.Violations {
		parts = append(parts, strings.TrimSpace(v.Field+" "+v.Rule))
	}
	return bounded(strings.Join(slices.Compact(parts), "; "))
}

// decisionsRequest is the body of POST /v1/decisions.
type decisionsRequest struct {
	Model     string            `json:"model"`
	Input     string            `json:"input"`
	Questions []requestQuestion `json:"questions"`
}

// requestQuestion is one entry of `questions`; which of Choices and Levels is
// present follows Type.
type requestQuestion struct {
	Type         string          `json:"type"`
	Name         string          `json:"name"`
	Instructions string          `json:"instructions"`
	Choices      []requestChoice `json:"choices,omitempty"`
	Levels       []requestLevel  `json:"levels,omitempty"`
}

// requestChoice and requestLevel carry an option's description. The neutral
// question has none, so it is always sent empty rather than invented.
type requestChoice struct {
	Value       string `json:"value"`
	Description string `json:"description"`
}

type requestLevel struct {
	Label       string `json:"label"`
	Description string `json:"description"`
}

// requestQuestions maps the neutral question set onto the API's question
// entries, in the author's order. The vendor's shape is the neutral shape plus
// per-option descriptions, so nothing is renamed or reordered.
func requestQuestions(set *decisionv1.QuestionSet) []requestQuestion {
	questions := make([]requestQuestion, 0, len(set.GetQuestions()))
	for _, q := range set.GetQuestions() {
		entry := requestQuestion{Name: q.GetName(), Instructions: q.GetInstructions()}
		switch kind := q.GetKind().(type) {
		case *decisionv1.Question_Predicate_:
			entry.Type = kindPredicate
		case *decisionv1.Question_Choice_:
			entry.Type = kindChoice
			for _, option := range kind.Choice.GetOptions() {
				entry.Choices = append(entry.Choices, requestChoice{Value: option})
			}
		case *decisionv1.Question_Score_:
			entry.Type = kindScore
			for _, level := range kind.Score.GetLevels() {
				entry.Levels = append(entry.Levels, requestLevel{Label: level})
			}
		}
		questions = append(questions, entry)
	}
	return questions
}
