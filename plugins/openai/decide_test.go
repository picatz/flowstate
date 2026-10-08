package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	decisionv1 "github.com/picatz/flowstate/pkg/flowstate/decision/v1"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	openaiv1 "github.com/picatz/flowstate/plugins/openai/gen/openai/v1"
)

const testKey = "sk-not-a-real-key-0123456789"

// TestMain gives this test binary the grant a worker gives a launched plugin,
// and installs it exactly as main does. The grant is captured once per process.
func TestMain(m *testing.M) {
	if err := os.Setenv(sdk.EgressPolicyEnv,
		base64.StdEncoding.EncodeToString([]byte("deployment_default: true\negress: {}\n"))); err != nil {
		panic(err)
	}

	installEgressPolicy()

	os.Exit(m.Run())
}

// loopbackClient is a governed client that may reach the test server, which the
// default policy would refuse.
func loopbackClient(t *testing.T) *http.Client {
	t.Helper()

	policy, err := netpolicy.New(
		netpolicy.WithAllowLoopback(),
		netpolicy.WithMaxResponseBytes(8<<20),
		netpolicy.WithTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("building test egress policy: %v", err)
	}
	return policy.Client()
}

func questionSet(t *testing.T) *decisionv1.QuestionSet {
	t.Helper()

	set, err := parseQuestionSet(flowstatev1.NewValue(map[string]any{
		"questions": []any{
			map[string]any{
				"name":         "category",
				"instructions": "Which team owns this ticket?",
				"choice":       map[string]any{"options": []any{"billing", "outage", "other"}},
			},
			map[string]any{"name": "urgent", "instructions": "Is it urgent?", "predicate": map[string]any{}},
			map[string]any{
				"name":  "severity",
				"score": map[string]any{"levels": []any{"low", "medium", "high"}},
			},
		},
	}))
	if err != nil {
		t.Fatalf("parseQuestionSet: %v", err)
	}
	return set
}

// reply wraps answers the way the API returns them.
func reply(answers ...string) string {
	return `{"id":"dec_1","model":"a-model","answers":[` + strings.Join(answers, ",") + `]}`
}

const (
	categoryAnswer = `{"type":"choice","name":"category","choice":"outage","confidence":0.7,` +
		`"probabilities":[{"value":"billing","probability":0.1},{"value":"outage","probability":0.7},{"value":"other","probability":0.2}]}`
	urgentAnswer   = `{"type":"predicate","name":"urgent","probability":0.9}`
	severityAnswer = `{"type":"score","name":"severity","score":1.1,"confidence":0.5,` +
		`"probabilities":[{"value":0,"label":"low","probability":0.2},{"value":1,"label":"medium","probability":0.5},{"value":2,"label":"high","probability":0.3}]}`
)

var goodReply = reply(categoryAnswer, urgentAnswer, severityAnswer)

// serve answers every request with status and body, recording the last request.
func serve(t *testing.T, status int, body string, got *atomic.Pointer[recorded]) string {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		if got != nil {
			got.Store(&recorded{method: r.Method, header: r.Header.Clone(), body: raw})
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(server.Close)
	return server.URL
}

type recorded struct {
	method string
	header http.Header
	body   []byte
}

func run(t *testing.T, url string) ([]*decisionv1.Answer, error) {
	t.Helper()
	return decide(context.Background(), loopbackClient(t), url, testKey, inputs(), questionSet(t))
}

func inputs() *openaiv1.DecideInputs {
	return &openaiv1.DecideInputs{Model: "a-model", Evidence: "The checkout page returns 500 for every customer."}
}

// TestEveryAnswerTypeIsMappedWithItsProbabilities is the contract: a predicate,
// a choice and a score each become a neutral answer that says its numbers are
// model probabilities, and the request that earned them is the vendor's shape.
func TestEveryAnswerTypeIsMappedWithItsProbabilities(t *testing.T) {
	var got atomic.Pointer[recorded]
	url := serve(t, 200, goodReply, &got)

	answers, err := run(t, url)
	if err != nil {
		t.Fatalf("decide: %v", err)
	}
	if len(answers) != 3 {
		t.Fatalf("got %d answers, want 3", len(answers))
	}
	for _, a := range answers {
		if a.GetCalibration() != decisionv1.Calibration_CALIBRATION_MODEL_PROBABILITY {
			t.Errorf("%s calibration = %v, want MODEL_PROBABILITY", a.GetName(), a.GetCalibration())
		}
	}

	choice := answers[0]
	if choice.GetName() != "category" || choice.GetChoice() != "outage" || choice.GetConfidence() != 0.7 {
		t.Errorf("choice = %v", choice)
	}
	wantDist(t, choice, map[string]float64{"billing": 0.1, "outage": 0.7, "other": 0.2})

	predicate := answers[1]
	if predicate.GetName() != "urgent" || predicate.GetPredicate() != true || predicate.GetResult() == nil {
		t.Errorf("predicate = %v", predicate)
	}
	if predicate.Confidence != nil {
		t.Errorf("a predicate carries a confidence %v the API did not give", predicate.GetConfidence())
	}
	wantDist(t, predicate, map[string]float64{"true": 0.9, "false": 0.1})

	// The API's weighted average of 1.1 is not a level; the level the API put
	// the most probability on is, and the average is not carried anywhere.
	score := answers[2]
	if score.GetName() != "severity" || score.GetScore() != "medium" || score.GetConfidence() != 0.5 {
		t.Errorf("score = %v", score)
	}
	wantDist(t, score, map[string]float64{"low": 0.2, "medium": 0.5, "high": 0.3})

	// The request is the contract with the provider.
	r := got.Load()
	if r == nil || r.method != http.MethodPost {
		t.Fatalf("request = %#v", r)
	}
	if r.header.Get("Authorization") != "Bearer "+testKey || r.header.Get("Content-Type") != "application/json" {
		t.Errorf("headers = %v", r.header)
	}
	if strings.Contains(string(r.body), testKey) {
		t.Error("the key was sent in the request body")
	}
	var sent struct {
		Model     string `json:"model"`
		Input     string `json:"input"`
		Questions []struct {
			Type         string `json:"type"`
			Name         string `json:"name"`
			Instructions string `json:"instructions"`
			Choices      []struct{ Value, Description string }
			Levels       []struct{ Label, Description string }
		} `json:"questions"`
	}
	if err := json.Unmarshal(r.body, &sent); err != nil {
		t.Fatalf("request is not JSON: %v", err)
	}
	if sent.Model != "a-model" || sent.Input != "The checkout page returns 500 for every customer." {
		t.Errorf("model/input = %q/%q", sent.Model, sent.Input)
	}
	if len(sent.Questions) != 3 {
		t.Fatalf("questions = %+v", sent.Questions)
	}
	if q := sent.Questions[0]; q.Type != "choice" || q.Name != "category" || q.Instructions != "Which team owns this ticket?" || len(q.Choices) != 3 ||
		q.Choices[0].Value != "billing" || q.Choices[2].Value != "other" || q.Choices[0].Description != "" {
		t.Errorf("choice question = %+v", q)
	}
	if q := sent.Questions[1]; q.Type != "predicate" || q.Name != "urgent" || len(q.Choices)+len(q.Levels) != 0 {
		t.Errorf("predicate question = %+v", q)
	}
	if q := sent.Questions[2]; q.Type != "score" || q.Name != "severity" || len(q.Levels) != 3 ||
		q.Levels[0].Label != "low" || q.Levels[2].Label != "high" || q.Levels[1].Description != "" {
		t.Errorf("score question = %+v", q)
	}
	// An option description is sent, and sent empty, rather than left out.
	if !strings.Contains(string(r.body), `{"value":"billing","description":""}`) ||
		!strings.Contains(string(r.body), `{"label":"low","description":""}`) {
		t.Errorf("descriptions are not sent empty: %s", r.body)
	}
}

func wantDist(t *testing.T, a *decisionv1.Answer, want map[string]float64) {
	t.Helper()
	got := a.GetDistribution()
	if len(got) != len(want) {
		t.Errorf("%s distribution = %v, want %v", a.GetName(), got, want)
		return
	}
	for k, w := range want {
		if math.Abs(got[k]-w) > 1e-12 {
			t.Errorf("%s distribution[%s] = %v, want %v", a.GetName(), k, got[k], w)
		}
	}
}

// TestAPredicateIsTrueFromHalfUp pins the threshold: 0.5 is true, as the
// design says, and the distribution is the probability and its complement.
func TestAPredicateIsTrueFromHalfUp(t *testing.T) {
	set, err := parseQuestionSet(flowstatev1.NewValue(map[string]any{
		"questions": []any{map[string]any{"name": "urgent", "predicate": map[string]any{}}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		p    string
		want bool
	}{{"0", false}, {"0.4999", false}, {"0.5", true}, {"0.5001", true}, {"1", true}} {
		t.Run(test.p, func(t *testing.T) {
			answers, err := answersFromReply(set, decodeAnswers(t, reply(`{"type":"predicate","name":"urgent","probability":`+test.p+`}`)))
			if err != nil {
				t.Fatal(err)
			}
			if got := answers[0].GetPredicate(); got != test.want {
				t.Errorf("probability %s -> %v, want %v", test.p, got, test.want)
			}
			if d := answers[0].GetDistribution(); len(d) != 2 || math.Abs(d["true"]+d["false"]-1) > 1e-12 {
				t.Errorf("distribution = %v", d)
			}
		})
	}
}

// TestAScoreTieTakesTheLowestLevel keeps the selected level a function of the
// reply alone.
func TestAScoreTieTakesTheLowestLevel(t *testing.T) {
	set, err := parseQuestionSet(flowstatev1.NewValue(map[string]any{
		"questions": []any{map[string]any{"name": "severity", "score": map[string]any{"levels": []any{"low", "medium", "high"}}}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	answers, err := answersFromReply(set, decodeAnswers(t, reply(
		`{"type":"score","name":"severity","score":1,"confidence":0.4,"probabilities":[`+
			`{"value":0,"label":"low","probability":0.25},{"value":1,"label":"medium","probability":0.25},{"value":2,"label":"high","probability":0.5}]}`)))
	if err != nil {
		t.Fatal(err)
	}
	if answers[0].GetScore() != "high" {
		t.Errorf("selected = %q, want the argmax", answers[0].GetScore())
	}
	answers, err = answersFromReply(set, decodeAnswers(t, reply(
		`{"type":"score","name":"severity","score":0.5,"confidence":0.4,"probabilities":[`+
			`{"value":2,"label":"high","probability":0.4},{"value":1,"label":"medium","probability":0.4},{"value":0,"label":"low","probability":0.2}]}`)))
	if err != nil {
		t.Fatal(err)
	}
	if answers[0].GetScore() != "medium" {
		t.Errorf("selected = %q, want the lowest of the tied levels", answers[0].GetScore())
	}
}

func decodeAnswers(t *testing.T, body string) []wireAnswer {
	t.Helper()
	var r decisionsResponse
	if err := json.Unmarshal([]byte(body), &r); err != nil {
		t.Fatalf("test reply is not JSON: %v", err)
	}
	return r.Answers
}

func TestMalformedAnswersFailClosed(t *testing.T) {
	dist := func(a, b, c string) string {
		return fmt.Sprintf(`[{"value":"billing","probability":%s},{"value":"outage","probability":%s},{"value":"other","probability":%s}]`, a, b, c)
	}
	choice := func(extra string) string {
		return `{"type":"choice","name":"category",` + extra + `}`
	}
	levels := `[{"value":0,"label":"low","probability":0.2},{"value":1,"label":"medium","probability":0.5},{"value":2,"label":"high","probability":0.3}]`

	for name, answers := range map[string][]string{
		"a refused question":                     {categoryAnswer, `{"type":"refusal","name":"urgent"}`, severityAnswer},
		"a refusal of the first":                 {`{"type":"refusal","name":"category"}`, urgentAnswer, severityAnswer},
		"a missing question":                     {categoryAnswer, urgentAnswer},
		"no answers":                             {},
		"an extra question":                      {categoryAnswer, urgentAnswer, severityAnswer, `{"type":"predicate","name":"extra","probability":0.5}`},
		"an unasked name in place of one":        {categoryAnswer, urgentAnswer, `{"type":"predicate","name":"other","probability":0.5}`},
		"a repeated name":                        {categoryAnswer, urgentAnswer, urgentAnswer},
		"a predicate answering a choice":         {`{"type":"predicate","name":"category","probability":0.9}`, urgentAnswer, severityAnswer},
		"a choice answering a predicate":         {categoryAnswer, strings.Replace(categoryAnswer, `"category"`, `"urgent"`, 1), severityAnswer},
		"a score answering a choice":             {strings.Replace(severityAnswer, `"severity"`, `"category"`, 1), urgentAnswer, severityAnswer},
		"an unknown type":                        {categoryAnswer, `{"type":"ranking","name":"urgent","probability":0.9}`, severityAnswer},
		"no type":                                {categoryAnswer, `{"name":"urgent","probability":0.9}`, severityAnswer},
		"a choice that was not offered":          {choice(`"choice":"refund","confidence":0.7,"probabilities":` + dist("0.1", "0.7", "0.2")), urgentAnswer, severityAnswer},
		"a choice in the wrong case":             {choice(`"choice":"Outage","confidence":0.7,"probabilities":` + dist("0.1", "0.7", "0.2")), urgentAnswer, severityAnswer},
		"no choice":                              {choice(`"confidence":0.7,"probabilities":` + dist("0.1", "0.7", "0.2")), urgentAnswer, severityAnswer},
		"a choice with no confidence":            {choice(`"choice":"outage","probabilities":` + dist("0.1", "0.7", "0.2")), urgentAnswer, severityAnswer},
		"a confidence above one":                 {choice(`"choice":"outage","confidence":1.1,"probabilities":` + dist("0.1", "0.7", "0.2")), urgentAnswer, severityAnswer},
		"a negative confidence":                  {choice(`"choice":"outage","confidence":-0.1,"probabilities":` + dist("0.1", "0.7", "0.2")), urgentAnswer, severityAnswer},
		"probabilities summing low":              {choice(`"choice":"outage","confidence":0.7,"probabilities":` + dist("0.1", "0.6", "0.2")), urgentAnswer, severityAnswer},
		"probabilities summing high":             {choice(`"choice":"outage","confidence":0.7,"probabilities":` + dist("0.3", "0.7", "0.2")), urgentAnswer, severityAnswer},
		"a negative probability":                 {choice(`"choice":"outage","confidence":0.7,"probabilities":` + dist("-0.2", "1.0", "0.2")), urgentAnswer, severityAnswer},
		"a probability above one":                {choice(`"choice":"outage","confidence":0.7,"probabilities":` + dist("-0.5", "1.5", "0")), urgentAnswer, severityAnswer},
		"a probability naming no option":         {choice(`"choice":"outage","confidence":0.7,"probabilities":[{"value":"billing","probability":0.1},{"value":"outage","probability":0.7},{"value":"refund","probability":0.2}]`), urgentAnswer, severityAnswer},
		"an option with no probability":          {choice(`"choice":"outage","confidence":0.7,"probabilities":[{"value":"billing","probability":0.3},{"value":"outage","probability":0.7}]`), urgentAnswer, severityAnswer},
		"a probability for a repeated one":       {choice(`"choice":"outage","confidence":0.7,"probabilities":[{"value":"billing","probability":0.1},{"value":"billing","probability":0.1},{"value":"outage","probability":0.8}]`), urgentAnswer, severityAnswer},
		"an extra probability":                   {choice(`"choice":"outage","confidence":0.7,"probabilities":[{"value":"billing","probability":0.1},{"value":"outage","probability":0.6},{"value":"other","probability":0.2},{"value":"x","probability":0.1}]`), urgentAnswer, severityAnswer},
		"a probability with no value":            {choice(`"choice":"outage","confidence":0.7,"probabilities":[{"probability":0.1},{"value":"outage","probability":0.7},{"value":"other","probability":0.2}]`), urgentAnswer, severityAnswer},
		"a probability with no number":           {choice(`"choice":"outage","confidence":0.7,"probabilities":[{"value":"billing"},{"value":"outage","probability":0.7},{"value":"other","probability":0.2}]`), urgentAnswer, severityAnswer},
		"a choice selected outside its own":      {choice(`"choice":"other","confidence":0.2,"probabilities":[{"value":"billing","probability":0.1},{"value":"outage","probability":0.9}]`), urgentAnswer, severityAnswer},
		"a predicate probability above one":      {categoryAnswer, `{"type":"predicate","name":"urgent","probability":1.5}`, severityAnswer},
		"a negative predicate probability":       {categoryAnswer, `{"type":"predicate","name":"urgent","probability":-0.5}`, severityAnswer},
		"a predicate with none":                  {categoryAnswer, `{"type":"predicate","name":"urgent"}`, severityAnswer},
		"a predicate with a null one":            {categoryAnswer, `{"type":"predicate","name":"urgent","probability":null}`, severityAnswer},
		"a predicate with a confidence":          {categoryAnswer, `{"type":"predicate","name":"urgent","probability":0.9,"confidence":0.9}`, severityAnswer},
		"a score with an unoffered label":        {categoryAnswer, urgentAnswer, `{"type":"score","name":"severity","score":1,"confidence":0.5,"probabilities":[{"value":0,"label":"low","probability":0.2},{"value":1,"label":"critical","probability":0.5},{"value":2,"label":"high","probability":0.3}]}`},
		"a score keyed by value alone":           {categoryAnswer, urgentAnswer, `{"type":"score","name":"severity","score":1,"confidence":0.5,"probabilities":[{"value":"low","probability":0.2},{"value":"medium","probability":0.5},{"value":"high","probability":0.3}]}`},
		"a score with no score":                  {categoryAnswer, urgentAnswer, `{"type":"score","name":"severity","confidence":0.5,"probabilities":` + levels + `}`},
		"a score level with no value":            {categoryAnswer, urgentAnswer, `{"type":"score","name":"severity","score":1,"confidence":0.5,"probabilities":[{"label":"low","probability":0.2},{"label":"medium","probability":0.5},{"label":"high","probability":0.3}]}`},
		"a score level value that is fractional": {categoryAnswer, urgentAnswer, `{"type":"score","name":"severity","score":1,"confidence":0.5,"probabilities":[{"value":0,"label":"low","probability":0.2},{"value":1.5,"label":"medium","probability":0.5},{"value":2,"label":"high","probability":0.3}]}`},
		"a score level value that is a string":   {categoryAnswer, urgentAnswer, `{"type":"score","name":"severity","score":1,"confidence":0.5,"probabilities":[{"value":0,"label":"low","probability":0.2},{"value":"1","label":"medium","probability":0.5},{"value":2,"label":"high","probability":0.3}]}`},
		"a score with a repeated level value":    {categoryAnswer, urgentAnswer, `{"type":"score","name":"severity","score":1,"confidence":0.5,"probabilities":[{"value":0,"label":"low","probability":0.2},{"value":0,"label":"medium","probability":0.5},{"value":2,"label":"high","probability":0.3}]}`},
		"a score with level values out of order": {categoryAnswer, urgentAnswer, `{"type":"score","name":"severity","score":1,"confidence":0.5,"probabilities":[{"value":0,"label":"low","probability":0.2},{"value":2,"label":"medium","probability":0.5},{"value":1,"label":"high","probability":0.3}]}`},
		"a score level value that is null":       {categoryAnswer, urgentAnswer, `{"type":"score","name":"severity","score":1,"confidence":0.5,"probabilities":[{"value":null,"label":"low","probability":0.2},{"value":1,"label":"medium","probability":0.5},{"value":2,"label":"high","probability":0.3}]}`},
		"a score on a scale too large to count":  {categoryAnswer, urgentAnswer, `{"type":"score","name":"severity","score":1e300,"confidence":0.5,"probabilities":[{"value":1e300,"label":"low","probability":0.2},{"value":1e300,"label":"medium","probability":0.5},{"value":1e300,"label":"high","probability":0.3}]}`},
		"a score one past the top of its scale":  {categoryAnswer, urgentAnswer, `{"type":"score","name":"severity","score":3,"confidence":0.5,"probabilities":[{"value":0,"label":"low","probability":0.2},{"value":1,"label":"medium","probability":0.5},{"value":2,"label":"high","probability":0.3}]}`},
		"a score off the scale":                  {categoryAnswer, urgentAnswer, `{"type":"score","name":"severity","score":9,"confidence":0.5,"probabilities":` + levels + `}`},
		"a negative score":                       {categoryAnswer, urgentAnswer, `{"type":"score","name":"severity","score":-1,"confidence":0.5,"probabilities":` + levels + `}`},
		"a score with no confidence":             {categoryAnswer, urgentAnswer, `{"type":"score","name":"severity","score":1,"probabilities":` + levels + `}`},
		"a score that does not sum":              {categoryAnswer, urgentAnswer, `{"type":"score","name":"severity","score":1,"confidence":0.5,"probabilities":[{"value":0,"label":"low","probability":0.2},{"value":1,"label":"medium","probability":0.5},{"value":2,"label":"high","probability":0.1}]}`},
	} {
		t.Run(name, func(t *testing.T) {
			url := serve(t, 200, reply(answers...), nil)
			got, err := run(t, url)
			if err == nil {
				t.Fatalf("a malformed reply was accepted: %v", got)
			}
			if !sdk.IsFailed(err) {
				t.Errorf("error = %v, want a typed Failed", err)
			}
			if got != nil {
				t.Errorf("a partial result came back with the error: %v", got)
			}
		})
	}
}

// TestAProbabilitySumIsHeldToTheSchemasTolerance accepts what rounding in a
// decimal reply produces and refuses what it cannot.
func TestAProbabilitySumIsHeldToTheSchemasTolerance(t *testing.T) {
	set, err := parseQuestionSet(flowstatev1.NewValue(map[string]any{
		"questions": []any{map[string]any{"name": "category", "choice": map[string]any{"options": []any{"a", "b"}}}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		a, b string
		ok   bool
	}{
		{"0.3333", "0.6667", true},
		{"0.3", "0.7004", true},
		{"0.3", "0.7015", false},
		{"0.3", "0.6985", false},
		{"0.5", "0.5", true},
	} {
		t.Run(test.a+"+"+test.b, func(t *testing.T) {
			_, err := answersFromReply(set, decodeAnswers(t, reply(fmt.Sprintf(
				`{"type":"choice","name":"category","choice":"a","confidence":0.5,"probabilities":[{"value":"a","probability":%s},{"value":"b","probability":%s}]}`,
				test.a, test.b))))
			if (err == nil) != test.ok {
				t.Fatalf("err = %v, want ok = %v", err, test.ok)
			}
		})
	}
}

func TestAnErrorNamesNoReplyText(t *testing.T) {
	url := serve(t, 200, reply(categoryAnswer, `{"type":"predicate","name":"leaky-name-from-the-reply","probability":0.5}`, severityAnswer), nil)
	_, err := run(t, url)
	if err == nil || strings.Contains(err.Error(), "leaky-name-from-the-reply") {
		t.Fatalf("error = %v, want one that does not repeat a name the request never sent", err)
	}
}

func TestNonJSONIsRefused(t *testing.T) {
	for name, test := range map[string]struct {
		status int
		check  func(error) bool
	}{
		"a 200":              {200, sdk.IsFailed},
		"a 502 from a proxy": {502, sdk.IsUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			url := serve(t, test.status, "<html>not json</html>", nil)
			if _, err := run(t, url); err == nil || !test.check(err) {
				t.Fatalf("error = %v", err)
			}
		})
	}
	// An object that is not a reply at all has no answers, which is a missing
	// answer for every question.
	if _, err := run(t, serve(t, 200, `{}`, nil)); err == nil || !sdk.IsFailed(err) {
		t.Fatalf("an empty object: %v", err)
	}
}

func TestAnOversizeBodyIsRefusedNotBuffered(t *testing.T) {
	url := serve(t, 200, goodReply+strings.Repeat(" ", maxResponseBytes), nil)
	_, err := run(t, url)
	if err == nil || !sdk.IsFailed(err) || !strings.Contains(err.Error(), "exceeded") {
		t.Fatalf("error = %v, want a typed refusal naming the size limit", err)
	}
}

func TestStatusesAreClassifiedForRetry(t *testing.T) {
	for _, test := range []struct {
		status int
		body   string
		check  func(error) bool
		want   string
	}{
		{401, `{"error":{"type":"invalid_request_error","code":"invalid_api_key","message":"Incorrect API key provided"}}`, sdk.IsPermissionDenied, "invalid_request_error"},
		{403, `{"error":{"type":"permission_error"}}`, sdk.IsPermissionDenied, "permission_error"},
		{400, `{"error":{"type":"invalid_request_error","code":null}}`, sdk.IsInvalidInput, "invalid_request_error"},
		{404, `{"error":{"type":"invalid_request_error","code":"model_not_found"}}`, sdk.IsInvalidInput, "invalid_request_error"},
		{429, `{"error":{"type":"rate_limit_error","code":"rate_limit_exceeded"}}`, sdk.IsUnavailable, "rate_limit_error"},
		{429, `{"error":{"type":"insufficient_quota","code":"insufficient_quota"}}`, sdk.IsPermissionDenied, "insufficient_quota"},
		{429, `{"error":{"type":"requests","code":"insufficient_quota"}}`, sdk.IsPermissionDenied, "insufficient_quota"},
		{500, `{"error":{"type":"server_error"}}`, sdk.IsUnavailable, "server_error"},
		{503, `{"error":{"type":"server_error"}}`, sdk.IsUnavailable, "server_error"},
		{418, `{}`, sdk.IsFailed, "unspecified_error"},
		{400, `{"error":"a string where an object belongs"}`, sdk.IsInvalidInput, "unspecified_error"},
	} {
		t.Run(fmt.Sprint(test.status, " ", test.want), func(t *testing.T) {
			url := serve(t, test.status, test.body, nil)
			_, err := run(t, url)
			if err == nil || !test.check(err) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %s", err, test.want)
			}
			if sdk.IsOutcomeUnknown(err) {
				t.Errorf("a received status is not an unknown outcome: %v", err)
			}
		})
	}
}

func TestRateLimitCarriesBoundedRetryAfter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "17")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(server.Close)

	_, err := run(t, server.URL)
	if err == nil || !strings.Contains(err.Error(), "retry after 17s") {
		t.Fatalf("error = %v, want a 17s hint", err)
	}
	if got := retryAfter("999999"); got != 5*time.Minute {
		t.Errorf("oversized Retry-After = %s, want the 5m bound", got)
	}
}

// TestTheKeyIsNeverInAnError proves the credential stays out of what a failure
// reports, whichever way the provider or the model tries to put it there: an
// error message that quotes the request, a model that repeats it as an answer,
// and a body that carries it in a field the plugin does not read.
func TestTheKeyIsNeverInAnError(t *testing.T) {
	quoting := fmt.Sprintf(`{"error":{"type":"invalid_request_error","code":"%s","message":"Incorrect API key provided: %s"}}`, testKey, testKey)
	for name, test := range map[string]struct {
		status int
		body   string
	}{
		"a 401 quoting the key":       {401, quoting},
		"a 400 quoting the key":       {400, quoting},
		"a 429 quoting the key":       {429, quoting},
		"a 500 quoting the key":       {500, quoting},
		"a 200 echoing it as a value": {200, reply(`{"type":"choice","name":"category","choice":"`+testKey+`","confidence":0.5,"probabilities":[]}`, urgentAnswer, severityAnswer)},
		"a 200 echoing it as a name":  {200, reply(`{"type":"predicate","name":"` + testKey + `","probability":0.5}`)},
		"a 200 echoing it as a type":  {200, reply(`{"type":"`+testKey+`","name":"category"}`, urgentAnswer, severityAnswer)},
		"a 200 echoing it as a label": {200, reply(categoryAnswer, urgentAnswer, `{"type":"score","name":"severity","score":1,"confidence":0.5,"probabilities":[{"label":"`+testKey+`","probability":1}]}`)},
		"a body that is the key":      {200, testKey},
	} {
		t.Run(name, func(t *testing.T) {
			url := serve(t, test.status, test.body, nil)
			_, err := run(t, url)
			if err == nil {
				t.Fatal("expected a failure")
			}
			if strings.Contains(err.Error(), testKey) {
				t.Fatalf("the key is in the error: %v", err)
			}
		})
	}
}

func TestADeniedDestinationIsNotDialed(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	t.Cleanup(server.Close)

	policy, err := netpolicy.New(netpolicy.WithMaxResponseBytes(64 << 10))
	if err != nil {
		t.Fatalf("building deny-by-default test policy: %v", err)
	}
	_, err = decide(context.Background(), policy.Client(), server.URL, testKey, inputs(), questionSet(t))
	if err == nil || !sdk.IsPermissionDenied(err) || !strings.Contains(err.Error(), "egress policy denied") {
		t.Fatalf("error = %v, want an egress-policy refusal", err)
	}
	if requests.Load() != 0 {
		t.Fatalf("the denied listener received %d request(s)", requests.Load())
	}
}

func TestAnUnreachableProviderIsRetryable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := server.URL
	server.Close()

	if _, err := run(t, url); err == nil || !sdk.IsUnavailable(err) {
		t.Fatalf("error = %v, want a retryable Unavailable", err)
	}
}

// TestALostConnectionAfterTheRequestWasSentIsAnUnknownOutcome is the other side
// of the unreachable-provider test: once the request has been written, a reset
// or a timeout does not prove the provider did not process, and bill, the call,
// so it is not retried automatically.
func TestALostConnectionAfterTheRequestWasSentIsAnUnknownOutcome(t *testing.T) {
	reset := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}))
	t.Cleanup(reset.Close)

	release := make(chan struct{})
	hang := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	t.Cleanup(hang.Close)
	t.Cleanup(func() { close(release) })

	policy, err := netpolicy.New(netpolicy.WithAllowLoopback(), netpolicy.WithTimeout(300*time.Millisecond))
	if err != nil {
		t.Fatalf("building test egress policy: %v", err)
	}

	for name, url := range map[string]string{"a reset": reset.URL, "a timeout": hang.URL} {
		t.Run(name, func(t *testing.T) {
			_, err := decide(context.Background(), policy.Client(), url, testKey, inputs(), questionSet(t))
			if err == nil || !sdk.IsOutcomeUnknown(err) || sdk.IsUnavailable(err) {
				t.Fatalf("error = %v, want OutcomeUnknown and not retryable", err)
			}
		})
	}
}

func TestTransportFailuresAreClassifiedByWhetherTheRequestWasSent(t *testing.T) {
	failure := io.ErrUnexpectedEOF
	if err := classifyTransportError(failure, false); !sdk.IsUnavailable(err) {
		t.Errorf("before the request was sent: %v, want retryable", err)
	}
	if err := classifyTransportError(failure, true); !sdk.IsOutcomeUnknown(err) {
		t.Errorf("after the request was sent: %v, want OutcomeUnknown", err)
	}
	if err := classifyTransportError(context.DeadlineExceeded, true); !sdk.IsOutcomeUnknown(err) {
		t.Errorf("a timeout after the request was sent: %v, want OutcomeUnknown", err)
	}
}

func TestAQuestionSetThatBreaksTheSchemaIsRefusedBeforeARequest(t *testing.T) {
	for name, set := range map[string]map[string]any{
		"no questions": {"questions": []any{}},
		"duplicate names": {"questions": []any{
			map[string]any{"name": "a", "predicate": map[string]any{}},
			map[string]any{"name": "a", "predicate": map[string]any{}},
		}},
		"no kind":            {"questions": []any{map[string]any{"name": "a"}}},
		"two kinds":          {"questions": []any{map[string]any{"name": "a", "predicate": map[string]any{}, "choice": map[string]any{"options": []any{"x"}}}}},
		"a choice with none": {"questions": []any{map[string]any{"name": "a", "choice": map[string]any{"options": []any{}}}}},
		"a repeated option":  {"questions": []any{map[string]any{"name": "a", "choice": map[string]any{"options": []any{"x", "x"}}}}},
		"an unknown field":   {"questions": []any{map[string]any{"name": "a", "predicate": map[string]any{}, "optionz": 1}}},
		"a bad name":         {"questions": []any{map[string]any{"name": "has space", "predicate": map[string]any{}}}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseQuestionSet(flowstatev1.NewValue(set)); err == nil || !sdk.IsInvalidInput(err) {
				t.Fatalf("error = %v, want InvalidInput", err)
			}
		})
	}

	if _, err := parseQuestionSet(nil); err == nil || !sdk.IsInvalidInput(err) {
		t.Fatalf("a missing question_set: %v", err)
	}
}

func TestInputsAreBoundedBeforeAnyRequest(t *testing.T) {
	set := &decisionv1.QuestionSet{Questions: []*decisionv1.Question{{Name: "a", Kind: &decisionv1.Question_Predicate_{Predicate: &decisionv1.Question_Predicate{}}}}}
	valid := func() *openaiv1.DecideInputs {
		return &openaiv1.DecideInputs{Model: "a-model", Evidence: "text", QuestionSet: set}
	}
	if _, err := validateInputs(valid()); err != nil {
		t.Fatalf("a valid input was refused: %v", err)
	}
	for name, mutate := range map[string]func(*openaiv1.DecideInputs){
		"no model":             func(in *openaiv1.DecideInputs) { in.Model = "" },
		"a model with a space": func(in *openaiv1.DecideInputs) { in.Model = "a model" },
		"a very long model":    func(in *openaiv1.DecideInputs) { in.Model = strings.Repeat("m", maxModelBytes+1) },
		"no evidence":          func(in *openaiv1.DecideInputs) { in.Evidence = "" },
		"huge evidence":        func(in *openaiv1.DecideInputs) { in.Evidence = strings.Repeat("x", maxEvidenceBytes+1) },
		"invalid UTF-8":        func(in *openaiv1.DecideInputs) { in.Evidence = "\xff" },
	} {
		t.Run(name, func(t *testing.T) {
			in := valid()
			mutate(in)
			if _, err := validateInputs(in); err == nil || !sdk.IsInvalidInput(err) {
				t.Fatalf("error = %v, want InvalidInput", err)
			}
		})
	}
}

func TestTheKeyMustHaveBeenResolvedByTheHost(t *testing.T) {
	if got, err := keyFromValue(flowstatev1.NewValue(testKey)); err != nil || got != testKey {
		t.Fatalf("a resolved key = %q, %v", got, err)
	}
	unresolved := &flowstatev1.Value{Kind: &flowstatev1.Value_SecretRef{SecretRef: &flowstatev1.SecretRef{Scheme: "env", Name: "K"}}}
	for name, v := range map[string]*flowstatev1.Value{
		"absent":     nil,
		"empty":      flowstatev1.NewValue(""),
		"a number":   flowstatev1.NewValue(int64(7)),
		"unresolved": unresolved,
		"a newline":  flowstatev1.NewValue("sk\nx-injected: 1"),
		"a tab":      flowstatev1.NewValue("sk\tx"),
		"a delete":   flowstatev1.NewValue("sk\x7fx"),
		"too long":   flowstatev1.NewValue(strings.Repeat("k", maxKeyBytes+1)),
	} {
		t.Run(name, func(t *testing.T) {
			got, err := keyFromValue(v)
			if err == nil || got != "" {
				t.Fatalf("keyFromValue = %q, %v, want a refusal", got, err)
			}
		})
	}
}

func TestTheTaskRefusesWithoutAnEgressGrant(t *testing.T) {
	old := egressPolicy
	egressPolicy = nil
	t.Cleanup(func() { egressPolicy = old })

	_, err := openaiDecide(context.Background(), map[string]*flowstatev1.Value{"api_key": flowstatev1.NewValue(testKey)}, nil)
	if err == nil || !sdk.IsPermissionDenied(err) || strings.Contains(err.Error(), testKey) {
		t.Fatalf("error = %v, want a permission refusal before any input is read", err)
	}
}

// TestTheDeploymentDefaultIsAcceptedAsTheGrant is this plugin's posture toward
// a policy no operator wrote (#1332): the Decisions API is an HTTPS POST to a
// public host, which the default permits, so refusing it would make installing
// the plugin require writing a policy file first.
func TestTheDeploymentDefaultIsAcceptedAsTheGrant(t *testing.T) {
	isDefault, err := sdk.EgressPolicyIsDeploymentDefault()
	if err != nil || !isDefault {
		t.Fatalf("this test binary did not receive the deployment default: %v, %v", isDefault, err)
	}
	if egressPolicy == nil {
		t.Fatalf("the deployment default was refused: %v", egressRefusal)
	}
}

// TestTheReasonIsTheMechanismThatRefused pins that a refusal and a bad sum are
// caught by this plugin's own checks, not only by the schema behind them, so a
// regression in either is a failing test and not a different error.
func TestTheReasonIsTheMechanismThatRefused(t *testing.T) {
	for name, test := range map[string]struct {
		answers []string
		want    string
	}{
		"a refusal": {
			[]string{categoryAnswer, `{"type":"refusal","name":"urgent"}`, severityAnswer},
			`OpenAI refused question "urgent"`,
		},
		"a sum": {
			[]string{strings.Replace(categoryAnswer, `"probability":0.7`, `"probability":0.6`, 1), urgentAnswer, severityAnswer},
			"do not sum to 1",
		},
		"a mismatched type": {
			[]string{categoryAnswer, `{"type":"choice","name":"urgent","choice":"x","confidence":1,"probabilities":[]}`, severityAnswer},
			"its type is not predicate",
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := run(t, serve(t, 200, reply(test.answers...), nil))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want one saying %q", err, test.want)
			}
		})
	}
}

// A scale the API counts from 1 is read from the reply, not guessed: the same
// levels with values 1, 2, 3 are accepted with a score inside that range and
// refused one past it.
func TestAScoreScaleIsReadFromTheReply(t *testing.T) {
	oneBased := func(score string) string {
		return `{"type":"score","name":"severity","score":` + score + `,"confidence":0.5,"probabilities":[` +
			`{"value":1,"label":"low","probability":0.2},{"value":2,"label":"medium","probability":0.5},{"value":3,"label":"high","probability":0.3}]}`
	}
	set := questionSet(t)
	for score, ok := range map[string]bool{"2.1": true, "3": true, "0.9": false, "3.5": false} {
		var wire struct {
			Answers []wireAnswer `json:"answers"`
		}
		body := reply(categoryAnswer, urgentAnswer, oneBased(score))
		if err := json.Unmarshal([]byte(body), &wire); err != nil {
			t.Fatalf("score %s: %v", score, err)
		}
		_, err := answersFromReply(set, wire.Answers)
		if (err == nil) != ok {
			t.Errorf("score %s over values 1 to 3: err = %v, want accepted = %v", score, err, ok)
		}
	}
}

func TestBoundedNeverSplitsARune(t *testing.T) {
	// An odd number of single-byte characters in front makes the byte cap fall
	// in the middle of a two-byte rune, which is the case being guarded.
	got := bounded(strings.Repeat("a", maxErrorBytes-1) + "éé")
	if want := strings.Repeat("a", maxErrorBytes-1) + "…"; got != want {
		t.Fatalf("bounded = %q, want the whole runes under the cap and the marker", got)
	}
	if !utf8.ValidString(got) {
		t.Fatalf("bounded produced invalid UTF-8: %q", got)
	}
	if len(got) > maxErrorBytes+len("…") {
		t.Errorf("bounded is %d bytes, over the cap", len(got))
	}
}

// parseQuestionSet is the author's mapping as the plugin receives it: filled
// into the typed input by the SDK, the same routine the host runs, and then
// checked by the plugin.
func parseQuestionSet(v *flowstatev1.Value) (*decisionv1.QuestionSet, error) {
	var in openaiv1.DecideInputs
	if err := sdk.DecodeInputs(map[string]*flowstatev1.Value{"question_set": v}, &in); err != nil {
		return nil, err
	}
	return checkQuestionSet(in.GetQuestionSet())
}
