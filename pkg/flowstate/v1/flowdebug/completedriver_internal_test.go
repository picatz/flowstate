package flowdebug

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// countingTarget answers a held run's snapshot and a fixed scope, counting the
// inspections it is asked.
type countingTarget struct {
	Target

	revision uint64
	inspects int
	children []*v1.DebugVariable

	// refuse, when set, is the error every inspection answers with: the
	// response-level refusal a historical target gives for a stale read.
	refuse string
}

func (c *countingTarget) Snapshot(context.Context) (*v1.DebugSnapshot, error) {
	return &v1.DebugSnapshot{Revision: c.revision}, nil
}

func (c *countingTarget) Inspect(_ context.Context, req *v1.DebugInspectRequest) (*v1.DebugInspectResponse, error) {
	c.inspects++
	if c.refuse != "" {
		return &v1.DebugInspectResponse{Error: c.refuse}, nil
	}
	if req.GetExpression() == "" {
		return &v1.DebugInspectResponse{Children: []*v1.DebugVariable{{
			Name: "inputs", Value: &v1.DebugValue{Expression: "scope:inputs"},
		}}}, nil
	}

	return &v1.DebugInspectResponse{Children: c.children, Total: int32(len(c.children))}, nil
}

// TestADriverNeverOffersAWithheldName: a name the target's redactor withheld
// arrives as the marker, which is no name an expression can use. It is dropped,
// not inserted into somebody's expression, and so is any name that would need
// quoting.
func TestADriverNeverOffersAWithheldName(t *testing.T) {
	t.Parallel()

	target := &countingTarget{revision: 1, children: []*v1.DebugVariable{
		{Name: "region", Value: &v1.DebugValue{Expression: "inputs.region"}},
		{Name: v1.SensitiveMarker, Value: &v1.DebugValue{Expression: "inputs.[redacted]"}},
		{Name: "needs quoting", Value: &v1.DebugValue{Expression: `inputs["needs quoting"]`}},
		{Name: "9starts", Value: &v1.DebugValue{Expression: `inputs["9starts"]`}},
	}}

	answer, err := NewDriver(target).Complete(t.Context(), "inspect inputs.")
	require.NoError(t, err)

	texts := make([]string, 0, len(answer.Candidates))
	for _, c := range answer.Candidates {
		texts = append(texts, c.Text)
	}
	assert.Equal(t, []string{"region"}, texts)
}

// TestARootListingIsReadOncePerRevision: a person pressing tab twice at one stop
// asks one question, and a durable target answers each inspection with a round
// trip to a worker. The cache is the revision's, so the next stop asks again.
func TestARootListingIsReadOncePerRevision(t *testing.T) {
	t.Parallel()

	target := &countingTarget{revision: 1, children: []*v1.DebugVariable{
		{Name: "region", Value: &v1.DebugValue{Expression: "inputs.region"}},
	}}
	driver := NewDriver(target)

	_, err := driver.Complete(t.Context(), "inspect i")
	require.NoError(t, err)
	first := target.inspects
	require.Positive(t, first)

	_, err = driver.Complete(t.Context(), "inspect in")
	require.NoError(t, err)
	assert.Equal(t, first, target.inspects, "the second tab at one revision asked the target again")

	target.revision = 2
	_, err = driver.Complete(t.Context(), "inspect in")
	require.NoError(t, err)
	assert.Greater(t, target.inspects, first, "a new revision reused the old stop's roots")
}

// TestARefusedRootListingIsNotRemembered: a target that answers an inspection
// with an error in the response, and not a failed call, has refused; what was
// read before the refusal is offered but is not the stop's answer, so the next
// tab at the same revision asks again instead of being told the refusal's
// emptiness for the rest of the stop.
func TestARefusedRootListingIsNotRemembered(t *testing.T) {
	t.Parallel()

	target := &countingTarget{revision: 1, refuse: "the read is stale", children: []*v1.DebugVariable{
		{Name: "region", Value: &v1.DebugValue{Expression: "inputs.region"}},
	}}
	driver := NewDriver(target)

	answer, err := driver.CompleteExpression(t.Context(), "i")
	require.NoError(t, err)
	assert.Empty(t, answer.Candidates, "a refused listing offered a scope root it did not read")

	target.refuse = ""
	answer, err = driver.CompleteExpression(t.Context(), "i")
	require.NoError(t, err)
	require.Len(t, answer.Candidates, 1, "the refusal was remembered for the revision")
	assert.Equal(t, "inputs", answer.Candidates[0].Text[:6])
}

// TestADriverNeverOffersANameThatIsAnEscapeSequence: an attached run's step and
// breakpoint ids are its server's to name, and a completion list is written to
// the terminal, so an id carrying a control character is not offered.
func TestADriverNeverOffersANameThatIsAnEscapeSequence(t *testing.T) {
	t.Parallel()

	offered := offerNamesFrom("", []string{"price", "\x1b[2Jevil", "bell\a", "each/touch"}, "a step")

	texts := make([]string, 0, len(offered.Candidates))
	for _, c := range offered.Candidates {
		texts = append(texts, c.Text)
	}
	assert.Equal(t, []string{"price", "each/touch"}, texts)
}
