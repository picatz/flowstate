package flowstatev1

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func failWorkflow(declared []string, fail *Fail) *Workflow {
	wf := &Workflow{Name: "w", Steps: []*Node{{Id: "refuse", Kind: &Node_Fail{Fail: fail}}}}
	for _, name := range declared {
		wf.DeclaredErrors = append(wf.DeclaredErrors, &ErrorDeclaration{Name: name})
	}

	return wf
}

func TestCheckErrorDeclarations(t *testing.T) {
	t.Parallel()

	require.NoError(t, CheckErrorDeclarations(failWorkflow([]string{"Refused"}, &Fail{Error: "Refused"})))

	var undeclared *UndeclaredFailError
	err := CheckErrorDeclarations(failWorkflow([]string{"Refused"}, &Fail{Error: "Refusd"}))
	require.ErrorAs(t, err, &undeclared)
	require.Equal(t, "refuse", undeclared.Step)
	require.Equal(t, "Refusd", undeclared.Name)

	require.ErrorContains(t, CheckErrorDeclarations(failWorkflow([]string{"Timeout"}, &Fail{Error: "Timeout"})), "built-in")
	require.ErrorContains(t, CheckErrorDeclarations(failWorkflow([]string{"A", "A"}, &Fail{Error: "A"})), "twice")
	require.Error(t, CheckErrorDeclarations(failWorkflow([]string{"lower"}, &Fail{Error: "lower"})))

	many := make([]string, 0, MaxDeclaredErrors+1)
	for i := range MaxDeclaredErrors + 1 {
		many = append(many, "E"+strings.Repeat("x", i+1))
	}
	require.Error(t, CheckErrorDeclarations(failWorkflow(many, &Fail{Error: many[0]})))
}

func TestEvalFailNodeRaisesTheDeclaredKind(t *testing.T) {
	t.Parallel()

	scope := NewScope("", nil)

	_, err := EvalFailNode(t.Context(), &Fail{Error: "Refused", Message: NewExpr(`"no " + "way"`)}, scope)
	var taskErr *TaskError
	require.ErrorAs(t, err, &taskErr)
	require.Equal(t, ErrorKind("Refused"), taskErr.Kind)
	require.Equal(t, ErrorKind("Refused"), ClassifyError(err))
	require.Equal(t, "no way", taskErr.Err.Error())
	require.False(t, ClassifyError(err).Retryable(), "a declared kind must fail closed: never retried")

	// No message: the declared name is the sentence.
	_, err = EvalFailNode(t.Context(), &Fail{Error: "Refused"}, scope)
	require.ErrorAs(t, err, &taskErr)
	require.Equal(t, "Refused", taskErr.Err.Error())

	// A message that is not a string, and one that is too long, are refused
	// rather than coerced or truncated, and neither is the declared kind.
	_, err = EvalFailNode(t.Context(), &Fail{Error: "Refused", Message: NewExpr(`1 + 1`)}, scope)
	require.ErrorContains(t, err, "must be a string")
	require.NotEqual(t, ErrorKind("Refused"), ClassifyError(err))

	_, err = EvalFailNode(t.Context(), &Fail{Error: "Refused", Message: NewExpr(`"x".repeat(5000)`)}, scope)
	require.Error(t, err)
	require.False(t, errors.As(err, &taskErr) && taskErr.Kind == "Refused")
}

func TestAClosedParserStillRefusesADeclaredName(t *testing.T) {
	t.Parallel()

	// Tolerance and retry decide on the workflow's own declarations, never on a
	// spelling, so the closed built-in parser must not learn a declared name.
	_, closed := ParseErrorKind("QuotaExceeded")
	require.False(t, closed)
}

func TestFailRefusesUndoAndAsync(t *testing.T) {
	t.Parallel()

	node := &Node{Id: "refuse", Kind: &Node_Fail{Fail: &Fail{Error: "Refused"}}}
	require.ErrorContains(t, CheckAsyncPlacement(&Node{Id: "refuse", Kind: node.Kind, Async: true}, UndoScopeTopLevel), "fail")

	withUndo := &Node{Id: "refuse", Kind: node.Kind, Undo: &Compensation{Task: &Task{Name: "log"}}}
	require.ErrorContains(t, CheckUndoPlacement(withUndo, UndoScopeTopLevel), "fail")
}

func TestFailMessageMayNotReachASecret(t *testing.T) {
	t.Parallel()

	secret := &Value{Kind: &Value_SecretRef{SecretRef: &SecretRef{Scheme: "env", Name: "TOKEN"}}}
	problems := FailMessageProblems(failWorkflow([]string{"Refused"}, &Fail{Error: "Refused", Message: secret}), DescendCalls)
	require.NotEmpty(t, problems)
	require.Equal(t, "refuse", problems[0].StepID)
	require.Empty(t, WaitPromptProblems(failWorkflow([]string{"Refused"}, &Fail{Error: "Refused", Message: secret}), DescendCalls),
		"a fail message is not a gate prompt")

	wf := failWorkflow([]string{"Refused"}, &Fail{Error: "Refused", Message: NewExpr(`"hello " + inputs.token`)})
	wf.DeclaredInputs = []*InputDeclaration{{Name: "token", Type: InputDeclaration_TYPE_STRING, Sensitive: true}}
	problems = FailMessageProblems(wf, DescendCalls)
	require.NotEmpty(t, problems)
	require.Contains(t, problems[0].Err.Error(), `"token"`)

	wf.DeclaredInputs[0].Sensitive = false
	require.Empty(t, FailMessageProblems(wf, DescendCalls))
}
