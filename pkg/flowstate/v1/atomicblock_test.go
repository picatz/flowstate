package flowstatev1

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// task returns a body step running a task; the walk counts it whatever its
// condition would decide, which taskGuarded below pins.
func task(id string) *Node {
	return &Node{
		Id: id,
		Kind: &Node_Task{Task: &Task{
			Name:   "http",
			Inputs: map[string]*Value{"method": NewLiteral("GET"), "url": NewLiteral("http://127.0.0.1:1/never")},
		}},
	}
}

func taskGuarded(id string) *Node {
	n := task(id)
	n.Condition = NewExpr("false")
	return n
}

func tasks(n int) []*Node {
	out := make([]*Node, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, task("t-"+strconv.Itoa(i)))
	}
	return out
}

// TestWorstCaseBodyActivities pins the walk's counting rules one construct at
// a time, with exact counts rather than bounds — an off-by-one in any rule
// moves a refusal boundary, so each case here is the mutation test for its
// rule.
func TestWorstCaseBodyActivities(t *testing.T) {
	tests := []struct {
		name string
		body []*Node
		want int
	}{
		{
			name: "a task counts one",
			body: []*Node{task("a")},
			want: 1,
		},
		{
			name: "a guarded task still counts: the walk cannot evaluate an if",
			body: []*Node{taskGuarded("a")},
			want: 1,
		},
		{
			name: "a value and a wait schedule no activity",
			body: []*Node{
				{Id: "v", Kind: &Node_Value{Value: NewExpr("1")}},
				{Id: "w", Kind: &Node_Wait{Wait: &Wait{}}},
				task("a"),
			},
			want: 1,
		},
		{
			name: "a switch counts its widest arm, default included",
			body: []*Node{{
				Id: "s",
				Kind: &Node_Switch{Switch: &Switch{
					Value: NewExpr("'x'"),
					Cases: []*Switch_Case{
						{Values: []*Value{NewLiteral("a")}, Steps: tasks(2)},
						{Values: []*Value{NewLiteral("b")}, Steps: tasks(5)},
					},
					Default: &Switch_Default{Steps: tasks(7)},
				}},
			}},
			want: 7,
		},
		{
			name: "parallel branches sum",
			body: []*Node{{
				Id: "p",
				Kind: &Node_Parallel{Parallel: &Parallel{
					Branches: []*Parallel_Branch{
						{Steps: tasks(3)},
						{Steps: tasks(4)},
					},
				}},
			}},
			want: 7,
		},
		{
			name: "a call counts its callee's steps",
			body: []*Node{{
				Id: "c",
				Kind: &Node_Call{Call: &Call{
					Workflow: &Workflow{Name: "callee", Steps: tasks(3)},
				}},
			}},
			want: 3,
		},
		{
			name: "a nested for_each multiplies by its trip ceiling: its own items are an expression this walk cannot see",
			body: []*Node{{
				Id: "inner",
				Kind: &Node_ForEach{ForEach: &ForEach{
					Items: NewExpr("[1]"),
					Body:  tasks(2),
				}},
			}},
			want: MaxForEachItems * 2,
		},
		{
			name: "a nested loop multiplies by its declared iteration ceiling",
			body: []*Node{{
				Id: "inner",
				Kind: &Node_Loop{Loop: &Loop{
					Until:         NewExpr("true"),
					MaxIterations: 4,
					Body:          tasks(3),
				}},
			}},
			want: 12,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, WorstCaseBodyActivities(test.body))
		})
	}
}

// TestWorstCaseBodyActivitiesSaturates pins that the arithmetic saturates
// rather than overflowing: nested ceilings multiply, and a few levels of
// [DefaultMaxIterations] would wrap an int long before the node bound stops
// the walk. Everything past the refusal point behaves identically, so
// saturation loses nothing a caller could act on.
func TestWorstCaseBodyActivitiesSaturates(t *testing.T) {
	inner := tasks(1)
	body := inner
	for i := 0; i < 6; i++ {
		body = []*Node{{
			Id: "level-" + strconv.Itoa(i),
			Kind: &Node_Loop{Loop: &Loop{
				Until: NewExpr("true"),
				Body:  body,
			}},
		}}
	}
	// Six levels of the 1,000-iteration default ceiling over one task is
	// 10^18 — past what an int32 or a naive product holds, and represented
	// here as exactly one past the ceiling, which every over-ceiling count
	// collapses to.
	require.Equal(t, atomicBlockSaturated, WorstCaseBodyActivities(body))
	require.Greater(t, atomicBlockSaturated, MaxAtomicBlockActivities,
		"saturation must land past the ceiling, or an overflowed product would be admitted")
}

// TestCheckAtomicBlockActivitiesBoundary pins the refusal boundary in the
// bound's own unit: a product of exactly the ceiling is allowed, one past it
// is refused, and the sentence carries the item count, the per-iteration
// count and the ceiling — the pieces both drivers' shared cases assert
// survive the trip through either executor.
func TestCheckAtomicBlockActivitiesBoundary(t *testing.T) {
	body := tasks(20)

	require.NoError(t, CheckAtomicBlockActivities(MaxAtomicBlockActivities/20, body),
		"a product of exactly the ceiling must run")

	err := CheckAtomicBlockActivities(MaxAtomicBlockActivities/20+1, body)
	require.Error(t, err, "a product past the ceiling must be refused")
	require.Contains(t, err.Error(), strconv.Itoa(MaxAtomicBlockActivities/20+1))
	require.Contains(t, err.Error(), strconv.Itoa(20))
	require.Contains(t, err.Error(), strconv.Itoa(MaxAtomicBlockActivities))
}
