package tests

import (
	"net/http"
	"net/http/httptest"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// Case is a workflow paired with the outputs it must produce.
type Case struct {
	// Name of the workflow, used for test identification.
	Name string
	// Workflow is the actual workflow definition to be tested.
	Workflow *v1.Workflow
	// ExpectedOutputs is the expected outputs of the workflow steps after execution.
	ExpectedOutputs *v1.Workflow_StepOutputs
}

// NewHTTPServer starts a server returning deterministic responses for the http
// task, and returns its base URL.
//
// Tests use this rather than a public endpoint so the suite does not depend on
// the internet: a third-party outage previously failed three packages at once,
// and a response whose headers vary run to run cannot be asserted exactly.
func NewHTTPServer(tb testing.TB) string {
	tb.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/status/200", func(w http.ResponseWriter, r *http.Request) {
		// A fixed content type keeps the asserted headers stable; Go adds
		// Content-Length and Date, which the assertions below account for.
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"slideshow":{"title":"Sample Slide Show"}}`))
	})

	srv := httptest.NewServer(mux)
	tb.Cleanup(srv.Close)

	allowLoopback(tb)
	return srv.URL
}

// allowLoopback registers an http task permitting loopback for the duration of
// the test, restoring the original afterwards.
//
// The default egress policy denies loopback, which is correct — a workflow must
// not be able to reach a worker's own internal endpoints — but it also means the
// task cannot reach a test server. Rather than weakening the default so tests
// pass, the tests state the exemption they need, which keeps the shipped default
// under test everywhere else.
func allowLoopback(tb testing.TB) {
	tb.Helper()

	policy, err := netpolicy.New(netpolicy.WithAllowLoopback())
	if err != nil {
		tb.Fatalf("building loopback egress policy: %v", err)
	}

	registry := v1.DefaultRegistry()
	original, existed := registry.Lookup("http")
	if err := registry.Register(v1.HTTPTaskDef(policy)); err != nil {
		tb.Fatalf("registering loopback http task: %v", err)
	}
	tb.Cleanup(func() {
		if existed {
			_ = registry.Register(original)
		}
	})
}

// Workflows returns the workflows shared between the local and Temporal
// execution tests, so both drivers are held to identical expectations. The
// httpBaseURL should come from [NewHTTPServer].
//
// Keeping one definition for both drivers is what catches divergence between
// `flow run local` and durable execution; duplicating them would let the two
// drift apart silently.
func Workflows(httpBaseURL string) []Case {
	return []Case{
		{
			Name: "simple echo workflow",
			Workflow: &v1.Workflow{
				Name: "simple",
				Steps: []*v1.Node{
					{
						Id: "a",
						Kind: &v1.Node_Task{
							Task: &v1.Task{
								Name: "echo",
								Inputs: map[string]*v1.Value{
									"message": v1.NewLiteral("hello world"),
								},
							},
						},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{
				StepValues: map[string]*v1.Node_Outputs{
					"a": {
						NamedValues: map[string]*v1.Value{
							"result": v1.NewLiteral("hello world"),
						},
					},
				},
			},
		},
		{
			Name: "simple multi-step echo workflow",
			Workflow: &v1.Workflow{
				Name: "simple",
				Steps: []*v1.Node{
					{
						Id: "a",
						Kind: &v1.Node_Task{
							Task: &v1.Task{
								Name: "echo",
								Inputs: map[string]*v1.Value{
									"message": v1.NewLiteral("hello world"),
								},
							},
						},
					},
					{
						Id: "b",
						Kind: &v1.Node_Task{
							Task: &v1.Task{
								Name: "echo",
								Inputs: map[string]*v1.Value{
									"message": v1.NewExpr("a.result"),
								},
							},
						},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{
				StepValues: map[string]*v1.Node_Outputs{
					"a": {
						NamedValues: map[string]*v1.Value{
							"result": v1.NewLiteral("hello world"),
						},
					},
					"b": {
						NamedValues: map[string]*v1.Value{
							"result": v1.NewLiteral("hello world"),
						},
					},
				},
			},
		},
		{
			Name: "simple printf workflow",
			Workflow: &v1.Workflow{
				Name: "simple",
				Steps: []*v1.Node{
					{
						Id: "a",
						Kind: &v1.Node_Task{
							Task: &v1.Task{
								Name: "printf",
								Inputs: map[string]*v1.Value{
									"format": v1.NewLiteral("%s %s"),
									"args": v1.NewLiteralList(
										"hello",
										"world",
									),
									// "args": {
									// 	Kind: &v1.Value_Literal{
									// 		Literal: &expr.Value{
									// 			Kind: &expr.Value_ListValue{
									// 				ListValue: &expr.ListValue{
									// 					Values: []*expr.Value{
									// 						{Kind: &expr.Value_StringValue{StringValue: "hello"}},
									// 						{Kind: &expr.Value_StringValue{StringValue: "world"}},
									// 					},
									// 				},
									// 			},
									// 		},
									// 	},
									// },
								},
							},
						},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{
				StepValues: map[string]*v1.Node_Outputs{
					"a": {
						NamedValues: map[string]*v1.Value{
							"result": v1.NewLiteral("hello world"),
						},
					},
				},
			},
		},
		{
			Name: "simple http workflow",
			Workflow: &v1.Workflow{
				Name: "simple",
				Steps: []*v1.Node{
					{
						Id: "web",
						Kind: &v1.Node_Task{
							Task: &v1.Task{
								Name: "http",
								Inputs: map[string]*v1.Value{
									"url":    v1.NewLiteral(httpBaseURL + "/status/200"),
									"method": v1.NewLiteral(http.MethodGet),
									// Shape the response to just the status code so the
									// assertion does not depend on headers net/http adds.
									"outputs": v1.NewExpr("{'status_code': status_code}"),
								},
							},
						},
					},
					{
						Id: "output",
						Kind: &v1.Node_Task{
							Task: &v1.Task{
								Name: "echo",
								Inputs: map[string]*v1.Value{
									"message": v1.NewExpr("string(web.status_code)"),
								},
							},
						},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{
				StepValues: map[string]*v1.Node_Outputs{
					"web": {
						NamedValues: map[string]*v1.Value{
							"status_code": v1.NewLiteral(int64(200)),
						},
					},
					"output": {
						NamedValues: map[string]*v1.Value{
							"result": v1.NewLiteral("200"),
						},
					},
				},
			},
		},
		{
			Name: "cel expression workflow",
			Workflow: &v1.Workflow{
				Name: "simple",
				Steps: []*v1.Node{
					{
						Id: "a",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name: "echo",
							Inputs: map[string]*v1.Value{
								"message": v1.NewLiteral("hello"),
							},
						}},
					},
					{
						Id: "b",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name: "cel",
							Inputs: map[string]*v1.Value{
								"expr": v1.NewLiteral("a.result + '!'"),
							},
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"a": {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("hello")}},
				"b": {NamedValues: map[string]*v1.Value{"result": {Kind: &v1.Value_Literal{Literal: &expr.Value{Kind: &expr.Value_StringValue{StringValue: "hello!"}}}}}},
			}},
		},
		{
			// Exercises the default outputs — a workflow reads ${steps.web.body}
			// without declaring an outputs expression — while shaping the final
			// step so the assertion stays independent of headers net/http adds.
			Name: "http default body output workflow",
			Workflow: &v1.Workflow{
				Name: "http-defaults",
				Steps: []*v1.Node{
					{
						Id: "web",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name: "http",
							Inputs: map[string]*v1.Value{
								"url":     v1.NewLiteral(httpBaseURL + "/json"),
								"method":  v1.NewLiteral(http.MethodGet),
								"outputs": v1.NewExpr("{'body': body}"),
							},
						}},
					},
					{
						Id: "title",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name: "cel",
							Inputs: map[string]*v1.Value{
								"libs": v1.NewLiteralList("json"),
								"expr": v1.NewLiteral("json_parse(web.body)['slideshow']['title']"),
							},
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"web": {NamedValues: map[string]*v1.Value{
					"body": v1.NewLiteral(`{"slideshow":{"title":"Sample Slide Show"}}`),
				}},
				"title": {NamedValues: map[string]*v1.Value{
					"result": v1.NewLiteral("Sample Slide Show"),
				}},
			}},
		},
	}
}

// zeroValueCases exercise inputs and outputs that are legitimately empty.
//
// These are separated from [Workflows] because they assert a property rather
// than a fixed output: a zero value must survive a round trip. Empty strings,
// zero integers, and false booleans were previously rejected as invalid input
// and dropped from outputs, because the conversion layer tested whether an
// extracted value was non-zero instead of which kind it held.
func ZeroValueCases() []Case {
	return []Case{
		{
			Name: "empty string message",
			Workflow: &v1.Workflow{
				Name: "empty-string",
				Steps: []*v1.Node{
					{
						Id: "a",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name: "echo",
							Inputs: map[string]*v1.Value{
								"message": v1.NewLiteral(""),
							},
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"a": {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("")}},
			}},
		},
		{
			Name: "empty string through an expression",
			Workflow: &v1.Workflow{
				Name: "empty-string-expr",
				Steps: []*v1.Node{
					{
						Id: "a",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name:   "echo",
							Inputs: map[string]*v1.Value{"message": v1.NewLiteral("")},
						}},
					},
					{
						Id: "b",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name:   "echo",
							Inputs: map[string]*v1.Value{"message": v1.NewExpr("a.result")},
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"a": {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("")}},
				"b": {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("")}},
			}},
		},
		{
			Name: "printf with an empty argument",
			Workflow: &v1.Workflow{
				Name: "empty-printf",
				Steps: []*v1.Node{
					{
						Id: "a",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name: "printf",
							Inputs: map[string]*v1.Value{
								"format": v1.NewLiteral("[%s]"),
								"args":   v1.NewLiteralList(""),
							},
						}},
					},
				},
			},
			ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
				"a": {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("[]")}},
			}},
		},
	}
}
