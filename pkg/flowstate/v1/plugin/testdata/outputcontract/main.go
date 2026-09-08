package main

import (
	"context"
	"fmt"
	"os"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

func main() {
	passthrough := func(_ context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
		return &flowstatev1.Node_Outputs{NamedValues: map[string]*flowstatev1.Value{
			"message": inputs["message"],
		}}, nil
	}
	malformed := func(_ context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
		return &flowstatev1.Node_Outputs{NamedValues: map[string]*flowstatev1.Value{
			"undeclared": inputs["message"],
		}}, nil
	}
	wrongType := func(_ context.Context, _ map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
		return &flowstatev1.Node_Outputs{NamedValues: map[string]*flowstatev1.Value{
			"message": flowstatev1.NewLiteral(int64(42)),
		}}, nil
	}
	observe := func(_ context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
		path := inputs["message"].GetLiteral().GetStringValue()
		if err := os.WriteFile(path, []byte("observed"), 0o600); err != nil {
			return nil, err
		}
		return &flowstatev1.Node_Outputs{}, nil
	}

	err := sdk.Run(context.Background(), sdk.Plugin{
		Name:        "output-contract",
		Version:     "0.0.1",
		Description: "adversarial output-contract fixture",
		Tasks: []sdk.Task{
			{Name: "valid", Input: &flowstatev1.Task_Log_Inputs{}, Output: &flowstatev1.Task_Log_Inputs{}, Fn: passthrough},
			{Name: "malformed", Input: &flowstatev1.Task_Log_Inputs{}, Output: &flowstatev1.Task_Log_Inputs{}, Fn: malformed},
			{Name: "no_contract", Input: &flowstatev1.Task_Log_Inputs{}, Fn: malformed},
			{Name: "wrong_type", Input: &flowstatev1.Task_Log_Inputs{}, Output: &flowstatev1.Task_Log_Inputs{}, Fn: wrongType},
			{Name: "observe", Input: &flowstatev1.Task_Log_Inputs{}, Output: &flowstatev1.Task_Log_Outputs{}, Fn: observe},
		},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
