package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/dst"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

func TestDriverFlagIsRefusedWhereItCannotWork(t *testing.T) {
	for name, tc := range map[string]struct {
		args []string
		want string
	}{
		"unknown":     {[]string{"--driver", "temporal"}, "is not a driver"},
		"with seeds":  {[]string{"--driver", "both", "--seeds", "3"}, "run them separately"},
		"with seed":   {[]string{"--driver", "both", "--seed", "3"}, "run them separately"},
		"with fuzz":   {[]string{"--driver", "both", "--fuzz", "3"}, "run them separately"},
		"with debug":  {[]string{"--driver", "both", "--debug"}, "cannot be combined with --debug"},
		"with mutate": {[]string{"--driver", "both", "--mutate"}, "run them separately"},
	} {
		t.Run(name, func(t *testing.T) {
			cmd := newTestCommand()
			require.NoError(t, cmd.ParseFlags(tc.args))
			budget, err := scheduleBudget(cmd)
			require.NoError(t, err)
			fuzz, err := fuzzOptions(cmd, budget)
			require.NoError(t, err)
			mutate, err := mutateOptions(cmd, budget, fuzz)
			require.NoError(t, err)
			_, err = driverOption(cmd, budget, fuzz, mutate)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestDriverFlagDefaultsToLocalAlone(t *testing.T) {
	cmd := newTestCommand()
	require.NoError(t, cmd.ParseFlags(nil))
	runner, err := driverOption(cmd, dst.Budget{}, flowtest.FuzzOptions{}, flowtest.MutateOptions{})
	require.NoError(t, err)
	assert.Nil(t, runner)
}
