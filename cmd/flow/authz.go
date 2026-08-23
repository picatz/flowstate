package main

import (
	"fmt"
	"io"
	"os"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const maxPolicyRehearsalBytes = 1 << 20

func newAuthCommand() *cobra.Command {
	authCmd := &cobra.Command{Use: "auth", Short: "Explain and rehearse authorization decisions"}
	for _, name := range []string{"explain", "rehearse"} {
		cmd := &cobra.Command{
			Use: name, Short: "Combine versioned policy sets offline using the production combiner",
			Args: cobra.NoArgs, RunE: runAuthExplain,
		}
		cmd.Flags().String("request", "", "authorization request JSON file (required; use - for stdin)")
		cmd.Flags().StringArray("policy-set", nil, "policy-set JSON file (repeatable)")
		_ = cmd.MarkFlagRequired("request")
		authCmd.AddCommand(cmd)
	}
	return authCmd
}

func runAuthExplain(cmd *cobra.Command, _ []string) error {
	requestPath, _ := cmd.Flags().GetString("request")
	policyPaths, _ := cmd.Flags().GetStringArray("policy-set")
	request := new(v1.AuthorizationRequest)
	if err := readBoundedProtoJSON(requestPath, request); err != nil {
		return fmt.Errorf("reading authorization request: %w", err)
	}
	sets := make([]*v1.PolicySet, 0, len(policyPaths))
	for _, path := range policyPaths {
		set := new(v1.PolicySet)
		if err := readBoundedProtoJSON(path, set); err != nil {
			return fmt.Errorf("reading policy set %s: %w", path, err)
		}
		sets = append(sets, set)
	}
	out, err := protojson.MarshalOptions{Multiline: true, Indent: "  ", UseProtoNames: true}.Marshal(v1.CombineAuthorization(request, sets))
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(cmd.OutOrStdout(), string(out))
	return err
}

func readBoundedProtoJSON(path string, message proto.Message) error {
	var reader io.Reader
	if path == "-" {
		reader = os.Stdin
	} else {
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		reader = file
	}
	data, err := io.ReadAll(io.LimitReader(reader, maxPolicyRehearsalBytes+1))
	if err != nil {
		return err
	}
	if len(data) > maxPolicyRehearsalBytes {
		return fmt.Errorf("input exceeds %d bytes", maxPolicyRehearsalBytes)
	}
	return protojson.Unmarshal(data, message)
}
