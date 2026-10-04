package flowstatev1

import (
	"fmt"
	"time"

	"github.com/picatz/flowstate/internal/strictyaml"
	"github.com/picatz/flowstate/pkg/flowstate/v1/execpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// ExecPolicyFlag and ExecPolicyEnv name how a deployment turns the exec task
// on. Exported so that the denial an unconfigured worker returns names them
// from the same spelling the commands register, rather than from a second copy
// that could drift (the lesson [AllowLoopbackEgressValue] records).
const (
	ExecPolicyFlag = "--exec-policy"
	ExecPolicyEnv  = "FLOWSTATE_EXEC_POLICY"
)

// MaxExecPolicyBytes bounds an exec policy file. A policy is configuration, not
// a data transport; 64 KiB is ample for a table of programs and their rules,
// and refuses an accidental or hostile file before it is parsed.
const MaxExecPolicyBytes = 64 << 10

// ParseExecPolicy decodes, validates and loads an exec policy file: the
// [ExecPolicy] message written as YAML or JSON.
//
// The document is decoded strictly (an unknown or duplicate key is an error, so
// a misspelled restriction fails at start-up instead of silently vanishing),
// checked against the schema's rules, and then handed to [execpolicy.New], which
// verifies every executable on disk, resolves the roots, and compiles every CEL
// rule. Every failure wraps [execpolicy.ErrInvalidPolicy] and refuses the
// command that loaded it: a policy that loaded partly would govern some programs
// and not others.
func ParseExecPolicy(data []byte) (*execpolicy.Policy, error) {
	if len(data) > MaxExecPolicyBytes {
		return nil, fmt.Errorf("%w: the file is %d bytes, over the %d-byte limit",
			execpolicy.ErrInvalidPolicy, len(data), MaxExecPolicyBytes)
	}

	doc := &ExecPolicy{}
	if err := strictyaml.UnmarshalProto(data, doc); err != nil {
		return nil, fmt.Errorf("%w: %w", execpolicy.ErrInvalidPolicy, err)
	}
	if err := Validate(doc); err != nil {
		return nil, fmt.Errorf("%w: %w", execpolicy.ErrInvalidPolicy, err)
	}
	if doc.GetExec() == nil {
		return nil, fmt.Errorf("%w: the file has no exec: section; a policy that lists nothing enables nothing, "+
			"so remove %s entirely if the exec task should stay denied", execpolicy.ErrInvalidPolicy, ExecPolicyFlag)
	}

	e := doc.GetExec()

	timeout, err := time.ParseDuration(e.GetTimeout())
	if err != nil {
		return nil, fmt.Errorf("%w: timeout %q is not a duration such as \"10m\" (and is required): %w",
			execpolicy.ErrInvalidPolicy, e.GetTimeout(), err)
	}
	maxOutput, err := netpolicy.ParseByteSize(e.GetMaxOutputBytes())
	if err != nil {
		return nil, fmt.Errorf("%w: max_output_bytes %q is not a size such as \"1MiB\" (and is required): %w",
			execpolicy.ErrInvalidPolicy, e.GetMaxOutputBytes(), err)
	}

	return execpolicy.New(execpolicy.Config{
		Executables:      e.GetExecutables(),
		ExecutableSHA256: e.GetExecutableSha256(),
		Roots:            e.GetRoots(),
		EnvPassthrough:   e.GetEnvPassthrough(),
		Env:              e.GetEnv(),
		EnvAuthored:      e.GetEnvAuthored(),
		Timeout:          timeout,
		MaxOutputBytes:   int64(maxOutput),
		Allow:            e.GetAllow(),
		Deny:             e.GetDeny(),
	})
}
