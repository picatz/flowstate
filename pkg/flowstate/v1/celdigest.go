package flowstatev1

import (
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

// digestSHA256Function is the CEL function name the digest library registers.
// It is named once because [byteCostEstimator] must recognise the call and
// charge the bytes hashed rather than the fixed-size string it returns.
const digestSHA256Function = "digest.sha256"

// digestLibrary exposes the repository's one content-identity spelling to CEL.
//
// It is deliberately one operation rather than a cryptography catalog. The
// result is a checksum for content identity and idempotency; it is not a
// signature, MAC, password hash, or proof of authenticity.
func digestLibrary() cel.EnvOption {
	digest := func(data []byte) ref.Val {
		return types.String(ContentDigest(data))
	}

	return cel.Function(digestSHA256Function,
		cel.Overload("digest_sha256_string",
			[]*cel.Type{cel.StringType}, cel.StringType,
			cel.UnaryBinding(func(val ref.Val) ref.Val {
				s, ok := val.Value().(string)
				if !ok {
					return types.NewErr("digest.sha256: expected string input, got %v", val.Type())
				}
				return digest([]byte(s))
			}),
		),
		cel.Overload("digest_sha256_bytes",
			[]*cel.Type{cel.BytesType}, cel.StringType,
			cel.UnaryBinding(func(val ref.Val) ref.Val {
				b, ok := val.Value().([]byte)
				if !ok {
					return types.NewErr("digest.sha256: expected bytes input, got %v", val.Type())
				}
				return digest(b)
			}),
		),
	)
}
