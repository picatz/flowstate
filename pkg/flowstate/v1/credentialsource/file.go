package credentialsource

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// MaxFileTokenBytes bounds a token file. A JWT with a generous set of claims
// is a few kilobytes; anything past this is a path pointing at the wrong
// thing, and refusing beats hashing an unrelated file into an Authorization
// header.
const MaxFileTokenBytes = 64 << 10

// fileSource reads a bearer token from a file, fresh on every call.
//
// The re-read matters more than anything else about it: this is the shape a
// federated credential actually arrives in, because Kubernetes rewrites a
// projected service account token in place as it rotates. A Source that cached
// the first read would start failing partway through a long-running command
// for no reason its caller could see.
type fileSource struct {
	path string
}

// NewFileSource returns a [Source] that reads path on every [Source.Token]
// call.
func NewFileSource(path string) Source {
	return fileSource{path: path}
}

func (f fileSource) Name() string { return SourceFile }

func (f fileSource) Token(ctx context.Context) (Token, error) {
	if err := ctx.Err(); err != nil {
		return Token{}, err
	}

	file, err := os.Open(f.path)
	if err != nil {
		return Token{}, fmt.Errorf("%w: reading %s: %w", ErrSourceUnusable, f.path, err)
	}
	defer func() { _ = file.Close() }()

	// One byte past the limit, so a token at exactly the limit still works and
	// one over it is refused rather than silently truncated into a token that
	// would be rejected for a reason nobody could diagnose.
	contents, err := io.ReadAll(io.LimitReader(file, MaxFileTokenBytes+1))
	if err != nil {
		return Token{}, fmt.Errorf("%w: reading %s: %w", ErrSourceUnusable, f.path, err)
	}
	if len(contents) > MaxFileTokenBytes {
		return Token{}, fmt.Errorf("%w: the credential in %s is larger than %d bytes, which is not a "+
			"token; check the path", ErrSourceUnusable, f.path, MaxFileTokenBytes)
	}

	// Trimmed because a file almost always ends in a newline, and a newline in
	// a header value is rejected by net/http as header injection — which would
	// report a transport error rather than "your token file has a newline in
	// it".
	raw := strings.TrimSpace(string(contents))
	if raw == "" {
		return Token{}, fmt.Errorf("%w: %s is empty", ErrSourceUnusable, f.path)
	}

	return newToken(SourceFile, raw, time.Time{}), nil
}
