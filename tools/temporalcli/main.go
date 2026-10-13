// Command temporalcli puts the Temporal CLI the Temporal-server tests run in
// the cache they read, so a test step never downloads it.
//
//	make temporal-cli              # fetch it if missing, with bounded retries
//	go run ./tools/temporalcli -describe >> "$GITHUB_OUTPUT"   # path= and key=
//
// The SDK downloads the CLI from temporal.download with no checksum or
// signature to pin (go.temporal.io/sdk testsuite.CachedDownload has neither),
// so the integrity here is the SDK's TLS fetch, the file's name carrying the
// SDK version, and a --version run before a cached file is trusted. CI caches
// the file under a key naming the SDK version, OS and architecture.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"

	"github.com/picatz/flowstate/internal/temporaltest"
)

const (
	attempts       = 4
	attemptTimeout = 2 * time.Minute
)

// backoff is the pause after the n-th failure: 5s, 10s, 20s.
func backoff(n int) time.Duration { return 5 * time.Second << (n - 1) }

func main() {
	describe := flag.Bool("describe", false, "print path= and key= lines for GITHUB_OUTPUT and fetch nothing")
	flag.Parse()
	if *describe {
		fmt.Printf("path=%s\nkey=%s\n", temporaltest.CLICachePath(), cacheKey())
		return
	}
	path, err := fetch(context.Background(), temporaltest.PrefetchCLI, time.Sleep, os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "temporalcli: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(path)
}

// cacheKey names the file's version and platform: the SDK version picks the
// file (see temporaltest.CLICachePath) and the platform picks the binary.
func cacheKey() string {
	return fmt.Sprintf("temporal-cli-%s-%s-sdk-%s", runtime.GOOS, runtime.GOARCH, temporal.SDKVersion)
}

// fetch runs prefetch up to attempts times, each bounded, sleeping backoff
// between failures, and returns the last error if none succeeds.
func fetch(ctx context.Context, prefetch func(context.Context, log.Logger) (string, error), sleep func(time.Duration), progress io.Writer) (string, error) {
	var err error
	for n := 1; n <= attempts; n++ {
		actx, cancel := context.WithTimeout(ctx, attemptTimeout)
		var path string
		path, err = prefetch(actx, writerLogger{progress})
		cancel()
		if err == nil {
			return path, nil
		}
		fmt.Fprintf(progress, "temporalcli: attempt %d/%d failed: %v\n", n, attempts, err)
		if n < attempts {
			sleep(backoff(n))
		}
	}
	return "", fmt.Errorf("after %d attempts: %w", attempts, err)
}

type writerLogger struct{ w io.Writer }

func (l writerLogger) line(level, msg string, kv []any) {
	fmt.Fprintf(l.w, "temporalcli: %s %s %v\n", level, msg, kv)
}
func (l writerLogger) Debug(string, ...any)        {}
func (l writerLogger) Info(msg string, kv ...any)  { l.line("info", msg, kv) }
func (l writerLogger) Warn(msg string, kv ...any)  { l.line("warn", msg, kv) }
func (l writerLogger) Error(msg string, kv ...any) { l.line("error", msg, kv) }
