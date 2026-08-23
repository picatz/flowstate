package appearance

import (
	"context"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/internal/covbuild"
)

// update re-records every golden from the tapes rather than comparing against
// them. It is what `make appearance-update` passes, and the only sanctioned way
// a golden changes: a golden edited by hand records an appearance nothing ever
// produced.
var update = flag.Bool("update", false, "re-record the appearance goldens from the tapes")

// required are the three binaries a recording needs. vhs drives a real
// terminal (ttyd) inside a real browser and hands the frames to ffmpeg, and it
// refuses to start when any of them is absent, so the test checks for all three
// and says which one is missing rather than letting vhs fail with its own
// installation advice halfway through a run.
var required = []string{"vhs", "ttyd", "ffmpeg"}

// frameSeparator is what vhs writes between screen dumps in a text recording.
// It saves the whole visible grid after every command it executes while
// recording, so a tape that ends with Show plus one Sleep leaves two identical
// dumps, and the last one is the settled screen.
const frameSeparator = "────────────────────────────────────────────────────────────────────────────────"

// recordingTimeout bounds a single tape. Each one sleeps for about four seconds
// of its own accord; the rest of the budget is browser start-up, which is slow
// on a cold cache and must not be able to hang a CI job.
const recordingTimeout = 3 * time.Minute

// TestAppearance records each tape and compares the settled screen against its
// golden. A failure here is a styled surface that changed shape: read the diff,
// decide whether the new shape is the intended one, and re-record with
// `make appearance-update` if it is.
func TestAppearance(t *testing.T) {
	skipUnlessRecordable(t)

	flowBin := buildFlow(t)

	tapes, err := filepath.Glob(filepath.Join("testdata", "*.tape"))
	require.NoError(t, err)
	require.NotEmpty(t, tapes, "no tapes found: this package is the tapes")

	for _, tape := range tapes {
		t.Run(strings.TrimSuffix(filepath.Base(tape), ".tape"), func(t *testing.T) {
			frame := record(t, tape, flowBin)

			golden := strings.TrimSuffix(tape, ".tape") + ".golden.txt"
			if *update {
				require.NoError(t, os.WriteFile(golden, []byte(frame), 0o600))
				t.Logf("recorded %s", golden)
				return
			}

			want, err := os.ReadFile(golden)
			require.NoError(t, err, "no golden yet: record one with `make appearance-update`")

			assert.Equal(t, string(want), frame,
				"the appearance of this surface changed; re-record with `make appearance-update` if the new shape is the intended one")
		})
	}
}

// record runs one tape and returns its settled screen.
func record(t *testing.T, tape, flowBin string) string {
	t.Helper()

	tapePath, err := filepath.Abs(tape)
	require.NoError(t, err)

	// vhs resolves the tape's Output path against its own working directory,
	// which is why the recording gets a directory of its own rather than
	// writing beside the tapes. HOME points into the same place so that a tape
	// scaffolding a project (`flow init`) writes somewhere disposable.
	work := t.TempDir()

	ctx, cancel := context.WithTimeout(t.Context(), recordingTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "vhs", tapePath)
	cmd.Dir = work
	cmd.Env = recordingEnv(work, flowBin)

	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "vhs failed to record %s:\n%s", tape, out)

	raw, err := os.ReadFile(filepath.Join(work, "out.txt"))
	require.NoError(t, err, "vhs recorded nothing for %s:\n%s", tape, out)

	frame := lastFrame(string(raw))
	require.NotEmpty(t, frame, "the recording of %s held no frames", tape)

	// Every tape types its command at a `$ ` prompt, so the first line of the
	// settled screen is that prompt. Anything else means the frame is taller
	// than the grid vhs gave us and the top scrolled away, which would
	// otherwise read as a baffling diff against the golden.
	require.True(t, strings.HasPrefix(frame, "$ "),
		"the terminal grid was too small to hold %s and its top scrolled away; the frame starts %q", tape, firstLine(frame))

	return frame
}

// lastFrame returns the final screen dump in a vhs text recording, with the
// trailing blank lines that pad it out to the height of the grid removed. The
// padding is a property of the machine that recorded, not of the CLI, and a
// golden should hold only the second kind of thing.
func lastFrame(raw string) string {
	frames := strings.Split(raw, frameSeparator)
	for i := len(frames) - 1; i >= 0; i-- {
		frame := strings.TrimRight(frames[i], " \n")
		frame = strings.TrimLeft(frame, "\n")
		if frame != "" {
			return frame + "\n"
		}
	}

	return ""
}

func firstLine(s string) string {
	line, _, _ := strings.Cut(s, "\n")
	return line
}

// recordingEnv is the pinned environment a tape records under. Everything the
// CLI would otherwise ask the terminal or the machine is answered here, because
// a golden that depends on an unanswered question is a golden that fails on
// somebody else's laptop.
func recordingEnv(home, flowBin string) []string {
	env := []string{
		"HOME=" + home,
		// Every tape gets a fresh HOME, so without a cache pointed somewhere
		// that outlives it, vhs would download a Chromium per tape rather than
		// once. This is the one piece of state a recording is allowed to keep.
		"XDG_CACHE_HOME=" + browserCache(),
		// The background query is the one question the ui package will ask a
		// terminal, and on a pty that never answers it costs seconds. Pinning
		// it also fixes which half of every light/dark role pair is chosen.
		"FLOWSTATE_BACKGROUND=dark",
		// Chromium refuses to start as root without this, which is the shape
		// of every container CI is likely to run in. It is vhs's own escape
		// hatch, and it is inert for a non-root recorder.
		"VHS_NO_SANDBOX=true",
		"TZ=UTC",
		"LANG=C.UTF-8",
		"PATH=" + filepath.Dir(flowBin) + string(os.PathListSeparator) + os.Getenv("PATH"),
	}

	// This environment is built from scratch rather than inherited, so
	// GOCOVERDIR does not reach the recorded flow binary on its own; add it
	// back when instrumentation was requested (see internal/covbuild).
	env = append(env, covbuild.Env()...)

	return env
}

// browserCache is where vhs may keep the browser it downloads on first use:
// the caller's cache if they have one, and otherwise a fixed directory under
// the system temporary directory, shared by every tape and by later runs.
func browserCache() string {
	if cache := os.Getenv("XDG_CACHE_HOME"); cache != "" {
		return cache
	}

	cache := filepath.Join(os.TempDir(), "flowstate-appearance-cache")
	_ = os.MkdirAll(cache, 0o750)

	return cache
}

// buildFlow builds the CLI the tapes invoke. Recording against the tree's own
// build rather than whatever `flow` happens to be installed is what makes the
// golden a statement about this commit.
func buildFlow(t *testing.T) string {
	t.Helper()

	bin := filepath.Join(t.TempDir(), "flow")

	args := append([]string{"build"}, covbuild.BuildArgs()...)
	args = append(args, "-o", bin, "./cmd/flow")

	cmd := exec.Command("go", args...)
	cmd.Dir = filepath.Join("..", "..", "..", "..")

	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "building flow for the recording failed:\n%s", out)

	return bin
}

// skipUnlessRecordable skips when the machine cannot record, naming what is
// missing. The skip is deliberate: `go test ./...` on a laptop should not
// require a browser and a video encoder. CI installs all three, so the check
// that holds these goldens is never the one that skipped.
func skipUnlessRecordable(t *testing.T) {
	t.Helper()

	var missing []string
	for _, bin := range required {
		if _, err := exec.LookPath(bin); err != nil {
			missing = append(missing, bin)
		}
	}

	if len(missing) > 0 {
		t.Skipf("appearance recording needs %s on PATH; missing: %s",
			strings.Join(required, ", "), strings.Join(missing, ", "))
	}
}
