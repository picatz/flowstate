package debugtui

import (
	"fmt"
	"strconv"
	"strings"

	"charm.land/lipgloss/v2"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

const (
	paneTimeline = "timeline"

	// pointPrefix is the id prefix a timeline point's hit is registered under;
	// the rest is the point's index, which is also what `goto` takes.
	pointPrefix = "point/"
)

// timelineOf is the timeline a frame carries, or nil when it has none to draw.
func timelineOf(f flowdebug.Frame, loaded bool) *v1.DebugTimeline {
	if !loaded || len(f.Snapshot.GetTimeline().GetPoints()) == 0 {
		return nil
	}

	return f.Snapshot.GetTimeline()
}

// gotoPoint is the point a `goto` line names.
func gotoPoint(line string) (int, bool) {
	fields := strings.Fields(line)
	if len(fields) != 2 || fields[0] != "goto" {
		return 0, false
	}
	n, err := strconv.Atoi(fields[1])

	return n, err == nil && n >= 0
}

// TimelineView is the one-row strip of the stops the run showed, each named by
// the index `goto` takes. The point the run is at is bracketed, a point a travel
// would not be tried for is muted, one a travel found the run could not be
// brought back to is marked with the failure symbol, and what does not fit is
// counted: "+N earlier" for the stops dropped or scrolled off to the left,
// "+N later" to the right. diverged holds those points by their number from the
// first the target kept (dropped + index), and replayTo is the point a travel in
// flight is going to, or -1: the points after it are muted while it replays.
//
// Each drawn point registers a [pane.KindPoint] hit under pointPrefix and its
// index. The row is exactly row.W cells.
func TimelineView(f flowdebug.Frame, loaded bool, diverged map[uint64]bool, replayTo int, row pane.Rect, hits *pane.Hits, o pane.Options) string {
	timeline := timelineOf(f, loaded)
	if timeline == nil || row.W <= 0 {
		return ""
	}
	points := timeline.GetPoints()
	current := int(timeline.GetCurrent())

	type item struct {
		text  string
		style lipgloss.Style
	}
	items := make([]item, len(points))
	for i, point := range points {
		label := strconv.Itoa(i)
		switch {
		case i == current:
			items[i] = item{"[" + label + "]", o.Theme.Accent}
		case diverged[uint64(timeline.GetDropped())+uint64(i)]:
			items[i] = item{o.Symbols.Failure + label, o.Theme.Danger}
		case !point.GetReachable() || (replayTo >= 0 && i > replayTo):
			items[i] = item{label, o.Theme.Muted}
		default:
			items[i] = item{label, o.Theme.Strong}
		}
	}

	head := o.Theme.Header.Render(paneTimeline)
	avail := row.W - lipgloss.Width(paneTimeline) - 1
	earlier := func(lo int) string {
		if n := int(timeline.GetDropped()) + lo; n > 0 {
			return fmt.Sprintf("+%d earlier ", n)
		}

		return ""
	}
	later := func(hi int) string {
		if n := len(points) - 1 - hi; n > 0 {
			return fmt.Sprintf(" +%d later", n)
		}

		return ""
	}
	width := func(lo, hi int) int {
		w := lipgloss.Width(earlier(lo)) + lipgloss.Width(later(hi)) + hi - lo
		for _, it := range items[lo : hi+1] {
			w += lipgloss.Width(it.text)
		}

		return w
	}

	// The window grows from the point the run is at, a point each way in turn,
	// for as long as it fits.
	lo := max(current, 0)
	if current < 0 {
		lo = len(points) - 1
	}
	hi := lo
	for {
		grew := false
		if lo > 0 && width(lo-1, hi) <= avail {
			lo--
			grew = true
		}
		if hi < len(points)-1 && width(lo, hi+1) <= avail {
			hi++
			grew = true
		}
		if !grew {
			break
		}
	}

	var b strings.Builder
	b.WriteString(head + " ")
	x := lipgloss.Width(paneTimeline) + 1
	if note := earlier(lo); note != "" {
		b.WriteString(o.Theme.Muted.Render(note))
		x += lipgloss.Width(note)
	}
	for i := lo; i <= hi; i++ {
		it := items[i]
		b.WriteString(it.style.Render(it.text))
		hits.Add(pane.Rect{X: row.X + x, Y: row.Y, W: lipgloss.Width(it.text), H: 1}, pointPrefix+strconv.Itoa(i), pane.KindPoint)
		x += lipgloss.Width(it.text)
		if i < hi {
			b.WriteString(" ")
			x++
		}
	}
	if note := later(hi); note != "" {
		b.WriteString(o.Theme.Muted.Render(note))
	}

	return ui.Trim(b.String(), row.W)
}

// travelNote is the line the transcript gets when a travel lands: the record
// reads as what happened, not as a rewound one. before and after are the
// timeline's current point either side of the move, -1 when unknown.
func travelNote(snapshot *v1.DebugSnapshot, before, after int, o pane.Options) string {
	address := ui.EscapeControl(snapshot.GetOccurrence().GetAddress())
	if address == "" {
		address = "the start"
	}
	arrow, verb := "←", "back to"
	if after > before && before >= 0 {
		arrow, verb = "→", "on to"
	}
	if o.Symbols.Arrow != "→" {
		arrow = map[string]string{"←": "<-", "→": "->"}[arrow]
	}

	return fmt.Sprintf("%s %s %s (rev %d)", arrow, verb, address, snapshot.GetRevision())
}
