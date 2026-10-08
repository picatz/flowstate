package debugtui

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// The source pane draws the Flowfile the run was started from beside the flow,
// with the lines of the step the run is held before marked, and the lines that
// carry a breakpoint.
//
// Which line a step is on is the source map's to say, and the map is trusted
// only where it is of the program the run executes ([Frame.SourceMap] is set by
// a caller that checked, and the screen drops it again for a run that reports a
// different program). A document's text is trusted only where it is the bytes
// the map was made from, by the digest the map records. Where either does not
// hold the pane draws the step's address and one sentence saying why, and never
// a line: a line from a file that moved would mark the wrong step.
//
// The texts are the caller's bytes (see [Config.Documents]), read once into
// lines that are safe to draw, so the pane is a function of the frame, those
// documents and a size.

const (
	// paneSource is the source pane's name in the grid, the ring and the hits.
	paneSource = "source"

	// sourcePrefix is the id prefix a line's hit is registered under, and
	// gutterPrefix the same for the gutter of a line, the part of it that arms
	// a breakpoint; the rest is the 1-based line number.
	sourcePrefix = "source/"
	gutterPrefix = "gutter/"

	// MaxSourceDocuments bounds the documents the pane holds: a program that
	// calls more files than this shows the rest by address.
	MaxSourceDocuments = 32

	// MaxSourceBytes bounds one document. A larger one is not loaded, and the
	// pane says how large it is.
	MaxSourceBytes = 1 << 20

	// MaxSourceLines bounds the lines held of one document; the pane says how
	// many it left out.
	MaxSourceLines = 20_000

	// MaxSourceLineRunes bounds one line. The rest is cut and the line ends in
	// the ellipsis mark, as it does where it is wider than the pane.
	MaxSourceLineRunes = 1000

	// sourceTabWidth is the stops tabs are expanded to.
	sourceTabWidth = 4

	// minNumberWidth is the least width of the line-number column.
	minNumberWidth = 3
)

// Document is one source text the screen may show: the bytes of a file, by the
// name the source map knows it under.
type Document struct {
	// URI names the document as the source map's [v1.DebugSourceDocument.Uri]
	// does; two spellings of one path are the same document
	// ([flowdebug.SameSourceURI]).
	URI  string
	Text []byte
}

// sourceDoc is a document read into lines that are safe to draw.
type sourceDoc struct {
	uri string

	// digest is the content digest of the bytes given, to be compared with the
	// one the source map records.
	digest string

	// lines are the text, one entry per line, with tabs expanded and control
	// characters spelled out. cut marks a line shortened to [MaxSourceLineRunes].
	lines []string
	cut   []bool

	// more is how many lines past [MaxSourceLines] were left out, and big is the
	// size of a document past [MaxSourceBytes], which is not loaded at all.
	more, big int
}

// prepare reads a document into lines. Nothing here is proportional to more
// than [MaxSourceBytes] of input, and no line holds more than
// [MaxSourceLineRunes] runes.
func prepare(d Document) sourceDoc {
	doc := sourceDoc{uri: d.URI}
	if len(d.Text) > MaxSourceBytes {
		doc.big = len(d.Text)

		return doc
	}
	doc.digest = v1.ContentDigest(d.Text)

	text := strings.ToValidUTF8(string(d.Text), "�")
	for text != "" {
		if len(doc.lines) >= MaxSourceLines {
			doc.more = strings.Count(text, "\n")
			if !strings.HasSuffix(text, "\n") {
				doc.more++
			}

			break
		}
		line, rest, _ := strings.Cut(text, "\n")
		text = rest
		safe, cut := sanitizeLine(strings.TrimSuffix(line, "\r"))
		doc.lines = append(doc.lines, safe)
		doc.cut = append(doc.cut, cut)
	}

	return doc
}

// sanitizeLine makes a line of source safe to put on a terminal: tabs become
// spaces to the next stop, a control character is spelled the way Go writes it
// (`\x1b`), a character that reorders the text around it is spelled as its
// code point, and a line past [MaxSourceLineRunes] is cut. It reports whether
// it cut.
func sanitizeLine(line string) (string, bool) {
	var b strings.Builder
	col, runes := 0, 0
	for _, r := range line {
		var spelled string
		switch {
		case r == '\t':
			spelled = strings.Repeat(" ", sourceTabWidth-col%sourceTabWidth)
		case unicode.IsControl(r):
			spelled = ui.EscapeControl(string(r))
		case reorders(r):
			spelled = fmt.Sprintf("\\u%04x", r)
		default:
			spelled = string(r)
		}
		// The bound is checked on what this character becomes, not before it
		// is spelled: a control character near the end is four cells long.
		n := len(spelled)
		if r != '\t' && !unicode.IsControl(r) && !reorders(r) {
			n = 1
		}
		if runes+n > MaxSourceLineRunes {
			return b.String(), true
		}
		b.WriteString(spelled)
		runes += n
		if r == '\t' || unicode.IsControl(r) || reorders(r) {
			col += n
		} else {
			col += max(1, lipgloss.Width(spelled))
		}
	}

	return b.String(), false
}

// reorders reports a character that changes the order text is displayed in: the
// bidirectional controls, which can make a line read as something it is not.
func reorders(r rune) bool {
	return r == 0x061C || r == 0x200E || r == 0x200F || (r >= 0x202A && r <= 0x202E) || (r >= 0x2066 && r <= 0x2069)
}

// Source is the source pane's state: the documents, which one is shown, the
// selected line, and whether the person has taken the view from the run.
//
// It follows the held frame the way [Flow] does: the view centres on the held
// lines and the selection follows them until the person moves either, and a
// movement of the run gives the view back. A Source is used from one goroutine,
// the screen's.
type Source struct {
	docs []sourceDoc

	// Selected is the 1-based selected line of the shown document, or zero.
	Selected int

	// Scrolled reports that the person moved the view, so it stays where Top
	// puts it instead of following the held lines. Top is the first line drawn
	// then, from zero.
	Scrolled bool
	Top      int

	// shown is the index into the source map's documents of the last document
	// the held step was in, drawn again while the run holds no step.
	shown int

	heldSeen int

	// index is the location of each site of the map it was built for.
	indexFor *v1.DebugSourceMap
	index    map[string]*v1.DebugSourceLocation
}

// NewSource holds the documents the screen was given, as many as
// [MaxSourceDocuments].
func NewSource(documents []Document) *Source {
	s := &Source{}
	for _, d := range documents[:min(len(documents), MaxSourceDocuments)] {
		s.docs = append(s.docs, prepare(d))
	}

	return s
}

// Follow gives the view back to the run.
func (s *Source) Follow() {
	if s != nil {
		s.Scrolled = false
	}
}

// indexOf is the location of each site the map names, the first entry of a
// site winning as it does where a target decorates its frames.
func (s *Source) indexOf(sm *v1.DebugSourceMap) map[string]*v1.DebugSourceLocation {
	if s.indexFor != sm || s.index == nil {
		s.indexFor, s.index = sm, make(map[string]*v1.DebugSourceLocation, len(sm.GetEntries()))
		for _, entry := range sm.GetEntries() {
			key := v1.DebugSiteKey(entry.GetSite())
			if _, seen := s.index[key]; !seen {
				s.index[key] = entry.GetLocation()
			}
		}
	}

	return s.index
}

// sourceFace is what the pane draws for a frame.
type sourceFace struct {
	// doc is the text to draw, or nil where the pane draws the step's address.
	doc *sourceDoc

	// uri and mapDoc name the document in the source map.
	uri    string
	mapDoc int

	// lo and hi are the held lines, zero where the run holds no mapped step.
	lo, hi int

	// why is the sentence that says why doc is nil, and note what is worth
	// saying under the text.
	why, note string

	// address is the held step's address.
	address string
}

// heldAddress is the address of the step the run is held before, or "".
func heldAddress(f flowdebug.Frame) string {
	if f.Snapshot.GetState() != v1.DebugRunState_DEBUG_RUN_STATE_HELD {
		return ""
	}
	occurrence := f.Snapshot.GetOccurrence()

	return cmp.Or(occurrence.GetAddress(), strings.Join(occurrence.GetSite().GetPath(), "/"))
}

// heldSite is the site of the held step, or nil.
func heldSite(f flowdebug.Frame) *v1.DebugSite {
	if f.Snapshot.GetState() != v1.DebugRunState_DEBUG_RUN_STATE_HELD {
		return nil
	}
	if frames := f.Snapshot.GetFrames(); len(frames) > 0 && frames[0].GetOccurrence().GetSite() != nil {
		return frames[0].GetOccurrence().GetSite()
	}

	return f.Snapshot.GetOccurrence().GetSite()
}

// heldLocation is where the map puts the held step, or nil where it puts it
// nowhere (or the run holds none).
func (s *Source) heldLocation(f flowdebug.Frame) *v1.DebugSourceLocation {
	sm := f.SourceMap
	site := heldSite(f)
	if sm == nil || site == nil {
		return nil
	}
	var loc *v1.DebugSourceLocation
	if frames := f.Snapshot.GetFrames(); len(frames) > 0 {
		loc = frames[0].GetSource()
	}
	if loc == nil {
		loc = s.indexOf(sm)[v1.DebugSiteKey(site)]
	}
	if document := int(loc.GetDocument()); loc.GetRange().GetStartLine() == 0 || document < 0 || document >= len(sm.GetDocuments()) {
		return nil
	}

	return loc
}

// load is the text of a map document, or the reason there is none.
func (s *Source) load(d *v1.DebugSourceDocument) (*sourceDoc, string) {
	name := ui.EscapeControl(flowdebug.SourceName(d.GetUri()))
	for i := range s.docs {
		doc := &s.docs[i]
		if !flowdebug.SameSourceURI(doc.uri, d.GetUri()) {
			continue
		}
		switch {
		case doc.big > 0:
			return nil, fmt.Sprintf("Lines are not shown: %s is %d bytes, past the %d this pane reads; showing the step's address.", name, doc.big, MaxSourceBytes)
		case doc.digest != d.GetDigest():
			return nil, fmt.Sprintf("Lines are not shown: %s is not the bytes its source map was made from (digest mismatch); showing the step's address.", name)
		}

		return doc, ""
	}

	return nil, fmt.Sprintf("Lines are not shown: the text of %s was not given to the screen; showing the step's address.", name)
}

// face decides what the pane draws for a frame.
func (s *Source) face(f flowdebug.Frame) sourceFace {
	face := sourceFace{mapDoc: -1, address: heldAddress(f)}
	sm := f.SourceMap
	if sm == nil {
		switch {
		case len(s.docs) == 0:
			face.why = "No source: pass --program <Flowfile> to see its lines; showing the step's address."
		default:
			face.why = fmt.Sprintf("Lines are not shown: %s does not match the program this run executes (digest mismatch), "+
				"so a line could mark the wrong step; showing the step's address.", ui.EscapeControl(flowdebug.SourceName(s.docs[0].uri)))
		}

		return face
	}

	loc := s.heldLocation(f)
	face.mapDoc = s.shown
	if loc != nil {
		face.mapDoc = int(loc.GetDocument())
	}
	if face.mapDoc < 0 || face.mapDoc >= len(sm.GetDocuments()) {
		face.mapDoc = 0
	}
	mapped := sm.GetDocuments()
	if len(mapped) == 0 {
		face.why = "Lines are not shown: the source map names no document; showing the step's address."

		return face
	}
	face.uri = mapped[face.mapDoc].GetUri()

	doc, why := s.load(mapped[face.mapDoc])
	if doc == nil {
		face.why = why

		return face
	}
	face.doc = doc

	if loc != nil {
		face.lo = int(loc.GetRange().GetStartLine())
		face.hi = max(face.lo, int(loc.GetRange().GetEndLine()))
		switch {
		case face.lo < 1:
			face.lo, face.hi = 0, 0
		case face.lo > len(doc.lines):
			// Past the lines kept: marking the last one would mark a line the
			// step is not on.
			face.lo, face.hi = 0, 0
			face.note = fmt.Sprintf("held at %s, which is past the %d lines shown",
				ui.EscapeControl(f.RedactText(face.address)), len(doc.lines))
		default:
			face.hi = min(face.hi, len(doc.lines))
		}
	} else if site := heldSite(f); site != nil {
		face.note = fmt.Sprintf("held at %s:%s, which the source map has no line for",
			ui.EscapeControl(f.RedactText(site.GetWorkflow())), ui.EscapeControl(f.RedactText(face.address)))
	}

	return face
}

// Apply folds a new frame in: the shown document follows the held step across
// a `call:`, and while the person has not taken the view the selection follows
// the held line.
func (s *Source) Apply(f flowdebug.Frame) {
	if s == nil {
		return
	}
	loc := s.heldLocation(f)
	held := 0
	if loc != nil {
		held = int(loc.GetRange().GetStartLine())
		if document := int(loc.GetDocument()); document != s.shown {
			s.shown, s.Selected, s.Scrolled, s.Top, s.heldSeen = document, 0, false, 0, 0
		}
	}
	if held != 0 && held != s.heldSeen && (s.Selected == 0 || !s.Scrolled) {
		s.Selected = held
	}
	s.heldSeen = held
}

// bodyRows is how many text lines a pane h lines tall shows for a face.
func (face sourceFace) bodyRows(h int) int {
	rows := h - 1
	if footerNotes(face) != "" {
		rows--
	}

	return max(0, rows)
}

// start is the first line drawn, from zero.
func (s *Source) start(face sourceFace, rows int) int {
	rows = max(1, rows)
	last := max(0, len(face.doc.lines)-rows)
	if s.Scrolled {
		return max(0, min(s.Top, last))
	}
	if face.lo == 0 {
		return 0
	}
	span := face.hi - face.lo + 1

	return max(0, min(face.lo-1-max(0, rows-span)/2, last))
}

// scrollBy moves the view by delta lines and takes it from the run.
func (s *Source) scrollBy(f flowdebug.Frame, rows, delta int) {
	face := s.face(f)
	if face.doc == nil {
		return
	}
	if !s.Scrolled {
		s.Scrolled, s.Top = true, s.start(face, rows)
	}
	s.Top = max(0, min(s.Top+delta, max(0, len(face.doc.lines)-max(1, rows))))
}

// moveTo selects a line and keeps it in view, taking the view from the run.
func (s *Source) moveTo(f flowdebug.Frame, rows, line int) {
	face := s.face(f)
	if face.doc == nil || len(face.doc.lines) == 0 {
		return
	}
	rows = max(1, rows)
	s.Selected = max(1, min(line, len(face.doc.lines)))
	if !s.Scrolled {
		s.Scrolled, s.Top = true, s.start(face, rows)
	}
	switch {
	case s.Selected-1 < s.Top:
		s.Top = s.Selected - 1
	case s.Selected-1 >= s.Top+rows:
		s.Top = s.Selected - rows
	}
	s.Top = max(0, min(s.Top, max(0, len(face.doc.lines)-rows)))
}

// marks are the lines of the shown document that carry an armed breakpoint, and
// the id of the breakpoint `delete` takes to remove it. A breakpoint is on the
// line the map puts any site it armed on, or the line it was set on.
func (s *Source) marks(f flowdebug.Frame, face sourceFace) map[int]string {
	marks := map[int]string{}
	if face.doc == nil || f.Snapshot == nil {
		return marks
	}
	idx := s.indexOf(f.SourceMap)
	set := func(line int, id string) {
		if _, ok := marks[line]; !ok && line >= 1 && line <= len(face.doc.lines) && len(marks) < flowdebug.MaxOverlayNodes {
			marks[line] = id
		}
	}
	// visits bounds the entries looked at, not only the marks kept.
	visits := 0
	for _, bp := range f.Snapshot.GetBreakpoints() {
		definition := bp.GetDefinition()
		if !bp.GetVerified() || definition.GetLogMessage() != "" {
			continue
		}
		if line := definition.GetLine(); line != nil && flowdebug.SameSourceURI(line.GetUri(), face.uri) {
			set(int(line.GetLine()), bp.GetId())
		}
		if loc := bp.GetSource(); loc != nil && int(loc.GetDocument()) == face.mapDoc {
			set(int(loc.GetRange().GetStartLine()), bp.GetId())
		}
		// A durable run's snapshot keeps the id and the step a line was resolved
		// to, not the line: an id this screen gave a line of this document says
		// which line it was.
		if line, ok := lineOfBreakpointID(bp.GetId(), face.uri); ok {
			set(line, bp.GetId())
		}
		for _, site := range bp.GetSites() {
			if visits++; visits > flowdebug.MaxOverlayNodes {
				return marks
			}
			if loc := idx[v1.DebugSiteKey(site)]; loc != nil && int(loc.GetDocument()) == face.mapDoc {
				set(int(loc.GetRange().GetStartLine()), bp.GetId())
			}
		}
	}

	return marks
}

// lineOfBreakpointID is the line an id names when it is the id
// [flowdebug.LineBreakpointID] gives that line of the document uri, recomputed
// so an id that only looks like one is not taken for it.
func lineOfBreakpointID(id, uri string) (int, bool) {
	_, tail, ok := strings.Cut(id, ":")
	if !ok {
		return 0, false
	}
	_, number, ok := strings.Cut(tail, ":")
	if !ok {
		return 0, false
	}
	// 31 bits: the line fits an int on every platform and a uint32 as well.
	line, err := strconv.ParseUint(number, 10, 31)
	if err != nil || line == 0 {
		return 0, false
	}

	return int(line), flowdebug.LineBreakpointID(uri, uint32(line)) == id
}

// ---- drawing ----

// SourceView is the source pane: the heading and the document the held step is
// in, windowed to the height and kept around the held lines unless the person
// moved the view. Where the lines cannot be trusted it is the step's address
// and the sentence that says why.
//
// Each text line registers a [pane.KindRow] hit under sourcePrefix and its line
// number, and its gutter one under gutterPrefix.
func SourceView(src *Source, f flowdebug.Frame, loaded bool, o pane.Options) string {
	if src == nil {
		src = NewSource(nil)
	}
	if !loaded {
		return pane.Heading(paneSource, "", o.Width, o)
	}

	face := src.face(f)
	if face.doc == nil {
		return pane.Heading(paneSource, "addresses only", o.Width, o) + "\n" + addressBody(face, f, o)
	}

	heading := pane.Heading(paneSource, headingNote(face, src), o.Width, o)
	rows := face.bodyRows(o.Height)
	if len(face.doc.lines) == 0 {
		return heading + "\n" + o.Theme.Muted.Render("  the document is empty")
	}

	numW := max(minNumberWidth, len(strconv.Itoa(len(face.doc.lines))))
	gutterW := 2 + numW + 1
	textW := max(0, o.Width-gutterW)
	marks := src.marks(f, face)
	start := src.start(face, rows)

	lines := make([]string, 0, o.Height)
	lines = append(lines, heading)
	for i := start; i < min(len(face.doc.lines), start+rows); i++ {
		n := i + 1
		lines = append(lines, ui.Trim(sourceLine(src, face, n, marks, numW, textW, o), o.Width))
		y := o.Origin.Y + len(lines) - 1
		o.Hits.Add(pane.Rect{X: o.Origin.X, Y: y, W: o.Width, H: 1}, sourcePrefix+strconv.Itoa(n), pane.KindRow)
		o.Hits.Add(pane.Rect{X: o.Origin.X, Y: y, W: min(gutterW, o.Width), H: 1}, gutterPrefix+strconv.Itoa(n), pane.KindRow)
	}
	for len(lines) < 1+rows {
		lines = append(lines, "")
	}
	if notes := footerNotes(face); notes != "" && o.Height > 1 {
		lines = append(lines, o.Theme.Warning.Render(ui.Trim(ui.EscapeControl(notes), o.Width)))
	}

	return strings.Join(lines, "\n")
}

// footerNotes is the one line under the text, or empty.
func footerNotes(face sourceFace) string {
	var notes []string
	if face.note != "" {
		notes = append(notes, face.note)
	}
	if face.doc != nil && face.doc.more > 0 {
		notes = append(notes, fmt.Sprintf("%d more lines not shown", face.doc.more))
	}

	return strings.Join(notes, "; ")
}

// headingNote is the document and line the heading names.
func headingNote(face sourceFace, src *Source) string {
	name := flowdebug.SourceName(face.uri)
	switch {
	case face.lo > 0:
		return fmt.Sprintf("%s:%d", name, face.lo)
	case src.Selected > 0:
		return fmt.Sprintf("%s:%d", name, src.Selected)
	default:
		return name
	}
}

// addressBody is what the pane draws where it has no lines: why, and the held
// step's address.
func addressBody(face sourceFace, f flowdebug.Frame, o pane.Options) string {
	text := pane.WrapWords(face.why, max(1, o.Width-2))
	lines := make([]string, 0, o.Height)
	for line := range strings.SplitSeq(text, "\n") {
		lines = append(lines, o.Theme.Muted.Render("  "+ui.EscapeControl(line)))
	}
	lines = append(lines, "")
	if face.address == "" {
		lines = append(lines, o.Theme.Muted.Render("  the run is not held; its step shows here at the next stop"))
	} else {
		lines = append(lines, "  held at "+o.Theme.Strong.Render(ui.EscapeControl(f.RedactText(face.address))))
	}
	if len(lines) > max(0, o.Height-1) {
		lines = lines[:max(0, o.Height-1)]
	}
	for i, line := range lines {
		lines[i] = ui.Trim(line, o.Width)
	}

	return strings.Join(lines, "\n")
}

// sourceLine draws line n: the marks, the number, and the text.
func sourceLine(src *Source, face sourceFace, n int, marks map[int]string, numW, textW int, o pane.Options) string {
	held := face.lo > 0 && n >= face.lo && n <= face.hi
	selected := n == src.Selected

	mark := " "
	switch {
	case held && n == face.lo:
		mark = o.Theme.Strong.Render(o.Symbols.Running)
	case selected:
		mark = cursor(o)
	case held:
		mark = o.Theme.Strong.Render(o.Symbols.Rail)
	}
	armed := " "
	if _, ok := marks[n]; ok {
		armed = o.Theme.Danger.Render(o.Symbols.Bullet)
	}

	number := fmt.Sprintf("%*d", numW, n)
	switch {
	case selected && o.Focused:
		number = o.Theme.Accent.Render(number)
	case selected || held:
		number = o.Theme.Strong.Render(number)
	default:
		number = o.Theme.Muted.Render(number)
	}

	text := clipLine(face.doc.lines[n-1], textW, o.Symbols.Ellipsis, face.doc.cut[n-1])
	switch {
	case held:
		text = o.Theme.Strong.Render(text)
	default:
		text = highlight(text, o)
	}

	return mark + armed + number + " " + text
}

// cursor is the mark of the selected line: the arrow, in the accent role while
// the pane has focus.
func cursor(o pane.Options) string {
	mark := cmp.Or(strings.TrimSpace(o.Symbols.Arrow), ">")
	if o.Focused {
		return o.Theme.Accent.Render(mark)
	}

	return o.Theme.Muted.Render(mark)
}

// clipLine cuts text to width cells, ending it in the ellipsis where it cut, or
// where it was cut before it got here.
func clipLine(text string, width int, ellipsis string, cut bool) string {
	if width <= 0 {
		return ""
	}
	tail := lipgloss.Width(ellipsis)
	if lipgloss.Width(text) <= width && !(cut && width > tail) {
		return text
	}
	var b strings.Builder
	used := 0
	for _, r := range text {
		w := lipgloss.Width(string(r))
		if used+w+tail > width {
			break
		}
		b.WriteRune(r)
		used += w
	}
	if width < tail {
		return ""
	}

	return b.String() + ellipsis
}

// highlight is the light touch the pane gives a line it draws unmarked: a
// comment line is muted and a `${…}` expression is in the accent role. It runs
// on text already cut to the pane, so it never splits a styled run.
func highlight(text string, o pane.Options) string {
	if strings.HasPrefix(strings.TrimLeft(text, " "), "#") {
		return o.Theme.Muted.Render(text)
	}
	if !strings.Contains(text, "${") {
		return text
	}

	var b strings.Builder
	for text != "" {
		before, rest, found := strings.Cut(text, "${")
		b.WriteString(before)
		if !found {
			break
		}
		end, depth := len(rest), 1
		for i, r := range rest {
			switch r {
			case '{':
				depth++
			case '}':
				if depth--; depth == 0 {
					end = i + 1
				}
			}
			if depth == 0 {
				break
			}
		}
		b.WriteString(o.Theme.Accent.Render("${" + rest[:end]))
		text = rest[end:]
	}

	return b.String()
}

// ---- keys, clicks and commands ----

// sourceRows is how many text lines the source pane shows now, or zero when it
// is not drawn.
func (m Model) sourceRows() int {
	rect, ok := m.cell(paneSource)
	if !ok {
		return 0
	}
	if !m.screen.Loaded {
		return max(0, rect.H-1)
	}

	return m.screen.Source.face(m.screen.Frame).bodyRows(rect.H)
}

// navigateSource handles a navigation key in the source pane.
func (m Model) navigateSource(name string) (tea.Model, tea.Cmd) {
	src, frame := m.screen.Source, m.screen.Frame
	if !m.screen.Loaded || src == nil {
		return m, nil
	}
	rows := max(1, m.sourceRows())
	at := src.Selected
	if at == 0 {
		at = max(1, src.face(frame).lo)
	}

	switch name {
	case bindUp:
		src.moveTo(frame, rows, at-1)
	case bindDown:
		src.moveTo(frame, rows, at+1)
	case bindPageUp:
		src.moveTo(frame, rows, at-rows)
	case bindPageDown:
		src.moveTo(frame, rows, at+rows)
	case bindHome:
		src.moveTo(frame, rows, 1)
	case bindEnd:
		src.moveTo(frame, rows, 1<<30)
	}

	return m, nil
}

// clickSource handles a click on a line of the source, or on its gutter.
func (m Model) clickSource(id string, gutter bool) (tea.Model, tea.Cmd) {
	line, err := strconv.Atoi(id)
	if err != nil || line < 1 || !m.screen.Loaded {
		return m, nil
	}
	m.setFocus(paneSource)
	m.screen.Source.Selected = line
	if gutter {
		return m.sourceBreak(line)
	}

	return m, nil
}

// sourceBreak arms a breakpoint on a line of the shown document, or removes the
// one that is there. It refuses, with a toast and nothing sent, where the lines
// are not trusted, where the front does not answer `break`, and where the map
// has no step written on the line.
func (m Model) sourceBreak(line int) (tea.Model, tea.Cmd) {
	frame, src := m.screen.Frame, m.screen.Source
	if !m.offers("break") {
		m.toast(ui.ToneWarning, "this front does not answer break, so a line cannot be armed")

		return m, nil
	}
	face := src.face(frame)
	if face.doc == nil {
		m.toast(ui.ToneWarning, face.why)

		return m, nil
	}
	if line < 1 || line > len(face.doc.lines) || line > math.MaxInt32 {
		m.toast(ui.ToneWarning, "select a line of the source first")

		return m, nil
	}

	if id, armed := src.marks(frame, face)[line]; armed {
		if !m.offers("delete") {
			m.toast(ui.ToneWarning, "this front does not answer delete")

			return m, nil
		}
		if strings.ContainsFunc(id, func(r rune) bool { return unicode.IsControl(r) || unicode.IsSpace(r) }) {
			m.toast(ui.ToneWarning, "that breakpoint's name cannot be typed; delete it in the console")

			return m, nil
		}
		// Removing what a line set is as unreplayable as setting it was.
		driver, line := m.cfg.Driver, "delete "+id
		cmd := m.start(line, true, func(ctx context.Context) (*flowdebug.DriveResult, error) {
			return driver.Do(ctx, line)
		})

		return m, cmd
	}

	if _, _, reason := flowdebug.SiteAtLine(frame.SourceMap, &v1.DebugSourceLine{Uri: face.uri, Line: uint32(line)}); reason != "" {
		m.toast(ui.ToneWarning, reason)

		return m, nil
	}
	driver, uri := m.cfg.Driver, face.uri
	echo := fmt.Sprintf("break %s:%d", ui.EscapeControl(flowdebug.SourceName(uri)), line)
	cmd := m.start(echo, true, func(ctx context.Context) (*flowdebug.DriveResult, error) {
		return driver.BreakLine(ctx, uri, uint32(line))
	})

	return m, cmd
}
