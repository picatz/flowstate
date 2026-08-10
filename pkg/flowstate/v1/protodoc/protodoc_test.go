package protodoc

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// The embedded artifact is the whole reason this package can answer anything, so
// it is checked before anything that reads it: a set that does not parse, or one
// built without source info, would otherwise show up as every other test in this
// file quietly finding no prose.
func TestEmbeddedDescriptorSetCarriesSourceInfo(t *testing.T) {
	if len(rawDescriptorSet) == 0 {
		t.Fatal("embedded descriptor set is empty; run the buf build step in the Makefile's check target")
	}
	// A sanity bound, not a budget. The schema is one file and its comments; if
	// this artifact ever arrives at megabytes it is carrying something it was
	// not meant to, most likely its imports.
	if len(rawDescriptorSet) > 4<<20 {
		t.Errorf("embedded descriptor set is %d bytes, which is larger than this schema can account for", len(rawDescriptorSet))
	}

	set := &descriptorpb.FileDescriptorSet{}
	if err := proto.Unmarshal(rawDescriptorSet, set); err != nil {
		t.Fatalf("embedded descriptor set does not parse: %v", err)
	}
	if len(set.GetFile()) == 0 {
		t.Fatal("embedded descriptor set holds no files")
	}

	var found *descriptorpb.FileDescriptorProto
	for _, file := range set.GetFile() {
		if file.GetName() == "flowstate/v1/flowstate.proto" {
			found = file
		}
	}
	if found == nil {
		t.Fatal("embedded descriptor set does not hold flowstate/v1/flowstate.proto")
	}
	if len(found.GetSourceCodeInfo().GetLocation()) == 0 {
		t.Fatal("flowstate/v1/flowstate.proto carries no SourceCodeInfo; the artifact must be built without --exclude-source-info")
	}
}

func TestFilesResolves(t *testing.T) {
	reg, err := Files()
	if err != nil {
		t.Fatalf("Files: %v", err)
	}
	if _, err := reg.FindDescriptorByName("flowstate.v1.WorkflowService"); err != nil {
		t.Fatalf("WorkflowService not found in the embedded schema: %v", err)
	}
}

func TestCommentFindsProse(t *testing.T) {
	for _, name := range []protoreflect.FullName{
		"flowstate.v1.WorkflowService",
		"flowstate.v1.WorkflowService.Signal",
		"flowstate.v1.RunRequest",
		"flowstate.v1.ListResponse.next_page_token",
	} {
		got, ok := Comment(name)
		if !ok {
			t.Errorf("Comment(%q) = _, false; want prose", name)
			continue
		}
		if strings.TrimSpace(got) == "" {
			t.Errorf("Comment(%q) reported ok with empty prose", name)
		}
		if strings.Contains(got, "//") {
			t.Errorf("Comment(%q) still carries comment markers: %q", name, got)
		}
	}
}

// Fail closed: every way of asking for something that is not there answers the
// same way, and none of them panics.
func TestCommentFailsClosed(t *testing.T) {
	for _, name := range []protoreflect.FullName{
		"",
		"flowstate.v1.NoSuchMessage",
		"flowstate.v1.WorkflowService.NoSuchMethod",
		"not a name at all",
		"google.protobuf.Struct", // real, but not in this set
	} {
		got, ok := Comment(name)
		if ok || got != "" {
			t.Errorf("Comment(%q) = %q, %v; want \"\", false", name, got, ok)
		}
	}

	if got, ok := CommentOf(nil); ok || got != "" {
		t.Errorf("CommentOf(nil) = %q, %v; want \"\", false", got, ok)
	}
}

func TestMethod(t *testing.T) {
	want, ok := Comment("flowstate.v1.WorkflowService.Signal")
	if !ok {
		t.Fatal("Signal has no comment; this test needs a documented RPC")
	}
	got, ok := Method("flowstate.v1.WorkflowService", "Signal")
	if !ok || got != want {
		t.Errorf("Method = %q, %v; want the same prose as Comment", got, ok)
	}

	for _, tc := range []struct {
		service protoreflect.FullName
		method  protoreflect.Name
	}{
		{"", "Signal"},
		{"flowstate.v1.WorkflowService", ""},
		{"flowstate.v1.NoSuchService", "Signal"},
	} {
		if got, ok := Method(tc.service, tc.method); ok || got != "" {
			t.Errorf("Method(%q, %q) = %q, %v; want \"\", false", tc.service, tc.method, got, ok)
		}
	}
}

// A descriptor from the linked-in registry carries no SourceCodeInfo, and the
// package promises to say so rather than to report the symbol as undocumented in
// a way a caller could mistake for the schema's own silence.
func TestCommentOfRejectsDescriptorsWithoutSourceInfo(t *testing.T) {
	desc := (&descriptorpb.FileDescriptorSet{}).ProtoReflect().Descriptor()
	if got, ok := CommentOf(desc); ok || got != "" {
		t.Errorf("CommentOf(linked-in descriptor) = %q, %v; want \"\", false", got, ok)
	}
}

func TestNormalize(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want string
		ok   bool
	}{
		{
			name: "empty",
			raw:  "",
			ok:   false,
		},
		{
			name: "whitespace only",
			raw:  " \n \n",
			ok:   false,
		},
		{
			name: "leading marker space is stripped",
			raw:  " One line.\n",
			want: "One line.",
			ok:   true,
		},
		{
			name: "hard wrapping inside a paragraph is unwrapped",
			raw:  " One line\n that was wrapped.\n",
			want: "One line that was wrapped.",
			ok:   true,
		},
		{
			name: "paragraphs are preserved",
			raw:  " First para.\n\n Second para.\n",
			want: "First para.\n\nSecond para.",
			ok:   true,
		},
		{
			name: "several blank lines still make one break",
			raw:  " First.\n\n\n Second.\n",
			want: "First.\n\nSecond.",
			ok:   true,
		},
		{
			name: "list items keep their own lines",
			raw:  " Bounds:\n\n - one\n - two\n\n And after.\n",
			want: "Bounds:\n\n- one\n- two\n\nAnd after.",
			ok:   true,
		},
		{
			name: "numbered items keep their own lines",
			raw:  " Steps:\n 1. first\n 2. second\n",
			want: "Steps:\n1. first\n2. second",
			ok:   true,
		},
		{
			name: "indented block keeps its shape",
			raw:  " Example:\n\n     flow run local\n\n Done.\n",
			want: "Example:\n\n    flow run local\n\nDone.",
			ok:   true,
		},
		{
			name: "symbol links become backticked names",
			raw:  " See [ValidationReport] and [flowstate.v1.RunRequest.workflow].\n",
			want: "See `ValidationReport` and `flowstate.v1.RunRequest.workflow`.",
			ok:   true,
		},
		{
			name: "bracketed prose is left alone",
			raw:  " A citation [1] and a note [see below] stay put.\n",
			want: "A citation [1] and a note [see below] stay put.",
			ok:   true,
		},
		{
			name: "brackets inside a code span are that span's text",
			raw:  " The type `list[string]` stays one span, and [ValidationReport] after it still links.\n",
			want: "The type `list[string]` stays one span, and `ValidationReport` after it still links.",
			ok:   true,
		},
		{
			name: "a bracket after a closed span links again",
			raw:  " First `code` then [RunRequest] links.\n",
			want: "First `code` then `RunRequest` links.",
			ok:   true,
		},
		{
			name: "unbalanced bracket is left alone",
			raw:  " An open [bracket with no close.\n",
			want: "An open [bracket with no close.",
			ok:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := normalize(tc.raw)
			if ok != tc.ok {
				t.Fatalf("normalize(%q) ok = %v; want %v", tc.raw, ok, tc.ok)
			}
			if got != tc.want {
				t.Errorf("normalize(%q) =\n%q\nwant\n%q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestFirstSentence(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"one sentence", "Run starts a workflow.", "Run starts a workflow."},
		{"stops at the first", "Run starts a workflow. It returns an id.", "Run starts a workflow."},
		{"stops at the first paragraph", "Run starts it.\n\nMore below.", "Run starts it."},
		{"unwraps the first paragraph", "Run starts a\nworkflow that runs.", "Run starts a workflow that runs."},
		{"no terminator returns the paragraph", "Run starts a workflow", "Run starts a workflow"},
		{"a dotted name is not a sentence end", "Reads flowstate.v1.RunRequest and stops. Then more.", "Reads flowstate.v1.RunRequest and stops."},
		{"an abbreviation is not a sentence end", "Bounded, e.g. by cost. Then more.", "Bounded, e.g. by cost."},
		{"a period inside backticks is not a sentence end", "Run `a.b.c` now. Then more.", "Run `a.b.c` now."},
		{"an initial is not a sentence end", "Named after A. Turing here. Then more.", "Named after A. Turing here."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := FirstSentence(tc.in); got != tc.want {
				t.Errorf("FirstSentence(%q) = %q; want %q", tc.in, got, tc.want)
			}
		})
	}
}

// The prose the package serves has to survive the round trip from the schema, so
// one real comment is checked end to end rather than only through normalize.
func TestRealCommentIsNormalized(t *testing.T) {
	got, ok := Comment("flowstate.v1.WorkflowService.SignalWithStart")
	if !ok {
		t.Fatal("SignalWithStart has no comment")
	}
	if strings.Contains(got, "[SignalWithStartRequest]") {
		t.Errorf("godoc link left untranslated in %q", got)
	}
	if !strings.Contains(got, "`SignalWithStartRequest`") {
		t.Errorf("godoc link not translated to a backticked name in %q", got)
	}
	if strings.HasPrefix(got, " ") {
		t.Errorf("comment retains its marker space: %q", got)
	}

	first := FirstSentence(got)
	if !strings.HasPrefix(got, first) {
		t.Errorf("FirstSentence(%q) = %q is not a prefix of the comment", got, first)
	}
	if strings.Contains(first, "\n") {
		t.Errorf("FirstSentence returned more than one line: %q", first)
	}
}
