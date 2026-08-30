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

	// Every file of the schema, named. Counting instead — or asking only that
	// some flowstate/v1 file is present — passes on an artifact holding one
	// file and missing the other eleven, which is the failure this is for:
	// reports.proto is reached by no other test here, so a descriptorset that
	// silently lost it would cost the documentation of every message in it and
	// nothing would say so.
	want := map[string]bool{
		"flowstate/v1/audit.proto":         false,
		"flowstate/v1/authorization.proto": false,
		"flowstate/v1/catalog.proto":       false,
		"flowstate/v1/debug.proto":         false,
		"flowstate/v1/diagnostics.proto":   false,
		"flowstate/v1/identity.proto":      false,
		"flowstate/v1/reports.proto":       false,
		"flowstate/v1/run.proto":           false,
		"flowstate/v1/schedule.proto":      false,
		"flowstate/v1/service.proto":       false,
		"flowstate/v1/signal.proto":        false,
		"flowstate/v1/task.proto":          false,
		"flowstate/v1/trigger.proto":       false,
		"flowstate/v1/value.proto":         false,
		"flowstate/v1/workflow.proto":      false,
	}

	for _, file := range set.GetFile() {
		name := file.GetName()
		if !strings.HasPrefix(name, "flowstate/v1/") {
			continue
		}
		if _, expected := want[name]; !expected {
			t.Errorf("embedded descriptor set holds unexpected schema file %s; add it here if the schema gained a file", name)
			continue
		}
		want[name] = true
		if len(file.GetSourceCodeInfo().GetLocation()) == 0 {
			t.Fatalf("%s carries no SourceCodeInfo; the artifact must be built without --exclude-source-info", name)
		}
	}

	for name, seen := range want {
		if !seen {
			t.Errorf("embedded descriptor set is missing %s; every message it declares would lose its prose", name)
		}
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

func TestManualAllowedPrincipalsDocumentationIsIssuerQualified(t *testing.T) {
	comment, ok := Comment("flowstate.v1.ManualTrigger.allowed_principals")
	if !ok {
		t.Fatal("manual allowed_principals has no descriptor documentation")
	}
	for _, want := range []string{"<issuer>#<subject>", "Principal.ID", "Bare subjects are invalid"} {
		if !strings.Contains(comment, want) {
			t.Errorf("manual allowed_principals descriptor documentation does not contain %q:\n%s", want, comment)
		}
	}
}

// A leading comment belongs to the declaration immediately below it. Presence
// alone did not catch RunState's prose being copied above WorkloadIdentity,
// where generated API documentation attributed both descriptions to the
// identity message and left RunState unnamed.
func TestDocumentedTopLevelDeclarationsNameThemselves(t *testing.T) {
	files, err := Files()
	if err != nil {
		t.Fatalf("Files: %v", err)
	}
	check := func(declaration protoreflect.Descriptor) {
		name := declaration.FullName()
		comment, ok := CommentOf(declaration)
		if !ok {
			if declaration.ParentFile().Package() == "flowstate.v1" {
				t.Errorf("Comment(%q) = _, false; want prose", name)
			}
			return
		}
		if want := string(name.Name()) + " "; !strings.HasPrefix(comment, want) {
			t.Errorf("Comment(%q) starts with %q; want its own declaration name %q", name, FirstSentence(comment), name.Name())
		}
	}
	files.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		for i, declarations := 0, file.Messages(); i < declarations.Len(); i++ {
			check(declarations.Get(i))
		}
		for i, declarations := 0, file.Enums(); i < declarations.Len(); i++ {
			check(declarations.Get(i))
		}
		for i, declarations := 0, file.Services(); i < declarations.Len(); i++ {
			check(declarations.Get(i))
		}
		return true
	})
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
