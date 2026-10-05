package protodoc

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/picatz/flowstate/pkg/flowstate/plugin/v1"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/protodoc/protodocimpl"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// The generated comments are the whole reason this package can answer
// anything, so they are checked before anything that reads them. Every file of
// the linked schema must have registered prose: a file the generator skipped,
// or a new .proto that nobody regenerated for, would otherwise show up as every
// test below quietly finding no prose for it.
//
// The files are taken from protoregistry.GlobalFiles rather than listed, so a
// new schema file is covered the day it is linked. The count is a floor that
// catches the opposite failure, a walk that silently found nothing.
func TestEveryLinkedSchemaFileRegistersItsComments(t *testing.T) {
	var files int
	protoregistry.GlobalFiles.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		if !strings.HasPrefix(file.Path(), "flowstate/") {
			return true
		}
		files++
		if !registersAny(file) {
			t.Errorf("%s registered no comments; run `buf generate` and commit the flowstate_*.doc.pb.go it writes", file.Path())
		}
		return true
	})
	if files < 18 {
		t.Errorf("walked %d schema files; want every flowstate/v1 file and flowstate/plugin/v1/plugin.proto", files)
	}
}

// A name two generated files disagree about is answered by nobody, and Lookup
// stays silent about it on purpose. So the engine's own schema is held to
// agreeing here: two generations of one .proto linked into this test binary, or
// a name declared by two files, would otherwise show up only as prose quietly
// missing from hover, MCP and the reference (#2171). The conflicts are reported
// first, since they are the cause; the floor after them keeps an empty list from
// passing because nothing registered at all.
func TestNoGeneratedCommentIsAmbiguous(t *testing.T) {
	for _, c := range protodocimpl.Conflicts() {
		t.Errorf("ambiguous generated comment, so it reads as no comment: %s", c)
	}
	if _, _, ok := protodocimpl.Lookup("flowstate.v1.RunRequest"); !ok {
		t.Error("flowstate.v1.RunRequest has no usable comment (unregistered or ambiguous), so an empty conflict list would prove nothing")
	}
}

// Every flowstate_*.doc.pb.go in this package must come from a .proto in the
// schema's source tree. buf generate never deletes output whose source is gone,
// so a deleted or renamed .proto would otherwise leave its old file registering
// comments: for declarations that no longer exist, or, after a rename, as a
// second registration that makes every name in it ambiguous. The drift pin
// cannot see such a file, because nothing regenerates it. The source tree is
// read rather than the linked registry, because a deleted .proto's stale .pb.go
// would still be linked and vouch for its stale doc file.
func TestEveryGeneratedDocFileHasASource(t *testing.T) {
	const protoRoot = "../../../../proto"
	want := make(map[string]bool)
	err := fs.WalkDir(os.DirFS(protoRoot), "flowstate", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, ".proto") {
			want[strings.ReplaceAll(strings.TrimSuffix(path, ".proto"), "/", "_")+".doc.pb.go"] = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("reading the schema under %s: %v", protoRoot, err)
	}
	if len(want) == 0 {
		t.Fatalf("found no .proto files under %s/flowstate", protoRoot)
	}

	generated, err := filepath.Glob("*.doc.pb.go")
	if err != nil {
		t.Fatal(err)
	}
	if len(generated) == 0 {
		t.Fatal("found no generated doc files; this test must run in the protodoc package directory")
	}
	for _, name := range generated {
		if !want[name] {
			t.Errorf("%s has no .proto under proto/flowstate; its source was deleted or renamed, so delete it and run `buf generate`", name)
		}
	}
}

// registersAny reports whether any top-level declaration of file has a
// registered comment attributed to that file.
func registersAny(file protoreflect.FileDescriptor) bool {
	var decls []protoreflect.Descriptor
	for i := range file.Messages().Len() {
		decls = append(decls, file.Messages().Get(i))
	}
	for i := range file.Enums().Len() {
		decls = append(decls, file.Enums().Get(i))
	}
	for i := range file.Services().Len() {
		decls = append(decls, file.Services().Get(i))
	}
	for i := range file.Extensions().Len() {
		decls = append(decls, file.Extensions().Get(i))
	}
	for _, d := range decls {
		if _, path, ok := protodocimpl.Lookup(d.FullName()); ok && path == file.Path() {
			return true
		}
	}
	return false
}

// The descriptors this binary links carry no source info, which is the case
// the generated comments exist for: CommentOf must answer for one directly,
// without a caller having to find the same symbol somewhere else by name.
func TestCommentOfAnswersForALinkedDescriptor(t *testing.T) {
	desc := (&flowstatev1.RunRequest{}).ProtoReflect().Descriptor()
	if n := desc.ParentFile().SourceLocations().Len(); n != 0 {
		t.Fatalf("the linked %s carries %d source locations; this test needs one without any", desc.ParentFile().Path(), n)
	}

	got, ok := CommentOf(desc)
	want, wantOK := Comment(desc.FullName())
	if !ok || !wantOK || got != want {
		t.Errorf("CommentOf(linked RunRequest) = %q, %v; want %q, true", got, ok, want)
	}
}

// A descriptor that carries its own comments is described in its own words,
// and a same-named declaration from a different file is not given this
// schema's prose.
func TestCommentOfPrefersItsOwnSourceAndChecksTheFile(t *testing.T) {
	build := func(path, comment string) protoreflect.MessageDescriptor {
		t.Helper()
		fdp := &descriptorpb.FileDescriptorProto{
			Name:        proto.String(path),
			Package:     proto.String("flowstate.v1"),
			Syntax:      proto.String("proto3"),
			MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("RunRequest")}},
		}
		if comment != "" {
			fdp.SourceCodeInfo = &descriptorpb.SourceCodeInfo{Location: []*descriptorpb.SourceCodeInfo_Location{{
				Path:            []int32{4, 0},
				Span:            []int32{0, 0, 1},
				LeadingComments: proto.String(comment),
			}}}
		}
		// A private registry: these files would conflict with the linked schema
		// in GlobalFiles, which is the point.
		file, err := protodesc.NewFile(fdp, new(protoregistry.Files))
		if err != nil {
			t.Fatalf("building %s: %v", path, err)
		}
		return file.Messages().Get(0)
	}

	if got, ok := CommentOf(build("flowstate/v1/service.proto", " Its own words.\n")); !ok || got != "Its own words." {
		t.Errorf("CommentOf(descriptor with source info) = %q, %v; want its own comment", got, ok)
	}
	if got, ok := CommentOf(build("elsewhere/run.proto", "")); ok {
		t.Errorf("CommentOf(same name, other file) = %q, true; want no prose", got)
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

func TestAllowPredicateDocumentationNamesItsScopeAndFailClosedRule(t *testing.T) {
	for name, wants := range map[string][]string{
		"flowstate.v1.SignalPolicy.allow":  {"sender.identity", "run.identity", "Fail closed", "inputs"},
		"flowstate.v1.ManualTrigger.allow": {"sender.identity", "no run yet", "Fail closed", "inputs"},
	} {
		comment, ok := Comment(protoreflect.FullName(name))
		if !ok {
			t.Fatalf("%s has no descriptor documentation", name)
		}
		for _, want := range wants {
			if !strings.Contains(comment, want) {
				t.Errorf("%s descriptor documentation does not contain %q:\n%s", name, want, comment)
			}
		}
	}
}

// A leading comment belongs to the declaration immediately below it. Presence
// alone did not catch RunState's prose being copied above WorkloadIdentity,
// where generated API documentation attributed both descriptions to the
// identity message and left RunState unnamed.
func TestDocumentedTopLevelDeclarationsNameThemselves(t *testing.T) {
	files := protoregistry.GlobalFiles
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

// A linked descriptor that carries no SourceCodeInfo and that no generated file
// describes (google.protobuf's own, here) has no prose, and the package says so
// rather than borrowing some other declaration's.
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
	if strings.Contains(got, "[Run]") {
		t.Errorf("godoc link left untranslated in %q", got)
	}
	if !strings.Contains(got, "`Run`") {
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
