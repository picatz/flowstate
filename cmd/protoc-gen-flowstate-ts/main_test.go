package main

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/pluginpb"
)

type (
	fdp   = descriptorpb.FieldDescriptorProto
	ftype = descriptorpb.FieldDescriptorProto_Type
)

func field(name, json string, typ ftype, typeName string, mods ...func(*fdp)) *fdp {
	f := &fdp{
		Name: proto.String(name), JsonName: proto.String(json), Number: proto.Int32(1),
		Type: typ.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
	}
	if typeName != "" {
		f.TypeName = proto.String(typeName)
	}
	for _, m := range mods {
		m(f)
	}
	return f
}

func repeated(f *fdp) { f.Label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum() }

func number(n int32) func(*fdp) { return func(f *fdp) { f.Number = proto.Int32(n) } }

// reportProto is a schema with each field shape the plugin maps, and comments
// on the declarations the generated TSDoc is checked against.
func reportProto() *descriptorpb.FileDescriptorProto {
	loc := func(comment string, path ...int32) *descriptorpb.SourceCodeInfo_Location {
		return &descriptorpb.SourceCodeInfo_Location{Path: path, Span: []int32{0, 0, 1}, LeadingComments: proto.String(comment)}
	}
	const (
		str  = descriptorpb.FieldDescriptorProto_TYPE_STRING
		i32  = descriptorpb.FieldDescriptorProto_TYPE_INT32
		i64  = descriptorpb.FieldDescriptorProto_TYPE_INT64
		bl   = descriptorpb.FieldDescriptorProto_TYPE_BOOL
		msg  = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
		enum = descriptorpb.FieldDescriptorProto_TYPE_ENUM
	)
	return &descriptorpb.FileDescriptorProto{
		Name:       proto.String("rep/v1/report.proto"),
		Package:    proto.String("rep.v1"),
		Syntax:     proto.String("proto3"),
		Dependency: []string{"google/protobuf/timestamp.proto"},
		Options:    &descriptorpb.FileOptions{GoPackage: proto.String("example.com/rep/v1;repv1")},
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("Report"),
				Field: []*fdp{
					field("file_name", "fileName", str, "", number(1)),
					field("count", "count", i32, "", number(2)),
					field("size", "size", i64, "", number(3)),
					field("problems", "problems", msg, ".rep.v1.Report.Problem", repeated, number(4)),
					field("labels", "labels", msg, ".rep.v1.Report.LabelsEntry", repeated, number(5)),
					field("severity", "severity", enum, ".rep.v1.Severity", number(6)),
					field("at", "at", msg, ".google.protobuf.Timestamp", number(7)),
					field("owner", "owner", msg, ".rep.v1.Owner", number(8)),
					field("note", "note", str, "", number(9), func(f *fdp) { f.Proto3Optional = proto.Bool(true); f.OneofIndex = proto.Int32(1) }),
					field("tags", "tags", str, "", repeated, number(10)),
					field("by_id", "byId", str, "", number(11), func(f *fdp) { f.OneofIndex = proto.Int32(0) }),
				},
				OneofDecl: []*descriptorpb.OneofDescriptorProto{{Name: proto.String("who")}, {Name: proto.String("_note")}},
				NestedType: []*descriptorpb.DescriptorProto{
					{Name: proto.String("Problem"), Field: []*fdp{field("ok", "ok", bl, "")}},
					{
						Name: proto.String("LabelsEntry"),
						Field: []*fdp{
							field("key", "key", str, "", number(1)),
							field("value", "value", msg, ".rep.v1.Owner", number(2)),
						},
						Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
					},
				},
			},
			{Name: proto.String("Owner"), Field: []*fdp{field("name", "name", str, "")}},
			{Name: proto.String("Unrelated"), Field: []*fdp{field("x", "x", str, "")}},
		},
		EnumType: []*descriptorpb.EnumDescriptorProto{{
			Name: proto.String("Severity"),
			Value: []*descriptorpb.EnumValueDescriptorProto{
				{Name: proto.String("SEVERITY_UNSPECIFIED"), Number: proto.Int32(0)},
				{Name: proto.String("SEVERITY_ERROR"), Number: proto.Int32(1)},
			},
		}},
		SourceCodeInfo: &descriptorpb.SourceCodeInfo{Location: []*descriptorpb.SourceCodeInfo_Location{
			loc(" Report is what a check found.\n\n It ends a comment */ early if unescaped.\n", 4, 0),
			loc(" FileName is the path as given.\n", 4, 0, 2, 0),
			loc(" Severity says how bad.\n", 5, 0),
		}},
	}
}

func timestampProto() *descriptorpb.FileDescriptorProto {
	return protodesc.ToFileDescriptorProto(timestamppb.File_google_protobuf_timestamp_proto)
}

func run(t *testing.T, parameter string, files ...*descriptorpb.FileDescriptorProto) *pluginpb.CodeGeneratorResponse {
	t.Helper()
	req := &pluginpb.CodeGeneratorRequest{Parameter: proto.String(parameter), ProtoFile: files}
	req.FileToGenerate = append(req.FileToGenerate, files[len(files)-1].GetName())
	opts, generate := plugin()
	gen, err := opts.New(req)
	if err != nil {
		t.Fatalf("protogen refused the request: %v", err)
	}
	if err := generate(gen); err != nil {
		gen.Error(err)
	}
	return gen.Response()
}

func TestGeneratesTheJSONShape(t *testing.T) {
	resp := run(t, "message=rep.v1.Report", timestampProto(), reportProto())
	if resp.Error != nil {
		t.Fatalf("plugin error: %s", resp.GetError())
	}
	if len(resp.File) != 1 || resp.File[0].GetName() != "flowstate.d.ts" {
		t.Fatalf("files = %v, want exactly flowstate.d.ts", resp.File)
	}
	got := resp.File[0].GetContent()

	for _, want := range []string{
		"export interface Report {",
		"  fileName: string\n",                 // lowerCamelCase JSON name
		"  count: number\n",                    // int32 is a number
		"  size: string\n",                     // int64 is a string in proto3 JSON
		"  problems: Report_Problem[]\n",       // nested message, repeated
		"  labels: { [key: string]: Owner }\n", // map
		"  severity: Severity\n",               // enum by name
		"  at?: string | null\n",               // Timestamp is a string; a message field may be null or absent
		"  owner?: Owner | null\n",             // message field
		"  note?: string\n",                    // proto3 optional may be absent
		"  tags: string[]\n",                   // repeated scalar
		"  byId?: string\n",                    // oneof member may be absent
		`export type Severity = "SEVERITY_UNSPECIFIED" | "SEVERITY_ERROR"`,
		"export interface Report_Problem {",
		"export interface Owner {",
		" * Report is what a check found.",
		"   * FileName is the path as given.",
		" * It ends a comment *\\/ early if unescaped.",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output lacks %q\n%s", want, got)
		}
	}
	// Only what the entry point reaches is declared.
	if strings.Contains(got, "Unrelated") {
		t.Errorf("declared a type no entry point reaches:\n%s", got)
	}
	// Map entries are an encoding detail, not a type.
	if strings.Contains(got, "LabelsEntry") {
		t.Errorf("declared a map entry:\n%s", got)
	}
}

func TestOutputIsDeterministic(t *testing.T) {
	a := run(t, "message=rep.v1.Owner,message=rep.v1.Report", timestampProto(), reportProto())
	b := run(t, "message=rep.v1.Report,message=rep.v1.Owner", timestampProto(), reportProto())
	if a.File[0].GetContent() != b.File[0].GetContent() {
		t.Error("entry-point order changed the output")
	}
}

func TestFileOptionNamesTheOutput(t *testing.T) {
	resp := run(t, "file=shapes.d.ts,message=rep.v1.Owner", timestampProto(), reportProto())
	if resp.File[0].GetName() != "shapes.d.ts" {
		t.Errorf("name = %q", resp.File[0].GetName())
	}
}

func TestRefusals(t *testing.T) {
	anyField := reportProto()
	anyField.Dependency = append(anyField.Dependency, "google/protobuf/any.proto")
	anyField.MessageType[1].Field = append(anyField.MessageType[1].Field,
		field("payload", "payload", descriptorpb.FieldDescriptorProto_TYPE_MESSAGE, ".google.protobuf.Any", number(2)))

	for name, tc := range map[string]struct {
		parameter string
		files     []*descriptorpb.FileDescriptorProto
		want      string
	}{
		"no entry point":       {"file=a.d.ts", []*descriptorpb.FileDescriptorProto{timestampProto(), reportProto()}, "no message= option"},
		"unknown message":      {"message=rep.v1.Nope", []*descriptorpb.FileDescriptorProto{timestampProto(), reportProto()}, "no such message"},
		"not a declaration":    {"file=a.ts,message=rep.v1.Owner", []*descriptorpb.FileDescriptorProto{timestampProto(), reportProto()}, "ends in .d.ts"},
		"undeclarable message": {"message=rep.v1.Owner", []*descriptorpb.FileDescriptorProto{timestampProto(), protodesc.ToFileDescriptorProto(anypb.File_google_protobuf_any_proto), anyField}, "no declared JSON shape"},
	} {
		t.Run(name, func(t *testing.T) {
			resp := run(t, tc.parameter, tc.files...)
			if !strings.Contains(resp.GetError(), tc.want) {
				t.Errorf("error = %q, want it to contain %q", resp.GetError(), tc.want)
			}
		})
	}
}
