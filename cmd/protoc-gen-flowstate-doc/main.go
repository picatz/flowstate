// Command protoc-gen-flowstate-doc generates Go source that carries a schema's
// comments to run time.
//
// protoc-gen-go strips SourceCodeInfo from the descriptors it embeds in a
// .pb.go, so a running process has a schema's shape and none of its prose. This
// plugin writes each .proto file's leading comments as a generated Go file that
// registers them with
// [github.com/picatz/flowstate/pkg/flowstate/v1/protodoc/registry] at init. The
// comments then come from the same `buf generate` run as the types, are held
// by the same drift check, and show up as text in a diff.
//
// Run it next to protoc-gen-go:
//
//	plugins:
//	  - plugin: go
//	    out: gen
//	    opt: paths=source_relative
//	  - plugin: flowstate-doc
//	    path: [go, tool, protoc-gen-flowstate-doc]
//	    out: gen
//	    opt: paths=source_relative
//
// By default each file's comments go into the same Go package as its .pb.go, in
// a file named after it with a .doc.pb.go suffix. The package=IMPORT_PATH
// option writes every file's comments into that one package instead, with the
// file names flattened (flowstate/v1/run.proto becomes
// flowstate_v1_run.doc.pb.go) directly under the plugin's out directory. The
// engine uses it to keep its own schema's comments in protodoc, so only a
// binary that imports protodoc carries them.
//
// Comments are recorded raw, in declaration order, one generated line per
// comment line. Nothing is normalized here: presentation belongs to whoever
// reads the registry.
package main

import (
	"context"
	"flag"
	"fmt"
	"go/token"
	"path"
	"strconv"
	"strings"

	"github.com/bufbuild/protoplugin"
	gengo "google.golang.org/protobuf/cmd/protoc-gen-go/internal_gengo"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// version is reported by --version.
const version = "0.1.0"

// registryPackage is the import path generated code registers with.
const registryPackage protogen.GoImportPath = "github.com/picatz/flowstate/pkg/flowstate/v1/protodoc/registry"

func main() {
	protoplugin.Main(protoplugin.HandlerFunc(handle), protoplugin.WithVersion(version))
}

// handle is the plugin: protoplugin decodes and validates the request and the
// response, and protogen lays out the Go files.
func handle(_ context.Context, _ protoplugin.PluginEnv, w protoplugin.ResponseWriter, req protoplugin.Request) error {
	var (
		flags       flag.FlagSet
		packagePath = flags.String("package", "", "write every file's comments into this Go import path")
	)
	gen, err := protogen.Options{ParamFunc: flags.Set}.New(req.CodeGeneratorRequest())
	if err != nil {
		return err
	}
	into := protogen.GoImportPath(*packagePath)
	if into != "" && !token.IsIdentifier(path.Base(string(into))) {
		return fmt.Errorf("package=%s: %q is not a Go package name", into, path.Base(string(into)))
	}

	// The same language surface protoc-gen-go accepts, since this plugin runs
	// beside it over the same files and only reads their comments.
	w.SetFeatureProto3Optional()
	w.SetFeatureSupportsEditions(gengo.SupportedEditionsMinimum, gengo.SupportedEditionsMaximum)

	if err := generate(gen, into); err != nil {
		gen.Error(err)
	}
	resp := gen.Response()
	w.AddCodeGeneratorResponseFiles(resp.GetFile()...)
	w.AddError(resp.GetError())
	return nil
}

// generate writes one Go file per file to generate that declares at least one
// comment. With an empty into, each file's comments go into its own Go package.
func generate(gen *protogen.Plugin, into protogen.GoImportPath) error {
	for _, file := range gen.Files {
		if !file.Generate {
			continue
		}
		// protoc and buf always send source info for the files to generate. A
		// file without any is a request this plugin cannot honor, and writing
		// nothing for it would look exactly like a schema with no comments.
		if file.Desc.SourceLocations().Len() == 0 {
			return fmt.Errorf("%s: the request carries no source info, so its comments cannot be read", file.Desc.Path())
		}

		comments := collect(file.Desc)
		if len(comments) == 0 {
			continue
		}

		filename, importPath := file.GeneratedFilenamePrefix+".doc.pb.go", file.GoImportPath
		if into != "" {
			filename = strings.ReplaceAll(strings.TrimSuffix(file.Desc.Path(), ".proto"), "/", "_") + ".doc.pb.go"
			importPath = into
		}
		write(gen.NewGeneratedFile(filename, importPath), file, packageName(file, into), comments)
	}
	return nil
}

// comment is one declaration and its raw leading comment.
type comment struct {
	name    protoreflect.FullName
	leading string
}

// collect returns every leading comment the file declares, in declaration
// order, so the generated file reads in the same order as the .proto.
func collect(file protoreflect.FileDescriptor) []comment {
	locs := file.SourceLocations()
	var out []comment
	add := func(d protoreflect.Descriptor) {
		if leading := locs.ByDescriptor(d).LeadingComments; strings.TrimSpace(leading) != "" {
			out = append(out, comment{name: d.FullName(), leading: leading})
		}
	}

	var message func(m protoreflect.MessageDescriptor)
	enum := func(e protoreflect.EnumDescriptor) {
		add(e)
		for i := range e.Values().Len() {
			add(e.Values().Get(i))
		}
	}
	message = func(m protoreflect.MessageDescriptor) {
		add(m)
		for i := range m.Fields().Len() {
			add(m.Fields().Get(i))
		}
		for i := range m.Oneofs().Len() {
			if o := m.Oneofs().Get(i); !o.IsSynthetic() {
				add(o)
			}
		}
		for i := range m.Extensions().Len() {
			add(m.Extensions().Get(i))
		}
		for i := range m.Messages().Len() {
			if nested := m.Messages().Get(i); !nested.IsMapEntry() {
				message(nested)
			}
		}
		for i := range m.Enums().Len() {
			enum(m.Enums().Get(i))
		}
	}

	for i := range file.Messages().Len() {
		message(file.Messages().Get(i))
	}
	for i := range file.Enums().Len() {
		enum(file.Enums().Get(i))
	}
	for i := range file.Extensions().Len() {
		add(file.Extensions().Get(i))
	}
	for i := range file.Services().Len() {
		s := file.Services().Get(i)
		add(s)
		for j := range s.Methods().Len() {
			add(s.Methods().Get(j))
		}
	}
	return out
}

// packageName is the Go package clause for a generated file.
func packageName(file *protogen.File, into protogen.GoImportPath) protogen.GoPackageName {
	if into == "" {
		return file.GoPackageName
	}
	return protogen.GoPackageName(path.Base(string(into)))
}

// write renders one generated file.
func write(g *protogen.GeneratedFile, file *protogen.File, pkg protogen.GoPackageName, comments []comment) {
	registerFile := g.QualifiedGoIdent(registryPackage.Ident("RegisterFile"))
	commentType := g.QualifiedGoIdent(registryPackage.Ident("Comment"))

	g.P("// Code generated by protoc-gen-flowstate-doc. DO NOT EDIT.")
	g.P("// source: ", file.Desc.Path())
	g.P()
	g.P("package ", pkg)
	g.P()
	g.P("func init() {")
	g.P(registerFile, "(", strconv.Quote(file.Desc.Path()), ", []", commentType, "{")
	for _, c := range comments {
		g.P("{")
		g.P("Name: ", strconv.Quote(string(c.name)), ",")
		lines := strings.SplitAfter(c.leading, "\n")
		if lines[len(lines)-1] == "" {
			lines = lines[:len(lines)-1]
		}
		for i, line := range lines {
			prefix, suffix := "", " +"
			if i == 0 {
				prefix = "Leading: "
			}
			if i == len(lines)-1 {
				suffix = ","
			}
			g.P(prefix, strconv.Quote(line), suffix)
		}
		g.P("},")
	}
	g.P("})")
	g.P("}")
}
