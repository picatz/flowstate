package flowstatev1_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// literalProbe describes, as a plugin's schema would arrive on the host:
//
//	message Item  { string name = 1 [literal]; string note = 2; }
//	message Inputs {
//	  Item item = 1; repeated Item items = 2; map<string, Item> by_name = 3;
//	  map<string, string> tags = 4 [literal]; repeated string labels = 5 [literal];
//	  string free = 6;
//	}
func literalProbe(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()

	const (
		str = descriptorpb.FieldDescriptorProto_TYPE_STRING
		msg = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
	)
	opt := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	rep := descriptorpb.FieldDescriptorProto_LABEL_REPEATED

	literal := func() *descriptorpb.FieldOptions {
		o := &descriptorpb.FieldOptions{}
		proto.SetExtension(o, v1.E_Input, &v1.InputOptions{Literal: true})

		return o
	}
	field := func(name string, n int32, label descriptorpb.FieldDescriptorProto_Label, typ descriptorpb.FieldDescriptorProto_Type, typeName string, o *descriptorpb.FieldOptions) *descriptorpb.FieldDescriptorProto {
		f := &descriptorpb.FieldDescriptorProto{Name: proto.String(name), Number: proto.Int32(n), Label: label.Enum(), Type: typ.Enum(), Options: o}
		if typeName != "" {
			f.TypeName = proto.String(typeName)
		}

		return f
	}
	entry := func(name string, value *descriptorpb.FieldDescriptorProto) *descriptorpb.DescriptorProto {
		return &descriptorpb.DescriptorProto{
			Name:    proto.String(name),
			Field:   []*descriptorpb.FieldDescriptorProto{field("key", 1, opt, str, "", nil), value},
			Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
		}
	}

	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:       proto.String("literalprobe/v1/probe.proto"),
		Package:    proto.String("literalprobe.v1"),
		Syntax:     proto.String("proto3"),
		Dependency: []string{"flowstate/v1/schema.proto"},
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: proto.String("Item"), Field: []*descriptorpb.FieldDescriptorProto{
				field("name", 1, opt, str, "", literal()),
				field("note", 2, opt, str, "", nil),
			}},
			{
				Name: proto.String("Inputs"),
				Field: []*descriptorpb.FieldDescriptorProto{
					field("item", 1, opt, msg, ".literalprobe.v1.Item", nil),
					field("items", 2, rep, msg, ".literalprobe.v1.Item", nil),
					field("by_name", 3, rep, msg, ".literalprobe.v1.Inputs.ByNameEntry", nil),
					field("tags", 4, rep, msg, ".literalprobe.v1.Inputs.TagsEntry", literal()),
					field("labels", 5, rep, str, "", literal()),
					field("free", 6, opt, str, "", nil),
				},
				NestedType: []*descriptorpb.DescriptorProto{
					entry("ByNameEntry", field("value", 2, opt, msg, ".literalprobe.v1.Item", nil)),
					entry("TagsEntry", field("value", 2, opt, str, "", nil)),
				},
			},
		},
	}, protoregistry.GlobalFiles)
	require.NoError(t, err)

	return file.Messages().ByName("Inputs")
}

func constExpr(s string) *expr.Expr {
	return &expr.Expr{ExprKind: &expr.Expr_ConstExpr{ConstExpr: &expr.Constant{ConstantKind: &expr.Constant_StringValue{StringValue: s}}}}
}

func identExpr(name string) *expr.Expr {
	return &expr.Expr{ExprKind: &expr.Expr_IdentExpr{IdentExpr: &expr.Expr_Ident{Name: name}}}
}

// asMap is what the compiler writes for a mapping holding an expression: a CEL
// map literal with constant keys.
func asMap(entries map[string]*expr.Expr) *expr.Expr {
	out := &expr.Expr_CreateStruct{}
	for k, v := range entries {
		out.Entries = append(out.Entries, &expr.Expr_CreateStruct_Entry{
			KeyKind: &expr.Expr_CreateStruct_Entry_MapKey{MapKey: constExpr(k)},
			Value:   v,
		})
	}

	return &expr.Expr{ExprKind: &expr.Expr_StructExpr{StructExpr: out}}
}

func listExpr(elements ...*expr.Expr) *expr.Expr {
	return &expr.Expr{ExprKind: &expr.Expr_ListExpr{ListExpr: &expr.Expr_CreateList{Elements: elements}}}
}

func exprValue(e *expr.Expr) *v1.Value {
	return &v1.Value{Kind: &v1.Value_Expr{Expr: &expr.ParsedExpr{Expr: e}}}
}

func mapExpr(entries map[string]*expr.Expr) *v1.Value { return exprValue(asMap(entries)) }

func secretValue() *v1.Value {
	return &v1.Value{Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "env", Name: "TOKEN"}}}
}

func TestInputsDescribeLiteralClaims(t *testing.T) {
	t.Parallel()

	notes := map[string][]string{}
	for _, f := range v1.Inputs(v1.TaskDef{Name: "probe", Inputs: literalProbe(t)}) {
		notes[f.Name] = f.Constraints
	}
	require.Contains(t, notes["item"], "must be literal text, never an expression, at: item.name")
	require.Contains(t, notes["labels"], "must be literal text, never an expression, at: labels")
	require.Empty(t, notes["free"])
}

func TestLiteralInputClaimsReadsEveryDepth(t *testing.T) {
	t.Parallel()

	paths, err := v1.LiteralInputClaims(literalProbe(t))
	require.NoError(t, err)
	require.Equal(t, []string{"item.name", "items.name", "by_name.name", "tags", "labels"}, paths)
}

// oneField links a message holding a single field with the given claims.
func oneField(t *testing.T, typ descriptorpb.FieldDescriptorProto_Type, opts *v1.InputOptions) (protoreflect.MessageDescriptor, error) {
	t.Helper()

	o := &descriptorpb.FieldOptions{}
	proto.SetExtension(o, v1.E_Input, opts)
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:       proto.String("literalbad/v1/bad.proto"),
		Package:    proto.String("literalbad.v1"),
		Syntax:     proto.String("proto3"),
		Dependency: []string{"flowstate/v1/schema.proto"},
		MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("Inputs"), Field: []*descriptorpb.FieldDescriptorProto{{
			Name: proto.String("count"), Number: proto.Int32(1), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			Type: typ.Enum(), Options: o,
		}}}},
	}, protoregistry.GlobalFiles)
	require.NoError(t, err)

	_, err = v1.InputClaims(file.Messages().ByName("Inputs"))

	return file.Messages().ByName("Inputs"), err
}

// TestInputClaimsRefusesADescriptorThatFansOut proves the claim search is
// bounded by work and not only by depth: message N has two fields of message N+1,
// which no stack guard sees as a cycle, and 31 levels of that name 2^31 paths.
func TestInputClaimsRefusesADescriptorThatFansOut(t *testing.T) {
	t.Parallel()

	const levels = 31
	opt := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	claimed := &descriptorpb.FieldOptions{}
	proto.SetExtension(claimed, v1.E_Input, &v1.InputOptions{Literal: true})

	var messages []*descriptorpb.DescriptorProto
	for i := range levels {
		next := fmt.Sprintf(".fanout.v1.M%d", i+1)
		messages = append(messages, &descriptorpb.DescriptorProto{
			Name: proto.String(fmt.Sprintf("M%d", i)),
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("a"), Number: proto.Int32(1), Label: opt, Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String(next)},
				{Name: proto.String("b"), Number: proto.Int32(2), Label: opt, Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String(next)},
			},
		})
	}
	messages = append(messages, &descriptorpb.DescriptorProto{
		Name: proto.String(fmt.Sprintf("M%d", levels)),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("leaf"), Number: proto.Int32(1), Label: opt, Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Options: claimed},
		},
	})
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:        proto.String("fanout/v1/fanout.proto"),
		Package:     proto.String("fanout.v1"),
		Syntax:      proto.String("proto3"),
		Dependency:  []string{"flowstate/v1/schema.proto"},
		MessageType: messages,
	}, protoregistry.GlobalFiles)
	require.NoError(t, err)

	start := time.Now()
	_, err = v1.InputClaims(file.Messages().ByName("M0"))
	require.ErrorContains(t, err, "while reading literal claims")
	require.Less(t, time.Since(start), 5*time.Second)
}

func TestInputClaimsRefusesAClaimOnAShapeItCannotConstrain(t *testing.T) {
	t.Parallel()

	_, err := oneField(t, descriptorpb.FieldDescriptorProto_TYPE_INT64, &v1.InputOptions{Literal: true})
	require.ErrorContains(t, err, `field "count" declares the literal claim`)
}

func TestInputClaimsKeepsLiteralApartFromSecret(t *testing.T) {
	t.Parallel()

	_, err := oneField(t, descriptorpb.FieldDescriptorProto_TYPE_STRING,
		&v1.InputOptions{Literal: true, Secret: v1.Secret_SECRET_WHOLE_VALUE})
	require.ErrorContains(t, err, "literal claim, which cannot both hold")

	md, err := oneField(t, descriptorpb.FieldDescriptorProto_TYPE_STRING, &v1.InputOptions{Literal: true})
	require.NoError(t, err)
	claims, err := v1.InputClaims(md)
	require.NoError(t, err)
	require.Equal(t, []v1.InputClaim{{Name: "count", Literal: []string{"count"}}}, claims)

	whole, required, nested, err := v1.SecretInputClaims(md)
	require.NoError(t, err)
	require.Empty(t, whole)
	require.Empty(t, required)
	require.Empty(t, nested)
}

func TestLiteralFieldViolation(t *testing.T) {
	t.Parallel()

	md := literalProbe(t)

	for _, tc := range []struct {
		name  string
		input string
		value *v1.Value
		// field and found are what the refusal names; empty field means accepted.
		field string
		found string
	}{
		{name: "an input with no claim takes an expression", input: "free", value: exprValue(identExpr("event"))},
		{name: "a literal item is accepted", input: "item", value: v1.NewValue(map[string]any{"name": "x", "note": "y"})},
		{
			name: "an expression for the claimed field is refused", input: "item",
			value: mapExpr(map[string]*expr.Expr{"name": identExpr("event")}),
			field: "item.name", found: "an expression",
		},
		{
			name: "a constant at the claimed field with an expression beside it is accepted", input: "item",
			value: mapExpr(map[string]*expr.Expr{"name": constExpr("x"), "note": identExpr("event")}),
		},
		{
			name: "an expression for the whole message hides the claim", input: "item",
			value: exprValue(identExpr("steps")),
			field: "item.name", found: "an expression",
		},
		{
			name: "a secret reference for the whole message is refused", input: "item",
			value: secretValue(),
			field: "item.name", found: "a secret reference",
		},
		{
			name: "a structure holding a secret reference at the claimed field is refused", input: "item",
			value: v1.NewStructureMap(map[string]*v1.Value{"name": secretValue()}),
			field: "item.name", found: "a secret reference",
		},
		{
			name: "a structure of literals is accepted", input: "item",
			value: v1.NewStructureMap(map[string]*v1.Value{"name": v1.NewLiteral("x")}),
		},
		{
			name: "a key that is not text hides which field the value lands in", input: "item",
			value: exprValue(&expr.Expr{ExprKind: &expr.Expr_StructExpr{StructExpr: &expr.Expr_CreateStruct{Entries: []*expr.Expr_CreateStruct_Entry{{
				KeyKind: &expr.Expr_CreateStruct_Entry_MapKey{MapKey: &expr.Expr{ExprKind: &expr.Expr_ConstExpr{ConstExpr: &expr.Constant{ConstantKind: &expr.Constant_Int64Value{Int64Value: 1}}}}},
				Value:   identExpr("event"),
			}}}}}),
			field: "item.name", found: "an expression",
		},
		{
			name: "a list element is held to the claim", input: "items",
			value: exprValue(listExpr(asMap(map[string]*expr.Expr{"name": constExpr("a")}), asMap(map[string]*expr.Expr{"name": identExpr("event")}))),
			field: "items[1].name", found: "an expression",
		},
		{
			name: "a list of constants is accepted", input: "items",
			value: exprValue(listExpr(asMap(map[string]*expr.Expr{"name": constExpr("a"), "note": identExpr("event")}))),
		},
		{
			name: "a map entry is held to the claim", input: "by_name",
			value: mapExpr(map[string]*expr.Expr{"k": asMap(map[string]*expr.Expr{"name": identExpr("event")})}),
			field: "by_name.k.name", found: "an expression",
		},
		{
			name: "a map entry of constants is accepted", input: "by_name",
			value: mapExpr(map[string]*expr.Expr{"k": asMap(map[string]*expr.Expr{"name": constExpr("a")})}),
		},
		{
			name: "a map of strings claimed as a whole refuses one expression entry", input: "tags",
			value: mapExpr(map[string]*expr.Expr{"a": constExpr("1"), "b": identExpr("event")}),
			field: "tags.b", found: "an expression",
		},
		{name: "a literal map of strings is accepted", input: "tags", value: v1.NewValue(map[string]any{"a": "1"})},
		{
			name: "a list of strings claimed as a whole refuses one expression element", input: "labels",
			value: exprValue(listExpr(constExpr("a"), identExpr("event"))),
			field: "labels[1]", found: "an expression",
		},
		{name: "a literal list of strings is accepted", input: "labels", value: v1.NewValue([]any{"a", "b"})},
		{
			name: "a structure list holding a secret reference is refused", input: "labels",
			value: &v1.Value{Kind: &v1.Value_Structure_{Structure: &v1.Value_Structure{Kind: &v1.Value_Structure_List_{
				List: &v1.Value_Structure_List{Values: []*v1.Value{v1.NewLiteral("a"), secretValue()}},
			}}}},
			field: "labels[1]", found: "a secret reference",
		},
		{
			name: "a computed key hides which field is written", input: "item",
			value: exprValue(&expr.Expr{ExprKind: &expr.Expr_StructExpr{StructExpr: &expr.Expr_CreateStruct{Entries: []*expr.Expr_CreateStruct_Entry{{
				KeyKind: &expr.Expr_CreateStruct_Entry_MapKey{MapKey: identExpr("k")}, Value: constExpr("x"),
			}}}}}),
			field: "item.name", found: "an expression",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := v1.LiteralFieldViolation(md, tc.input, tc.value)
			if tc.field == "" {
				require.Nil(t, got)

				return
			}
			require.NotNil(t, got)
			require.Equal(t, tc.input, got.Input)
			require.Equal(t, tc.field, got.Field)
			require.Equal(t, tc.found, got.Found)
			require.Contains(t, got.Error(), tc.field)
		})
	}
}

func TestLiteralFieldViolationIsBoundedByDepth(t *testing.T) {
	t.Parallel()

	deep := listExpr(constExpr("x"))
	for range v1.MaxStructureDepth + 2 {
		deep = listExpr(deep)
	}
	require.NotNil(t, v1.LiteralFieldViolation(literalProbe(t), "labels", exprValue(deep)))
}

func TestCheckInputClaimsAlsoHoldsLiteralClaims(t *testing.T) {
	const name = "test-literal-fields.probe"
	require.NoError(t, v1.DefaultRegistry().Register(v1.TaskDef{
		Name:   name,
		Inputs: literalProbe(t),
		Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
			return nil, nil
		},
	}))
	t.Cleanup(func() { v1.DefaultRegistry().Unregister(name) })

	bad := map[string]*v1.Value{"item": mapExpr(map[string]*expr.Expr{"name": identExpr("event")})}
	good := map[string]*v1.Value{"item": mapExpr(map[string]*expr.Expr{"name": constExpr("x"), "note": identExpr("event")})}
	step := func(id string, task *v1.Task, undo *v1.Task) *v1.Node {
		n := &v1.Node{Id: id, Kind: &v1.Node_Task{Task: task}}
		if undo != nil {
			n.Undo = &v1.Compensation{Task: undo}
		}

		return n
	}

	t.Run("accepted", func(t *testing.T) {
		wf := &v1.Workflow{Name: "w", Steps: []*v1.Node{step("a", &v1.Task{Name: name, Inputs: good}, nil)}}
		require.NoError(t, v1.CheckInputClaims(wf, v1.DefaultRegistry()))
	})
	t.Run("step", func(t *testing.T) {
		wf := &v1.Workflow{Name: "w", Steps: []*v1.Node{step("a", &v1.Task{Name: name, Inputs: bad}, nil)}}
		err := v1.CheckInputClaims(wf, v1.DefaultRegistry())
		require.ErrorContains(t, err, `step "a"`)
		require.ErrorContains(t, err, "item.name")
	})
	t.Run("undo", func(t *testing.T) {
		wf := &v1.Workflow{Name: "w", Steps: []*v1.Node{step("a", &v1.Task{Name: name, Inputs: good}, &v1.Task{Name: name, Inputs: bad})}}
		require.ErrorContains(t, v1.CheckInputClaims(wf, v1.DefaultRegistry()), `step "a" undo`)
	})
	t.Run("callee", func(t *testing.T) {
		callee := &v1.Workflow{Name: "c", Steps: []*v1.Node{step("inner", &v1.Task{Name: name, Inputs: bad}, nil)}}
		wf := &v1.Workflow{Name: "w", Steps: []*v1.Node{{Id: "outer", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee}}}}}
		require.ErrorContains(t, v1.CheckInputClaims(wf, v1.DefaultRegistry()), `step "inner"`)
	})
}
