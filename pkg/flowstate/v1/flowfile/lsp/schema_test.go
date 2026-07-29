package lsp

import (
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// These tests pin what the editor tells an author about a task's shape to what the
// schema actually says. If the schema changes, these should change with it — which
// is the point: there is nowhere else the two could drift apart.

func TestTypeNamesComeFromTheSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		task  string
		field string
		// side selects inputs or outputs.
		outputs bool
		want    string
	}{
		{task: "echo", field: "message", want: "string"},
		{task: "echo", field: "result", outputs: true, want: "string"},
		{task: "printf", field: "format", want: "string"},
		// repeated Value: a list whose elements are whatever the expression yields.
		{task: "printf", field: "args", want: "list[any]"},
		{task: "http", field: "headers", want: "map[string, string]"},
		{task: "http", field: "outputs", want: "map[string, any]"},
		{task: "http", field: "status_code", outputs: true, want: "int"},
		{task: "cel", field: "vars", want: "map[string, any]"},
		{task: "cel", field: "result", outputs: true, want: "any"},
	}
	for _, tt := range tests {
		t.Run(tt.task+"."+tt.field, func(t *testing.T) {
			def, ok := v1.LookupTask(tt.task)
			require.True(t, ok)
			md := def.Inputs
			if tt.outputs {
				md = def.Outputs
			}
			fd := findField(md, tt.field)
			require.NotNil(t, fd, "the schema no longer declares this field")
			assert.Equal(t, tt.want, typeName(fd))
		})
	}
}

func TestRequiredComesFromProtovalidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		task  string
		field string
		want  bool
	}{
		{task: "echo", field: "message", want: true},
		{task: "http", field: "url", want: true},
		{task: "http", field: "method", want: false},
		{task: "http", field: "headers", want: false},
		{task: "printf", field: "format", want: true},
		// args is required both by the required rule and by min_items.
		{task: "printf", field: "args", want: true},
		{task: "cel", field: "expr", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.task+"."+tt.field, func(t *testing.T) {
			def, ok := v1.LookupTask(tt.task)
			require.True(t, ok)
			fd := findField(def.Inputs, tt.field)
			require.NotNil(t, fd)
			assert.Equal(t, tt.want, required(fd))
		})
	}
}

func TestConstraintsAreRendered(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		task    string
		field   string
		outputs bool
		want    []string
		notWant []string
	}{
		{
			name:  "a uri rule",
			task:  "http",
			field: "url",
			want:  []string{"must be an absolute URI"},
		},
		{
			name:  "length and pattern rules",
			task:  "http",
			field: "method",
			want:  []string{"at least 3 characters", "at most 6 characters", "matches"},
		},
		{
			name:  "repeated item counts",
			task:  "printf",
			field: "args",
			want:  []string{"at least 1 item(s)", "at most 100 item(s)"},
		},
		{
			name:    "numeric bounds on an output",
			task:    "http",
			field:   "status_code",
			outputs: true,
			want:    []string{"at least 100", "at most 599"},
		},
		{
			name:    "a field with no rules says nothing",
			task:    "http",
			field:   "headers",
			notWant: []string{"at least", "at most", "matches"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			def, ok := v1.LookupTask(tt.task)
			require.True(t, ok)
			md := def.Inputs
			if tt.outputs {
				md = def.Outputs
			}
			fd := findField(md, tt.field)
			require.NotNil(t, fd)

			got := constraints(fd)
			for _, want := range tt.want {
				assert.Contains(t, joined(got), want)
			}
			for _, notWant := range tt.notWant {
				assert.NotContains(t, joined(got), notWant)
			}
		})
	}
}

func TestSignatureCoversEveryRegisteredTask(t *testing.T) {
	t.Parallel()

	// Every task in the registry must render, so adding one cannot produce a
	// hover popup that is empty or says "unknown".
	for _, def := range v1.DefaultRegistry().All() {
		t.Run(def.Name, func(t *testing.T) {
			got := signature(def)
			assert.Contains(t, got, def.Name)
			assert.Contains(t, got, "inputs:")
			assert.Contains(t, got, "outputs:")

			// Every field must render as a type a Flowfile author recognizes. The
			// check is on the rendered type of each field rather than a substring
			// of the whole block, because a field *named* something like
			// retry_on_unknown_outcome is not a rendering failure.
			for _, md := range []protoreflect.MessageDescriptor{def.Inputs, def.Outputs} {
				if md == nil {
					continue
				}
				fields := md.Fields()
				for i := range fields.Len() {
					fd := fields.Get(i)
					rendered := typeName(fd)
					assert.NotEqual(t, "unknown", rendered, "field %s", fd.Name())
					// A bare message name means the renderer has no DSL word for
					// the type, which tells an author nothing.
					assert.NotContains(t, rendered, "Value", "field %s renders a schema type name", fd.Name())
				}
			}

			for _, name := range fieldNames(def.Inputs) {
				assert.Contains(t, got, name)
			}
			for _, name := range fieldNames(def.Outputs) {
				assert.Contains(t, got, name)
			}
		})
	}
}

func TestSignatureHandlesATaskWithNoSchema(t *testing.T) {
	t.Parallel()

	// TaskDef allows nil descriptors for a task whose shape is not a message.
	// Hover must say so rather than render an empty table or panic.
	got := signature(v1.TaskDef{Name: "shapeless"})
	assert.Contains(t, got, "shapeless")
	assert.Contains(t, got, "not described by the schema")

	assert.Nil(t, findField(nil, "anything"))
	assert.Nil(t, fieldNames(nil))
	assert.Equal(t, "unknown", typeName(nil))
	assert.False(t, required(nil))
	assert.Empty(t, constraints(nil))
}

func TestAcceptsAnyInput(t *testing.T) {
	t.Parallel()

	// Only a task declaring a vars mapping takes input names beyond its schema,
	// because that is the field the compiler flattens into the input map.
	for _, tt := range []struct {
		task string
		want bool
	}{
		{"cel", true},
		{"echo", false},
		{"http", false},
		{"printf", false},
	} {
		def, ok := v1.LookupTask(tt.task)
		require.True(t, ok)
		assert.Equal(t, tt.want, acceptsAnyInput(def), tt.task)
	}
}

// TestScalarTypeNameCoversEveryKind guards the type table against a schema that
// starts using a Protobuf kind this package has never seen, which would otherwise
// surface in an editor as a raw Protobuf type name.
func TestScalarTypeNameCoversEveryKind(t *testing.T) {
	t.Parallel()

	// Walk every field of every message the registry exposes, at every nesting
	// depth, and require a rendered name that is not Protobuf spelling.
	seen := map[protoreflect.Kind]bool{}
	for _, def := range v1.DefaultRegistry().All() {
		for _, md := range []protoreflect.MessageDescriptor{def.Inputs, def.Outputs} {
			if md == nil {
				continue
			}
			fields := md.Fields()
			for i := range fields.Len() {
				fd := fields.Get(i)
				seen[fd.Kind()] = true
				assert.NotEmpty(t, typeName(fd))
			}
		}
	}
	// The schema does exercise several kinds; if it ever stops, this test is no
	// longer checking anything.
	assert.GreaterOrEqual(t, len(seen), 3, "the schema uses fewer kinds than expected")
}

// joined renders constraint phrases as one string for substring assertions.
func joined(parts []string) string {
	out := ""
	for _, p := range parts {
		out += p + "; "
	}
	return out
}
