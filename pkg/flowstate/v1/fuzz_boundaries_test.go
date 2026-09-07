package flowstatev1_test

import (
	"encoding/json"
	"reflect"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// FuzzBindRunInputs fuzzes the door a submitter's inputs come through: bytes
// decoded as JSON into named values and bound against a workflow's declared
// shape, which is what `flow run` does with `--input` and `--input-file`
// before anything evaluates (#1721). When the bytes are not JSON they are
// bound as one string input, so the fuzzer still reaches the binder. Every
// input is an error or bound values, never both, and never a panic.
func FuzzBindRunInputs(f *testing.F) {
	f.Add([]byte(`{"tag": "v1", "retries": 3, "flags": ["a", "b"], "meta": {"k": "v"}}`))
	f.Add([]byte(`{"tag": 3}`))
	f.Add([]byte(`{"unknown": true}`))
	f.Add([]byte(`{"retries": 2.5}`))
	f.Add([]byte(`{"meta": {"deep": {"deeper": {"deepest": [1, [2, [3]]]}}}}`))
	f.Add([]byte("plain text, not JSON"))
	f.Add([]byte(""))

	spec := &v1.Workflow{
		Name: "fuzz-bind",
		DeclaredInputs: []*v1.InputDeclaration{
			{Name: "tag", Type: v1.InputDeclaration_TYPE_STRING},
			{Name: "retries", Type: v1.InputDeclaration_TYPE_INT, Default: v1.NewLiteral(int64(3))},
			{Name: "flags", Type: v1.InputDeclaration_TYPE_LIST},
			{Name: "meta", Type: v1.InputDeclaration_TYPE_STRUCT},
			{Name: "kind", Type: v1.InputDeclaration_TYPE_ENUM, Values: []string{"a", "b"}},
		},
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		var submitted map[string]any
		if err := json.Unmarshal(data, &submitted); err != nil {
			submitted = map[string]any{"tag": string(data)}
		}

		bound, err := v1.BindRunInputs(spec, v1.NewNamedValues(submitted))
		if err != nil && bound != nil {
			t.Fatalf("BindRunInputs returned both an error and bound inputs: %v", err)
		}
		if err == nil && bound == nil {
			t.Fatal("BindRunInputs returned neither bound inputs nor an error")
		}
	})
}

// FuzzRootParsers fuzzes the root package's remaining boundary parsers: the
// task-shape policy an operator writes, a plugin's descriptor prose, and the
// word parsers a Flowfile's fields go through (#1721). Every input is an
// error or a value, never both, and never a panic.
func FuzzRootParsers(f *testing.F) {
	f.Add([]byte("deny:\n  - 'task == \"http\"'\n"))
	f.Add([]byte("allow:\n  - 'identity.subject == \"ci\"'\ndeny:\n  - \"true\"\n"))
	f.Add([]byte("denny:\n  - \"true\"\n"))
	f.Add([]byte("# A task\n\nSays hello.\n"))
	f.Add([]byte("10m"))
	f.Add([]byte("string"))
	f.Add([]byte("reject"))
	f.Add([]byte(""))

	f.Fuzz(func(t *testing.T, data []byte) {
		if cfg, err := v1.ParseTaskPolicyConfig(data); err != nil && !reflect.DeepEqual(cfg, v1.TaskPolicyConfig{}) {
			t.Fatalf("ParseTaskPolicyConfig returned both an error and a config: %v", err)
		}
		if prose, err := v1.ParseDescriptorProse(data); err != nil && prose != nil {
			t.Fatalf("ParseDescriptorProse returned both an error and prose: %v", err)
		}

		word := string(data)
		if d, err := v1.ParseDuration(word); err != nil && d != 0 {
			t.Fatalf("ParseDuration returned both an error and a duration: %v", err)
		}
		_, _ = v1.ParseErrorKind(word)
		_, _ = v1.ParseDeclaredType(word)
		_, _ = v1.ParseOverlap(word)
		_, _ = v1.ParseConcurrencyOnConflict(word)
	})
}
