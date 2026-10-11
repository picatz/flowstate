package termsafe

import (
	"encoding/json"
	"testing"
)

func TestEscapeJSON(t *testing.T) {
	t.Parallel()

	cases := map[string]struct{ in, want string }{
		"c1":           {"\"a\u009bb\"", `"a\u009bb"`},
		"nel":          {"\"\u0085\"", `"\u0085"`},
		"bidi":         {"\"\u202e\u2066\u061c\u200f\"", `"\u202e\u2066\u061c\u200f"`},
		"separators":   {"\"\u2028\u2029\"", `"\u2028\u2029"`},
		"del":          {"\"\u007f\"", `"\u007f"`},
		"ordinary":     {"\"日本語 \U0001F680 café\"", "\"日本語 \U0001F680 café\""},
		"zwj kept":     {"\"\u200d\u200b\"", "\"\u200d\u200b\""},
		"already done": {`"\u202e \\u009b"`, `"\u202e \\u009b"`},
		"structure":    {"{\"a\":[1,true,null]}\n", "{\"a\":[1,true,null]}\n"},
		"invalid utf8": {"\"\xff\u009b\"", "\"\xff\\u009b\""},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if got := string(EscapeJSON([]byte(tc.in))); got != tc.want {
				t.Fatalf("EscapeJSON(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestEscapeJSONDecodesToOriginal(t *testing.T) {
	t.Parallel()

	original := "x\u009b\u202e\u2028\u2029\u007fy日本"
	encoded, err := json.Marshal(map[string]string{original: original})
	if err != nil {
		t.Fatal(err)
	}

	var decoded map[string]string
	if err := json.Unmarshal(EscapeJSON(encoded), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded[original] != original {
		t.Fatalf("round trip changed the string: %q", decoded[original])
	}
}
