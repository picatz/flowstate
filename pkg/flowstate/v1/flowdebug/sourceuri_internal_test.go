package flowdebug

import "testing"

// TestSameSourceURIReadsEverySpellingOfOneFile is the one comparison a
// breakpoint's source goes through: a plain path, a file URI and its
// percent-encoded form, and a Windows drive path in either slash and either
// case of drive letter, each name one file.
func TestSameSourceURIReadsEverySpellingOfOneFile(t *testing.T) {
	t.Parallel()

	for _, pair := range [][2]string{
		{"/home/u/flows/x.yaml", "/home/u/flows/x.yaml"},
		{"file:///home/u/flows/x.yaml", "/home/u/flows/x.yaml"},
		{"file:///home/u/my%20flows/x%231.yaml", "/home/u/my flows/x#1.yaml"},
		{"file:///c%3A/Users/u/x.yaml", `C:\Users\u\x.yaml`},
		{"file:///C:/Users/u/x.yaml", "c:/Users/u/x.yaml"},
	} {
		if !SameSourceURI(pair[0], pair[1]) {
			t.Errorf("SameSourceURI(%q, %q) = false, want true", pair[0], pair[1])
		}
	}
	for _, pair := range [][2]string{
		{"/home/u/flows/x.yaml", "/home/u/flows/y.yaml"},
		{"/home/u/my%20flows/x.yaml", "/home/u/my flows/x.yaml"},
		{`C:\Users\u\x.yaml`, `D:\Users\u\x.yaml`},
	} {
		if SameSourceURI(pair[0], pair[1]) {
			t.Errorf("SameSourceURI(%q, %q) = true, want false", pair[0], pair[1])
		}
	}
}
