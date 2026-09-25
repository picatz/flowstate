package secrets

import (
	"fmt"
	"strings"
	"testing"
)

// BenchmarkScrub measures redacting one HTTP response body, which is what
// pkg/flowstate/v1's http task does to every reply it reads before the body can
// reach an expectation, a task output, or workflow history. The default read
// bound is netpolicy.DefaultMaxResponseBytes (1 MiB), so that is the size the
// worker has to be able to absorb per step.
//
// The shapes are the three that matter and not a sweep: a peer that echoed
// nothing back, which is nearly every reply and the case the scan is built
// around; one that echoed a credential into its body, which is the case the
// scrubber exists for; and a body that is almost entirely registered values,
// which is where the result is far shorter than the buffer it was built in and
// so where [shrinkToFit] is what keeps the answer from retaining it.
//
// pkg/flowstate/v1/celeval_bench_test.go explains what standing a benchmark
// has in this repository; the same applies here.
func BenchmarkScrub(b *testing.B) {
	const value = "tok-live-9f8e7d6c-4b21-4e5a-9c3d-7a1f08e6b2d4"

	scrubber := &Scrubber{}
	scrubber.AddValue(value)

	// Built one at a time, in order: a map would randomize the order between
	// runs and hold every body for a size at once, which at the megabyte size
	// is three of them.
	shapes := []struct {
		name string
		body func(size int) string
	}{
		{"hits=0", func(size int) string { return benchBody(size, value, 0) }},
		{"hits=8", func(size int) string { return benchBody(size, value, 8) }},
		// Wall to wall values: the shape where the answer is a small fraction
		// of the text it was built from, and so the one that would retain the
		// whole builder without [shrinkToFit].
		{"hits=saturated", func(size int) string { return strings.Repeat(value, size/len(value)) }},
	}

	for _, size := range []int{4 << 10, 1 << 20} {
		for _, shape := range shapes {
			name, text := shape.name, shape.body(size)
			b.Run(fmt.Sprintf("size=%d/%s", size, name), func(b *testing.B) {
				b.SetBytes(int64(len(text)))
				b.ReportAllocs()
				for b.Loop() {
					_ = scrubber.Scrub(text)
				}
			})
		}
	}
}

// benchBody builds a response body of about size bytes with value planted in it
// hits times, spread evenly so a scan cannot finish early on the first one.
func benchBody(size int, value string, hits int) string {
	const chunk = `{"id":"7f3a","status":"ok","detail":"the upstream answered without incident"},`

	var b strings.Builder
	for b.Len() < size {
		b.WriteString(chunk)
	}
	text := b.String()[:size]
	if hits == 0 {
		return text
	}

	var out strings.Builder
	step := size / hits
	for i := range hits {
		out.WriteString(text[i*step : (i+1)*step-len(value)])
		out.WriteString(value)
	}

	return out.String()
}
