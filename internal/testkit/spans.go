package testkit

import (
	"context"
	"fmt"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// RecordSpans installs a recording tracer provider for the duration of a test
// and returns the recorder.
//
// The global provider is where the code under test sends its spans, so it is
// the global that is replaced, and it is restored afterward because the test
// binary is shared with every other test in the package: a provider left
// installed would record one test's spans into another's assertions.
func RecordSpans(t testing.TB) *tracetest.SpanRecorder {
	t.Helper()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)

	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		_ = provider.Shutdown(context.Background())
	})

	return recorder
}

// RenderedSpans renders every recorded span through the %v family — over the
// batch, over each span individually, and over a struct holding one — plus
// every name, description, attribute, event and link on its own.
//
// These are the containment shapes a "nothing sensitive reached a span"
// assertion has to cover: `fmt` reaching a value through an unexported field
// prints the fields rather than calling any accessor, so a value that redacts
// itself in `String()` is still printed whole by `%+v` on a struct that holds
// it. A test that checked one rendering would pass on exactly the shape a
// collector's exporter uses.
func RenderedSpans(recorder *tracetest.SpanRecorder) []string {
	stubs := tracetest.SpanStubsFromReadOnlySpans(recorder.Ended())

	type wrapper struct {
		one   tracetest.SpanStub
		batch []tracetest.SpanStub
	}

	rendered := []string{
		fmt.Sprintf("%v", stubs),
		fmt.Sprintf("%+v", stubs),
		fmt.Sprintf("%#v", stubs),
	}

	if len(stubs) > 0 {
		w := wrapper{one: stubs[0], batch: stubs}
		rendered = append(rendered,
			fmt.Sprintf("%v", w), fmt.Sprintf("%+v", w), fmt.Sprintf("%#v", w))
	}

	for _, stub := range stubs {
		rendered = append(rendered,
			fmt.Sprintf("%v", stub),
			fmt.Sprintf("%+v", stub),
			fmt.Sprintf("%#v", stub),
			stub.Name,
			stub.Status.Description,
		)

		for _, attr := range stub.Attributes {
			rendered = append(rendered, string(attr.Key), attr.Value.String(),
				fmt.Sprintf("%v", attr), fmt.Sprintf("%+v", attr), fmt.Sprintf("%#v", attr))
		}

		for _, event := range stub.Events {
			rendered = append(rendered, event.Name, fmt.Sprintf("%+v", event), fmt.Sprintf("%#v", event))
		}

		for _, link := range stub.Links {
			rendered = append(rendered, fmt.Sprintf("%+v", link), fmt.Sprintf("%#v", link))
		}
	}

	return rendered
}
