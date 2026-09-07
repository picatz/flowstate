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

// RenderedSpans renders every recorded span through the containment shapes
// CLAUDE.md names rather than through the containment value: the four verbs
// `%v`, `%+v`, `%#v` and `%s`, over the batch, over each span, over a struct
// holding those through an *unexported* field and over a slice of such
// structs — which is the whole point, because `fmt` cannot call a method on a
// value it reaches that way and prints the fields instead. A redacting
// String() protects a value printed directly and does nothing one level down.
// Every name, description, attribute, event and link is rendered on its own
// as well.
//
// The `%s` shapes are over [spanText] rather than over [tracetest.SpanStub],
// which is not a decision about coverage: a SpanStub is mostly ints and
// timestamps, so `go vet` rejects the verb against it and what it would print
// is `%!s(int=0)` beside the strings the other three verbs already printed.
// spanText is the string-shaped part of the same span, reached through
// unexported fields, which is where `%s` means something.
//
// One renderer for every containment test in the module (Codex, #1836): the
// conformance cases and the netpolicy and plugin tests each had their own,
// and they had already diverged — one rendered links, the other the `%s`
// shapes — so a credential exposed through one shape could pass the tests
// that used the other renderer.
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
			fmt.Sprintf("%v", w), fmt.Sprintf("%+v", w), fmt.Sprintf("%#v", w),
			fmt.Sprintf("%v", []wrapper{w}), fmt.Sprintf("%+v", []wrapper{w}),
			fmt.Sprintf("%#v", []wrapper{w}))
	}

	texts := spanTexts(stubs)
	rendered = append(rendered,
		fmt.Sprintf("%v", texts), fmt.Sprintf("%+v", texts),
		fmt.Sprintf("%#v", texts), fmt.Sprintf("%s", texts))

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

	for _, text := range texts {
		rendered = append(rendered,
			fmt.Sprintf("%v", text), fmt.Sprintf("%+v", text),
			fmt.Sprintf("%#v", text), fmt.Sprintf("%s", text))
	}

	return rendered
}

// spanText is everything a span says in words, held through unexported fields.
//
// Unexported deliberately: this is the arrangement a redacting formatter cannot
// survive, so it is the arrangement the containment assertions have to check.
type spanText struct {
	name        string
	description string
	attributes  []string
	events      []string
}

// spanTexts reduces recorded spans to their [spanText].
func spanTexts(stubs []tracetest.SpanStub) []spanText {
	texts := make([]spanText, 0, len(stubs))
	for _, stub := range stubs {
		text := spanText{name: stub.Name, description: stub.Status.Description}
		for _, attr := range stub.Attributes {
			text.attributes = append(text.attributes, string(attr.Key)+"="+attr.Value.String())
		}
		for _, event := range stub.Events {
			text.events = append(text.events, event.Name)
			for _, attr := range event.Attributes {
				text.events = append(text.events, string(attr.Key)+"="+attr.Value.String())
			}
		}
		texts = append(texts, text)
	}

	return texts
}
