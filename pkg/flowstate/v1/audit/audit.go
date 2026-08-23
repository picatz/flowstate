// Package audit defines the deliberately small boundary between auditable
// actions and the process-specific sinks which retain their records.
//
// Record is not a protobuf on purpose: audit data must never enter a workflow
// payload or history.  Constructors bound and scrub every caller-controlled
// string before an Emitter can observe it.
package audit

import (
	"context"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

const MaxFieldBytes = 256

// Record contains only the stable, pre-redacted identifiers needed to answer
// who attempted what, against which object, and whether it was accepted.
type Record struct {
	Time      time.Time `json:"time"`
	Action    string    `json:"action"`
	Outcome   string    `json:"outcome"`
	Subject   string    `json:"subject,omitempty"`
	Namespace string    `json:"namespace,omitempty"`
	Resource  string    `json:"resource,omitempty"`
	Reason    string    `json:"reason,omitempty"`
}

// Emitter is called at the action, not by tracing or logging callbacks.  An
// error means a required sink could not durably accept the record and callers
// must fail the action closed.
type Emitter interface {
	Emit(context.Context, Record) error
}

type NopEmitter struct{}

func (NopEmitter) Emit(context.Context, Record) error { return nil }

// NewRecord applies the same process scrubber used by other signals, followed
// by a UTF-8-safe byte bound. Callers cannot construct an unsafe record without
// consciously bypassing this constructor.
func NewRecord(scrubber *secrets.Scrubber, action, outcome, subject, namespace, resource, reason string) Record {
	clean := func(s string) string {
		if scrubber != nil {
			s = scrubber.Scrub(s)
		}
		if len(s) <= MaxFieldBytes {
			return s
		}
		n := MaxFieldBytes - len("...(truncated)")
		for n > 0 && !utf8.RuneStart(s[n]) {
			n--
		}
		return strings.TrimSpace(s[:n]) + "...(truncated)"
	}
	return Record{Time: time.Now().UTC(), Action: clean(action), Outcome: clean(outcome), Subject: clean(subject), Namespace: clean(namespace), Resource: clean(resource), Reason: clean(reason)}
}
