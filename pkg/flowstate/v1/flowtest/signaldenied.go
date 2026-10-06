package flowtest

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/nearest"
)

// signalOutcomes is what became of each scripted delivery, by signal name. It
// is separate from the transcript, which is optional and truncating, for the
// reason [invocationLog] is: a claim needs a complete account whenever one is
// declared.
//
// The verdict is [v1.LocalSignals.DeliverFrom]'s own, so `expect.denied_signals`
// is decided by the one evaluator the server's Signal door shares
// ([v1.SignalPolicyCheck]) and not by a second reading of the policy.
type signalOutcomes struct {
	mu        sync.Mutex
	delivered map[string]int
	denied    map[string]int
	// otherRefused names a signal a delivery failed for a reason that is not
	// the policy's: an over-deep payload, a full queue.
	otherRefused map[string]int
	// dropped counts the deliveries a `signal:` fault lost, which never
	// reached the policy and so are neither delivered nor denied.
	dropped map[string]int
	// delayed counts the deliveries a `signal:` fault made late, whether or not
	// the run was still there when they arrived.
	delayed map[string]int
}

func newSignalOutcomes() *signalOutcomes {
	return &signalOutcomes{
		delivered:    map[string]int{},
		denied:       map[string]int{},
		otherRefused: map[string]int{},
		dropped:      map[string]int{},
		delayed:      map[string]int{},
	}
}

// note records the result of one delivery of name.
func (o *signalOutcomes) note(name string, err error) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()

	switch {
	case err == nil:
		o.delivered[name]++
	case errors.Is(err, v1.ErrSignalDenied):
		o.denied[name]++
	default:
		o.otherRefused[name]++
	}
}

// noteDropped records that a fault lost one delivery of name.
func (o *signalOutcomes) noteDropped(name string) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()

	o.dropped[name]++
}

// noteDelayed records that a fault made one delivery of name late, when the
// sender sent it; it may arrive after the run has ended.
func (o *signalOutcomes) noteDelayed(name string) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()

	o.delayed[name]++
}

// delayedNames lists, sorted, the signals at least one delivery of which a
// fault made late.
func (o *signalOutcomes) delayedNames() []string {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()

	return slices.Sorted(maps.Keys(o.delayed))
}

// droppedNames lists, sorted, the signals at least one delivery of which a
// fault lost.
func (o *signalOutcomes) droppedNames() []string {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()

	return slices.Sorted(maps.Keys(o.dropped))
}

// checkDeniedSignalNames refuses an `expect.denied_signals:` entry that no
// run could make true, before the run, for the reason [checkExpectationNames]
// does: a claim about a ghost passes or fails forever while reading as a
// check. A name must be a signal the case sends, and one the workflow declares
// a `signals:` policy for, since a signal with no policy has nothing to deny it.
func checkDeniedSignalNames(want *Expectation, scripts []SignalScript, spec *v1.Workflow) error {
	if len(want.DeniedSignals) == 0 {
		return nil
	}
	// A webhook replay the case expects refused ends the case before any run
	// starts, so a signal denial could never be judged; refusing the pair keeps
	// the claim from passing without having been checked.
	if want.Refused != nil && *want.Refused {
		return errors.New("expect.denied_signals cannot be combined with `refused: true`: a refused delivery starts no run, so no signal is ever sent; put the signal denial in a case that starts a run")
	}
	sent := map[string]bool{}
	for _, s := range scripts {
		sent[s.Name] = true
	}
	policies := spec.GetSignals()
	for i, name := range want.DeniedSignals {
		where := fmt.Sprintf("expect.denied_signals[%d]", i)
		if _, declared := policies[name]; !declared {
			if suggestion, ok := nearest.Name(name, slices.Sorted(maps.Keys(policies))); ok {
				return fmt.Errorf("%s names signal %q, which this workflow declares no `signals:` policy for; did you mean %q?", where, name, suggestion)
			}

			return fmt.Errorf("%s names signal %q, which this workflow declares no `signals:` policy for, so nothing could deny it", where, name)
		}
		if !sent[name] {
			return fmt.Errorf("%s names signal %q, which this case never sends; add it to `signals:` with the `sender:` the policy should refuse", where, name)
		}
	}

	return nil
}

// deniedSignalFailures judges `expect.denied_signals:`: each named signal must
// have had at least one delivery refused by its policy. A signal also accepted
// from another sender still counts, so a case can show the wrong sender
// refused and the right one admitted.
func deniedSignalFailures(want *Expectation, o *signalOutcomes) []*v1.Diagnostic {
	if len(want.DeniedSignals) == 0 {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()

	var failures []*v1.Diagnostic
	for i, name := range want.DeniedSignals {
		if o.denied[name] > 0 {
			continue
		}
		reason := "it was never sent, because the run ended before its `at:`"
		switch {
		case o.delivered[name] > 0:
			reason = "it was delivered, so its policy admitted the sender"
		case o.otherRefused[name] > 0:
			reason = "it was refused, but not by its policy (an over-deep payload or a full queue)"
		}
		failures = append(failures, &v1.Diagnostic{
			Field:   fmt.Sprintf("expect.denied_signals[%d]", i),
			Value:   name,
			Message: fmt.Sprintf("expected signal %q to be denied by its policy, but %s", name, reason),
		})
	}

	return failures
}
