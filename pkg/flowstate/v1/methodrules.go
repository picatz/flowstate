package flowstatev1

import (
	"iter"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// methodRules reads the method_rules option off every WorkflowService method,
// once. A method that declares none maps to
// the zero rules, whose effect is METHOD_EFFECT_UNSPECIFIED, which
// TestEveryRPCDeclaresAnEffect refuses. Lazy for the reason [boundActions] is.
var methodRules = sync.OnceValue(func() map[string]*MethodRules {
	methods := File_flowstate_v1_service_proto.Services().ByName("WorkflowService").Methods()
	rules := make(map[string]*MethodRules, methods.Len())
	for i := range methods.Len() {
		method := methods.Get(i)
		options, _ := method.Options().(*descriptorpb.MethodOptions)
		declared, _ := proto.GetExtension(options, E_MethodRules).(*MethodRules)
		rules[string(method.Name())] = declared
	}

	return rules
})

// MethodRulesForRPC returns the rules the named WorkflowService method
// declares, and false when the method is unknown or declares none.
func MethodRulesForRPC(rpc string) (*MethodRules, bool) {
	rules, ok := methodRules()[rpc]
	if !ok || rules == nil {
		return nil, false
	}

	return rules, true
}

// AllMethodRules yields every WorkflowService method name with its declared
// rules (nil when it declares none), in no particular order.
func AllMethodRules() iter.Seq2[string, *MethodRules] {
	return func(yield func(string, *MethodRules) bool) {
		for rpc, rules := range methodRules() {
			if !yield(rpc, rules) {
				return
			}
		}
	}
}
