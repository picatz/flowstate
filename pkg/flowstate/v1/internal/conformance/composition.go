package conformance

import (
	"context"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"google.golang.org/protobuf/proto"
)

const temporaryEnvironmentContract = "flowstate.example.temporary_environment.v1"

// CompositionPrototype builds the approval-driven temporary-environment case
// used by both drivers. The leaf library is cloned unchanged behind two copies
// of a forwarding wrapper; admission binds those copies to different providers.
func CompositionPrototype(record func(string)) (*v1.Workflow, []v1.TaskDef, *v1.PluginCatalog, *v1.Workflow_StepOutputs, error) {
	providers := []string{"billing-environment", "support-environment"}
	defs := make([]v1.TaskDef, 0, len(providers)*2)
	plugins := make([]*v1.PluginDescription, 0, len(providers))
	for _, provider := range providers {
		provider := provider
		provision := v1.TaskDef{
			Name: provider + ".provision",
			Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
				record(provider + ":provision")
				return &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
					"environment_id": v1.NewLiteral(provider + "-1"),
				}}, nil
			},
		}
		release := v1.TaskDef{
			Name: provider + ".release",
			Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
				record(provider + ":release")
				return &v1.Node_Outputs{}, nil
			},
		}
		defs = append(defs, provision, release)
		plugins = append(plugins, &v1.PluginDescription{
			Name: provider, Version: "v1.0.0", ProtocolVersion: 2,
			Tasks:              []*v1.TaskDescription{v1.DescribeTask(provision), v1.DescribeTask(release)},
			TaskSchemaDigest:   "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			DistributionDigest: v1.ContentDigest([]byte(provider)),
			ClaimsDigest:       "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		})
	}

	contractDigest, err := v1.CapabilityContractDigest(plugins[0].GetTasks())
	if err != nil {
		return nil, nil, nil, nil, err
	}
	parameter := func(name string) *v1.CapabilityParameter {
		return &v1.CapabilityParameter{
			Name: name, Contract: temporaryEnvironmentContract, ContractDigest: contractDigest,
			Tasks: []string{"provision", "release"},
		}
	}

	library := &v1.Workflow{
		Name:    "temporary-environment-library",
		Profile: v1.CurrentProfile,
		DeclaredInputs: []*v1.InputDeclaration{
			{Name: "approved", Type: v1.InputDeclaration_TYPE_BOOL, Required: true},
		},
		CapabilityParameters: []*v1.CapabilityParameter{parameter("environment")},
		Steps: []*v1.Node{
			{
				Id: "provision", Condition: v1.NewExpr("inputs.approved"),
				Kind: &v1.Node_Task{Task: &v1.Task{Name: "environment.provision"}},
				Undo: &v1.Compensation{Task: &v1.Task{
					Name:   "environment.release",
					Inputs: map[string]*v1.Value{"environment_id": v1.NewExpr("steps.provision.environment_id")},
				}},
			},
			{
				Id: "release", Condition: v1.NewExpr("inputs.approved"),
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "environment.release",
					Inputs: map[string]*v1.Value{"environment_id": v1.NewExpr("steps.provision.environment_id")},
				}},
			},
		},
		DeclaredOutputs: []*v1.OutputDeclaration{{
			Name: "environment_id", Value: v1.NewExpr("steps.provision.environment_id"),
		}},
	}

	wrapper := &v1.Workflow{
		Name:    "approved-environment-wrapper",
		Profile: v1.CurrentProfile,
		DeclaredInputs: []*v1.InputDeclaration{
			{Name: "approved", Type: v1.InputDeclaration_TYPE_BOOL, Required: true},
		},
		CapabilityParameters: []*v1.CapabilityParameter{parameter("environment")},
		Steps: []*v1.Node{{
			Id: "environment",
			Kind: &v1.Node_Call{Call: &v1.Call{
				Workflow:            proto.Clone(library).(*v1.Workflow),
				Arguments:           map[string]*v1.Value{"approved": v1.NewExpr("inputs.approved")},
				CapabilityArguments: map[string]string{"environment": "environment"},
			}},
		}},
		DeclaredOutputs: []*v1.OutputDeclaration{{
			Name: "environment_id", Value: v1.NewExpr("steps.environment.environment_id"),
		}},
	}

	root := &v1.Workflow{
		Name:    "approval-driven-temporary-environments",
		Profile: v1.CurrentProfile,
		CapabilityParameters: []*v1.CapabilityParameter{
			parameter("billing"), parameter("support"),
		},
		Steps: []*v1.Node{
			{
				Id: "billing",
				Kind: &v1.Node_Call{Call: &v1.Call{
					Workflow:            proto.Clone(wrapper).(*v1.Workflow),
					Arguments:           map[string]*v1.Value{"approved": v1.NewLiteral(true)},
					CapabilityArguments: map[string]string{"environment": "billing"},
				}},
			},
			{
				Id: "support",
				Kind: &v1.Node_Call{Call: &v1.Call{
					Workflow:            proto.Clone(wrapper).(*v1.Workflow),
					Arguments:           map[string]*v1.Value{"approved": v1.NewLiteral(true)},
					CapabilityArguments: map[string]string{"environment": "support"},
				}},
			},
		},
	}

	catalog := &v1.PluginCatalog{Plugins: plugins, ClaimsSchemaVersion: v1.CurrentClaimsSchemaVersion}
	for _, provider := range providers {
		binding := &v1.CapabilityBinding{
			BindingId: provider + "-stable", Qualifier: provider,
			Contract: temporaryEnvironmentContract, ContractDigest: contractDigest,
			Provider:           &v1.CapabilityBinding_Plugin{Plugin: &v1.CapabilityBinding_PluginProvider{PluginName: provider}},
			Locality:           v1.CapabilityBinding_LOCALITY_LOCAL,
			CredentialPostures: []string{"workload_identity"}, TrustTier: 3,
			Rehearsal: v1.CapabilityBinding_REHEARSAL_POSTURE_DISPATCH,
		}
		binding.Revision = v1.CapabilityBindingRevision(binding)
		catalog.CapabilityBindings = append(catalog.CapabilityBindings, binding)
	}

	expected := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"billing": {NamedValues: map[string]*v1.Value{"environment_id": v1.NewLiteral("billing-environment-1")}},
		"support": {NamedValues: map[string]*v1.Value{"environment_id": v1.NewLiteral("support-environment-1")}},
	}}
	return root, defs, catalog, expected, nil
}
