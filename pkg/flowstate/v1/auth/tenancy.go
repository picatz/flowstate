package auth

import (
	"fmt"
	"maps"
	"slices"
)

// Tenancy maps Flowstate namespaces onto the Temporal namespaces their runs
// execute in.
//
// Temporal namespaces isolate history and visibility, so a deployment that wants
// one tenant unable to read another's run history maps them apart. It is optional
// on purpose: a single-team deployment should not have to operate several Temporal
// namespaces to use the engine, and a first run should need none of this. With no
// Tenancy configured, every run uses whatever Temporal namespace the deployment was
// started with.
//
// This type is only the mapping. Dialing and selecting clients belongs to the
// connection layer, which asks [Tenancy.TemporalNamespace] per run and can
// pre-dial everything [Tenancy.TemporalNamespaces] returns.
type Tenancy struct {
	// Temporal maps a Flowstate namespace to the Temporal namespace its runs
	// execute in.
	Temporal map[string]string `json:"temporal,omitempty" yaml:"temporal,omitempty"`

	// Default is the Temporal namespace for a Flowstate namespace with no entry
	// in the map.
	//
	// Leaving it empty means an unmapped namespace is refused with
	// [ErrNoTemporalNamespace], which is the right choice for a deployment that
	// separates tenants: a tenant whose isolation was asked for but not configured
	// must not quietly land in another tenant's namespace. Set it for a deployment
	// where most tenants share one namespace and only some are separated.
	Default string `json:"default,omitempty" yaml:"default,omitempty"`
}

// Validate reports whether the mapping is usable.
func (t *Tenancy) Validate() error {
	if t == nil {
		return nil
	}

	for _, namespace := range slices.Sorted(maps.Keys(t.Temporal)) {
		switch {
		case namespace == "":
			return fmt.Errorf("%w: a temporal mapping key must name a Flowstate namespace", ErrInvalidPolicy)
		case t.Temporal[namespace] == "":
			return fmt.Errorf("%w: namespace %q maps to an empty Temporal namespace; remove the entry to fall back to the default, or name one",
				ErrInvalidPolicy, namespace)
		}
	}

	return nil
}

// TemporalNamespace returns the Temporal namespace the given Flowstate namespace's
// runs execute in.
//
// It reports false with no error when this deployment maps nothing, meaning the
// caller should use the Temporal namespace it was configured with. It returns
// [ErrNoTemporalNamespace] when the deployment does map namespaces but has neither
// an entry for this one nor a default, which is a configuration gap rather than a
// reason to place a tenant's runs somewhere arbitrary.
func (t *Tenancy) TemporalNamespace(namespace string) (string, bool, error) {
	if t == nil || (len(t.Temporal) == 0 && t.Default == "") {
		return "", false, nil
	}

	if mapped, ok := t.Temporal[namespace]; ok {
		return mapped, true, nil
	}

	if t.Default != "" {
		return t.Default, true, nil
	}

	return "", false, fmt.Errorf("%w: namespace %q has no temporal mapping and no default is set",
		ErrNoTemporalNamespace, truncate(namespace, 128))
}

// TemporalNamespaces returns every Temporal namespace this mapping can select,
// sorted and without duplicates.
//
// A connection layer uses it to dial one client per namespace at startup, so that
// selecting a client per run is a map lookup rather than a connection attempt. It
// is empty when nothing is mapped.
func (t *Tenancy) TemporalNamespaces() []string {
	if t == nil {
		return nil
	}

	namespaces := make([]string, 0, len(t.Temporal)+1)
	if t.Default != "" {
		namespaces = append(namespaces, t.Default)
	}
	for _, mapped := range t.Temporal {
		if !slices.Contains(namespaces, mapped) {
			namespaces = append(namespaces, mapped)
		}
	}

	slices.Sort(namespaces)

	return namespaces
}
