// Package stepup coordinates interactive authorization outside workflow code.
//
// A workflow may persist Reference and nothing else. In particular, Provider is
// the protocol boundary: PKCE verifiers, codes, tokens, proof keys, and browser
// state remain in that runtime component and are never returned by Coordinator.
package stepup

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

var (
	ErrInvalid       = errors.New("invalid step-up request")
	ErrNotFound      = errors.New("step-up transaction not found")
	ErrPending       = errors.New("step-up authorization is pending")
	ErrDenied        = errors.New("step-up authorization was denied")
	ErrExpired       = errors.New("step-up authorization expired")
	ErrCanceled      = errors.New("step-up authorization was canceled")
	ErrPolicyChanged = errors.New("step-up policy changed")
	ErrPlanChanged   = errors.New("step-up action changed")
)

// Status is deliberately non-secret and safe for workflow history.
type Status string

const (
	StatusPending  Status = "pending"
	StatusComplete Status = "complete"
	StatusDenied   Status = "denied"
	StatusExpired  Status = "expired"
	StatusCanceled Status = "canceled"
	StatusConsumed Status = "consumed"
)

// Reference is the complete value permitted to cross into workflow history.
// It contains no protocol artifact and survives Continue-As-New unchanged.
type Reference struct {
	TransactionID     string
	RequiredAssurance string
	PlanDigest        string
	ExpiresAt         time.Time
	StatusReference   string
}

// Binding is held by the runtime store and binds a one-shot transaction to the
// complete authorization context. None of its values is accepted from a resume
// request.
type Binding struct {
	Principal, OAuthClient, ProtectedResource string
	RequestedAction, Resource, Tenant         string
	RedirectURI, PolicyRevision, ProofKey     string
	ActorChain                                []string
}

// Request describes a new, narrowly bound authorization transaction.
type Request struct {
	Binding
	RequiredAssurance string
	PlanDigest        string
	TTL               time.Duration
	PreferDeviceFlow  bool
}

// Presentation is ephemeral output for a CLI or MCP caller. It must never be
// placed in workflow state: VerificationURL can contain browser protocol state.
type Presentation struct {
	Message         string `json:"message"`
	VerificationURL string `json:"verification_url,omitempty"`
	UserCode        string `json:"user_code,omitempty"`
	DeviceFlow      bool   `json:"device_flow,omitempty"`
}

// Record is runtime-only transaction metadata. A Store must provide atomic
// updates so duplicate callbacks and concurrent resumes cannot consume twice.
type Record struct {
	Reference Reference
	Binding   Binding
	Status    Status
}

// Store is external to Temporal. Mutate atomically changes a record and is the
// replay/duplicate-callback serialization point.
type Store interface {
	Create(context.Context, Record) error
	Get(context.Context, string) (Record, error)
	Mutate(context.Context, string, func(*Record) error) error
}

// Provider owns every secret-bearing OAuth artifact. Complete validates a
// callback server-side; Validate proves completion without exporting a token.
type Provider interface {
	Begin(context.Context, string, Binding, bool) (Presentation, error)
	Complete(context.Context, string, url.Values) (Status, error)
	Validate(context.Context, string, Binding, string) error
	Cancel(context.Context, string) error
}

// Authorizer re-evaluates current policy for the exact bound operation.
type Authorizer interface {
	Reauthorize(context.Context, Binding, string, string) error
}

type Coordinator struct {
	Store      Store
	Provider   Provider
	Authorizer Authorizer
	Now        func() time.Time
	NewID      func() (string, error)
}

func (c *Coordinator) Start(ctx context.Context, req Request) (Reference, Presentation, error) {
	if err := req.validate(); err != nil {
		return Reference{}, Presentation{}, err
	}
	if c.Store == nil || c.Provider == nil || c.NewID == nil {
		return Reference{}, Presentation{}, fmt.Errorf("%w: coordinator is not configured", ErrInvalid)
	}
	id, err := c.NewID()
	if err != nil || id == "" {
		return Reference{}, Presentation{}, fmt.Errorf("starting step-up transaction")
	}
	now := time.Now()
	if c.Now != nil {
		now = c.Now()
	}
	ref := Reference{TransactionID: id, RequiredAssurance: req.RequiredAssurance, PlanDigest: req.PlanDigest, ExpiresAt: now.Add(req.TTL), StatusReference: statusReference(id)}
	record := Record{Reference: ref, Binding: cloneBinding(req.Binding), Status: StatusPending}
	if err := c.Store.Create(ctx, record); err != nil {
		return Reference{}, Presentation{}, fmt.Errorf("starting step-up transaction")
	}
	presentation, err := c.Provider.Begin(ctx, id, record.Binding, req.PreferDeviceFlow)
	if err != nil {
		_ = c.Store.Mutate(ctx, id, func(r *Record) error { r.Status = StatusCanceled; return nil })
		return Reference{}, Presentation{}, fmt.Errorf("starting step-up authorization")
	}
	if presentation.Message == "" {
		if presentation.DeviceFlow {
			presentation.Message = "Open the verification URL on any device and enter the user code."
		} else {
			presentation.Message = "Open the verification URL in a browser to continue."
		}
	}
	return ref, presentation, nil
}

// Callback processes an authorization-server response without ever accepting
// protocol values into a workflow, command line, log field, or returned error.
func (c *Coordinator) Callback(ctx context.Context, id string, values url.Values) error {
	r, err := c.Store.Get(ctx, id)
	if err != nil {
		return ErrNotFound
	}
	if err := c.checkExpiry(r); err != nil {
		return err
	}
	if r.Status != StatusPending {
		if r.Status == StatusComplete || r.Status == StatusDenied {
			return nil
		}
		return statusError(r.Status)
	}
	status, err := c.Provider.Complete(ctx, id, values)
	if err != nil {
		return errors.New("step-up callback validation failed")
	}
	if status != StatusComplete && status != StatusDenied {
		return errors.New("step-up callback returned an invalid status")
	}
	return c.Store.Mutate(ctx, id, func(current *Record) error {
		if current.Status == status {
			return nil
		} // duplicate callback
		if current.Status != StatusPending {
			return statusError(current.Status)
		}
		current.Status = status
		return nil
	})
}

// Resume is intended to run in a server-side activity. It validates provider
// completion, compares the persisted digest in constant time, reauthorizes the
// bound operation under current policy, and consumes the grant atomically.
func (c *Coordinator) Resume(ctx context.Context, ref Reference, currentPlanDigest, currentPolicyRevision string) error {
	r, err := c.Store.Get(ctx, ref.TransactionID)
	if err != nil {
		return ErrNotFound
	}
	if !sameReference(ref, r.Reference) {
		return ErrPlanChanged
	}
	if err := c.checkLive(r); err != nil {
		return err
	}
	if subtle.ConstantTimeCompare([]byte(r.Reference.PlanDigest), []byte(currentPlanDigest)) != 1 {
		return ErrPlanChanged
	}
	if r.Binding.PolicyRevision != currentPolicyRevision {
		return ErrPolicyChanged
	}
	if err := c.Provider.Validate(ctx, ref.TransactionID, r.Binding, r.Reference.RequiredAssurance); err != nil {
		return errors.New("step-up validation failed")
	}
	if c.Authorizer == nil {
		return errors.New("step-up reauthorization is unavailable")
	}
	if err := c.Authorizer.Reauthorize(ctx, r.Binding, currentPlanDigest, currentPolicyRevision); err != nil {
		return errors.New("step-up reauthorization denied")
	}
	return c.Store.Mutate(ctx, ref.TransactionID, func(current *Record) error {
		if current.Status != StatusComplete {
			return statusError(current.Status)
		}
		current.Status = StatusConsumed // terminal consumption; never a reusable grant
		return nil
	})
}

func (c *Coordinator) Cancel(ctx context.Context, ref Reference) error {
	_ = c.Provider.Cancel(ctx, ref.TransactionID)
	return c.Store.Mutate(ctx, ref.TransactionID, func(r *Record) error {
		if r.Status == StatusCanceled {
			return nil
		}
		r.Status = StatusCanceled
		return nil
	})
}

func (c *Coordinator) checkLive(r Record) error {
	if err := c.checkExpiry(r); err != nil {
		return err
	}
	return statusError(r.Status)
}

func (c *Coordinator) checkExpiry(r Record) error {
	now := time.Now()
	if c.Now != nil {
		now = c.Now()
	}
	if !now.Before(r.Reference.ExpiresAt) {
		_ = c.Store.Mutate(context.Background(), r.Reference.TransactionID, func(x *Record) error { x.Status = StatusExpired; return nil })
		return ErrExpired
	}
	return nil
}

func statusError(s Status) error {
	switch s {
	case StatusPending:
		return ErrPending
	case StatusComplete:
		return nil
	case StatusDenied:
		return ErrDenied
	case StatusExpired:
		return ErrExpired
	case StatusCanceled, StatusConsumed:
		return ErrCanceled
	default:
		return errors.New("invalid step-up status")
	}
}
func sameReference(a, b Reference) bool {
	return a.TransactionID == b.TransactionID && a.RequiredAssurance == b.RequiredAssurance && a.PlanDigest == b.PlanDigest && a.ExpiresAt.Equal(b.ExpiresAt) && a.StatusReference == b.StatusReference
}
func cloneBinding(b Binding) Binding { b.ActorChain = append([]string(nil), b.ActorChain...); return b }
func statusReference(id string) string {
	sum := sha256.Sum256([]byte("flowstate-step-up-status\x00" + id))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func (r Request) validate() error {
	if r.TTL <= 0 || r.TTL > 15*time.Minute || r.RequiredAssurance == "" || r.PlanDigest == "" {
		return fmt.Errorf("%w: assurance, digest, and a TTL no greater than 15 minutes are required", ErrInvalid)
	}
	values := []string{r.Principal, r.OAuthClient, r.ProtectedResource, r.RequestedAction, r.Resource, r.Tenant, r.RedirectURI, r.PolicyRevision, r.ProofKey}
	for _, v := range values {
		if strings.TrimSpace(v) == "" {
			return fmt.Errorf("%w: every authorization binding is required", ErrInvalid)
		}
	}
	if len(r.ActorChain) == 0 || len(r.ActorChain) > 16 {
		return fmt.Errorf("%w: actor chain must contain 1 to 16 actors", ErrInvalid)
	}
	u, err := url.Parse(r.RedirectURI)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Fragment != "" {
		return fmt.Errorf("%w: redirect URI must be an absolute HTTPS URL without userinfo or fragment", ErrInvalid)
	}
	return nil
}
