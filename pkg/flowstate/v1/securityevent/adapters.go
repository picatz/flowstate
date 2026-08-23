package securityevent

import (
	"fmt"
	"strconv"
	"time"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Shared Signals event type URIs used by CAEP and RISC profiles. Deployments
// may add vendor URIs only by supplying another Adapter; unknown URIs fail.
const (
	CAEPSessionRevoked                  = "https://schemas.openid.net/secevent/caep/event-type/session-revoked"
	CAEPTokenClaimsChange               = "https://schemas.openid.net/secevent/caep/event-type/token-claims-change"
	CAEPCredentialChange                = "https://schemas.openid.net/secevent/caep/event-type/credential-change"
	CAEPDeviceComplianceChange          = "https://schemas.openid.net/secevent/caep/event-type/device-compliance-change"
	CAEPAssuranceLevelChange            = "https://schemas.openid.net/secevent/caep/event-type/assurance-level-change"
	RISCAccountDisabled                 = "https://schemas.openid.net/secevent/risc/event-type/account-disabled"
	RISCAccountCredentialChangeRequired = "https://schemas.openid.net/secevent/risc/event-type/account-credential-change-required"
	RISCAccountPurged                   = "https://schemas.openid.net/secevent/risc/event-type/account-purged"
	RISCIdentifierChanged               = "https://schemas.openid.net/secevent/risc/event-type/identifier-changed"
)

type eventRule struct {
	event   v1.SecurityEventType
	subject v1.SecuritySubjectType
	action  v1.EnforcementAction
}

// SharedSignalsAdapter recognizes the standard CAEP/RISC profiles above plus
// Flowstate's explicit workload-security extensions.
type SharedSignalsAdapter struct {
	Audience string
	TTL      time.Duration
}

func (a SharedSignalsAdapter) Normalize(c Claims) ([]*v1.SecurityEvent, error) {
	iss, iok := c["iss"].(string)
	jti, jok := c["jti"].(string)
	iat, ok := number(c["iat"])
	if !iok || !jok || !ok || iss == "" || jti == "" || a.Audience == "" {
		return nil, ErrRefused
	}
	events, ok := c["events"].(map[string]any)
	if !ok || len(events) != 1 {
		return nil, ErrRefused
	}
	ttl := a.TTL
	if ttl <= 0 {
		ttl = DefaultMaxAge
	}
	issued := time.Unix(iat, 0)
	expires := issued.Add(ttl)
	if exp, present := c["exp"]; present {
		n, good := number(exp)
		if !good {
			return nil, ErrRefused
		}
		expires = time.Unix(n, 0)
	}
	for uri, raw := range events {
		rule, ok := standardRules[uri]
		if !ok {
			return nil, fmt.Errorf("%w: unknown event URI", ErrRefused)
		}
		body, ok := raw.(map[string]any)
		if !ok {
			return nil, ErrRefused
		}
		subject, err := normalizedSubject(body["subject"], rule.subject)
		if err != nil {
			return nil, err
		}
		revision, _ := number(body["policy_revision"])
		return []*v1.SecurityEvent{{Type: rule.event, Issuer: iss, Audience: a.Audience, EventId: jti, IssuedAt: timestamppb.New(issued), ExpiresAt: timestamppb.New(expires), Subject: subject, PolicyRevision: uint64(max(revision, 0)), Enforcement: rule.action}}, nil
	}
	return nil, ErrRefused
}

var standardRules = map[string]eventRule{
	RISCAccountDisabled:                 {v1.SecurityEventType_SECURITY_EVENT_TYPE_PRINCIPAL_DISABLED, v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_PRINCIPAL, v1.EnforcementAction_ENFORCEMENT_ACTION_CANCEL_RUN},
	RISCAccountPurged:                   {v1.SecurityEventType_SECURITY_EVENT_TYPE_PRINCIPAL_DISABLED, v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_PRINCIPAL, v1.EnforcementAction_ENFORCEMENT_ACTION_CANCEL_RUN},
	RISCAccountCredentialChangeRequired: {v1.SecurityEventType_SECURITY_EVENT_TYPE_CREDENTIAL_COMPROMISED, v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_TOKEN, v1.EnforcementAction_ENFORCEMENT_ACTION_QUARANTINE_RUN},
	CAEPSessionRevoked:                  {v1.SecurityEventType_SECURITY_EVENT_TYPE_SESSION_REVOKED, v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_SESSION, v1.EnforcementAction_ENFORCEMENT_ACTION_CANCEL_RUN},
	CAEPDeviceComplianceChange:          {v1.SecurityEventType_SECURITY_EVENT_TYPE_DEVICE_POSTURE_CHANGED, v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_DEVICE, v1.EnforcementAction_ENFORCEMENT_ACTION_PAUSE_RUN},
	CAEPAssuranceLevelChange:            {v1.SecurityEventType_SECURITY_EVENT_TYPE_DEVICE_POSTURE_CHANGED, v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_DEVICE, v1.EnforcementAction_ENFORCEMENT_ACTION_PAUSE_RUN},
	CAEPTokenClaimsChange:               {v1.SecurityEventType_SECURITY_EVENT_TYPE_GROUP_MEMBERSHIP_CHANGED, v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_GROUP, v1.EnforcementAction_ENFORCEMENT_ACTION_FUTURE_EXTERNAL_CALLS},
	CAEPCredentialChange:                {v1.SecurityEventType_SECURITY_EVENT_TYPE_CREDENTIAL_COMPROMISED, v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_TOKEN, v1.EnforcementAction_ENFORCEMENT_ACTION_QUARANTINE_RUN},
	"https://flowstate.dev/secevent/application-access-withdrawn": {v1.SecurityEventType_SECURITY_EVENT_TYPE_APPLICATION_ACCESS_WITHDRAWN, v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_APPLICATION, v1.EnforcementAction_ENFORCEMENT_ACTION_CANCEL_RUN},
	"https://flowstate.dev/secevent/delegation-revoked":           {v1.SecurityEventType_SECURITY_EVENT_TYPE_DELEGATION_REVOKED, v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_DELEGATION, v1.EnforcementAction_ENFORCEMENT_ACTION_CANCEL_RUN},
	"https://flowstate.dev/secevent/issuer-key-compromised":       {v1.SecurityEventType_SECURITY_EVENT_TYPE_ISSUER_KEY_COMPROMISED, v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_ISSUER_KEY, v1.EnforcementAction_ENFORCEMENT_ACTION_QUARANTINE_RUN},
	"https://flowstate.dev/secevent/tenant-relationship-removed":  {v1.SecurityEventType_SECURITY_EVENT_TYPE_TENANT_RELATIONSHIP_REMOVED, v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_TENANT_RELATIONSHIP, v1.EnforcementAction_ENFORCEMENT_ACTION_CANCEL_RUN},
}

func normalizedSubject(raw any, want v1.SecuritySubjectType) (*v1.SecuritySubject, error) {
	s, ok := raw.(map[string]any)
	if !ok {
		return nil, ErrRefused
	}
	typ, _ := s["subject_type"].(string)
	expected := map[v1.SecuritySubjectType]string{v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_PRINCIPAL: "principal", v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_SESSION: "session", v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_TOKEN: "token", v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_DEVICE: "device", v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_GROUP: "group", v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_APPLICATION: "application", v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_DELEGATION: "delegation", v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_ISSUER_KEY: "issuer_key", v1.SecuritySubjectType_SECURITY_SUBJECT_TYPE_TENANT_RELATIONSHIP: "tenant_relationship"}[want]
	if typ != expected {
		return nil, ErrRefused
	}
	id, _ := s["id"].(string)
	if id == "" {
		id, _ = s["sub"].(string)
	}
	if id == "" {
		return nil, ErrRefused
	}
	tenant, _ := s["tenant"].(string)
	return &v1.SecuritySubject{Type: want, Identifier: id, Tenant: tenant}, nil
}

func number(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		if n != float64(int64(n)) {
			return 0, false
		}
		return int64(n), true
	case int64:
		return n, true
	case jsonNumber:
		x, e := strconv.ParseInt(string(n), 10, 64)
		return x, e == nil
	}
	return 0, false
}

type jsonNumber string
