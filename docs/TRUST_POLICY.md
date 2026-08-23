# Trust admission policies

Issuer entries may combine the compatibility `require` matcher with named CEL
`conditions`. Every matcher and condition must pass. Conditions are compiled and
type-checked when the policy loads and must return `bool`.

The CEL activation is deliberately closed:

- `claims` is `map<string, dyn>` containing only signature-verified claims;
- `request` is a typed `flowstate.auth.v1.TrustAdmissionRequest` containing the
  verified issuer, subject, audiences, and token lifetime; and
- `deployment` is a typed `flowstate.auth.v1.TrustAdmissionDeployment` containing
  the matched issuer-entry name and its configured role and fixed namespace.

It never contains a bearer token, authorization header, certificate bytes, key,
or secret. Evaluation uses Flowstate's common CEL cost and cancellation limits.
A compile, type, missing-value, runtime, cancellation, or cost error denies.
Matched issuer-entry and condition names are retained on the principal for audit;
expressions and claim values are not logged.

## Vetted starting points

These examples are intentionally narrow starting points. Replace names and
Flowstate audiences with exact deployment values; do not weaken exact equality
into prefix or substring matching.

### GitHub Actions

```yaml
- name: github-production
  issuer: https://token.actions.githubusercontent.com
  audiences: [https://flowstate.example.com]
  conditions:
    - name: repository-and-protected-ref
      expression: claims.repository == 'acme/app' && claims.ref == 'refs/heads/main'
  namespace: production
```

Pin the repository (and normally the ref/environment). GitHub's public issuer and
caller-selected audience do not identify a tenant by themselves.

### GitLab

```yaml
- name: gitlab-production
  issuer: https://gitlab.com
  audiences: [https://flowstate.example.com]
  conditions:
    - name: protected-project
      expression: claims.project_path == 'acme/app' && claims.ref_protected == true
  namespace: production
```

For self-managed GitLab, use that instance's exact discovery issuer. For
GitLab.com, always pin a signed namespace/project claim.

### Kubernetes

```yaml
- name: cluster-runner
  issuer: https://kubernetes.default.svc.cluster.local
  audiences: [flowstate]
  conditions:
    - name: service-account
      expression: request.subject == 'system:serviceaccount:flowstate:runner'
  namespace: platform
```

Use projected, audience-bound service-account tokens and pin the full subject;
do not accept legacy non-expiring service-account tokens.

### SPIFFE (mTLS)

```yaml
- name: mesh-runner
  kind: mtls
  issuer: production-mesh
  client_ca_file: /etc/flowstate/spiffe-bundle.pem
  subject_from: uri_san
  conditions:
    - name: workload-id
      expression: request.subject == 'spiffe://prod.example/ns/flowstate/sa/runner'
  namespace: platform
```

Trust a bounded local bundle and one exact URI SAN. Certificate DER and subject
DN are never exposed to CEL.

### Workforce IdP

```yaml
- name: workforce-platform
  issuer: https://id.example.com
  audiences: [https://flowstate.example.com]
  conditions:
    - name: verified-platform-member
      expression: claims.email_verified == true && 'flowstate-platform' in claims.groups
  role: operator
  namespace: platform
```

Use the provider's stable group/object identifiers where available rather than
display names, and require its verified-email signal if email participates.

### Flowstate to Flowstate

```yaml
- name: peer-production
  issuer: https://flowstate.us.example.com
  audiences: [https://flowstate.eu.example.com]
  conditions:
    - name: deployment-workload
      expression: claims['flowstate.deployment'] == 'prod' && claims['flowstate.workflow'] == 'release'
  namespace: platform
```

Pin both the peer deployment and workload claims. Keep the target audience exact
so an assertion minted for a different Flowstate deployment cannot be replayed.
