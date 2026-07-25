package vault

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// Resolve implements [secrets.Provider], reading one field of one KV v2 secret.
//
// The value is returned through [secrets.NewSecret] and is not retained: no copy
// of it outlives this call, and nothing about it is logged. Wrap the provider in a
// [secrets.Cache] to bound how often Vault is asked.
func (p *Provider) Resolve(ctx context.Context, req secrets.Request) (secrets.Secret, error) {
	ref := req.Ref

	if ref == nil {
		return secrets.Secret{}, &secrets.ResolveError{
			Ref: ref,
			Err: fmt.Errorf("%w: reference is missing", secrets.ErrInvalidRef),
		}
	}

	if ctx == nil {
		return secrets.Secret{}, &secrets.ResolveError{
			Ref: ref,
			Err: fmt.Errorf("secrets/vault: Resolve requires a context"),
		}
	}

	// A cancelled activity should not start a network round trip, and should not
	// spend a token on one either.
	if err := ctx.Err(); err != nil {
		return secrets.Secret{}, &secrets.ResolveError{Ref: ref, Err: err}
	}

	path, field, err := parseName(ref.GetName())
	if err != nil {
		return secrets.Secret{}, &secrets.ResolveError{Ref: ref, Err: err}
	}

	apiPath, err := p.secretPath(req.Namespace, path)
	if err != nil {
		return secrets.Secret{}, &secrets.ResolveError{Ref: ref, Err: err}
	}

	body, err := p.read(ctx, apiPath)
	if err != nil {
		return secrets.Secret{}, &secrets.ResolveError{Ref: ref, Err: err}
	}

	value, err := p.fieldValue(body, apiPath, field)
	if err != nil {
		return secrets.Secret{}, &secrets.ResolveError{Ref: ref, Err: err}
	}

	return secrets.NewSecret(ref, value), nil
}

// read fetches the body of a KV v2 data path, authenticating and classifying the
// result.
//
// A 403 is retried exactly once, and only after a fresh login. Vault answers 403
// both for a token it no longer accepts and for a path policy forbids, and the API
// does not say which — so the one retry distinguishes them: if a new token is also
// refused, the refusal is about the path, and that is permanent. A static token has
// nothing to retry with, so its 403 is final on the first attempt.
func (p *Provider) read(ctx context.Context, apiPath string) ([]byte, error) {
	const attempts = 2

	for attempt := range attempts {
		token, generation, err := p.authToken(ctx)
		if err != nil {
			return nil, err
		}

		status, body, err := p.do(ctx, http.MethodGet, apiPath, token, nil)
		if err != nil {
			// A vault that answered "sealed" and then failed to deliver the body of
			// that answer is an unavailable vault, not an oversized response: the
			// status is the more accurate of the two classifications, and the engine
			// retries on it. Except when the caller is what ended: its own error is
			// already in hand, and reclassifying it would tell the engine to retry a
			// step that has been abandoned.
			if ctx.Err() == nil && unavailable(status) {
				return nil, unavailableStatus(p.addr, status, apiPath)
			}

			return nil, err
		}

		switch {
		case status == http.StatusOK:
			return body, nil

		case status == http.StatusForbidden:
			if attempt == 0 && p.canReauthenticate() {
				p.forget(generation)
				continue
			}

			// Whatever token is cached now is either the one just issued or the only
			// one there will ever be, and in neither case is it the problem: this
			// refusal is about the path. Keeping it is what stops one forbidden
			// secret from costing every other read a login — and, for a static
			// token, from making every secret unreadable until a restart.
			return nil, fmt.Errorf(
				"%w: %s refused to read %q",
				secrets.ErrPermission, p.addr, apiPath,
			)

		case status == http.StatusNotFound:
			return nil, p.notFound(apiPath, body)

		case unavailable(status):
			return nil, unavailableStatus(p.addr, status, apiPath)

		default:
			// Unclassified, and therefore permanent. A 4xx this provider does not
			// recognize means the request was wrong rather than badly timed, and
			// retrying a wrong request only spends the step's attempt budget.
			return nil, fmt.Errorf("%s answered %d reading %q", p.addr, status, apiPath)
		}
	}

	// Unreachable: the loop either returns or continues exactly once.
	return nil, fmt.Errorf("%w: %s refused to read %q", secrets.ErrPermission, p.addr, apiPath)
}

// notFound reports a 404 as a missing secret, with the hint an operator needs.
//
// Vault answers 404 for a secret that is not there, for a mount that is not there,
// and for a KV v1 mount asked for a versioned path. All three are permanent, and
// all three look identical to a workflow, so the difference is drawn from the
// shape of the error body — matched against, never quoted.
func (p *Provider) notFound(apiPath string, body []byte) error {
	if bytes.Contains(body, []byte("no handler for route")) {
		return fmt.Errorf(
			"%w: %s has no KV v2 mount at %q, so %q resolves to nothing",
			secrets.ErrNotFound, p.addr, p.mount, apiPath,
		)
	}

	return fmt.Errorf(
		"%w: %s holds no secret at %q (a KV v1 mount answers the same way, so check that %q is KV v2)",
		secrets.ErrNotFound, p.addr, apiPath, p.mount,
	)
}

// kvSecret is the shape of a KV v2 read: the value of the current version lives at
// data.data, and its version metadata beside it at data.metadata.
//
// Fields are held as [json.RawMessage] so that only the one being asked for is
// decoded. The others are never turned into Go strings, which keeps a secret with
// nine unused fields from putting nine values on the heap for the sake of reading
// the tenth.
type kvSecret struct {
	Data struct {
		Data     map[string]json.RawMessage `json:"data"`
		Metadata struct {
			Version   int64 `json:"version"`
			Destroyed bool  `json:"destroyed"`
		} `json:"metadata"`
	} `json:"data"`
}

// fieldValue extracts one field's value from a KV v2 read.
//
// When no field was named the secret must hold exactly one. Guessing among several
// would mean a workflow's credential could change because somebody added an
// unrelated key, and the guess would be silent. Neither this error nor the missing
// field error lists the keys the secret does hold: a resolution error is recorded
// in workflow history, and what a tenant's secret is composed of is not something
// to write there. The count is enough to tell the two mistakes apart.
func (p *Provider) fieldValue(body []byte, apiPath, field string) (string, error) {
	var secret kvSecret
	if err := decodeJSON(body, &secret); err != nil {
		return "", fmt.Errorf("%s answered %q with %w", p.addr, apiPath, err)
	}

	fields := secret.Data.Data

	switch {
	case fields == nil:
		// A read of a deleted version is a 404, but a version deleted while it was
		// the current one comes back as a 200 with a null value.
		if secret.Data.Metadata.Destroyed {
			return "", fmt.Errorf(
				"%w: version %d of %q was destroyed",
				secrets.ErrNotFound, secret.Data.Metadata.Version, apiPath,
			)
		}

		return "", fmt.Errorf(
			"%w: the current version of %q was deleted",
			secrets.ErrNotFound, apiPath,
		)

	case len(fields) == 0:
		return "", fmt.Errorf("%w: %q holds no fields", secrets.ErrEmpty, apiPath)
	}

	if field == "" {
		if len(fields) != 1 {
			return "", fmt.Errorf(
				"%w: %q holds %d fields, so the reference must name one, as in %q",
				secrets.ErrInvalidRef, apiPath, len(fields),
				"apps/api"+fieldSeparator+"token",
			)
		}

		for only := range fields {
			field = only
		}
	}

	raw, ok := fields[field]
	if !ok {
		return "", fmt.Errorf(
			"%w: %q has no field %q (it holds %d)",
			secrets.ErrNotFound, apiPath, field, len(fields),
		)
	}

	value, err := scalar(raw, field)
	if err != nil {
		return "", err
	}

	if value == "" {
		return "", fmt.Errorf("%w: field %q of %q is empty", secrets.ErrEmpty, field, apiPath)
	}

	return value, nil
}

// scalar decodes one JSON value as the text of a credential.
//
// A KV v2 field can hold any JSON value, and a secret written by "vault kv put
// port=8200 enabled=true" holds numbers and booleans that are perfectly reasonable
// to resolve. An object or an array is not: there is no single value to return, and
// the error says so without printing what was found, because what was found is the
// secret.
func scalar(raw json.RawMessage, field string) (string, error) {
	var value any
	if err := decodeJSON(raw, &value); err != nil {
		return "", fmt.Errorf("field %q holds %w", field, err)
	}

	switch typed := value.(type) {
	case string:
		return typed, nil
	case json.Number:
		return typed.String(), nil
	case bool:
		return strconv.FormatBool(typed), nil
	case nil:
		return "", fmt.Errorf("%w: field %q is null", secrets.ErrEmpty, field)
	case map[string]any:
		return "", fmt.Errorf("field %q holds a JSON object rather than a value", field)
	case []any:
		return "", fmt.Errorf("field %q holds a JSON array rather than a value", field)
	default:
		return "", fmt.Errorf("field %q holds a %T rather than a value", field, typed)
	}
}
