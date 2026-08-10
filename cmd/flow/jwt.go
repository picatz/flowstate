package main

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"time"

	"github.com/picatz/jose/pkg/header"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/picatz/jose/pkg/jwt"
	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// maxSignTTL caps how long a token `flow jwt sign` mints stays valid, mirroring
// [auth.MaxAssertionLifetime]: a debugging tool that can mint a token good for a
// week is a standing credential-issuance path with none of an [auth.Issuer]'s
// discovery, rotation, or revocation. It is redeclared rather than imported for
// the same reason [minGeneratedRSABits] is: this command's own floor, not a
// number borrowed from a package whose job is to enforce it on someone else.
const maxSignTTL = time.Hour

// defaultSignTTL mirrors [auth.DefaultAssertionLifetime].
const defaultSignTTL = 5 * time.Minute

// reservedJWTClaims are the claims `flow jwt sign` sets itself from dedicated
// flags. A --claim of the same name would silently fight the flag that also
// sets it, so it is refused rather than left to whichever one the flag parser
// applies last.
var reservedJWTClaims = map[jwt.ClaimName]bool{
	jwt.Issuer:         true,
	jwt.Subject:        true,
	jwt.Audience:       true,
	jwt.ExpirationTime: true,
	jwt.NotBefore:      true,
	jwt.IssuedAt:       true,
	jwt.JWTID:          true,
}

func newJWTCommand() *cobra.Command {
	jwtCmd := &cobra.Command{
		Use:   "jwt",
		Short: "Sign and inspect JSON Web Tokens for admin debugging",
		// Written for a terminal, in the vocabulary the rest of the CLI teaches.
		// The doc comment above `maxSignTTL` says the same thing in godoc's, and
		// the two are adjacent for a reason that has already cost once: a `Long`
		// string in godoc's dialect reaches a reader as literal brackets around a
		// Go identifier from a package they have no reason to have heard of.
		Long: "Sign a JWT with a key from `flow keys generate`, or inspect one a " +
			"workload, worker, or relying party produced. For debugging identity, " +
			"not for minting production workload assertions: a real issuer, named " +
			"in the trust policy `flow server` is started with (`--auth-policy`), " +
			"publishes its keys for discovery and can rotate and revoke them, and " +
			"this command deliberately does none of that.",
	}

	jwtCmd.AddCommand(newJWTSignCommand())
	jwtCmd.AddCommand(newJWTInspectCommand())

	return jwtCmd
}

func newJWTSignCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sign",
		Short: "Sign a JWT with a private key",
		Long: fmt.Sprintf("Sign a JWT with a private key from `flow keys generate`. "+
			"The token's lifetime is capped at %s, because this command mints "+
			"debugging tokens directly rather than through an issuer with rotation "+
			"and revocation.", maxSignTTL),
		Args: cobra.NoArgs,
		RunE: runJWTSign,
		Example: `# Sign a short-lived token for a debugging session:
flow jwt sign --key identity/2026-08.pem --issuer https://flowstate.internal \
  --subject worker-1 --audience flowstate-worker

# Carry an extra claim:
flow jwt sign --key identity/2026-08.pem --issuer https://flowstate.internal \
  --subject worker-1 --audience flowstate-worker --claim namespace=team-a`,
	}

	cmd.Flags().String("key", "", "path to a PKCS#8 private key PEM (required)")
	cmd.Flags().String("id", "", "key id in the JWT \"kid\" header "+
		"(default: --key's file name, without its extension)")
	cmd.Flags().String("issuer", "", "the \"iss\" claim")
	cmd.Flags().String("subject", "", "the \"sub\" claim")
	cmd.Flags().String("audience", "", "the \"aud\" claim")
	cmd.Flags().Duration("ttl", defaultSignTTL, "how long the token is valid for, capped at "+maxSignTTL.String())
	cmd.Flags().StringArray("claim", nil, "an additional name=value claim (repeatable)")
	_ = cmd.MarkFlagRequired("key")
	_ = cmd.MarkFlagRequired("issuer")
	_ = cmd.MarkFlagRequired("subject")
	_ = cmd.MarkFlagRequired("audience")

	return cmd
}

func newJWTInspectCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "inspect <token>",
		Short: "Print a JWT's header and claims",
		Long: "Print a JWT's header and claims without trusting them: the " +
			"signature is checked only when --key is given, and an unparseable " +
			"token is reported without echoing it back, since a garbled string " +
			"handed to this command may be a credential rather than a typo.",
		Args: cobra.ExactArgs(1),
		RunE: runJWTInspect,
		Example: `# What does this token claim, unverified:
flow jwt inspect eyJhbGciOi...

# Also check it was signed by this key:
flow jwt inspect --key identity/2026-08.pem eyJhbGciOi...`,
	}

	cmd.Flags().String("key", "", "path to a PKCS#8 private key PEM; verifies the token was signed with it")

	return cmd
}

// signJWT mirrors the unexported signerFor in pkg/flowstate/v1/auth/issuer.go:
// the same header shape, the same type switch, because auth.SigningKey has no
// exported way to sign directly.
func signJWT(id string, algorithm jwa.Algorithm, private crypto.PrivateKey, claims jwt.ClaimsSet) (string, error) {
	params := header.Parameters{
		header.Type:      jwt.Type,
		header.Algorithm: algorithm,
		header.KeyID:     id,
	}

	var (
		token *jwt.Token
		err   error
	)
	switch typed := private.(type) {
	case *rsa.PrivateKey:
		token, err = jwt.New(params, claims, typed)
	case *ecdsa.PrivateKey:
		token, err = jwt.New(params, claims, typed)
	case ed25519.PrivateKey:
		token, err = jwt.New(params, claims, typed)
	default:
		return "", fmt.Errorf("cannot sign with %T", private)
	}
	if err != nil {
		return "", fmt.Errorf("signing token with key %q: %w", id, err)
	}

	return token.String(), nil
}

func parseClaimFlags(raw []string) (jwt.ClaimsSet, error) {
	claims := jwt.ClaimsSet{}
	for _, entry := range raw {
		name, value, ok := splitClaim(entry)
		if !ok {
			return nil, fmt.Errorf("--claim %q must be name=value", entry)
		}
		if reservedJWTClaims[name] {
			return nil, fmt.Errorf("--claim %q: %q is set by its own flag, not --claim", entry, name)
		}
		claims[name] = value
	}
	return claims, nil
}

func splitClaim(entry string) (name, value string, ok bool) {
	for i := 0; i < len(entry); i++ {
		if entry[i] == '=' {
			return entry[:i], entry[i+1:], i > 0
		}
	}
	return "", "", false
}

func runJWTSign(cmd *cobra.Command, _ []string) error {
	surface := newSurface(cmd)

	keyPath, _ := cmd.Flags().GetString("key")
	id, _ := cmd.Flags().GetString("id")
	issuer, _ := cmd.Flags().GetString("issuer")
	subject, _ := cmd.Flags().GetString("subject")
	audience, _ := cmd.Flags().GetString("audience")
	ttl, _ := cmd.Flags().GetDuration("ttl")
	rawClaims, _ := cmd.Flags().GetStringArray("claim")

	if ttl <= 0 {
		return fmt.Errorf("--ttl must be positive, got %s", ttl)
	}
	if ttl > maxSignTTL {
		// The remedy names something a person can go and do, which a Go type is
		// not: the issuer they would reach for is the one their trust policy
		// configures, and that is what `flow server --auth-policy` points at.
		return fmt.Errorf("--ttl %s exceeds the %s cap this command enforces; "+
			"a token that outlives it comes from a real issuer, configured in the "+
			"trust policy passed to flow server --auth-policy", ttl, maxSignTTL)
	}

	if id == "" {
		id = keyIDFromPath(keyPath)
	}

	private, err := readPrivateKeyPEM(keyPath)
	if err != nil {
		return err
	}

	key, err := auth.NewSigningKey(id, private)
	if err != nil {
		return fmt.Errorf("%s: %w", keyPath, err)
	}

	claims, err := parseClaimFlags(rawClaims)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	claims[jwt.Issuer] = issuer
	claims[jwt.Subject] = subject
	claims[jwt.Audience] = audience
	claims[jwt.IssuedAt] = now.Unix()
	claims[jwt.ExpirationTime] = now.Add(ttl).Unix()

	token, err := signJWT(id, key.Algorithm(), private, claims)
	if err != nil {
		return err
	}

	fmt.Fprintf(surface.Err, "signed with %s key %q, expires %s\n",
		key.Algorithm(), id, now.Add(ttl).Format(time.RFC3339))
	fmt.Fprintf(surface.Out, "%s\n", token)

	return nil
}

// inspectResult is what `flow jwt inspect` prints. A struct with named fields
// rather than the raw *jwt.Token, so the JSON shape is this command's contract
// and not incidentally whatever the library's field names happen to be.
type inspectResult struct {
	Header  header.Parameters `json:"header"`
	Claims  jwt.ClaimsSet     `json:"claims"`
	Expired *bool             `json:"expired,omitempty"`
	Valid   *bool             `json:"signatureValid,omitempty"`
}

func runJWTInspect(cmd *cobra.Command, args []string) error {
	surface := newSurface(cmd)

	// The raw token is deliberately never interpolated into an error message
	// below: it may be a live credential, and a garbled one is exactly the
	// case where a person is most likely to paste it into a bug report or a
	// terminal someone else can see over their shoulder.
	token, err := jwt.ParseString(args[0])
	if err != nil {
		return fmt.Errorf("that is not a well-formed JWT: %w", err)
	}

	result := inspectResult{
		Header: token.Header,
		Claims: token.Claims,
	}

	// Read straight from the claims rather than through [jwt.Token.Expired]:
	// that method type-asserts "exp" as int64, but a token round-tripped
	// through JSON — which every parsed token has been — decodes numbers as
	// float64, so it would silently fail to report expiry on exactly the
	// tokens this command exists to inspect.
	if isExpired, ok := tokenExpired(token.Claims, time.Now()); ok {
		result.Expired = &isExpired
	}

	keyPath, _ := cmd.Flags().GetString("key")
	if keyPath != "" {
		private, err := readPrivateKeyPEM(keyPath)
		if err != nil {
			return err
		}
		public, err := publicKeyOf(private)
		if err != nil {
			return err
		}

		alg, err := token.Header.Algorithm()
		if err != nil {
			return fmt.Errorf("token header: %w", err)
		}

		// Keyed by the token's own "kid" header, not the key file's name: a
		// token signed with `flow jwt sign --id custom` carries "custom" in
		// its header regardless of what the key file is called, and
		// VerifySignature looks the key up by that header value. Keying by
		// the file name instead would report a correctly-signed token as
		// invalid whenever the two disagree.
		kid := headerKeyID(token.Header)
		if kid == "" {
			kid = keyIDFromPath(keyPath)
		}
		verifyErr := token.VerifySignature([]jwa.Algorithm{alg}, map[string]any{kid: public})
		valid := verifyErr == nil
		result.Valid = &valid
	}

	return writeInspectResult(surface, result)
}

func writeInspectResult(surface *ui.UI, result inspectResult) error {
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("rendering token: %w", err)
	}

	_, err = fmt.Fprintf(surface.Out, "%s\n", encoded)
	return err
}

// headerKeyID reads the "kid" header parameter, the same way auth.headerString
// does for verification elsewhere in this codebase — a defensive read that
// returns "" for a missing or non-string value rather than an error, since a
// token with no "kid" is not malformed, just unidentified.
func headerKeyID(params header.Parameters) string {
	value, err := params.Get(header.KeyID)
	if err != nil {
		return ""
	}
	text, _ := value.(string)
	return text
}

// tokenExpired reports whether claims carries an "exp" claim and, if so,
// whether it is before now. ok is false when there is no "exp" claim to
// judge, or when its value is not a number this can interpret.
func tokenExpired(claims jwt.ClaimsSet, now time.Time) (expired bool, ok bool) {
	raw, present := claims[jwt.ExpirationTime]
	if !present {
		return false, false
	}

	var exp int64
	switch typed := raw.(type) {
	case int64:
		exp = typed
	case float64:
		exp = int64(typed)
	default:
		return false, false
	}

	return time.Unix(exp, 0).Before(now), true
}
