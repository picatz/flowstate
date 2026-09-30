package envelope

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/fips140"
	"crypto/rand"
	"fmt"
	"slices"

	"golang.org/x/crypto/chacha20poly1305"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Suite is the AEAD a payload is sealed with, as [v1.PayloadSuite] numbers
// it. Every suite derives its content key and its key commitment the same way,
// with HKDF-SHA256 from the data key; a suite differs only in the AEAD, and a
// different derivation would be a different suite.
//
// Agility is in the registry rather than in the format: the header names its
// suite, Decode accepts the namespace's allow-list, and adding a suite is one
// row here and one enum value in the schema. What a suite may never do is
// reuse another's number.
type Suite = v1.PayloadSuite

// Suites this build implements.
const (
	SuiteAES256GCM         = v1.PayloadSuite_PAYLOAD_SUITE_AES256_GCM
	SuiteXChaCha20Poly1305 = v1.PayloadSuite_PAYLOAD_SUITE_XCHACHA20_POLY1305
)

// DefaultSuite is what a namespace that names none seals with.
const DefaultSuite = SuiteAES256GCM

// suiteSpec is one row of the registry.
type suiteSpec struct {
	// overhead is what the AEAD adds to a plaintext: nonce and tag.
	overhead int

	// fipsApproved is whether the AEAD is approved in FIPS 140-3 mode.
	fipsApproved bool

	// seal and open are the AEAD under a single-use content key. seal
	// returns nonce ‖ ciphertext ‖ tag, with the nonce drawn at random.
	seal func(key, plaintext, aad []byte) ([]byte, error)
	open func(key, sealed, aad []byte) ([]byte, error)
}

var suites = map[Suite]suiteSpec{
	// AES-256-GCM through NewGCMWithRandomNonce, the FIPS 140-3 module's own
	// random-nonce construction, which prepends the nonce itself.
	SuiteAES256GCM: {
		overhead:     12 + 16,
		fipsApproved: true,
		seal: func(key, plaintext, aad []byte) ([]byte, error) {
			gcm, err := gcmRandomNonce(key)
			if err != nil {
				return nil, err
			}
			return gcm.Seal(nil, nil, plaintext, aad), nil
		},
		open: func(key, sealed, aad []byte) ([]byte, error) {
			gcm, err := gcmRandomNonce(key)
			if err != nil {
				return nil, err
			}
			return gcm.Open(nil, nil, sealed, aad)
		},
	},

	// XChaCha20-Poly1305: a 192-bit random nonce, prepended here.
	SuiteXChaCha20Poly1305: {
		overhead: chacha20poly1305.NonceSizeX + chacha20poly1305.Overhead,
		seal: func(key, plaintext, aad []byte) ([]byte, error) {
			x, err := chacha20poly1305.NewX(key)
			if err != nil {
				return nil, err
			}
			out := make([]byte, chacha20poly1305.NonceSizeX, chacha20poly1305.NonceSizeX+len(plaintext)+x.Overhead())
			_, _ = rand.Read(out)
			return x.Seal(out, out, plaintext, aad), nil
		},
		open: func(key, sealed, aad []byte) ([]byte, error) {
			x, err := chacha20poly1305.NewX(key)
			if err != nil {
				return nil, err
			}
			if len(sealed) < chacha20poly1305.NonceSizeX+x.Overhead() {
				return nil, ErrMalformed
			}
			return x.Open(nil, sealed[:chacha20poly1305.NonceSizeX], sealed[chacha20poly1305.NonceSizeX:], aad)
		},
	},
}

func gcmRandomNonce(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCMWithRandomNonce(block)
}

// maxOverhead is the largest overhead of any implemented suite, for bounds
// that must hold whichever suite a payload names.
var maxOverhead = func() int {
	n := 0
	for _, s := range suites {
		n = max(n, s.overhead)
	}
	return n
}()

// usableSuites is every implemented suite this process may use: all of them,
// or only the approved ones in FIPS 140-3 mode.
func usableSuites() []Suite {
	var out []Suite
	for id, s := range suites {
		if s.fipsApproved || !fips140.Enabled() {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out
}

// resolveSuites is a namespace's encrypt suite and decrypt allow-list with
// defaults applied, refused when either names a suite this process may not
// use.
//
// writes is whether the codec has a current key. A decode-only codec never
// encrypts, so an encrypt suite left at its default is not checked against
// anything: a recovery namespace narrowed to the one suite its history uses
// is not refused over a suite it will never write with.
func resolveSuites(encrypt Suite, decrypt []Suite, writes bool) (Suite, []Suite, error) {
	usable := usableSuites()
	checkEncrypt := writes || encrypt != v1.PayloadSuite_PAYLOAD_SUITE_UNSPECIFIED
	if encrypt == v1.PayloadSuite_PAYLOAD_SUITE_UNSPECIFIED {
		encrypt = DefaultSuite
	}
	if checkEncrypt && !slices.Contains(usable, encrypt) {
		return 0, nil, unusableSuite(encrypt)
	}
	if len(decrypt) == 0 {
		return encrypt, usable, nil
	}
	for _, s := range decrypt {
		if !slices.Contains(usable, s) {
			return 0, nil, unusableSuite(s)
		}
	}
	if writes && !slices.Contains(decrypt, encrypt) {
		return 0, nil, fmt.Errorf("envelope: the encrypt suite %s is not among the decrypt suites, "+
			"so this namespace could not read what it writes", encrypt)
	}
	return encrypt, slices.Compact(slices.Sorted(slices.Values(decrypt))), nil
}

func unusableSuite(s Suite) error {
	if spec, ok := suites[s]; ok && !spec.fipsApproved {
		return fmt.Errorf("envelope: suite %s is not FIPS 140-3 approved, and this process runs in FIPS mode", s)
	}
	return fmt.Errorf("envelope: suite %s is not one this build implements", s)
}
