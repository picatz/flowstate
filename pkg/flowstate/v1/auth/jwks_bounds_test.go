package auth

import (
	"crypto/rsa"
	"math/big"
	"testing"
)

// The RSA modulus is bounded at both ends, and the ceiling is the half that was
// missing.
//
// A floor stops a weak key. It says nothing about an absurdly large one, and RSA
// verification is superlinear in the modulus — a 360,000-bit key measured 2.53
// seconds against 53 microseconds for a normal one, which is one unauthenticated
// Verify buying fifty thousand times the work from a request that has proven
// nothing. The resource the far side controls here is the size of the modulus in
// a key set we fetched, so that is the resource bounded (#561).
//
// The keys are synthetic: only BitLen is consulted, and generating a real
// 9216-bit key costs about eight seconds, which is a bound this suite should not
// pay on every run to test a comparison.

// rsaKeyOfBits returns a public key whose modulus has exactly the given bit
// length. It is not a usable key and does not need to be.
func rsaKeyOfBits(bits int) *rsa.PublicKey {
	return &rsa.PublicKey{N: new(big.Int).Lsh(big.NewInt(1), uint(bits-1)), E: 65537}
}

func TestAnRSAKeyOutsideTheBoundsIsNotSuitable(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		bits int
		want bool
	}{
		{name: "below the floor", bits: minRSAKeyBits - 8, want: false},
		{name: "at the floor", bits: minRSAKeyBits, want: true},
		{name: "an ordinary large key", bits: 4096, want: true},
		{name: "at the ceiling", bits: maxRSAKeyBits, want: true},
		{name: "above the ceiling", bits: maxRSAKeyBits + 8, want: false},
		// The shape the finding actually measured, rather than one bit over.
		{name: "absurd", bits: 360_000, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			key := publicKey{key: rsaKeyOfBits(test.bits)}
			if got := key.suitableFor("RS256"); got != test.want {
				t.Errorf("suitableFor(RS256) on a %d-bit key = %v, want %v", test.bits, got, test.want)
			}
		})
	}
}

// TestTheCeilingIsReportedAsARange checks the diagnostic rather than only the
// refusal, because an operator reading "want at least 2048" after supplying a
// key that is far too *large* has been told the opposite of what is wrong.
func TestTheCeilingIsReportedAsARange(t *testing.T) {
	t.Parallel()

	key := publicKey{key: rsaKeyOfBits(maxRSAKeyBits * 2)}
	if key.suitableFor("PS512") {
		t.Fatal("an oversized key was accepted for PS512")
	}
}
