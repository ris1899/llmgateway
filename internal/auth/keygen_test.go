package auth

import (
	"strings"
	"testing"
)

func TestGenerateProducesPrefixedUniqueKeysMatchingTheirHash(t *testing.T) {
	t.Parallel()
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		plain, hash := Generate()
		if !strings.HasPrefix(plain, Prefix) {
			t.Fatalf("Generate() plain = %q, missing prefix %q", plain, Prefix)
		}
		// 32 random bytes base64url-encoded is 43 chars (no padding); the
		// total length check catches an accidental entropy reduction.
		if got, want := len(plain)-len(Prefix), 43; got != want {
			t.Fatalf("Generate() plaintext body length = %d, want %d (32 random bytes)", got, want)
		}
		if hash != Hash(plain) {
			t.Fatalf("Generate() hash does not match Hash(plain)")
		}
		if seen[plain] {
			t.Fatalf("Generate() produced a duplicate key: %q", plain)
		}
		seen[plain] = true
	}
}

func TestNewIDIsUniqueAndPrefixed(t *testing.T) {
	t.Parallel()
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := NewID()
		if !strings.HasPrefix(id, "key_") {
			t.Fatalf("NewID() = %q, missing key_ prefix", id)
		}
		if seen[id] {
			t.Fatalf("NewID() produced a duplicate: %q", id)
		}
		seen[id] = true
	}
}
