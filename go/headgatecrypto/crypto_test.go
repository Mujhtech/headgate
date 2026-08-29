package headgatecrypto

import (
	"fmt"
	"testing"

	headgate "github.com/mujhtech/headgate/go"
)

func keyring(t *testing.T) *StaticKeyring {
	t.Helper()
	ring, err := NewStaticKeyring("k1", map[string][32]byte{"k1": {7}})
	if err != nil {
		t.Fatal(err)
	}
	return ring
}

func envelope() headgate.Envelope {
	return headgate.Envelope{ID: "job-1", Kind: "secret:task", SchemaVersion: 1, Payload: []byte("top secret"), Queue: "default"}
}

func TestRoundTripBindsIdentityAndPreservesPlaintextFingerprint(t *testing.T) {
	a, err := EncryptEnvelope(keyring(t), envelope())
	if err != nil {
		t.Fatal(err)
	}
	b, err := EncryptEnvelope(keyring(t), envelope())
	if err != nil {
		t.Fatal(err)
	}
	if string(a.Payload) == string(b.Payload) {
		t.Fatal("fresh nonce must change ciphertext")
	}
	if a.Fingerprint != b.Fingerprint {
		t.Fatal("fingerprint must be plaintext-derived, not nonce-derived")
	}
	plaintext, err := DecryptEnvelope(keyring(t), a)
	if err != nil || string(plaintext) != "top secret" {
		t.Fatalf("round trip = %q, %v", plaintext, err)
	}
	a.ID = "job-2"
	if _, err := DecryptEnvelope(keyring(t), a); err == nil {
		t.Fatal("job id must be authenticated AAD")
	}
}

func TestTamperingAndMissingKeysFail(t *testing.T) {
	env, err := EncryptEnvelope(keyring(t), envelope())
	if err != nil {
		t.Fatal(err)
	}
	env.Payload[len(env.Payload)-1] ^= 1
	if _, err := DecryptEnvelope(keyring(t), env); err == nil {
		t.Fatal("tampered ciphertext authenticated")
	}
	other, _ := NewStaticKeyring("other", map[string][32]byte{"other": {9}})
	env, _ = EncryptEnvelope(keyring(t), envelope())
	if _, err := DecryptEnvelope(other, env); err == nil {
		t.Fatal("missing historical key decrypted")
	}
}

func TestWireVector(t *testing.T) {
	key := [32]byte{}
	for i := range key {
		key[i] = 7
	}
	nonce := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
	got, err := seal("k1", key, nonce, aad(envelope()), []byte("top secret"))
	if err != nil {
		t.Fatal(err)
	}
	const want = "484745430100026b31000102030405060708090a0b6cee99506e6cba3b12c6527e0b794110389ff91129360bd1446d"
	if hex := fmt.Sprintf("%x", got); hex != want {
		t.Fatalf("wire vector = %s", hex)
	}
}
