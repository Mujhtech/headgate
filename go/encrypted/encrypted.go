// Package encrypted provides client-side AES-256-GCM payload encryption. Stores see
// ciphertext only; core and every driver remain crypto-free.
package encrypted

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	headgate "github.com/mujhtech/headgate/go"
)

var magic = []byte{'H', 'G', 'E', 'C', 1}

const nonceLen = 12

type KeyProvider interface {
	ActiveKey() (id string, key [32]byte, err error)
	Key(id string) ([32]byte, error)
}

type StaticKeyring struct {
	active string
	keys   map[string][32]byte
}

func NewStaticKeyring(active string, keys map[string][32]byte) (*StaticKeyring, error) {
	if _, ok := keys[active]; active == "" || !ok {
		return nil, errors.New("headgate encrypted: active encryption key is missing")
	}
	copyKeys := make(map[string][32]byte, len(keys))
	for id, key := range keys {
		copyKeys[id] = key
	}
	return &StaticKeyring{active: active, keys: copyKeys}, nil
}

func (r *StaticKeyring) ActiveKey() (string, [32]byte, error) {
	return r.active, r.keys[r.active], nil
}

func (r *StaticKeyring) Key(id string) ([32]byte, error) {
	key, ok := r.keys[id]
	if !ok {
		return key, fmt.Errorf("headgate encrypted: encryption key %q is unavailable", id)
	}
	return key, nil
}

// EncryptEnvelope preserves a plaintext-derived fingerprint before adding a random
// nonce, so identical poison jobs still share quarantine identity.
func EncryptEnvelope(keys KeyProvider, env headgate.Envelope) (headgate.Envelope, error) {
	if env.Fingerprint == "" {
		env.Fingerprint = headgate.Fingerprint(env.Kind, env.Payload)
	}
	id, key, err := keys.ActiveKey()
	if err != nil {
		return env, err
	}
	if len(id) == 0 || len(id) > 65535 {
		return env, errors.New("headgate encrypted: encryption key id must be 1..65535 bytes")
	}
	nonce := make([]byte, nonceLen)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return env, err
	}
	env.Payload, err = seal(id, key, nonce, aad(env), env.Payload)
	return env, err
}

func DecryptEnvelope(keys KeyProvider, env headgate.Envelope) ([]byte, error) {
	if len(env.Payload) < len(magic)+2+nonceLen+16 || string(env.Payload[:len(magic)]) != string(magic) {
		return nil, errors.New("headgate encrypted: payload is not a headgate encrypted envelope")
	}
	idLen := int(binary.BigEndian.Uint16(env.Payload[5:7]))
	idStart := 7
	nonceStart := idStart + idLen
	if nonceStart+nonceLen+16 > len(env.Payload) {
		return nil, errors.New("headgate encrypted: payload header is truncated")
	}
	id := string(env.Payload[idStart:nonceStart])
	key, err := keys.Key(id)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	plaintext, err := gcm.Open(nil, env.Payload[nonceStart:nonceStart+nonceLen], env.Payload[nonceStart+nonceLen:], aad(env))
	if err != nil {
		return nil, errors.New("headgate encrypted: payload authentication failed")
	}
	return plaintext, nil
}

func seal(id string, key [32]byte, nonce, additional, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	ciphertext := gcm.Seal(nil, nonce, plaintext, additional)
	out := make([]byte, 0, len(magic)+2+len(id)+len(nonce)+len(ciphertext))
	out = append(out, magic...)
	var length [2]byte
	binary.BigEndian.PutUint16(length[:], uint16(len(id)))
	out = append(out, length[:]...)
	out = append(out, id...)
	out = append(out, nonce...)
	out = append(out, ciphertext...)
	return out, nil
}

func aad(env headgate.Envelope) []byte {
	out := make([]byte, 0, len(env.ID)+len(env.Kind)+12)
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(env.ID)))
	out = append(out, length[:]...)
	out = append(out, env.ID...)
	binary.BigEndian.PutUint32(length[:], uint32(len(env.Kind)))
	out = append(out, length[:]...)
	out = append(out, env.Kind...)
	binary.BigEndian.PutUint32(length[:], env.SchemaVersion)
	out = append(out, length[:]...)
	return out
}

func RegisterEncrypted[T headgate.Args](
	registry *headgate.Registry,
	keys KeyProvider,
	handler func(context.Context, *headgate.Job[T]) error,
) error {
	return headgate.RegisterRaw[T](registry, func(ctx context.Context, claim headgate.Claim) error {
		plaintext, err := DecryptEnvelope(keys, claim.Envelope)
		if err != nil {
			return &headgate.UndecodableError{Cause: err}
		}
		env := claim.Envelope
		env.Payload = plaintext
		args, err := headgate.DecodeArgs[T](env)
		if err != nil {
			return &headgate.UndecodableError{Cause: err}
		}
		job := &headgate.Job[T]{
			ID: env.ID, Args: args, Queue: env.Queue, Attempt: env.Attempt,
			CrashAttempt: env.CrashAttempt, MaxAttempts: env.MaxAttempts, Fence: claim.Fence,
			PartitionKey: env.PartitionKey, RateClass: env.RateClass, Weight: headgate.EffectiveWeight(env.Weight),
		}
		if env.DeadlineMs > 0 {
			job.Deadline = time.UnixMilli(env.DeadlineMs)
		}
		return handler(ctx, job)
	})
}
