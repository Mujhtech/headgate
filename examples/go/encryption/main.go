package main

import (
	"bytes"
	"encoding/json"
	"fmt"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgatecrypto"
)

type secretReport struct {
	Account string `json:"account"`
}

func (secretReport) Kind() string { return "example:secret-report" }

func main() {
	keys, err := headgatecrypto.NewStaticKeyring("2026-08", map[string][32]byte{
		"2026-08": {7},
	})
	if err != nil {
		panic(err)
	}
	plaintext, err := json.Marshal(secretReport{Account: "customer-42"})
	if err != nil {
		panic(err)
	}
	envelope, err := headgatecrypto.EncryptEnvelope(keys, headgate.Envelope{
		ID:            "secret-report-1",
		Kind:          secretReport{}.Kind(),
		SchemaVersion: 1,
		Payload:       plaintext,
		Queue:         "reports",
	})
	if err != nil {
		panic(err)
	}
	decoded, err := headgatecrypto.DecryptEnvelope(keys, envelope)
	if err != nil {
		panic(err)
	}
	if bytes.Equal(envelope.Payload, plaintext) || !bytes.Equal(decoded, plaintext) {
		panic("encrypted payload did not round trip")
	}
	fmt.Println("payload encrypted before enqueue and authenticated on decode")
}
