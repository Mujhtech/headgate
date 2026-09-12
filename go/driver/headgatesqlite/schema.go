package headgatesqlite

import (
	_ "embed"
	"encoding/json"
	"fmt"

	headgate "github.com/mujhtech/headgate/go"
)

//go:embed migrations/0001_init.sql
var schema string

func marshalTags(tags []string) (string, error) {
	encoded, err := json.Marshal(headgate.CanonicalTags(tags))
	if err != nil {
		return "", fmt.Errorf("encoding SQLite tags: %w", err)
	}
	return string(encoded), nil
}
