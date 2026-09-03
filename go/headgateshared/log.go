package headgateshared

import (
	"encoding/json"
	"math"
	"sort"
	"strings"
	"unicode/utf8"
)

// LogPrefix identifies structured entries within the existing []string attempt log wire format.
const LogPrefix = "\x1eheadgate-log-v1:"

// MaxLogBytes bounds the entire wire entry, including its prefix and JSON encoding.
const MaxLogBytes = 2048

// MaxLogFields bounds the number of scalar fields retained in a structured entry.
const MaxLogFields = 32

// LogCapMessage is appended once when an attempt exceeds its 100-entry budget.
const LogCapMessage = "... log cap reached (100 lines/attempt)"

// LogEntry contains diagnostic worker-clock time, never a timestamp used for admission.
// Fields contain JSON scalars only. Legacy strings decode as info without a timestamp.
type LogEntry struct {
	Level     string         `json:"level"`
	AtMs      int64          `json:"at_ms,omitempty"`
	Message   string         `json:"message"`
	Fields    map[string]any `json:"fields,omitempty"`
	Truncated bool           `json:"truncated,omitempty"`
}

// LogText bounds UTF-8 text and detaches it from potentially large caller buffers.
func LogText(s string, limit int) string {
	if limit < 0 {
		limit = 0
	}
	if len(s) > limit {
		s = s[:limit]
	}
	s = strings.ToValidUTF8(s, "�")
	if len(s) > limit {
		s = s[:limit]
		for len(s) > 0 && !utf8.ValidString(s) {
			s = s[:len(s)-1]
		}
	}
	return strings.Clone(s)
}

// ValidLogLevel reports whether level belongs to the version-one wire vocabulary.
func ValidLogLevel(level string) bool {
	return level == "debug" || level == "info" || level == "warn" || level == "error"
}

// LogScalar snapshots supported values without retaining arbitrary application objects.
func LogScalar(value any) any {
	switch v := value.(type) {
	case nil, bool, int64, uint64, int, uint:
		return v
	case float64:
		if !math.IsNaN(v) && !math.IsInf(v, 0) {
			return v
		}
		return "[non-finite number]"
	case string:
		return LogText(v, 1024)
	default:
		return "[unsupported log value]"
	}
}

// EncodeLog bounds the complete serialized entry, including escaping and field overhead.
// Truncation is explicit; fields are removed in reverse key order before shortening text.
func EncodeLog(entry LogEntry) string {
	if !ValidLogLevel(entry.Level) {
		entry.Level = "info"
	}
	message := LogText(entry.Message, MaxLogBytes)
	entry.Truncated = entry.Truncated || message != entry.Message
	entry.Message = message
	fields := make(map[string]any)
	keys := make([]string, 0, len(entry.Fields))
	for key := range entry.Fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for i, key := range keys {
		if i >= MaxLogFields {
			entry.Truncated = true
			break
		}
		value := LogScalar(entry.Fields[key])
		fields[LogText(key, 128)] = value
		if len(key) > 128 {
			entry.Truncated = true
		}
		if s, ok := entry.Fields[key].(string); ok && len(s) > 1024 {
			entry.Truncated = true
		}
	}
	entry.Fields = fields
	keys = keys[:0]
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for {
		encoded, err := json.Marshal(entry)
		if err == nil && len(LogPrefix)+len(encoded) <= MaxLogBytes {
			return LogPrefix + string(encoded)
		}
		entry.Truncated = true
		if len(keys) > 0 {
			delete(fields, keys[len(keys)-1])
			keys = keys[:len(keys)-1]
		} else {
			entry.Message = LogText(entry.Message, len(entry.Message)/2)
		}
	}
}

// DecodeLog leaves unknown versions and malformed entries visible as literal info logs.
func DecodeLog(line string) LogEntry {
	if strings.HasPrefix(line, LogPrefix) && len(line) <= MaxLogBytes {
		var entry LogEntry
		body := []byte(strings.TrimPrefix(line, LogPrefix))
		var shape map[string]json.RawMessage
		if json.Unmarshal(body, &shape) == nil && validLogShape(shape) &&
			json.Unmarshal(body, &entry) == nil && ValidLogLevel(entry.Level) && validLogFields(entry.Fields) {
			return entry
		}
	}
	return LogEntry{Level: "info", Message: line}
}

func validLogShape(shape map[string]json.RawMessage) bool {
	if shape["message"] == nil || string(shape["message"]) == "null" {
		return false
	}
	for _, key := range []string{"at_ms", "fields", "truncated"} {
		if string(shape[key]) == "null" {
			return false
		}
	}
	return true
}

func validLogFields(fields map[string]any) bool {
	for _, value := range fields {
		switch value.(type) {
		case nil, string, float64, bool:
		default:
			return false
		}
	}
	return true
}
