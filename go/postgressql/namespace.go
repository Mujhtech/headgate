// Package postgressql owns safe, explicit Postgres object qualification shared by the
// driver and migrator. It has no driver dependency and never changes search_path.
package postgressql

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"unicode/utf8"
)

var objects = map[string]struct{}{
	"headgate_state": {}, "headgate_job": {}, "headgate_rate_bucket": {},
	"headgate_quarantine": {}, "headgate_partition_deficit": {},
	"headgate_active_partition": {}, "headgate_inflight": {},
	"headgate_concurrency_limit": {}, "headgate_queue_counter": {},
	"headgate_partition_counter": {}, "headgate_queue_state": {},
	"headgate_duty": {}, "headgate_schedule": {}, "headgate_schedule_event": {}, "headgate_worker": {},
	"headgate_effect": {}, "headgate_operation": {},
	"headgate_enqueue_policy": {}, "headgate_enqueue_counter": {},
	"headgate_job_tag": {}, "headgate_queue_sample": {},
	"headgate_archive_policy": {}, "headgate_job_archive": {},
	"headgate_job_archive_before_2025": {},
	"headgate_job_archive_202501":      {},
	"headgate_job_archive_202502":      {},
	"headgate_job_archive_202503":      {},
	"headgate_job_archive_202504":      {},
	"headgate_job_archive_202505":      {},
	"headgate_job_archive_202506":      {},
	"headgate_job_archive_202507":      {},
	"headgate_job_archive_202508":      {},
	"headgate_job_archive_202509":      {},
	"headgate_job_archive_202510":      {},
	"headgate_job_archive_202511":      {},
	"headgate_job_archive_202512":      {},
	"headgate_job_archive_202601":      {},
	"headgate_job_archive_202602":      {},
	"headgate_job_archive_202603":      {},
	"headgate_job_archive_202604":      {},
	"headgate_job_archive_202605":      {},
	"headgate_job_archive_202606":      {},
	"headgate_job_archive_202607":      {},
	"headgate_job_archive_202608":      {},
	"headgate_job_archive_202609":      {},
	"headgate_job_archive_202610":      {},
	"headgate_job_archive_202611":      {},
	"headgate_job_archive_202612":      {},
	"headgate_job_archive_202701":      {},
	"headgate_job_archive_202702":      {},
	"headgate_job_archive_202703":      {},
	"headgate_job_archive_202704":      {},
	"headgate_job_archive_202705":      {},
	"headgate_job_archive_202706":      {},
	"headgate_job_archive_202707":      {},
	"headgate_job_archive_202708":      {},
	"headgate_job_archive_202709":      {},
	"headgate_job_archive_202710":      {},
	"headgate_job_archive_202711":      {},
	"headgate_job_archive_202712":      {},
	"headgate_job_archive_202801":      {},
	"headgate_job_archive_202802":      {},
	"headgate_job_archive_202803":      {},
	"headgate_job_archive_202804":      {},
	"headgate_job_archive_202805":      {},
	"headgate_job_archive_202806":      {},
	"headgate_job_archive_202807":      {},
	"headgate_job_archive_202808":      {},
	"headgate_job_archive_202809":      {},
	"headgate_job_archive_202810":      {},
	"headgate_job_archive_202811":      {},
	"headgate_job_archive_202812":      {},
	"headgate_job_archive_202901":      {},
	"headgate_job_archive_202902":      {},
	"headgate_job_archive_202903":      {},
	"headgate_job_archive_202904":      {},
	"headgate_job_archive_202905":      {},
	"headgate_job_archive_202906":      {},
	"headgate_job_archive_202907":      {},
	"headgate_job_archive_202908":      {},
	"headgate_job_archive_202909":      {},
	"headgate_job_archive_202910":      {},
	"headgate_job_archive_202911":      {},
	"headgate_job_archive_202912":      {},
	"headgate_job_archive_203001":      {},
	"headgate_job_archive_203002":      {},
	"headgate_job_archive_203003":      {},
	"headgate_job_archive_203004":      {},
	"headgate_job_archive_203005":      {},
	"headgate_job_archive_203006":      {},
	"headgate_job_archive_203007":      {},
	"headgate_job_archive_203008":      {},
	"headgate_job_archive_203009":      {},
	"headgate_job_archive_203010":      {},
	"headgate_job_archive_203011":      {},
	"headgate_job_archive_203012":      {},
	"headgate_job_archive_203101":      {},
	"headgate_job_archive_203102":      {},
	"headgate_job_archive_203103":      {},
	"headgate_job_archive_203104":      {},
	"headgate_job_archive_203105":      {},
	"headgate_job_archive_203106":      {},
	"headgate_job_archive_203107":      {},
	"headgate_job_archive_203108":      {},
	"headgate_job_archive_203109":      {},
	"headgate_job_archive_203110":      {},
	"headgate_job_archive_203111":      {},
	"headgate_job_archive_203112":      {},
	"headgate_job_archive_after_2031":  {},
	"headgate_track_enqueue_depth":     {}, "headgate_schema_migration": {},
}

var indexes = map[string]struct{}{
	"headgate_job_unique": {}, "headgate_job_unique_throttle": {},
	"headgate_job_sticky_available": {}, "headgate_job_avail_sticky": {},
	"headgate_job_archive_queue_time": {},
}

type Namespace struct {
	name          string
	quoted        string
	wakeupChannel string
}

func NewNamespace(name string) (Namespace, error) {
	if name == "" {
		return Namespace{}, errors.New("headgate: Postgres schema must not be empty")
	}
	if strings.IndexByte(name, 0) >= 0 {
		return Namespace{}, errors.New("headgate: Postgres schema must not contain NUL")
	}
	if len(name) > 63 {
		return Namespace{}, errors.New("headgate: Postgres schema must be at most 63 UTF-8 bytes")
	}
	digest := sha256.Sum256([]byte(name))
	return Namespace{
		name:          name,
		quoted:        QuoteIdentifier(name),
		wakeupChannel: "headgate_wakeup_" + hex.EncodeToString(digest[:8]),
	}, nil
}

func (n Namespace) Name() string          { return n.name }
func (n Namespace) WakeupChannel() string { return n.wakeupChannel }

func QuoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func (n Namespace) Qualified(object string) string {
	return n.quoted + "." + QuoteIdentifier(object)
}

func (n Namespace) Render(sql string) string {
	if n.name == "" {
		return sql
	}
	return qualify(sql, n.quoted, n.wakeupChannel)
}

func identStart(value byte) bool {
	return value == '_' || value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z'
}

func identContinue(value byte) bool {
	return identStart(value) || value >= '0' && value <= '9' || value == '$'
}

func dollarDelimiter(sql string, start int) string {
	if start >= len(sql) || sql[start] != '$' {
		return ""
	}
	end := start + 1
	for end < len(sql) && identContinue(sql[end]) && sql[end] != '$' {
		end++
	}
	if end < len(sql) && sql[end] == '$' {
		return sql[start : end+1]
	}
	return ""
}

func qualify(sql, quotedSchema, wakeupChannel string) string {
	var out strings.Builder
	out.Grow(len(sql) + 64)
	previousIdent, lastIdent := "", ""
	for i := 0; i < len(sql); {
		if strings.HasPrefix(sql[i:], "--") {
			end := strings.IndexByte(sql[i:], '\n')
			if end < 0 {
				out.WriteString(sql[i:])
				break
			}
			end += i + 1
			out.WriteString(sql[i:end])
			i = end
			continue
		}
		if strings.HasPrefix(sql[i:], "/*") {
			start, depth := i, 1
			i += 2
			for i < len(sql) && depth > 0 {
				switch {
				case strings.HasPrefix(sql[i:], "/*"):
					depth++
					i += 2
				case strings.HasPrefix(sql[i:], "*/"):
					depth--
					i += 2
				default:
					i++
				}
			}
			out.WriteString(sql[start:i])
			continue
		}
		if sql[i] == '\'' {
			start := i
			i++
			for i < len(sql) {
				switch sql[i] {
				case '\\':
					i += min(2, len(sql)-i)
				case '\'':
					i++
					if i < len(sql) && sql[i] == '\'' {
						i++
					} else {
						goto stringDone
					}
				default:
					i++
				}
			}
		stringDone:
			literal := sql[start:i]
			if literal == "'headgate_wakeup'" {
				out.WriteByte('\'')
				out.WriteString(wakeupChannel)
				out.WriteByte('\'')
			} else {
				out.WriteString(literal)
			}
			continue
		}
		if sql[i] == '"' {
			start := i
			i++
			for i < len(sql) {
				if sql[i] != '"' {
					i++
					continue
				}
				i++
				if i < len(sql) && sql[i] == '"' {
					i++
					continue
				}
				break
			}
			out.WriteString(sql[start:i])
			continue
		}
		if delimiter := dollarDelimiter(sql, i); delimiter != "" {
			start := i
			body := i + len(delimiter)
			end := strings.Index(sql[body:], delimiter)
			if end < 0 {
				i = len(sql)
			} else {
				i = body + end + len(delimiter)
			}
			out.WriteString(sql[start:i])
			continue
		}
		if identStart(sql[i]) {
			start := i
			i++
			for i < len(sql) && identContinue(sql[i]) {
				i++
			}
			token := sql[start:i]
			_, object := objects[token]
			_, index := indexes[token]
			droppingIndex := index && strings.EqualFold(previousIdent, "DROP") && strings.EqualFold(lastIdent, "INDEX")
			if object || droppingIndex {
				out.WriteString(quotedSchema)
				out.WriteByte('.')
			}
			out.WriteString(token)
			previousIdent, lastIdent = lastIdent, token
			continue
		}
		_, width := utf8.DecodeRuneInString(sql[i:])
		out.WriteString(sql[i : i+width])
		i += width
	}
	return out.String()
}
