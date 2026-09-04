package headgate

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/mujhtech/headgate/go/headgateshared"
)

// Logger returns a job-scoped slog logger. It captures all four levels into the
// current attempt, not stdout or the global logger. Outside a job it discards logs.
// Entries persist at acknowledgement, not live. Error logs do not fail the job.
// Keep this logger attempt-local; it stops accepting records once the attempt ends.
func Logger(ctx context.Context) *slog.Logger {
	state, _ := stepStateFrom(ctx)
	return slog.New(&jobLogHandler{state: state})
}

type jobLogHandler struct {
	state     *stepState
	fields    map[string]any
	group     string
	truncated bool
}

var _ slog.Handler = (*jobLogHandler)(nil)

func (h *jobLogHandler) Enabled(context.Context, slog.Level) bool {
	if h.state == nil {
		return false
	}
	h.state.mu.Lock()
	defer h.state.mu.Unlock()
	return !h.state.logsClosed && len(h.state.logs) <= 100
}

func (h *jobLogHandler) Handle(_ context.Context, record slog.Record) error {
	if h.state == nil {
		return nil
	}
	fields := h.cloneFields()
	truncated := h.truncated
	budget := 128
	record.Attrs(func(attr slog.Attr) bool {
		if budget == 0 || len(fields) >= headgateshared.MaxLogFields {
			truncated = true
			return false
		}
		addLogAttr(fields, h.group, attr, 0, &budget, &truncated)
		return true
	})
	level := "debug"
	switch {
	case record.Level >= slog.LevelError:
		level = "error"
	case record.Level >= slog.LevelWarn:
		level = "warn"
	case record.Level >= slog.LevelInfo:
		level = "info"
	}
	at := record.Time
	if at.IsZero() {
		at = time.Now()
	}
	h.state.appendLog(headgateshared.EncodeLog(headgateshared.LogEntry{
		Level: level, AtMs: at.UnixMilli(), Message: record.Message, Fields: fields, Truncated: truncated,
	}))
	return nil
}

func (h *jobLogHandler) cloneFields() map[string]any {
	fields := make(map[string]any, len(h.fields))
	for key, value := range h.fields {
		fields[key] = value
	}
	return fields
}

func (h *jobLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clone := *h
	clone.fields = h.cloneFields()
	budget := 128
	for _, attr := range attrs {
		if budget == 0 || len(clone.fields) >= headgateshared.MaxLogFields {
			clone.truncated = true
			break
		}
		addLogAttr(clone.fields, h.group, attr, 0, &budget, &clone.truncated)
	}
	return &clone
}

func (h *jobLogHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	clone := *h
	clone.truncated = clone.truncated || len(name) > 128
	if clone.group != "" {
		clone.group += "."
	}
	clone.group += headgateshared.LogText(name, 128)
	if len(clone.group) > 128 {
		clone.truncated = true
	}
	clone.group = headgateshared.LogText(clone.group, 128)
	return &clone
}

func addLogAttr(fields map[string]any, group string, attr slog.Attr, depth int, budget *int, truncated *bool) {
	if *budget == 0 || len(fields) >= headgateshared.MaxLogFields || depth > 4 {
		*truncated = true
		return
	}
	*budget--
	attr.Value = attr.Value.Resolve()
	if attr.Equal(slog.Attr{}) {
		return
	}
	key := attr.Key
	if group != "" && key != "" {
		key = group + "." + key
	} else if key == "" {
		key = group
	}
	if attr.Value.Kind() == slog.KindGroup {
		for _, child := range attr.Value.Group() {
			if *budget == 0 || len(fields) >= headgateshared.MaxLogFields {
				*truncated = true
				break
			}
			addLogAttr(fields, key, child, depth+1, budget, truncated)
		}
		return
	}
	var value any
	switch attr.Value.Kind() {
	case slog.KindString:
		value = attr.Value.String()
	case slog.KindBool:
		value = attr.Value.Bool()
	case slog.KindInt64:
		value = attr.Value.Int64()
	case slog.KindUint64:
		value = attr.Value.Uint64()
	case slog.KindFloat64:
		value = attr.Value.Float64()
	case slog.KindDuration:
		value = attr.Value.Duration().String()
	case slog.KindTime:
		value = attr.Value.Time().UTC().Format(time.RFC3339Nano)
	default:
		value = attr.Value.Any()
		if err, ok := value.(error); ok {
			value = fmt.Sprint(err)
		}
	}
	if text, ok := value.(string); ok && len(text) > 1024 {
		*truncated = true
	}
	if len(key) > 128 {
		*truncated = true
	}
	fields[headgateshared.LogText(key, 128)] = headgateshared.LogScalar(value)
}

func (s *stepState) appendLog(line string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.logsClosed {
		return
	}
	if len(s.logs) < 100 {
		s.logs = append(s.logs, line)
	} else if len(s.logs) == 100 {
		s.logs = append(s.logs, headgateshared.LogCapMessage)
	}
}

func plainLogLine(message string) string {
	if strings.HasPrefix(message, headgateshared.LogPrefix) {
		return headgateshared.EncodeLog(headgateshared.LogEntry{Level: "info", AtMs: time.Now().UnixMilli(), Message: message})
	}
	return headgateshared.LogText(message, headgateshared.MaxLogBytes)
}
