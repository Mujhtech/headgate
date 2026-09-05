//! Versioned structured records inside the backwards-compatible string log transport.

use serde_json::{Map, Value, json};

pub const LOG_PREFIX: &str = "\u{1e}headgate-log-v1:";
pub const MAX_LOG_BYTES: usize = 2048;
pub const MAX_LOG_FIELDS: usize = 32;
pub const LOG_CAP_MESSAGE: &str = "... log cap reached (100 lines/attempt)";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }

    pub fn parse(level: &str) -> Option<Self> {
        match level {
            "debug" => Some(Self::Debug),
            "info" => Some(Self::Info),
            "warn" => Some(Self::Warn),
            "error" => Some(Self::Error),
            _ => None,
        }
    }
}

/// Diagnostic worker-clock time does not participate in admission or lease decisions.
#[derive(Clone, Debug, PartialEq)]
pub struct LogEntry {
    pub level: LogLevel,
    pub at_ms: Option<i64>,
    pub message: String,
    pub fields: Map<String, Value>,
    pub truncated: bool,
}

pub fn log_text(text: &str, limit: usize) -> String {
    let mut end = text.len().min(limit);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_owned()
}

impl LogEntry {
    pub fn new(level: LogLevel, at_ms: Option<i64>, message: &str) -> Self {
        Self {
            level,
            at_ms,
            message: log_text(message, MAX_LOG_BYTES),
            fields: Map::new(),
            truncated: message.len() > MAX_LOG_BYTES,
        }
    }

    /// Fields are bounded scalars. Application objects are not retained or traversed.
    pub fn insert_field(&mut self, key: &str, value: Value) {
        if self.fields.len() >= MAX_LOG_FIELDS {
            self.truncated = true;
            return;
        }
        self.truncated |= key.len() > 128;
        let value = match value {
            Value::String(s) => {
                self.truncated |= s.len() > 1024;
                Value::String(log_text(&s, 1024))
            }
            Value::Array(_) | Value::Object(_) => Value::String("[unsupported log value]".into()),
            scalar => scalar,
        };
        self.fields.insert(log_text(key, 128), value);
    }

    /// Bounds the entire encoded record, not only its message. Truncation stays visible.
    pub fn encode(mut self) -> String {
        // Also normalize records constructed directly through the public fields.
        let fields = std::mem::take(&mut self.fields);
        for (key, value) in fields {
            self.insert_field(&key, value);
        }
        self.truncated |= self.message.len() > MAX_LOG_BYTES;
        self.message = log_text(&self.message, MAX_LOG_BYTES);
        loop {
            let mut value = json!({"level": self.level.as_str(), "message": self.message});
            if let Some(at) = self.at_ms {
                value["at_ms"] = at.into();
            }
            if !self.fields.is_empty() {
                value["fields"] = Value::Object(self.fields.clone());
            }
            if self.truncated {
                value["truncated"] = true.into();
            }
            let encoded = format!("{LOG_PREFIX}{value}");
            if encoded.len() <= MAX_LOG_BYTES {
                return encoded;
            }
            self.truncated = true;
            if let Some(key) = self.fields.keys().next_back().cloned() {
                self.fields.remove(&key);
            } else {
                self.message = log_text(&self.message, self.message.len() / 2);
            }
        }
    }

    /// Unknown versions or malformed records remain readable as literal info messages.
    pub fn decode(line: &str) -> Self {
        let plain = || Self {
            level: LogLevel::Info,
            at_ms: None,
            message: line.to_owned(),
            fields: Map::new(),
            truncated: false,
        };
        if line.len() <= MAX_LOG_BYTES
            && let Some(body) = line.strip_prefix(LOG_PREFIX)
            && let Ok(value) = serde_json::from_str::<Value>(body)
            && let (Some(level), Some(message)) = (
                value
                    .get("level")
                    .and_then(Value::as_str)
                    .and_then(LogLevel::parse),
                value.get("message").and_then(Value::as_str),
            )
        {
            let fields_valid = value.get("fields").is_none_or(|fields| {
                fields
                    .as_object()
                    .is_some_and(|fields| fields.values().all(|v| !v.is_array() && !v.is_object()))
            });
            if !fields_valid
                || value.get("at_ms").is_some_and(|at| !at.is_i64())
                || value.get("truncated").is_some_and(|v| !v.is_boolean())
            {
                return plain();
            }
            return Self {
                level,
                at_ms: value.get("at_ms").and_then(Value::as_i64),
                message: message.to_owned(),
                fields: value
                    .get("fields")
                    .and_then(Value::as_object)
                    .cloned()
                    .unwrap_or_default(),
                truncated: value
                    .get("truncated")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            };
        }
        plain()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_wire_compatibility() {
        for line in [
            "plain",
            r#"{"level":"error","message":"ordinary JSON"}"#,
            "\u{1e}headgate-log-v2:{}",
            "\u{1e}headgate-log-v1:{",
        ] {
            let entry = LogEntry::decode(line);
            assert_eq!(entry.level, LogLevel::Info);
            assert_eq!(entry.message, line);
            assert_eq!(entry.at_ms, None);
        }
        let line = format!(
            "{LOG_PREFIX}{}",
            r#"{"at_ms":1788393600123,"fields":{"bytes":42,"cached":false,"file_id":"résumé"},"level":"warn","message":"download \"slow\""}"#
        );
        let entry = LogEntry::decode(&line);
        assert_eq!(entry.level, LogLevel::Warn);
        assert_eq!(entry.message, "download \"slow\"");
        assert_eq!(entry.fields["file_id"], "résumé");
        assert_eq!(LogEntry::decode(&entry.clone().encode()), entry);
    }

    #[test]
    fn malformed_log_fields_remain_literal() {
        for field in [
            r#""fields":null"#,
            r#""fields":{"x":[]}"#,
            r#""at_ms":null"#,
            r#""at_ms":"bad""#,
            r#""truncated":null"#,
            r#""truncated":1"#,
        ] {
            let line = format!("{LOG_PREFIX}{{\"level\":\"warn\",\"message\":\"test\",{field}}}");
            let entry = LogEntry::decode(&line);
            assert_eq!(entry.message, line);
            assert_eq!(entry.level, LogLevel::Info);
        }
    }
}
