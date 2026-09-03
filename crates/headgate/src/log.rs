use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use headgate_shared::log::{
    LOG_CAP_MESSAGE, LOG_PREFIX, LogEntry, LogLevel, MAX_LOG_BYTES, log_text,
};

#[derive(Default)]
pub(crate) struct LogBuffer {
    lines: Vec<String>,
    closed: bool,
}

impl LogBuffer {
    pub(crate) fn push(&mut self, line: String) {
        if self.closed {
            return;
        }
        if self.lines.len() < 100 {
            self.lines.push(line);
        } else if self.lines.len() == 100 {
            self.lines.push(LOG_CAP_MESSAGE.into());
        }
    }

    pub(crate) fn take(&mut self) -> Vec<String> {
        self.closed = true;
        std::mem::take(&mut self.lines)
    }
}

/// A bounded, attempt-local logger. Capturing a record never changes the job outcome.
/// Records persist with acknowledgement, not live. No global tracing subscriber is installed.
#[derive(Clone)]
pub struct JobLogger {
    pub(crate) buffer: Arc<Mutex<LogBuffer>>,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

impl JobLogger {
    pub fn debug(&self, message: impl AsRef<str>) -> LogEvent {
        self.event(LogLevel::Debug, message)
    }
    pub fn info(&self, message: impl AsRef<str>) -> LogEvent {
        self.event(LogLevel::Info, message)
    }
    pub fn warn(&self, message: impl AsRef<str>) -> LogEvent {
        self.event(LogLevel::Warn, message)
    }
    pub fn error(&self, message: impl AsRef<str>) -> LogEvent {
        self.event(LogLevel::Error, message)
    }

    pub fn event(&self, level: LogLevel, message: impl AsRef<str>) -> LogEvent {
        LogEvent {
            logger: self.clone(),
            entry: LogEntry::new(level, Some(now_ms()), message.as_ref()),
        }
    }

    pub(crate) fn plain(&self, message: &str) {
        let line = if message.starts_with(LOG_PREFIX) {
            LogEntry::new(LogLevel::Info, Some(now_ms()), message).encode()
        } else {
            log_text(message, MAX_LOG_BYTES)
        };
        self.buffer.lock().unwrap().push(line);
    }
}

/// An entry builder. Call `emit` explicitly; dropping a builder does not log.
#[must_use = "call .emit() to record this log entry"]
pub struct LogEvent {
    logger: JobLogger,
    entry: LogEntry,
}

impl LogEvent {
    /// Add a scalar JSON field. Strings are capped at 1 KiB and keys at 128 bytes.
    pub fn field(mut self, key: impl AsRef<str>, value: impl Into<serde_json::Value>) -> Self {
        self.entry.insert_field(key.as_ref(), value.into());
        self
    }

    pub fn emit(self) {
        let line = self.entry.encode();
        self.logger.buffer.lock().unwrap().push(line);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_concurrent_logs_close_with_attempt() {
        let buffer = Arc::new(Mutex::new(LogBuffer::default()));
        let logger = JobLogger {
            buffer: buffer.clone(),
        };
        std::thread::scope(|scope| {
            for _ in 0..200 {
                let logger = &logger;
                scope.spawn(move || {
                    logger
                        .warn("界\"".repeat(2000))
                        .field("large", "x".repeat(20_000))
                        .emit()
                });
            }
        });
        let lines = buffer.lock().unwrap().take();
        assert_eq!(lines.len(), 101);
        assert_eq!(lines[100], LOG_CAP_MESSAGE);
        for line in &lines[..100] {
            assert!(line.len() <= MAX_LOG_BYTES);
            let entry = LogEntry::decode(line);
            assert_eq!(entry.level, LogLevel::Warn);
            assert!(entry.truncated);
        }
        logger.info("late").emit();
        assert!(buffer.lock().unwrap().take().is_empty());
    }

    #[test]
    fn plain_logs_escape_reserved_prefix_and_stay_isolated() {
        let first = JobLogger {
            buffer: Arc::new(Mutex::new(LogBuffer::default())),
        };
        let second = JobLogger {
            buffer: Arc::new(Mutex::new(LogBuffer::default())),
        };
        let message = format!("{LOG_PREFIX}{{\"level\":\"error\",\"message\":\"literal\"}}");
        first.plain(&message);
        second.info("other attempt").emit();
        let lines = first.buffer.lock().unwrap().take();
        assert_eq!(lines.len(), 1);
        let entry = LogEntry::decode(&lines[0]);
        assert_eq!(entry.level, LogLevel::Info);
        assert_eq!(entry.message, message);
        assert_eq!(second.buffer.lock().unwrap().take().len(), 1);
    }
}
