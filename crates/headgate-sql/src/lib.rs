use std::borrow::Cow;

use sha2::{Digest, Sha256};

// Every durable Postgres relation or type the driver or migrator may name. Index names
// are handled separately: CREATE INDEX requires an unqualified name and places it beside
// its explicitly qualified table, while DROP INDEX must identify the schema explicitly.
const OBJECTS: &[&str] = &[
    "headgate_state",
    "headgate_job",
    "headgate_rate_bucket",
    "headgate_quarantine",
    "headgate_partition_deficit",
    "headgate_active_partition",
    "headgate_inflight",
    "headgate_concurrency_limit",
    "headgate_queue_counter",
    "headgate_partition_counter",
    "headgate_queue_state",
    "headgate_duty",
    "headgate_schedule",
    "headgate_schedule_event",
    "headgate_worker",
    "headgate_effect",
    "headgate_operation",
    "headgate_enqueue_policy",
    "headgate_enqueue_counter",
    "headgate_job_tag",
    "headgate_queue_sample",
    "headgate_archive_policy",
    "headgate_job_archive",
    "headgate_job_archive_before_2025",
    "headgate_job_archive_202501",
    "headgate_job_archive_202502",
    "headgate_job_archive_202503",
    "headgate_job_archive_202504",
    "headgate_job_archive_202505",
    "headgate_job_archive_202506",
    "headgate_job_archive_202507",
    "headgate_job_archive_202508",
    "headgate_job_archive_202509",
    "headgate_job_archive_202510",
    "headgate_job_archive_202511",
    "headgate_job_archive_202512",
    "headgate_job_archive_202601",
    "headgate_job_archive_202602",
    "headgate_job_archive_202603",
    "headgate_job_archive_202604",
    "headgate_job_archive_202605",
    "headgate_job_archive_202606",
    "headgate_job_archive_202607",
    "headgate_job_archive_202608",
    "headgate_job_archive_202609",
    "headgate_job_archive_202610",
    "headgate_job_archive_202611",
    "headgate_job_archive_202612",
    "headgate_job_archive_202701",
    "headgate_job_archive_202702",
    "headgate_job_archive_202703",
    "headgate_job_archive_202704",
    "headgate_job_archive_202705",
    "headgate_job_archive_202706",
    "headgate_job_archive_202707",
    "headgate_job_archive_202708",
    "headgate_job_archive_202709",
    "headgate_job_archive_202710",
    "headgate_job_archive_202711",
    "headgate_job_archive_202712",
    "headgate_job_archive_202801",
    "headgate_job_archive_202802",
    "headgate_job_archive_202803",
    "headgate_job_archive_202804",
    "headgate_job_archive_202805",
    "headgate_job_archive_202806",
    "headgate_job_archive_202807",
    "headgate_job_archive_202808",
    "headgate_job_archive_202809",
    "headgate_job_archive_202810",
    "headgate_job_archive_202811",
    "headgate_job_archive_202812",
    "headgate_job_archive_202901",
    "headgate_job_archive_202902",
    "headgate_job_archive_202903",
    "headgate_job_archive_202904",
    "headgate_job_archive_202905",
    "headgate_job_archive_202906",
    "headgate_job_archive_202907",
    "headgate_job_archive_202908",
    "headgate_job_archive_202909",
    "headgate_job_archive_202910",
    "headgate_job_archive_202911",
    "headgate_job_archive_202912",
    "headgate_job_archive_203001",
    "headgate_job_archive_203002",
    "headgate_job_archive_203003",
    "headgate_job_archive_203004",
    "headgate_job_archive_203005",
    "headgate_job_archive_203006",
    "headgate_job_archive_203007",
    "headgate_job_archive_203008",
    "headgate_job_archive_203009",
    "headgate_job_archive_203010",
    "headgate_job_archive_203011",
    "headgate_job_archive_203012",
    "headgate_job_archive_203101",
    "headgate_job_archive_203102",
    "headgate_job_archive_203103",
    "headgate_job_archive_203104",
    "headgate_job_archive_203105",
    "headgate_job_archive_203106",
    "headgate_job_archive_203107",
    "headgate_job_archive_203108",
    "headgate_job_archive_203109",
    "headgate_job_archive_203110",
    "headgate_job_archive_203111",
    "headgate_job_archive_203112",
    "headgate_job_archive_after_2031",
    "headgate_track_enqueue_depth",
    "headgate_schema_migration",
];

const INDEXES: &[&str] = &[
    "headgate_job_unique",
    "headgate_job_unique_throttle",
    "headgate_job_sticky_available",
    "headgate_job_avail_sticky",
    "headgate_job_archive_queue_time",
];

#[derive(Clone, Debug, Default)]
pub struct PostgresNamespace {
    name: Option<String>,
    quoted: String,
    wakeup_channel: String,
}

impl PostgresNamespace {
    pub fn explicit(name: &str) -> Result<Self, String> {
        if name.is_empty() {
            return Err("Postgres schema must not be empty".into());
        }
        if name.as_bytes().contains(&0) {
            return Err("Postgres schema must not contain NUL".into());
        }
        // NAMEDATALEN is 64, including the terminator. Reject instead of allowing the
        // server to truncate two configured instances onto the same identifier.
        if name.len() > 63 {
            return Err("Postgres schema must be at most 63 UTF-8 bytes".into());
        }
        let digest = Sha256::digest(name.as_bytes());
        let digest_hex = format!("{digest:x}");
        let channel_hash = &digest_hex[..16];
        Ok(Self {
            name: Some(name.to_owned()),
            quoted: quote_identifier(name),
            wakeup_channel: format!("headgate_wakeup_{channel_hash}"),
        })
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn wakeup_channel(&self) -> &str {
        if self.name.is_some() {
            &self.wakeup_channel
        } else {
            "headgate_wakeup"
        }
    }

    pub fn render<'a>(&self, sql: &'a str) -> Cow<'a, str> {
        if self.name.is_none() {
            return Cow::Borrowed(sql);
        }
        Cow::Owned(qualify_sql(sql, &self.quoted, self.wakeup_channel()))
    }
}

pub fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn is_ident_start(byte: u8) -> bool {
    byte == b'_' || byte.is_ascii_alphabetic()
}

fn is_ident_continue(byte: u8) -> bool {
    is_ident_start(byte) || byte.is_ascii_digit() || byte == b'$'
}

fn dollar_delimiter(sql: &str, start: usize) -> Option<&str> {
    let bytes = sql.as_bytes();
    if bytes.get(start) != Some(&b'$') {
        return None;
    }
    let mut end = start + 1;
    while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
        end += 1;
    }
    (bytes.get(end) == Some(&b'$')).then(|| &sql[start..=end])
}

fn qualify_sql(sql: &str, quoted_schema: &str, wakeup_channel: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + 64);
    let mut i = 0;
    let mut previous_ident = "";
    let mut last_ident = "";
    while i < bytes.len() {
        // Line comments.
        if bytes[i..].starts_with(b"--") {
            let end = sql[i..].find('\n').map_or(bytes.len(), |n| i + n + 1);
            out.push_str(&sql[i..end]);
            i = end;
            continue;
        }
        // Nested block comments (Postgres permits nesting).
        if bytes[i..].starts_with(b"/*") {
            let start = i;
            i += 2;
            let mut depth = 1_u32;
            while i < bytes.len() && depth != 0 {
                if bytes[i..].starts_with(b"/*") {
                    depth += 1;
                    i += 2;
                } else if bytes[i..].starts_with(b"*/") {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            out.push_str(&sql[start..i]);
            continue;
        }
        // SQL strings. Handle doubled quotes and E'...' backslash escapes conservatively.
        if bytes[i] == b'\'' {
            let start = i;
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\\' {
                    i = (i + 2).min(bytes.len());
                } else if bytes[i] == b'\'' {
                    i += 1;
                    if i < bytes.len() && bytes[i] == b'\'' {
                        i += 1;
                    } else {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            let literal = &sql[start..i];
            if literal == "'headgate_wakeup'" {
                out.push('\'');
                out.push_str(wakeup_channel);
                out.push('\'');
            } else {
                out.push_str(literal);
            }
            continue;
        }
        // Existing quoted identifiers are intentional and never rewritten.
        if bytes[i] == b'"' {
            let start = i;
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'"' {
                    i += 1;
                    if i < bytes.len() && bytes[i] == b'"' {
                        i += 1;
                    } else {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            out.push_str(&sql[start..i]);
            continue;
        }
        // Dollar-quoted function bodies and strings.
        if let Some(delimiter) = dollar_delimiter(sql, i) {
            let start = i;
            let body = i + delimiter.len();
            i = sql[body..]
                .find(delimiter)
                .map_or(bytes.len(), |n| body + n + delimiter.len());
            out.push_str(&sql[start..i]);
            continue;
        }
        if is_ident_start(bytes[i]) {
            let start = i;
            i += 1;
            while i < bytes.len() && is_ident_continue(bytes[i]) {
                i += 1;
            }
            let token = &sql[start..i];
            let dropping_index = INDEXES.contains(&token)
                && previous_ident.eq_ignore_ascii_case("DROP")
                && last_ident.eq_ignore_ascii_case("INDEX");
            if OBJECTS.contains(&token) || dropping_index {
                out.push_str(quoted_schema);
                out.push('.');
                out.push_str(token);
            } else {
                out.push_str(token);
            }
            previous_ident = last_ident;
            last_ident = token;
            continue;
        }
        let ch = sql[i..].chars().next().expect("valid UTF-8");
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_schema_quotes_objects_but_not_literals_comments_or_aliases() {
        let namespace = PostgresNamespace::explicit("tenant-\"blue").unwrap();
        let sql = "SELECT headgate_job.id, headgate_inflight_stale FROM headgate_job \
                   JOIN headgate_rate_bucket b ON true \
                   JOIN headgate_schedule_event e ON true \
                   WHERE note = 'headgate_job' /* headgate_duty */ -- headgate_worker\n\
                   AND state = 'available'::headgate_state";
        let rendered = namespace.render(sql);
        assert!(rendered.contains("\"tenant-\"\"blue\".headgate_job.id"));
        assert!(rendered.contains("\"tenant-\"\"blue\".headgate_rate_bucket"));
        assert!(rendered.contains("\"tenant-\"\"blue\".headgate_schedule_event"));
        assert!(rendered.contains("::\"tenant-\"\"blue\".headgate_state"));
        assert!(rendered.contains("headgate_inflight_stale FROM"));
        assert!(rendered.contains("'headgate_job' /* headgate_duty */"));
        assert!(rendered.contains("-- headgate_worker"));
    }

    #[test]
    fn explicit_schema_namespaces_notifications_and_default_is_byte_identity() {
        let sql = "SELECT pg_notify('headgate_wakeup', queue) FROM headgate_job";
        assert_eq!(PostgresNamespace::default().render(sql), sql);
        let namespace = PostgresNamespace::explicit("tenant").unwrap();
        let rendered = namespace.render(sql);
        assert!(rendered.contains(namespace.wakeup_channel()));
        assert!(rendered.contains("\"tenant\".headgate_job"));
        assert_eq!(
            quote_identifier(namespace.wakeup_channel()),
            format!("\"{}\"", namespace.wakeup_channel())
        );
    }

    #[test]
    fn invalid_schema_names_fail_instead_of_truncating_or_sharing() {
        assert!(PostgresNamespace::explicit("").is_err());
        assert!(PostgresNamespace::explicit("bad\0schema").is_err());
        assert!(PostgresNamespace::explicit(&"x".repeat(64)).is_err());
        assert!(PostgresNamespace::explicit(&"x".repeat(63)).is_ok());
    }

    #[test]
    fn enqueue_backpressure_objects_are_namespaced_outside_the_trigger_body() {
        let namespace = PostgresNamespace::explicit("tenant").unwrap();
        let sql = "CREATE TABLE headgate_enqueue_policy (queue text);\n\
                   CREATE TABLE headgate_enqueue_counter (queue text);\n\
                   CREATE OR REPLACE FUNCTION headgate_track_enqueue_depth()\n\
                   RETURNS trigger LANGUAGE plpgsql AS $$\n\
                   BEGIN\n\
                     EXECUTE format('INSERT INTO %I.headgate_enqueue_counter VALUES ($1)',\n\
                                    TG_TABLE_SCHEMA) USING NEW.queue;\n\
                     RETURN NEW;\n\
                   END;\n\
                   $$;\n\
                   CREATE TRIGGER track AFTER INSERT ON headgate_job\n\
                   FOR EACH ROW EXECUTE FUNCTION headgate_track_enqueue_depth();";
        let rendered = namespace.render(sql);

        assert!(rendered.contains("CREATE TABLE \"tenant\".headgate_enqueue_policy"));
        assert!(rendered.contains("CREATE TABLE \"tenant\".headgate_enqueue_counter"));
        assert!(
            rendered
                .contains("CREATE OR REPLACE FUNCTION \"tenant\".headgate_track_enqueue_depth()")
        );
        assert!(rendered.contains("ON \"tenant\".headgate_job"));
        assert!(rendered.contains("EXECUTE FUNCTION \"tenant\".headgate_track_enqueue_depth()"));
        assert!(rendered.contains("%I.headgate_enqueue_counter"));
        assert!(rendered.contains("TG_TABLE_SCHEMA"));
    }

    #[test]
    fn migration_indexes_and_new_metric_tables_stay_inside_the_explicit_schema() {
        let namespace = PostgresNamespace::explicit("tenant").unwrap();
        let rendered = namespace.render(
            "DROP INDEX headgate_job_unique;\n\
             CREATE UNIQUE INDEX headgate_job_unique ON headgate_job (unique_key);\n\
             CREATE TABLE headgate_job_tag (job_id bigint);\n\
             CREATE TABLE headgate_queue_sample (queue text);",
        );
        assert!(rendered.contains("DROP INDEX \"tenant\".headgate_job_unique"));
        assert!(
            rendered.contains("CREATE UNIQUE INDEX headgate_job_unique ON \"tenant\".headgate_job")
        );
        assert!(rendered.contains("CREATE TABLE \"tenant\".headgate_job_tag"));
        assert!(rendered.contains("CREATE TABLE \"tenant\".headgate_queue_sample"));
    }
}
