use std::{collections::BTreeMap, ffi::OsString, path::PathBuf, process::Stdio, time::Duration};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

use crate::{BoxError, CodecError, Control, Envelope, ErasedHandler, HandlerFuture, JobCtx};

pub const ISOLATED_PROTOCOL_PREFIX: &str = "HEADGATE/1 ";
const DEFAULT_MAX_OUTPUT_BYTES: usize = 64 * 1024;

/// Immutable request written to an isolated handler's stdin as one JSON document.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IsolatedRequest {
    pub version: u32,
    pub job_id: String,
    pub kind: String,
    pub schema_version: u32,
    pub payload_base64: String,
    pub queue: String,
    pub partition_key: String,
    pub rate_class: String,
    pub weight: u32,
    pub attempt: u32,
    pub crash_attempt: u32,
    pub max_attempts: u32,
    pub fence: u64,
    pub deadline_ms: i64,
}

impl IsolatedRequest {
    pub fn payload(&self) -> Result<Vec<u8>, base64::DecodeError> {
        STANDARD.decode(&self.payload_base64)
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IsolatedOutcome {
    Success,
    Retry,
    Skip,
    Revoke,
    Snooze,
    RateLimited,
    Undecodable,
}

/// The child emits `HEADGATE/1 ` followed by this JSON object on one stdout line.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IsolatedResponse {
    pub version: u32,
    pub outcome: IsolatedOutcome,
    #[serde(default)]
    pub error: String,
    #[serde(default)]
    pub delay_ms: i64,
}

/// A fixed child-process command. Environment inheritance is disabled by default so a
/// handler receives only explicitly configured values, not every worker secret.
#[derive(Clone, Debug)]
pub struct IsolatedProcess {
    pub program: PathBuf,
    pub args: Vec<OsString>,
    pub env: BTreeMap<OsString, OsString>,
    pub inherit_env: bool,
    pub max_output_bytes: usize,
}

impl IsolatedProcess {
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            env: BTreeMap::new(),
            inherit_env: false,
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
        }
    }

    pub fn arg(mut self, arg: impl Into<OsString>) -> Self {
        self.args.push(arg.into());
        self
    }

    pub fn env(mut self, key: impl Into<OsString>, value: impl Into<OsString>) -> Self {
        self.env.insert(key.into(), value.into());
        self
    }

    pub fn inherit_env(mut self, inherit: bool) -> Self {
        self.inherit_env = inherit;
        self
    }

    pub fn max_output_bytes(mut self, bytes: usize) -> Self {
        self.max_output_bytes = bytes;
        self
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.program.as_os_str().is_empty() {
            return Err("isolated process program must not be empty".into());
        }
        if self.max_output_bytes == 0 {
            return Err("isolated process max_output_bytes must be greater than zero".into());
        }
        Ok(())
    }
}

pub(crate) struct IsolatedHandler {
    process: IsolatedProcess,
}

impl IsolatedHandler {
    pub(crate) fn new(process: IsolatedProcess) -> Self {
        Self { process }
    }
}

impl ErasedHandler for IsolatedHandler {
    fn call(&self, ctx: JobCtx, env: Envelope) -> HandlerFuture {
        let process = self.process.clone();
        Box::pin(async move { execute(process, ctx, env).await })
    }
}

async fn execute(process: IsolatedProcess, ctx: JobCtx, env: Envelope) -> Result<(), BoxError> {
    let request = IsolatedRequest {
        version: 1,
        job_id: env.id,
        kind: env.kind,
        schema_version: env.schema_version,
        payload_base64: STANDARD.encode(env.payload),
        queue: env.queue,
        partition_key: env.partition_key,
        rate_class: env.rate_class,
        weight: headgate_core::effective_weight(env.weight),
        attempt: env.attempt,
        crash_attempt: env.crash_attempt,
        max_attempts: env.max_attempts,
        fence: ctx.lease().fence,
        deadline_ms: env.deadline_ms,
    };
    execute_request(process, request).await
}

async fn execute_request(
    process: IsolatedProcess,
    request: IsolatedRequest,
) -> Result<(), BoxError> {
    let input = serde_json::to_vec(&request)?;

    let mut command = tokio::process::Command::new(&process.program);
    command
        .args(&process.args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if !process.inherit_env {
        command.env_clear();
    }
    command.envs(&process.env);
    let mut child = command.spawn()?;
    let mut stdin = child.stdin.take().expect("piped child stdin");
    let stdout = child.stdout.take().expect("piped child stdout");
    let stderr = child.stderr.take().expect("piped child stderr");
    let max = process.max_output_bytes;

    let write = async move {
        let result = stdin.write_all(&input).await;
        let _ = stdin.shutdown().await;
        result
    };
    let (write_result, stdout_result, stderr_result, status_result) = tokio::join!(
        write,
        read_bounded(stdout, max),
        read_bounded(stderr, max),
        child.wait(),
    );
    if let Err(error) = write_result {
        if error.kind() != std::io::ErrorKind::BrokenPipe {
            return Err(Box::new(error));
        }
    }
    let (stdout, stdout_overflow) = stdout_result?;
    let (stderr, stderr_overflow) = stderr_result?;
    if stdout_overflow || stderr_overflow {
        return Err(message_error(format!(
            "isolated handler output exceeded {max} bytes"
        )));
    }
    let status = status_result?;
    if !status.success() {
        let detail = String::from_utf8_lossy(&stderr);
        return Err(message_error(format!(
            "isolated handler exited with {status}: {}",
            detail.trim()
        )));
    }
    let response = parse_response(&stdout)?;
    response_result(response)
}

async fn read_bounded(
    mut reader: impl AsyncRead + Unpin,
    max: usize,
) -> std::io::Result<(Vec<u8>, bool)> {
    let mut bytes = Vec::with_capacity(max.min(8192));
    let mut limited = (&mut reader).take((max as u64).saturating_add(1));
    limited.read_to_end(&mut bytes).await?;
    let overflow = bytes.len() > max;
    if overflow {
        bytes.truncate(max);
        tokio::io::copy(&mut reader, &mut tokio::io::sink()).await?;
    }
    Ok((bytes, overflow))
}

fn parse_response(stdout: &[u8]) -> Result<IsolatedResponse, BoxError> {
    let prefix = ISOLATED_PROTOCOL_PREFIX.as_bytes();
    let line = stdout
        .split(|b| *b == b'\n')
        .rev()
        .find(|line| line.starts_with(prefix))
        .ok_or_else(|| message_error("isolated handler emitted no HEADGATE/1 response"))?;
    let response: IsolatedResponse = serde_json::from_slice(&line[prefix.len()..])?;
    if response.version != 1 {
        return Err(message_error(format!(
            "isolated handler response version {} is unsupported",
            response.version
        )));
    }
    Ok(response)
}

fn response_result(response: IsolatedResponse) -> Result<(), BoxError> {
    match response.outcome {
        IsolatedOutcome::Success => Ok(()),
        IsolatedOutcome::Retry => Err(message_error(if response.error.is_empty() {
            "isolated handler requested retry".into()
        } else {
            response.error
        })),
        IsolatedOutcome::Skip => Err(Box::new(Control::Skip)),
        IsolatedOutcome::Revoke => Err(Box::new(Control::Revoke)),
        IsolatedOutcome::RateLimited => Err(Box::new(Control::RateLimited)),
        IsolatedOutcome::Snooze => Err(Box::new(Control::Snooze(Duration::from_millis(
            u64::try_from(response.delay_ms).unwrap_or(0),
        )))),
        IsolatedOutcome::Undecodable => Err(Box::new(CodecError::Malformed(
            if response.error.is_empty() {
                "isolated handler rejected payload".into()
            } else {
                response.error
            },
        ))),
    }
}

fn message_error(message: impl Into<String>) -> BoxError {
    Box::new(std::io::Error::other(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    fn request(payload: &[u8]) -> IsolatedRequest {
        IsolatedRequest {
            version: 1,
            job_id: "isolated-1".into(),
            kind: "isolated:test".into(),
            schema_version: 1,
            payload_base64: STANDARD.encode(payload),
            queue: "default".into(),
            partition_key: "tenant".into(),
            rate_class: "api".into(),
            weight: 1,
            attempt: 2,
            crash_attempt: 1,
            max_attempts: 5,
            fence: 7,
            deadline_ms: 0,
        }
    }

    #[test]
    fn parses_prefixed_response_after_handler_logs() {
        let response = parse_response(
            b"ordinary child log\nHEADGATE/1 {\"version\":1,\"outcome\":\"success\"}\n",
        )
        .unwrap();
        assert_eq!(response.outcome, IsolatedOutcome::Success);
    }

    #[test]
    fn rejects_unknown_protocol_version() {
        let error =
            parse_response(b"HEADGATE/1 {\"version\":2,\"outcome\":\"success\"}\n").unwrap_err();
        assert!(error.to_string().contains("unsupported"));
    }

    #[test]
    fn maps_child_control_outcomes_to_runtime_controls() {
        let response = |outcome, delay_ms| IsolatedResponse {
            version: 1,
            outcome,
            error: String::new(),
            delay_ms,
        };
        assert!(response_result(response(IsolatedOutcome::Success, 0)).is_ok());
        assert!(
            response_result(response(IsolatedOutcome::Skip, 0))
                .unwrap_err()
                .downcast_ref::<Control>()
                .is_some_and(|c| *c == Control::Skip)
        );
        assert!(
            response_result(response(IsolatedOutcome::Snooze, 250))
                .unwrap_err()
                .downcast_ref::<Control>()
                .is_some_and(|c| *c == Control::Snooze(Duration::from_millis(250)))
        );
    }

    #[test]
    #[ignore]
    fn isolated_child_helper() {
        if std::env::var_os("HG_ISOLATED_HELPER").is_none() {
            return;
        }
        let mut input = Vec::new();
        std::io::stdin().read_to_end(&mut input).unwrap();
        let request: IsolatedRequest = serde_json::from_slice(&input).unwrap();
        assert_eq!(request.job_id, "isolated-1");
        assert_eq!(request.payload().unwrap(), b"ok");
        assert!(
            std::env::var_os("PATH").is_none(),
            "the isolated environment must be clear by default"
        );
        if request.kind == "isolated:sleep" {
            std::thread::sleep(Duration::from_secs(5));
        }
        writeln!(
            std::io::stdout(),
            "{ISOLATED_PROTOCOL_PREFIX}{{\"version\":1,\"outcome\":\"success\"}}"
        )
        .unwrap();
    }

    fn helper_process() -> IsolatedProcess {
        IsolatedProcess::new(std::env::current_exe().unwrap())
            .arg("--ignored")
            .arg("--exact")
            .arg("isolated::tests::isolated_child_helper")
            .arg("--nocapture")
            .env("HG_ISOLATED_HELPER", "1")
    }

    #[tokio::test]
    async fn executes_versioned_request_with_sanitized_environment() {
        execute_request(helper_process(), request(b"ok"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn dropping_attempt_future_kills_sleeping_child() {
        let mut sleepy = request(b"ok");
        sleepy.kind = "isolated:sleep".into();
        let timed = tokio::time::timeout(
            Duration::from_millis(100),
            execute_request(helper_process(), sleepy),
        )
        .await;
        assert!(
            timed.is_err(),
            "the helper must still be sleeping at the deadline"
        );
    }
}
