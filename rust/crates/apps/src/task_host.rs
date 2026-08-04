use std::{
    env,
    future::Future,
    io,
    net::{Ipv4Addr, SocketAddr},
    path::Path,
    pin::Pin,
    time::Duration,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    time,
};

use crate::shutdown::{ShutdownSignalError, ShutdownSignalFuture, install_shutdown_signal};

pub type TaskHostStopFuture<'a, Exit, Error> =
    Pin<Box<dyn Future<Output = Result<Exit, Error>> + Send + 'a>>;

pub trait TaskHostStatus: Clone + Send + 'static {
    fn is_terminal(&self) -> bool;
}

pub trait TaskHost {
    type Status: TaskHostStatus;
    type Exit: Copy + Send + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    fn status(&self) -> Self::Status;

    fn stop(&mut self) -> TaskHostStopFuture<'_, Self::Exit, Self::Error>;
}

pub const TASK_CONTROL_TOKEN_ENV: &str = "CRYPTO_TRADING_TASK_CONTROL_TOKEN";

const TASK_CONTROL_TOKEN_MIN_BYTES: usize = 32;
const TASK_CONTROL_TOKEN_MAX_BYTES: usize = 512;
const TASK_CONTROL_STATUS_TIMEOUT: Duration = Duration::from_secs(1);
const TASK_CONTROL_STOP_TIMEOUT: Duration = Duration::from_secs(125);
const TASK_CONTROL_READ_TIMEOUT: Duration = Duration::from_millis(250);
const TASK_CONTROL_MAX_LINE_BYTES: usize = 1_024;
const TASK_CONTROL_MAX_RESPONSE_BYTES: usize = 16 * 1024;

#[derive(Clone, Eq, PartialEq)]
struct TaskHostControlToken(String);

impl TaskHostControlToken {
    fn load_from_env() -> Result<Self, TaskHostControlTokenError> {
        Self::from_env_result(env::var(TASK_CONTROL_TOKEN_ENV))
    }

    fn from_env_result(
        value: Result<String, env::VarError>,
    ) -> Result<Self, TaskHostControlTokenError> {
        match value {
            Ok(token) => Self::validate(token),
            Err(_) => Err(TaskHostControlTokenError::Missing(TASK_CONTROL_TOKEN_ENV)),
        }
    }

    fn validate(token: String) -> Result<Self, TaskHostControlTokenError> {
        let len = token.len();
        if !(TASK_CONTROL_TOKEN_MIN_BYTES..=TASK_CONTROL_TOKEN_MAX_BYTES).contains(&len) {
            return Err(TaskHostControlTokenError::Invalid(format!(
                "{TASK_CONTROL_TOKEN_ENV} must be {TASK_CONTROL_TOKEN_MIN_BYTES}-{TASK_CONTROL_TOKEN_MAX_BYTES} bytes"
            )));
        }
        if !token.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err(TaskHostControlTokenError::Invalid(format!(
                "{TASK_CONTROL_TOKEN_ENV} must contain only printable non-whitespace ASCII bytes"
            )));
        }
        Ok(Self(token))
    }

    fn write_request(&self, command: TaskHostControlCommand) -> String {
        format!("auth {}\n{}", self.0, command.as_wire())
    }

    fn matches(&self, presented: &str) -> bool {
        constant_time_eq(self.0.as_bytes(), presented.as_bytes())
    }
}

impl std::fmt::Debug for TaskHostControlToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("TaskHostControlToken([REDACTED])")
    }
}

/// Verifies that the loopback task-control secret is present and well formed.
///
/// # Errors
///
/// Returns [`TaskHostControlTokenError`] when the environment variable is
/// absent or does not satisfy the bounded printable-ASCII contract.
pub fn ensure_control_token_configured() -> Result<(), TaskHostControlTokenError> {
    TaskHostControlToken::load_from_env().map(|_| ())
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TaskHostControlTokenError {
    Missing(&'static str),
    Invalid(String),
}

impl std::fmt::Display for TaskHostControlTokenError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing(name) => {
                write!(formatter, "required environment variable {name} is not set")
            }
            Self::Invalid(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for TaskHostControlTokenError {}

#[derive(Debug)]
pub enum TaskHostControlError {
    Io(io::Error),
    Token(TaskHostControlTokenError),
    Unauthorized,
    Protocol(String),
}

impl std::fmt::Display for TaskHostControlError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(source) => write!(formatter, "task host control I/O failed: {source}"),
            Self::Token(source) => write!(formatter, "{source}"),
            Self::Unauthorized => formatter.write_str("task host control token was rejected"),
            Self::Protocol(message) => {
                write!(formatter, "task host control request failed: {message}")
            }
        }
    }
}

impl std::error::Error for TaskHostControlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(source) => Some(source),
            Self::Token(source) => Some(source),
            Self::Unauthorized | Self::Protocol(_) => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskHostControlCommand {
    Status,
    Stop,
}

impl TaskHostControlCommand {
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::Status => "status\n",
            Self::Stop => "stop\n",
        }
    }

    fn parse(line: &str) -> Result<Self, &'static str> {
        match line.trim() {
            "status" => Ok(Self::Status),
            "stop" => Ok(Self::Stop),
            _ => Err("unknown task host control command"),
        }
    }
}

#[derive(Debug)]
pub enum TaskHostServeOutcome<Status, Exit> {
    StopRequested(Exit),
    Terminal(Status),
}

#[derive(Debug)]
pub enum TaskHostServeError<E> {
    Io(io::Error),
    Task(E),
    Shutdown(ShutdownSignalError),
    ControlToken(TaskHostControlTokenError),
}

impl<E> std::fmt::Display for TaskHostServeError<E>
where
    E: std::fmt::Display,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(source) => write!(formatter, "task host control I/O failed: {source}"),
            Self::Task(source) => write!(formatter, "task host operation failed: {source}"),
            Self::Shutdown(source) => {
                write!(formatter, "task host shutdown handling failed: {source}")
            }
            Self::ControlToken(source) => {
                write!(formatter, "task host control token is invalid: {source}")
            }
        }
    }
}

impl<E> std::error::Error for TaskHostServeError<E>
where
    E: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(source) => Some(source),
            Self::Task(source) => Some(source),
            Self::Shutdown(source) => Some(source),
            Self::ControlToken(source) => Some(source),
        }
    }
}

pub fn control_addr(task_id: &str, history_path: &Path, control_port: Option<u16>) -> SocketAddr {
    let port = control_port.unwrap_or_else(|| default_control_port(task_id, history_path));
    SocketAddr::from((Ipv4Addr::LOCALHOST, port))
}

/// Sends one control command to a running task host and returns its text response.
///
/// # Errors
/// Returns any TCP connect, write, shutdown, or read failure from the loopback socket.
pub async fn query_control(
    address: SocketAddr,
    command: TaskHostControlCommand,
) -> Result<String, TaskHostControlError> {
    let token = TaskHostControlToken::load_from_env().map_err(TaskHostControlError::Token)?;
    query_control_with_token(address, command, &token).await
}

async fn query_control_with_token(
    address: SocketAddr,
    command: TaskHostControlCommand,
    token: &TaskHostControlToken,
) -> Result<String, TaskHostControlError> {
    query_control_with_token_timeout(address, command, token, control_timeout(command)).await
}

async fn query_control_with_token_timeout(
    address: SocketAddr,
    command: TaskHostControlCommand,
    token: &TaskHostControlToken,
    timeout: Duration,
) -> Result<String, TaskHostControlError> {
    let response = time::timeout(timeout, async {
        let mut stream = TcpStream::connect(address)
            .await
            .map_err(TaskHostControlError::Io)?;
        stream
            .write_all(token.write_request(command).as_bytes())
            .await
            .map_err(TaskHostControlError::Io)?;
        stream.shutdown().await.map_err(TaskHostControlError::Io)?;
        read_control_response(&mut stream).await
    })
    .await
    .map_err(|_| {
        TaskHostControlError::Io(io::Error::new(
            io::ErrorKind::TimedOut,
            "task host control request timed out",
        ))
    })??;
    match response.trim() {
        "error: unauthorized" => Err(TaskHostControlError::Unauthorized),
        message if message.starts_with("error: ") => {
            Err(TaskHostControlError::Protocol(message.to_owned()))
        }
        _ => Ok(response),
    }
}

const fn control_timeout(command: TaskHostControlCommand) -> Duration {
    match command {
        TaskHostControlCommand::Status => TASK_CONTROL_STATUS_TIMEOUT,
        TaskHostControlCommand::Stop => TASK_CONTROL_STOP_TIMEOUT,
    }
}

/// Runs a loopback control host for a task and exits when the task stops or terminates.
///
/// # Errors
/// Returns I/O failures from the listener/socket path or task stop failures surfaced by `host`.
pub async fn serve_host<H, FStatus, FStop>(
    host: &mut H,
    listener: TcpListener,
    poll_interval: Duration,
    render_status: FStatus,
    render_stop: FStop,
) -> Result<TaskHostServeOutcome<H::Status, H::Exit>, TaskHostServeError<H::Error>>
where
    H: TaskHost,
    FStatus: Fn(&H::Status) -> String,
    FStop: Fn(&H::Status, H::Exit) -> String,
{
    serve_host_with_shutdown(
        host,
        listener,
        poll_interval,
        render_status,
        render_stop,
        install_shutdown_signal().map_err(TaskHostServeError::Shutdown),
    )
    .await
}

pub(crate) async fn serve_host_with_shutdown<H, FStatus, FStop>(
    host: &mut H,
    listener: TcpListener,
    poll_interval: Duration,
    render_status: FStatus,
    render_stop: FStop,
    shutdown: Result<ShutdownSignalFuture, TaskHostServeError<H::Error>>,
) -> Result<TaskHostServeOutcome<H::Status, H::Exit>, TaskHostServeError<H::Error>>
where
    H: TaskHost,
    FStatus: Fn(&H::Status) -> String,
    FStop: Fn(&H::Status, H::Exit) -> String,
{
    let token = TaskHostControlToken::load_from_env().map_err(TaskHostServeError::ControlToken)?;
    serve_host_with_shutdown_token(
        host,
        listener,
        poll_interval,
        render_status,
        render_stop,
        shutdown,
        &token,
    )
    .await
}

async fn serve_host_with_shutdown_token<H, FStatus, FStop>(
    host: &mut H,
    listener: TcpListener,
    poll_interval: Duration,
    render_status: FStatus,
    render_stop: FStop,
    shutdown: Result<ShutdownSignalFuture, TaskHostServeError<H::Error>>,
    token: &TaskHostControlToken,
) -> Result<TaskHostServeOutcome<H::Status, H::Exit>, TaskHostServeError<H::Error>>
where
    H: TaskHost,
    FStatus: Fn(&H::Status) -> String,
    FStop: Fn(&H::Status, H::Exit) -> String,
{
    let mut shutdown = match shutdown {
        Ok(shutdown) => shutdown,
        Err(error) => {
            tracing::error!(
                event = "task_host_signal_registration_failed",
                "task host cannot guarantee graceful termination"
            );
            return Err(error);
        }
    };
    tracing::info!(
        event = "task_host_ready",
        "task host is accepting loopback control requests"
    );
    loop {
        tokio::select! {
            result = &mut shutdown => {
                let signal = match result {
                    Ok(signal) => signal,
                    Err(error) => {
                        tracing::error!(
                            event = "task_host_signal_receive_failed",
                            "task host lost its graceful-shutdown signal stream"
                        );
                        return Err(TaskHostServeError::Shutdown(error));
                    }
                };
                tracing::info!(
                    event = "task_host_shutdown_requested",
                    signal = ?signal,
                    "task host received an operating-system shutdown signal"
                );
                let exit = match host.stop().await {
                    Ok(exit) => exit,
                    Err(error) => {
                        tracing::error!(
                            event = "task_host_shutdown_failed",
                            "task host failed to reach its bounded stop outcome"
                        );
                        return Err(TaskHostServeError::Task(error));
                    }
                };
                tracing::info!(
                    event = "task_host_shutdown_completed",
                    "task host reached its graceful stop outcome"
                );
                return Ok(TaskHostServeOutcome::StopRequested(exit));
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted.map_err(TaskHostServeError::Io)?;
                if let Some(exit) =
                    handle_connection(host, stream, &render_status, &render_stop, token).await?
                {
                    return Ok(TaskHostServeOutcome::StopRequested(exit));
                }
            }
            () = time::sleep(poll_interval) => {
                let status = host.status();
                if status.is_terminal() {
                    tracing::info!(
                        event = "task_host_terminal",
                        "task host observed a terminal owner status"
                    );
                    return Ok(TaskHostServeOutcome::Terminal(status));
                }
            }
        }
    }
}

async fn handle_connection<H, FStatus, FStop>(
    host: &mut H,
    mut stream: TcpStream,
    render_status: &FStatus,
    render_stop: &FStop,
    token: &TaskHostControlToken,
) -> Result<Option<H::Exit>, TaskHostServeError<H::Error>>
where
    H: TaskHost,
    FStatus: Fn(&H::Status) -> String,
    FStop: Fn(&H::Status, H::Exit) -> String,
{
    let mut reader = BufReader::new(stream);
    let auth_line = match read_control_line(&mut reader).await {
        Ok(Some(line)) => line,
        Ok(None) => return Ok(None),
        Err(error) => {
            tracing::warn!(
                event = "task_host_control_rejected",
                reason = %error,
                "task host rejected an invalid loopback control request"
            );
            stream = reader.into_inner();
            let _ = write_response(&mut stream, "error: unauthorized\n").await;
            return Ok(None);
        }
    };
    let Some(presented) = auth_line.strip_prefix("auth ") else {
        let mut stream = reader.into_inner();
        let _ = write_response(&mut stream, "error: unauthorized\n").await;
        return Ok(None);
    };
    if !token.matches(presented) {
        let mut stream = reader.into_inner();
        let _ = write_response(&mut stream, "error: unauthorized\n").await;
        return Ok(None);
    }
    let command_line = match read_control_line(&mut reader).await {
        Ok(Some(line)) if !line.trim().is_empty() => line,
        Ok(Some(_) | None) => {
            let mut stream = reader.into_inner();
            let _ = write_response(&mut stream, "error: unauthorized\n").await;
            return Ok(None);
        }
        Err(error) => {
            tracing::warn!(
                event = "task_host_control_rejected",
                reason = %error,
                "task host rejected an invalid loopback control request"
            );
            let mut stream = reader.into_inner();
            let _ = write_response(&mut stream, "error: unauthorized\n").await;
            return Ok(None);
        }
    };
    let mut stream = reader.into_inner();
    if command_line.trim().is_empty() {
        let _ = write_response(&mut stream, "error: unauthorized\n").await;
        return Ok(None);
    }

    match TaskHostControlCommand::parse(&command_line) {
        Ok(TaskHostControlCommand::Status) => {
            write_response(&mut stream, &render_status(&host.status()))
                .await
                .map_err(TaskHostServeError::Io)?;
            Ok(None)
        }
        Ok(TaskHostControlCommand::Stop) => {
            tracing::info!(
                event = "task_host_stop_requested",
                source = "loopback_control",
                "task host received a local stop command"
            );
            let exit = match host.stop().await {
                Ok(exit) => exit,
                Err(error) => {
                    tracing::error!(
                        event = "task_host_shutdown_failed",
                        source = "loopback_control",
                        "task host failed to reach its bounded stop outcome"
                    );
                    return Err(TaskHostServeError::Task(error));
                }
            };
            let status = host.status();
            write_response(&mut stream, &render_stop(&status, exit))
                .await
                .map_err(TaskHostServeError::Io)?;
            tracing::info!(
                event = "task_host_shutdown_completed",
                source = "loopback_control",
                "task host reached its graceful stop outcome"
            );
            Ok(Some(exit))
        }
        Err(message) => {
            write_response(&mut stream, &format!("error: {message}\n"))
                .await
                .map_err(TaskHostServeError::Io)?;
            Ok(None)
        }
    }
}

async fn write_response(stream: &mut TcpStream, response: &str) -> io::Result<()> {
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

async fn read_control_response(stream: &mut TcpStream) -> Result<String, TaskHostControlError> {
    let mut response = Vec::new();
    let mut buffer = [0_u8; 1024];
    loop {
        let read = stream
            .read(&mut buffer)
            .await
            .map_err(TaskHostControlError::Io)?;
        if read == 0 {
            break;
        }
        if response.len().saturating_add(read) > TASK_CONTROL_MAX_RESPONSE_BYTES {
            return Err(TaskHostControlError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "task host control response exceeded {TASK_CONTROL_MAX_RESPONSE_BYTES} bytes"
                ),
            )));
        }
        response.extend_from_slice(&buffer[..read]);
    }
    String::from_utf8(response).map_err(|_| {
        TaskHostControlError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "task host control response was not valid UTF-8",
        ))
    })
}

#[derive(Debug)]
enum ReadControlLineError {
    Timeout,
    TooLarge,
    InvalidEncoding,
    Io(io::Error),
}

impl std::fmt::Display for ReadControlLineError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout => formatter.write_str("timed out waiting for a control line"),
            Self::TooLarge => formatter.write_str("control line exceeded the byte limit"),
            Self::InvalidEncoding => formatter.write_str("control line was not valid UTF-8"),
            Self::Io(source) => write!(formatter, "control line I/O failed: {source}"),
        }
    }
}

async fn read_control_line(
    reader: &mut BufReader<TcpStream>,
) -> Result<Option<String>, ReadControlLineError> {
    match time::timeout(TASK_CONTROL_READ_TIMEOUT, read_control_line_inner(reader)).await {
        Ok(result) => result,
        Err(_) => Err(ReadControlLineError::Timeout),
    }
}

async fn read_control_line_inner(
    reader: &mut BufReader<TcpStream>,
) -> Result<Option<String>, ReadControlLineError> {
    let mut bytes = Vec::new();
    loop {
        let byte = match reader.read_u8().await {
            Ok(byte) => byte,
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
                if bytes.is_empty() {
                    return Ok(None);
                }
                break;
            }
            Err(error) => return Err(ReadControlLineError::Io(error)),
        };
        if byte == b'\n' {
            break;
        }
        if byte != b'\r' {
            if bytes.len() >= TASK_CONTROL_MAX_LINE_BYTES {
                return Err(ReadControlLineError::TooLarge);
            }
            bytes.push(byte);
        }
    }
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|_| ReadControlLineError::InvalidEncoding)
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0_u8;
    for (&lhs, &rhs) in left.iter().zip(right.iter()) {
        diff |= lhs ^ rhs;
    }
    diff == 0
}

fn default_control_port(task_id: &str, history_path: &Path) -> u16 {
    const OFFSET: u64 = 14_695_981_039_346_656_037;
    const PRIME: u64 = 1_099_511_628_211;
    const BASE: u16 = 49_152;
    const SPAN: u16 = 16_384;

    let mut hash = OFFSET;
    for byte in history_path
        .to_string_lossy()
        .bytes()
        .chain(std::iter::once(0))
        .chain(task_id.bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(PRIME);
    }
    BASE + u16::try_from(hash % u64::from(SPAN)).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shutdown::{ShutdownSignal, ShutdownSignalError, ShutdownSignalStage};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    #[derive(Clone, Debug)]
    struct MockStatus;

    impl TaskHostStatus for MockStatus {
        fn is_terminal(&self) -> bool {
            false
        }
    }

    #[derive(Clone)]
    struct MockHost {
        stopped: Arc<AtomicBool>,
    }

    impl MockHost {
        fn new() -> Self {
            Self {
                stopped: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    impl TaskHost for MockHost {
        type Status = MockStatus;
        type Exit = &'static str;
        type Error = io::Error;

        fn status(&self) -> Self::Status {
            MockStatus
        }

        fn stop(&mut self) -> TaskHostStopFuture<'_, Self::Exit, Self::Error> {
            self.stopped.store(true, Ordering::SeqCst);
            Box::pin(async { Ok("stopped") })
        }
    }

    #[derive(Clone)]
    struct DelayedStopHost {
        stopped: Arc<AtomicBool>,
        delay: Duration,
    }

    impl DelayedStopHost {
        fn new(delay: Duration) -> Self {
            Self {
                stopped: Arc::new(AtomicBool::new(false)),
                delay,
            }
        }
    }

    impl TaskHost for DelayedStopHost {
        type Status = MockStatus;
        type Exit = &'static str;
        type Error = io::Error;

        fn status(&self) -> Self::Status {
            MockStatus
        }

        fn stop(&mut self) -> TaskHostStopFuture<'_, Self::Exit, Self::Error> {
            self.stopped.store(true, Ordering::SeqCst);
            let delay = self.delay;
            Box::pin(async move {
                time::sleep(delay).await;
                Ok("stopped")
            })
        }
    }

    #[tokio::test]
    async fn injected_shutdown_signal_requests_a_graceful_stop() {
        let mut host = MockHost::new();
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let token =
            TaskHostControlToken::validate("0123456789abcdef0123456789abcdef".to_owned()).unwrap();

        let outcome = serve_host_with_shutdown_token(
            &mut host,
            listener,
            Duration::from_secs(60),
            |_| "status\n".to_owned(),
            |_, exit| format!("exit={exit}\n"),
            Ok(Box::pin(async { Ok(ShutdownSignal::CtrlC) })),
            &token,
        )
        .await
        .unwrap();

        assert!(matches!(
            outcome,
            TaskHostServeOutcome::StopRequested("stopped")
        ));
        assert!(host.stopped.load(Ordering::SeqCst));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn injected_sigterm_uses_the_same_graceful_stop_path() {
        let mut host = MockHost::new();
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let token =
            TaskHostControlToken::validate("0123456789abcdef0123456789abcdef".to_owned()).unwrap();

        let outcome = serve_host_with_shutdown_token(
            &mut host,
            listener,
            Duration::from_secs(60),
            |_| "status\n".to_owned(),
            |_, exit| format!("exit={exit}\n"),
            Ok(Box::pin(async { Ok(ShutdownSignal::Sigterm) })),
            &token,
        )
        .await
        .unwrap();

        assert!(matches!(
            outcome,
            TaskHostServeOutcome::StopRequested("stopped")
        ));
        assert!(host.stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn shutdown_registration_failure_is_typed_and_fail_closed() {
        let mut host = MockHost::new();
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let token =
            TaskHostControlToken::validate("0123456789abcdef0123456789abcdef".to_owned()).unwrap();

        let error = serve_host_with_shutdown_token(
            &mut host,
            listener,
            Duration::from_secs(60),
            |_| "status\n".to_owned(),
            |_, exit| format!("exit={exit}\n"),
            Err(TaskHostServeError::Shutdown(ShutdownSignalError::register(
                "SIGTERM",
                io::Error::from(io::ErrorKind::PermissionDenied),
            ))),
            &token,
        )
        .await
        .unwrap_err();

        let TaskHostServeError::Shutdown(error) = error else {
            panic!("expected a typed shutdown failure");
        };
        assert_eq!(error.signal(), "SIGTERM");
        assert_eq!(error.stage(), ShutdownSignalStage::Register);
        assert!(!host.stopped.load(Ordering::SeqCst));
    }

    #[test]
    fn missing_control_token_is_typed_without_touching_process_environment() {
        let error =
            TaskHostControlToken::from_env_result(Err(std::env::VarError::NotPresent)).unwrap_err();
        assert_eq!(
            error,
            TaskHostControlTokenError::Missing(TASK_CONTROL_TOKEN_ENV)
        );
    }

    #[tokio::test]
    async fn status_and_stop_require_a_matching_control_token() {
        let mut host = MockHost::new();
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let valid =
            TaskHostControlToken::validate("0123456789abcdef0123456789abcdef".to_owned()).unwrap();
        let server_token = valid.clone();
        let invalid =
            TaskHostControlToken::validate("fedcba9876543210fedcba9876543210".to_owned()).unwrap();

        let server = tokio::spawn(async move {
            serve_host_with_shutdown_token(
                &mut host,
                listener,
                Duration::from_millis(10),
                |_| "status\n".to_owned(),
                |_, exit| format!("exit={exit}\n"),
                Ok(Box::pin(std::future::pending::<
                    Result<ShutdownSignal, ShutdownSignalError>,
                >())),
                &server_token,
            )
            .await
        });

        let status = query_control_with_token(address, TaskHostControlCommand::Status, &invalid)
            .await
            .unwrap_err();
        assert!(matches!(status, TaskHostControlError::Unauthorized));

        let stop = query_control_with_token(address, TaskHostControlCommand::Stop, &invalid)
            .await
            .unwrap_err();
        assert!(matches!(stop, TaskHostControlError::Unauthorized));

        let response = query_control_with_token(address, TaskHostControlCommand::Stop, &valid)
            .await
            .unwrap();
        assert!(response.contains("exit=stopped"), "{response}");

        let outcome = server.await.unwrap().unwrap();
        assert!(matches!(
            outcome,
            TaskHostServeOutcome::StopRequested("stopped")
        ));
    }

    #[tokio::test]
    async fn stalled_connections_time_out_without_stopping_the_owner() {
        let mut host = MockHost::new();
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let token =
            TaskHostControlToken::validate("0123456789abcdef0123456789abcdef".to_owned()).unwrap();
        let server_token = token.clone();

        let server = tokio::spawn(async move {
            serve_host_with_shutdown_token(
                &mut host,
                listener,
                Duration::from_millis(10),
                |_| "status\n".to_owned(),
                |_, exit| format!("exit={exit}\n"),
                Ok(Box::pin(std::future::pending::<
                    Result<ShutdownSignal, ShutdownSignalError>,
                >())),
                &server_token,
            )
            .await
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(b"a").await.unwrap();
        time::sleep(TASK_CONTROL_READ_TIMEOUT + Duration::from_millis(50)).await;
        drop(client);

        let status = query_control_with_token(address, TaskHostControlCommand::Status, &token)
            .await
            .unwrap();
        assert!(status.contains("status"), "{status}");

        let stop = query_control_with_token(address, TaskHostControlCommand::Stop, &token)
            .await
            .unwrap();
        assert!(stop.contains("exit=stopped"), "{stop}");

        let outcome = server.await.unwrap().unwrap();
        assert!(matches!(
            outcome,
            TaskHostServeOutcome::StopRequested("stopped")
        ));
    }

    #[tokio::test]
    async fn oversized_control_lines_are_rejected_without_stopping_the_owner() {
        let mut host = MockHost::new();
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let token =
            TaskHostControlToken::validate("0123456789abcdef0123456789abcdef".to_owned()).unwrap();
        let server_token = token.clone();

        let server = tokio::spawn(async move {
            serve_host_with_shutdown_token(
                &mut host,
                listener,
                Duration::from_millis(10),
                |_| "status\n".to_owned(),
                |_, exit| format!("exit={exit}\n"),
                Ok(Box::pin(std::future::pending::<
                    Result<ShutdownSignal, ShutdownSignalError>,
                >())),
                &server_token,
            )
            .await
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        let oversized = "x".repeat(TASK_CONTROL_MAX_LINE_BYTES + 16);
        client
            .write_all(format!("auth {oversized}\nstatus\n").as_bytes())
            .await
            .unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).await.unwrap();
        assert_eq!(response.trim(), "error: unauthorized");

        let status = query_control_with_token(address, TaskHostControlCommand::Status, &token)
            .await
            .unwrap();
        assert!(status.contains("status"), "{status}");

        let stop = query_control_with_token(address, TaskHostControlCommand::Stop, &token)
            .await
            .unwrap();
        assert!(stop.contains("exit=stopped"), "{stop}");

        let outcome = server.await.unwrap().unwrap();
        assert!(matches!(
            outcome,
            TaskHostServeOutcome::StopRequested("stopped")
        ));
    }

    #[tokio::test]
    async fn query_control_times_out_when_the_peer_stalls_after_accepting() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let token =
            TaskHostControlToken::validate("0123456789abcdef0123456789abcdef".to_owned()).unwrap();

        let stalled = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            time::sleep(TASK_CONTROL_STATUS_TIMEOUT + Duration::from_millis(200)).await;
        });

        let error = query_control_with_token(address, TaskHostControlCommand::Status, &token)
            .await
            .unwrap_err();
        let TaskHostControlError::Io(error) = error else {
            panic!("expected transport timeout");
        };
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);

        stalled.await.unwrap();
    }

    #[tokio::test]
    async fn stop_queries_allow_a_graceful_response_that_exceeds_status_timeout() {
        let mut host =
            DelayedStopHost::new(TASK_CONTROL_STATUS_TIMEOUT + Duration::from_millis(200));
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let token =
            TaskHostControlToken::validate("0123456789abcdef0123456789abcdef".to_owned()).unwrap();
        let server_token = token.clone();

        let server = tokio::spawn(async move {
            serve_host_with_shutdown_token(
                &mut host,
                listener,
                Duration::from_millis(10),
                |_| "status\n".to_owned(),
                |_, exit| format!("exit={exit}\n"),
                Ok(Box::pin(std::future::pending::<
                    Result<ShutdownSignal, ShutdownSignalError>,
                >())),
                &server_token,
            )
            .await
        });

        let response = query_control_with_token(address, TaskHostControlCommand::Stop, &token)
            .await
            .unwrap();
        assert!(response.contains("exit=stopped"), "{response}");

        let outcome = server.await.unwrap().unwrap();
        assert!(matches!(
            outcome,
            TaskHostServeOutcome::StopRequested("stopped")
        ));
    }

    #[tokio::test]
    async fn stop_queries_still_time_out_when_the_peer_never_finishes_stopping() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let token =
            TaskHostControlToken::validate("0123456789abcdef0123456789abcdef".to_owned()).unwrap();

        let stalled = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 256];
            let _ = stream.read(&mut request).await.unwrap();
            time::sleep(Duration::from_millis(400)).await;
        });

        let error = query_control_with_token_timeout(
            address,
            TaskHostControlCommand::Stop,
            &token,
            Duration::from_millis(200),
        )
        .await
        .unwrap_err();
        let TaskHostControlError::Io(error) = error else {
            panic!("expected stop timeout to remain a transport timeout");
        };
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);

        stalled.await.unwrap();
    }

    #[tokio::test]
    async fn query_control_rejects_responses_that_exceed_the_bounded_limit() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let token =
            TaskHostControlToken::validate("0123456789abcdef0123456789abcdef".to_owned()).unwrap();

        let oversized = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 256];
            let _ = stream.read(&mut request).await.unwrap();
            let response = vec![b'x'; TASK_CONTROL_MAX_RESPONSE_BYTES + 1];
            stream.write_all(&response).await.unwrap();
            stream.shutdown().await.unwrap();
        });

        let error = query_control_with_token(address, TaskHostControlCommand::Status, &token)
            .await
            .unwrap_err();
        let TaskHostControlError::Io(error) = error else {
            panic!("expected oversized response to stay a transport failure");
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error
                .to_string()
                .contains(&TASK_CONTROL_MAX_RESPONSE_BYTES.to_string()),
            "{error}"
        );

        oversized.await.unwrap();
    }
}
