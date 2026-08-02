use std::{
    future::Future,
    io,
    net::{Ipv4Addr, SocketAddr},
    path::Path,
    pin::Pin,
    time::Duration,
};

use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
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
) -> io::Result<String> {
    let mut stream = TcpStream::connect(address).await?;
    stream.write_all(command.as_wire().as_bytes()).await?;
    stream.shutdown().await?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await?;
    Ok(response)
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
    let mut shutdown = shutdown?;
    loop {
        tokio::select! {
            result = &mut shutdown => {
                result.map_err(TaskHostServeError::Shutdown)?;
                let exit = host.stop().await.map_err(TaskHostServeError::Task)?;
                return Ok(TaskHostServeOutcome::StopRequested(exit));
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted.map_err(TaskHostServeError::Io)?;
                if let Some(exit) =
                    handle_connection(host, stream, &render_status, &render_stop).await?
                {
                    return Ok(TaskHostServeOutcome::StopRequested(exit));
                }
            }
            () = time::sleep(poll_interval) => {
                let status = host.status();
                if status.is_terminal() {
                    return Ok(TaskHostServeOutcome::Terminal(status));
                }
            }
        }
    }
}

async fn handle_connection<H, FStatus, FStop>(
    host: &mut H,
    stream: TcpStream,
    render_status: &FStatus,
    render_stop: &FStop,
) -> Result<Option<H::Exit>, TaskHostServeError<H::Error>>
where
    H: TaskHost,
    FStatus: Fn(&H::Status) -> String,
    FStop: Fn(&H::Status, H::Exit) -> String,
{
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    let read = reader
        .read_line(&mut line)
        .await
        .map_err(TaskHostServeError::Io)?;
    let mut stream = reader.into_inner();
    if read == 0 {
        return Ok(None);
    }

    match TaskHostControlCommand::parse(&line) {
        Ok(TaskHostControlCommand::Status) => {
            write_response(&mut stream, &render_status(&host.status()))
                .await
                .map_err(TaskHostServeError::Io)?;
            Ok(None)
        }
        Ok(TaskHostControlCommand::Stop) => {
            let exit = host.stop().await.map_err(TaskHostServeError::Task)?;
            let status = host.status();
            write_response(&mut stream, &render_stop(&status, exit))
                .await
                .map_err(TaskHostServeError::Io)?;
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

    #[tokio::test]
    async fn injected_shutdown_signal_requests_a_graceful_stop() {
        let mut host = MockHost::new();
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();

        let outcome = serve_host_with_shutdown(
            &mut host,
            listener,
            Duration::from_secs(60),
            |_| "status\n".to_owned(),
            |_, exit| format!("exit={exit}\n"),
            Ok(Box::pin(async { Ok(ShutdownSignal::CtrlC) })),
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

        let outcome = serve_host_with_shutdown(
            &mut host,
            listener,
            Duration::from_secs(60),
            |_| "status\n".to_owned(),
            |_, exit| format!("exit={exit}\n"),
            Ok(Box::pin(async { Ok(ShutdownSignal::Sigterm) })),
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

        let error = serve_host_with_shutdown(
            &mut host,
            listener,
            Duration::from_secs(60),
            |_| "status\n".to_owned(),
            |_, exit| format!("exit={exit}\n"),
            Err(TaskHostServeError::Shutdown(ShutdownSignalError::register(
                "SIGTERM",
                io::Error::from(io::ErrorKind::PermissionDenied),
            ))),
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
}
