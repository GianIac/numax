use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use http_body_util::Limited;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use serde::Serialize;
use subtle::ConstantTimeEq;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::timeout;
use tower::Service;

pub const DEFAULT_MANAGEMENT_LISTEN: &str = "127.0.0.1:9102";
pub const DEFAULT_MANAGEMENT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
pub const MAX_MANAGEMENT_REQUEST_BODY_SIZE: usize = 16 * 1024 * 1024;
pub const MAX_MANAGEMENT_CONCURRENT_REQUESTS: usize = 64;

#[derive(Clone)]
pub struct ManagementConfig {
    listen_addr: SocketAddr,
    authorization: Arc<[u8]>,
    request_timeout: Duration,
    request_body_limit: usize,
    concurrent_request_limit: usize,
    allow_non_loopback: bool,
}

impl ManagementConfig {
    pub fn new(listen_addr: &str, bearer_token: &str, allow_non_loopback: bool) -> Result<Self> {
        let listen_addr = listen_addr
            .parse::<SocketAddr>()
            .map_err(|error| anyhow!("invalid management listen address: {error}"))?;
        if !listen_addr.ip().is_loopback() && !allow_non_loopback {
            bail!(
                "management.listen must be loopback unless management.allow_non_loopback is true"
            );
        }
        validate_bearer_token(bearer_token)?;

        Ok(Self {
            listen_addr,
            authorization: Arc::from(format!("Bearer {bearer_token}").into_bytes()),
            request_timeout: DEFAULT_MANAGEMENT_REQUEST_TIMEOUT,
            request_body_limit: MAX_MANAGEMENT_REQUEST_BODY_SIZE,
            concurrent_request_limit: MAX_MANAGEMENT_CONCURRENT_REQUESTS,
            allow_non_loopback,
        })
    }

    pub fn with_request_timeout(mut self, request_timeout: Duration) -> Result<Self> {
        if request_timeout.is_zero() {
            bail!("management request timeout must be greater than zero");
        }
        self.request_timeout = request_timeout;
        Ok(self)
    }

    pub fn with_request_body_limit(mut self, request_body_limit: usize) -> Result<Self> {
        if !(1..=MAX_MANAGEMENT_REQUEST_BODY_SIZE).contains(&request_body_limit) {
            bail!(
                "management request body limit must be between 1 and {MAX_MANAGEMENT_REQUEST_BODY_SIZE} bytes"
            );
        }
        self.request_body_limit = request_body_limit;
        Ok(self)
    }

    pub fn with_concurrent_request_limit(
        mut self,
        concurrent_request_limit: usize,
    ) -> Result<Self> {
        if !(1..=MAX_MANAGEMENT_CONCURRENT_REQUESTS).contains(&concurrent_request_limit) {
            bail!(
                "management concurrent request limit must be between 1 and {MAX_MANAGEMENT_CONCURRENT_REQUESTS}"
            );
        }
        self.concurrent_request_limit = concurrent_request_limit;
        Ok(self)
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }

    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    pub fn allows_non_loopback(&self) -> bool {
        self.allow_non_loopback
    }
}

impl fmt::Debug for ManagementConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ManagementConfig")
            .field("listen_addr", &self.listen_addr)
            .field("bearer_token", &"[REDACTED]")
            .field("request_timeout", &self.request_timeout)
            .field("request_body_limit", &self.request_body_limit)
            .field("concurrent_request_limit", &self.concurrent_request_limit)
            .field("allow_non_loopback", &self.allow_non_loopback)
            .finish()
    }
}

fn validate_bearer_token(token: &str) -> Result<()> {
    if token.is_empty() {
        bail!("management bearer token must not be empty");
    }
    if !token.is_ascii()
        || token
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
    {
        bail!("management bearer token must contain only non-whitespace ASCII characters");
    }
    Ok(())
}

#[derive(Clone)]
struct AuthState {
    authorization: Arc<[u8]>,
}

#[derive(Clone, Copy)]
struct TimeoutState(Duration);

#[derive(Clone, Copy)]
struct BodyLimitState(usize);

#[derive(Clone)]
struct ConcurrencyState(Arc<Semaphore>);

#[derive(Serialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Serialize)]
struct ErrorBody {
    code: &'static str,
    message: &'static str,
}

pub struct ManagementServer {
    local_addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<Duration>>,
    task: JoinHandle<Result<()>>,
}

impl ManagementServer {
    pub async fn start(config: ManagementConfig) -> Result<Self> {
        Self::start_with_router(config, Router::new().fallback(StatusCode::NOT_FOUND)).await
    }

    async fn start_with_router(config: ManagementConfig, router: Router) -> Result<Self> {
        let listener = TcpListener::bind(config.listen_addr()).await?;
        let local_addr = listener.local_addr()?;
        let auth = AuthState {
            authorization: config.authorization,
        };
        let header_timeout = config.request_timeout;
        let request_timeout = TimeoutState(header_timeout);
        let request_body_limit = BodyLimitState(config.request_body_limit);
        let concurrency =
            ConcurrencyState(Arc::new(Semaphore::new(config.concurrent_request_limit)));
        let router = router
            .layer(middleware::from_fn_with_state(
                request_body_limit,
                enforce_request_body_limit,
            ))
            .layer(middleware::from_fn_with_state(
                request_timeout,
                enforce_request_timeout,
            ))
            .layer(middleware::from_fn_with_state(
                concurrency,
                enforce_concurrency_limit,
            ))
            .layer(middleware::from_fn_with_state(auth, require_bearer));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(run_server(listener, router, header_timeout, shutdown_rx));

        tracing::info!(addr = %local_addr, "management API listener started");
        Ok(Self {
            local_addr,
            shutdown_tx: Some(shutdown_tx),
            task,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn shutdown(mut self, shutdown_timeout: Duration) -> Result<()> {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(shutdown_timeout);
        }

        self.task
            .await
            .map_err(|error| anyhow!("management server task failed: {error}"))??;
        tracing::info!("management API listener stopped");
        Ok(())
    }
}

async fn run_server(
    listener: TcpListener,
    router: Router,
    header_timeout: Duration,
    mut shutdown_rx: oneshot::Receiver<Duration>,
) -> Result<()> {
    let (graceful_tx, graceful_rx) = watch::channel(false);
    let mut connections = JoinSet::new();

    let shutdown_timeout = loop {
        tokio::select! {
            shutdown_timeout = &mut shutdown_rx => {
                break shutdown_timeout.unwrap_or(Duration::ZERO);
            }
            accepted = listener.accept() => match accepted {
                Ok((stream, peer_addr)) => {
                    connections.spawn(serve_connection(
                        stream,
                        peer_addr,
                        router.clone(),
                        header_timeout,
                        graceful_rx.clone(),
                    ));
                }
                Err(error) => {
                    tracing::warn!(%error, "management API accept failed");
                    tokio::select! {
                        shutdown_timeout = &mut shutdown_rx => {
                            break shutdown_timeout.unwrap_or(Duration::ZERO);
                        }
                        () = tokio::time::sleep(Duration::from_secs(1)) => {}
                    }
                }
            },
            joined = connections.join_next(), if !connections.is_empty() => {
                log_connection_result(joined);
            }
        }
    };

    drop(listener);
    let _ = graceful_tx.send(true);

    let drain = async {
        while let Some(joined) = connections.join_next().await {
            log_connection_result(Some(joined));
        }
    };
    if timeout(shutdown_timeout, drain).await.is_err() {
        connections.abort_all();
        while connections.join_next().await.is_some() {}
        bail!("management API shutdown timed out after {shutdown_timeout:?}");
    }

    Ok(())
}

async fn serve_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    router: Router,
    header_timeout: Duration,
    mut graceful_rx: watch::Receiver<bool>,
) {
    let service = service_fn(move |request: hyper::Request<Incoming>| {
        let mut router = router.clone();
        async move { router.call(request.map(Body::new)).await }
    });
    let mut builder = http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .header_read_timeout(header_timeout);
    let connection = builder.serve_connection(TokioIo::new(stream), service);
    tokio::pin!(connection);

    tokio::select! {
        result = connection.as_mut() => {
            if let Err(error) = result {
                tracing::debug!(%peer_addr, %error, "management API connection ended with an error");
            }
        }
        changed = graceful_rx.changed() => {
            if changed.is_ok() && *graceful_rx.borrow() {
                connection.as_mut().graceful_shutdown();
                if let Err(error) = connection.await {
                    tracing::debug!(%peer_addr, %error, "management API connection ended with an error during shutdown");
                }
            }
        }
    }
}

fn log_connection_result(joined: Option<std::result::Result<(), tokio::task::JoinError>>) {
    if let Some(Err(error)) = joined
        && !error.is_cancelled()
    {
        tracing::warn!(%error, "management API connection task failed");
    }
}

async fn enforce_request_timeout(
    State(state): State<TimeoutState>,
    request: Request,
    next: Next,
) -> Response {
    match timeout(state.0, next.run(request)).await {
        Ok(response) => response,
        Err(_) => error_response(
            StatusCode::GATEWAY_TIMEOUT,
            "request_timeout",
            "request timed out",
        ),
    }
}

async fn enforce_request_body_limit(
    State(state): State<BodyLimitState>,
    request: Request,
    next: Next,
) -> Response {
    if request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|length| length > state.0 as u64)
    {
        return payload_too_large_response();
    }

    let request = request.map(|body| Body::new(Limited::new(body, state.0)));
    let response = next.run(request).await;
    if response.status() == StatusCode::PAYLOAD_TOO_LARGE {
        payload_too_large_response()
    } else {
        response
    }
}

async fn enforce_concurrency_limit(
    State(state): State<ConcurrencyState>,
    request: Request,
    next: Next,
) -> Response {
    let Ok(_permit) = state.0.try_acquire_owned() else {
        let mut response = error_response(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            "management API concurrency limit reached",
        );
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        return response;
    };

    next.run(request).await
}

async fn require_bearer(State(state): State<AuthState>, request: Request, next: Next) -> Response {
    let authorized = request
        .headers()
        .get(header::AUTHORIZATION)
        .is_some_and(|value| bool::from(value.as_bytes().ct_eq(state.authorization.as_ref())));

    if !authorized {
        let mut response = error_response(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "bearer authentication required",
        );
        response
            .headers_mut()
            .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        return response;
    }

    next.run(request).await
}

fn payload_too_large_response() -> Response {
    error_response(
        StatusCode::PAYLOAD_TOO_LARGE,
        "payload_too_large",
        "request body exceeds the configured limit",
    )
}

fn error_response(status: StatusCode, code: &'static str, message: &'static str) -> Response {
    (
        status,
        Json(ErrorEnvelope {
            error: ErrorBody { code, message },
        }),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Bytes;
    use axum::routing::{get, post};
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::Notify;

    async fn raw_request(addr: SocketAddr, request: &[u8]) -> String {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.write_all(request).await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
    }

    async fn request(addr: SocketAddr, path: &str, authorization: Option<&str>) -> String {
        let authorization = authorization
            .map(|value| format!("Authorization: {value}\r\n"))
            .unwrap_or_default();
        let request = format!(
            "GET {path} HTTP/1.1\r\nHost: {addr}\r\n{authorization}Connection: close\r\n\r\n"
        );
        raw_request(addr, request.as_bytes()).await
    }

    #[test]
    fn config_redacts_token_and_rejects_external_bind_without_opt_in() {
        let config = ManagementConfig::new("127.0.0.1:9102", "top-secret", false).unwrap();
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("top-secret"));
        assert!(rendered.contains("[REDACTED]"));
        assert!(rendered.contains("request_body_limit: 16777216"));
        assert!(rendered.contains("concurrent_request_limit: 64"));

        let error = ManagementConfig::new("0.0.0.0:9102", "top-secret", false).unwrap_err();
        assert!(error.to_string().contains("allow_non_loopback"));
        ManagementConfig::new("0.0.0.0:9102", "top-secret", true).unwrap();
    }

    #[test]
    fn config_rejects_zero_or_above_contract_transport_limits() {
        let config = || ManagementConfig::new("127.0.0.1:9102", "top-secret", false).unwrap();

        assert!(config().with_request_body_limit(0).is_err());
        assert!(
            config()
                .with_request_body_limit(MAX_MANAGEMENT_REQUEST_BODY_SIZE + 1)
                .is_err()
        );
        assert!(config().with_concurrent_request_limit(0).is_err());
        assert!(
            config()
                .with_concurrent_request_limit(MAX_MANAGEMENT_CONCURRENT_REQUESTS + 1)
                .is_err()
        );
    }

    #[tokio::test]
    async fn server_requires_bearer_authentication_and_shuts_down() {
        let config = ManagementConfig::new("127.0.0.1:0", "top-secret", false).unwrap();
        let server = ManagementServer::start(config).await.unwrap();
        let addr = server.local_addr();

        let unauthorized = request(addr, "/api/v1/health", None).await;
        assert!(unauthorized.starts_with("HTTP/1.1 401 Unauthorized"));
        assert!(unauthorized.contains("content-type: application/json"));
        assert!(unauthorized.contains(
            r#"{"error":{"code":"unauthorized","message":"bearer authentication required"}}"#
        ));
        let authorized = request(addr, "/api/v1/health", Some("Bearer top-secret")).await;
        assert!(authorized.starts_with("HTTP/1.1 404 Not Found"));

        server.shutdown(Duration::from_secs(1)).await.unwrap();
        assert!(tokio::net::TcpStream::connect(addr).await.is_err());
    }

    #[tokio::test]
    async fn start_fails_when_the_port_is_already_in_use() {
        let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = occupied.local_addr().unwrap();
        let config = ManagementConfig::new(&addr.to_string(), "top-secret", false).unwrap();

        assert!(ManagementServer::start(config).await.is_err());
    }

    #[tokio::test]
    async fn shutdown_drains_an_active_request() {
        let config = ManagementConfig::new("127.0.0.1:0", "top-secret", false).unwrap();
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let handler_started = started.clone();
        let handler_release = release.clone();
        let router = Router::new().route(
            "/hold",
            get(move || {
                let started = handler_started.clone();
                let release = handler_release.clone();
                async move {
                    started.notify_one();
                    release.notified().await;
                    StatusCode::NO_CONTENT
                }
            }),
        );
        let server = ManagementServer::start_with_router(config, router)
            .await
            .unwrap();
        let addr = server.local_addr();
        let request =
            tokio::spawn(async move { request(addr, "/hold", Some("Bearer top-secret")).await });
        started.notified().await;

        let shutdown = tokio::spawn(server.shutdown(Duration::from_secs(1)));
        tokio::task::yield_now().await;
        assert!(!shutdown.is_finished());
        release.notify_one();

        shutdown.await.unwrap().unwrap();
        let response = request.await.unwrap();
        assert!(response.starts_with("HTTP/1.1 204 No Content"));
    }

    #[tokio::test]
    async fn shutdown_aborts_an_active_request_after_the_deadline() {
        struct DropFlag(Arc<AtomicBool>);

        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let config = ManagementConfig::new("127.0.0.1:0", "top-secret", false)
            .unwrap()
            .with_request_timeout(Duration::from_secs(5))
            .unwrap();
        let started = Arc::new(Notify::new());
        let dropped = Arc::new(AtomicBool::new(false));
        let handler_started = started.clone();
        let handler_dropped = dropped.clone();
        let router = Router::new().route(
            "/hold",
            get(move || {
                let started = handler_started.clone();
                let dropped = handler_dropped.clone();
                async move {
                    let _drop_flag = DropFlag(dropped);
                    started.notify_one();
                    std::future::pending::<StatusCode>().await
                }
            }),
        );
        let server = ManagementServer::start_with_router(config, router)
            .await
            .unwrap();
        let addr = server.local_addr();
        let request =
            tokio::spawn(async move { request(addr, "/hold", Some("Bearer top-secret")).await });
        started.notified().await;

        let error = server
            .shutdown(Duration::from_millis(20))
            .await
            .unwrap_err();

        assert!(error.to_string().contains("shutdown timed out"));
        assert!(dropped.load(Ordering::SeqCst));
        request.abort();
    }

    #[tokio::test]
    async fn header_read_timeout_closes_an_incomplete_request() {
        let config = ManagementConfig::new("127.0.0.1:0", "top-secret", false)
            .unwrap()
            .with_request_timeout(Duration::from_millis(20))
            .unwrap()
            .with_concurrent_request_limit(1)
            .unwrap();
        let server = ManagementServer::start(config).await.unwrap();
        let mut stream = tokio::net::TcpStream::connect(server.local_addr())
            .await
            .unwrap();
        stream
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nAuthorization:")
            .await
            .unwrap();

        let mut response = String::new();
        timeout(Duration::from_secs(1), stream.read_to_string(&mut response))
            .await
            .expect("connection should close after the header timeout")
            .unwrap();

        assert!(response.is_empty());
        server.shutdown(Duration::from_secs(1)).await.unwrap();
    }

    #[tokio::test]
    async fn request_body_limit_accepts_the_boundary_and_rejects_larger_bodies() {
        let config = ManagementConfig::new("127.0.0.1:0", "top-secret", false)
            .unwrap()
            .with_request_body_limit(8)
            .unwrap();
        let router = Router::new().route(
            "/upload",
            post(|body: Bytes| async move {
                assert_eq!(body.len(), 8);
                StatusCode::NO_CONTENT
            }),
        );
        let server = ManagementServer::start_with_router(config, router)
            .await
            .unwrap();
        let addr = server.local_addr();

        let boundary = format!(
            "POST /upload HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer top-secret\r\nContent-Length: 8\r\nConnection: close\r\n\r\n12345678"
        );
        let accepted = raw_request(addr, boundary.as_bytes()).await;
        assert!(accepted.starts_with("HTTP/1.1 204 No Content"));

        let oversized = format!(
            "POST /upload HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer top-secret\r\nContent-Length: 9\r\nConnection: close\r\n\r\n123456789"
        );
        let rejected = raw_request(addr, oversized.as_bytes()).await;
        assert_payload_too_large(&rejected);

        server.shutdown(Duration::from_secs(1)).await.unwrap();
    }

    #[tokio::test]
    async fn request_body_limit_rejects_oversized_chunked_bodies() {
        let config = ManagementConfig::new("127.0.0.1:0", "top-secret", false)
            .unwrap()
            .with_request_body_limit(8)
            .unwrap();
        let router =
            Router::new().route("/upload", post(|_: Bytes| async { StatusCode::NO_CONTENT }));
        let server = ManagementServer::start_with_router(config, router)
            .await
            .unwrap();
        let addr = server.local_addr();
        let request = format!(
            "POST /upload HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer top-secret\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n9\r\n123456789\r\n0\r\n\r\n"
        );

        let response = raw_request(addr, request.as_bytes()).await;

        assert_payload_too_large(&response);
        server.shutdown(Duration::from_secs(1)).await.unwrap();
    }

    #[tokio::test]
    async fn routed_request_timeout_cancels_the_handler_and_returns_json() {
        struct DropFlag(Arc<AtomicBool>);

        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let config = ManagementConfig::new("127.0.0.1:0", "top-secret", false)
            .unwrap()
            .with_request_timeout(Duration::from_millis(20))
            .unwrap();
        let started = Arc::new(Notify::new());
        let dropped = Arc::new(AtomicBool::new(false));
        let handler_started = started.clone();
        let handler_dropped = dropped.clone();
        let router = Router::new()
            .route(
                "/hold",
                get(move || {
                    let started = handler_started.clone();
                    let dropped = handler_dropped.clone();
                    async move {
                        let _drop_flag = DropFlag(dropped);
                        started.notify_one();
                        std::future::pending::<StatusCode>().await
                    }
                }),
            )
            .route("/quick", get(|| async { StatusCode::NO_CONTENT }));
        let server = ManagementServer::start_with_router(config, router)
            .await
            .unwrap();
        let addr = server.local_addr();
        let response =
            tokio::spawn(async move { request(addr, "/hold", Some("Bearer top-secret")).await });
        started.notified().await;

        let response = response.await.unwrap();

        assert!(response.starts_with("HTTP/1.1 504 Gateway Timeout"));
        assert!(
            response
                .contains(r#"{"error":{"code":"request_timeout","message":"request timed out"}}"#)
        );
        assert!(dropped.load(Ordering::SeqCst));
        let recovered = request(addr, "/quick", Some("Bearer top-secret")).await;
        assert!(recovered.starts_with("HTTP/1.1 204 No Content"));
        server.shutdown(Duration::from_secs(1)).await.unwrap();
    }

    #[tokio::test]
    async fn concurrency_limit_rejects_immediately_and_recovers_the_slot() {
        let config = ManagementConfig::new("127.0.0.1:0", "top-secret", false)
            .unwrap()
            .with_concurrent_request_limit(1)
            .unwrap();
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let handler_started = started.clone();
        let handler_release = release.clone();
        let router = Router::new()
            .route(
                "/hold",
                get(move || {
                    let started = handler_started.clone();
                    let release = handler_release.clone();
                    async move {
                        started.notify_one();
                        release.notified().await;
                        StatusCode::NO_CONTENT
                    }
                }),
            )
            .route("/quick", get(|| async { StatusCode::NO_CONTENT }));
        let server = ManagementServer::start_with_router(config, router)
            .await
            .unwrap();
        let addr = server.local_addr();
        let held =
            tokio::spawn(async move { request(addr, "/hold", Some("Bearer top-secret")).await });
        started.notified().await;

        let unauthorized = request(addr, "/quick", None).await;
        assert!(unauthorized.starts_with("HTTP/1.1 401 Unauthorized"));
        let rejected = request(addr, "/quick", Some("Bearer top-secret")).await;
        assert!(rejected.starts_with("HTTP/1.1 429 Too Many Requests"));
        assert!(rejected.contains("retry-after: 1"));
        assert!(rejected.contains(
            r#"{"error":{"code":"rate_limited","message":"management API concurrency limit reached"}}"#
        ));

        release.notify_one();
        assert!(held.await.unwrap().starts_with("HTTP/1.1 204 No Content"));
        let recovered = request(addr, "/quick", Some("Bearer top-secret")).await;
        assert!(recovered.starts_with("HTTP/1.1 204 No Content"));
        server.shutdown(Duration::from_secs(1)).await.unwrap();
    }

    fn assert_payload_too_large(response: &str) {
        assert!(response.starts_with("HTTP/1.1 413 Payload Too Large"));
        assert!(response.contains("content-type: application/json"));
        assert!(response.contains(
            r#"{"error":{"code":"payload_too_large","message":"request body exceeds the configured limit"}}"#
        ));
    }
}
