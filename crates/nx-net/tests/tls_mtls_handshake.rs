use std::time::Duration;

use nx_net::{TestPki, TlsConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

#[tokio::test]
async fn mtls_handshake_succeeds_with_valid_certs() {
    let pki = TestPki::generate().expect("generate test PKI");

    let server_cfg: TlsConfig = pki.node1_config();
    let client_cfg: TlsConfig = pki.node2_config();

    let acceptor = server_cfg.build_acceptor().expect("build_acceptor");
    let connector = client_cfg.build_connector().expect("build_connector");

    // Bind server on ephemeral port
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");

    // Server task: accept TCP, then TLS accept, then read/write
    let server_task = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.expect("accept tcp");
        let mut tls = acceptor.accept(tcp).await.expect("tls accept");

        let mut buf = [0u8; 1];
        tls.read_exact(&mut buf).await.expect("server read");
        assert_eq!(buf[0], 42);

        tls.write_all(&[43]).await.expect("server write");
    });

    // Client: connect TCP, then TLS connect, then write/read
    let client_task = tokio::spawn(async move {
        let tcp = TcpStream::connect(addr).await.expect("connect tcp");

        // rustls wants a ServerName; we can use "localhost" for tests.
        let server_name =
            rustls::pki_types::ServerName::try_from("localhost").expect("valid server name");

        let mut tls = connector
            .connect(server_name, tcp)
            .await
            .expect("tls connect");

        tls.write_all(&[42]).await.expect("client write");

        let mut buf = [0u8; 1];
        tls.read_exact(&mut buf).await.expect("client read");
        assert_eq!(buf[0], 43);
    });

    // Add timeouts to avoid hanging CI
    timeout(Duration::from_secs(5), server_task)
        .await
        .expect("server timeout")
        .expect("server join");
    timeout(Duration::from_secs(5), client_task)
        .await
        .expect("client timeout")
        .expect("client join");
}

#[tokio::test]
async fn mtls_handshake_fails_without_client_cert() {
    let pki = TestPki::generate().expect("generate test PKI");

    let server_cfg: TlsConfig = pki.node1_config();

    // Client config: we provide CA, but NO client cert/key => server should reject (mTLS)
    let client_cfg: TlsConfig = TlsConfig {
        ca_path: Some(pki.dir_path().join("ca.pem").to_string_lossy().to_string()),
        ..Default::default()
    };

    let acceptor = server_cfg.build_acceptor().expect("build_acceptor");
    let connector = client_cfg.build_connector().expect("build_connector");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");

    let server_task = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.expect("accept tcp");
        // This should fail during handshake because client has no cert
        let res = acceptor.accept(tcp).await;
        assert!(res.is_err(), "server expected mTLS handshake to fail");
    });

    let client_task = tokio::spawn(async move {
        let tcp = TcpStream::connect(addr).await.expect("connect tcp");

        let server_name =
            rustls::pki_types::ServerName::try_from("localhost").expect("valid server name");

        // Client side may also error; either side failing is ok.
        let _ = connector.connect(server_name, tcp).await;
    });

    timeout(Duration::from_secs(5), server_task)
        .await
        .expect("server timeout")
        .expect("server join");
    timeout(Duration::from_secs(5), client_task)
        .await
        .expect("client timeout")
        .expect("client join");
}

#[tokio::test]
async fn mtls_handshake_fails_with_invalid_client_cert() {
    let server_pki = TestPki::generate().expect("generate server PKI");
    let attacker_pki = TestPki::generate().expect("generate attacker PKI");

    // Server requires mTLS and trusts only server_pki's CA for client auth.
    let server_cfg: TlsConfig = server_pki.node1_config();

    // Client config:
    // - trusts the server CA (so server cert verification succeeds),
    // - but presents a client cert signed by a different CA (so client auth must fail).
    let client_cfg: TlsConfig = TlsConfig {
        ca_path: Some(
            server_pki
                .dir_path()
                .join("ca.pem")
                .to_string_lossy()
                .to_string(),
        ),
        cert_path: Some(
            attacker_pki
                .dir_path()
                .join("node2.pem")
                .to_string_lossy()
                .to_string(),
        ),
        key_path: Some(
            attacker_pki
                .dir_path()
                .join("node2-key.pem")
                .to_string_lossy()
                .to_string(),
        ),
        ..Default::default()
    };

    let acceptor = server_cfg.build_acceptor().expect("build_acceptor");
    let connector = client_cfg.build_connector().expect("build_connector");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");

    let server_task = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.expect("accept tcp");

        // This should fail because the client certificate is not signed by the CA
        // configured on the server for mTLS verification.
        let res = acceptor.accept(tcp).await;
        assert!(res.is_err(), "server expected mTLS handshake to fail");
    });

    let client_task = tokio::spawn(async move {
        let tcp = TcpStream::connect(addr).await.expect("connect tcp");

        let server_name =
            rustls::pki_types::ServerName::try_from("localhost").expect("valid server name");

        // The client should be able to verify the server (correct CA),
        // but the handshake must still fail because the server rejects the client cert.
        let _ = connector.connect(server_name, tcp).await;
    });

    timeout(Duration::from_secs(5), server_task)
        .await
        .expect("server timeout")
        .expect("server join");
    timeout(Duration::from_secs(5), client_task)
        .await
        .expect("client timeout")
        .expect("client join");
}
