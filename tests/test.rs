use std::io::Read;
use std::{fs::File, future::Future};

use etcd::{kv, Client, ClientBuilder};
use reqwest::{Certificate, Identity};
use tokio::runtime::Runtime;

/// Wrapper around Client that automatically cleans up etcd after each test.
pub struct TestClient {
    client: Client,
    run_destructor: bool,
    runtime: Runtime,
}

impl TestClient {
    /// Creates a new client for a test.
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            client: Client::new(&["http://etcd:2379"]),
            run_destructor: true,
            runtime: Runtime::new().expect("failed to create Tokio runtime"),
        }
    }

    /// Creates a new client for a test that will not clean up the key space afterwards.
    #[allow(dead_code)]
    pub fn no_destructor() -> Self {
        let mut client = Self::new();
        client.run_destructor = false;
        client
    }

    /// Creates a new HTTPS client for a test.
    #[allow(dead_code)]
    pub fn https(use_client_cert: bool) -> TestClient {
        let client_builder = ClientBuilder::new(&["https://etcdsecure:2379"]);

        let mut ca_cert_file = File::open("/source/tests/ssl/ca.der").unwrap();
        let mut ca_cert_buffer = Vec::new();
        ca_cert_file.read_to_end(&mut ca_cert_buffer).unwrap();

        let certificate = Certificate::from_der(&ca_cert_buffer).unwrap();
        let client_builder = client_builder.with_root_certificate(certificate);

        let client_builder = if use_client_cert {
            let mut pkcs12_file = File::open("/source/tests/ssl/client.p12").unwrap();
            let mut pkcs12_buffer = Vec::new();
            pkcs12_file.read_to_end(&mut pkcs12_buffer).unwrap();

            let identity = Identity::from_pkcs12_der(&pkcs12_buffer, "secret").unwrap();
            client_builder.with_client_identity(identity)
        } else {
            client_builder
        };

        TestClient {
            client: client_builder.build(),
            run_destructor: true,
            runtime: Runtime::new().expect("failed to create Tokio runtime"),
        }
    }
}

impl TestClient {
    #[allow(dead_code)]
    pub fn run<'a, F, U, R>(&'a self, func: F) -> R
    where
        F: FnOnce(&'a Client) -> U,
        U: Future<Output = R> + 'a,
    {
        self.runtime.block_on((func)(&self.client))
    }
}

impl Drop for TestClient {
    fn drop(&mut self) {
        if self.run_destructor {
            self.runtime.block_on(async {
                kv::delete(&self.client, "/test", true).await.ok();
            });
        }
    }
}
