use grpcio;
use std::sync::Arc;

use super::backend::{BackendConfiguration, KeyManager};
use ekiden_common::error::Result;
use ekiden_common::identity::NodeIdentity;
use ekiden_rpc_api;

/// Key manager node configuration.
pub struct KeyManagerConfiguration {
    /// gRPC server port.
    pub port: u16,
    /// Backend configuration.
    pub backend: BackendConfiguration,
    /// gRPC environment
    pub environment: Arc<grpcio::Environment>,
    /// Credentials.
    pub identity: Arc<NodeIdentity>,
}

/// Key manager node.
pub struct KeyManagerNode {
    /// gRPC server.
    server: grpcio::Server,
}

impl KeyManagerNode {
    /// Create a key manger node.
    pub fn new(config: KeyManagerConfiguration) -> Result<Self> {
        // Create worker.
        let grpc_service = ekiden_rpc_api::create_enclave_rpc(KeyManager::new(config.backend));

        // Create gRPC server.
        let server = grpcio::ServerBuilder::new(config.environment)
            .register_service(grpc_service)
            .bind_secure(
                "0.0.0.0",
                config.port,
                grpcio::ServerCredentialsBuilder::new()
                    .add_cert(
                        config.identity.get_tls_certificate().get_pem()?,
                        config.identity.get_tls_private_key().get_pem()?,
                    )
                    .build(),
            )
            .build()?;

        Ok(Self { server })
    }

    /// Start the key manager node.
    pub fn start(&mut self) {
        // Start gRPC server.
        self.server.start();

        for &(ref host, port) in self.server.bind_addrs() {
            info!("Key manager node listening on {}:{}", host, port);
        }
    }
}