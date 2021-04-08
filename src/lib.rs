//! Crate `etcd` provides a client for [etcd](https://github.com/coreos/etcd), a distributed
//! key-value store from [CoreOS](https://coreos.com/).
//!
//! The client uses etcd's v2 API. Support for the v3 API is planned, and will be added via
//! separate types for backwards compatibility and to support both APIs simultaneously.
//!
//! The client uses asynchronous I/O, backed by the `futures` and `tokio` crates, and requires
//! both to be used alongside. Where possible, futures are returned using "impl Trait" instead of
//! boxing.
//!
//! The client is tested against etcd 2.3.8.
//!
//! # Usage
//!
//! `Client` is an HTTP client required for all API calls. It can be constructed to use HTTP or
//! HTTPS, and supports authenticating to the etcd cluster via HTTP basic authentication (username
//! and password) and/or X.509 client certificates.
//!
//! To get basic information about the health and versions of etcd running in a cluster, use the
//! `Client::health` and `Client::versions` methods, respectively. All other API calls are made by
//! passing a `Client` reference to the functions in the `auth`, `kv`, `members`, and `stats`
//! modules. These modules contain functions for API calls to the authentication and authorization
//! API, the primary key-value store API, the cluster membership API, and statistics API,
//! respectively.
//!
//! # Examples
//!
//! Basic usage:
//!
//! ```no_run
//! use etcd::Client;
//! use etcd::kv::{self, Action};
//!
//! async fn usage() {
//!     // Create a client to access a single cluster member. Addresses of multiple cluster
//!     // members can be provided and the client will try each one in sequence until it
//!     // receives a successful response.
//!     let client = Client::new(&["http://etcd.example.com:2379"]);
//!
//!     // Set the key "/foo" to the value "bar" with no expiration.
//!     assert!(kv::set(&client, "/foo", "bar", None).await.is_ok());
//!     // Once the key has been set, ask for details about it.
//!     let response = kv::get(&client, "/foo", kv::GetOptions::default()).await.unwrap();
//!     // The information returned tells you what kind of operation was performed.
//!     assert_eq!(response.data.action, Action::Get);
//!
//!     // The value of the key is what we set it to previously.
//!     assert_eq!(response.data.node.value, Some("bar".to_string()));
//!
//!     // Each API call also returns information about the etcd cluster extracted from
//!     // HTTP response headers.
//!     assert!(response.cluster_info.etcd_index.is_some());
//!
//! }
//! ```
//!
//! # Cargo features
//!
//! Crate `etcd` has one Cargo feature, `tls`, which adds HTTPS support via the `Client::https`
//! constructor. This feature is enabled by default.
#![deny(missing_debug_implementations, missing_docs, warnings)]

pub use crate::client::{Client, ClientBuilder, ClusterInfo, Health, Response};
pub use crate::error::{ApiError, Error};
pub use crate::version::VersionInfo;

pub mod auth;
pub mod kv;
pub mod members;
pub mod stats;

mod client;
mod error;
mod options;
mod version;
