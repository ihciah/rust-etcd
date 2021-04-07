//! Contains the etcd client. All API calls are made via the client.

use std::sync::Arc;

use futures::Future;
use http::{
    header::{HeaderMap, HeaderValue},
    StatusCode, Uri,
};
use log::error;
use reqwest::IntoUrl;
use serde::de::DeserializeOwned;
use serde_derive::{Deserialize, Serialize};
use serde_json;

use crate::error::{ApiError, Error};

const XETCD_CLUSTER_ID: &str = "X-Etcd-Cluster-Id";
const XETCD_INDEX: &str = "X-Etcd-Index";
const XRAFT_INDEX: &str = "X-Raft-Index";
const XRAFT_TERM: &str = "X-Raft-Term";

/// API client for etcd.
///
/// All API calls require a client.
#[derive(Clone, Debug)]
pub struct Client {
    endpoints: Arc<Vec<Uri>>,
    http_client: reqwest::Client,
}

/// A username and password to use for HTTP basic authentication.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BasicAuth {
    /// The username to use for authentication.
    pub username: String,
    /// The password to use for authentication.
    pub password: String,
}

/// A value returned by the health check API endpoint to indicate a healthy cluster member.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Health {
    /// The health status of the cluster member.
    pub health: String,
}

impl Client {
    /// Constructs a new client using the HTTP protocol.
    ///
    /// # Parameters
    ///
    /// * endpoints: URLs for one or more cluster members. When making an API call, the client will
    /// make the call to each member in order until it receives a successful respponse.
    /// * basic_auth: Credentials for HTTP basic authentication.
    ///
    /// # Errors
    ///
    /// Panics if no endpoints are provided or if any of the endpoints is an invalid URL.
    pub fn new<T: Into<String>>(endpoints: &[&str], basic_auth: Option<BasicAuth>) -> Self {
        if endpoints.is_empty() {
            panic!("invariant: no endpoints provided")
        }

        let endpoints = Arc::new(
            endpoints
                .into_iter()
                .map(|e| {
                    e.parse()
                        .expect(&format!("invariant: could not parse endpoint: {}", e))
                })
                .collect(),
        );

        let client_builder = reqwest::ClientBuilder::new();
        let client_builder = match basic_auth {
            Some(auth) => {
                let mut headers = HeaderMap::new();
                let basic_auth = base64::encode(format!("{}:{}", auth.username, auth.password));
                headers.insert(
                    reqwest::header::AUTHORIZATION,
                    HeaderValue::from_str(&format!("Basic {}", basic_auth))
                        .expect("invariant: could not create basic auth header."),
                );
                client_builder.default_headers(headers)
            }
            None => client_builder,
        };

        let http_client = client_builder
            .build()
            .expect("invariant: could not create http client");

        Self {
            endpoints,
            http_client,
        }
    }

    /// Lets other internal code access the `HttpClient`.
    pub(crate) fn http_client(&self) -> &reqwest::Client {
        &self.http_client
    }

    /// Lets other internal code access the cluster endpoints.
    pub(crate) fn endpoints(&self) -> &[Uri] {
        &self.endpoints
    }

    /// Runs a basic health check against each etcd member.
    pub fn health(&self) -> () {
        // let futures = self.endpoints.iter().map(|endpoint| {
        //     let url = build_url(&endpoint, "health");
        //     let uri = url.parse().map_err(Error::from).into_future();
        //     let cloned_client = self.http_client.clone();
        //     let response = uri.and_then(move |uri| cloned_client.get(uri).map_err(Error::from));
        //     response.and_then(|response| {
        //         let status = response.status();
        //         let cluster_info = ClusterInfo::from(response.headers());
        //         let body = response.into_body().concat2().map_err(Error::from);

        //         body.and_then(move |ref body| {
        //             if status == StatusCode::OK {
        //                 match serde_json::from_slice::<Health>(body) {
        //                     Ok(data) => Ok(Response { data, cluster_info }),
        //                     Err(error) => Err(Error::Serialization(error)),
        //                 }
        //             } else {
        //                 match serde_json::from_slice::<ApiError>(body) {
        //                     Ok(error) => Err(Error::Api(error)),
        //                     Err(error) => Err(Error::Serialization(error)),
        //                 }
        //             }
        //         })
        //     })
        // });

        // futures_unordered(futures)

        todo!()
    }

    /// Returns version information from each etcd cluster member the client was initialized with.
    pub fn versions(&self) -> () {
        todo!();
        // let futures = self.endpoints.iter().map(|endpoint| {
        //     let url = build_url(&endpoint, "version");
        //     let uri = url.parse().map_err(Error::from).into_future();
        //     let cloned_client = self.http_client.clone();
        //     let response = uri.and_then(move |uri| cloned_client.get(uri).map_err(Error::from));
        //     response.and_then(|response| {
        //         let status = response.status();
        //         let cluster_info = ClusterInfo::from(response.headers());
        //         let body = response.into_body().concat2().map_err(Error::from);

        //         body.and_then(move |ref body| {
        //             if status == StatusCode::OK {
        //                 match serde_json::from_slice::<VersionInfo>(body) {
        //                     Ok(data) => Ok(Response { data, cluster_info }),
        //                     Err(error) => Err(Error::Serialization(error)),
        //                 }
        //             } else {
        //                 match serde_json::from_slice::<ApiError>(body) {
        //                     Ok(error) => Err(Error::Api(error)),
        //                     Err(error) => Err(Error::Serialization(error)),
        //                 }
        //             }
        //         })
        //     })
        // });

        // futures_unordered(futures)
        todo!()
    }

    pub(crate) async fn first_ok<'a, H, F, T, E>(&'a self, handler: H) -> Result<T, Vec<E>>
    where
        F: Future<Output = Result<T, E>> + 'a,
        H: Fn(&'a Client, &'a Uri) -> F,
    {
        let mut errors = Vec::new();

        // JAKE-TODO: Randomly shuffle the endpoints.
        for endpoint in self.endpoints.iter() {
            let result = (handler)(&self, endpoint).await;
            match result {
                Ok(response) => return Ok(response),
                Err(err) => errors.push(err),
            }
        }

        Err(errors)
    }

    /// Attempts to issue a GET request to the given path on all endpoints, returning the result of the first successful request.
    pub(crate) async fn request_first_ok<T, P>(&self, path: P) -> Result<Response<T>, Error>
    where
        P: AsRef<str>,
        T: DeserializeOwned,
    {
        let path = path.as_ref();
        let result = self
            .first_ok(|client, endpoint| client.request(format!("{}{}", endpoint, path)))
            .await;

        match result {
            Ok(response) => Ok(response),
            Err(errors) => Err(errors
                .into_iter()
                .next()
                .expect("invariant: errors array should never be empty.")),
        }
    }

    /// Attempts to issue a GET request to the given path on all endpoints, returning results from each endpoint.
    pub(crate) async fn request_on_each_endpoint<T, P>(
        &self,
        path: P,
    ) -> Vec<Result<Response<T>, Error>>
    where
        P: AsRef<str>,
        T: DeserializeOwned,
    {
        let path = path.as_ref();
        let mut results = Vec::with_capacity(self.endpoints.len());

        for endpoint in self.endpoints.iter() {
            let result = self.request(format!("{}{}", endpoint, path)).await;
            results.push(result);
        }

        results
    }

    /// Lets other internal code make basic HTTP requests.
    pub(crate) async fn request<T, U>(&self, uri: U) -> Result<Response<T>, Error>
    where
        U: IntoUrl,
        T: DeserializeOwned,
    {
        let response = self.http_client.get(uri).send().await?;
        parse_etcd_response(response, |s| s == StatusCode::OK).await
    }
}

pub(crate) async fn parse_etcd_response<T>(
    response: reqwest::Response,
    status_code_is_success: impl FnOnce(StatusCode) -> bool,
) -> Result<Response<T>, Error>
where
    T: DeserializeOwned,
{
    let status = response.status();
    let cluster_info = ClusterInfo::from(response.headers());
    let body = response.bytes().await?;
    if status_code_is_success(status) {
        match serde_json::from_slice::<T>(&body) {
            Ok(data) => Ok(Response { data, cluster_info }),
            Err(error) => Err(Error::Serialization(error)),
        }
    } else {
        match serde_json::from_slice::<ApiError>(&body) {
            Ok(error) => Err(Error::Api(error)),
            Err(error) => Err(Error::Serialization(error)),
        }
    }
}

/// A wrapper type returned by all API calls.
///
/// Contains the primary data of the response along with information about the cluster extracted
/// from the HTTP response headers.
#[derive(Clone, Debug)]
pub struct Response<T> {
    /// Information about the state of the cluster.
    pub cluster_info: ClusterInfo,
    /// The primary data of the response.
    pub data: T,
}

/// Information about the state of the etcd cluster from an API response's HTTP headers.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct ClusterInfo {
    /// An internal identifier for the cluster.
    pub cluster_id: Option<String>,
    /// A unique, monotonically-incrementing integer created for each change to etcd.
    pub etcd_index: Option<u64>,
    /// A unique, monotonically-incrementing integer used by the Raft protocol.
    pub raft_index: Option<u64>,
    /// The current Raft election term.
    pub raft_term: Option<u64>,
}

impl<'a> From<&'a HeaderMap<HeaderValue>> for ClusterInfo {
    fn from(headers: &'a HeaderMap<HeaderValue>) -> Self {
        let cluster_id = headers.get(XETCD_CLUSTER_ID).and_then(|v| {
            match String::from_utf8(v.as_bytes().to_vec()) {
                Ok(s) => Some(s),
                Err(e) => {
                    error!("{} header decode error: {:?}", XETCD_CLUSTER_ID, e);
                    None
                }
            }
        });

        let etcd_index = headers.get(XETCD_INDEX).and_then(|v| {
            match String::from_utf8(v.as_bytes().to_vec())
                .map_err(|e| format!("{:?}", e))
                .and_then(|s| s.parse().map_err(|e| format!("{:?}", e)))
            {
                Ok(i) => Some(i),
                Err(e) => {
                    error!("{} header decode error: {}", XETCD_INDEX, e);
                    None
                }
            }
        });

        let raft_index = headers.get(XRAFT_INDEX).and_then(|v| {
            match String::from_utf8(v.as_bytes().to_vec())
                .map_err(|e| format!("{:?}", e))
                .and_then(|s| s.parse().map_err(|e| format!("{:?}", e)))
            {
                Ok(i) => Some(i),
                Err(e) => {
                    error!("{} header decode error: {}", XRAFT_INDEX, e);
                    None
                }
            }
        });

        let raft_term = headers.get(XRAFT_TERM).and_then(|v| {
            match String::from_utf8(v.as_bytes().to_vec())
                .map_err(|e| format!("{:?}", e))
                .and_then(|s| s.parse().map_err(|e| format!("{:?}", e)))
            {
                Ok(i) => Some(i),
                Err(e) => {
                    error!("{} header decode error: {}", XRAFT_TERM, e);
                    None
                }
            }
        });

        ClusterInfo {
            cluster_id,
            etcd_index,
            raft_index,
            raft_term,
        }
    }
}

/// Constructs the full URL for the versions API call.
fn build_url(endpoint: &Uri, path: &str) -> String {
    format!("{}{}", endpoint, path)
}
