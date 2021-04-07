//! etcd's members API.
//!
//! These API endpoints are used to manage cluster membership.

use crate::{
    client::{parse_empty_response, parse_etcd_response},
    Client, Error, Response,
};

use http::{StatusCode, Uri};
use serde_derive::{Deserialize, Serialize};
use serde_json;

/// An etcd server that is a member of a cluster.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Member {
    /// An internal identifier for the cluster member.
    pub id: String,
    /// A human-readable name for the cluster member.
    pub name: String,
    /// URLs exposing this cluster member's peer API.
    #[serde(rename = "peerURLs")]
    pub peer_urls: Vec<String>,
    /// URLs exposing this cluster member's client API.
    #[serde(rename = "clientURLs")]
    pub client_urls: Vec<String>,
}

/// The request body for `POST /v2/members` and `PUT /v2/members/:id`.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
struct PeerUrls {
    /// The peer URLs.
    #[serde(rename = "peerURLs")]
    peer_urls: Vec<String>,
}

/// A small wrapper around `Member` to match the response of `GET /v2/members`.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
struct ListResponse {
    /// The members.
    members: Vec<Member>,
}

type EtcdMembersResult<T = ()> = Result<Response<T>, Vec<Error>>;

/// Adds a new member to the cluster.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * peer_urls: URLs exposing this cluster member's peer API.
pub async fn add(client: &Client, peer_urls: Vec<String>) -> EtcdMembersResult {
    let peer_urls = PeerUrls { peer_urls };
    let body = serde_json::to_string(&peer_urls).map_err(|e| vec![e.into()])?;

    client
        .first_ok(|client, endpoint| {
            let body = body.clone();
            async move {
                let url = build_url(endpoint, "");
                let response = client.http_client().get(url).body(body).send().await?;
                parse_empty_response(response).await
            }
        })
        .await
}

/// Deletes a member from the cluster.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * id: The unique identifier of the member to delete.
pub async fn delete<K>(client: &Client, id: K) -> EtcdMembersResult
where
    K: AsRef<str>,
{
    let id = id.as_ref();
    client
        .first_ok(|client, endpoint| {
            let url = build_url(endpoint, &format!("/{}", id));
            async move {
                let response = client.http_client().delete(url).send().await?;
                parse_empty_response(response).await
            }
        })
        .await
}

/// Lists the members of the cluster.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
pub async fn list(client: &Client) -> EtcdMembersResult<Vec<Member>> {
    client
        .first_ok(|client, endpoint| async move {
            let url = build_url(endpoint, "");
            let response = client.http_client().get(url).send().await?;
            let response: Response<ListResponse> =
                parse_etcd_response(response, |s| s == StatusCode::OK).await?;
            Ok(Response {
                cluster_info: response.cluster_info,
                data: response.data.members,
            })
        })
        .await
}

/// Updates the peer URLs of a member of the cluster.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * id: The unique identifier of the member to update.
/// * peer_urls: URLs exposing this cluster member's peer API.
pub async fn update(client: &Client, id: String, peer_urls: Vec<String>) -> EtcdMembersResult {
    let peer_urls = PeerUrls { peer_urls };
    let body = serde_json::to_string(&peer_urls).map_err(|e| vec![e.into()])?;

    client
        .first_ok(|client, endpoint| {
            let url = build_url(endpoint, &format!("/{}", id));
            let body = body.clone();
            async move {
                let response = client.http_client().put(url).body(body).send().await?;
                parse_empty_response(response).await
            }
        })
        .await
}

/// Constructs the full URL for an API call.
fn build_url(endpoint: &Uri, path: &str) -> String {
    format!("{}v2/members{}", endpoint, path)
}
