//! etcd's key-value API.
//!
//! The term "node" in the documentation for this module refers to a key-value pair or a directory
//! of key-value pairs. For example, "/foo" is a key if it has a value, but it is a directory if
//! there other other key-value pairs "underneath" it, such as "/foo/bar".

use std::time::Duration;

use http::{StatusCode, Uri};
use serde_derive::{Deserialize, Serialize};
use tokio::time::timeout;

pub use crate::error::WatchError;

use crate::client::{parse_etcd_response, Client, Response};
use crate::error::Error;
use crate::options::{
    ComparisonConditions, DeleteOptions, GetOptions as InternalGetOptions, SetOptions,
};

type EtcdKeyValueResult<E = Vec<Error>> = Result<Response<KeyValueInfo>, E>;

/// Information about the result of a successful key-value API operation.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct KeyValueInfo {
    /// The action that was taken, e.g. `get`, `set`.
    pub action: Action,
    /// The etcd `Node` that was operated upon.
    pub node: Node,
    /// The previous state of the target node.
    #[serde(rename = "prevNode")]
    pub prev_node: Option<Node>,
}

/// The type of action that was taken in response to a key value API request.
///
/// "Node" refers to the key or directory being acted upon.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum Action {
    /// Atomic deletion of a node based on previous state.
    #[serde(rename = "compareAndDelete")]
    CompareAndDelete,
    /// Atomtic update of a node based on previous state.
    #[serde(rename = "compareAndSwap")]
    CompareAndSwap,
    /// Creation of a node that didn't previously exist.
    #[serde(rename = "create")]
    Create,
    /// Deletion of a node.
    #[serde(rename = "delete")]
    Delete,
    /// Expiration of a node.
    #[serde(rename = "expire")]
    Expire,
    /// Retrieval of a node.
    #[serde(rename = "get")]
    Get,
    /// Assignment of a node, which may have previously existed.
    #[serde(rename = "set")]
    Set,
    /// Update of an existing node.
    #[serde(rename = "update")]
    Update,
}

/// An etcd key or directory.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Node {
    /// The new value of the etcd creation index.
    #[serde(rename = "createdIndex")]
    pub created_index: Option<u64>,
    /// Whether or not the node is a directory.
    pub dir: Option<bool>,
    /// An ISO 8601 timestamp for when the key will expire.
    pub expiration: Option<String>,
    /// The name of the key.
    pub key: Option<String>,
    /// The new value of the etcd modification index.
    #[serde(rename = "modifiedIndex")]
    pub modified_index: Option<u64>,
    /// Child nodes of a directory.
    pub nodes: Option<Vec<Node>>,
    /// The key's time to live in seconds.
    pub ttl: Option<i64>,
    /// The value of the key.
    pub value: Option<String>,
}

/// Options for customizing the behavior of `kv::get`.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct GetOptions {
    /// If true and the node is a directory, child nodes will be returned as well.
    pub recursive: bool,
    /// If true and the node is a directory, any child nodes returned will be sorted
    /// alphabetically.
    pub sort: bool,
    /// If true, the etcd node serving the response will synchronize with the quorum before
    /// returning the value.
    ///
    /// This is slower but avoids possibly stale data from being returned.
    pub strong_consistency: bool,
}

/// Options for customizing the behavior of `kv::watch`.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct WatchOptions {
    /// If given, the watch operation will return the first change at the index or greater,
    /// allowing you to watch for changes that happened in the past.
    pub index: Option<u64>,
    /// Whether or not to watch all child keys as well.
    pub recursive: bool,
    /// If given, the watch operation will time out if it's still waiting after the duration.
    pub timeout: Option<Duration>,
}

/// Deletes a node only if the given current value and/or current modified index match.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * key: The name of the node to delete.
/// * current_value: If given, the node must currently have this value for the operation to
/// succeed.
/// * current_modified_index: If given, the node must currently be at this modified index for the
/// operation to succeed.
///
/// # Errors
///
/// Fails if the conditions didn't match or if no conditions were given.
pub async fn compare_and_delete<K>(
    client: &Client,
    key: K,
    current_value: Option<&str>,
    current_modified_index: Option<u64>,
) -> EtcdKeyValueResult
where
    K: AsRef<str>,
{
    raw_delete(
        client,
        key,
        DeleteOptions {
            conditions: Some(ComparisonConditions {
                value: current_value,
                modified_index: current_modified_index,
            }),
            ..Default::default()
        },
    )
    .await
}

/// Updates a node only if the given current value and/or current modified index
/// match.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * key: The name of the node to update.
/// * value: The new value for the node.
/// * ttl: If given, the node will expire after this many seconds.
/// * current_value: If given, the node must currently have this value for the operation to
/// succeed.
/// * current_modified_index: If given, the node must currently be at this modified index for the
/// operation to succeed.
///
/// # Errors
///
/// Fails if the conditions didn't match or if no conditions were given.
pub async fn compare_and_swap<K, V>(
    client: &Client,
    key: K,
    value: V,
    ttl: Option<u64>,
    current_value: Option<&str>,
    current_modified_index: Option<u64>,
) -> EtcdKeyValueResult
where
    K: AsRef<str>,
    V: AsRef<str>,
{
    let value = value.as_ref();

    raw_set(
        client,
        key,
        SetOptions {
            conditions: Some(ComparisonConditions {
                value: current_value,
                modified_index: current_modified_index,
            }),
            ttl,
            value: Some(value),
            ..Default::default()
        },
    )
    .await
}

/// Creates a new key-value pair.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * key: The name of the key-value pair to create.
/// * value: The new value for the node.
/// * ttl: If given, the node will expire after this many seconds.
///
/// # Errors
///
/// Fails if the key already exists.
pub async fn create<K, V>(client: &Client, key: K, value: V, ttl: Option<u64>) -> EtcdKeyValueResult
where
    K: AsRef<str>,
    V: AsRef<str>,
{
    let value = value.as_ref();

    raw_set(
        client,
        key,
        SetOptions {
            prev_exist: Some(false),
            ttl,
            value: Some(value),
            ..Default::default()
        },
    )
    .await
}

/// Creates a new empty directory.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * key: The name of the directory to create.
/// * ttl: If given, the node will expire after this many seconds.
///
/// # Errors
///
/// Fails if the key already exists.
pub async fn create_dir<K>(client: &Client, key: K, ttl: Option<u64>) -> EtcdKeyValueResult
where
    K: AsRef<str>,
{
    raw_set(
        client,
        key,
        SetOptions {
            dir: Some(true),
            prev_exist: Some(false),
            ttl: ttl,
            ..Default::default()
        },
    )
    .await
}

/// Creates a new key-value pair in a directory with a numeric key name larger than any of its
/// sibling key-value pairs.
///
/// For example, the first value created with this function under the directory "/foo" will have a
/// key name like "00000000000000000001" automatically generated. The second value created with
/// this function under the same directory will have a key name like "00000000000000000002".
///
/// This behavior is guaranteed by the server.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * key: The name of the directory to create a key-value pair in.
/// * value: The new value for the key-value pair.
/// * ttl: If given, the node will expire after this many seconds.
///
/// # Errors
///
/// Fails if the key already exists and is not a directory.
pub async fn create_in_order<K, V>(
    client: &Client,
    key: K,
    value: V,
    ttl: Option<u64>,
) -> EtcdKeyValueResult
where
    K: AsRef<str>,
    V: AsRef<str>,
{
    let value = value.as_ref();
    raw_set(
        client,
        key,
        SetOptions {
            create_in_order: true,
            ttl: ttl,
            value: Some(value),
            ..Default::default()
        },
    )
    .await
}

/// Deletes a node.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * key: The name of the node to delete.
/// * recursive: If true, and the key is a directory, the directory and all child key-value
/// pairs and directories will be deleted as well.
///
/// # Errors
///
/// Fails if the key is a directory and `recursive` is `false`.
pub async fn delete<K>(client: &Client, key: K, recursive: bool) -> EtcdKeyValueResult
where
    K: AsRef<str>,
{
    raw_delete(
        client,
        key,
        DeleteOptions {
            recursive: Some(recursive),
            ..Default::default()
        },
    )
    .await
}

/// Deletes an empty directory or a key-value pair at the given key.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * key: The name of the node to delete.
///
/// # Errors
///
/// Fails if the directory is not empty.
pub async fn delete_dir<K>(client: &Client, key: K) -> EtcdKeyValueResult
where
    K: AsRef<str>,
{
    raw_delete(
        client,
        key,
        DeleteOptions {
            dir: Some(true),
            ..Default::default()
        },
    )
    .await
}

/// Gets the value of a node.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * key: The name of the node to retrieve.
/// * options: Options to customize the behavior of the operation.
///
/// # Errors
///
/// Fails if the key doesn't exist.
pub async fn get<K>(client: &Client, key: K, options: GetOptions) -> EtcdKeyValueResult
where
    K: AsRef<str>,
{
    raw_get(
        client,
        key,
        InternalGetOptions {
            recursive: options.recursive,
            sort: Some(options.sort),
            strong_consistency: options.strong_consistency,
            ..Default::default()
        },
    )
    .await
}

/// Sets the value of a key-value pair.
///
/// Any previous value and TTL will be replaced.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * key: The name of the key-value pair to set.
/// * value: The new value for the key-value pair.
/// * ttl: If given, the node will expire after this many seconds.
///
/// # Errors
///
/// Fails if the node is a directory.
pub async fn set<K, V>(client: &Client, key: K, value: V, ttl: Option<u64>) -> EtcdKeyValueResult
where
    K: AsRef<str>,
    V: AsRef<str>,
{
    let value = value.as_ref();
    raw_set(
        client,
        key,
        SetOptions {
            ttl,
            value: Some(value),
            ..Default::default()
        },
    )
    .await
}

/// Refreshes the already set etcd key, bumping its TTL without triggering watcher updates.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * key: The name of the key-value pair to set.
/// * ttl: If given, the node will expire after this many seconds.
///
/// # Errors
///
/// Fails if the node does not exist.
pub async fn refresh<K>(client: &Client, key: K, ttl: u64) -> EtcdKeyValueResult
where
    K: AsRef<str>,
{
    raw_set(
        client,
        key,
        SetOptions {
            ttl: Some(ttl),
            refresh: true,
            prev_exist: Some(true),
            ..Default::default()
        },
    )
    .await
}

/// Sets the key to an empty directory.
///
/// An existing key-value pair will be replaced, but an existing directory will not.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * key: The name of the directory to set.
/// * ttl: If given, the node will expire after this many seconds.
///
/// # Errors
///
/// Fails if the node is an existing directory.
pub async fn set_dir<K>(client: &Client, key: K, ttl: Option<u64>) -> EtcdKeyValueResult
where
    K: AsRef<str>,
{
    raw_set(
        client,
        key,
        SetOptions {
            dir: Some(true),
            ttl,
            ..Default::default()
        },
    )
    .await
}

/// Updates an existing key-value pair.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * key: The name of the key-value pair to update.
/// * value: The new value for the key-value pair.
/// * ttl: If given, the node will expire after this many seconds.
///
/// # Errors
///
/// Fails if the key does not exist.
pub async fn update<K, V>(client: &Client, key: K, value: V, ttl: Option<u64>) -> EtcdKeyValueResult
where
    K: AsRef<str>,
    V: AsRef<str>,
{
    let value = value.as_ref();
    raw_set(
        client,
        key,
        SetOptions {
            prev_exist: Some(true),
            ttl,
            value: Some(value),
            ..Default::default()
        },
    )
    .await
}

/// Updates a directory.
///
/// If the directory already existed, only the TTL is updated. If the key was a key-value pair, its
/// value is removed and its TTL is updated.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * key: The name of the node to update.
/// * ttl: If given, the node will expire after this many seconds.
///
/// # Errors
///
/// Fails if the node does not exist.
pub async fn update_dir<K>(client: &Client, key: K, ttl: Option<u64>) -> EtcdKeyValueResult
where
    K: AsRef<str>,
{
    raw_set(
        client,
        key,
        SetOptions {
            dir: Some(true),
            prev_exist: Some(true),
            ttl,
            ..Default::default()
        },
    )
    .await
}

/// Watches a node for changes and returns the new value as soon as a change takes place.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * key: The name of the node to watch.
/// * options: Options to customize the behavior of the operation.
///
/// # Errors
///
/// Fails if `options.index` is too old and has been flushed out of etcd's internal store of the
/// most recent change events. In this case, the key should be queried for its latest
/// "modified index" value and that should be used as the new `options.index` on a subsequent
/// `watch`.
///
/// Fails if a timeout is specified and the duration lapses without a response from the etcd
/// cluster.
pub async fn watch<K>(
    client: &Client,
    key: K,
    options: WatchOptions,
) -> EtcdKeyValueResult<WatchError>
where
    K: AsRef<str>,
{
    let fut = raw_get(
        client,
        key,
        InternalGetOptions {
            recursive: options.recursive,
            wait_index: options.index,
            wait: true,
            ..Default::default()
        },
    );

    if let Some(duration) = options.timeout {
        match timeout(duration, fut).await {
            Ok(result) => result.map_err(WatchError::Other),
            Err(_elapsed) => Err(WatchError::Timeout),
        }
    } else {
        fut.await.map_err(WatchError::Other)
    }
}

/// Handles all delete operations.
async fn raw_delete<K>(client: &Client, key: K, options: DeleteOptions<'_>) -> EtcdKeyValueResult
where
    K: AsRef<str>,
{
    let key = key.as_ref();
    let query_params = options.into_query_params().map_err(|e| vec![e])?;

    client
        .first_ok(move |client, endpoint| {
            let url = build_url(endpoint, key, Some(&query_params));
            async move {
                let response = client.http_client().delete(url).send().await?;
                parse_etcd_response(response, |s| s == StatusCode::OK).await
            }
        })
        .await
}

/// Handles all get operations.
async fn raw_get<K>(client: &Client, key: K, options: InternalGetOptions) -> EtcdKeyValueResult
where
    K: AsRef<str>,
{
    let query_params = options.into_query_params();
    let key = key.as_ref();

    client
        .first_ok(move |client, endpoint| {
            let url = build_url(endpoint, key, Some(&query_params));
            async move {
                let response = client.http_client().get(url).send().await?;
                parse_etcd_response(response, |s| s == StatusCode::OK).await
            }
        })
        .await
}

/// Handles all set operations.
async fn raw_set<K>(client: &Client, key: K, options: SetOptions<'_>) -> EtcdKeyValueResult
where
    K: AsRef<str>,
{
    let key = key.as_ref();
    let create_in_order = options.create_in_order;
    let request_body = options.into_request_body().map_err(|e| vec![e])?;

    client
        .first_ok(move |client, endpoint| {
            let request_body = request_body.clone();

            async move {
                let url = build_url(endpoint, key, None);
                let request = if create_in_order {
                    client.http_client().post(url)
                } else {
                    client.http_client().put(url)
                };
                let response = request.body(request_body).send().await?;
                parse_etcd_response(response, |s| {
                    s == StatusCode::OK || s == StatusCode::CREATED
                })
                .await
            }
        })
        .await
}

/// Constructs the full URL for an API call.
fn build_url(endpoint: &Uri, path: &str, query_params: Option<&str>) -> String {
    if let Some(query_params) = query_params {
        format!("{}v2/keys{}?{}", endpoint, path, query_params)
    } else {
        format!("{}v2/keys{}", endpoint, path)
    }
}
