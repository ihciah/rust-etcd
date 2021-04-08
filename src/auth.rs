//! etcd's authentication and authorization API.
//!
//! These API endpoints are used to manage users and roles.

use http::{StatusCode, Uri};
use serde::de::DeserializeOwned;
use serde_derive::{Deserialize, Serialize};
use serde_json;

use crate::client::{parse_empty_response, Client, ClusterInfo, Response};
use crate::error::Error;

/// The structure returned by the `GET /v2/auth/enable` endpoint.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
struct AuthStatus {
    /// Whether or not the auth system is enabled.
    pub enabled: bool,
}

/// The type returned when the auth system is successfully enabled or disabled.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum AuthChange {
    /// The auth system was successfully enabled or disabled.
    Changed,
    /// The auth system was already in the desired state.
    Unchanged,
}

/// An existing etcd user with a list of their granted roles.
#[derive(Debug, Clone, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct User {
    /// The user's name.
    #[serde(rename = "user")]
    name: String,
    /// The names of roles granted to the user.
    roles: Vec<String>,
}

impl User {
    /// Returns the user's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the names of the roles granted to the user.
    pub fn role_names(&self) -> &[String] {
        &self.roles
    }
}

/// An existing etcd user with details of granted roles.
#[derive(Debug, Clone, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct UserDetail {
    /// The user's name.
    #[serde(rename = "user")]
    name: String,
    /// Roles granted to the user.
    roles: Vec<Role>,
}

impl UserDetail {
    /// Returns the user's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the roles granted to the user.
    pub fn roles(&self) -> &[Role] {
        &self.roles
    }
}

/// A list of all users.
#[derive(Debug, Clone, Deserialize, Eq, Hash, PartialEq, Serialize)]
struct Users {
    users: Option<Vec<UserDetail>>,
}

/// Paramters used to create a new etcd user.
#[derive(Debug, Deserialize, Clone, Eq, Hash, PartialEq, Serialize)]
pub struct NewUser {
    /// The user's name.
    #[serde(rename = "user")]
    name: String,
    /// The user's password.
    password: String,
    /// An initial set of roles granted to the user.
    #[serde(skip_serializing_if = "Option::is_none")]
    roles: Option<Vec<String>>,
}

impl NewUser {
    /// Creates a new user.
    pub fn new<N, P>(name: N, password: P) -> Self
    where
        N: Into<String>,
        P: Into<String>,
    {
        NewUser {
            name: name.into(),
            password: password.into(),
            roles: None,
        }
    }

    /// Gets the name of the new user.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Grants a role to the new user.
    pub fn add_role<R>(&mut self, role: R)
    where
        R: Into<String>,
    {
        match self.roles {
            Some(ref mut roles) => roles.push(role.into()),
            None => self.roles = Some(vec![role.into()]),
        }
    }
}

/// Parameters used to update an existing etcd user.
#[derive(Debug, Deserialize, Clone, Eq, Hash, PartialEq, Serialize)]
pub struct UserUpdate {
    /// The user's name.
    #[serde(rename = "user")]
    name: String,
    /// A new password for the user.
    #[serde(skip_serializing_if = "Option::is_none")]
    password: Option<String>,
    /// Roles being granted to the user.
    #[serde(rename = "grant")]
    #[serde(skip_serializing_if = "Option::is_none")]
    grants: Option<Vec<String>>,
    /// Roles being revoked from the user.
    #[serde(rename = "revoke")]
    #[serde(skip_serializing_if = "Option::is_none")]
    revocations: Option<Vec<String>>,
}

impl UserUpdate {
    /// Creates a new `UserUpdate` for the given user.
    pub fn new<N>(name: N) -> Self
    where
        N: Into<String>,
    {
        UserUpdate {
            name: name.into(),
            password: None,
            grants: None,
            revocations: None,
        }
    }

    /// Gets the name of the user being updated.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Updates the user's password.
    pub fn update_password<P>(&mut self, password: P)
    where
        P: Into<String>,
    {
        self.password = Some(password.into());
    }

    /// Grants the given role to the user.
    pub fn grant_role<R>(&mut self, role: R)
    where
        R: Into<String>,
    {
        match self.grants {
            Some(ref mut grants) => grants.push(role.into()),
            None => self.grants = Some(vec![role.into()]),
        }
    }

    /// Revokes the given role from the user.
    pub fn revoke_role<R>(&mut self, role: R)
    where
        R: Into<String>,
    {
        match self.revocations {
            Some(ref mut revocations) => revocations.push(role.into()),
            None => self.revocations = Some(vec![role.into()]),
        }
    }
}

/// An authorization role.
#[derive(Debug, Deserialize, Clone, Eq, Hash, PartialEq, Serialize)]
pub struct Role {
    /// The name of the role.
    #[serde(rename = "role")]
    name: String,
    /// Permissions granted to the role.
    permissions: Permissions,
}

impl Role {
    /// Creates a new role.
    pub fn new<N>(name: N) -> Self
    where
        N: Into<String>,
    {
        Role {
            name: name.into(),
            permissions: Permissions::new(),
        }
    }

    /// Gets the name of the role.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Grants read permission for a key in etcd's key-value store to this role.
    pub fn grant_kv_read_permission<K>(&mut self, key: K)
    where
        K: Into<String>,
    {
        self.permissions.kv.modify_read_permission(key)
    }

    /// Grants write permission for a key in etcd's key-value store to this role.
    pub fn grant_kv_write_permission<K>(&mut self, key: K)
    where
        K: Into<String>,
    {
        self.permissions.kv.modify_write_permission(key)
    }

    /// Returns a list of keys in etcd's key-value store that this role is allowed to read.
    pub fn kv_read_permissions(&self) -> &[String] {
        match self.permissions.kv.read {
            Some(ref read) => read,
            None => &[],
        }
    }

    /// Returns a list of keys in etcd's key-value store that this role is allowed to write.
    pub fn kv_write_permissions(&self) -> &[String] {
        match self.permissions.kv.write {
            Some(ref write) => write,
            None => &[],
        }
    }
}

/// A list of all roles.
#[derive(Debug, Clone, Deserialize, Eq, Hash, PartialEq, Serialize)]
struct Roles {
    roles: Option<Vec<Role>>,
}

/// Parameters used to update an existing authorization role.
#[derive(Debug, Deserialize, Clone, Eq, Hash, PartialEq, Serialize)]
pub struct RoleUpdate {
    /// The name of the role.
    #[serde(rename = "role")]
    name: String,
    /// Permissions being added to the role.
    #[serde(rename = "grant")]
    #[serde(skip_serializing_if = "Option::is_none")]
    grants: Option<Permissions>,
    /// Permissions being removed from the role.
    #[serde(rename = "revoke")]
    #[serde(skip_serializing_if = "Option::is_none")]
    revocations: Option<Permissions>,
}

impl RoleUpdate {
    /// Creates a new `RoleUpdate` for the given role.
    pub fn new<R>(role: R) -> Self
    where
        R: Into<String>,
    {
        RoleUpdate {
            name: role.into(),
            grants: None,
            revocations: None,
        }
    }

    /// Gets the name of the role being updated.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Grants read permission for a key in etcd's key-value store to this role.
    pub fn grant_kv_read_permission<K>(&mut self, key: K)
    where
        K: Into<String>,
    {
        match self.grants {
            Some(ref mut grants) => grants.kv.modify_read_permission(key),
            None => {
                let mut permissions = Permissions::new();
                permissions.kv.modify_read_permission(key);
                self.grants = Some(permissions);
            }
        }
    }

    /// Grants write permission for a key in etcd's key-value store to this role.
    pub fn grant_kv_write_permission<K>(&mut self, key: K)
    where
        K: Into<String>,
    {
        match self.grants {
            Some(ref mut grants) => grants.kv.modify_write_permission(key),
            None => {
                let mut permissions = Permissions::new();
                permissions.kv.modify_write_permission(key);
                self.grants = Some(permissions);
            }
        }
    }

    /// Revokes read permission for a key in etcd's key-value store from this role.
    pub fn revoke_kv_read_permission<K>(&mut self, key: K)
    where
        K: Into<String>,
    {
        match self.revocations {
            Some(ref mut revocations) => revocations.kv.modify_read_permission(key),
            None => {
                let mut permissions = Permissions::new();
                permissions.kv.modify_read_permission(key);
                self.revocations = Some(permissions);
            }
        }
    }

    /// Revokes write permission for a key in etcd's key-value store from this role.
    pub fn revoke_kv_write_permission<K>(&mut self, key: K)
    where
        K: Into<String>,
    {
        match self.revocations {
            Some(ref mut revocations) => revocations.kv.modify_write_permission(key),
            None => {
                let mut permissions = Permissions::new();
                permissions.kv.modify_write_permission(key);
                self.revocations = Some(permissions);
            }
        }
    }
}

/// The access permissions granted to a role.
#[derive(Debug, Deserialize, Clone, Eq, Hash, PartialEq, Serialize)]
struct Permissions {
    /// Permissions for etcd's key-value store.
    kv: Permission,
}

impl Permissions {
    /// Creates a new set of permissions.
    fn new() -> Self {
        Permissions {
            kv: Permission::new(),
        }
    }
}

/// A set of read and write access permissions for etcd resources.
#[derive(Debug, Deserialize, Clone, Eq, Hash, PartialEq, Serialize)]
struct Permission {
    /// Resources allowed to be read.
    #[serde(skip_serializing_if = "Option::is_none")]
    read: Option<Vec<String>>,
    /// Resources allowed to be written.
    #[serde(skip_serializing_if = "Option::is_none")]
    write: Option<Vec<String>>,
}

impl Permission {
    /// Creates a new permission record.
    fn new() -> Self {
        Permission {
            read: None,
            write: None,
        }
    }

    /// Modifies read access to a resource.
    fn modify_read_permission<K>(&mut self, key: K)
    where
        K: Into<String>,
    {
        match self.read {
            Some(ref mut read) => read.push(key.into()),
            None => self.read = Some(vec![key.into()]),
        }
    }

    /// Modifies write access to a resource.
    fn modify_write_permission<K>(&mut self, key: K)
    where
        K: Into<String>,
    {
        match self.write {
            Some(ref mut write) => write.push(key.into()),
            None => self.write = Some(vec![key.into()]),
        }
    }
}

type EtcdAuthResult<T> = Result<Response<T>, Vec<Error>>;

/// Creates a new role.
pub async fn create_role(client: &Client, role: Role) -> EtcdAuthResult<Role> {
    let body = serde_json::to_string(&role).map_err(|e| vec![e.into()])?;

    client
        .first_ok(|client, endpoint| {
            let body = body.clone();
            let url = build_url(endpoint, &format!("/roles/{}", role.name));
            async move {
                let response = client
                    .http_client()
                    .put(url)
                    .body(body)
                    .header(
                        http::header::CONTENT_TYPE,
                        "application/x-www-form-urlencoded",
                    )
                    .send()
                    .await?;
                parse_auth_response(response, |s| {
                    s == StatusCode::OK || s == StatusCode::CREATED
                })
                .await
            }
        })
        .await
}

/// Creates a new user.
pub async fn create_user(client: &Client, user: NewUser) -> EtcdAuthResult<User> {
    let body = serde_json::to_string(&user).map_err(|e| vec![e.into()])?;

    client
        .first_ok(|client, endpoint| {
            let url = build_url(endpoint, &format!("/users/{}", user.name));
            let body = body.clone();
            async move {
                let response = client
                    .http_client()
                    .put(url)
                    .body(body)
                    .header(
                        http::header::CONTENT_TYPE,
                        "application/x-www-form-urlencoded",
                    )
                    .send()
                    .await?;
                parse_auth_response(response, |s| {
                    s == StatusCode::OK || s == StatusCode::CREATED
                })
                .await
            }
        })
        .await
}

/// Deletes a role.
pub async fn delete_role<N>(client: &Client, role_name: N) -> EtcdAuthResult<()>
where
    N: AsRef<str>,
{
    let role_name = role_name.as_ref();

    client
        .first_ok(|client, endpoint| {
            let url = build_url(endpoint, &format!("/roles/{}", role_name));
            async move {
                let response = client.http_client().delete(url).send().await?;
                parse_empty_response(response).await
            }
        })
        .await
}

/// Deletes a user.
pub async fn delete_user<N>(client: &Client, user_name: N) -> EtcdAuthResult<()>
where
    N: AsRef<str>,
{
    let user_name = user_name.as_ref();
    client
        .first_ok(|client, endpoint| {
            let url = build_url(endpoint, &format!("/users/{}", user_name));
            async move {
                let response = client.http_client().delete(url).send().await?;
                parse_empty_response(response).await
            }
        })
        .await
}

/// Attempts to disable the auth system.
pub async fn disable(client: &Client) -> EtcdAuthResult<AuthChange> {
    client
        .first_ok(|client, endpoint| {
            let url = build_url(endpoint, "/enable");
            async move {
                let response = client.http_client().delete(url).send().await?;
                parse_auth_change_response(response)
            }
        })
        .await
}

/// Attempts to enable the auth system.
pub async fn enable(client: &Client) -> EtcdAuthResult<AuthChange> {
    client
        .first_ok(|client, endpoint| {
            let url = build_url(endpoint, "/enable");
            async move {
                let response = client.http_client().put(url).send().await?;
                parse_auth_change_response(response)
            }
        })
        .await
}

/// Get a role.
pub async fn get_role<N>(client: &Client, role_name: N) -> EtcdAuthResult<Role>
where
    N: AsRef<str>,
{
    let role_name = role_name.as_ref();

    client
        .first_ok(|client, endpoint| {
            let url = build_url(endpoint, &format!("/roles/{}", role_name));
            async move {
                let response = client.http_client().get(url).send().await?;
                parse_auth_response(response, |s| s == StatusCode::OK).await
            }
        })
        .await
}

/// Get a role.
pub async fn get_roles<N>(client: &Client) -> EtcdAuthResult<Vec<Role>> {
    client
        .first_ok(|client, endpoint| {
            let url = build_url(endpoint, "/roles");
            async move {
                let response = client.http_client().get(url).send().await?;
                parse_auth_response(response, |s| s == StatusCode::OK).await
            }
        })
        .await
}

/// Get a user.
pub async fn get_user<N>(client: &Client, user_name: N) -> EtcdAuthResult<User>
where
    N: AsRef<str>,
{
    let user_name = user_name.as_ref();

    client
        .first_ok(|client, endpoint| {
            let url = build_url(endpoint, &format!("/users/{}", user_name));
            async move {
                let response = client.http_client().get(url).send().await?;
                parse_auth_response(response, |s| s == StatusCode::OK).await
            }
        })
        .await
}

/// Gets all users.
pub async fn get_users<N>(client: &Client) -> EtcdAuthResult<Vec<User>> {
    client
        .first_ok(|client, endpoint| {
            let url = build_url(endpoint, "/users");
            async move {
                let response = client.http_client().get(url).send().await?;
                parse_auth_response(response, |s| s == StatusCode::OK).await
            }
        })
        .await
}

/// Determines whether or not the auth system is enabled.
pub async fn status(client: &Client) -> EtcdAuthResult<bool> {
    client
        .first_ok(|client, endpoint| {
            let url = build_url(endpoint, "/enable");
            async move {
                let response = client.http_client().get(url).send().await?;
                let response: Response<AuthStatus> =
                    parse_auth_response(response, |s| s == StatusCode::OK).await?;

                Ok(Response {
                    cluster_info: response.cluster_info,
                    data: response.data.enabled,
                })
            }
        })
        .await
}

/// Updates an existing role.
pub async fn update_role(client: &Client, role: RoleUpdate) -> EtcdAuthResult<Role> {
    let body = serde_json::to_string(&role).map_err(|e| vec![e.into()])?;

    client
        .first_ok(|client, endpoint| {
            let url = build_url(endpoint, &format!("/roles/{}", role.name));
            let body = body.clone();
            async move {
                let response = client
                    .http_client()
                    .put(url)
                    .body(body)
                    .header(
                        http::header::CONTENT_TYPE,
                        "application/x-www-form-urlencoded",
                    )
                    .send()
                    .await?;
                parse_auth_response(response, |s| s == StatusCode::OK).await
            }
        })
        .await
}

/// Updates an existing user
pub async fn update_user(client: &Client, user: UserUpdate) -> EtcdAuthResult<User> {
    let body = serde_json::to_string(&user).map_err(|e| vec![e.into()])?;

    client
        .first_ok(|client, endpoint| {
            let url = build_url(endpoint, &format!("/users/{}", user.name));
            let body = body.clone();
            async move {
                let response = client
                    .http_client()
                    .put(url)
                    .body(body)
                    .header(
                        http::header::CONTENT_TYPE,
                        "application/x-www-form-urlencoded",
                    )
                    .send()
                    .await?;
                parse_auth_response(response, |s| s == StatusCode::OK).await
            }
        })
        .await
}

/// Constructs the full URL for an API call.
fn build_url(endpoint: &Uri, path: &str) -> String {
    format!("{}v2/auth{}", endpoint, path)
}

async fn parse_auth_response<T>(
    response: reqwest::Response,
    status_code_is_success: impl FnOnce(StatusCode) -> bool,
) -> Result<Response<T>, Error>
where
    T: DeserializeOwned,
{
    let status_code = response.status();
    let cluster_info = ClusterInfo::from(response.headers());
    let body = response.bytes().await?;
    if status_code_is_success(status_code) {
        match serde_json::from_slice::<T>(&body) {
            Ok(data) => Ok(Response { data, cluster_info }),
            Err(error) => Err(Error::Serialization(error)),
        }
    } else {
        Err(Error::UnexpectedStatus(status_code))
    }
}

fn parse_auth_change_response(response: reqwest::Response) -> Result<Response<AuthChange>, Error> {
    let status = response.status();
    let cluster_info = ClusterInfo::from(response.headers());
    match status {
        StatusCode::OK => Ok(Response {
            data: AuthChange::Changed,
            cluster_info,
        }),
        StatusCode::CONFLICT => Ok(Response {
            data: AuthChange::Unchanged,
            cluster_info,
        }),
        _ => Err(Error::UnexpectedStatus(status)),
    }
}
