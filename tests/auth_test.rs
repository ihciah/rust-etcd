use crate::test::TestClient;
use etcd::{
    auth::{self, AuthChange, NewUser, Role, RoleUpdate, UserUpdate},
    ClientBuilder,
};

mod test;

#[test]
fn auth() {
    let test_client = TestClient::no_destructor();
    let authed_client = ClientBuilder::new(&["http://etcd:2379"])
        .with_basic_auth("root", "secret")
        .build();

    // Check that auth is disabled first.
    {
        let response = test_client.run(|c| auth::status(c)).unwrap();
        assert_eq!(response.data, false);
    }

    // Create a new user.
    {
        let root_user = NewUser::new("root", "secret");
        let response = test_client
            .run(|c| auth::create_user(c, root_user))
            .unwrap();

        assert_eq!(response.data.name(), "root");
    }

    // Enable auth:
    {
        let response = test_client.run(|c| auth::enable(c)).unwrap();
        assert_eq!(response.data, AuthChange::Changed);
    }

    // Update role:
    {
        let mut update_guest = RoleUpdate::new("guest");
        update_guest.revoke_kv_write_permission("/*");
        test_client
            .run(|_| auth::update_role(&authed_client, update_guest))
            .unwrap();
    }

    // Update another role:
    {
        let mut rkt_role = Role::new("rkt");
        rkt_role.grant_kv_read_permission("/rkt/*");
        rkt_role.grant_kv_write_permission("/rkt/*");
        test_client
            .run(|_| auth::create_role(&authed_client, rkt_role))
            .unwrap();
    }

    // Create a new user:
    {
        let mut rkt_user = NewUser::new("rkt", "secret");
        rkt_user.add_role("rkt");
        let response = test_client
            .run(|_| auth::create_user(&authed_client, rkt_user))
            .unwrap();

        let rkt_user = response.data;
        assert_eq!(rkt_user.name(), "rkt");
        let role_name = &rkt_user.role_names()[0];
        assert_eq!(role_name, "rkt");
    }

    // Update our user:
    {
        let mut update_rkt_user = UserUpdate::new("rkt");

        update_rkt_user.update_password("secret2");
        update_rkt_user.grant_role("root");
        test_client
            .run(|_| auth::update_user(&authed_client, update_rkt_user))
            .unwrap();
    }

    // Read the role back:
    {
        let response = test_client
            .run(|_| auth::get_role(&&authed_client, "rkt"))
            .unwrap();
        let role = response.data;
        assert!(role.kv_read_permissions().contains(&"/rkt/*".to_owned()));
        assert!(role.kv_write_permissions().contains(&"/rkt/*".to_owned()));
    }

    // Delete the user & role:
    {
        test_client
            .run(|_| auth::delete_user(&authed_client, "rkt"))
            .unwrap();
        test_client
            .run(|_| auth::delete_role(&authed_client, "rkt"))
            .unwrap();
    }

    // Update guest role:
    {
        let mut update_guest = RoleUpdate::new("guest");
        update_guest.grant_kv_write_permission("/*");
        test_client
            .run(|_| auth::update_role(&authed_client, update_guest))
            .unwrap();
    }

    // Disable auth:
    {
        let response = test_client.run(|_| auth::disable(&authed_client)).unwrap();
        assert_eq!(response.data, AuthChange::Changed);
    }

    // Check that auth is disabled, using unauthorized client:
    {
        let response = test_client.run(|c| auth::status(c)).unwrap();
        assert_eq!(response.data, false);
    }
}
