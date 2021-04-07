use crate::test::TestClient;

mod test;

#[test]
fn health() {
    let mut client = TestClient::no_destructor();
    let responses = client.run(|c| c.health());

    for response in responses {
        assert_eq!(response.unwrap().data.health, "true");
    }
}

#[test]
fn versions() {
    let mut client = TestClient::no_destructor();
    let responses = client.run(|c| c.versions());

    for response in responses {
        let response = response.unwrap();
        assert_eq!(response.data.cluster_version, "2.3.0");
        assert_eq!(response.data.server_version, "2.3.8");
    }
}
