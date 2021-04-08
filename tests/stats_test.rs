use etcd::stats;

use crate::test::TestClient;

mod test;

#[test]
fn leader_stats() {
    let client = TestClient::no_destructor();
    client.run(|c| stats::leader_stats(&c)).unwrap();
}

#[test]
fn self_stats() {
    let client = TestClient::no_destructor();
    let results = client.run(|c| stats::self_stats(&c));
    for result in results {
        result.unwrap();
    }
}

#[test]
fn store_stats() {
    let client = TestClient::no_destructor();
    let results = client.run(|c| stats::store_stats(&c));
    for result in results {
        result.unwrap();
    }
}
