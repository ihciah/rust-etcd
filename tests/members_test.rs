use etcd::members;

use crate::test::TestClient;

mod test;

#[test]
fn list() {
    let client = TestClient::no_destructor();
    let res = client.run(|c| members::list(c)).unwrap();
    let members = res.data;
    let member = &members[0];
    assert_eq!(member.name, "default");
}
