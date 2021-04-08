use std::time::Duration;

use etcd::kv::{self, Action, GetOptions, KeyValueInfo, WatchError, WatchOptions};
use etcd::Error;

use crate::test::TestClient;

mod test;

#[test]
fn create() {
    let client = TestClient::new();

    let response = client
        .run(|c| kv::create(c, "/test/foo", "bar", Some(60)))
        .unwrap();
    let node = response.data.node;

    assert_eq!(response.data.action, Action::Create);
    assert_eq!(node.value.unwrap(), "bar");
    assert_eq!(node.ttl.unwrap(), 60);
}

#[test]
fn create_does_not_replace_existing_key() {
    let client = TestClient::new();
    client
        .run(|c| kv::create(c, "/test/foo", "bar", Some(60)))
        .unwrap();

    let result = client.run(|c| kv::create(c, "/test/foo", "bar", Some(60)));
    match result {
        Ok(_) => panic!("expected EtcdError due to pre-existing key"),
        Err(errors) => {
            for error in errors {
                match error {
                    Error::Api(ref error) => {
                        assert_eq!(error.message, "Key already exists")
                    }
                    _ => panic!("expected EtcdError due to pre-existing key"),
                }
            }
        }
    }
}

#[test]
fn create_in_order() {
    let client = TestClient::new();
    let results: Result<Vec<_>, _> = (1..4)
        .map(|_| client.run(|c| kv::create_in_order(c, "/test/foo", "bar", None)))
        .collect();
    let results = results.unwrap();
    let mut kvis: Vec<KeyValueInfo> = results.into_iter().map(|response| response.data).collect();
    kvis.sort_by_key(|ref kvi| kvi.node.modified_index);

    let keys: Vec<String> = kvis.into_iter().map(|kvi| kvi.node.key.unwrap()).collect();

    assert!(keys[0] < keys[1]);
    assert!(keys[1] < keys[2]);
}

#[test]
fn create_in_order_must_operate_on_a_directory() {
    let client = TestClient::new();
    client
        .run(|c| kv::create(&c, "/test/foo", "bar", None))
        .unwrap();

    let result = client.run(|c| kv::create_in_order(c, "/test/foo", "baz", None));
    assert!(result.is_err());
}

#[test]
fn compare_and_delete() {
    let client = TestClient::new();
    let res = client
        .run(|c| kv::create(c, "/test/foo", "bar", None))
        .unwrap();
    let index = res.data.node.modified_index;

    let res = client
        .run(|c| kv::compare_and_delete(c, "/test/foo", Some("bar"), index))
        .unwrap();
    assert_eq!(res.data.action, Action::CompareAndDelete);
}

#[test]
fn compare_and_delete_only_index() {
    let client = TestClient::new();
    let res = client
        .run(|c| kv::create(c, "/test/foo", "bar", None))
        .unwrap();
    let index = res.data.node.modified_index;

    let res = client
        .run(|c| kv::compare_and_delete(c, "/test/foo", None, index))
        .unwrap();
    assert_eq!(res.data.action, Action::CompareAndDelete);
}

#[test]
fn compare_and_delete_only_value() {
    let client = TestClient::new();
    client
        .run(|c| kv::create(c, "/test/foo", "bar", None))
        .unwrap();

    let res = client
        .run(|c| kv::compare_and_delete(c, "/test/foo", Some("bar"), None))
        .unwrap();
    assert_eq!(res.data.action, Action::CompareAndDelete);
}

#[test]
fn compare_and_delete_requires_conditions() {
    let client = TestClient::new();

    client
        .run(|c| kv::create(c, "/test/foo", "bar", None))
        .unwrap();

    let result = client.run(|c| kv::compare_and_delete(c, "/test/foo", None, None));
    match result {
        Ok(_) => panic!("expected Error::InvalidConditions"),
        Err(errors) => {
            if errors.len() == 1 {
                match errors[0] {
                    Error::InvalidConditions => {}
                    _ => panic!("expected Error::InvalidConditions"),
                }
            } else {
                panic!("expected a single error: Error::InvalidConditions");
            }
        }
    }
}

#[test]
fn test_compare_and_swap() {
    let client = TestClient::new();

    let res = client
        .run(|c| kv::create(c, "/test/foo", "bar", None))
        .unwrap();
    let index = res.data.node.modified_index;

    let res = client
        .run(|c| kv::compare_and_swap(c, "/test/foo", "baz", Some(100), Some("bar"), index))
        .unwrap();
    assert_eq!(res.data.action, Action::CompareAndSwap);
}

#[test]
fn compare_and_swap_only_index() {
    let client = TestClient::new();

    let res = client
        .run(|c| kv::create(c, "/test/foo", "bar", None))
        .unwrap();
    let index = res.data.node.modified_index;

    let res = client
        .run(|c| kv::compare_and_swap(c, "/test/foo", "baz", None, None, index))
        .unwrap();
    assert_eq!(res.data.action, Action::CompareAndSwap);
}

#[test]
fn compare_and_swap() {
    let client = TestClient::new();

    client
        .run(|c| kv::create(c, "/test/foo", "bar", None))
        .unwrap();

    let res = client
        .run(|c| kv::compare_and_swap(c, "/test/foo", "baz", None, Some("bar"), None))
        .unwrap();
    assert_eq!(res.data.action, Action::CompareAndSwap);
}

#[test]
fn compare_and_swap_requires_conditions() {
    let client = TestClient::new();

    client
        .run(|c| kv::create(c, "/test/foo", "bar", None))
        .unwrap();

    let result = client.run(|c| kv::compare_and_swap(c, "/test/foo", "baz", None, None, None));
    match result {
        Ok(_) => panic!("expected Error::InvalidConditions"),
        Err(errors) => {
            if errors.len() == 1 {
                match errors[0] {
                    Error::InvalidConditions => {}
                    _ => panic!("expected Error::InvalidConditions"),
                }
            } else {
                panic!("expected a single error: Error::InvalidConditions");
            }
        }
    }
}

#[test]
fn get() {
    let client = TestClient::new();

    client
        .run(|c| kv::create(c, "/test/foo", "bar", Some(60)))
        .unwrap();

    let res = client
        .run(|c| kv::get(c, "/test/foo", GetOptions::default()))
        .unwrap();
    assert_eq!(res.data.action, Action::Get);

    let node = res.data.node;
    assert_eq!(node.value.unwrap(), "bar");
    assert_eq!(node.ttl.unwrap(), 60);
}

#[test]
fn get_non_recursive() {
    let client = TestClient::new();

    client
        .run(|c| kv::set(c, "/test/dir/baz", "blah", None))
        .unwrap();
    client
        .run(|c| kv::set(c, "/test/foo", "bar", None))
        .unwrap();

    let res = client
        .run(|c| {
            kv::get(
                c,
                "/test",
                GetOptions {
                    sort: true,
                    ..Default::default()
                },
            )
        })
        .unwrap();

    let node = res.data.node;
    assert_eq!(node.dir.unwrap(), true);

    let nodes = node.nodes.unwrap();
    assert_eq!(nodes[0].clone().key.unwrap(), "/test/dir");
    assert_eq!(nodes[0].clone().dir.unwrap(), true);
    assert_eq!(nodes[1].clone().key.unwrap(), "/test/foo");
    assert_eq!(nodes[1].clone().value.unwrap(), "bar");
}

#[test]
fn get_recursive() {
    let client = TestClient::new();

    client
        .run(|c| kv::set(c, "/test/dir/baz", "blah", None))
        .unwrap();

    let res = client
        .run(|c| {
            kv::get(
                c,
                "/test",
                GetOptions {
                    recursive: true,
                    sort: true,
                    ..Default::default()
                },
            )
        })
        .unwrap();
    let nodes = res.data.node.nodes.unwrap();

    assert_eq!(
        nodes[0].clone().nodes.unwrap()[0].clone().value.unwrap(),
        "blah"
    );
}

#[test]
fn get_root() {
    let client = TestClient::new();

    client
        .run(|c| kv::create(c, "/test/foo", "bar", Some(60)))
        .unwrap();

    let res = client
        .run(|c| kv::get(c, "/", GetOptions::default()))
        .unwrap();
    assert_eq!(res.data.action, Action::Get);

    let node = res.data.node;
    assert!(node.created_index.is_none());
    assert!(node.modified_index.is_none());
    assert_eq!(node.nodes.unwrap().len(), 1);
    assert_eq!(node.dir.unwrap(), true);
}

#[test]
fn https() {
    let client = TestClient::https(true);

    client
        .run(|c| kv::set(c, "/test/foo", "bar", Some(60)))
        .unwrap();
}

#[test]
fn https_without_valid_client_certificate() {
    let client = TestClient::https(false);
    let res = client.run(|c| kv::set(c, "/test/foo", "bar", Some(60)));
    assert!(res.is_err());
}

#[test]
fn set() {
    let client = TestClient::new();

    let res = client
        .run(|c| kv::set(c, "/test/foo", "baz", None))
        .unwrap();
    assert_eq!(res.data.action, Action::Set);

    let node = res.data.node;
    assert_eq!(node.value.unwrap(), "baz");
    assert!(node.ttl.is_none());
}

#[test]
fn set_and_refresh() {
    let client = TestClient::new();

    let res = client
        .run(|c| kv::set(c, "/test/foo", "baz", Some(30)))
        .unwrap();
    assert_eq!(res.data.action, Action::Set);

    let node = res.data.node;
    assert_eq!(node.value.unwrap(), "baz");
    assert!(node.ttl.is_some());

    let res = client.run(|c| kv::refresh(c, "/test/foo", 30)).unwrap();
    assert_eq!(res.data.action, Action::Update);

    let node = res.data.node;
    assert_eq!(node.value.unwrap(), "baz");
    assert!(node.ttl.is_some());
}

#[test]
fn set_dir() {
    let client = TestClient::new();

    client.run(|c| kv::set_dir(c, "/test", None)).unwrap();
    match client.run(|c| kv::set_dir(c, "/test", None)) {
        Ok(_) => panic!("set_dir should fail on an existing dir"),
        Err(_) => {}
    }

    client
        .run(|c| kv::set(c, "/test/foo", "bar", None))
        .unwrap();
    client.run(|c| kv::set_dir(c, "/test/foo", None)).unwrap();
}

#[test]
fn update() {
    let client = TestClient::new();

    client
        .run(|c| kv::create(c, "/test/foo", "bar", None))
        .unwrap();

    let res = client
        .run(|c| kv::update(c, "/test/foo", "blah", Some(30)))
        .unwrap();
    assert_eq!(res.data.action, Action::Update);

    let node = res.data.node;
    assert_eq!(node.value.unwrap(), "blah");
    assert_eq!(node.ttl.unwrap(), 30);
}

#[test]
fn update_requires_existing_key() {
    let client = TestClient::no_destructor();

    match client.run(|c| kv::update(c, "/test/foo", "bar", None)) {
        Err(ref errors) => match errors[0] {
            Error::Api(ref error) => assert_eq!(error.message, "Key not found"),
            _ => panic!("expected EtcdError due to missing key"),
        },
        _ => panic!("expected EtcdError due to missing key"),
    }
}

#[test]
fn update_dir() {
    let client = TestClient::new();

    client.run(|c| kv::create_dir(c, "/test", None)).unwrap();

    let res = client
        .run(|c| kv::update_dir(c, "/test", Some(60)))
        .unwrap();
    assert_eq!(res.data.node.ttl.unwrap(), 60);
}

#[test]
fn update_dir_replaces_key() {
    let client = TestClient::new();
    client
        .run(|c| kv::set(c, "/test/foo", "bar", None))
        .unwrap();
    let res = client
        .run(|c| kv::update_dir(c, "/test/foo", Some(60)))
        .unwrap();

    let node = res.data.node;
    assert_eq!(node.value.unwrap(), "");
    assert_eq!(node.ttl.unwrap(), 60);
}

#[test]
fn update_dir_requires_existing_dir() {
    let client = TestClient::no_destructor();

    let res = client.run(|c| kv::update_dir(c, "/test", None));
    assert!(res.is_err());
}

#[test]
fn delete() {
    let client = TestClient::new();

    client
        .run(|c| kv::create(c, "/test/foo", "bar", None))
        .unwrap();

    let res = client.run(|c| kv::delete(c, "/test/foo", false)).unwrap();
    assert_eq!(res.data.action, Action::Delete);
}

#[test]
fn create_dir() {
    let client = TestClient::new();

    let res = client
        .run(|c| kv::create_dir(c, "/test/dir", None))
        .unwrap();
    assert_eq!(res.data.action, Action::Create);

    let node = res.data.node;
    assert!(node.dir.is_some());
    assert!(node.value.is_none());
}

#[test]
fn delete_dir() {
    let client = TestClient::new();
    client
        .run(|c| kv::create_dir(c, "/test/dir", None))
        .unwrap();
    let res = client.run(|c| kv::delete_dir(c, "/test/dir")).unwrap();
    assert_eq!(res.data.action, Action::Delete);
}

#[test]
fn watch() {
    let client = TestClient::new();
    let create_response = client
        .run(|c| kv::create(&c, "/test/foo", "bar", None))
        .unwrap();
    let set_response = client
        .run(|c| kv::set(c, "/test/foo", "baz", None))
        .unwrap();
    let watch_response = client
        .run(|c| {
            kv::watch(
                c,
                "/test/foo",
                WatchOptions {
                    index: Some(create_response.data.node.created_index.unwrap() + 1),
                    ..Default::default()
                },
            )
        })
        .unwrap();

    assert_eq!(watch_response.data.node.value.unwrap(), "baz");
    assert_eq!(
        watch_response.data.node.modified_index.unwrap(),
        set_response.data.node.modified_index.unwrap()
    );
}

#[test]
fn watch_cancel() {
    let client = TestClient::new();
    client
        .run(|c| kv::create(c, "/test/foo", "bar", None))
        .unwrap();

    let res = client.run(|c| {
        kv::watch(
            c,
            "/test/foo",
            WatchOptions {
                timeout: Some(Duration::from_millis(1)),
                ..Default::default()
            },
        )
    });

    match res {
        Err(WatchError::Timeout) => {}
        _ => panic!("expected WatchError::Timeout"),
    }
}

#[test]
fn watch_index() {
    let client = TestClient::new();
    let res = client
        .run(|c| kv::set(c, "/test/foo", "bar", None))
        .unwrap();
    let index = res.data.node.modified_index;
    let res = client
        .run(|c| {
            kv::watch(
                c,
                "/test/foo",
                WatchOptions {
                    index,
                    ..Default::default()
                },
            )
        })
        .unwrap();
    let node = res.data.node;

    assert_eq!(node.modified_index, index);
    assert_eq!(node.value.unwrap(), "bar");
}

#[test]
fn watch_recursive() {
    let client = TestClient::new();

    let watch_result = client
        .run(|c| async move {
            let task_c = c.clone();
            let set_handle = tokio::task::spawn(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                kv::set(&task_c, "/test/foo/bar", "baz", None)
                    .await
                    .unwrap();
            });

            let watch_result = kv::watch(
                c,
                "/test",
                WatchOptions {
                    recursive: true,
                    timeout: Some(Duration::from_millis(1000)),
                    ..Default::default()
                },
            )
            .await;

            // Ensure the set has run, and has not panicked.
            set_handle.await.unwrap();

            watch_result
        })
        .unwrap();

    let node = watch_result.data.node;
    assert_eq!(node.key.unwrap(), "/test/foo/bar");
    assert_eq!(node.value.unwrap(), "baz");
}
