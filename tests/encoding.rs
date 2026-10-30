use msgpack_rpc_peer::{Config, Value, encode_value};

#[test]
fn dynamic_encoding_preserves_string_tags_and_enforces_exact_budgets() {
    let invalid = rmpv::decode::read_value(&mut b"\xa1\xff".as_slice()).unwrap();
    let value = Value::Array(vec![invalid]);
    let config = Config {
        max_outgoing_bytes: 3,
        max_depth: 1,
        max_nodes: 2,
        ..Config::default()
    };
    assert_eq!(encode_value(&value, &config).unwrap(), b"\x91\xa1\xff");
    assert!(
        encode_value(
            &value,
            &Config {
                max_outgoing_bytes: 2,
                ..config.clone()
            }
        )
        .is_err()
    );
    assert!(
        encode_value(
            &value,
            &Config {
                max_nodes: 1,
                ..config.clone()
            }
        )
        .is_err()
    );
    assert!(encode_value(&Value::Array(vec![value]), &config).is_err());
}
