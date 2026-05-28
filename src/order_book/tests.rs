use super::{
    ExecInstruction, FillRecord, OrderRecord, OrderSide, OrderStatus, OrderType,
    SelfTradePrevention, TimeInForce, key_client_id, key_open_order, key_order, key_owner_nonce,
    key_trade, prefix_range_end, read_open_orders, read_recent_trades, serialize, u256_to_be,
};
use crate::error::AedbError;
use crate::storage::keyspace::Keyspace;
use primitive_types::U256;

fn encode_fill(fill_id: u64, seq: u64) -> Vec<u8> {
    rmp_serde::to_vec(&FillRecord {
        fill_id,
        instrument: "BTC-USD".to_string(),
        price_ticks: 100 + fill_id as i64,
        qty_be: {
            let mut out = [0u8; 32];
            out[31] = 1;
            out
        },
        aggressor_order_id: fill_id,
        aggressor_owner: "agg".to_string(),
        aggressor_side: OrderSide::Bid,
        passive_order_id: fill_id + 10,
        passive_owner: "pas".to_string(),
        seq,
    })
    .expect("encode fill")
}

#[test]
fn read_recent_trades_returns_latest_n_in_order() {
    let mut ks = Keyspace::default();
    for i in 1..=5u64 {
        ks.kv_set(
            "p",
            "app",
            key_trade("BTC-USD", i),
            encode_fill(i, 100 + i),
            100 + i,
        )
        .expect("set trade");
    }
    let snapshot = ks.snapshot();
    let recent =
        read_recent_trades(&snapshot, "p", "app", "BTC-USD", 3).expect("read recent trades");
    let ids: Vec<u64> = recent.into_iter().map(|f| f.fill_id).collect();
    assert_eq!(ids, vec![3, 4, 5]);
}

#[test]
fn read_recent_trades_limit_zero_is_empty() {
    let mut ks = Keyspace::default();
    ks.kv_set(
        "p",
        "app",
        key_trade("BTC-USD", 1),
        encode_fill(1, 101),
        101,
    )
    .expect("set trade");
    let snapshot = ks.snapshot();
    let recent =
        read_recent_trades(&snapshot, "p", "app", "BTC-USD", 0).expect("read recent trades");
    assert!(recent.is_empty());
}

#[test]
fn read_recent_trades_isolated_by_instrument_prefix() {
    let mut ks = Keyspace::default();
    for i in 1..=3u64 {
        ks.kv_set(
            "p",
            "app",
            key_trade("BTC-USD", i),
            encode_fill(i, 100 + i),
            100 + i,
        )
        .expect("set btc trade");
        ks.kv_set(
            "p",
            "app",
            key_trade("ETH-USD", i),
            encode_fill(100 + i, 200 + i),
            200 + i,
        )
        .expect("set eth trade");
    }
    let snapshot = ks.snapshot();
    let recent =
        read_recent_trades(&snapshot, "p", "app", "BTC-USD", 10).expect("read recent trades");
    let ids: Vec<u64> = recent.into_iter().map(|f| f.fill_id).collect();
    assert_eq!(ids, vec![1, 2, 3]);
}

#[test]
fn read_recent_trades_rejects_malformed_trade_payload() {
    let mut ks = Keyspace::default();
    ks.kv_set("p", "app", key_trade("BTC-USD", 1), vec![0, 1, 2, 3], 101)
        .expect("set malformed trade");
    let snapshot = ks.snapshot();
    let err = read_recent_trades(&snapshot, "p", "app", "BTC-USD", 1)
        .expect_err("malformed payload should fail decode");
    assert!(matches!(err, AedbError::Decode(_)));
}

#[test]
fn prefix_range_end_handles_regular_and_all_ff_prefixes() {
    let regular = vec![0x6f, 0x62, 0x3a, 0x00, 0x7f];
    let end = prefix_range_end(&regular).expect("regular prefix end");
    assert!(end > regular);

    let all_ff = vec![0xff, 0xff, 0xff];
    assert!(
        prefix_range_end(&all_ff).is_none(),
        "all-0xff prefix has no finite upper bound"
    );
}

#[test]
fn key_client_id_is_collision_resistant_for_delimiter_inputs() {
    let k1 = key_client_id("BTC-USD", "alice:desk", "order-1");
    let k2 = key_client_id("BTC-USD", "alice", "desk:order-1");
    assert_ne!(k1, k2, "length-prefixed encoding must be unambiguous");
}

#[test]
fn key_owner_nonce_is_collision_resistant_for_instrument_alias_inputs() {
    let k1 = key_owner_nonce("X:nonce:alice", "bob");
    let k2 = key_owner_nonce("X", "alice:nonce:bob");
    assert_ne!(k1, k2, "instrument encoding must not alias nonce owners");
}

#[test]
fn read_open_orders_enforces_owner_for_matching_prefix() {
    let mut ks = Keyspace::default();
    let order_a = OrderRecord {
        order_id: 1,
        instrument: "BTC-USD".into(),
        client_order_id: "cid-a".into(),
        owner: "alice".into(),
        account: None,
        side: OrderSide::Bid,
        order_type: OrderType::Limit,
        time_in_force: TimeInForce::Gtc,
        exec_instructions: ExecInstruction(0),
        self_trade_prevention: SelfTradePrevention::None,
        price_ticks: 100,
        original_qty_be: u256_to_be(U256::from(1u8)),
        remaining_qty_be: u256_to_be(U256::from(1u8)),
        filled_qty_be: u256_to_be(U256::zero()),
        status: OrderStatus::Open,
        placed_seq: 1,
        last_modified_seq: 1,
        nonce: 1,
    };
    let order_b = OrderRecord {
        order_id: 2,
        owner: "alice:desk".into(),
        client_order_id: "cid-b".into(),
        ..order_a.clone()
    };
    ks.kv_set(
        "p",
        "app",
        key_order("BTC-USD", order_a.order_id),
        serialize(&order_a).expect("encode"),
        1,
    )
    .expect("set order a");
    ks.kv_set(
        "p",
        "app",
        key_order("BTC-USD", order_b.order_id),
        serialize(&order_b).expect("encode"),
        1,
    )
    .expect("set order b");
    ks.kv_set(
        "p",
        "app",
        key_open_order("BTC-USD", &order_a.owner, order_a.order_id),
        vec![1],
        1,
    )
    .expect("set open order a");
    ks.kv_set(
        "p",
        "app",
        key_open_order("BTC-USD", &order_b.owner, order_b.order_id),
        vec![1],
        1,
    )
    .expect("set open order b");

    let snapshot = ks.snapshot();
    let open =
        read_open_orders(&snapshot, "p", "app", "BTC-USD", "alice").expect("read open orders");
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].owner, "alice");
}
