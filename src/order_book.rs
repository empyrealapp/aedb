use crate::error::AedbError;
use crate::storage::keyspace::{Keyspace, KeyspaceSnapshot};
use primitive_types::U256;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum OrderSide {
    Bid = 0,
    Ask = 1,
}

impl OrderSide {
    pub fn as_u8(self) -> u8 {
        match self {
            Self::Bid => 0,
            Self::Ask => 1,
        }
    }

    pub fn opposite(self) -> Self {
        match self {
            Self::Bid => Self::Ask,
            Self::Ask => Self::Bid,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[repr(u8)]
pub enum OrderType {
    Limit = 0,
    Market = 1,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[repr(u8)]
pub enum TimeInForce {
    Gtc = 0,
    Ioc = 1,
    Fok = 2,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[repr(u8)]
pub enum SelfTradePrevention {
    None = 0,
    CancelResting = 1,
    CancelAggressor = 2,
    CancelBoth = 3,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[repr(u8)]
pub enum OrderStatus {
    Open = 0,
    PartiallyFilled = 1,
    Filled = 2,
    Cancelled = 3,
    Rejected = 4,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[repr(u8)]
pub enum OrderBookTableMode {
    PerAsset = 0,
    MultiAsset = 1,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrderBookTableSpec {
    pub table_id: String,
    pub mode: OrderBookTableMode,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BalanceConfig {
    pub base_balance_key: String,
    pub quote_balance_key: String,
    pub escrow_on_place: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InstrumentConfig {
    pub instrument: String,
    pub tick_size: i64,
    pub lot_size_be: [u8; 32],
    pub min_price_ticks: i64,
    pub max_price_ticks: i64,
    pub market_order_price_band: Option<i64>,
    pub halted: bool,
    pub balance_config: Option<BalanceConfig>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecInstruction(pub u16);

impl ExecInstruction {
    pub const POST_ONLY: u16 = 0b0000_0001;

    pub fn post_only(self) -> bool {
        (self.0 & Self::POST_ONLY) != 0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrderRequest {
    pub instrument: String,
    pub client_order_id: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub exec_instructions: ExecInstruction,
    pub self_trade_prevention: SelfTradePrevention,
    pub price_ticks: i64,
    pub qty_be: [u8; 32],
    pub owner: String,
    pub account: Option<String>,
    pub nonce: u64,
    pub price_limit_ticks: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FillSpec {
    pub aggressor_order_id: u64,
    pub passive_order_id: u64,
    pub price_ticks: i64,
    pub qty_be: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FillRecord {
    pub fill_id: u64,
    pub instrument: String,
    pub price_ticks: i64,
    pub qty_be: [u8; 32],
    pub aggressor_order_id: u64,
    pub aggressor_owner: String,
    pub aggressor_side: OrderSide,
    pub passive_order_id: u64,
    pub passive_owner: String,
    pub seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RejectReason {
    InsufficientBalance,
    PostOnlyWouldCross,
    FokCannotFill,
    SelfTradeBlocked,
    InvalidPrice,
    InvalidQuantity,
    DuplicateClientOrderId,
    NonceTooLow,
    InstrumentNotFound,
    InstrumentHalted,
    MarketOrderNoLiquidity,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionReport {
    pub order_id: u64,
    pub client_order_id: String,
    pub status: OrderStatus,
    pub fills: Vec<FillRecord>,
    pub remaining_qty_be: [u8; 32],
    pub total_filled_qty_be: [u8; 32],
    pub avg_price_ticks: Option<i64>,
    pub reject_reason: Option<RejectReason>,
    pub seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrderRecord {
    pub order_id: u64,
    pub instrument: String,
    pub client_order_id: String,
    pub owner: String,
    pub account: Option<String>,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub exec_instructions: ExecInstruction,
    pub self_trade_prevention: SelfTradePrevention,
    pub price_ticks: i64,
    pub original_qty_be: [u8; 32],
    pub remaining_qty_be: [u8; 32],
    pub filled_qty_be: [u8; 32],
    pub status: OrderStatus,
    pub placed_seq: u64,
    pub last_modified_seq: u64,
    pub nonce: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriceLevel {
    pub price_ticks: i64,
    pub total_qty_be: [u8; 32],
    pub order_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderBookDepth {
    pub instrument: String,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spread {
    pub best_bid: Option<i64>,
    pub best_ask: Option<i64>,
    pub mid: Option<i64>,
    pub seq: u64,
}

pub fn u256_from_be(bytes: [u8; 32]) -> U256 {
    U256::from_big_endian(&bytes)
}

pub fn u256_to_be(v: U256) -> [u8; 32] {
    let mut out = [0u8; 32];
    v.to_big_endian(&mut out);
    out
}

pub fn encode_i64_ordered(v: i64) -> [u8; 8] {
    ((v as u64) ^ (1u64 << 63)).to_be_bytes()
}

pub fn decode_i64_ordered(bytes: [u8; 8]) -> i64 {
    (u64::from_be_bytes(bytes) ^ (1u64 << 63)) as i64
}

fn encode_price_component(side: OrderSide, price_ticks: i64) -> [u8; 8] {
    let mut p = encode_i64_ordered(price_ticks);
    if matches!(side, OrderSide::Bid) {
        for b in &mut p {
            *b = !*b;
        }
    }
    p
}

fn decode_price_component(side: OrderSide, mut enc: [u8; 8]) -> i64 {
    if matches!(side, OrderSide::Bid) {
        for b in &mut enc {
            *b = !*b;
        }
    }
    decode_i64_ordered(enc)
}

pub fn encode_order_id(order_id: u64) -> [u8; 8] {
    order_id.to_be_bytes()
}

pub fn decode_order_id(bytes: [u8; 8]) -> u64 {
    u64::from_be_bytes(bytes)
}

pub fn key_next_order_id(instrument: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(17 + instrument.len());
    k.extend_from_slice(b"ob:");
    k.extend_from_slice(instrument.as_bytes());
    k.extend_from_slice(b":meta:next_oid");
    k
}

pub fn key_order_book_table_spec(table_id: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(7 + table_id.len());
    k.extend_from_slice(b"ob:def:");
    k.extend_from_slice(table_id.as_bytes());
    k
}

pub fn scoped_instrument(table_id: &str, asset_id: &str) -> String {
    let mut scoped = String::with_capacity(table_id.len() + 1 + asset_id.len());
    scoped.push_str(table_id);
    scoped.push('/');
    scoped.push_str(asset_id);
    scoped
}

pub fn parse_scoped_instrument(scoped: &str) -> Option<(&str, &str)> {
    scoped.split_once('/')
}

pub fn apply_order_book_define_table(
    keyspace: &mut Keyspace,
    project_id: &str,
    scope_id: &str,
    table_id: &str,
    mode: OrderBookTableMode,
    commit_seq: u64,
) -> Result<(), AedbError> {
    let spec = OrderBookTableSpec {
        table_id: table_id.to_string(),
        mode,
    };
    keyspace.kv_set(
        project_id,
        scope_id,
        key_order_book_table_spec(table_id),
        serialize(&spec)?,
        commit_seq,
    );
    Ok(())
}

pub fn apply_order_book_drop_table(
    keyspace: &mut Keyspace,
    project_id: &str,
    scope_id: &str,
    table_id: &str,
    commit_seq: u64,
) -> Result<(), AedbError> {
    keyspace.kv_del(
        project_id,
        scope_id,
        &key_order_book_table_spec(table_id),
        commit_seq,
    );
    Ok(())
}

pub fn apply_set_instrument_config(
    keyspace: &mut Keyspace,
    project_id: &str,
    scope_id: &str,
    instrument: &str,
    config: &InstrumentConfig,
    commit_seq: u64,
) -> Result<(), AedbError> {
    if config.instrument != instrument {
        return Err(AedbError::Validation(
            "instrument config instrument mismatch".into(),
        ));
    }
    if config.tick_size <= 0 {
        return Err(AedbError::Validation("tick_size must be > 0".into()));
    }
    if config.min_price_ticks > config.max_price_ticks {
        return Err(AedbError::Validation("invalid min/max price ticks".into()));
    }
    if u256_from_be(config.lot_size_be).is_zero() {
        return Err(AedbError::Validation("lot_size must be > 0".into()));
    }
    keyspace.kv_set(
        project_id,
        scope_id,
        key_instrument_config(instrument),
        serialize(config)?,
        commit_seq,
    );
    Ok(())
}

pub fn apply_set_instrument_halted(
    keyspace: &mut Keyspace,
    project_id: &str,
    scope_id: &str,
    instrument: &str,
    halted: bool,
    commit_seq: u64,
) -> Result<(), AedbError> {
    let mut config = load_instrument_config(keyspace, project_id, scope_id, instrument)?
        .unwrap_or_else(|| InstrumentConfig {
            instrument: instrument.to_string(),
            tick_size: 1,
            lot_size_be: u256_to_be(U256::one()),
            min_price_ticks: i64::MIN,
            max_price_ticks: i64::MAX,
            market_order_price_band: None,
            halted: false,
            balance_config: None,
        });
    config.halted = halted;
    keyspace.kv_set(
        project_id,
        scope_id,
        key_instrument_config(instrument),
        serialize(&config)?,
        commit_seq,
    );
    Ok(())
}

pub fn key_next_fill_id(instrument: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(18 + instrument.len());
    k.extend_from_slice(b"ob:");
    k.extend_from_slice(instrument.as_bytes());
    k.extend_from_slice(b":meta:next_fill");
    k
}

pub fn key_instrument_config(instrument: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(7 + instrument.len());
    k.extend_from_slice(b"ob:");
    k.extend_from_slice(instrument.as_bytes());
    k.extend_from_slice(b":cfg");
    k
}

pub fn key_execution_report(instrument: &str, commit_seq: u64, order_id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(12 + instrument.len() + 1 + 8 + 1 + 8);
    key.extend_from_slice(b"ob:");
    key.extend_from_slice(instrument.as_bytes());
    key.extend_from_slice(b":report:");
    key.extend_from_slice(&commit_seq.to_be_bytes());
    key.push(b':');
    key.extend_from_slice(&order_id.to_be_bytes());
    key
}

pub fn key_execution_report_last(instrument: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(12 + instrument.len());
    k.extend_from_slice(b"ob:");
    k.extend_from_slice(instrument.as_bytes());
    k.extend_from_slice(b":report:last");
    k
}

pub fn key_owner_nonce(instrument: &str, owner: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(10 + instrument.len() + owner.len());
    k.extend_from_slice(b"ob:");
    k.extend_from_slice(instrument.as_bytes());
    k.extend_from_slice(b":nonce:");
    k.extend_from_slice(owner.as_bytes());
    k
}

pub fn key_client_id(instrument: &str, owner: &str, client_order_id: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(9 + instrument.len() + owner.len() + client_order_id.len());
    k.extend_from_slice(b"ob:");
    k.extend_from_slice(instrument.as_bytes());
    k.extend_from_slice(b":cid:");
    k.extend_from_slice(owner.as_bytes());
    k.push(b':');
    k.extend_from_slice(client_order_id.as_bytes());
    k
}

pub fn key_order(instrument: &str, order_id: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(7 + instrument.len() + 8);
    k.extend_from_slice(b"ob:");
    k.extend_from_slice(instrument.as_bytes());
    k.extend_from_slice(b":ord:");
    k.extend_from_slice(&encode_order_id(order_id));
    k
}

pub fn key_plqty(instrument: &str, side: OrderSide, price_ticks: i64) -> Vec<u8> {
    let mut k = Vec::with_capacity(12 + instrument.len() + 8);
    k.extend_from_slice(b"ob:");
    k.extend_from_slice(instrument.as_bytes());
    k.extend_from_slice(b":plqty:");
    k.push(b'0' + side.as_u8());
    k.push(b':');
    k.extend_from_slice(&encode_price_component(side, price_ticks));
    k
}

pub fn key_fifo(
    instrument: &str,
    side: OrderSide,
    price_ticks: i64,
    placed_seq: u64,
    order_id: u64,
) -> Vec<u8> {
    let mut k = Vec::with_capacity(11 + instrument.len() + 8 + 1 + 8 + 1 + 8);
    k.extend_from_slice(b"ob:");
    k.extend_from_slice(instrument.as_bytes());
    k.extend_from_slice(b":fifo:");
    k.push(b'0' + side.as_u8());
    k.push(b':');
    k.extend_from_slice(&encode_price_component(side, price_ticks));
    k.push(b':');
    k.extend_from_slice(&placed_seq.to_be_bytes());
    k.push(b':');
    k.extend_from_slice(&order_id.to_be_bytes());
    k
}

pub fn key_open_order(instrument: &str, owner: &str, order_id: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(11 + instrument.len() + owner.len() + 8);
    k.extend_from_slice(b"ob:");
    k.extend_from_slice(instrument.as_bytes());
    k.extend_from_slice(b":open:");
    k.extend_from_slice(owner.as_bytes());
    k.push(b':');
    k.extend_from_slice(&order_id.to_be_bytes());
    k
}

pub fn key_trade(instrument: &str, fill_id: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(9 + instrument.len() + 8);
    k.extend_from_slice(b"ob:");
    k.extend_from_slice(instrument.as_bytes());
    k.extend_from_slice(b":trade:");
    k.extend_from_slice(&fill_id.to_be_bytes());
    k
}

pub fn trade_prefix(instrument: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(9 + instrument.len());
    k.extend_from_slice(b"ob:");
    k.extend_from_slice(instrument.as_bytes());
    k.extend_from_slice(b":trade:");
    k
}

pub fn fifo_prefix(instrument: &str, side: OrderSide, price_ticks: i64) -> Vec<u8> {
    let mut k = Vec::with_capacity(11 + instrument.len() + 8 + 1);
    k.extend_from_slice(b"ob:");
    k.extend_from_slice(instrument.as_bytes());
    k.extend_from_slice(b":fifo:");
    k.push(b'0' + side.as_u8());
    k.push(b':');
    k.extend_from_slice(&encode_price_component(side, price_ticks));
    k.push(b':');
    k
}

pub fn plqty_prefix(instrument: &str, side: OrderSide) -> Vec<u8> {
    let mut k = Vec::with_capacity(11 + instrument.len());
    k.extend_from_slice(b"ob:");
    k.extend_from_slice(instrument.as_bytes());
    k.extend_from_slice(b":plqty:");
    k.push(b'0' + side.as_u8());
    k.push(b':');
    k
}

pub fn open_orders_prefix(instrument: &str, owner: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(10 + instrument.len() + owner.len());
    k.extend_from_slice(b"ob:");
    k.extend_from_slice(instrument.as_bytes());
    k.extend_from_slice(b":open:");
    k.extend_from_slice(owner.as_bytes());
    k.push(b':');
    k
}

pub fn all_orders_prefix(instrument: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(9 + instrument.len());
    k.extend_from_slice(b"ob:");
    k.extend_from_slice(instrument.as_bytes());
    k.extend_from_slice(b":ord:");
    k
}

pub fn parse_plqty_price(side: OrderSide, key: &[u8]) -> Option<i64> {
    let marker = {
        let mut m = Vec::with_capacity(9);
        m.extend_from_slice(b":plqty:");
        m.push(b'0' + side.as_u8());
        m.push(b':');
        m
    };
    let pos = key.windows(marker.len()).position(|w| w == marker)?;
    let start = pos + marker.len();
    let bytes: [u8; 8] = key.get(start..start + 8)?.try_into().ok()?;
    Some(decode_price_component(side, bytes))
}

pub fn parse_fifo_order_id(key: &[u8]) -> Option<u64> {
    let bytes: [u8; 8] = key.get(key.len().checked_sub(8)?..)?.try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}

pub fn parse_order_id_suffix(key: &[u8]) -> Option<u64> {
    let bytes: [u8; 8] = key.get(key.len().checked_sub(8)?..)?.try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}

pub fn apply_order_book_new(
    keyspace: &mut Keyspace,
    project_id: &str,
    scope_id: &str,
    request: &OrderRequest,
    commit_seq: u64,
) -> Result<(), AedbError> {
    validate_instrument_against_table_mode(keyspace, project_id, scope_id, &request.instrument)?;
    let config = load_instrument_config(keyspace, project_id, scope_id, &request.instrument)?;
    if let Some(cfg) = &config {
        if cfg.halted {
            return Err(AedbError::Validation("instrument halted".into()));
        }
        if request.price_ticks < cfg.min_price_ticks || request.price_ticks > cfg.max_price_ticks {
            return Err(AedbError::Validation(
                "price outside instrument bounds".into(),
            ));
        }
        let lot = u256_from_be(cfg.lot_size_be);
        let qty = u256_from_be(request.qty_be);
        if !lot.is_zero() && (qty % lot) != U256::zero() {
            return Err(AedbError::Validation("quantity violates lot size".into()));
        }
    }
    let effective_request = if let Some(cfg) = &config {
        effective_request_for_config(request, cfg, keyspace, project_id, scope_id)?
    } else {
        request.clone()
    };
    let qty = u256_from_be(request.qty_be);
    if qty.is_zero() {
        return Err(AedbError::Validation("qty must be > 0".into()));
    }
    if request.exec_instructions.post_only() && matches!(request.order_type, OrderType::Market) {
        return Err(AedbError::Validation(
            "post_only is invalid for market orders".into(),
        ));
    }
    let nonce_key = key_owner_nonce(&request.instrument, &request.owner);
    let last_nonce = load_u64(keyspace, project_id, scope_id, &nonce_key)?.unwrap_or(0);
    if request.nonce <= last_nonce {
        return Err(AedbError::Validation("nonce too low".into()));
    }
    let client_key = key_client_id(
        &request.instrument,
        &request.owner,
        &request.client_order_id,
    );
    if keyspace.kv_get(project_id, scope_id, &client_key).is_some() {
        return Err(AedbError::Validation("duplicate client_order_id".into()));
    }

    if effective_request.exec_instructions.post_only() {
        if let Some(best_price) = best_price_for_side(
            keyspace,
            project_id,
            scope_id,
            &effective_request.instrument,
            effective_request.side.opposite(),
        ) && crosses(
            effective_request.side,
            effective_request.price_ticks,
            best_price,
        ) {
            return Err(AedbError::Validation("post_only would cross".into()));
        }
    }

    if matches!(effective_request.time_in_force, TimeInForce::Fok)
        && !can_fok_fill(keyspace, project_id, scope_id, &effective_request)?
    {
        return Err(AedbError::Validation("fok cannot fill".into()));
    }
    if matches!(effective_request.order_type, OrderType::Market)
        && best_price_for_side(
            keyspace,
            project_id,
            scope_id,
            &effective_request.instrument,
            effective_request.side.opposite(),
        )
        .is_none()
    {
        return Err(AedbError::Validation(
            "market order has no liquidity".into(),
        ));
    }

    let order_id = allocate_next_id(
        keyspace,
        project_id,
        scope_id,
        &key_next_order_id(&effective_request.instrument),
        commit_seq,
    )?;

    let mut remaining = qty;
    let mut filled = U256::zero();
    let mut execution_fills = Vec::new();
    let mut preferred_passive_price: Option<i64> = None;

    while !remaining.is_zero() {
        let (best_price, mut passive) = if let Some(price) = preferred_passive_price {
            if let Some(order) = first_passive_order_at_price(
                keyspace,
                project_id,
                scope_id,
                &effective_request.instrument,
                effective_request.side.opposite(),
                price,
                &effective_request,
            )? {
                (price, order)
            } else {
                let Some((price, order)) = best_passive_order(
                    keyspace,
                    project_id,
                    scope_id,
                    &effective_request.instrument,
                    effective_request.side.opposite(),
                    &effective_request,
                )?
                else {
                    break;
                };
                (price, order)
            }
        } else {
            let Some((price, order)) = best_passive_order(
                keyspace,
                project_id,
                scope_id,
                &effective_request.instrument,
                effective_request.side.opposite(),
                &effective_request,
            )?
            else {
                break;
            };
            (price, order)
        };

        let passive_remaining = u256_from_be(passive.remaining_qty_be);
        if passive_remaining.is_zero() {
            clear_open_order(keyspace, project_id, scope_id, &passive)?;
            preferred_passive_price = None;
            continue;
        }
        if passive.owner == effective_request.owner {
            match effective_request.self_trade_prevention {
                SelfTradePrevention::None => {}
                SelfTradePrevention::CancelResting => {
                    apply_order_book_cancel(
                        keyspace,
                        project_id,
                        scope_id,
                        &effective_request.instrument,
                        passive.order_id,
                        Some(passive.client_order_id.as_str()),
                        &passive.owner,
                        commit_seq,
                    )?;
                    continue;
                }
                SelfTradePrevention::CancelAggressor => {
                    break;
                }
                SelfTradePrevention::CancelBoth => {
                    apply_order_book_cancel(
                        keyspace,
                        project_id,
                        scope_id,
                        &effective_request.instrument,
                        passive.order_id,
                        Some(passive.client_order_id.as_str()),
                        &passive.owner,
                        commit_seq,
                    )?;
                    break;
                }
            }
        }
        let fill_qty = if remaining < passive_remaining {
            remaining
        } else {
            passive_remaining
        };

        apply_passive_fill(
            keyspace,
            project_id,
            scope_id,
            &effective_request.instrument,
            &mut passive,
            fill_qty,
            commit_seq,
        )?;
        let fill = write_fill(
            keyspace,
            project_id,
            scope_id,
            &effective_request.instrument,
            &effective_request,
            order_id,
            &passive,
            best_price,
            fill_qty,
            commit_seq,
        )?;
        execution_fills.push(fill);
        remaining -= fill_qty;
        filled += fill_qty;
        let level_key = key_plqty(
            &effective_request.instrument,
            effective_request.side.opposite(),
            best_price,
        );
        preferred_passive_price = keyspace
            .kv_get(project_id, scope_id, &level_key)
            .and_then(|entry| decode_u256_bytes(&entry.value).ok())
            .filter(|qty| !qty.is_zero())
            .map(|_| best_price);
    }

    let remaining_after_match = remaining;
    if matches!(effective_request.order_type, OrderType::Market) && filled.is_zero() {
        return Err(AedbError::Validation(
            "market order has no liquidity".into(),
        ));
    }
    let rest_on_book = matches!(effective_request.order_type, OrderType::Limit)
        && matches!(effective_request.time_in_force, TimeInForce::Gtc)
        && !remaining_after_match.is_zero();
    let stored_remaining = if rest_on_book {
        remaining_after_match
    } else {
        U256::zero()
    };

    let status = if remaining_after_match.is_zero() {
        OrderStatus::Filled
    } else if rest_on_book {
        if filled.is_zero() {
            OrderStatus::Open
        } else {
            OrderStatus::PartiallyFilled
        }
    } else {
        if filled.is_zero() {
            OrderStatus::Cancelled
        } else {
            OrderStatus::PartiallyFilled
        }
    };

    let record = OrderRecord {
        order_id,
        instrument: effective_request.instrument.clone(),
        client_order_id: effective_request.client_order_id.clone(),
        owner: effective_request.owner.clone(),
        account: effective_request.account.clone(),
        side: effective_request.side,
        order_type: effective_request.order_type,
        time_in_force: effective_request.time_in_force,
        exec_instructions: effective_request.exec_instructions,
        self_trade_prevention: effective_request.self_trade_prevention,
        price_ticks: effective_request.price_ticks,
        original_qty_be: effective_request.qty_be,
        remaining_qty_be: u256_to_be(stored_remaining),
        filled_qty_be: u256_to_be(filled),
        status,
        placed_seq: commit_seq,
        last_modified_seq: commit_seq,
        nonce: effective_request.nonce,
    };
    store_order(keyspace, project_id, scope_id, &record, commit_seq)?;

    if rest_on_book {
        keyspace.kv_set(
            project_id,
            scope_id,
            key_fifo(
                &effective_request.instrument,
                effective_request.side,
                effective_request.price_ticks,
                record.placed_seq,
                order_id,
            ),
            vec![1],
            commit_seq,
        );
        keyspace.kv_inc_u256(
            project_id,
            scope_id,
            key_plqty(
                &effective_request.instrument,
                effective_request.side,
                effective_request.price_ticks,
            ),
            remaining,
            commit_seq,
        )?;
        keyspace.kv_set(
            project_id,
            scope_id,
            key_open_order(
                &effective_request.instrument,
                &effective_request.owner,
                order_id,
            ),
            vec![1],
            commit_seq,
        );
    }

    keyspace.kv_set(
        project_id,
        scope_id,
        key_client_id(
            &effective_request.instrument,
            &effective_request.owner,
            &effective_request.client_order_id,
        ),
        order_id.to_be_bytes().to_vec(),
        commit_seq,
    );
    keyspace.kv_set(
        project_id,
        scope_id,
        key_owner_nonce(&effective_request.instrument, &effective_request.owner),
        effective_request.nonce.to_be_bytes().to_vec(),
        commit_seq,
    );
    write_execution_report(
        keyspace,
        project_id,
        scope_id,
        &ExecutionReport {
            order_id,
            client_order_id: effective_request.client_order_id.clone(),
            status,
            fills: execution_fills,
            remaining_qty_be: u256_to_be(stored_remaining),
            total_filled_qty_be: u256_to_be(filled),
            avg_price_ticks: None,
            reject_reason: None,
            seq: commit_seq,
        },
        &effective_request.instrument,
        commit_seq,
    )
}

pub fn apply_order_book_cancel(
    keyspace: &mut Keyspace,
    project_id: &str,
    scope_id: &str,
    instrument: &str,
    order_id: u64,
    client_order_id: Option<&str>,
    owner: &str,
    commit_seq: u64,
) -> Result<(), AedbError> {
    let resolved_order_id = if let Some(cid) = client_order_id {
        match load_client_order_id(keyspace, project_id, scope_id, instrument, owner, cid)? {
            Some(id) => id,
            None => return Ok(()),
        }
    } else {
        order_id
    };
    if resolved_order_id == 0 {
        return Ok(());
    }
    let Some(mut order) = load_order(
        keyspace,
        project_id,
        scope_id,
        instrument,
        resolved_order_id,
    )?
    else {
        return Ok(());
    };
    if order.owner != owner {
        return Err(AedbError::PermissionDenied(
            "order ownership mismatch".into(),
        ));
    }
    let remaining = u256_from_be(order.remaining_qty_be);
    if !remaining.is_zero() {
        dec_price_level_qty(
            keyspace,
            project_id,
            scope_id,
            instrument,
            order.side,
            order.price_ticks,
            remaining,
            commit_seq,
        )?;
        keyspace.kv_del(
            project_id,
            scope_id,
            &key_fifo(
                instrument,
                order.side,
                order.price_ticks,
                order.placed_seq,
                resolved_order_id,
            ),
            commit_seq,
        );
        keyspace.kv_del(
            project_id,
            scope_id,
            &key_open_order(instrument, &order.owner, order.order_id),
            commit_seq,
        );
    }
    order.remaining_qty_be = u256_to_be(U256::zero());
    order.status = OrderStatus::Cancelled;
    order.last_modified_seq = commit_seq;
    store_order(keyspace, project_id, scope_id, &order, commit_seq)?;
    write_execution_report(
        keyspace,
        project_id,
        scope_id,
        &ExecutionReport {
            order_id: order.order_id,
            client_order_id: order.client_order_id.clone(),
            status: OrderStatus::Cancelled,
            fills: Vec::new(),
            remaining_qty_be: u256_to_be(U256::zero()),
            total_filled_qty_be: order.filled_qty_be,
            avg_price_ticks: None,
            reject_reason: None,
            seq: commit_seq,
        },
        instrument,
        commit_seq,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn apply_order_book_cancel_replace(
    keyspace: &mut Keyspace,
    project_id: &str,
    scope_id: &str,
    instrument: &str,
    order_id: u64,
    owner: &str,
    new_price_ticks: Option<i64>,
    new_qty_be: Option<[u8; 32]>,
    new_time_in_force: Option<TimeInForce>,
    new_exec_instructions: Option<ExecInstruction>,
    commit_seq: u64,
) -> Result<(), AedbError> {
    let Some(mut order) = load_order(keyspace, project_id, scope_id, instrument, order_id)? else {
        return Ok(());
    };
    if order.owner != owner {
        return Err(AedbError::PermissionDenied(
            "order ownership mismatch".into(),
        ));
    }
    let mut remaining = u256_from_be(order.remaining_qty_be);
    let old_remaining = remaining;
    let old_price = order.price_ticks;
    let old_side = order.side;
    let old_placed_seq = order.placed_seq;

    if let Some(qty_be) = new_qty_be {
        let target = u256_from_be(qty_be);
        if target.is_zero() {
            return apply_order_book_cancel(
                keyspace, project_id, scope_id, instrument, order_id, None, owner, commit_seq,
            );
        }
        remaining = target;
    }
    if let Some(price) = new_price_ticks {
        order.price_ticks = price;
    }
    if let Some(tif) = new_time_in_force {
        order.time_in_force = tif;
    }
    if let Some(flags) = new_exec_instructions {
        order.exec_instructions = flags;
    }

    if order.exec_instructions.post_only()
        && would_cross_now(
            keyspace,
            project_id,
            scope_id,
            instrument,
            order.side,
            order.price_ticks,
        )?
    {
        return Err(AedbError::Validation(
            "cancel_replace post_only would cross".into(),
        ));
    }

    let loses_priority = order.price_ticks != old_price || remaining > old_remaining;

    keyspace.kv_del(
        project_id,
        scope_id,
        &key_fifo(
            instrument,
            old_side,
            old_price,
            old_placed_seq,
            order.order_id,
        ),
        commit_seq,
    );
    if remaining > old_remaining {
        keyspace.kv_inc_u256(
            project_id,
            scope_id,
            key_plqty(instrument, old_side, old_price),
            remaining - old_remaining,
            commit_seq,
        )?;
    } else if old_remaining > remaining {
        dec_price_level_qty(
            keyspace,
            project_id,
            scope_id,
            instrument,
            old_side,
            old_price,
            old_remaining - remaining,
            commit_seq,
        )?;
    }

    if old_price != order.price_ticks {
        dec_price_level_qty(
            keyspace, project_id, scope_id, instrument, old_side, old_price, remaining, commit_seq,
        )?;
        keyspace.kv_inc_u256(
            project_id,
            scope_id,
            key_plqty(instrument, old_side, order.price_ticks),
            remaining,
            commit_seq,
        )?;
    }

    order.remaining_qty_be = u256_to_be(remaining);
    order.last_modified_seq = commit_seq;
    if loses_priority {
        order.placed_seq = commit_seq;
    }
    order.status = if u256_from_be(order.filled_qty_be).is_zero() {
        OrderStatus::Open
    } else {
        OrderStatus::PartiallyFilled
    };
    store_order(keyspace, project_id, scope_id, &order, commit_seq)?;

    keyspace.kv_set(
        project_id,
        scope_id,
        key_fifo(
            instrument,
            order.side,
            order.price_ticks,
            order.placed_seq,
            order.order_id,
        ),
        vec![1],
        commit_seq,
    );

    if matches!(order.time_in_force, TimeInForce::Ioc | TimeInForce::Fok) {
        apply_order_book_cancel(
            keyspace, project_id, scope_id, instrument, order_id, None, owner, commit_seq,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn apply_order_book_mass_cancel(
    keyspace: &mut Keyspace,
    project_id: &str,
    scope_id: &str,
    instrument: &str,
    owner: &str,
    side: Option<OrderSide>,
    owner_filter: Option<&str>,
    price_range_ticks: Option<(i64, i64)>,
    commit_seq: u64,
) -> Result<(), AedbError> {
    let orders = keyspace.kv_scan_prefix(
        project_id,
        scope_id,
        &all_orders_prefix(instrument),
        usize::MAX,
    );
    for (_, entry) in orders {
        let order: OrderRecord = deserialize(&entry.value)?;
        if u256_from_be(order.remaining_qty_be).is_zero() {
            continue;
        }
        if let Some(s) = side
            && order.side != s
        {
            continue;
        }
        if let Some(filter_owner) = owner_filter
            && order.owner != filter_owner
        {
            continue;
        }
        if let Some((min_price, max_price)) = price_range_ticks
            && (order.price_ticks < min_price || order.price_ticks > max_price)
        {
            continue;
        }
        if let Some(filter_owner) = owner_filter {
            if filter_owner != owner && order.owner != owner {
                continue;
            }
        } else if order.owner != owner {
            continue;
        }
        apply_order_book_cancel(
            keyspace,
            project_id,
            scope_id,
            instrument,
            order.order_id,
            None,
            &order.owner,
            commit_seq,
        )?;
    }
    Ok(())
}

pub fn apply_order_book_reduce(
    keyspace: &mut Keyspace,
    project_id: &str,
    scope_id: &str,
    instrument: &str,
    order_id: u64,
    owner: &str,
    reduce_by: U256,
    commit_seq: u64,
) -> Result<(), AedbError> {
    let Some(mut order) = load_order(keyspace, project_id, scope_id, instrument, order_id)? else {
        return Ok(());
    };
    if order.owner != owner {
        return Err(AedbError::PermissionDenied(
            "order ownership mismatch".into(),
        ));
    }
    let remaining = u256_from_be(order.remaining_qty_be);
    if remaining.is_zero() {
        return Ok(());
    }
    let next = if reduce_by >= remaining {
        U256::zero()
    } else {
        remaining - reduce_by
    };
    let delta = remaining - next;
    dec_price_level_qty(
        keyspace,
        project_id,
        scope_id,
        instrument,
        order.side,
        order.price_ticks,
        delta,
        commit_seq,
    )?;
    if next.is_zero() {
        keyspace.kv_del(
            project_id,
            scope_id,
            &key_fifo(
                instrument,
                order.side,
                order.price_ticks,
                order.placed_seq,
                order.order_id,
            ),
            commit_seq,
        );
        keyspace.kv_del(
            project_id,
            scope_id,
            &key_open_order(instrument, &order.owner, order.order_id),
            commit_seq,
        );
        order.status = OrderStatus::Cancelled;
    }
    order.remaining_qty_be = u256_to_be(next);
    order.last_modified_seq = commit_seq;
    store_order(keyspace, project_id, scope_id, &order, commit_seq)
}

pub fn apply_order_book_match(
    keyspace: &mut Keyspace,
    project_id: &str,
    scope_id: &str,
    instrument: &str,
    fills: &[FillSpec],
    commit_seq: u64,
) -> Result<(), AedbError> {
    for fill in fills {
        let qty = u256_from_be(fill.qty_be);
        if qty.is_zero() {
            return Err(AedbError::Validation("fill qty must be > 0".into()));
        }
        let mut aggressor = load_order_required(
            keyspace,
            project_id,
            scope_id,
            instrument,
            fill.aggressor_order_id,
        )?;
        let mut passive = load_order_required(
            keyspace,
            project_id,
            scope_id,
            instrument,
            fill.passive_order_id,
        )?;
        if aggressor.side == passive.side {
            return Err(AedbError::Validation(
                "match orders must be opposite side".into(),
            ));
        }
        apply_fill_to_order(
            keyspace,
            project_id,
            scope_id,
            instrument,
            &mut aggressor,
            qty,
            commit_seq,
        )?;
        apply_fill_to_order(
            keyspace,
            project_id,
            scope_id,
            instrument,
            &mut passive,
            qty,
            commit_seq,
        )?;
        let _ = write_fill(
            keyspace,
            project_id,
            scope_id,
            instrument,
            &OrderRequest {
                instrument: instrument.to_string(),
                client_order_id: String::new(),
                side: aggressor.side,
                order_type: aggressor.order_type,
                time_in_force: aggressor.time_in_force,
                exec_instructions: aggressor.exec_instructions,
                self_trade_prevention: aggressor.self_trade_prevention,
                price_ticks: fill.price_ticks,
                qty_be: fill.qty_be,
                owner: aggressor.owner.clone(),
                account: aggressor.account.clone(),
                nonce: aggressor.nonce,
                price_limit_ticks: None,
            },
            aggressor.order_id,
            &passive,
            fill.price_ticks,
            qty,
            commit_seq,
        )?;
    }
    Ok(())
}

pub fn read_order_status(
    snapshot: &KeyspaceSnapshot,
    project_id: &str,
    scope_id: &str,
    instrument: &str,
    order_id: u64,
) -> Result<Option<OrderRecord>, AedbError> {
    snapshot
        .kv_get(project_id, scope_id, &key_order(instrument, order_id))
        .map(|entry| deserialize::<OrderRecord>(&entry.value))
        .transpose()
}

pub fn read_open_orders(
    snapshot: &KeyspaceSnapshot,
    project_id: &str,
    scope_id: &str,
    instrument: &str,
    owner: &str,
) -> Result<Vec<OrderRecord>, AedbError> {
    let open = snapshot_scan_prefix(
        project_id,
        scope_id,
        &open_orders_prefix(instrument, owner),
        usize::MAX,
        snapshot,
    );
    let mut out = Vec::with_capacity(open.len());
    for (k, _) in open {
        if let Some(order_id) = parse_order_id_suffix(&k)
            && let Some(order) =
                read_order_status(snapshot, project_id, scope_id, instrument, order_id)?
        {
            out.push(order);
        }
    }
    Ok(out)
}

pub fn read_recent_trades(
    snapshot: &KeyspaceSnapshot,
    project_id: &str,
    scope_id: &str,
    instrument: &str,
    limit: usize,
) -> Result<Vec<FillRecord>, AedbError> {
    let mut trades = snapshot_scan_prefix(
        project_id,
        scope_id,
        &trade_prefix(instrument),
        usize::MAX,
        snapshot,
    );
    if trades.len() > limit {
        let start = trades.len() - limit;
        trades = trades.split_off(start);
    }
    let mut out = Vec::with_capacity(trades.len());
    for (_, entry) in trades {
        out.push(deserialize::<FillRecord>(&entry.value)?);
    }
    Ok(out)
}

pub fn read_last_execution_report(
    snapshot: &KeyspaceSnapshot,
    project_id: &str,
    scope_id: &str,
    instrument: &str,
) -> Result<Option<ExecutionReport>, AedbError> {
    let Some(entry) = snapshot.kv_get(project_id, scope_id, &key_execution_report_last(instrument))
    else {
        return Ok(None);
    };
    if let Some((commit_seq, order_id)) = decode_last_report_pointer(&entry.value)
        && let Some(report_entry) = snapshot.kv_get(
            project_id,
            scope_id,
            &key_execution_report(instrument, commit_seq, order_id),
        )
    {
        return deserialize::<ExecutionReport>(&report_entry.value).map(Some);
    }
    deserialize::<ExecutionReport>(&entry.value).map(Some)
}

pub fn read_spread(
    snapshot: &KeyspaceSnapshot,
    project_id: &str,
    scope_id: &str,
    instrument: &str,
    seq: u64,
) -> Result<Spread, AedbError> {
    let best_bid =
        best_price_for_side_snapshot(snapshot, project_id, scope_id, instrument, OrderSide::Bid);
    let best_ask =
        best_price_for_side_snapshot(snapshot, project_id, scope_id, instrument, OrderSide::Ask);
    let mid = match (best_bid, best_ask) {
        (Some(b), Some(a)) => Some((b + a) / 2),
        _ => None,
    };
    Ok(Spread {
        best_bid,
        best_ask,
        mid,
        seq,
    })
}

pub fn read_top_n(
    snapshot: &KeyspaceSnapshot,
    project_id: &str,
    scope_id: &str,
    instrument: &str,
    depth: usize,
    seq: u64,
) -> Result<OrderBookDepth, AedbError> {
    let bids = top_side(
        snapshot,
        project_id,
        scope_id,
        instrument,
        OrderSide::Bid,
        depth,
    )?;
    let asks = top_side(
        snapshot,
        project_id,
        scope_id,
        instrument,
        OrderSide::Ask,
        depth,
    )?;
    Ok(OrderBookDepth {
        instrument: instrument.to_string(),
        bids,
        asks,
        seq,
    })
}

fn top_side(
    snapshot: &KeyspaceSnapshot,
    project_id: &str,
    scope_id: &str,
    instrument: &str,
    side: OrderSide,
    depth: usize,
) -> Result<Vec<PriceLevel>, AedbError> {
    let entries = snapshot_scan_prefix(
        project_id,
        scope_id,
        &plqty_prefix(instrument, side),
        usize::MAX,
        snapshot,
    );
    let mut out = Vec::new();
    for (k, v) in entries {
        let qty = decode_u256_bytes(&v.value)?;
        if qty.is_zero() {
            continue;
        }
        let Some(price) = parse_plqty_price(side, &k) else {
            continue;
        };
        let order_count = snapshot_scan_prefix(
            project_id,
            scope_id,
            &fifo_prefix(instrument, side, price),
            usize::MAX,
            snapshot,
        )
        .len() as u32;
        out.push(PriceLevel {
            price_ticks: price,
            total_qty_be: u256_to_be(qty),
            order_count,
        });
        if out.len() >= depth {
            break;
        }
    }
    Ok(out)
}

fn load_u64(
    keyspace: &Keyspace,
    project_id: &str,
    scope_id: &str,
    key: &[u8],
) -> Result<Option<u64>, AedbError> {
    let Some(entry) = keyspace.kv_get(project_id, scope_id, key) else {
        return Ok(None);
    };
    if entry.value.len() != 8 {
        return Err(AedbError::Validation("invalid u64 value length".into()));
    }
    let mut b = [0u8; 8];
    b.copy_from_slice(&entry.value);
    Ok(Some(u64::from_be_bytes(b)))
}

fn load_instrument_config(
    keyspace: &Keyspace,
    project_id: &str,
    scope_id: &str,
    instrument: &str,
) -> Result<Option<InstrumentConfig>, AedbError> {
    keyspace
        .kv_get(project_id, scope_id, &key_instrument_config(instrument))
        .map(|entry| deserialize::<InstrumentConfig>(&entry.value))
        .transpose()
}

fn validate_instrument_against_table_mode(
    keyspace: &Keyspace,
    project_id: &str,
    scope_id: &str,
    instrument: &str,
) -> Result<(), AedbError> {
    let Some((table_id, asset_id)) = parse_scoped_instrument(instrument) else {
        return Ok(());
    };
    let spec_key = key_order_book_table_spec(table_id);
    let Some(entry) = keyspace.kv_get(project_id, scope_id, &spec_key) else {
        return Err(AedbError::Validation(format!(
            "order book table not defined: {table_id}"
        )));
    };
    let spec: OrderBookTableSpec = deserialize(&entry.value)?;
    match spec.mode {
        OrderBookTableMode::MultiAsset => Ok(()),
        OrderBookTableMode::PerAsset => {
            if asset_id == table_id {
                Ok(())
            } else {
                Err(AedbError::Validation(format!(
                    "table {table_id} is PerAsset and only supports asset_id={table_id}"
                )))
            }
        }
    }
}

fn load_client_order_id(
    keyspace: &Keyspace,
    project_id: &str,
    scope_id: &str,
    instrument: &str,
    owner: &str,
    client_order_id: &str,
) -> Result<Option<u64>, AedbError> {
    let key = key_client_id(instrument, owner, client_order_id);
    load_u64(keyspace, project_id, scope_id, &key)
}

fn snapshot_scan_prefix(
    project_id: &str,
    scope_id: &str,
    prefix: &[u8],
    limit: usize,
    snapshot: &KeyspaceSnapshot,
) -> Vec<(Vec<u8>, crate::storage::keyspace::KvEntry)> {
    let ns = crate::storage::keyspace::NamespaceId::project_scope(project_id, scope_id);
    let Some(namespace) = snapshot.namespaces.get(&ns) else {
        return Vec::new();
    };
    namespace
        .kv
        .entries
        .iter()
        .filter(|(k, _)| k.starts_with(prefix))
        .take(limit)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

fn allocate_next_id(
    keyspace: &mut Keyspace,
    project_id: &str,
    scope_id: &str,
    key: &[u8],
    commit_seq: u64,
) -> Result<u64, AedbError> {
    let current = load_u64(keyspace, project_id, scope_id, key)?.unwrap_or(1);
    let next = current
        .checked_add(1)
        .ok_or_else(|| AedbError::Validation("id overflow".into()))?;
    keyspace.kv_set(
        project_id,
        scope_id,
        key.to_vec(),
        next.to_be_bytes().to_vec(),
        commit_seq,
    );
    Ok(current)
}

fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, AedbError> {
    rmp_serde::to_vec(value).map_err(|e| AedbError::Encode(e.to_string()))
}

fn deserialize<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, AedbError> {
    rmp_serde::from_slice(bytes).map_err(|e| AedbError::Decode(e.to_string()))
}

fn decode_u256_bytes(bytes: &[u8]) -> Result<U256, AedbError> {
    if bytes.len() != 32 {
        return Err(AedbError::Validation("invalid u256 bytes length".into()));
    }
    Ok(U256::from_big_endian(bytes))
}

fn load_order(
    keyspace: &Keyspace,
    project_id: &str,
    scope_id: &str,
    instrument: &str,
    order_id: u64,
) -> Result<Option<OrderRecord>, AedbError> {
    let Some(entry) = keyspace.kv_get(project_id, scope_id, &key_order(instrument, order_id))
    else {
        return Ok(None);
    };
    Ok(Some(deserialize::<OrderRecord>(&entry.value)?))
}

fn load_order_required(
    keyspace: &Keyspace,
    project_id: &str,
    scope_id: &str,
    instrument: &str,
    order_id: u64,
) -> Result<OrderRecord, AedbError> {
    load_order(keyspace, project_id, scope_id, instrument, order_id)?
        .ok_or_else(|| AedbError::Validation(format!("order not found: {order_id}")))
}

fn store_order(
    keyspace: &mut Keyspace,
    project_id: &str,
    scope_id: &str,
    order: &OrderRecord,
    commit_seq: u64,
) -> Result<(), AedbError> {
    keyspace.kv_set(
        project_id,
        scope_id,
        key_order(&order.instrument, order.order_id),
        serialize(order)?,
        commit_seq,
    );
    Ok(())
}

fn best_price_for_side(
    keyspace: &Keyspace,
    project_id: &str,
    scope_id: &str,
    instrument: &str,
    side: OrderSide,
) -> Option<i64> {
    let mut out = None;
    keyspace.kv_visit_prefix_ref(
        project_id,
        scope_id,
        &plqty_prefix(instrument, side),
        usize::MAX,
        |k, v| {
            if decode_u256_bytes(&v.value)
                .ok()
                .is_some_and(|x| !x.is_zero())
            {
                out = parse_plqty_price(side, k);
                return false;
            }
            true
        },
    );
    out
}

fn best_price_for_side_snapshot(
    snapshot: &KeyspaceSnapshot,
    project_id: &str,
    scope_id: &str,
    instrument: &str,
    side: OrderSide,
) -> Option<i64> {
    let entries = snapshot_scan_prefix(
        project_id,
        scope_id,
        &plqty_prefix(instrument, side),
        usize::MAX,
        snapshot,
    );
    for (k, v) in entries {
        if decode_u256_bytes(&v.value)
            .ok()
            .is_some_and(|x| !x.is_zero())
        {
            return parse_plqty_price(side, &k);
        }
    }
    None
}

fn crosses(aggressor_side: OrderSide, aggressor_price: i64, passive_price: i64) -> bool {
    match aggressor_side {
        OrderSide::Bid => passive_price <= aggressor_price,
        OrderSide::Ask => passive_price >= aggressor_price,
    }
}

fn price_allows(request: &OrderRequest, passive_price: i64) -> bool {
    match request.order_type {
        OrderType::Limit => crosses(request.side, request.price_ticks, passive_price),
        OrderType::Market => {
            if let Some(limit) = request.price_limit_ticks {
                crosses(request.side, limit, passive_price)
            } else {
                true
            }
        }
    }
}

fn best_passive_order(
    keyspace: &Keyspace,
    project_id: &str,
    scope_id: &str,
    instrument: &str,
    passive_side: OrderSide,
    request: &OrderRequest,
) -> Result<Option<(i64, OrderRecord)>, AedbError> {
    let mut out = None;
    let mut error: Option<AedbError> = None;
    keyspace.kv_visit_prefix_ref(
        project_id,
        scope_id,
        &plqty_prefix(instrument, passive_side),
        usize::MAX,
        |k, v| {
            let level_qty = match decode_u256_bytes(&v.value) {
                Ok(q) => q,
                Err(e) => {
                    error = Some(e);
                    return false;
                }
            };
            if level_qty.is_zero() {
                return true;
            }
            let Some(price) = parse_plqty_price(passive_side, k) else {
                return true;
            };
            if !price_allows(request, price) {
                return !matches!(request.order_type, OrderType::Limit);
            }
            let mut first_fifo_order_id = None;
            keyspace.kv_visit_prefix_ref(
                project_id,
                scope_id,
                &fifo_prefix(instrument, passive_side, price),
                1,
                |fifo_key, _| {
                    first_fifo_order_id = parse_fifo_order_id(fifo_key);
                    false
                },
            );
            let Some(order_id) = first_fifo_order_id else {
                return true;
            };
            match load_order(keyspace, project_id, scope_id, instrument, order_id) {
                Ok(Some(order)) => {
                    out = Some((price, order));
                    false
                }
                Ok(None) => true,
                Err(e) => {
                    error = Some(e);
                    false
                }
            }
        },
    );
    if let Some(e) = error {
        return Err(e);
    }
    Ok(out)
}

fn first_passive_order_at_price(
    keyspace: &Keyspace,
    project_id: &str,
    scope_id: &str,
    instrument: &str,
    passive_side: OrderSide,
    price: i64,
    request: &OrderRequest,
) -> Result<Option<OrderRecord>, AedbError> {
    if !price_allows(request, price) {
        return Ok(None);
    }
    let fifo = keyspace.kv_scan_prefix_ref(
        project_id,
        scope_id,
        &fifo_prefix(instrument, passive_side, price),
        1,
    );
    let Some((fifo_key, _)) = fifo.into_iter().next() else {
        return Ok(None);
    };
    let Some(order_id) = parse_fifo_order_id(fifo_key) else {
        return Ok(None);
    };
    load_order(keyspace, project_id, scope_id, instrument, order_id)
}

fn can_fok_fill(
    keyspace: &Keyspace,
    project_id: &str,
    scope_id: &str,
    request: &OrderRequest,
) -> Result<bool, AedbError> {
    let mut needed = u256_from_be(request.qty_be);
    let mut error: Option<AedbError> = None;
    keyspace.kv_visit_prefix_ref(
        project_id,
        scope_id,
        &plqty_prefix(&request.instrument, request.side.opposite()),
        usize::MAX,
        |k, v| {
            let Some(price) = parse_plqty_price(request.side.opposite(), k) else {
                return true;
            };
            if !price_allows(request, price) {
                return !matches!(request.order_type, OrderType::Limit);
            }
            match decode_u256_bytes(&v.value) {
                Ok(qty) => {
                    if qty >= needed {
                        needed = U256::zero();
                        false
                    } else {
                        needed -= qty;
                        true
                    }
                }
                Err(e) => {
                    error = Some(e);
                    false
                }
            }
        },
    );
    if let Some(e) = error {
        return Err(e);
    }
    Ok(needed.is_zero())
}

fn apply_passive_fill(
    keyspace: &mut Keyspace,
    project_id: &str,
    scope_id: &str,
    instrument: &str,
    passive: &mut OrderRecord,
    fill_qty: U256,
    commit_seq: u64,
) -> Result<(), AedbError> {
    apply_fill_to_order(
        keyspace, project_id, scope_id, instrument, passive, fill_qty, commit_seq,
    )
}

fn apply_fill_to_order(
    keyspace: &mut Keyspace,
    project_id: &str,
    scope_id: &str,
    instrument: &str,
    order: &mut OrderRecord,
    fill_qty: U256,
    commit_seq: u64,
) -> Result<(), AedbError> {
    let remaining = u256_from_be(order.remaining_qty_be);
    if remaining < fill_qty {
        return Err(AedbError::Underflow);
    }
    let next_remaining = remaining - fill_qty;
    let filled = u256_from_be(order.filled_qty_be)
        .checked_add(fill_qty)
        .ok_or(AedbError::Overflow)?;
    dec_price_level_qty(
        keyspace,
        project_id,
        scope_id,
        instrument,
        order.side,
        order.price_ticks,
        fill_qty,
        commit_seq,
    )?;
    if next_remaining.is_zero() {
        clear_open_order(keyspace, project_id, scope_id, order)?;
        order.status = OrderStatus::Filled;
    } else {
        order.status = OrderStatus::PartiallyFilled;
    }
    order.remaining_qty_be = u256_to_be(next_remaining);
    order.filled_qty_be = u256_to_be(filled);
    order.last_modified_seq = commit_seq;
    store_order(keyspace, project_id, scope_id, order, commit_seq)
}

fn clear_open_order(
    keyspace: &mut Keyspace,
    project_id: &str,
    scope_id: &str,
    order: &OrderRecord,
) -> Result<(), AedbError> {
    keyspace.kv_del(
        project_id,
        scope_id,
        &key_fifo(
            &order.instrument,
            order.side,
            order.price_ticks,
            order.placed_seq,
            order.order_id,
        ),
        order.last_modified_seq,
    );
    keyspace.kv_del(
        project_id,
        scope_id,
        &key_open_order(&order.instrument, &order.owner, order.order_id),
        order.last_modified_seq,
    );
    Ok(())
}

fn dec_price_level_qty(
    keyspace: &mut Keyspace,
    project_id: &str,
    scope_id: &str,
    instrument: &str,
    side: OrderSide,
    price_ticks: i64,
    delta: U256,
    commit_seq: u64,
) -> Result<(), AedbError> {
    let key = key_plqty(instrument, side, price_ticks);
    let next = keyspace.kv_dec_u256(project_id, scope_id, key.clone(), delta, commit_seq)?;
    if next.is_zero() {
        keyspace.kv_del(project_id, scope_id, &key, commit_seq);
    }
    Ok(())
}

fn write_fill(
    keyspace: &mut Keyspace,
    project_id: &str,
    scope_id: &str,
    instrument: &str,
    request: &OrderRequest,
    aggressor_order_id: u64,
    passive: &OrderRecord,
    price_ticks: i64,
    qty: U256,
    commit_seq: u64,
) -> Result<FillRecord, AedbError> {
    let fill_id = allocate_next_id(
        keyspace,
        project_id,
        scope_id,
        &key_next_fill_id(instrument),
        commit_seq,
    )?;
    let fill = FillRecord {
        fill_id,
        instrument: instrument.to_string(),
        price_ticks,
        qty_be: u256_to_be(qty),
        aggressor_order_id,
        aggressor_owner: request.owner.clone(),
        aggressor_side: request.side,
        passive_order_id: passive.order_id,
        passive_owner: passive.owner.clone(),
        seq: commit_seq,
    };
    keyspace.kv_set(
        project_id,
        scope_id,
        key_trade(instrument, fill_id),
        serialize(&fill)?,
        commit_seq,
    );
    Ok(fill)
}

fn write_execution_report(
    keyspace: &mut Keyspace,
    project_id: &str,
    scope_id: &str,
    report: &ExecutionReport,
    instrument: &str,
    commit_seq: u64,
) -> Result<(), AedbError> {
    let bytes = serialize(report)?;
    keyspace.kv_set(
        project_id,
        scope_id,
        key_execution_report(instrument, commit_seq, report.order_id),
        bytes,
        commit_seq,
    );
    keyspace.kv_set(
        project_id,
        scope_id,
        key_execution_report_last(instrument),
        encode_last_report_pointer(commit_seq, report.order_id),
        commit_seq,
    );
    Ok(())
}

fn encode_last_report_pointer(commit_seq: u64, order_id: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(&commit_seq.to_be_bytes());
    out.extend_from_slice(&order_id.to_be_bytes());
    out
}

fn decode_last_report_pointer(bytes: &[u8]) -> Option<(u64, u64)> {
    let commit: [u8; 8] = bytes.get(0..8)?.try_into().ok()?;
    let order: [u8; 8] = bytes.get(8..16)?.try_into().ok()?;
    Some((u64::from_be_bytes(commit), u64::from_be_bytes(order)))
}

fn would_cross_now(
    keyspace: &Keyspace,
    project_id: &str,
    scope_id: &str,
    instrument: &str,
    side: OrderSide,
    price_ticks: i64,
) -> Result<bool, AedbError> {
    let Some(best_opposite) =
        best_price_for_side(keyspace, project_id, scope_id, instrument, side.opposite())
    else {
        return Ok(false);
    };
    Ok(crosses(side, price_ticks, best_opposite))
}

fn effective_request_for_config(
    request: &OrderRequest,
    config: &InstrumentConfig,
    keyspace: &Keyspace,
    project_id: &str,
    scope_id: &str,
) -> Result<OrderRequest, AedbError> {
    if !matches!(request.order_type, OrderType::Market) || request.price_limit_ticks.is_some() {
        return Ok(request.clone());
    }
    let Some(band) = config.market_order_price_band else {
        return Ok(request.clone());
    };
    let bid = best_price_for_side(
        keyspace,
        project_id,
        scope_id,
        &request.instrument,
        OrderSide::Bid,
    );
    let ask = best_price_for_side(
        keyspace,
        project_id,
        scope_id,
        &request.instrument,
        OrderSide::Ask,
    );
    let Some(mid) = (match (bid, ask) {
        (Some(b), Some(a)) => Some((b + a) / 2),
        _ => None,
    }) else {
        return Ok(request.clone());
    };
    let mut next = request.clone();
    next.price_limit_ticks = Some(match request.side {
        OrderSide::Bid => mid.saturating_add(band),
        OrderSide::Ask => mid.saturating_sub(band),
    });
    Ok(next)
}
