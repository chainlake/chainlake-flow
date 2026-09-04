use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use apache_avro::{to_avro_datum, types::Value, Schema};
use once_cell::sync::Lazy;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use rayon::prelude::*;

// ---------------------------------------------------------------------------
// Field-kind enum (matches rpcstream's scalar_type + repeated flag)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum FieldKind {
    String,       // scalar_type="string", repeated=false
    Long,         // scalar_type="int64",  repeated=false
    Bool,         // scalar_type="bool",   repeated=false
    StringArray,  // scalar_type="string", repeated=true
    LongArray,    // scalar_type="int64",  repeated=true  (uncommon but handled)
    BoolArray,    // scalar_type="bool",   repeated=true
}

#[derive(Clone, Debug)]
struct FieldSpec {
    name: String,
    kind: FieldKind,
}

// ---------------------------------------------------------------------------
// Schema entry cache
// ---------------------------------------------------------------------------

struct SchemaEntry {
    schema: Schema,
    fields: Vec<FieldSpec>,
}

// Schema is a pure Rust data tree with no Rc / raw-pointer members.
unsafe impl Send for SchemaEntry {}
unsafe impl Sync for SchemaEntry {}

static SCHEMA_CACHE: Lazy<RwLock<HashMap<i64, Arc<SchemaEntry>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

// ---------------------------------------------------------------------------
// Cached reference to Python's json.dumps (avoids repeated sys.modules look-ups)
// ---------------------------------------------------------------------------

static JSON_DUMPS: Lazy<Py<PyAny>> = Lazy::new(|| {
    Python::with_gil(|py| {
        py.import_bound("json")
            .and_then(|m| m.getattr("dumps"))
            .expect("failed to cache json.dumps")
            .unbind()
    })
});

// ---------------------------------------------------------------------------
// Rust-native value representation (after GIL extraction, before encoding)
// ---------------------------------------------------------------------------

enum ExtractedValue {
    Str(String),
    Long(i64),
    Bool(bool),
    StrArr(Vec<String>),
    LongArr(Vec<i64>),
    BoolArr(Vec<bool>),
}

fn to_avro_value(opt: Option<ExtractedValue>) -> Value {
    match opt {
        None => Value::Union(0, Box::new(Value::Null)),
        Some(ExtractedValue::Str(s)) => Value::Union(1, Box::new(Value::String(s))),
        Some(ExtractedValue::Long(n)) => Value::Union(1, Box::new(Value::Long(n))),
        Some(ExtractedValue::Bool(b)) => Value::Union(1, Box::new(Value::Boolean(b))),
        Some(ExtractedValue::StrArr(v)) => Value::Union(
            1,
            Box::new(Value::Array(v.into_iter().map(Value::String).collect())),
        ),
        Some(ExtractedValue::LongArr(v)) => Value::Union(
            1,
            Box::new(Value::Array(v.into_iter().map(Value::Long).collect())),
        ),
        Some(ExtractedValue::BoolArr(v)) => Value::Union(
            1,
            Box::new(Value::Array(v.into_iter().map(Value::Boolean).collect())),
        ),
    }
}

// ---------------------------------------------------------------------------
// GIL-held helpers: Python → Rust value extraction
// ---------------------------------------------------------------------------

// Mirrors normalize_scalar("string", value): dict/list → compact JSON, else str().
fn extract_string(json_dumps: &Bound<'_, PyAny>, obj: &Bound<'_, PyAny>) -> PyResult<String> {
    if obj.is_instance_of::<PyDict>() || obj.is_instance_of::<PyList>() {
        json_dumps.call1((obj,))?.extract::<String>()
    } else {
        Ok(obj.str()?.to_str()?.to_owned())
    }
}

fn extract_value(
    json_dumps: &Bound<'_, PyAny>,
    obj: &Bound<'_, PyAny>,
    kind: &FieldKind,
) -> PyResult<Option<ExtractedValue>> {
    if obj.is_none() {
        return Ok(None);
    }
    let v = match kind {
        FieldKind::String => ExtractedValue::Str(extract_string(json_dumps, obj)?),
        FieldKind::Long => ExtractedValue::Long(obj.extract::<i64>()?),
        FieldKind::Bool => ExtractedValue::Bool(obj.extract::<bool>()?),
        FieldKind::StringArray => {
            let items: Vec<String> = if let Ok(lst) = obj.downcast::<PyList>() {
                lst.iter()
                    .map(|item| extract_string(json_dumps, &item))
                    .collect::<PyResult<_>>()?
            } else {
                // scalar wrapped as single-element array
                vec![extract_string(json_dumps, obj)?]
            };
            ExtractedValue::StrArr(items)
        }
        FieldKind::LongArray => {
            let items: Vec<i64> = if let Ok(lst) = obj.downcast::<PyList>() {
                lst.iter()
                    .map(|item| item.extract::<i64>())
                    .collect::<PyResult<_>>()?
            } else {
                vec![obj.extract::<i64>()?]
            };
            ExtractedValue::LongArr(items)
        }
        FieldKind::BoolArray => {
            let items: Vec<bool> = if let Ok(lst) = obj.downcast::<PyList>() {
                lst.iter()
                    .map(|item| item.extract::<bool>())
                    .collect::<PyResult<_>>()?
            } else {
                vec![obj.extract::<bool>()?]
            };
            ExtractedValue::BoolArr(items)
        }
    };
    Ok(Some(v))
}

// Extracts one Python dict into a Vec of Option<ExtractedValue> (one per schema field).
// Absent / None values become None (encoded as Avro null by to_avro_value).
fn extract_row(
    py: Python<'_>,
    row: &Bound<'_, PyDict>,
    fields: &[FieldSpec],
) -> PyResult<Vec<Option<ExtractedValue>>> {
    let json_dumps = JSON_DUMPS.bind(py);
    fields
        .iter()
        .map(|field| match row.get_item(&field.name)? {
            None => Ok(None),
            Some(v) => extract_value(json_dumps, &v, &field.kind),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// GIL-free: Avro encode one row → Confluent wire-format bytes
// ---------------------------------------------------------------------------

fn encode_row(
    entry: &SchemaEntry,
    schema_id: i64,
    extracted: Vec<Option<ExtractedValue>>,
) -> Result<Vec<u8>, String> {
    let record_fields: Vec<(String, Value)> = entry
        .fields
        .iter()
        .zip(extracted)
        .map(|(spec, val)| (spec.name.clone(), to_avro_value(val)))
        .collect();

    let avro_bytes = to_avro_datum(&entry.schema, Value::Record(record_fields))
        .map_err(|e| format!("avro_encode error: {}", e))?;

    // Confluent Schema Registry wire format: magic(1) + schema_id(4 BE) + payload
    let mut buf = Vec::with_capacity(5 + avro_bytes.len());
    buf.push(0u8);
    buf.extend_from_slice(&(schema_id as i32).to_be_bytes());
    buf.extend_from_slice(&avro_bytes);
    Ok(buf)
}

// ---------------------------------------------------------------------------
// Public Python API
// ---------------------------------------------------------------------------

/// Register an Avro schema so encode_batch can use it.
///
/// schema_id       – Confluent Schema Registry integer ID for this schema.
/// avro_schema_json – The full Avro JSON schema string (same as passed to AvroSerializer).
/// field_specs     – [(name, scalar_type, repeated), ...] in schema field order.
///                   scalar_type must be "string", "int64", or "bool".
#[pyfunction]
fn register_schema(
    schema_id: i64,
    avro_schema_json: &str,
    field_specs: Vec<(String, String, bool)>,
) -> PyResult<()> {
    let schema = Schema::parse_str(avro_schema_json).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("invalid Avro schema: {}", e))
    })?;

    let fields = field_specs
        .into_iter()
        .map(|(name, scalar_type, repeated)| {
            let kind = match (scalar_type.as_str(), repeated) {
                ("string", false) => FieldKind::String,
                ("string", true) => FieldKind::StringArray,
                ("int64", false) => FieldKind::Long,
                ("int64", true) => FieldKind::LongArray,
                ("bool", false) => FieldKind::Bool,
                ("bool", true) => FieldKind::BoolArray,
                _ => FieldKind::String, // safe fallback
            };
            FieldSpec { name, kind }
        })
        .collect();

    SCHEMA_CACHE
        .write()
        .unwrap()
        .insert(schema_id, Arc::new(SchemaEntry { schema, fields }));
    Ok(())
}

/// Encode a batch of rows for a single schema and return Confluent wire-format bytes.
///
/// schema_id – ID previously registered via register_schema().
/// rows      – list of dicts, each already prepared by _prepare_row_only()
///             (id and ingest_timestamp set, kafka_partition_key removed).
///
/// Returns list[bytes], one entry per input row, in input order.
///
/// GIL strategy:
///   Phase 1 (GIL held)    – extract Python dicts into Rust-native values.
///   Phase 2 (GIL released) – rayon parallel Avro encode (all threads run freely).
///   Phase 3 (GIL acquired) – wrap each Vec<u8> in PyBytes so Python always
///                             receives bytes, not list[int] (PyO3 ≤0.20 quirk).
#[pyfunction]
fn encode_batch(
    py: Python<'_>,
    schema_id: i64,
    rows: &Bound<'_, PyList>,
) -> PyResult<Vec<Py<PyBytes>>> {
    // Cheap Arc clone; drops the RwLock read guard before touching Python objects.
    let entry: Arc<SchemaEntry> = {
        let cache = SCHEMA_CACHE.read().unwrap();
        cache.get(&schema_id).cloned().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "schema_id {} not registered; call register_schema() first",
                schema_id
            ))
        })?
    };

    // Phase 1: GIL held — extract all Python values into Rust types
    let extracted: Vec<Vec<Option<ExtractedValue>>> = rows
        .iter()
        .map(|item| {
            let row = item.downcast::<PyDict>()?;
            extract_row(py, row, &entry.fields)
        })
        .collect::<PyResult<_>>()?;

    // Phase 2: GIL released — true parallel Avro encoding with rayon
    let results: Vec<Result<Vec<u8>, String>> = py.allow_threads(|| {
        extracted
            .into_par_iter()
            .map(|row| encode_row(&entry, schema_id, row))
            .collect()
    });

    // Propagate first encoding error (rare: only fires on schema/value mismatch).
    // Explicitly wrap as PyBytes so Python always receives bytes, not list[int].
    results.into_iter().map(|r| {
        r.map_err(|msg| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(msg))
         .map(|v| PyBytes::new_bound(py, &v).unbind())
    }).collect()
}

// ---------------------------------------------------------------------------
// Block envelope parser: raw JSON strings → EVM entity rows
//
// parse_block_envelope(block_json_str, receipts_json_str) → dict
//
// GIL strategy:
//   allow_threads phase  — serde_json parse + ALL entity extraction including
//                          token_transfer detection, receipt→tx enrichment,
//                          and block_timestamp injection (pure Rust, no Python
//                          objects; multiple workers run in true parallel).
//   GIL-held phase       — convert Rust HashMaps → Python dicts/lists.
//                          Fast: no JSON parsing, no hex conversion.
//
// Replaces Python parse_blocks/parse_transactions/parse_receipts (process),
// EvmDecoder.decode() (token_transfer), and EvmEnricher.enrich() (receipt→tx,
// block context). Python decode+enrich held the GIL for ~150 ms/block.
// ---------------------------------------------------------------------------

type Row = HashMap<String, serde_json::Value>;

// ERC-20/721 Transfer(address,address,uint256) topic0
const TRANSFER_TOPIC: &str =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
// ERC-1155 TransferSingle(address,address,address,uint256,uint256) topic0
const ERC1155_SINGLE_TOPIC: &str =
    "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62";
// ERC-1155 TransferBatch topic0 prefix (first 4 bytes as hex = function selector)
const ERC1155_BATCH_PREFIX: &str = "0x4a39dc06";

// Receipt JSON camelCase keys copied to transaction rows as receipt_* fields.
// (dest_snake_key, src_camelCase_in_receipt_json, is_hex_value)
const RECEIPT_TX_FIELDS: &[(&str, &str, bool)] = &[
    ("receipt_cumulative_gas_used", "cumulativeGasUsed",  true),
    ("receipt_gas_used",            "gasUsed",            true),
    ("receipt_contract_address",    "contractAddress",    false),
    ("receipt_status",              "status",             true),
    ("receipt_effective_gas_price", "effectiveGasPrice",  true),
    ("receipt_transaction_type",    "type",               true),
    ("receipt_l1_fee",              "l1Fee",              true),
    ("receipt_l1_gas_used",         "l1GasUsed",          true),
    ("receipt_l1_gas_price",        "l1GasPrice",         true),
    ("receipt_l1_fee_scalar",       "l1FeeScalar",        false),
    ("receipt_blob_gas_price",      "blobGasPrice",       true),
    ("receipt_blob_gas_used",       "blobGasUsed",        true),
];

/// Parse a hex-encoded integer string ("0x1a3f" or "1a3f") → i64.
/// Returns None for missing, null, or overflow values.
fn hex_to_i64(s: &str) -> Option<i64> {
    let stripped = s.trim_start_matches("0x").trim_start_matches("0X");
    if stripped.is_empty() {
        return Some(0);
    }
    i64::from_str_radix(stripped, 16).ok()
}

fn val_hex(obj: &serde_json::Value, key: &str) -> serde_json::Value {
    obj.get(key)
        .and_then(|v| v.as_str())
        .and_then(|s| hex_to_i64(s))
        .map(serde_json::Value::from)
        .unwrap_or(serde_json::Value::Null)
}

fn val_hex_or(obj: &serde_json::Value, key: &str, default: i64) -> serde_json::Value {
    obj.get(key)
        .and_then(|v| v.as_str())
        .and_then(|s| hex_to_i64(s))
        .map(serde_json::Value::from)
        .unwrap_or_else(|| serde_json::Value::from(default))
}

fn val_str(obj: &serde_json::Value, key: &str) -> serde_json::Value {
    obj.get(key).cloned().unwrap_or(serde_json::Value::Null)
}

fn val_arr(obj: &serde_json::Value, key: &str) -> serde_json::Value {
    obj.get(key)
        .cloned()
        .unwrap_or_else(|| serde_json::Value::Array(vec![]))
}

// ---------------------------------------------------------------------------
// Hex / ABI decoding helpers for token transfer detection
// ---------------------------------------------------------------------------

/// Decode a hex word (with or without 0x prefix) as a decimal string.
/// Handles up to 128-bit values (sufficient for token amounts/IDs in practice).
fn hex_word_to_decimal(word: &str) -> Option<String> {
    let s = word.trim_start_matches("0x").trim_start_matches("0X");
    let s = s.trim_start_matches('0');
    if s.is_empty() { return Some("0".into()); }
    if s.len() <= 32 {
        u128::from_str_radix(s, 16).ok().map(|n| n.to_string())
    } else {
        Some(format!("0x{}", s))  // >128-bit: return hex (rare for real amounts)
    }
}

/// Extract an Ethereum address from the last 20 bytes of a 32-byte topic.
fn topic_to_address(topic: &serde_json::Value) -> serde_json::Value {
    match topic.as_str() {
        None => serde_json::Value::Null,
        Some(s) => {
            let s = s.trim_start_matches("0x").trim_start_matches("0X");
            if s.len() < 40 { return serde_json::Value::Null; }
            serde_json::Value::String(format!("0x{}", &s[s.len() - 40..]))
        }
    }
}

/// Normalize a topic hex string to lowercase with 0x prefix.
fn normalize_topic(s: &str) -> String {
    let s = s.trim().to_lowercase();
    if s.starts_with("0x") { s } else { format!("0x{}", s) }
}

/// Decode one dynamic uint256[] from ABI-encoded data (hex without 0x).
/// array_index selects which array (0 = first, 1 = second, …).
fn decode_abi_uint256_array(data_hex: &str, array_index: usize) -> Vec<String> {
    if data_hex.len() < 64 { return vec![]; }

    // Read the byte-offset stored at slot array_index (each slot = 32 bytes = 64 hex chars).
    let slot_pos = array_index * 64;
    if slot_pos + 64 > data_hex.len() { return vec![]; }
    let offset_word = data_hex[slot_pos..slot_pos + 64].trim_start_matches('0');
    let offset_bytes: usize = if offset_word.is_empty() { 0 } else {
        match u64::from_str_radix(offset_word, 16) {
            Ok(n) => n as usize,
            Err(_) => return vec![],
        }
    };

    // Read array length at the offset position.
    let len_hex_pos = offset_bytes * 2;
    if len_hex_pos + 64 > data_hex.len() { return vec![]; }
    let len_word = data_hex[len_hex_pos..len_hex_pos + 64].trim_start_matches('0');
    let length: usize = if len_word.is_empty() { 0 } else {
        match u64::from_str_radix(len_word, 16) {
            Ok(n) => n as usize,
            Err(_) => return vec![],
        }
    };
    if length == 0 || length > 10_000 { return vec![]; }

    // Read elements (each 32 bytes = 64 hex chars).
    let elem_start = len_hex_pos + 64;
    let mut result = Vec::with_capacity(length);
    for i in 0..length {
        let pos = elem_start + i * 64;
        if pos + 64 > data_hex.len() { break; }
        match hex_word_to_decimal(&data_hex[pos..pos + 64]) {
            Some(n) => result.push(n),
            None => break,
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Token transfer row builder and event decoders
// ---------------------------------------------------------------------------

fn build_token_transfer(
    log: &Row,
    transfer_type: &str,
    from_address: serde_json::Value,
    to_address: serde_json::Value,
    token_id: serde_json::Value,
    amount: serde_json::Value,
    transfer_index: usize,
) -> Row {
    let tx_hash  = log.get("transaction_hash").cloned().unwrap_or(serde_json::Value::Null);
    let log_idx  = log.get("log_index").cloned().unwrap_or(serde_json::Value::Null);

    let source_log_id = match (tx_hash.as_str(), log_idx.as_i64()) {
        (Some(h), Some(i)) => serde_json::Value::String(format!("log_{h}_{i}")),
        _ => serde_json::Value::Null,
    };
    let transfer_id = match (tx_hash.as_str(), log_idx.as_i64()) {
        (Some(h), Some(i)) =>
            serde_json::Value::String(format!("token_transfer_{h}_{i}_{transfer_index}")),
        _ => serde_json::Value::Null,
    };

    let mut row = HashMap::with_capacity(16);
    row.insert("transfer_type".into(),     serde_json::Value::String(transfer_type.into()));
    row.insert("source_log_id".into(),     source_log_id);
    row.insert("token_address".into(),     log.get("address").cloned().unwrap_or(serde_json::Value::Null));
    row.insert("from_address".into(),      from_address);
    row.insert("to_address".into(),        to_address);
    row.insert("token_id".into(),          token_id);
    row.insert("amount".into(),            amount);
    row.insert("transaction_hash".into(),  tx_hash);
    row.insert("transaction_index".into(), log.get("transaction_index").cloned().unwrap_or(serde_json::Value::Null));
    row.insert("block_hash".into(),        log.get("block_hash").cloned().unwrap_or(serde_json::Value::Null));
    row.insert("block_number".into(),      log.get("block_number").cloned().unwrap_or(serde_json::Value::Null));
    row.insert("log_index".into(),         log_idx);
    row.insert("transfer_index".into(),    serde_json::Value::from(transfer_index as i64));
    row.insert("id".into(),                transfer_id);
    row.insert("block_timestamp".into(),   log.get("block_timestamp").cloned().unwrap_or(serde_json::Value::Null));
    row
}

fn decode_erc20_erc721_transfer(log: &Row, topics: &[serde_json::Value]) -> Vec<Row> {
    let from = if topics.len() > 1 { topic_to_address(&topics[1]) } else { serde_json::Value::Null };
    let to   = if topics.len() > 2 { topic_to_address(&topics[2]) } else { serde_json::Value::Null };

    if topics.len() >= 4 {
        // ERC-721: tokenId in topics[3]
        let token_id = topics[3].as_str()
            .and_then(|s| hex_word_to_decimal(s.trim_start_matches("0x").trim_start_matches("0X")))
            .map(serde_json::Value::String)
            .unwrap_or(serde_json::Value::Null);
        vec![build_token_transfer(log, "erc721", from, to, token_id, serde_json::Value::Null, 0)]
    } else {
        // ERC-20: amount is first 32 bytes of data
        let amount = log.get("data").and_then(|v| v.as_str()).and_then(|data| {
            let s = data.trim_start_matches("0x").trim_start_matches("0X");
            let word = if s.len() >= 64 { &s[..64] } else { s };
            hex_word_to_decimal(word).map(serde_json::Value::String)
        }).unwrap_or(serde_json::Value::Null);
        vec![build_token_transfer(log, "erc20", from, to, serde_json::Value::Null, amount, 0)]
    }
}

fn decode_erc1155_single(log: &Row, topics: &[serde_json::Value]) -> Row {
    let from = if topics.len() > 2 { topic_to_address(&topics[2]) } else { serde_json::Value::Null };
    let to   = if topics.len() > 3 { topic_to_address(&topics[3]) } else { serde_json::Value::Null };

    // data layout: [tokenId (32 bytes), amount (32 bytes)]
    let (token_id, amount) = log.get("data").and_then(|v| v.as_str())
        .map(|data| {
            let s = data.trim_start_matches("0x").trim_start_matches("0X");
            let w0 = if s.len() >= 64  { &s[..64]   } else { s };
            let w1 = if s.len() >= 128 { &s[64..128] } else { "" };
            let tid = hex_word_to_decimal(w0).map(serde_json::Value::String).unwrap_or(serde_json::Value::Null);
            let amt = if w1.is_empty() { serde_json::Value::Null }
                      else { hex_word_to_decimal(w1).map(serde_json::Value::String).unwrap_or(serde_json::Value::Null) };
            (tid, amt)
        })
        .unwrap_or((serde_json::Value::Null, serde_json::Value::Null));

    build_token_transfer(log, "erc1155", from, to, token_id, amount, 0)
}

fn decode_erc1155_batch(log: &Row, topics: &[serde_json::Value]) -> Vec<Row> {
    let from = if topics.len() > 2 { topic_to_address(&topics[2]) } else { serde_json::Value::Null };
    let to   = if topics.len() > 3 { topic_to_address(&topics[3]) } else { serde_json::Value::Null };

    let data_hex = match log.get("data").and_then(|v| v.as_str()) {
        Some(s) => s.trim_start_matches("0x").trim_start_matches("0X").to_lowercase(),
        None => return vec![],
    };

    let ids     = decode_abi_uint256_array(&data_hex, 0);
    let amounts = decode_abi_uint256_array(&data_hex, 1);
    if ids.is_empty() || ids.len() != amounts.len() { return vec![]; }

    ids.into_iter().zip(amounts).enumerate()
        .map(|(i, (tid, amt))| build_token_transfer(
            log, "erc1155",
            from.clone(), to.clone(),
            serde_json::Value::String(tid),
            serde_json::Value::String(amt),
            i,
        ))
        .collect()
}

/// Extract token_transfer rows from a single already-parsed log row.
fn token_transfers_from_log(log: &Row) -> Vec<Row> {
    let topics = match log.get("topics").and_then(|v| v.as_array()) {
        Some(arr) if !arr.is_empty() => arr,
        _ => return vec![],
    };
    let topic0 = match topics[0].as_str() {
        Some(s) => normalize_topic(s),
        None => return vec![],
    };

    if topic0 == TRANSFER_TOPIC {
        decode_erc20_erc721_transfer(log, topics)
    } else if topic0 == ERC1155_SINGLE_TOPIC {
        vec![decode_erc1155_single(log, topics)]
    } else if topic0.starts_with(ERC1155_BATCH_PREFIX) {
        decode_erc1155_batch(log, topics)
    } else {
        vec![]
    }
}

// ---------------------------------------------------------------------------
// Entity extraction (JSON → Row)
// ---------------------------------------------------------------------------

fn extract_block_row(block: &serde_json::Value) -> Row {
    let tx_count = block
        .get("transactions")
        .and_then(|v| v.as_array())
        .map(|a| a.len() as i64)
        .unwrap_or(0);
    let mut row = HashMap::with_capacity(25);
    row.insert("type".into(),              "block".into());
    row.insert("number".into(),            val_hex(block, "number"));
    row.insert("hash".into(),              val_str(block, "hash"));
    row.insert("parent_hash".into(),       val_str(block, "parentHash"));
    row.insert("nonce".into(),             val_hex(block, "nonce"));
    row.insert("sha3_uncles".into(),       val_str(block, "sha3Uncles"));
    row.insert("logs_bloom".into(),        val_str(block, "logsBloom"));
    row.insert("transactions_root".into(), val_str(block, "transactionsRoot"));
    row.insert("state_root".into(),        val_str(block, "stateRoot"));
    row.insert("receipts_root".into(),     val_str(block, "receiptsRoot"));
    row.insert("miner".into(),             val_str(block, "miner"));
    row.insert("difficulty".into(),        val_hex_or(block, "difficulty",       0));
    row.insert("total_difficulty".into(),  val_hex_or(block, "totalDifficulty",  0));
    row.insert("size".into(),              val_hex_or(block, "size",             0));
    row.insert("extra_data".into(),        val_str(block, "extraData"));
    row.insert("gas_limit".into(),         val_hex_or(block, "gasLimit",         0));
    row.insert("gas_used".into(),          val_hex_or(block, "gasUsed",          0));
    row.insert("timestamp".into(),         val_hex_or(block, "timestamp",        0));
    row.insert("transaction_count".into(), serde_json::Value::from(tx_count));
    row.insert("base_fee_per_gas".into(),  val_hex(block, "baseFeePerGas"));
    row.insert("withdrawals_root".into(),  val_str(block, "withdrawalsRoot"));
    row.insert("withdrawals".into(),       val_arr(block, "withdrawals"));
    row.insert("blob_gas_used".into(),     val_hex(block, "blobGasUsed"));
    row.insert("excess_blob_gas".into(),   val_hex(block, "excessBlobGas"));
    row
}

/// Transactions enriched with receipt fields (receipt_* prefix).
/// receipt_by_hash: transactionHash (camelCase from JSON) → receipt JSON object.
fn extract_tx_rows(
    block: &serde_json::Value,
    receipt_by_hash: &HashMap<&str, &serde_json::Value>,
) -> Vec<Row> {
    let txs = match block.get("transactions").and_then(|v| v.as_array()) {
        Some(a) => a,
        None => return vec![],
    };
    let block_hash      = val_str(block, "hash");
    let block_number    = val_hex(block, "number");
    let block_timestamp = val_hex_or(block, "timestamp", 0);

    txs.iter().enumerate().map(|(i, tx)| {
        let mut row = HashMap::with_capacity(34);  // 21 tx + 12 receipt_* + 1 slack
        row.insert("hash".into(),                     val_str(tx, "hash"));
        row.insert("block_hash".into(),               block_hash.clone());
        row.insert("transaction_index".into(),        serde_json::Value::from(i as i64));
        row.insert("from_address".into(),             val_str(tx, "from"));
        row.insert("to_address".into(),               val_str(tx, "to"));
        row.insert("nonce".into(),                    val_hex(tx, "nonce"));
        row.insert("block_number".into(),             block_number.clone());
        row.insert("block_timestamp".into(),          block_timestamp.clone());
        row.insert("value".into(),                    val_hex(tx, "value"));
        row.insert("gas".into(),                      val_hex(tx, "gas"));
        row.insert("gas_price".into(),                val_hex(tx, "gasPrice"));
        row.insert("max_fee_per_gas".into(),          val_hex(tx, "maxFeePerGas"));
        row.insert("max_priority_fee_per_gas".into(), val_hex(tx, "maxPriorityFeePerGas"));
        row.insert("max_fee_per_blob_gas".into(),     val_hex(tx, "maxFeePerBlobGas"));
        row.insert("transaction_type".into(),         val_hex(tx, "type"));
        row.insert("chain_id".into(),                 val_hex(tx, "chainId"));
        row.insert("v".into(),                        val_hex(tx, "v"));
        row.insert("r".into(),                        val_str(tx, "r"));
        row.insert("s".into(),                        val_str(tx, "s"));
        row.insert("input".into(),                    val_str(tx, "input"));
        row.insert("blob_versioned_hashes".into(),    val_arr(tx, "blobVersionedHashes"));

        // Receipt enrichment — copy receipt_* fields from the matching receipt.
        if let Some(tx_hash) = tx.get("hash").and_then(|v| v.as_str()) {
            if let Some(receipt) = receipt_by_hash.get(tx_hash) {
                for (dest_key, src_key, is_hex) in RECEIPT_TX_FIELDS {
                    let v = if *is_hex { val_hex(receipt, src_key) } else { val_str(receipt, src_key) };
                    row.insert(dest_key.to_string(), v);
                }
            }
        }
        row
    }).collect()
}

/// Receipts and logs extracted with block_timestamp injected, plus token_transfers
/// decoded from each log's topic data (GIL-free ERC-20/721/1155 detection).
fn extract_receipts_logs_transfers(
    receipts: &[serde_json::Value],
    block_timestamp: &serde_json::Value,
) -> (Vec<Row>, Vec<Row>, Vec<Row>) {
    let mut receipt_rows  = Vec::with_capacity(receipts.len());
    let mut log_rows      = Vec::new();
    let mut transfer_rows = Vec::new();

    for r in receipts {
        let block_number = val_hex(r, "blockNumber");
        let block_hash   = val_str(r, "blockHash");

        let mut receipt = HashMap::with_capacity(21);
        receipt.insert("type".into(),                "receipt".into());
        receipt.insert("transaction_hash".into(),    val_str(r, "transactionHash"));
        receipt.insert("transaction_index".into(),   val_hex(r, "transactionIndex"));
        receipt.insert("block_hash".into(),          block_hash);
        receipt.insert("block_number".into(),        block_number);
        receipt.insert("block_timestamp".into(),     block_timestamp.clone());
        receipt.insert("from_address".into(),        val_str(r, "from"));
        receipt.insert("to_address".into(),          val_str(r, "to"));
        receipt.insert("cumulative_gas_used".into(), val_hex(r, "cumulativeGasUsed"));
        receipt.insert("gas_used".into(),            val_hex(r, "gasUsed"));
        receipt.insert("contract_address".into(),    val_str(r, "contractAddress"));
        receipt.insert("status".into(),              val_hex(r, "status"));
        receipt.insert("effective_gas_price".into(), val_hex(r, "effectiveGasPrice"));
        receipt.insert("transaction_type".into(),    val_hex(r, "type"));
        receipt.insert("l1_fee".into(),              val_hex(r, "l1Fee"));
        receipt.insert("l1_gas_used".into(),         val_hex(r, "l1GasUsed"));
        receipt.insert("l1_gas_price".into(),        val_hex(r, "l1GasPrice"));
        receipt.insert("l1_fee_scalar".into(),       val_str(r, "l1FeeScalar"));
        receipt.insert("blob_gas_price".into(),      val_hex(r, "blobGasPrice"));
        receipt.insert("blob_gas_used".into(),       val_hex(r, "blobGasUsed"));
        receipt_rows.push(receipt);

        if let Some(logs) = r.get("logs").and_then(|v| v.as_array()) {
            for log in logs {
                let mut log_row = HashMap::with_capacity(12);
                log_row.insert("type".into(),              "log".into());
                log_row.insert("log_index".into(),         val_hex(log, "logIndex"));
                log_row.insert("transaction_hash".into(),  val_str(log, "transactionHash"));
                log_row.insert("transaction_index".into(), val_hex(log, "transactionIndex"));
                log_row.insert("block_hash".into(),        val_str(log, "blockHash"));
                log_row.insert("block_number".into(),      val_hex(log, "blockNumber"));
                log_row.insert("block_timestamp".into(),   block_timestamp.clone());
                log_row.insert("address".into(),           val_str(log, "address"));
                log_row.insert("data".into(),              val_str(log, "data"));
                log_row.insert("topics".into(),            val_arr(log, "topics"));
                let removed = log.get("removed").and_then(|v| v.as_bool()).unwrap_or(false);
                log_row.insert("removed".into(), serde_json::Value::Bool(removed));

                // Token transfer detection from this log's topics.
                let transfers = token_transfers_from_log(&log_row);
                transfer_rows.extend(transfers);

                log_rows.push(log_row);
            }
        }
    }

    (receipt_rows, log_rows, transfer_rows)
}

// ---------------------------------------------------------------------------
// Rust-native row → Avro encode helpers (GIL-free path)
// ---------------------------------------------------------------------------

/// Convert a serde_json Value to a string using the same rules as Python's
/// normalize_scalar: strings pass through, numbers/bools use Display, arrays
/// and objects use compact JSON (matches json.dumps with separators=(',',':')).
fn json_val_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => String::new(),
        _ => serde_json::to_string(v).unwrap_or_default(),
    }
}

/// Extract a Rust-native Row (serde_json HashMap) into schema-ordered
/// Vec<Option<ExtractedValue>>, ready for encode_row.  No GIL required.
fn extract_row_from_json(row: &Row, fields: &[FieldSpec]) -> Vec<Option<ExtractedValue>> {
    fields
        .iter()
        .map(|field| {
            let v = match row.get(&field.name) {
                None | Some(serde_json::Value::Null) => return None,
                Some(v) => v,
            };
            match &field.kind {
                FieldKind::String => Some(ExtractedValue::Str(json_val_to_string(v))),
                FieldKind::Long => v.as_i64().map(ExtractedValue::Long),
                FieldKind::Bool => v.as_bool().map(ExtractedValue::Bool),
                FieldKind::StringArray => match v {
                    serde_json::Value::Array(arr) => Some(ExtractedValue::StrArr(
                        arr.iter().map(json_val_to_string).collect(),
                    )),
                    _ => Some(ExtractedValue::StrArr(vec![json_val_to_string(v)])),
                },
                FieldKind::LongArray => match v {
                    serde_json::Value::Array(arr) => Some(ExtractedValue::LongArr(
                        arr.iter().filter_map(|x| x.as_i64()).collect(),
                    )),
                    _ => None,
                },
                FieldKind::BoolArray => match v {
                    serde_json::Value::Array(arr) => Some(ExtractedValue::BoolArr(
                        arr.iter().filter_map(|x| x.as_bool()).collect(),
                    )),
                    _ => None,
                },
            }
        })
        .collect()
}

/// Pure Rust implementation: parse JSON, extract + enrich all entities, Avro-encode.
///
/// Returns (HashMap<entity_name, Vec<(kafka_key_str, avro_bytes)>>, block_ts_ms, ingest_ts_ms).
/// Only entities present in `entity_config` are processed and returned.
fn encode_block_envelope_impl(
    block_json_str: &str,
    receipts_json_str: &str,
    entity_config: &HashMap<String, (i64, Arc<SchemaEntry>)>,
) -> Result<(HashMap<String, Vec<(String, Vec<u8>)>>, i64, i64), String> {
    let block: serde_json::Value = serde_json::from_str(block_json_str)
        .map_err(|e| format!("block parse error: {e}"))?;
    let receipts: Vec<serde_json::Value> = match serde_json::from_str(receipts_json_str) {
        Ok(serde_json::Value::Array(arr)) => arr,
        Ok(_) => vec![],
        Err(e) => return Err(format!("receipts parse error: {e}")),
    };

    let ingest_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    // Block timestamp in seconds → milliseconds for Python-side lag computation.
    let block_ts_sec: i64 = block.get("timestamp")
        .and_then(|v| v.as_str())
        .and_then(|s| hex_to_i64(s))
        .unwrap_or(0);
    let block_ts_ms = block_ts_sec * 1000;

    let mut result: HashMap<String, Vec<(String, Vec<u8>)>> = HashMap::new();

    // Block
    if let Some((schema_id, entry)) = entity_config.get("block") {
        let mut row = extract_block_row(&block);
        let block_hash = block.get("hash").and_then(|v| v.as_str()).unwrap_or("");
        let event_id = format!("block_{block_hash}");
        row.insert("id".into(), serde_json::Value::String(event_id.clone()));
        row.insert("ingest_timestamp".into(), serde_json::Value::from(ingest_ts));
        let extracted = extract_row_from_json(&row, &entry.fields);
        let avro = encode_row(entry, *schema_id, extracted)?;
        result.insert("block".into(), vec![(event_id, avro)]);
    }

    // Transactions: build receipt lookup inside a block so it drops before
    // we borrow `receipts` again for log/transfer extraction.
    if let Some((schema_id, entry)) = entity_config.get("transaction") {
        let block_hash = block.get("hash").and_then(|v| v.as_str()).unwrap_or("").to_owned();
        let tx_rows = {
            let receipt_by_hash: HashMap<&str, &serde_json::Value> = receipts
                .iter()
                .filter_map(|r| r.get("transactionHash")?.as_str().map(|h| (h, r)))
                .collect();
            extract_tx_rows(&block, &receipt_by_hash)
        };
        let mut messages = Vec::with_capacity(tx_rows.len());
        for mut row in tx_rows {
            let tx_idx = row.get("transaction_index").and_then(|v| v.as_i64()).unwrap_or(0);
            let event_id = format!("enriched_transaction_{block_hash}_{tx_idx}");
            row.insert("id".into(), serde_json::Value::String(event_id.clone()));
            row.insert("ingest_timestamp".into(), serde_json::Value::from(ingest_ts));
            let extracted = extract_row_from_json(&row, &entry.fields);
            let avro = encode_row(entry, *schema_id, extracted)?;
            messages.push((event_id, avro));
        }
        result.insert("transaction".into(), messages);
    }

    // Logs and token_transfers share one extraction pass.
    let need_logs = entity_config.contains_key("log");
    let need_transfers = entity_config.contains_key("token_transfer");

    if need_logs || need_transfers {
        let block_timestamp = val_hex_or(&block, "timestamp", 0);
        let (_, log_rows, transfer_rows) =
            extract_receipts_logs_transfers(&receipts, &block_timestamp);

        if need_logs {
            if let Some((schema_id, entry)) = entity_config.get("log") {
                let mut messages = Vec::with_capacity(log_rows.len());
                for mut row in log_rows {
                    let tx_hash = row
                        .get("transaction_hash")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_owned();
                    let log_index = row.get("log_index").and_then(|v| v.as_i64()).unwrap_or(0);
                    let event_id = format!("log_{tx_hash}_{log_index}");
                    row.insert("id".into(), serde_json::Value::String(event_id.clone()));
                    row.insert("ingest_timestamp".into(), serde_json::Value::from(ingest_ts));
                    let extracted = extract_row_from_json(&row, &entry.fields);
                    let avro = encode_row(entry, *schema_id, extracted)?;
                    messages.push((event_id, avro));
                }
                result.insert("log".into(), messages);
            }
        }

        if need_transfers {
            if let Some((schema_id, entry)) = entity_config.get("token_transfer") {
                let mut messages = Vec::with_capacity(transfer_rows.len());
                for mut row in transfer_rows {
                    // build_token_transfer already set "id" = "token_transfer_{hash}_{idx}_{n}"
                    let event_id = row
                        .get("id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_owned();
                    row.insert("ingest_timestamp".into(), serde_json::Value::from(ingest_ts));
                    let extracted = extract_row_from_json(&row, &entry.fields);
                    let avro = encode_row(entry, *schema_id, extracted)?;
                    messages.push((event_id, avro));
                }
                result.insert("token_transfer".into(), messages);
            }
        }
    }

    Ok((result, block_ts_ms, ingest_ts))
}

/// Parse raw_envelope JSON strings, encode all entity rows to Avro bytes
/// in a single GIL-free pass, and return pre-encoded messages by entity.
///
/// Arguments:
///   block_json_str    – raw JSON from eth_getBlockByNumber(true)
///   receipts_json_str – raw JSON from eth_getBlockReceipts
///   entity_schema_ids – Python dict: entity_name → (schema_id: int, topic: str)
///                       Only entities listed here are processed.
///
/// Returns dict[entity_name, list[(key_bytes, avro_bytes)]]:
///   key_bytes  – Kafka partition key (event_id UTF-8)
///   avro_bytes – Confluent wire format (magic byte + 4-byte schema_id + avro)
///
/// GIL strategy:
///   Phase 1 (GIL held, ~0.01ms) — extract schema_ids from Python dict.
///   Phase 2 (GIL released)      — serde_json parse + entity extraction + Avro encode.
///   Phase 3 (GIL held, ~0.1ms)  — wrap encoded bytes as PyBytes objects.
///
/// Compare: parse_block_envelope holds the GIL for ~15ms (rows_to_pylist).
#[pyfunction]
fn parse_and_encode_block_envelope(
    py: Python<'_>,
    block_json_str: &str,
    receipts_json_str: &str,
    entity_schema_ids: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    // Phase 1: extract entity config (GIL held — Arc clone is cheap)
    let entity_config: HashMap<String, (i64, Arc<SchemaEntry>)> = {
        let cache = SCHEMA_CACHE.read().unwrap();
        entity_schema_ids
            .iter()
            .filter_map(|(k, v)| {
                let entity = k.extract::<String>().ok()?;
                let (schema_id, _topic): (i64, String) = v.extract().ok()?;
                let entry = cache.get(&schema_id)?.clone();
                Some((entity, (schema_id, entry)))
            })
            .collect()
    };

    // Phase 2: all CPU work with GIL released
    let (encoded, block_ts_ms, ingest_ts_ms): (HashMap<String, Vec<(String, Vec<u8>)>>, i64, i64) = py
        .allow_threads(|| {
            encode_block_envelope_impl(block_json_str, receipts_json_str, &entity_config)
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // Phase 3: wrap bytes into Python objects (~0.1ms for ~1700 rows)
    let out = PyDict::new_bound(py);
    for (entity, messages) in encoded {
        let list = PyList::empty_bound(py);
        for (key_str, avro) in messages {
            let key_py = PyBytes::new_bound(py, key_str.as_bytes());
            let avro_py = PyBytes::new_bound(py, &avro);
            list.append((key_py, avro_py))?;
        }
        out.set_item(entity, list)?;
    }
    // __meta__ carries block/ingest timestamps so Python can compute ingestion_lag_ms
    // without decoding the Avro bytes. Stored as a list so the engine's extend() call
    // works correctly (dict values in parsed_bundle must be lists).
    let meta_dict = PyDict::new_bound(py);
    meta_dict.set_item("block_timestamp_ms", block_ts_ms)?;
    meta_dict.set_item("ingest_timestamp_ms", ingest_ts_ms)?;
    let meta_list = PyList::empty_bound(py);
    meta_list.append(meta_dict)?;
    out.set_item("__meta__", meta_list)?;
    Ok(out.unbind().into_any())
}

/// Convert a serde_json Value to a Python object (GIL must be held).
#[allow(deprecated)]
fn json_to_py(py: Python<'_>, v: &serde_json::Value) -> PyObject {
    match v {
        serde_json::Value::Null       => py.None(),
        serde_json::Value::Bool(b)    => b.into_py(py),
        serde_json::Value::Number(n)  => {
            if let Some(i) = n.as_i64() { i.into_py(py) }
            else if let Some(f) = n.as_f64() { f.into_py(py) }
            else { py.None() }
        }
        serde_json::Value::String(s)  => s.as_str().into_py(py),
        serde_json::Value::Array(arr) => {
            let list = PyList::empty_bound(py);
            for item in arr { list.append(json_to_py(py, item)).unwrap(); }
            list.unbind().into_any()
        }
        serde_json::Value::Object(map) => {
            let dict = PyDict::new_bound(py);
            for (k, v) in map { dict.set_item(k.as_str(), json_to_py(py, v)).unwrap(); }
            dict.unbind().into_any()
        }
    }
}

#[allow(deprecated)]
fn rows_to_pylist(py: Python<'_>, rows: Vec<Row>) -> PyObject {
    let list = PyList::empty_bound(py);
    for row in rows {
        let dict = PyDict::new_bound(py);
        for (k, v) in row {
            dict.set_item(k.as_str(), json_to_py(py, &v)).unwrap();
        }
        list.append(dict).unwrap();
    }
    list.unbind().into_any()
}

/// Parse raw_envelope JSON strings into fully enriched EVM entity rows.
///
/// Arguments:
///   block_json_str    – raw JSON string from eth_getBlockByNumber(true)
///   receipts_json_str – raw JSON string from eth_getBlockReceipts (array or "null")
///
/// Returns a dict:
///   {"block": [...], "transaction": [...], "receipt": [...],
///    "log": [...], "token_transfer": [...]}
///
/// All enrichment is done inside allow_threads (GIL released):
///   • transaction rows include receipt_* fields (EvmEnricher.enrich equivalent)
///   • receipt / log rows include block_timestamp (EvmEnricher._inject_block_context)
///   • token_transfer rows detected from log topics (EvmDecoder.decode equivalent)
///
/// Bypasses Python EvmDecoder and EvmEnricher entirely — pass decoder=None,
/// enricher=None to IngestionEngine when this function is available.
#[pyfunction]
fn parse_block_envelope(
    py: Python<'_>,
    block_json_str: &str,
    receipts_json_str: &str,
) -> PyResult<PyObject> {
    // Rust-only phase: JSON parse + ALL entity extraction with GIL released.
    let (block_row, tx_rows, receipt_rows, log_rows, transfer_rows) = py
        .allow_threads(|| -> Result<_, String> {
            let block: serde_json::Value = serde_json::from_str(block_json_str)
                .map_err(|e| format!("block_json parse error: {e}"))?;

            let receipts: Vec<serde_json::Value> =
                match serde_json::from_str(receipts_json_str) {
                    Ok(serde_json::Value::Array(arr)) => arr,
                    Ok(_)  => vec![],
                    Err(e) => return Err(format!("receipts_json parse error: {e}")),
                };

            // Build receipt lookup by transactionHash for tx enrichment.
            let receipt_by_hash: HashMap<&str, &serde_json::Value> = receipts
                .iter()
                .filter_map(|r| r.get("transactionHash")?.as_str().map(|h| (h, r)))
                .collect();

            let block_timestamp = val_hex_or(&block, "timestamp", 0);
            let block_row  = extract_block_row(&block);
            let tx_rows    = extract_tx_rows(&block, &receipt_by_hash);
            let (receipt_rows, log_rows, transfer_rows) =
                extract_receipts_logs_transfers(&receipts, &block_timestamp);

            Ok((block_row, tx_rows, receipt_rows, log_rows, transfer_rows))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // GIL-held phase: convert Rust types → Python dicts (fast, no JSON work).
    let out = PyDict::new_bound(py);
    out.set_item("block",          rows_to_pylist(py, vec![block_row]))?;
    out.set_item("transaction",    rows_to_pylist(py, tx_rows))?;
    out.set_item("receipt",        rows_to_pylist(py, receipt_rows))?;
    out.set_item("log",            rows_to_pylist(py, log_rows))?;
    out.set_item("token_transfer", rows_to_pylist(py, transfer_rows))?;
    Ok(out.unbind().into_any())
}

#[pymodule]
fn chainlake_avro(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(register_schema, m)?)?;
    m.add_function(wrap_pyfunction!(encode_batch, m)?)?;
    m.add_function(wrap_pyfunction!(parse_block_envelope, m)?)?;
    m.add_function(wrap_pyfunction!(parse_and_encode_block_envelope, m)?)?;
    Ok(())
}
