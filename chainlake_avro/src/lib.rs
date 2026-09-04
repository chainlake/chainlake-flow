use std::collections::HashMap;
use std::sync::{Arc, RwLock};

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
//   allow_threads phase  — serde_json parse + entity extraction (pure Rust,
//                          no Python objects touched; multiple workers run
//                          truly in parallel on separate OS threads).
//   GIL-held phase       — convert Rust HashMaps → Python dicts/lists.
//                          Fast: no JSON parsing, no hex conversion.
//
// This replaces the Python parse_blocks / parse_transactions / parse_receipts
// hot path in the derived pipeline. Benchmark: ~1–2 ms/block in Rust vs
// ~100–300 ms/block in Python, lifting single-pod derived throughput from
// ~3 blk/s to ~100+ blk/s.
// ---------------------------------------------------------------------------

type Row = HashMap<String, serde_json::Value>;

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

fn extract_tx_rows(block: &serde_json::Value) -> Vec<Row> {
    let txs = match block.get("transactions").and_then(|v| v.as_array()) {
        Some(a) => a,
        None => return vec![],
    };
    let block_hash      = val_str(block, "hash");
    let block_number    = val_hex(block, "number");
    let block_timestamp = val_hex_or(block, "timestamp", 0);

    txs.iter()
        .enumerate()
        .map(|(i, tx)| {
            let mut row = HashMap::with_capacity(22);
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
            row
        })
        .collect()
}

fn extract_receipts_and_logs(receipts: &[serde_json::Value]) -> (Vec<Row>, Vec<Row>) {
    let mut receipt_rows = Vec::with_capacity(receipts.len());
    let mut log_rows     = Vec::new();

    for r in receipts {
        let block_number = val_hex(r, "blockNumber");
        let block_hash   = val_str(r, "blockHash");

        let mut receipt = HashMap::with_capacity(20);
        receipt.insert("type".into(),               "receipt".into());
        receipt.insert("transaction_hash".into(),   val_str(r, "transactionHash"));
        receipt.insert("transaction_index".into(),  val_hex(r, "transactionIndex"));
        receipt.insert("block_hash".into(),         block_hash);
        receipt.insert("block_number".into(),       block_number);
        receipt.insert("from_address".into(),       val_str(r, "from"));
        receipt.insert("to_address".into(),         val_str(r, "to"));
        receipt.insert("cumulative_gas_used".into(),val_hex(r, "cumulativeGasUsed"));
        receipt.insert("gas_used".into(),           val_hex(r, "gasUsed"));
        receipt.insert("contract_address".into(),   val_str(r, "contractAddress"));
        receipt.insert("status".into(),             val_hex(r, "status"));
        receipt.insert("effective_gas_price".into(),val_hex(r, "effectiveGasPrice"));
        receipt.insert("transaction_type".into(),   val_hex(r, "type"));
        receipt.insert("l1_fee".into(),             val_hex(r, "l1Fee"));
        receipt.insert("l1_gas_used".into(),        val_hex(r, "l1GasUsed"));
        receipt.insert("l1_gas_price".into(),       val_hex(r, "l1GasPrice"));
        receipt.insert("l1_fee_scalar".into(),      val_str(r, "l1FeeScalar"));
        receipt.insert("blob_gas_price".into(),     val_hex(r, "blobGasPrice"));
        receipt.insert("blob_gas_used".into(),      val_hex(r, "blobGasUsed"));
        receipt_rows.push(receipt);

        if let Some(logs) = r.get("logs").and_then(|v| v.as_array()) {
            for log in logs {
                let mut log_row = HashMap::with_capacity(11);
                log_row.insert("type".into(),              "log".into());
                log_row.insert("log_index".into(),         val_hex(log, "logIndex"));
                log_row.insert("transaction_hash".into(),  val_str(log, "transactionHash"));
                log_row.insert("transaction_index".into(), val_hex(log, "transactionIndex"));
                log_row.insert("block_hash".into(),        val_str(log, "blockHash"));
                log_row.insert("block_number".into(),      val_hex(log, "blockNumber"));
                // block_timestamp: not present in receipt logs JSON; Python parse_receipts
                // also leaves this None — the Avro schema accepts null here.
                log_row.insert("block_timestamp".into(),   serde_json::Value::Null);
                log_row.insert("address".into(),           val_str(log, "address"));
                log_row.insert("data".into(),              val_str(log, "data"));
                log_row.insert("topics".into(),            val_arr(log, "topics"));
                let removed = log.get("removed")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                log_row.insert("removed".into(), serde_json::Value::Bool(removed));
                log_rows.push(log_row);
            }
        }
    }

    (receipt_rows, log_rows)
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

/// Parse raw_envelope JSON strings into EVM entity rows.
///
/// Arguments:
///   block_json_str    – raw JSON string from eth_getBlockByNumber(true)
///   receipts_json_str – raw JSON string from eth_getBlockReceipts (array or "null")
///
/// Returns a dict: {"block": [...], "transaction": [...], "receipt": [...], "log": [...]}
///
/// GIL is released for the entire serde_json parse + entity extraction phase
/// via py.allow_threads(), so multiple derived pipeline workers can decode
/// blocks in true parallel on separate OS threads.
#[pyfunction]
fn parse_block_envelope(
    py: Python<'_>,
    block_json_str: &str,
    receipts_json_str: &str,
) -> PyResult<PyObject> {
    // Rust-only phase: JSON parse + field extraction with GIL released.
    let (block_row, tx_rows, receipt_rows, log_rows) = py
        .allow_threads(|| -> Result<_, String> {
            let block: serde_json::Value = serde_json::from_str(block_json_str)
                .map_err(|e| format!("block_json parse error: {e}"))?;

            // receipts may be a JSON array or JSON null (no-tx blocks on some nodes)
            let receipts: Vec<serde_json::Value> =
                match serde_json::from_str(receipts_json_str) {
                    Ok(serde_json::Value::Array(arr)) => arr,
                    Ok(_)  => vec![],
                    Err(e) => return Err(format!("receipts_json parse error: {e}")),
                };

            let block_row                  = extract_block_row(&block);
            let tx_rows                    = extract_tx_rows(&block);
            let (receipt_rows, log_rows)   = extract_receipts_and_logs(&receipts);

            Ok((block_row, tx_rows, receipt_rows, log_rows))
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    // GIL-held phase: convert Rust types → Python dicts (fast, no JSON work).
    let out = PyDict::new_bound(py);
    out.set_item("block",       rows_to_pylist(py, vec![block_row]))?;
    out.set_item("transaction", rows_to_pylist(py, tx_rows))?;
    out.set_item("receipt",     rows_to_pylist(py, receipt_rows))?;
    out.set_item("log",         rows_to_pylist(py, log_rows))?;
    Ok(out.unbind().into_any())
}

#[pymodule]
fn chainlake_avro(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(register_schema, m)?)?;
    m.add_function(wrap_pyfunction!(encode_batch, m)?)?;
    m.add_function(wrap_pyfunction!(parse_block_envelope, m)?)?;
    Ok(())
}
